use async_trait::async_trait;
use derive_more::{Display, Error, From};
use futures::future::try_join_all;
use rand::{seq::SliceRandom, thread_rng};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
  stream_consumer::StreamConsumer, CommitMode, Consumer, ConsumerContext, Rebalance,
};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::Message;
use rdkafka::producer::{future_producer::FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{error::SendError, unbounded_channel, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;

use crate::channel::{get_data_channel_map_from_env, get_data_channel_value_from_env};
use crate::util::parse_env_var;

const KAFKA_ENC_TOPICS_ENV_KEY: &str = "KAFKA_ENCRYPTED_TOPICS";
const KAFKA_OUT_TOPICS_ENV_KEY: &str = "KAFKA_OUTPUT_TOPICS";
const DEFAULT_ENC_KAFKA_TOPICS: &str = "typical=p3a-star-enc";
const DEFAULT_OUT_KAFKA_TOPICS: &str = "typical=p3a-star-out";
const KAFKA_BROKERS_ENV_KEY: &str = "KAFKA_BROKERS";
const KAFKA_ENABLE_PLAINTEXT_ENV_KEY: &str = "KAFKA_ENABLE_PLAINTEXT";
const KAFKA_PRODUCER_QUEUE_TASK_COUNT_ENV_KEY: &str = "KAFKA_PRODUCE_QUEUE_TASK_COUNT";
const KAFKA_TLS_CA_CERT_PATH_ENV_KEY: &str = "KAFKA_TLS_CA_CERT_PATH";
const KAFKA_TLS_CERT_PATH_ENV_KEY: &str = "KAFKA_TLS_CERT_PATH";
const KAFKA_TLS_KEY_PATH_ENV_KEY: &str = "KAFKA_TLS_KEY_PATH";
const DEFAULT_KAFKA_PRODUCER_QUEUE_TASK_COUNT: &str = "64";

const KAFKA_INIT_TRX_TIMEOUT_SECS: u64 = 30;
const KAFKA_COMMIT_TRX_TIMEOUT_SECS: u64 = 60 * 30;

const KAFKA_PRODUCE_TIMEOUT_SECS: u64 = 12;

#[derive(Debug, Display, Error, From)]
#[display(fmt = "Record stream error: {}")]
pub enum RecordStreamError {
  Kafka(KafkaError),
  Deserialize,
  ProducerNotPresent,
  TestConsumeTimeout,
  MpscSendError(SendError<Vec<u8>>),
  Join(JoinError),
}

struct KafkaContext;

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
  fn pre_rebalance(&self, rebalance: &Rebalance) {
    info!("Kafka: rebalancing: {:?}", rebalance);
  }

  fn post_rebalance(&self, _rebalance: &Rebalance) {
    info!("Kafka: rebalance complete");
  }

  fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
    debug!("Kafka: committing offsets: {:?}", result);
  }
}

#[async_trait]
pub trait RecordStream {
  fn init_producer_transactions(&self) -> Result<(), RecordStreamError>;

  fn begin_producer_transaction(&self) -> Result<(), RecordStreamError>;

  fn commit_producer_transaction(&self) -> Result<(), RecordStreamError>;

  fn has_assigned_partitions(&self) -> Result<bool, RecordStreamError>;

  async fn produce(&self, record: &[u8]) -> Result<(), RecordStreamError>;

  async fn init_producer_queues(&self);

  async fn queue_produce(&self, record: Vec<u8>) -> Result<(), RecordStreamError>;

  async fn join_produce_queues(&self) -> Result<(), RecordStreamError>;

  async fn consume(&self) -> Result<Vec<u8>, RecordStreamError>;

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError>;
}

pub type DynRecordStream = dyn RecordStream + Send + Sync;
pub type RecordStreamArc = Arc<DynRecordStream>;

pub struct KafkaRecordStream {
  producer: Option<Arc<FutureProducer<KafkaContext>>>,
  consumer: Option<StreamConsumer<KafkaContext>>,
  topic: String,
  producer_queues: RwLock<
    Vec<(
      JoinHandle<Result<(), RecordStreamError>>,
      UnboundedSender<Vec<u8>>,
    )>,
  >,
}

pub fn get_data_channel_topic_map_from_env(use_output_topics: bool) -> HashMap<String, String> {
  match use_output_topics {
    true => get_data_channel_map_from_env(KAFKA_OUT_TOPICS_ENV_KEY, DEFAULT_OUT_KAFKA_TOPICS),
    false => get_data_channel_map_from_env(KAFKA_ENC_TOPICS_ENV_KEY, DEFAULT_ENC_KAFKA_TOPICS),
  }
}

pub fn get_data_channel_topic_from_env(use_output_topic: bool, channel_name: &str) -> String {
  match use_output_topic {
    true => get_data_channel_value_from_env(
      KAFKA_OUT_TOPICS_ENV_KEY,
      DEFAULT_OUT_KAFKA_TOPICS,
      channel_name,
    ),
    false => get_data_channel_value_from_env(
      KAFKA_ENC_TOPICS_ENV_KEY,
      DEFAULT_ENC_KAFKA_TOPICS,
      channel_name,
    ),
  }
}

impl KafkaRecordStream {
  pub fn new(
    enable_producer: bool,
    enable_consumer: bool,
    topic: String,
    use_output_group_id: bool,
  ) -> Self {
    let group_id = match use_output_group_id {
      true => "star-agg-dec",
      false => "star-agg-enc",
    };

    let mut result = Self {
      producer: None,
      consumer: None,
      topic: topic.clone(),
      producer_queues: RwLock::new(Vec::new()),
    };
    if enable_producer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      let mut config_ref = &mut config;
      if use_output_group_id {
        config_ref = config_ref.set("transactional.id", "main");
      }
      result.producer = Some(Arc::new(
        config_ref
          .set("message.timeout.ms", "3600000")
          .set("transaction.timeout.ms", "3600000")
          .set("request.timeout.ms", "900000")
          .set("socket.timeout.ms", "300000")
          .create_with_context(context)
          .unwrap(),
      ));
      info!("Producing to topic: {}", topic);
    }
    if enable_consumer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      result.consumer = Some(
        config
          .set("group.id", group_id)
          .set("enable.auto.commit", "false")
          .set("session.timeout.ms", "21000")
          .set("max.poll.interval.ms", "14400000")
          .set("auto.offset.reset", "earliest")
          .set("queued.max.messages.kbytes", "300000")
          .create_with_context(context)
          .unwrap(),
      );
      info!(
        "Consuming from topic: {} (current offsets: {:?})",
        topic,
        result.consumer.as_ref().unwrap().position().unwrap()
      );
      result
        .consumer
        .as_ref()
        .unwrap()
        .subscribe(&[&topic])
        .unwrap();
    }
    result
  }

  fn new_client_config() -> ClientConfig {
    let brokers = env::var(KAFKA_BROKERS_ENV_KEY)
      .unwrap_or_else(|_| panic!("{} env var must be defined", KAFKA_BROKERS_ENV_KEY));
    let mut result = ClientConfig::new();
    result.set("bootstrap.servers", brokers);
    if env::var(KAFKA_ENABLE_PLAINTEXT_ENV_KEY).unwrap_or_default() == "true" {
      result.set("security.protocol", "plaintext");
    }
    if let Ok(cert_path) = env::var(KAFKA_TLS_CERT_PATH_ENV_KEY) {
      result
        .set("security.protocol", "ssl")
        .set("ssl.certificate.location", cert_path);
    }
    if let Ok(cert_path) = env::var(KAFKA_TLS_CA_CERT_PATH_ENV_KEY) {
      result.set("ssl.ca.location", cert_path);
    }
    if let Ok(key_path) = env::var(KAFKA_TLS_KEY_PATH_ENV_KEY) {
      result.set("ssl.key.location", key_path);
    }
    result
  }
}

#[async_trait]
impl RecordStream for KafkaRecordStream {
  fn init_producer_transactions(&self) -> Result<(), RecordStreamError> {
    Ok(
      self
        .producer
        .as_ref()
        .ok_or(RecordStreamError::ProducerNotPresent)?
        .init_transactions(Duration::from_secs(KAFKA_INIT_TRX_TIMEOUT_SECS))?,
    )
  }

  fn begin_producer_transaction(&self) -> Result<(), RecordStreamError> {
    Ok(
      self
        .producer
        .as_ref()
        .ok_or(RecordStreamError::ProducerNotPresent)?
        .begin_transaction()?,
    )
  }

  fn commit_producer_transaction(&self) -> Result<(), RecordStreamError> {
    let producer = self
      .producer
      .as_ref()
      .ok_or(RecordStreamError::ProducerNotPresent)?;
    let timeout = Duration::from_secs(KAFKA_COMMIT_TRX_TIMEOUT_SECS);
    if let Err(e) = producer.commit_transaction(timeout) {
      producer.abort_transaction(timeout)?;
      return Err(RecordStreamError::from(e));
    }
    Ok(())
  }

  fn has_assigned_partitions(&self) -> Result<bool, RecordStreamError> {
    if let Some(consumer) = self.consumer.as_ref() {
      return Ok(consumer.assignment()?.count() > 0);
    }
    Ok(false)
  }

  async fn produce(&self, record: &[u8]) -> Result<(), RecordStreamError> {
    let producer = self.producer.as_ref().expect("Kafka producer not enabled");
    let record: FutureRecord<str, [u8]> = FutureRecord::to(&self.topic).payload(record);
    let send_result = producer
      .send(record, Duration::from_secs(KAFKA_PRODUCE_TIMEOUT_SECS))
      .await;
    send_result.map_err(|(e, _)| RecordStreamError::from(e))?;
    Ok(())
  }

  async fn init_producer_queues(&self) {
    let task_count = parse_env_var::<usize>(
      KAFKA_PRODUCER_QUEUE_TASK_COUNT_ENV_KEY,
      DEFAULT_KAFKA_PRODUCER_QUEUE_TASK_COUNT,
    );
    let mut producer_queues = self.producer_queues.write().await;
    for _ in 0..task_count {
      let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
      let producer = self.producer.as_ref().unwrap().clone();
      let topic = self.topic.clone();
      let handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
          let record: FutureRecord<str, [u8]> = FutureRecord::to(&topic).payload(&msg);
          let send_result = producer
            .send(record, Duration::from_secs(KAFKA_PRODUCE_TIMEOUT_SECS))
            .await;
          send_result.map_err(|(e, _)| RecordStreamError::from(e))?;
        }
        Ok(())
      });
      producer_queues.push((handle, tx));
    }
  }

  async fn queue_produce(&self, record: Vec<u8>) -> Result<(), RecordStreamError> {
    let producer_queues = self.producer_queues.read().await;
    let (_, tx) = producer_queues.choose(&mut thread_rng()).unwrap();
    Ok(tx.send(record)?)
  }

  async fn join_produce_queues(&self) -> Result<(), RecordStreamError> {
    let mut producer_queues = self.producer_queues.write().await;
    try_join_all(
      producer_queues
        .drain(..)
        .map(|(handle, _)| handle)
        .collect::<Vec<_>>(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<()>, RecordStreamError>>()?;
    Ok(())
  }

  async fn consume(&self) -> Result<Vec<u8>, RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    let msg = consumer.recv().await?;
    let empty = Vec::new();
    let payload = match msg.payload_view::<[u8]>() {
      None => Ok(empty.as_slice()),
      Some(s) => s.map_err(|_| RecordStreamError::Deserialize),
    }?;
    trace!(
      "recv partition = {} offset = {}",
      msg.partition(),
      msg.offset()
    );
    Ok(payload.to_vec())
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    trace!("committing");
    if let Err(e) = consumer.commit_consumer_state(CommitMode::Sync) {
      if let Some(e_code) = e.rdkafka_error_code() {
        if e_code == RDKafkaErrorCode::NoOffset {
          // No messages were consumed in this case; we can ignore this error
          return Ok(());
        }
      }
      Err(RecordStreamError::from(e))
    } else {
      Ok(())
    }
  }
}

#[derive(Default)]
pub struct TestRecordStream {
  pub records_to_consume: Mutex<Vec<Vec<u8>>>,
  pub records_produced: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl RecordStream for TestRecordStream {
  fn init_producer_transactions(&self) -> Result<(), RecordStreamError> {
    Ok(())
  }

  fn begin_producer_transaction(&self) -> Result<(), RecordStreamError> {
    Ok(())
  }

  fn commit_producer_transaction(&self) -> Result<(), RecordStreamError> {
    Ok(())
  }

  fn has_assigned_partitions(&self) -> Result<bool, RecordStreamError> {
    Ok(true)
  }

  async fn produce(&self, record: &[u8]) -> Result<(), RecordStreamError> {
    self.records_produced.lock().await.push(record.to_vec());
    Ok(())
  }

  async fn init_producer_queues(&self) {}

  async fn queue_produce(&self, record: Vec<u8>) -> Result<(), RecordStreamError> {
    self.produce(&record).await
  }

  async fn join_produce_queues(&self) -> Result<(), RecordStreamError> {
    Ok(())
  }

  async fn consume(&self) -> Result<Vec<u8>, RecordStreamError> {
    let mut records_to_consume = self.records_to_consume.lock().await;
    if records_to_consume.is_empty() {
      drop(records_to_consume);
      sleep(Duration::from_secs(90)).await;
      return Err(RecordStreamError::TestConsumeTimeout);
    }
    Ok(records_to_consume.remove(0))
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    Ok(())
  }
}
