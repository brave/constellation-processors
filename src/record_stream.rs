use async_trait::async_trait;
use derive_more::{Display, Error, From};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
  stream_consumer::StreamConsumer, CommitMode, Consumer, ConsumerContext, Rebalance,
};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::Message;
use rdkafka::producer::{future_producer::FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

const KAFKA_ENC_TOPIC_ENV_KEY: &str = "KAFKA_ENCRYPTED_TOPIC";
const KAFKA_OUT_TOPIC_ENV_KEY: &str = "KAFKA_OUTPUT_TOPIC";
const DEFAULT_ENC_KAFKA_TOPIC: &str = "p3a-star-enc";
const DEFAULT_OUT_KAFKA_TOPIC: &str = "p3a-star-out";
const KAFKA_BROKERS_ENV_KEY: &str = "KAFKA_BROKERS";
const KAFKA_ENABLE_PLAINTEXT_ENV_KEY: &str = "KAFKA_ENABLE_PLAINTEXT";

const KAFKA_INIT_TRX_TIMEOUT_SECS: u64 = 30;
const KAFKA_COMMIT_TRX_TIMEOUT_SECS: u64 = 60 * 30;

#[derive(Debug, Display, Error, From)]
#[display(fmt = "Record stream error: {}")]
pub enum RecordStreamError {
  Kafka(KafkaError),
  Deserialize,
  ProducerNotPresent,
  TestConsumeTimeout,
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

  async fn produce(&self, record: &str) -> Result<(), RecordStreamError>;

  async fn consume(&self) -> Result<String, RecordStreamError>;

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError>;
}

pub type DynRecordStream = dyn RecordStream + Send + Sync;
pub type RecordStreamArc = Arc<DynRecordStream>;

pub struct KafkaRecordStream {
  producer: Option<FutureProducer<KafkaContext>>,
  consumer: Option<StreamConsumer<KafkaContext>>,
  tpl: Mutex<TopicPartitionList>,
  topic: String,
}

impl KafkaRecordStream {
  pub fn new(enable_producer: bool, enable_consumer: bool, use_output_topic: bool) -> Self {
    let topic = if use_output_topic {
      env::var(KAFKA_OUT_TOPIC_ENV_KEY).unwrap_or(DEFAULT_OUT_KAFKA_TOPIC.to_string())
    } else {
      env::var(KAFKA_ENC_TOPIC_ENV_KEY).unwrap_or(DEFAULT_ENC_KAFKA_TOPIC.to_string())
    };

    let mut result = Self {
      producer: None,
      consumer: None,
      tpl: Mutex::new(TopicPartitionList::new()),
      topic: topic.clone(),
    };
    if enable_producer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      let mut config_ref = &mut config;
      if use_output_topic {
        config_ref = config_ref.set("transactional.id", "main");
      }
      result.producer = Some(
        config_ref
          .set("message.timeout.ms", "6000")
          .set("transaction.timeout.ms", "3600000")
          .create_with_context(context)
          .unwrap(),
      );
    }
    if enable_consumer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      result.consumer = Some(
        config
          .set("group.id", "star-agg")
          .set("enable.auto.commit", "false")
          .set("session.timeout.ms", "21000")
          .set("max.poll.interval.ms", "14400000")
          .set("auto.offset.reset", "smallest")
          .create_with_context(context)
          .unwrap(),
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
    let producer = self.producer
      .as_ref()
      .ok_or(RecordStreamError::ProducerNotPresent)?;
    let timeout = Duration::from_secs(KAFKA_COMMIT_TRX_TIMEOUT_SECS);
    if let Err(e) = producer.commit_transaction(timeout) {
      producer.abort_transaction(timeout)?;
      return Err(RecordStreamError::from(e));
    }
    Ok(())
  }

  async fn produce(&self, record: &str) -> Result<(), RecordStreamError> {
    let producer = self.producer.as_ref().expect("Kafka producer not enabled");
    let record: FutureRecord<str, str> = FutureRecord::to(&self.topic).payload(record);
    let send_result = producer.send(record, Duration::from_secs(12)).await;
    match send_result {
      Ok(_) => Ok(()),
      Err((e, _)) => Err(RecordStreamError::from(e)),
    }
  }

  async fn consume(&self) -> Result<String, RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    let msg = consumer.recv().await?;
    let payload = match msg.payload_view::<str>() {
      None => Ok(""),
      Some(s) => s.map_err(|_| RecordStreamError::Deserialize),
    }?;
    trace!(
      "recv partition = {} offset = {}",
      msg.partition(),
      msg.offset()
    );
    let mut tpl = self.tpl.lock().await;
    tpl.add_partition_offset(
      msg.topic(),
      msg.partition(),
      Offset::Offset(msg.offset() + 1),
    )?;
    Ok(payload.to_string())
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    let tpl = self.tpl.lock().await;
    trace!("committing = {:?}", tpl);
    consumer.commit(&tpl, CommitMode::Sync)?;
    Ok(())
  }
}

#[derive(Default)]
pub struct TestRecordStream {
  pub records_to_consume: Mutex<Vec<String>>,
  pub records_produced: Mutex<Vec<String>>,
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

  async fn produce(&self, record: &str) -> Result<(), RecordStreamError> {
    self.records_produced.lock().await.push(record.to_string());
    Ok(())
  }

  async fn consume(&self) -> Result<String, RecordStreamError> {
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
