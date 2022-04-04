use super::{RecordStream, RecordStreamError};
use tokio::sync::Mutex;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{Consumer, ConsumerContext,
  stream_consumer::StreamConsumer, Rebalance, CommitMode};
use rdkafka::producer::{future_producer::FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{TopicPartitionList, Offset};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::error::KafkaResult;
use async_trait::async_trait;
use std::env;
use std::time::Duration;

const KAFKA_TOPIC_ENV_KEY: &str = "KAFKA_TOPIC";
const DEFAULT_KAFKA_TOPIC: &str = "p3a-star";
const KAFKA_BROKERS_ENV_KEY: &str = "KAFKA_BROKERS";
const KAFKA_ENABLE_PLAINTEXT_ENV_KEY: &str = "KAFKA_ENABLE_PLAINTEXT";

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

pub struct KafkaRecordStream {
  producer: Option<FutureProducer<KafkaContext>>,
  consumer: Option<StreamConsumer<KafkaContext>>,
  tpl: Mutex<TopicPartitionList>,
  topic: String
}

impl KafkaRecordStream {
  pub fn new(enable_producer: bool, enable_consumer: bool) -> Self {
    let topic = env::var(KAFKA_TOPIC_ENV_KEY).unwrap_or(DEFAULT_KAFKA_TOPIC.to_string());

    let mut result = KafkaRecordStream {
      producer: None,
      consumer: None,
      tpl: Mutex::new(TopicPartitionList::new()),
      topic: topic.clone()
    };
    if enable_producer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      result.producer = Some(config
        .set("message.timeout.ms", "6000")
        .create_with_context(context)
        .unwrap()
      );
    }
    if enable_consumer {
      let context = KafkaContext;
      let mut config = Self::new_client_config();
      result.consumer = Some(config
        .set("group.id", "star-agg")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create_with_context(context)
        .unwrap());
      result.consumer.as_ref().unwrap().subscribe(&[&topic]).unwrap();
    }
    result
  }

  fn new_client_config() -> ClientConfig {
    let brokers = env::var(KAFKA_BROKERS_ENV_KEY)
      .expect("KAFKA_BROKERS env var must be defined");
    let mut result = ClientConfig::new();
    result.set("bootstrap.servers", brokers.clone());
    if env::var(KAFKA_ENABLE_PLAINTEXT_ENV_KEY).unwrap_or(Default::default()) == "true" {
      result.set("security.protocol", "plaintext");
    }
    result
  }
}

#[async_trait]
impl RecordStream for KafkaRecordStream {
  async fn produce(&self, record: &str) -> Result<(), RecordStreamError> {
    let producer = self.producer.as_ref().expect("Kafka producer not enabled");
    let record: FutureRecord<str, str> = FutureRecord::to(&self.topic)
      .payload(record);
    let send_result = producer.send(
      record,
      Duration::from_secs(12)
    ).await;
    match send_result {
      Ok(_) => Ok(()),
      Err((e, _)) => Err(RecordStreamError::from(format!("Send error: {}", e)))
    }
  }

  async fn consume(&self) -> Result<String, RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    match consumer.recv().await {
      Err(e) => Err(RecordStreamError::from(format!("Recv error: {}", e))),
      Ok(msg) => {
        let payload = match msg.payload_view::<str>() {
          None => "",
          Some(Ok(s)) => s,
          Some(Err(e)) => {
            return Err(RecordStreamError::from(format!("Deserialize error: {}", e)));
          }
        };
        trace!("recv partition = {} offset = {}", msg.partition(), msg.offset());
        let mut tpl = self.tpl.lock().await;
        if let Err(e) = tpl
          .add_partition_offset(msg.topic(), msg.partition(), Offset::Offset(msg.offset() + 1)) {
          return Err(RecordStreamError::from(format!("Offset store error: {}", e)));
        }
        Ok(payload.to_string())
      }
    }
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let consumer = self.consumer.as_ref().expect("Kafka consumer not enabled");
    let tpl = self.tpl.lock().await;
    trace!("committing = {:?}", tpl);
    match consumer.commit(&tpl, CommitMode::Async) {
      Ok(_) => Ok(()),
      Err(e) => Err(RecordStreamError::from(format!("Commit error: {}", e)))
    }
  }
}
