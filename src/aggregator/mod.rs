mod consume;
mod group;
mod processing;
mod recovered;
mod report;
mod spot;

use crate::aggregator::spot::check_spot_termination_status;
use crate::epoch::EpochConfig;
use crate::models::{
  begin_db_transaction, commit_db_transaction, DBConnectionType, DBPool, DBStorageConnections,
  PgStoreError,
};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::{
  get_data_channel_topic_from_env, KafkaRecordStream, KafkaRecordStreamConfig, RecordStream,
  RecordStreamArc, RecordStreamError,
};
use crate::star::AppSTARError;
use crate::util::parse_env_var;
use consume::consume_and_group;
use derive_more::{Display, Error, From};
use futures::future::try_join_all;
use processing::{process_expired_epochs, start_subtask};
use star_constellation::Error as ConstellationError;
use std::str::Utf8Error;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinError;

pub const DEFAULT_K_THRESHOLD_ENV_KEY: &str = "K_THRESHOLD";
pub const DEFAULT_K_THRESHOLD_DEFAULT: &str = "50";
pub const MIN_MSGS_TO_PROCESS_ENV_KEY: &str = "MIN_MSGS_TO_PROCESS";
pub const MIN_MSGS_TO_PROCESS_DEFAULT: &str = "1000";

const CONSUMER_COUNT: usize = 4;

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Aggregator error: {}")]
pub enum AggregatorError {
  Utf8(Utf8Error),
  AppSTAR(AppSTARError),
  Constellation(ConstellationError),
  Database(PgStoreError),
  RecordStream(RecordStreamError),
  Join(JoinError),
  JSONSerialize(serde_json::Error),
  ThresholdTooBig,
  SpotTermination,
  IMDSRequestFail,
}

fn create_output_stream(
  output_measurements_to_stdout: bool,
  channel_name: &str,
) -> Result<Option<RecordStreamArc>, AggregatorError> {
  let topic = get_data_channel_topic_from_env(true, channel_name);
  Ok(if output_measurements_to_stdout {
    None
  } else {
    let out_stream = Arc::new(KafkaRecordStream::new(KafkaRecordStreamConfig {
      enable_producer: true,
      enable_consumer: false,
      topic,
      use_output_group_id: true,
    }));
    out_stream.init_producer_transactions()?;
    Some(out_stream)
  })
}

async fn wait_and_commit_producer(out_stream: &RecordStreamArc) -> Result<(), AggregatorError> {
  info!("Waiting for Kafka producer queues to finish...");
  out_stream.join_produce_queues().await?;

  check_spot_termination_status(false).await?;

  info!("Committing Kafka output transaction");
  out_stream.commit_producer_transaction()?;
  Ok(())
}

pub async fn start_aggregation(
  channel_name: &str,
  worker_count: usize,
  msg_collect_count: usize,
  iterations: usize,
  output_measurements_to_stdout: bool,
  epoch_config: Arc<EpochConfig>,
) -> Result<(), AggregatorError> {
  info!("Current epoch is {}", epoch_config.current_epoch.epoch);

  let default_k_threshold =
    parse_env_var::<usize>(DEFAULT_K_THRESHOLD_ENV_KEY, DEFAULT_K_THRESHOLD_DEFAULT);
  let min_msgs_to_process =
    parse_env_var::<usize>(MIN_MSGS_TO_PROCESS_ENV_KEY, MIN_MSGS_TO_PROCESS_DEFAULT);

  let db_pool = Arc::new(DBPool::new(DBConnectionType::Normal { channel_name }));

  info!("Starting aggregation...");

  let out_stream = create_output_stream(output_measurements_to_stdout, channel_name)?;

  let mut in_streams: Vec<RecordStreamArc> = Vec::new();
  let in_stream_topic = get_data_channel_topic_from_env(false, channel_name);
  for _ in 0..CONSUMER_COUNT {
    in_streams.push(Arc::new(KafkaRecordStream::new(KafkaRecordStreamConfig {
      enable_producer: false,
      enable_consumer: true,
      topic: in_stream_topic.clone(),
      use_output_group_id: false,
    })));
  }

  for i in 0..iterations {
    let profiler = Arc::new(Profiler::default());

    info!("Starting iteration {}", i);

    if let Some(out_stream) = out_stream.as_ref() {
      out_stream.init_producer_queues().await;
    }

    info!("Consuming messages from stream");
    let download_start_instant = Instant::now();
    // Consume & group as much data from Kafka as possible
    let (grouped_msgs, count) =
      consume_and_group(&in_streams, msg_collect_count, default_k_threshold).await?;

    if count == 0 {
      info!("No messages consumed");
      break;
    }
    info!("Consumed {} messages", count);

    if count < min_msgs_to_process {
      info!("Message count too low, finished aggregation");
      break;
    }

    profiler
      .record_total_time(ProfilerStat::DownloadTime, download_start_instant)
      .await;

    if let Some(out_stream) = out_stream.as_ref() {
      out_stream.begin_producer_transaction()?;
    }

    // Split message tags/grouped messages into multiple chunks
    // Process each one in a separate task/thread
    let mut tasks = Vec::new();

    let processing_start_instant = Instant::now();

    let store_conns = Arc::new(DBStorageConnections::new(&db_pool, false).await?);

    let grouped_msgs_split = grouped_msgs.split(worker_count).into_iter().enumerate();
    for (id, grouped_msgs) in grouped_msgs_split {
      tasks.push(start_subtask(
        id,
        store_conns.clone(),
        db_pool.clone(),
        out_stream.clone(),
        grouped_msgs,
        epoch_config.clone(),
        profiler.clone(),
      ));
    }

    let measurement_counts = tokio::select! {
      measurement_counts_res = try_join_all(tasks) => {
        measurement_counts_res?
      },
      termination_res = check_spot_termination_status(true) => {
        return Err(termination_res.unwrap_err());
      }
    };

    let total_measurement_count = measurement_counts.iter().map(|(c, _)| c).sum::<i64>();
    let total_error_count = measurement_counts.iter().map(|(_, e)| e).sum::<usize>();

    if let Some(out_stream) = out_stream.as_ref() {
      wait_and_commit_producer(out_stream).await?;
    }

    info!("Committing DB transactions");
    store_conns.commit()?;

    // Commit consumption to Kafka cluster, to mark messages as "already read"
    info!("Committing Kafka consumption");
    for in_stream in &in_streams {
      in_stream.commit_last_consume().await.unwrap();
    }

    profiler
      .record_total_time(ProfilerStat::TotalProcessingTime, processing_start_instant)
      .await;

    info!("Reported {} final measurements", total_measurement_count);
    if total_error_count > 0 {
      error!(
        "Failed to recover {} measurements due to bincode deserialization errors",
        total_error_count
      );
    }

    info!("Profiler summary:\n{}", profiler.summary().await);
  }

  // Check for expired epochs. Send off partial measurements.
  // Delete pending/recovered messages from DB.
  info!("Checking/processing expired epochs");
  let profiler = Arc::new(Profiler::default());
  let mut out_stream = create_output_stream(output_measurements_to_stdout, channel_name)?;
  if let Some(out_stream) = out_stream.as_mut() {
    out_stream.init_producer_queues().await;
    out_stream.begin_producer_transaction()?;
  }
  let db_conn = Arc::new(Mutex::new(db_pool.get().await?));
  begin_db_transaction(db_conn.clone())?;
  process_expired_epochs(
    db_conn.clone(),
    &epoch_config,
    out_stream.as_ref().map(|v| v.as_ref()),
    profiler.clone(),
  )
  .await?;
  if let Some(out_stream) = out_stream.as_ref() {
    wait_and_commit_producer(out_stream).await?;
  }
  commit_db_transaction(db_conn)?;
  info!("Profiler summary:\n{}", profiler.summary().await);

  info!("Finished aggregation");
  Ok(())
}
