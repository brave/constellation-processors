mod consume;
mod group;
mod processing;
mod recovered;
mod report;

use crate::epoch::get_current_epoch;
use crate::models::{
  begin_db_transaction, commit_db_transaction, DBPool, DBStorageConnections, PgStoreError,
};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::{KafkaRecordStream, RecordStream, RecordStreamArc, RecordStreamError};
use crate::star::AppSTARError;
use consume::consume_and_group;
use derive_more::{Display, Error, From};
use futures::future::try_join_all;
use nested_sta_rs::errors::NestedSTARError;
use processing::{process_expired_epochs, start_subtask};
use std::env;
use std::str::{FromStr, Utf8Error};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinError;
use tokio::time::sleep;

pub const K_THRESHOLD_ENV_KEY: &str = "K_THRESHOLD";
pub const K_THRESHOLD_DEFAULT: &str = "100";
const COOLOFF_SECONDS: u64 = 15;

const CONSUMER_COUNT: usize = 4;

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Aggregator error: {}")]
pub enum AggregatorError {
  Utf8(Utf8Error),
  AppSTAR(AppSTARError),
  NestedSTAR(NestedSTARError),
  Database(PgStoreError),
  RecordStream(RecordStreamError),
  Join(JoinError),
  JSONSerialize(serde_json::Error),
}

fn create_output_stream(
  output_measurements_to_stdout: bool,
) -> Result<Option<RecordStreamArc>, AggregatorError> {
  Ok(if output_measurements_to_stdout {
    None
  } else {
    let out_stream = Arc::new(KafkaRecordStream::new(true, false, true));
    out_stream.init_producer_transactions()?;
    Some(out_stream)
  })
}

async fn wait_and_commit_producer(out_stream: &RecordStreamArc) -> Result<(), AggregatorError> {
  info!("Waiting for Kafka producer queues to finish...");
  out_stream.join_produce_queues().await?;
  info!("Committing Kafka output transaction");
  out_stream.commit_producer_transaction()?;
  Ok(())
}

pub async fn start_aggregation(
  worker_count: usize,
  msg_collect_count: usize,
  iterations: usize,
  output_measurements_to_stdout: bool,
  test_epoch: Option<u8>,
) -> Result<(), AggregatorError> {
  let current_epoch = if let Some(epoch) = test_epoch {
    epoch
  } else {
    get_current_epoch().await
  };
  info!("Current epoch is {}", current_epoch);

  let k_threshold =
    usize::from_str(&env::var(K_THRESHOLD_ENV_KEY).unwrap_or(K_THRESHOLD_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", K_THRESHOLD_ENV_KEY));

  let db_pool = Arc::new(DBPool::new(false));

  info!("Starting aggregation...");

  for i in 0..iterations {
    let profiler = Arc::new(Profiler::default());

    info!("Starting iteration {}", i);

    let out_stream = create_output_stream(output_measurements_to_stdout)?;

    let mut in_streams: Vec<RecordStreamArc> = Vec::new();
    for _ in 0..CONSUMER_COUNT {
      in_streams.push(Arc::new(KafkaRecordStream::new(false, true, false)));
    }

    info!("Consuming messages from stream");
    let download_start_instant = Instant::now();
    // Consume & group as much data from Kafka as possible
    let (grouped_msgs, count) = consume_and_group(&in_streams, msg_collect_count).await?;

    if count == 0 {
      info!("No messages consumed");
      break;
    }
    info!("Consumed {} messages", count);
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
        k_threshold,
        profiler.clone(),
      ));
    }

    let measurement_counts = try_join_all(tasks).await?;

    let total_measurement_count = measurement_counts.iter().sum::<i64>();

    if let Some(out_stream) = out_stream.as_ref() {
      wait_and_commit_producer(out_stream).await?;
    }

    info!("Committing DB transactions");
    store_conns.commit()?;

    // Commit consumption to Kafka cluster, to mark messages as "already read"
    info!("Committing Kafka consumption");
    for in_stream in in_streams {
      in_stream.commit_last_consume().await.unwrap();
    }

    profiler
      .record_total_time(ProfilerStat::TotalProcessingTime, processing_start_instant)
      .await;

    info!("Reported {} final measurements", total_measurement_count);

    info!("Profiler summary:\n{}", profiler.summary().await);

    info!("Cooling off for {} seconds", COOLOFF_SECONDS);
    sleep(Duration::from_secs(COOLOFF_SECONDS)).await;
  }

  // Check for expired epochs. Send off partial measurements.
  // Delete pending/recovered messages from DB.
  info!("Checking/processing expired epochs");
  let profiler = Arc::new(Profiler::default());
  let mut out_stream = create_output_stream(output_measurements_to_stdout)?;
  if let Some(out_stream) = out_stream.as_mut() {
    out_stream.begin_producer_transaction()?;
  }
  let db_conn = Arc::new(Mutex::new(db_pool.get().await?));
  begin_db_transaction(db_conn.clone())?;
  process_expired_epochs(
    db_conn.clone(),
    current_epoch,
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
