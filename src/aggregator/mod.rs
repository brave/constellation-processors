mod consume;
mod group;
mod processing;
mod recovered;
mod report;

use crate::epoch::get_current_epoch;
use crate::models::{create_db_pool, PgStoreError};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::AppSTARError;
use consume::consume_and_group;
use derive_more::{Display, Error, From};
use futures::future::try_join_all;
use nested_sta_rs::errors::NestedSTARError;
use processing::{process_expired_epochs, start_subtask};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinError;

const K_THRESHOLD_ENV_KEY: &str = "K_THRESHOLD";
const K_THRESHOLD_DEFAULT: &str = "100";

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Aggregator error: {}")]
pub enum AggregatorError {
  AppSTAR(AppSTARError),
  NestedSTAR(NestedSTARError),
  Database(PgStoreError),
  RecordStream(RecordStreamError),
  Join(JoinError),
  JSONSerialize(serde_json::Error),
}

pub async fn start_aggregation(
  worker_count: usize,
  msg_collect_count: usize,
  iterations: usize,
  output_measurements_to_stdout: bool,
) -> Result<(), AggregatorError> {
  let current_epoch = get_current_epoch().await;
  info!("Current epoch is {}", current_epoch);

  let k_threshold =
    usize::from_str(&env::var(K_THRESHOLD_ENV_KEY).unwrap_or(K_THRESHOLD_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", K_THRESHOLD_ENV_KEY));

  let out_stream = if output_measurements_to_stdout {
    None
  } else {
    Some(Arc::new(RecordStream::new(true, false, true)))
  };

  let db_pool = Arc::new(create_db_pool());

  info!("Starting aggregation...");

  for i in 0..iterations {
    info!("Starting iteration {}", i);
    info!("Consuming messages from stream");
    // Consume & group as much data from Kafka as possible
    let (grouped_msgs, rec_stream, count) = consume_and_group(msg_collect_count).await?;

    if count == 0 {
      info!("No messages consumed");
      break;
    }
    info!("Consumed {} messages", count);

    // Split message tags/grouped messages into multiple chunks
    // Process each one in a separate task/thread
    let tasks: Vec<_> = grouped_msgs
      .split(worker_count)
      .into_iter()
      .enumerate()
      .map(|(i, v)| start_subtask(i, db_pool.clone(), out_stream.clone(), v, k_threshold))
      .collect();

    let measurement_counts = try_join_all(tasks).await?;

    let total_measurement_count = measurement_counts.iter().sum::<i64>();
    info!("Reported {} final measurements", total_measurement_count);

    // Check for expired epochs. Send off partial measurements.
    // Delete pending/recovered messages from DB.
    info!("Checking/processing expired epochs");
    process_expired_epochs(
      db_pool.clone(),
      current_epoch,
      out_stream.as_ref().map(|v| v.as_ref()),
    )
    .await?;

    // Commit consumption to Kafka cluster, to mark messages as "already read"
    info!("Committing Kafka consumption");
    rec_stream.commit_last_consume().await?;
  }

  info!("Finished aggregation");
  Ok(())
}
