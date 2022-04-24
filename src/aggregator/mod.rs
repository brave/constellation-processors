mod report;
mod group;
mod consume;
mod recovered;
mod processing;

use crate::models::{create_db_pool, PgStoreError};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::AppSTARError;
use derive_more::{Display, Error, From};
use nested_sta_rs::errors::NestedSTARError;
use tokio::task::JoinError;
use std::sync::Arc;
use consume::consume_and_group;
use processing::{process_expired_epochs, start_subtask};
use std::env;
use std::str::FromStr;
use futures::future::try_join_all;

const K_THRESHOLD_ENV_KEY: &str = "K_THRESHOLD";
const K_THRESHOLD_DEFAULT: &str = "100";
const TASK_COUNT: usize = 16;

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
  out_stream: Option<Arc<RecordStream>>,
) -> Result<(), AggregatorError> {
  let k_threshold = usize::from_str(&env::var(K_THRESHOLD_ENV_KEY).unwrap_or(K_THRESHOLD_DEFAULT.to_string()))
    .expect(format!("{} must be a positive integer", K_THRESHOLD_ENV_KEY).as_str());

  let db_pool = Arc::new(create_db_pool());

  info!("Starting aggregation...");

  info!("Consuming messages from stream");
  // Consume & group as much data from Kafka as possible
  let (grouped_msgs, rec_stream, count) = consume_and_group().await?;

  if count == 0 {
    info!("No messages consumed, exiting");
    return Ok(());
  }
  info!("Consumed {} messages", count);

  let tasks: Vec<_> = grouped_msgs.split(TASK_COUNT).into_iter().enumerate().map(|(i, v)| {
    start_subtask(i, db_pool.clone(), out_stream.clone(), v, k_threshold)
  }).collect();

  let measurement_counts = try_join_all(tasks).await?;

  let total_measurement_count = measurement_counts.iter().fold(0, |acc, x| acc + x);
  info!("Reported {} final measurements", total_measurement_count);

  // Check for expired epochs. Send off partial measurements.
  // Delete pending/recovered messages from DB.
  info!("Checking/processing expired epochs");
  process_expired_epochs(db_pool.clone(), out_stream.as_ref().map(|v| v.as_ref())).await?;

  // Commit consumption to Kafka cluster, to mark messages as "already read"
  info!("Committing Kafka consumption");
  rec_stream.commit_last_consume().await?;

  info!("Finished aggregation");
  Ok(())
}
