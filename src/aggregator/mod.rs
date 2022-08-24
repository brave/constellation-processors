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
use diesel::connection::{Connection, TransactionManager};
use futures::future::try_join_all;
use nested_sta_rs::errors::NestedSTARError;
use processing::{process_expired_epochs, start_subtask};
use std::env;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
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

  let mut out_stream = if output_measurements_to_stdout {
    None
  } else {
    Some(Arc::new(RecordStream::new(true, false, true)))
  };
  let in_stream = RecordStream::new(false, true, false);

  let db_pool = Arc::new(create_db_pool());

  info!("Starting aggregation...");

  for i in 0..iterations {
    info!("Starting iteration {}", i);

    info!("Consuming messages from stream");
    // Consume & group as much data from Kafka as possible
    let (grouped_msgs, count) = consume_and_group(&in_stream, msg_collect_count).await?;

    if count == 0 {
      info!("No messages consumed");
      break;
    }
    info!("Consumed {} messages", count);

    if let Some(out_stream) = out_stream.as_mut() {
      out_stream.init_producer_transactions()?;
      out_stream.begin_producer_transaction()?;
    }

    // Split message tags/grouped messages into multiple chunks
    // Process each one in a separate task/thread
    let mut tasks = Vec::new();
    let mut db_connections = Vec::new();
    let grouped_msgs_split = grouped_msgs.split(worker_count).into_iter().enumerate();
    for (id, grouped_msgs) in grouped_msgs_split {
      let conn = Arc::new(Mutex::new(
        db_pool.get().map_err(|e| PgStoreError::from(e))?,
      ));
      let conn_lock = conn.lock().unwrap();
      conn_lock
        .transaction_manager()
        .begin_transaction(conn_lock.deref())
        .map_err(|e| PgStoreError::from(e))?;
      drop(conn_lock);

      db_connections.push(conn.clone());
      tasks.push(start_subtask(
        id,
        conn,
        db_pool.clone(),
        out_stream.clone(),
        grouped_msgs,
        k_threshold,
      ));
    }

    let measurement_counts = try_join_all(tasks).await?;

    let total_measurement_count = measurement_counts.iter().sum::<i64>();

    for conn in db_connections {
      info!("Committing DB transactions");
      let conn = conn.lock().unwrap();
      conn
        .transaction_manager()
        .commit_transaction(conn.deref())
        .map_err(|e| PgStoreError::from(e))?;
    }
    info!("Reported {} final measurements", total_measurement_count);

    if let Some(out_stream) = out_stream.as_ref() {
      info!("Committing Kafka output transaction");
      out_stream.commit_producer_transaction()?;
    }

    // Commit consumption to Kafka cluster, to mark messages as "already read"
    info!("Committing Kafka consumption");
    in_stream.commit_last_consume().await?;
  }

  // Check for expired epochs. Send off partial measurements.
  // Delete pending/recovered messages from DB.
  info!("Checking/processing expired epochs");
  process_expired_epochs(
    Arc::new(Mutex::new(
      db_pool.get().map_err(|e| PgStoreError::from(e))?,
    )),
    current_epoch,
    out_stream.as_ref().map(|v| v.as_ref()),
  )
  .await?;

  info!("Finished aggregation");
  Ok(())
}
