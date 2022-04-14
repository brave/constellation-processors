mod pending;
mod report;

use crate::models::{create_db_pool, PgStoreError};
use crate::star::AppSTARError;
use derive_more::{Display, Error, From};
use nested_sta_rs::errors::NestedSTARError;
use pending::process_pending_msgs;
use report::report_measurements;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinError;

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Aggregator error: {}")]
pub enum AggregatorError {
  AppSTAR(AppSTARError),
  NestedSTAR(NestedSTARError),
  Database(PgStoreError),
  Join(JoinError),
  JSONSerialize(serde_json::Error),
}

pub async fn start_aggregation() -> Result<(), AggregatorError> {
  let k_threshold = usize::from_str(&env::var("K_THRESHOLD").unwrap_or("100".to_string()))
    .expect("K_THRESHOLD must be a positive integer");

  let db_pool = Arc::new(create_db_pool());

  info!("Starting aggregation...");

  // Phase 1: Check pending msgs that have existing keys.
  //          Recover their nested messages, increment recovered counts for relevant tags
  process_pending_msgs(db_pool.clone(), k_threshold, false).await?;
  // Phase 2: Check pending msgs that don't have existing keys.
  //          Recover the keys and the nested messages
  process_pending_msgs(db_pool.clone(), k_threshold, true).await?;
  // Phase 3: Check for full recovered measurements, send off measurements to Kafka to be
  //          stored in data lake/warehouse
  report_measurements(db_pool.clone(), false).await?;
  // Phase 4: Check for expired epochs. Send off partial measurements.
  //          Delete pending/recovered messages from DB.

  info!("Finished aggregation");
  Ok(())
}
