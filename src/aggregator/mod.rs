mod pending;
mod report;

use crate::epoch::{get_current_epoch, is_epoch_expired};
use crate::models::{create_db_pool, DBPool, PendingMessage, PgStoreError, RecoveredMessage};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::AppSTARError;
use derive_more::{Display, Error, From};
use nested_sta_rs::errors::NestedSTARError;
use tokio::task::JoinError;
use std::sync::Arc;
use pending::process_pending_msgs;
use report::report_measurements;
use std::env;
use std::str::FromStr;

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

async fn process_expired_epochs(
  db_pool: Arc<DBPool>,
  out_stream: Option<&RecordStream>,
) -> Result<(), AggregatorError> {
  let current_epoch = get_current_epoch();
  let epochs = RecoveredMessage::list_distinct_epochs(db_pool.clone()).await?;
  for epoch in epochs {
    if !is_epoch_expired(epoch as u8, current_epoch) {
      continue;
    }
    info!("Detected expired epoch '{}', processing...", epoch);
    report_measurements(db_pool.clone(), Some(epoch), out_stream).await?;
    RecoveredMessage::delete_epoch(db_pool.clone(), epoch).await?;
    PendingMessage::delete_epoch(db_pool.clone(), epoch).await?;
  }
  Ok(())
}

pub async fn start_aggregation(
  out_stream: Option<RecordStream>,
) -> Result<(), AggregatorError> {
  let k_threshold = usize::from_str(&env::var(K_THRESHOLD_ENV_KEY).unwrap_or(K_THRESHOLD_DEFAULT.to_string()))
    .expect(format!("{} must be a positive integer", K_THRESHOLD_ENV_KEY).as_str());

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
  report_measurements(db_pool.clone(), None, out_stream.as_ref()).await?;
  // Phase 4: Check for expired epochs. Send off partial measurements.
  //          Delete pending/recovered messages from DB.
  process_expired_epochs(db_pool.clone(), out_stream.as_ref()).await?;

  info!("Finished aggregation");
  Ok(())
}
