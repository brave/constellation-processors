use crate::models::{DBPool, BatchInsert, NewPendingMessage, PgStoreError};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::{parse_message, AppSTARError};
use derive_more::{Display, Error, From};
use std::env;
use std::sync::Arc;
use std::str::FromStr;

const BATCH_SIZE_ENV_KEY: &str = "DB_SINK_BATCH_SIZE";
const BATCH_SIZE_DEFAULT: &str = "10000";

#[derive(Error, From, Display, Debug)]
#[display(fmt = "DB sink error: {}")]
pub enum DBSinkError {
  STAR(AppSTARError),
  RecordStream(RecordStreamError),
  Database(PgStoreError),
}

pub async fn start_dbsink(
  rec_stream: RecordStream,
  db_pool: Arc<DBPool>
) -> Result<(), DBSinkError> {
  let batch_size = usize::from_str(
    &env::var(BATCH_SIZE_ENV_KEY).unwrap_or(BATCH_SIZE_DEFAULT.to_string())
  ).expect(format!("{} must be a positive integer", BATCH_SIZE_ENV_KEY).as_str());

  let mut batch = Vec::with_capacity(batch_size);
  loop {
    let record = rec_stream.consume().await?;
    match parse_message(&record) {
      Err(e) => debug!("failed to parse message: {}", e),
      Ok(data) => {
        batch.push(NewPendingMessage {
          msg_tag: data.msg.unencrypted_layer.tag.clone(),
          epoch_tag: data.msg.epoch as i16,
          parent_recovered_msg_id: None,
          message: data.bincode_msg,
        });
      }
    };

    if batch.len() >= batch_size {
      batch.insert_batch(db_pool.clone()).await?;
      rec_stream.commit_last_consume().await?;
      debug!("Inserted batch in DB, committed");
      batch = Vec::with_capacity(batch_size);
    }
  }
}
