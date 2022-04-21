use crate::models::{DBPool, BatchInsert, NewPendingMessage, PgStoreError};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::{parse_message, AppSTARError};
use derive_more::{Display, Error, From};
use std::sync::Arc;

const BATCH_SIZE: usize = 250;

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
  let mut batch = Vec::with_capacity(BATCH_SIZE);
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

    if batch.len() >= BATCH_SIZE {
      batch.insert_batch(db_pool.clone()).await?;
      rec_stream.commit_last_consume().await?;
      debug!("Inserted batch in DB, committed");
      batch = Vec::with_capacity(BATCH_SIZE);
    }
  }
}
