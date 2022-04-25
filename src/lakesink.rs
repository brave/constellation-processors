use crate::lake::{DataLake, DataLakeError};
use crate::record_stream::{RecordStream, RecordStreamError};
use derive_more::{Display, Error, From};
use std::env;
use std::str::FromStr;
use tokio_util::sync::CancellationToken;

const BATCH_SIZE_ENV_KEY: &str = "LAKE_SINK_BATCH_SIZE";
const BATCH_SIZE_DEFAULT: &str = "1000";

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Lake sink error: {}")]
pub enum LakeSinkError {
  RecordStream(RecordStreamError),
  Lake(DataLakeError),
}

async fn store_batch(
  lake: &DataLake,
  rec_stream: &RecordStream,
  batch: &[String],
) -> Result<(), LakeSinkError> {
  let contents = batch.join("\n");
  lake.store(&contents).await?;
  rec_stream.commit_last_consume().await?;
  debug!("Saved batch to lake, committed");
  Ok(())
}

pub async fn start_lakesink(
  rec_stream: RecordStream,
  cancel_token: CancellationToken,
) -> Result<(), LakeSinkError> {
  let batch_size =
    usize::from_str(&env::var(BATCH_SIZE_ENV_KEY).unwrap_or(BATCH_SIZE_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", BATCH_SIZE_ENV_KEY));

  let lake = DataLake::new();
  let mut batch = Vec::with_capacity(batch_size);
  loop {
    tokio::select! {
      record_res = rec_stream.consume() => {
        let record = record_res?;
        batch.push(record);
        if batch.len() >= batch_size {
          store_batch(&lake, &rec_stream, &batch).await?;
          batch.clear();
        }
      },
      _ = cancel_token.cancelled() => {
        info!("Ending lakesink task...");
        if !batch.is_empty() {
          store_batch(&lake, &rec_stream, &batch).await?;
        }
        break;
      }
    }
  }
  Ok(())
}
