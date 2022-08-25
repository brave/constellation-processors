use crate::lake::{DataLake, DataLakeError};
use crate::prometheus::DataLakeMetrics;
use crate::record_stream::{RecordStream, RecordStreamError};
use derive_more::{Display, Error, From};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const BATCH_SIZE_ENV_KEY: &str = "LAKE_SINK_BATCH_SIZE";
const BATCH_SIZE_DEFAULT: &str = "1000";
const BATCH_TIMEOUT_SECS: u64 = 45;

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
  metrics: &DataLakeMetrics,
) -> Result<(), LakeSinkError> {
  let contents = batch.join("\n");
  lake.store(&contents).await?;

  rec_stream.commit_last_consume().await?;

  metrics.records_flushed(batch.len());
  debug!("Saved batch to lake, committed");
  Ok(())
}

pub async fn start_lakesink(
  metrics: Arc<DataLakeMetrics>,
  cancel_token: CancellationToken,
  output_measurements_to_stdout: bool,
) -> Result<(), LakeSinkError> {
  let batch_size =
    usize::from_str(&env::var(BATCH_SIZE_ENV_KEY).unwrap_or(BATCH_SIZE_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", BATCH_SIZE_ENV_KEY));

  let rec_stream = RecordStream::new(false, true, true);

  let lake = if output_measurements_to_stdout {
    None
  } else {
    Some(DataLake::new())
  };
  let mut batch = Vec::with_capacity(batch_size);
  let batch_timeout = Duration::from_secs(BATCH_TIMEOUT_SECS);
  loop {
    tokio::select! {
      record_res = rec_stream.consume() => {
        let record = record_res?;
        metrics.record_received();
        match lake.as_ref() {
          Some(lake) => {
            batch.push(record);
            if batch.len() >= batch_size {
              store_batch(lake, &rec_stream, &batch, &metrics).await?;
              batch.clear();
            }
          },
          None => {
            println!("{}", record);
            rec_stream.commit_last_consume().await?;
          }
        };
      },
      _ = sleep(batch_timeout) => {
        if let Some(lake) = lake.as_ref() {
          if !batch.is_empty() {
            store_batch(lake, &rec_stream, &batch, &metrics).await?;
            batch.clear();
          }
        }
      },
      _ = cancel_token.cancelled() => {
        info!("Ending lakesink task...");
        if let Some(lake) = lake.as_ref() {
          if !batch.is_empty() {
            store_batch(lake, &rec_stream, &batch, &metrics).await?;
          }
        }
        break;
      }
    }
  }
  Ok(())
}
