use crate::lake::{DataLake, DataLakeError};
use crate::prometheus::DataLakeMetrics;
use crate::record_stream::{DynRecordStream, KafkaRecordStream, RecordStream, RecordStreamError};
use crate::util::parse_env_var;
use derive_more::{Display, Error, From};
use std::str::{from_utf8, Utf8Error};
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
  Utf8(Utf8Error),
  RecordStream(RecordStreamError),
  Lake(DataLakeError),
}

async fn store_batch(
  lake: &DataLake,
  rec_stream: &DynRecordStream,
  channel_name: &str,
  batch: &[Vec<u8>],
  metrics: &DataLakeMetrics,
) -> Result<(), LakeSinkError> {
  let json_lines: Vec<String> = batch
    .iter()
    .map(|v| from_utf8(v).map(|v| v.to_string()))
    .collect::<Result<Vec<String>, Utf8Error>>()?;
  let contents = json_lines.join("\n");
  lake.store(channel_name, &contents).await?;

  rec_stream.commit_last_consume().await?;

  metrics.records_flushed(batch.len());
  debug!("Saved batch to lake, committed");
  Ok(())
}

pub async fn start_lakesink(
  channel_name: String,
  stream_topic: String,
  metrics: Arc<DataLakeMetrics>,
  cancel_token: CancellationToken,
  output_measurements_to_stdout: bool,
) -> Result<(), LakeSinkError> {
  let batch_size = parse_env_var::<usize>(BATCH_SIZE_ENV_KEY, BATCH_SIZE_DEFAULT);

  let rec_stream = KafkaRecordStream::new(false, true, stream_topic, true);

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
              store_batch(lake, &rec_stream, &channel_name, &batch, &metrics).await?;
              batch.clear();
            }
          },
          None => {
            println!("{}", from_utf8(&record)?);
            rec_stream.commit_last_consume().await?;
          }
        };
      },
      _ = sleep(batch_timeout) => {
        if let Some(lake) = lake.as_ref() {
          if !batch.is_empty() {
            store_batch(lake, &rec_stream, &channel_name, &batch, &metrics).await?;
            batch.clear();
          }
        }
      },
      _ = cancel_token.cancelled() => {
        info!("Ending lakesink task...");
        if let Some(lake) = lake.as_ref() {
          if !batch.is_empty() {
            store_batch(lake, &rec_stream, &channel_name, &batch, &metrics).await?;
          }
        }
        break;
      }
    }
  }
  Ok(())
}
