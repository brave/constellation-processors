use crate::buffer::{RecordBuffer, RecordBufferError};
use crate::lake::{DataLake, LakeListing, DataLakeError};
use crate::models::MsgInfo;
use crate::star::{parse_message, serialize_message, AppSTARError};
use futures::stream::StreamExt;
use std::env;
use std::str::{self, FromStr, Utf8Error};
use tokio::io;
use derive_more::{Error, From, Display};
use nested_sta_rs::api::{NestedMessage, key_recover, recover};
use sta_rs::Message;
use nested_sta_rs::errors::NestedSTARError;

pub struct Aggregator {
  record_buffer: RecordBuffer,
  k_threshold: usize
}

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Aggregator error: {}")]
pub enum AggregatorError {
  AppSTAR(AppSTARError),
  NestedSTAR(NestedSTARError),
  DataLake(DataLakeError),
  IO(io::Error),
  Utf8(Utf8Error),
  RecordBuffer(RecordBufferError),
}

const K_THRESHOLD_ENV_KEY: &str = "K_THRESHOLD";
const K_THRESHOLD_DEFAULT: usize = 100;
const SHOULD_DOWNLOAD_FILE_THRESHOLD: usize = 800;
const SHOULD_DOWNLOAD_SIZE_THRESHOLD: usize = 300000;
const RECOVERY_BATCH_SIZE: usize = 10000;

impl Aggregator {

  pub fn new() -> Self {
    let k_threshold = env::var(K_THRESHOLD_ENV_KEY)
      .map(|v| usize::from_str(&v))
      .unwrap_or(Ok(K_THRESHOLD_DEFAULT))
      .expect("K_THRESHOLD should be a positive number");
    Self {
      record_buffer: RecordBuffer::new(),
      k_threshold
    }
  }

  fn should_process_tag_in_lake(listing: &LakeListing) -> bool {
    if listing.files.len() >= SHOULD_DOWNLOAD_FILE_THRESHOLD {
      return true;
    }
    let total_size = listing.files.iter().fold(0, |total, v| total + v.size);
    total_size >= SHOULD_DOWNLOAD_SIZE_THRESHOLD
  }

  async fn recover_messages(&mut self, msg_info: &MsgInfo, messages: &[String],
    existing_key: Option<&[u8]>) -> Result<Option<Vec<u8>>, AggregatorError> {

    let mut nested_messages = messages.iter()
      .map(|v| parse_message(&v))
      .collect::<Result<Vec<NestedMessage>, AppSTARError>>()?;

    let recovered_key = if existing_key.is_none() {
      let key_recover_msg_size = self.k_threshold.min(nested_messages.len());
      let key_recover_messages: Vec<&Message> = nested_messages[..key_recover_msg_size]
        .iter()
        .map(|v| &v.unencrypted_layer)
        .collect();
      Some(key_recover(&key_recover_messages, msg_info.epoch_tag)?)
    } else {
      None
    };
    let key = recovered_key.as_ref().map(|v| v.as_slice()).unwrap_or(existing_key.unwrap());

    let messages: Vec<&Message> = nested_messages.iter()
      .map(|v| &v.unencrypted_layer)
      .collect();
    let measurements = recover(&messages, key)?;
    let mtext = str::from_utf8(measurements[0].measurement.0[0].as_slice())?;

    for msg in nested_messages.iter_mut() {
      msg.decrypt_next_layer(key);
      let mut child_msg_info = msg_info.clone();
      child_msg_info.msg_tags.push(msg.unencrypted_layer.tag.clone());
      let msg_str = serialize_message(msg.clone())?;
      self.record_buffer.send_to_buffer(&child_msg_info, &msg_str).await?;
    }

    Ok(recovered_key)
  }

  pub async fn aggregate(&mut self) -> Result<(), AggregatorError> {
    let lake = DataLake::new();
    info!("Starting aggregation...");

    let root_listing = lake.list_partitioned_files(None).next().await.unwrap()?;
    for epoch_prefix in root_listing.prefixes {
      info!("Processing epoch key: {}", epoch_prefix);
      let mut epoch_listing_stream = lake.list_partitioned_files(Some(epoch_prefix));

      while let Some(epoch_listing) = epoch_listing_stream.next().await {
        let epoch_listing = epoch_listing?;

        for tag_prefix in epoch_listing.prefixes {
          info!("Processing tag key: {}", tag_prefix);
          let mut messages_to_recover = Vec::new();
          let mut recovered_key: Option<Vec<u8>> = None;
          let mut size_checked = false;
          let mut tag_listing_stream = lake.list_partitioned_files(Some(tag_prefix.clone()));
          let msg_info = MsgInfo::from(tag_prefix.as_str());
          while let Some(tag_listing) = tag_listing_stream.next().await {
            let tag_listing = tag_listing?;
            if !size_checked {
              size_checked = true;
              if !Self::should_process_tag_in_lake(&tag_listing) {
                return Ok(());
              }
            }
            for file in tag_listing.files {
              if !file.key.ends_with(".b64l") {
                continue;
              }
              let contents = lake.download_file_to_string(&file.key, true).await?;
              let file_messages = contents
                .split('\n')
                .filter_map(|v| match v.is_empty() {
                  true => None,
                  false => Some(v.to_string())
                });
              messages_to_recover.extend(file_messages);

              if messages_to_recover.len() >= RECOVERY_BATCH_SIZE {
                let rec_res = self.recover_messages(&msg_info,
                  &messages_to_recover, recovered_key.as_ref().map(|v| v.as_slice())).await?;
                if rec_res.is_some() {
                  recovered_key = rec_res;
                }
                messages_to_recover.clear();
              }
            }
          }
          if !messages_to_recover.is_empty() {
            self.recover_messages(&msg_info,
              &messages_to_recover, recovered_key.as_ref().map(|v| v.as_slice())).await?;
          }
          self.record_buffer.bake_everything();
        }
      }
    }

    info!("Finished aggregation");
    Ok(())
  }
}
