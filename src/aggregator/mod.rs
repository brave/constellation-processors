mod report;
mod group;
mod consume;
mod recovered;

use crate::epoch::{get_current_epoch, is_epoch_expired};
use crate::models::{create_db_pool, DBPool, PendingMessage, PgStoreError, RecoveredMessage};
use crate::record_stream::{RecordStream, RecordStreamError};
use crate::star::{recover_key, recover_msgs, parse_message_bincode, AppSTARError, MsgRecoveryInfo};
use derive_more::{Display, Error, From};
use nested_sta_rs::errors::NestedSTARError;
use tokio::task::{JoinError, JoinHandle};
use std::sync::Arc;
use group::GroupedMessages;
use consume::consume_and_group;
use report::report_measurements;
use recovered::RecoveredMessages;
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
    let mut rec_msgs = RecoveredMessages::default();
    rec_msgs.fetch_all_recovered_with_nonzero_count(db_pool.clone(), epoch as u8).await?;

    report_measurements(&mut rec_msgs, epoch as u8, true, out_stream).await?;
    RecoveredMessage::delete_epoch(db_pool.clone(), epoch).await?;
    PendingMessage::delete_epoch(db_pool.clone(), epoch).await?;
  }
  Ok(())
}

fn process_one_layer(
  grouped_msgs: &mut GroupedMessages,
  rec_msgs: &mut RecoveredMessages,
  k_threshold: usize
) -> Result<(GroupedMessages, Vec<(u8, Vec<u8>)>, bool), AggregatorError> {
  let mut next_grouped_msgs = GroupedMessages::default();
  let mut pending_tags_to_remove = Vec::new();
  let mut has_processed = false;

  for (epoch, epoch_map) in &mut grouped_msgs.msg_chunks {
    for (msg_tag, chunk) in epoch_map {
      if chunk.new_msgs.len() + chunk.pending_msgs.len() < k_threshold {
        continue;
      }
      let mut msgs = Vec::new();
      msgs.append(&mut chunk.new_msgs);
      for pending_msg in chunk.pending_msgs.drain(..) {
        msgs.push(parse_message_bincode(&pending_msg.message)?);
      }

      let existing_rec_msg = rec_msgs.get_mut(*epoch, msg_tag);
      let key = if let Some(rec_msg) = existing_rec_msg.as_ref() {
        rec_msg.key.clone()
      } else {
        recover_key(&msgs, *epoch, k_threshold)?
      };

      let msgs_len = msgs.len() as i64;

      let MsgRecoveryInfo { measurement, next_layer_messages } = recover_msgs(msgs, &key)?;

      if let Some(rec_msg) = existing_rec_msg {
        rec_msg.count += msgs_len;
      } else {
        rec_msgs.add(RecoveredMessage {
          id: 0,
          msg_tag: msg_tag.clone(),
          epoch_tag: *epoch as i16,
          metric_name: measurement.0,
          metric_value: measurement.1,
          parent_recovered_msg_tag: chunk.parent_msg_tag.clone(),
          count: msgs_len,
          key,
          has_children: next_layer_messages.is_some()
        });
      }

      if let Some(child_msgs) = next_layer_messages {
        for msg in child_msgs {
          next_grouped_msgs.add(msg, Some(msg_tag));
        }
      }

      pending_tags_to_remove.push((*epoch, msg_tag.clone()));
      has_processed = true;
    }
  }

  Ok((next_grouped_msgs, pending_tags_to_remove, has_processed))
}

pub fn start_subtask(
  id: usize,
  db_pool: Arc<DBPool>,
  out_stream: Option<Arc<RecordStream>>,
  mut grouped_msgs: GroupedMessages,
  k_threshold: usize
) -> JoinHandle<i64> {
  tokio::spawn(async move {
    let mut pending_tags_to_remove = Vec::new();

    let mut rec_msgs = RecoveredMessages::default();

    let mut it_count = 1;
    loop {
      info!("Task {}: processing layer of messages (round {})", id, it_count);
      // Fetch recovered message info (which includes key) for collected tags, if available
      debug!("Task {}: Fetching recovered messages", id);
      grouped_msgs.fetch_recovered(db_pool.clone(), &mut rec_msgs).await.unwrap();

      // Fetch pending messages for collected tags, if available
      debug!("Task {}: Fetching pending messages", id);
      grouped_msgs.fetch_pending(db_pool.clone()).await.unwrap();

      debug!("Task {}: Starting actual processing", id);
      let (new_grouped_msgs, pending_tags_to_remove_chunk, has_processed) =
        process_one_layer(&mut grouped_msgs, &mut rec_msgs, k_threshold).unwrap();

      pending_tags_to_remove.extend(pending_tags_to_remove_chunk);

      debug!("Task {}: Storing new pending messages", id);
      grouped_msgs.store_new_pending_msgs(db_pool.clone()).await.unwrap();

      if !has_processed {
        break;
      }

      it_count += 1;
      grouped_msgs = new_grouped_msgs;
    }

    info!("Task {}: Deleting old pending messages", id);
    for (epoch, msg_tag) in pending_tags_to_remove {
      PendingMessage::delete_tag(db_pool.clone(), epoch as i16, msg_tag).await.unwrap();
    }

    // Check for full recovered measurements, send off measurements to Kafka to be
    // stored in data lake/warehouse
    info!("Task {}: Reporting final measurements", id);
    let rec_epochs: Vec<u8> = rec_msgs.map.keys().cloned().collect();
    let mut measurements_count = 0;
    for epoch in rec_epochs {
      measurements_count += report_measurements(
        &mut rec_msgs, epoch, false, out_stream.as_ref().map(|v| v.as_ref())
      ).await.unwrap();
    }

    info!("Task {}: Saving recovered messages", id);
    rec_msgs.save(db_pool.clone()).await.unwrap();
    measurements_count
  })
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
