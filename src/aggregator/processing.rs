use super::group::GroupedMessages;
use super::recovered::RecoveredMessages;
use super::report::report_measurements;
use super::AggregatorError;
use crate::epoch::is_epoch_expired;
use crate::models::{DBConnection, DBPool, PendingMessage, RecoveredMessage};
use crate::record_stream::{DynRecordStream, RecordStreamArc};
use crate::star::{
  parse_message_bincode, recover_key, recover_msgs, AppSTARError, MsgRecoveryInfo,
};
use nested_sta_rs::errors::NestedSTARError;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub async fn process_expired_epochs(
  conn: Arc<Mutex<DBConnection>>,
  current_epoch: u8,
  out_stream: Option<&DynRecordStream>,
) -> Result<(), AggregatorError> {
  let epochs = RecoveredMessage::list_distinct_epochs(conn.clone()).await?;
  for epoch in epochs {
    if !is_epoch_expired(epoch as u8, current_epoch) {
      continue;
    }
    info!("Detected expired epoch '{}', processing...", epoch);
    let mut rec_msgs = RecoveredMessages::default();
    rec_msgs
      .fetch_all_recovered_with_nonzero_count(conn.clone(), epoch as u8)
      .await?;

    report_measurements(&mut rec_msgs, epoch as u8, true, out_stream).await?;
    RecoveredMessage::delete_epoch(conn.clone(), epoch).await?;
    PendingMessage::delete_epoch(conn.clone(), epoch).await?;
  }
  Ok(())
}

fn process_one_layer(
  grouped_msgs: &mut GroupedMessages,
  rec_msgs: &mut RecoveredMessages,
  k_threshold: usize,
) -> Result<(GroupedMessages, Vec<(u8, Vec<u8>)>, bool), AggregatorError> {
  let mut next_grouped_msgs = GroupedMessages::default();
  let mut pending_tags_to_remove = Vec::new();
  let mut has_processed = false;

  for (epoch, epoch_map) in &mut grouped_msgs.msg_chunks {
    for (msg_tag, chunk) in epoch_map {
      let existing_rec_msg = rec_msgs.get_mut(*epoch, msg_tag);
      // if we don't have a key for this tag, check to see if it meets the k threshold
      // if not, skip it
      if existing_rec_msg.is_none() && chunk.new_msgs.len() + chunk.pending_msgs.len() < k_threshold
      {
        continue;
      }
      let new_msg_count = chunk.new_msgs.len();

      // concat new messages from kafka, and pending messages from PG into one vec
      let mut msgs = Vec::new();
      msgs.append(&mut chunk.new_msgs);
      for pending_msg in chunk.pending_msgs.drain(..) {
        msgs.push(parse_message_bincode(&pending_msg.message)?);
      }

      // if a recovered msg exists, use the key that was already recovered.
      // otherwise, recover the key
      let key = if let Some(rec_msg) = existing_rec_msg.as_ref() {
        rec_msg.key.clone()
      } else {
        match recover_key(&msgs, *epoch, k_threshold) {
          Err(e) => {
            match e {
              AppSTARError::Recovery(NestedSTARError::ShareRecoveryFailedError) => {
                // Store new messages until we receive more shares in the future
                for msg in msgs.drain(..new_msg_count) {
                  chunk.new_msgs.push(msg);
                }
                continue;
              }
              _ => return Err(AggregatorError::AppSTAR(e)),
            };
          }
          Ok(key) => key,
        }
      };

      let msgs_len = msgs.len() as i64;

      let MsgRecoveryInfo {
        measurement,
        next_layer_messages,
      } = recover_msgs(msgs, &key)?;

      // create or update recovered msg with new count
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
          has_children: next_layer_messages.is_some(),
        });
      }

      // save messages in the next layer in a new GroupedMessages struct
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
  conn: Arc<Mutex<DBConnection>>,
  db_pool: Arc<DBPool>,
  out_stream: Option<RecordStreamArc>,
  mut grouped_msgs: GroupedMessages,
  k_threshold: usize,
) -> JoinHandle<i64> {
  tokio::spawn(async move {
    let mut pending_tags_to_remove = Vec::new();

    let mut rec_msgs = RecoveredMessages::default();

    let mut it_count = 1;
    loop {
      info!(
        "Task {}: processing layer of messages (round {})",
        id, it_count
      );
      // Fetch recovered message info (which includes key) for collected tags, if available
      debug!("Task {}: Fetching recovered messages", id);
      grouped_msgs
        .fetch_recovered(conn.clone(), &mut rec_msgs)
        .await
        .unwrap();

      // Fetch pending messages for collected tags, if available
      debug!("Task {}: Fetching pending messages", id);
      grouped_msgs.fetch_pending(db_pool.clone()).await.unwrap();

      debug!("Task {}: Starting actual processing", id);
      let (new_grouped_msgs, pending_tags_to_remove_chunk, has_processed) =
        process_one_layer(&mut grouped_msgs, &mut rec_msgs, k_threshold).unwrap();

      pending_tags_to_remove.extend(pending_tags_to_remove_chunk);

      debug!("Task {}: Storing new pending messages", id);
      grouped_msgs
        .store_new_pending_msgs(conn.clone())
        .await
        .unwrap();

      if !has_processed {
        break;
      }

      it_count += 1;
      grouped_msgs = new_grouped_msgs;
    }

    info!("Task {}: Deleting old pending messages", id);
    for (epoch, msg_tag) in pending_tags_to_remove {
      PendingMessage::delete_tag(conn.clone(), epoch as i16, msg_tag)
        .await
        .unwrap();
    }

    // Check for full recovered measurements, send off measurements to Kafka to be
    // stored in data lake/warehouse
    info!("Task {}: Reporting final measurements", id);
    let rec_epochs: Vec<u8> = rec_msgs.map.keys().cloned().collect();
    let mut measurements_count = 0;
    for epoch in rec_epochs {
      measurements_count += report_measurements(
        &mut rec_msgs,
        epoch,
        false,
        out_stream.as_ref().map(|v| v.as_ref()),
      )
      .await
      .unwrap();
    }

    info!("Task {}: Saving recovered messages", id);
    rec_msgs.save(conn).await.unwrap();
    measurements_count
  })
}
