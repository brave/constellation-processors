use super::group::GroupedMessages;
use super::recovered::RecoveredMessages;
use super::report::report_measurements;
use super::{AggregatorError, MessageWithThreshold};
use crate::epoch::EpochConfig;
use crate::models::{DBConnection, DBPool, DBStorageConnections, PendingMessage, RecoveredMessage};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::{DynRecordStream, RecordStreamArc};
use crate::star::{recover_key, recover_msgs, AppSTARError, MsgRecoveryInfo};
use star_constellation::Error as ConstellationError;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;

pub async fn process_expired_epochs(
  conn: Arc<Mutex<DBConnection>>,
  epoch_config: &EpochConfig,
  out_stream: Option<&DynRecordStream>,
  profiler: Arc<Profiler>,
) -> Result<(), AggregatorError> {
  let epochs = RecoveredMessage::list_distinct_epochs(conn.clone()).await?;
  for epoch in epochs {
    if !epoch_config.is_epoch_expired(epoch as u8) {
      continue;
    }
    info!("Detected expired epoch '{}', processing...", epoch);
    let mut rec_msgs = RecoveredMessages::default();
    rec_msgs
      .fetch_all_recovered_with_nonzero_count(conn.clone(), epoch as u8, profiler.clone())
      .await?;

    report_measurements(
      &mut rec_msgs,
      epoch_config,
      epoch as u8,
      true,
      out_stream,
      profiler.clone(),
    )
    .await?;
    RecoveredMessage::delete_epoch(conn.clone(), epoch, profiler.clone()).await?;
    PendingMessage::delete_epoch(conn.clone(), epoch, profiler.clone()).await?;
  }
  Ok(())
}

fn process_one_layer(
  grouped_msgs: &mut GroupedMessages,
  rec_msgs: &mut RecoveredMessages,
) -> Result<(GroupedMessages, Vec<(u8, Vec<u8>)>, usize, bool), AggregatorError> {
  let mut next_grouped_msgs = GroupedMessages::default();
  let mut pending_tags_to_remove = Vec::new();
  let mut total_error_count = 0;
  let mut has_processed = false;

  for (epoch, epoch_map) in &mut grouped_msgs.msg_chunks {
    for (msg_tag, chunk) in epoch_map {
      let existing_rec_msg = rec_msgs.get_mut(*epoch, msg_tag);

      let threshold = chunk
        .most_common_threshold()
        .expect("should be able to find common threshold among messages");

      // if we don't have a key for this tag, check to see if it meets the k threshold
      // if not, skip it
      if existing_rec_msg.is_none() && chunk.new_msgs.len() + chunk.pending_msgs.len() < threshold {
        continue;
      }
      let new_msg_count = chunk.new_msgs.len();
      let has_pending_msgs = !chunk.pending_msgs.is_empty();

      // concat new messages from kafka, and pending messages from PG into one vec
      // "mwt" = message with threshold
      let mut mwts = Vec::new();
      mwts.append(&mut chunk.new_msgs);
      for pending_msg in chunk.pending_msgs.drain(..) {
        mwts.push(pending_msg.try_into()?);
      }

      // if a recovered msg exists, use the key that was already recovered.
      // otherwise, recover the key
      let key = if let Some(rec_msg) = existing_rec_msg.as_ref() {
        rec_msg.key.clone()
      } else {
        match recover_key(
          mwts.iter().map(|mwt| &mwt.msg),
          mwts.len(),
          *epoch,
          threshold,
        ) {
          Err(e) => {
            match e {
              AppSTARError::Recovery(ConstellationError::ShareRecovery) => {
                // Store new messages until we receive more shares in the future.
                for msg in mwts.drain(..new_msg_count) {
                  chunk.new_msgs.push(msg);
                }
                continue;
              }
              _ => return Err(e.into()),
            };
          }
          Ok(key) => key,
        }
      };

      let mwts_len = mwts.len() as i64;

      let MsgRecoveryInfo {
        measurement,
        next_layer_messages,
        error_count,
      } = recover_msgs(mwts.into_iter().map(|mwt| mwt.msg).collect(), &key)?;
      total_error_count += error_count;

      // create or update recovered msg with new count
      if let Some(rec_msg) = existing_rec_msg {
        rec_msg.count += mwts_len;
      } else {
        rec_msgs.add(RecoveredMessage {
          id: 0,
          msg_tag: msg_tag.clone(),
          epoch_tag: *epoch as i16,
          metric_name: measurement.0,
          metric_value: measurement.1,
          parent_recovered_msg_tag: chunk.parent_msg_tag.clone(),
          count: mwts_len,
          key: key.to_vec(),
          has_children: next_layer_messages.is_some(),
        });
      }

      // save messages in the next layer in a new GroupedMessages struct
      if let Some(child_msgs) = next_layer_messages {
        for msg in child_msgs {
          next_grouped_msgs.add(MessageWithThreshold { msg, threshold }, Some(msg_tag));
        }
      }

      if has_pending_msgs {
        pending_tags_to_remove.push((*epoch, msg_tag.clone()));
      }
      has_processed = true;
    }
  }

  Ok((
    next_grouped_msgs,
    pending_tags_to_remove,
    total_error_count,
    has_processed,
  ))
}

pub fn start_subtask(
  id: usize,
  store_conns: Arc<DBStorageConnections>,
  db_pool: Arc<DBPool>,
  out_stream: Option<RecordStreamArc>,
  mut grouped_msgs: GroupedMessages,
  epoch_config: Arc<EpochConfig>,
  profiler: Arc<Profiler>,
) -> JoinHandle<(i64, usize)> {
  tokio::spawn(async move {
    let mut pending_tags_to_remove = Vec::new();

    let mut rec_msgs = RecoveredMessages::default();

    let processing_start_instant = Instant::now();
    let mut error_count = 0;

    let mut it_count = 1;
    loop {
      info!(
        "Task {}: processing layer of messages (round {})",
        id, it_count
      );
      // Fetch recovered message info (which includes key) for collected tags, if available
      debug!("Task {}: Fetching recovered messages", id);
      grouped_msgs
        .fetch_recovered(db_pool.clone(), &mut rec_msgs, profiler.clone())
        .await
        .unwrap();

      // Fetch pending messages for collected tags, if available
      debug!("Task {}: Fetching pending messages", id);
      grouped_msgs
        .fetch_pending(db_pool.clone(), &mut rec_msgs, profiler.clone())
        .await
        .unwrap();

      let tag_count = grouped_msgs
        .msg_chunks
        .values()
        .map(|c| c.len())
        .sum::<usize>();
      debug!(
        "Task {}: Starting actual processing (tag count = {})",
        id, tag_count
      );
      let (new_grouped_msgs, pending_tags_to_remove_chunk, layer_error_count, has_processed) =
        process_one_layer(&mut grouped_msgs, &mut rec_msgs).unwrap();
      error_count += layer_error_count;

      pending_tags_to_remove.extend(pending_tags_to_remove_chunk);

      debug!("Task {}: Storing new pending messages", id);
      grouped_msgs
        .store_new_pending_msgs(&store_conns, profiler.clone())
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
      PendingMessage::delete_tag(store_conns.get(), epoch as i16, msg_tag, profiler.clone())
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
        epoch_config.as_ref(),
        epoch,
        false,
        out_stream.as_ref().map(|v| v.as_ref()),
        profiler.clone(),
      )
      .await
      .unwrap();
    }

    info!("Task {}: Saving recovered messages", id);
    rec_msgs.save(&store_conns, profiler.clone()).await.unwrap();

    profiler
      .record_range_time(ProfilerStat::TaskProcessingTime, processing_start_instant)
      .await;

    (measurements_count, error_count)
  })
}
