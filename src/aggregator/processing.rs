use super::group::{GroupedMessages, MessageChunk};
use super::recovered::RecoveredMessages;
use super::report::report_measurements;
use super::AggregatorError;
use crate::aggregator::spot::check_spot_termination_status;
use crate::aggregator::wait_and_commit_producer;
use crate::epoch::EpochConfig;
use crate::models::{
  begin_db_transaction, commit_db_transaction, DBConnection, DBPool, DBStorageConnections,
  MessageWithThreshold, PendingMessage, RecoveredMessage,
};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::{DynRecordStream, RecordStreamArc};
use crate::star::{recover_key, recover_msgs, AppSTARError, MsgRecoveryInfo};
use crate::util::parse_env_var;
use star_constellation::api::NestedMessage;
use star_constellation::Error as ConstellationError;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;

const TOTAL_RECOVERY_FAIL_MAX_MSGS_ENV: &str = "TOTAL_RECOVERY_FAIL_MAX_MSGS";
const TOTAL_RECOVERY_FAIL_MAX_MSGS_DEFAULT: &str = "75";

pub async fn process_expired_epoch(
  conn: Arc<Mutex<DBConnection>>,
  epoch_config: &EpochConfig,
  out_stream: Option<&DynRecordStream>,
  profiler: Arc<Profiler>,
  epoch: i16,
) -> Result<(), AggregatorError> {
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
  PendingMessage::delete_epoch(conn, epoch, profiler).await?;
  Ok(())
}

pub async fn process_expired_epochs(
  conn: Arc<Mutex<DBConnection>>,
  epoch_config: &EpochConfig,
  out_stream: Option<RecordStreamArc>,
  profiler: Arc<Profiler>,
  channel_name: &str,
) -> Result<(), AggregatorError> {
  let epochs = RecoveredMessage::list_distinct_epochs(conn.clone()).await?;
  for epoch in epochs {
    if !epoch_config.is_epoch_expired(epoch as u8) {
      continue;
    }
    info!("Detected expired epoch '{}', processing...", epoch);
    if let Some(out_stream) = out_stream.as_ref() {
      out_stream.init_producer_queues().await;
      out_stream.begin_producer_transaction()?;
    }
    begin_db_transaction(conn.clone())?;

    tokio::select! {
      res = process_expired_epoch(conn.clone(), epoch_config, out_stream.as_ref().map(|v| v.as_ref()), profiler.clone(), epoch) => {
        res?
      },
      termination_res = check_spot_termination_status(true, channel_name) => {
        return Err(termination_res.unwrap_err());
      }
    };

    if let Some(out_stream) = out_stream.as_ref() {
      wait_and_commit_producer(out_stream, channel_name).await?;
    }
    commit_db_transaction(conn.clone())?;
  }
  Ok(())
}

fn drain_chunk_messages_for_threshold(
  chunk: &mut MessageChunk,
  threshold: usize,
) -> Result<Vec<NestedMessage>, AggregatorError> {
  let mut msgs = Vec::new();
  if let Some(new_msgs) = chunk.new_msgs.get_mut(&threshold) {
    msgs.append(new_msgs);
  }
  if let Some(pending_msgs) = chunk.pending_msgs.get_mut(&threshold) {
    for pending_msg in pending_msgs.drain(..) {
      msgs.push(pending_msg.try_into()?);
    }
  }
  Ok(msgs)
}

/// Returns None if key recovery process failed, which indicates
/// that the chunk messages should be stored for a future
/// recovery attempt. Returns Some with a tuple containing the key,
/// and the messages drained from the chunk that were used for recovery, if any.
fn get_recovery_key(
  epoch: u8,
  chunk: &mut MessageChunk,
  recovery_threshold: Option<usize>,
  existing_rec_msg: Option<&&mut RecoveredMessage>,
) -> Result<Option<(Vec<u8>, Option<Vec<NestedMessage>>)>, AggregatorError> {
  let mut key_recovery_msgs: Option<Vec<_>> = None;

  // if a recovered msg exists, use the key that was already recovered.
  // otherwise, recover the key
  let key = if let Some(rec_msg) = existing_rec_msg {
    rec_msg.key.clone()
  } else {
    let threshold = recovery_threshold.unwrap();
    let new_msg_count = chunk
      .new_msgs
      .get(&threshold)
      .map(|nm| nm.len())
      .unwrap_or_default();

    // drain messages required for recovery into the vec
    let mut msgs = drain_chunk_messages_for_threshold(chunk, threshold)?;

    let key = match recover_key(&msgs, epoch, threshold) {
      Err(e) => {
        match e {
          AppSTARError::Recovery(ConstellationError::ShareRecovery) => {
            // Store new messages until we receive more shares in the future.
            for msg in msgs.drain(..new_msg_count) {
              chunk.new_msgs.get_mut(&threshold).unwrap().push(msg);
            }
            return Ok(None);
          }
          _ => return Err(e.into()),
        };
      }
      Ok(key) => key,
    };
    // cache the messages used for key recovery, so they can be used
    // for measurement recovery
    key_recovery_msgs = Some(msgs);
    key
  };
  Ok(Some((key, key_recovery_msgs)))
}

fn process_one_layer(
  grouped_msgs: &mut GroupedMessages,
  rec_msgs: &mut RecoveredMessages,
  id: usize,
) -> Result<(GroupedMessages, Vec<(u8, Vec<u8>)>, usize, bool), AggregatorError> {
  let mut next_grouped_msgs = GroupedMessages::default();
  let mut pending_tags_to_remove = Vec::new();
  let mut total_error_count = 0;
  let mut has_processed = false;

  for (epoch, epoch_map) in &mut grouped_msgs.msg_chunks {
    for (msg_tag, chunk) in epoch_map {
      let existing_rec_msg = rec_msgs.get_mut(*epoch, msg_tag);

      let recovery_threshold = chunk.recoverable_threshold();

      // if we don't have a key for this tag, check to see if it meets the k threshold
      // if not, skip it
      if existing_rec_msg.is_none() && recovery_threshold.is_none() {
        continue;
      }

      let has_pending_msgs = chunk.pending_msgs.values().any(|v| !v.is_empty());

      let (key, mut key_recovery_msgs) =
        match get_recovery_key(*epoch, chunk, recovery_threshold, existing_rec_msg.as_ref())? {
          Some(res) => res,
          None => {
            // key recovery failed. stop processing for the current message chunk/tag,
            // save messages in db for later attempt
            continue;
          }
        };

      let mut msgs_len = 0i64;
      let mut metric_name: Option<String> = None;
      let mut metric_value: Option<String> = None;
      let mut has_children = false;

      let mut thresholds = HashSet::new();
      thresholds.extend(chunk.new_msgs.keys().chain(chunk.pending_msgs.keys()));

      // recover each k-threshold group separately so we store
      // new nested pending messages with the correct threshold value
      for threshold in thresholds {
        let msgs = if recovery_threshold == Some(threshold) && key_recovery_msgs.is_some() {
          // messages for this threshold were already drained in the key
          // recovery step, so use this existing vec
          key_recovery_msgs.take().unwrap()
        } else {
          drain_chunk_messages_for_threshold(chunk, threshold)?
        };

        if msgs.is_empty() {
          continue;
        }
        let threshold_msgs_len = msgs.len();

        let MsgRecoveryInfo {
          measurement,
          next_layer_messages,
          error_count,
        } = match recover_msgs(msgs, &key) {
          Ok(info) => info,
          Err(e) => {
            debug!(
              "failed to recover {threshold_msgs_len} messages for threshold {threshold} on id {id} for tag {} and parent tag {}: {e}",
              hex::encode(msg_tag),
              hex::encode(chunk.parent_msg_tag.clone().unwrap_or_default()),
            );
            if threshold_msgs_len
              <= parse_env_var::<usize>(
                TOTAL_RECOVERY_FAIL_MAX_MSGS_ENV,
                TOTAL_RECOVERY_FAIL_MAX_MSGS_DEFAULT,
              )
            {
              total_error_count += threshold_msgs_len;
              continue;
            }
            return Err(e.into());
          }
        };

        msgs_len += threshold_msgs_len as i64;
        metric_name = Some(measurement.0);
        metric_value = Some(measurement.1);

        total_error_count += error_count;
        if next_layer_messages.is_some() {
          has_children = true;
        }

        // save messages in the next layer in a new GroupedMessages struct
        if let Some(child_msgs) = next_layer_messages {
          for msg in child_msgs {
            next_grouped_msgs.add(MessageWithThreshold { msg, threshold }, Some(msg_tag));
          }
        }
      }

      if msgs_len == 0 {
        continue;
      }

      // create or update recovered msg with new count
      if let Some(rec_msg) = existing_rec_msg {
        rec_msg.count += msgs_len;
      } else {
        rec_msgs.add(RecoveredMessage {
          id: 0,
          msg_tag: msg_tag.clone(),
          epoch_tag: *epoch as i16,
          metric_name: metric_name.unwrap(),
          metric_value: metric_value.unwrap(),
          parent_recovered_msg_tag: chunk.parent_msg_tag.clone(),
          count: msgs_len,
          key: key.to_vec(),
          has_children,
        });
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
        process_one_layer(&mut grouped_msgs, &mut rec_msgs, id).unwrap();
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
