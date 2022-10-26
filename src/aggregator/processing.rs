use super::group::GroupedMessages;
use super::recovered::RecoveredMessages;
use super::report::report_measurements;
use super::AggregatorError;
use crate::epoch::is_epoch_expired;
use crate::models::{DBConnection, DBPool, DBStorageConnections, PendingMessage, RecoveredMessage};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::{DynRecordStream, RecordStreamArc};
use crate::star::{parse_message, recover_key, recover_msgs, AppSTARError, MsgRecoveryInfo};
use nested_sta_rs::api::NestedMessage;
use nested_sta_rs::errors::NestedSTARError;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

const LAYER_SUBTASK_COUNT: usize = 16;

#[derive(Debug)]
struct LayerSubtaskRequest {
  epoch: u8,
  tag: Vec<u8>,
  parent_msg_tag: Option<Vec<u8>>,
  has_pending_msgs: bool,
  existing_key: Option<Vec<u8>>,
  new_msg_count: usize,
  msgs: Vec<NestedMessage>,
}

#[derive(Debug, Default)]
struct LayerSubtaskResponse {
  epoch: u8,
  tag: Vec<u8>,
  parent_msg_tag: Option<Vec<u8>>,
  recovered_msg_count: Option<usize>,
  has_pending_msgs: bool,
  new_msgs_to_store: Option<Vec<NestedMessage>>,
  new_key: Option<Vec<u8>>,
  recovery_info: Option<MsgRecoveryInfo>,
}

pub async fn process_expired_epochs(
  conn: Arc<Mutex<DBConnection>>,
  current_epoch: u8,
  out_stream: Option<&DynRecordStream>,
  profiler: Arc<Profiler>,
) -> Result<(), AggregatorError> {
  let epochs = RecoveredMessage::list_distinct_epochs(conn.clone()).await?;
  for epoch in epochs {
    if !is_epoch_expired(epoch as u8, current_epoch) {
      continue;
    }
    info!("Detected expired epoch '{}', processing...", epoch);
    let mut rec_msgs = RecoveredMessages::default();
    rec_msgs
      .fetch_all_recovered_with_nonzero_count(conn.clone(), epoch as u8, profiler.clone())
      .await?;

    report_measurements(
      &mut rec_msgs,
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

fn layer_subtask(
  mut req_rx: UnboundedReceiver<LayerSubtaskRequest>,
  result_tx: UnboundedSender<LayerSubtaskResponse>,
  k_threshold: usize,
) -> JoinHandle<Result<(), AggregatorError>> {
  tokio::spawn(async move {
    while let Some(mut req) = req_rx.recv().await {
      let mut resp = LayerSubtaskResponse {
        epoch: req.epoch,
        tag: req.tag.clone(),
        parent_msg_tag: req.parent_msg_tag.clone(),
        has_pending_msgs: req.has_pending_msgs,
        ..Default::default()
      };
      // if a recovered msg exists, use the key that was already recovered.
      // otherwise, recover the key
      let key = if let Some(key) = req.existing_key.as_ref() {
        key
      } else {
        match recover_key(&req.msgs, req.epoch, k_threshold) {
          Err(e) => {
            match e {
              AppSTARError::Recovery(NestedSTARError::ShareRecoveryFailedError) => {
                // Store new messages until we receive more shares in the future
                resp.new_msgs_to_store = Some(req.msgs.drain(..req.new_msg_count).collect());
                result_tx.send(resp).unwrap();
                continue;
              }
              _ => return Err(AggregatorError::AppSTAR(e)),
            };
          }
          Ok(key) => {
            resp.new_key = Some(key);
            resp.new_key.as_ref().unwrap()
          }
        }
      };

      resp.recovered_msg_count = Some(req.msgs.len());
      resp.recovery_info = Some(recover_msgs(req.msgs, key)?);
      result_tx.send(resp).unwrap();
    }
    Ok(())
  })
}

async fn process_one_layer(
  grouped_msgs: &mut GroupedMessages,
  rec_msgs: &mut RecoveredMessages,
  k_threshold: usize,
) -> Result<(GroupedMessages, Vec<(u8, Vec<u8>)>, bool), AggregatorError> {
  let mut next_grouped_msgs = GroupedMessages::default();
  let mut pending_tags_to_remove = Vec::new();
  let mut has_processed = false;

  let (subtask_result_tx, mut subtask_result_rx) = unbounded_channel();
  let mut layer_subtask_index = 0;
  let layer_subtasks: Vec<_> = (0..LAYER_SUBTASK_COUNT)
    .map(|_| {
      let (subtask_req_tx, subtask_req_rx) = unbounded_channel();
      let handle = layer_subtask(subtask_req_rx, subtask_result_tx.clone(), k_threshold);
      (handle, subtask_req_tx)
    })
    .collect();
  drop(subtask_result_tx);

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
      let has_pending_msgs = !chunk.pending_msgs.is_empty();

      // concat new messages from kafka, and pending messages from PG into one vec
      let mut msgs = Vec::new();
      msgs.append(&mut chunk.new_msgs);
      for pending_msg in chunk.pending_msgs.drain(..) {
        msgs.push(parse_message(&pending_msg.message)?);
      }

      layer_subtasks[layer_subtask_index]
        .1
        .send(LayerSubtaskRequest {
          epoch: *epoch,
          tag: msg_tag.clone(),
          parent_msg_tag: chunk.parent_msg_tag.clone(),
          existing_key: existing_rec_msg.as_ref().map(|v| v.key.clone()),
          has_pending_msgs,
          new_msg_count,
          msgs,
        })
        .unwrap();
      layer_subtask_index = (layer_subtask_index + 1) % LAYER_SUBTASK_COUNT;
    }
  }
  let layer_subtask_handles: Vec<_> = layer_subtasks
    .into_iter()
    .map(|(handle, _)| handle)
    .collect();

  while let Some(resp) = subtask_result_rx.recv().await {
    if let Some(new_msgs) = resp.new_msgs_to_store {
      let group_chunk = grouped_msgs
        .msg_chunks
        .get_mut(&resp.epoch)
        .unwrap()
        .get_mut(&resp.tag)
        .unwrap();
      group_chunk.new_msgs.extend(new_msgs);
    } else {
      let existing_rec_msg = rec_msgs.get_mut(resp.epoch, &resp.tag);
      // create or update recovered msg with new count
      if let Some(rec_msg) = existing_rec_msg {
        rec_msg.count += resp.recovered_msg_count.unwrap() as i64;
      } else {
        rec_msgs.add(RecoveredMessage {
          id: 0,
          msg_tag: resp.tag.clone(),
          epoch_tag: resp.epoch as i16,
          metric_name: resp.recovery_info.as_ref().unwrap().measurement.0.clone(),
          metric_value: resp.recovery_info.as_ref().unwrap().measurement.1.clone(),
          parent_recovered_msg_tag: resp.parent_msg_tag.clone(),
          count: resp.recovered_msg_count.unwrap() as i64,
          key: resp.new_key.unwrap(),
          has_children: resp
            .recovery_info
            .as_ref()
            .unwrap()
            .next_layer_messages
            .is_some(),
        });
      }

      // save messages in the next layer in a new GroupedMessages struct
      if let Some(child_msgs) = resp.recovery_info.unwrap().next_layer_messages {
        for msg in child_msgs {
          next_grouped_msgs.add(msg, Some(&resp.tag));
        }
      }

      if resp.has_pending_msgs {
        pending_tags_to_remove.push((resp.epoch, resp.tag.clone()));
      }

      has_processed = true;
    }
  }

  for handle in layer_subtask_handles {
    handle.await??;
  }

  Ok((next_grouped_msgs, pending_tags_to_remove, has_processed))
}

pub fn process_task(
  id: usize,
  store_conns: Arc<DBStorageConnections>,
  db_pool: Arc<DBPool>,
  out_stream: Option<RecordStreamArc>,
  mut grouped_msgs: GroupedMessages,
  k_threshold: usize,
  profiler: Arc<Profiler>,
) -> JoinHandle<i64> {
  tokio::spawn(async move {
    let mut pending_tags_to_remove = Vec::new();

    let mut rec_msgs = RecoveredMessages::default();

    let processing_start_instant = Instant::now();

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

      debug!("Task {}: Starting actual processing", id);
      let (new_grouped_msgs, pending_tags_to_remove_chunk, has_processed) =
        process_one_layer(&mut grouped_msgs, &mut rec_msgs, k_threshold)
          .await
          .unwrap();

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

    measurements_count
  })
}
