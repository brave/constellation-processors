use super::AggregatorError;
use crate::models::{
  BatchDelete, BatchInsert, DBPool, NewPendingMessage, NewRecoveredMessage, PendingMessage,
  PendingMessageCount, RecoveredMessage,
};
use crate::star::{
  parse_message_bincode, recover_key, recover_msgs, AppSTARError, SerializedMessageWithTag,
};
use futures::future::try_join_all;
use nested_sta_rs::api::NestedMessage;
use std::cmp::max;
use std::sync::Arc;

const WORKER_COUNT: usize = 256;

async fn save_next_layer_messages(
  db_pool: Arc<DBPool>,
  msgs: Vec<SerializedMessageWithTag>,
  epoch_tag: i16,
  parent_recovered_msg_id: i64,
) -> Result<(), AggregatorError> {
  let batch: Vec<_> = msgs
    .into_iter()
    .map(|v| NewPendingMessage {
      msg_tag: v.tag,
      epoch_tag,
      parent_recovered_msg_id: Some(parent_recovered_msg_id),
      message: v.message,
    })
    .collect();

  batch.insert_batch(db_pool).await?;
  Ok(())
}

async fn process_new_tag(
  db_pool: &Arc<DBPool>,
  count: &PendingMessageCount,
  pending_msgs: Vec<PendingMessage>,
  nested_msgs: Vec<NestedMessage>,
  k_threshold: usize,
) -> Result<bool, AggregatorError> {
  let recovered_key = match recover_key(&nested_msgs, count.epoch_tag as u8, k_threshold) {
    Err(e) => {
      debug!(
        "Could not recover key for tag {}: {:?}",
        hex::encode(&count.msg_tag),
        e
      );
      return Ok(false);
    }
    Ok(k) => k,
  };
  let msg_rec = match recover_msgs(nested_msgs, &recovered_key) {
    Err(e) => {
      debug!(
        "Could not recover messages for tag {}: {:?}",
        hex::encode(&count.msg_tag),
        e
      );
      return Ok(false);
    }
    Ok(r) => r,
  };

  let new_rec_msg = NewRecoveredMessage {
    msg_tag: count.msg_tag.clone(),
    epoch_tag: count.epoch_tag,
    metric_name: msg_rec.measurement.0,
    metric_value: msg_rec.measurement.1,
    parent_recovered_msg_id: pending_msgs[0].parent_recovered_msg_id,
    count: pending_msgs.len() as i64,
    key: recovered_key,
    has_children: msg_rec.next_layer_messages.is_some(),
  };

  let rec_msg_id = new_rec_msg.insert(db_pool.clone()).await?;

  if let Some(next_layer_messages) = msg_rec.next_layer_messages {
    save_next_layer_messages(
      db_pool.clone(),
      next_layer_messages,
      count.epoch_tag,
      rec_msg_id,
    )
    .await?;
  }

  pending_msgs.delete_batch(db_pool.clone()).await?;
  Ok(true)
}

async fn process_recovered_tag(
  db_pool: &Arc<DBPool>,
  count: &PendingMessageCount,
  pending_msgs: Vec<PendingMessage>,
  nested_msgs: Vec<NestedMessage>,
) -> Result<bool, AggregatorError> {
  let msg_rec = match recover_msgs(nested_msgs, count.key.as_ref().unwrap()) {
    Err(e) => {
      debug!(
        "Could not recover messages for tag {}: {:?}",
        hex::encode(&count.msg_tag),
        e
      );
      return Ok(false);
    }
    Ok(r) => r,
  };

  let new_count = count.recovered_count.unwrap() + (pending_msgs.len() as i64);
  RecoveredMessage::update_count(db_pool.clone(), count.recovered_msg_id.unwrap(), new_count)
    .await?;

  if let Some(next_layer_messages) = msg_rec.next_layer_messages {
    save_next_layer_messages(
      db_pool.clone(),
      next_layer_messages,
      count.epoch_tag,
      count.recovered_msg_id.unwrap(),
    )
    .await?;
  }

  pending_msgs.delete_batch(db_pool.clone()).await?;
  Ok(true)
}

async fn get_pending_and_nested_msgs(
  db_pool: &Arc<DBPool>,
  epoch_tag: i16,
  msg_tag: Vec<u8>,
) -> Result<(Vec<PendingMessage>, Vec<NestedMessage>), AggregatorError> {
  let pending_msgs = PendingMessage::list(db_pool.clone(), epoch_tag, msg_tag).await?;

  let nested_msgs = pending_msgs
    .iter()
    .map(|v| parse_message_bincode(&v.message))
    .collect::<Result<Vec<NestedMessage>, AppSTARError>>()?;

  Ok((pending_msgs, nested_msgs))
}

pub async fn process_pending_msgs(
  db_pool: Arc<DBPool>,
  k_threshold: usize,
  check_new: bool,
) -> Result<(), AggregatorError> {
  let mut it_count = 1;
  loop {
    info!(
      "Processing pending messages to attempt recovery ({} tags), round {}",
      if check_new { "new" } else { "recovered" },
      it_count
    );
    let pending_counts = if check_new {
      PendingMessageCount::count_new(db_pool.clone(), k_threshold).await?
    } else {
      PendingMessageCount::count_recovered(db_pool.clone()).await?
    };

    if pending_counts.is_empty() {
      break;
    }

    let tasks: Vec<_> = pending_counts
      .chunks(max(pending_counts.len() / WORKER_COUNT, 1))
      .into_iter()
      .map(|counts| {
        let db_pool = db_pool.clone();
        let counts = counts.to_vec();
        tokio::spawn(async move {
          let mut has_processed = false;
          for (i, count) in counts.iter().enumerate() {
            let (pending_msgs, nested_msgs) =
              get_pending_and_nested_msgs(&db_pool, count.epoch_tag, count.msg_tag.clone())
                .await
                .unwrap();

            let process_res = if check_new {
              process_new_tag(&db_pool, count, pending_msgs, nested_msgs, k_threshold).await
            } else {
              process_recovered_tag(&db_pool, count, pending_msgs, nested_msgs).await
            }
            .unwrap();

            if process_res {
              has_processed = true;
              debug!("Processed tag {}", hex::encode(&count.msg_tag));
            }
            if i > 0 && i % 20 == 0 {
              info!("Processed {}/{} tags in chunk", i + 1, counts.len());
            }
          }
          has_processed
        })
      })
      .collect();

    let task_results = try_join_all(tasks).await?;
    if !task_results.iter().any(|v| *v) {
      break;
    }

    it_count += 1;
  }
  Ok(())
}
