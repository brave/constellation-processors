use super::AggregatorError;
use crate::models::{
  BatchDelete, BatchInsert, DBPool, NewPendingMessage, NewRecoveredMessage, PendingMessage,
  RecoverablePendingTag, RecoveredMessage,
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
  tag_info: &mut RecoverablePendingTag,
  pending_msgs: Vec<PendingMessage>,
  nested_msgs: Vec<NestedMessage>,
  k_threshold: usize,
) -> Result<bool, AggregatorError> {
  let recovered_key = match recover_key(&nested_msgs, tag_info.epoch_tag as u8, k_threshold) {
    Err(e) => {
      debug!(
        "Could not recover key for tag {}: {:?}",
        hex::encode(&tag_info.msg_tag),
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
        hex::encode(&tag_info.msg_tag),
        e
      );
      return Ok(false);
    }
    Ok(r) => r,
  };

  let new_rec_msg = NewRecoveredMessage {
    msg_tag: tag_info.msg_tag.clone(),
    epoch_tag: tag_info.epoch_tag,
    metric_name: msg_rec.measurement.0,
    metric_value: msg_rec.measurement.1,
    parent_recovered_msg_id: pending_msgs[0].parent_recovered_msg_id,
    count: pending_msgs.len() as i64,
    key: recovered_key.clone(),
    has_children: msg_rec.next_layer_messages.is_some(),
  };

  let rec_msg_id = new_rec_msg.insert(db_pool.clone()).await?;

  tag_info.recovered_msg_id = Some(rec_msg_id);
  tag_info.key = Some(recovered_key);
  tag_info.recovered_count = Some(pending_msgs.len() as i64);

  if let Some(next_layer_messages) = msg_rec.next_layer_messages {
    save_next_layer_messages(
      db_pool.clone(),
      next_layer_messages,
      tag_info.epoch_tag,
      rec_msg_id,
    )
    .await?;
  }

  pending_msgs.delete_batch(db_pool.clone()).await?;
  Ok(true)
}

async fn process_recovered_tag(
  db_pool: &Arc<DBPool>,
  tag_info: &mut RecoverablePendingTag,
  pending_msgs: Vec<PendingMessage>,
  nested_msgs: Vec<NestedMessage>,
) -> Result<bool, AggregatorError> {
  let msg_rec = match recover_msgs(nested_msgs, tag_info.key.as_ref().unwrap()) {
    Err(e) => {
      debug!(
        "Could not recover messages for tag {}: {:?}",
        hex::encode(&tag_info.msg_tag),
        e
      );
      return Ok(false);
    }
    Ok(r) => r,
  };

  let new_count = tag_info.recovered_count.unwrap() + (pending_msgs.len() as i64);
  tag_info.recovered_count = Some(new_count);
  RecoveredMessage::update_count(db_pool.clone(), tag_info.recovered_msg_id.unwrap(), new_count)
    .await?;

  if let Some(next_layer_messages) = msg_rec.next_layer_messages {
    save_next_layer_messages(
      db_pool.clone(),
      next_layer_messages,
      tag_info.epoch_tag,
      tag_info.recovered_msg_id.unwrap(),
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
  last_pending_msg_id: i64
) -> Result<(Vec<PendingMessage>, Vec<NestedMessage>), AggregatorError> {
  let pending_msgs = PendingMessage::list(
    db_pool.clone(), epoch_tag, msg_tag, last_pending_msg_id
  ).await?;

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
    let pending_tags = if check_new {
      RecoverablePendingTag::count_new(db_pool.clone(), k_threshold).await?
    } else {
      RecoverablePendingTag::count_recovered(db_pool.clone()).await?
    };

    if pending_tags.is_empty() {
      break;
    }

    let tasks: Vec<_> = pending_tags
      .chunks(max(pending_tags.len() / WORKER_COUNT, 1))
      .into_iter()
      .map(|tag_infos| {
        let db_pool = db_pool.clone();
        let mut tag_infos = tag_infos.to_vec();
        let chunk_len = tag_infos.len();
        tokio::spawn(async move {
          let mut has_processed = false;
          for (i, tag_info) in tag_infos.iter_mut().enumerate() {
            let mut last_pending_msg_id = 0;
            // Loop through the "pages" of pending messages (max 10000 each page)
            loop {
              let (pending_msgs, nested_msgs) = get_pending_and_nested_msgs(
                &db_pool, tag_info.epoch_tag, tag_info.msg_tag.clone(), last_pending_msg_id
              ).await.unwrap();

              if pending_msgs.is_empty() {
                break;
              }

              let is_first_page = last_pending_msg_id == 0;
              last_pending_msg_id = pending_msgs.last().unwrap().id;

              let process_res = if check_new && is_first_page {
                process_new_tag(&db_pool, tag_info, pending_msgs, nested_msgs, k_threshold).await
              } else {
                process_recovered_tag(&db_pool, tag_info, pending_msgs, nested_msgs).await
              }
              .unwrap();

              if process_res {
                has_processed = true;
              }
              if i > 0 && i % 20 == 0 {
                info!("Processed {}/{} tags in chunk", i + 1, chunk_len);
              }
            }
            debug!("Processed tag {}", hex::encode(&tag_info.msg_tag));
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
