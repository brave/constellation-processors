use super::recovered::RecoveredMessages;
use super::AggregatorError;
use crate::models::{BatchInsert, DBPool, NewPendingMessage, PendingMessage};
use crate::star::serialize_message_bincode;
use futures::future::try_join_all;
use nested_sta_rs::api::NestedMessage;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Default, Clone)]
pub struct MessageChunk {
  pub new_msgs: Vec<NestedMessage>,
  pub pending_msgs: Vec<PendingMessage>,
  pub parent_msg_tag: Option<Vec<u8>>,
}

type EpochChunksMap = HashMap<Vec<u8>, MessageChunk>;
type ChunksMap = HashMap<u8, EpochChunksMap>;

type PendingMessageMap = HashMap<Vec<u8>, Vec<PendingMessage>>;

#[derive(Default)]
pub struct GroupedMessages {
  pub msg_chunks: ChunksMap,
}

const DB_WORKERS: usize = 4;
const INSERT_BATCH_SIZE: usize = 10000;

impl GroupedMessages {
  pub fn add(&mut self, msg: NestedMessage, parent_msg_tag: Option<&[u8]>) {
    let epoch_chunk = self.msg_chunks.entry(msg.epoch).or_default();
    let chunk = epoch_chunk
      .entry(msg.unencrypted_layer.tag.clone())
      .or_default();
    chunk.new_msgs.push(msg);
    if chunk.parent_msg_tag.is_none() {
      if let Some(tag) = parent_msg_tag {
        chunk.parent_msg_tag = Some(tag.to_vec());
      }
    }
  }

  pub async fn fetch_recovered(
    &mut self,
    db_pool: Arc<DBPool>,
    rec_msgs: &mut RecoveredMessages,
  ) -> Result<(), AggregatorError> {
    for (epoch, epoch_chunks) in self.msg_chunks.iter() {
      let msg_tags: Vec<Vec<u8>> = epoch_chunks.keys().cloned().collect();
      rec_msgs
        .fetch_recovered(db_pool.clone(), *epoch, msg_tags)
        .await?;
    }
    Ok(())
  }

  pub async fn fetch_pending(&mut self, db_pool: Arc<DBPool>) -> Result<(), AggregatorError> {
    for (epoch, epoch_chunks) in self.msg_chunks.iter_mut() {
      let msg_tags_count = epoch_chunks.len();
      let msg_tags: Vec<Vec<u8>> = epoch_chunks.keys().cloned().collect();
      let pending_fetch_tasks: Vec<JoinHandle<Result<PendingMessageMap, AggregatorError>>> =
        msg_tags
          .chunks(max(msg_tags_count / DB_WORKERS, 1))
          .map(|tags| {
            let db_pool = db_pool.clone();
            let tags = tags.to_vec();
            let epoch = *epoch as i16;
            tokio::spawn(async move {
              let mut pending_msgs = PendingMessageMap::new();
              for tag in tags {
                pending_msgs.insert(
                  tag.clone(),
                  PendingMessage::list(db_pool.clone(), epoch, tag).await?,
                );
              }
              Ok(pending_msgs)
            })
          })
          .collect();

      let task_results = try_join_all(pending_fetch_tasks).await?;
      for res in task_results {
        for (tag, msgs) in res? {
          let chunk = epoch_chunks.get_mut(&tag).unwrap();
          chunk.pending_msgs = msgs;
        }
      }
    }
    Ok(())
  }

  pub async fn store_new_pending_msgs(self, db_pool: Arc<DBPool>) -> Result<(), AggregatorError> {
    for (epoch, mut epoch_chunks) in self.msg_chunks {
      for chunk in epoch_chunks.values_mut() {
        chunk.pending_msgs.clear();
      }

      let mut new_pending_msgs = Vec::new();

      for (tag, chunk) in epoch_chunks {
        for msg in chunk.new_msgs {
          new_pending_msgs.push(NewPendingMessage {
            msg_tag: tag.clone(),
            epoch_tag: epoch as i16,
            message: serialize_message_bincode(msg)?,
          });
        }
      }

      if !new_pending_msgs.is_empty() {
        for new_msgs in new_pending_msgs.chunks(INSERT_BATCH_SIZE) {
          let new_msgs = new_msgs.to_vec();
          new_msgs.insert_batch(db_pool.clone()).await?;
        }
      }
    }
    Ok(())
  }

  pub fn split(mut self, chunk_count: usize) -> Vec<Self> {
    let mut result: Vec<Self> = (0..chunk_count).map(|_| Self::default()).collect();
    for (epoch, old_epoch_chunk) in &mut self.msg_chunks {
      let msg_tags: Vec<Vec<u8>> = old_epoch_chunk.keys().cloned().collect();
      let msg_tag_chunks: Vec<Vec<Vec<u8>>> = msg_tags
        .chunks(max(1, msg_tags.len() / chunk_count))
        .map(|v| v.to_vec())
        .collect();
      for (mut i, tag_chunk) in msg_tag_chunks.into_iter().enumerate() {
        i = min(i, chunk_count - 1);
        let new_epoch_chunk = result[i].msg_chunks.entry(*epoch).or_default();
        for tag in tag_chunk {
          let msg_chunk = old_epoch_chunk.remove(&tag).unwrap();
          new_epoch_chunk.insert(tag, msg_chunk);
        }
      }
    }
    result
  }
}
