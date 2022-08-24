use super::AggregatorError;
use crate::models::{BatchInsert, DBConnection, NewRecoveredMessage, RecoveredMessage};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

type EpochRecMsgsMap = HashMap<Vec<u8>, RecoveredMessage>;
type RecMsgsMap = HashMap<u8, EpochRecMsgsMap>;

type EpochParentTagsMap = HashMap<Vec<u8>, HashSet<Vec<u8>>>;
type ParentTagsMap = HashMap<u8, EpochParentTagsMap>;

#[derive(Default)]
pub struct RecoveredMessages {
  pub map: RecMsgsMap,
  pub parent_tags_map: ParentTagsMap,
}

const INSERT_BATCH_SIZE: usize = 10000;

impl RecoveredMessages {
  pub fn add(&mut self, rec_msg: RecoveredMessage) {
    let parent_epoch_map = self
      .parent_tags_map
      .entry(rec_msg.epoch_tag as u8)
      .or_default();
    let parent_msg_tag_key = rec_msg.parent_recovered_msg_tag.clone().unwrap_or_default();
    let parent_tag_set = parent_epoch_map.entry(parent_msg_tag_key).or_default();
    parent_tag_set.insert(rec_msg.msg_tag.clone());
    let epoch_map = self.map.entry(rec_msg.epoch_tag as u8).or_default();
    epoch_map.insert(rec_msg.msg_tag.clone(), rec_msg);
  }

  pub fn get_mut(&mut self, epoch: u8, msg_tag: &[u8]) -> Option<&mut RecoveredMessage> {
    let epoch_tag = self.map.entry(epoch).or_default();
    epoch_tag.get_mut(msg_tag)
  }

  pub fn get_tags_by_parent(&self, epoch: u8, parent_msg_tag: Option<Vec<u8>>) -> Vec<Vec<u8>> {
    self
      .parent_tags_map
      .get(&epoch)
      .map(|epoch_map| {
        epoch_map
          .get(&parent_msg_tag.unwrap_or_default())
          .map(|s| s.iter().cloned().collect())
          .unwrap_or_default()
      })
      .unwrap_or_default()
  }

  pub async fn fetch_recovered(
    &mut self,
    conn: Arc<Mutex<DBConnection>>,
    epoch: u8,
    msg_tags: Vec<Vec<u8>>,
  ) -> Result<(), AggregatorError> {
    let recovered_msgs = RecoveredMessage::list(conn, epoch as i16, msg_tags).await?;
    for rec_msg in recovered_msgs {
      self.add(rec_msg);
    }
    Ok(())
  }

  pub async fn fetch_all_recovered_with_nonzero_count(
    &mut self,
    conn: Arc<Mutex<DBConnection>>,
    epoch: u8,
  ) -> Result<(), AggregatorError> {
    let recovered_msgs = RecoveredMessage::list_with_nonzero_count(conn, epoch as i16).await?;
    for rec_msg in recovered_msgs {
      self.add(rec_msg);
    }
    Ok(())
  }

  pub async fn save(self, conn: Arc<Mutex<DBConnection>>) -> Result<(), AggregatorError> {
    let mut new_msgs = Vec::new();
    for (_, epoch_map) in self.map {
      for (_, rec_msg) in epoch_map {
        if rec_msg.id == 0 {
          new_msgs.push(NewRecoveredMessage::from(rec_msg));
        } else {
          RecoveredMessage::update_count(conn.clone(), rec_msg.id, rec_msg.count).await?;
        }
      }
    }
    for new_msgs in new_msgs.chunks(INSERT_BATCH_SIZE) {
      let new_msgs = new_msgs.to_vec();
      new_msgs.insert_batch(conn.clone()).await?;
    }
    Ok(())
  }
}
