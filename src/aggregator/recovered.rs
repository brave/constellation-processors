use super::AggregatorError;
use crate::models::{
  BatchInsert, DBConnection, DBStorageConnections, NewRecoveredMessage, RecoveredMessage,
};
use crate::profiler::Profiler;
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

const INSERT_BATCH_SIZE: usize = 5000;
const FETCH_BATCH_SIZE: usize = 10000;

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
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
    for msg_tags in msg_tags.chunks(FETCH_BATCH_SIZE) {
      let recovered_msgs = RecoveredMessage::list(
        conn.clone(),
        epoch as i16,
        msg_tags.to_vec(),
        profiler.clone(),
      )
      .await?;
      for rec_msg in recovered_msgs {
        self.add(rec_msg);
      }
    }
    Ok(())
  }

  pub async fn fetch_all_recovered_with_nonzero_count(
    &mut self,
    conn: Arc<Mutex<DBConnection>>,
    epoch: u8,
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
    let recovered_msgs =
      RecoveredMessage::list_with_nonzero_count(conn, epoch as i16, profiler).await?;
    for rec_msg in recovered_msgs {
      self.add(rec_msg);
    }
    Ok(())
  }

  pub async fn save(
    self,
    store_conns: &Arc<DBStorageConnections>,
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
    let mut new_msgs = Vec::new();
    for (_, epoch_map) in self.map {
      for (_, rec_msg) in epoch_map {
        if rec_msg.id == 0 {
          new_msgs.push(NewRecoveredMessage::from(rec_msg));
        } else {
          RecoveredMessage::update_count(
            store_conns.get(),
            rec_msg.id,
            rec_msg.count,
            profiler.clone(),
          )
          .await?;
        }
      }
    }
    for new_msgs in new_msgs.chunks(INSERT_BATCH_SIZE) {
      let new_msgs = new_msgs.to_vec();
      new_msgs
        .insert_batch(store_conns.get(), profiler.clone())
        .await?;
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::models::{DBConnectionType, DBPool};
  use dotenvy::dotenv;

  #[tokio::test]
  async fn add_and_save() {
    dotenv().ok();
    let mut recovered_msgs = RecoveredMessages::default();
    let profiler = Arc::new(Profiler::default());

    let new_rec_msgs = vec![
      RecoveredMessage {
        id: 0,
        msg_tag: vec![51; 20],
        epoch_tag: 2,
        metric_name: "test".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 12,
        key: vec![88; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![52; 20],
        epoch_tag: 2,
        metric_name: "test".to_string(),
        metric_value: "2".to_string(),
        parent_recovered_msg_tag: None,
        count: 25,
        key: vec![77; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![53; 20],
        epoch_tag: 3,
        metric_name: "test".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: None,
        count: 7,
        key: vec![99; 32],
        has_children: true,
      },
    ];

    for rec_msg in new_rec_msgs {
      recovered_msgs.add(rec_msg);
    }

    assert_eq!(
      recovered_msgs.get_mut(2, &vec![51; 20]).unwrap().key,
      vec![88u8; 32]
    );
    assert_eq!(
      recovered_msgs.get_mut(2, &vec![52; 20]).unwrap().key,
      vec![77u8; 32]
    );
    assert_eq!(
      recovered_msgs.get_mut(3, &vec![53; 20]).unwrap().key,
      vec![99u8; 32]
    );
    assert!(recovered_msgs.get_mut(4, &vec![53; 20]).is_none());
    assert!(recovered_msgs.get_mut(3, &vec![55; 20]).is_none());

    let db_pool = Arc::new(DBPool::new(DBConnectionType::Test));
    let store_conns = Arc::new(DBStorageConnections::new(&db_pool, true).await.unwrap());

    recovered_msgs
      .save(&store_conns, profiler.clone())
      .await
      .unwrap();
    drop(store_conns);

    let conn = Arc::new(Mutex::new(db_pool.get().await.unwrap()));
    recovered_msgs = RecoveredMessages::default();
    for epoch in 0..6 {
      let tags = vec![
        vec![51u8; 20],
        vec![52u8; 20],
        vec![53u8; 20],
        vec![54u8; 20],
        vec![55u8; 20],
      ];
      recovered_msgs
        .fetch_recovered(conn.clone(), epoch, tags, profiler.clone())
        .await
        .unwrap();
    }

    assert_eq!(
      recovered_msgs.get_mut(2, &vec![51; 20]).unwrap().key,
      vec![88u8; 32]
    );
    assert_eq!(
      recovered_msgs.get_mut(2, &vec![52; 20]).unwrap().key,
      vec![77u8; 32]
    );
    assert_eq!(
      recovered_msgs.get_mut(3, &vec![53; 20]).unwrap().key,
      vec![99u8; 32]
    );
    assert!(recovered_msgs.get_mut(4, &vec![53u8; 20]).is_none());
    assert!(recovered_msgs.get_mut(3, &vec![55u8; 20]).is_none());
  }

  #[tokio::test]
  async fn update_and_save() {
    dotenv().ok();
    let profiler = Arc::new(Profiler::default());

    let new_rec_msgs = vec![
      NewRecoveredMessage {
        msg_tag: vec![60; 20],
        epoch_tag: 3,
        metric_name: "test".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: None,
        count: 20,
        key: vec![20; 32],
        has_children: true,
      },
      NewRecoveredMessage {
        msg_tag: vec![60; 20],
        epoch_tag: 4,
        metric_name: "test".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: None,
        count: 40,
        key: vec![40; 32],
        has_children: true,
      },
    ];

    let db_pool = Arc::new(DBPool::new(DBConnectionType::Test));
    let conn = Arc::new(Mutex::new(db_pool.get().await.unwrap()));

    new_rec_msgs
      .clone()
      .insert_batch(conn.clone(), profiler.clone())
      .await
      .unwrap();

    let mut recovered_msgs = RecoveredMessages::default();

    for epoch in 3..=4 {
      let mut rec_msg =
        RecoveredMessage::list(conn.clone(), epoch, vec![vec![60; 20]], profiler.clone())
          .await
          .unwrap()[0]
          .clone();
      rec_msg.count += 5;
      recovered_msgs.add(rec_msg);
    }

    drop(conn);
    let store_conns = Arc::new(DBStorageConnections::new(&db_pool, true).await.unwrap());
    recovered_msgs
      .save(&store_conns, profiler.clone())
      .await
      .unwrap();
    drop(store_conns);
    let conn = Arc::new(Mutex::new(db_pool.get().await.unwrap()));

    for epoch in 3..=4 {
      let rec_msg =
        RecoveredMessage::list(conn.clone(), epoch, vec![vec![60; 20]], profiler.clone())
          .await
          .unwrap()[0]
          .clone();
      assert_eq!(rec_msg.count, rec_msg.key[0] as i64 + 5);
    }
  }

  #[tokio::test]
  async fn parent_tags() {
    dotenv().ok();

    let mut recovered_msgs = RecoveredMessages::default();

    let new_rec_msgs = vec![
      RecoveredMessage {
        id: 0,
        msg_tag: vec![51; 20],
        epoch_tag: 2,
        metric_name: "test".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 12,
        key: vec![88; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![52; 20],
        epoch_tag: 2,
        metric_name: "test".to_string(),
        metric_value: "2".to_string(),
        parent_recovered_msg_tag: Some(vec![51; 20]),
        count: 25,
        key: vec![77; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![53; 20],
        epoch_tag: 2,
        metric_name: "test".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: Some(vec![52; 20]),
        count: 7,
        key: vec![99; 32],
        has_children: true,
      },
    ];

    for rec_msg in new_rec_msgs {
      recovered_msgs.add(rec_msg);
    }

    assert_eq!(
      recovered_msgs.get_tags_by_parent(2, None),
      vec![vec![51u8; 20]]
    );

    assert_eq!(
      recovered_msgs.get_tags_by_parent(2, Some(vec![51; 20])),
      vec![vec![52u8; 20]]
    );

    assert_eq!(
      recovered_msgs.get_tags_by_parent(2, Some(vec![52; 20])),
      vec![vec![53u8; 20]]
    );
  }
}
