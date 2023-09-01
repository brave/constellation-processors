use super::recovered::RecoveredMessages;
use super::AggregatorError;
use crate::models::{BatchInsert, DBPool, DBStorageConnections, NewPendingMessage, PendingMessage};
use crate::profiler::Profiler;
use crate::star::serialize_message_bincode;
use futures::future::try_join_all;
use star_constellation::api::NestedMessage;
use std::cmp::max;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

const DB_WORKERS: usize = 4;
const INSERT_BATCH_SIZE: usize = 10000;

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
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
    let conn = Arc::new(Mutex::new(db_pool.get().await?));
    for (epoch, epoch_chunks) in self.msg_chunks.iter() {
      let msg_tags: Vec<Vec<u8>> = epoch_chunks.keys().cloned().collect();
      rec_msgs
        .fetch_recovered(conn.clone(), *epoch, msg_tags, profiler.clone())
        .await?;
    }
    Ok(())
  }

  pub async fn fetch_pending(
    &mut self,
    db_pool: Arc<DBPool>,
    rec_msgs: &mut RecoveredMessages,
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
    for (epoch, epoch_chunks) in self.msg_chunks.iter_mut() {
      let msg_tags_count = epoch_chunks.len();
      let msg_tags: Vec<Vec<u8>> = epoch_chunks
        .keys()
        .filter(|tag| rec_msgs.get_mut(*epoch, tag).is_none())
        .cloned()
        .collect();
      let pending_fetch_tasks: Vec<JoinHandle<Result<PendingMessageMap, AggregatorError>>> =
        msg_tags
          .chunks(max(msg_tags_count / DB_WORKERS, 1))
          .map(|tags| {
            let db_pool = db_pool.clone();
            let tags = tags.to_vec();
            let epoch = *epoch as i16;
            let profiler = profiler.clone();
            tokio::spawn(async move {
              let conn = Arc::new(Mutex::new(db_pool.get().await?));
              let mut pending_msgs = PendingMessageMap::new();

              for tag in tags {
                let msgs =
                  PendingMessage::list(conn.clone(), epoch, tag.clone(), profiler.clone()).await?;
                pending_msgs.insert(tag, msgs);
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

  pub async fn store_new_pending_msgs(
    self,
    store_conns: &Arc<DBStorageConnections>,
    profiler: Arc<Profiler>,
  ) -> Result<(), AggregatorError> {
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
          new_msgs
            .insert_batch(store_conns.get(), profiler.clone())
            .await?;
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
        i = i % chunk_count;
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::models::{DBConnectionType, DBPool, NewRecoveredMessage};
  use crate::star::tests::generate_test_message;
  use dotenvy::dotenv;
  use star_constellation::randomness::testing::LocalFetcher;

  #[tokio::test]
  async fn basic_group_and_pending_storage() {
    dotenv().ok();
    let mut grouped_msgs = GroupedMessages::default();
    let mut rec_msgs = RecoveredMessages::default();
    let profiler = Arc::new(Profiler::default());

    let fetcher = LocalFetcher::new();

    let msg_infos = vec![
      (0, "a|0"),
      (0, "a|0"),
      (0, "a|1"),
      (1, "a|2"),
      (1, "a|3"),
      (2, "a|4"),
      (3, "a|4"),
    ];

    for (epoch, measurement) in msg_infos {
      grouped_msgs.add(
        generate_test_message(epoch, &vec![measurement.as_bytes().to_vec()], &fetcher),
        None,
      );
    }

    let expected_epoch_counts = vec![(0, vec![1, 2]), (1, vec![1, 1]), (2, vec![1]), (3, vec![1])];

    let db_pool = Arc::new(DBPool::new(DBConnectionType::Test));

    let expected_epochs = vec![0, 1, 2, 3];

    let mut epochs: Vec<u8> = grouped_msgs.msg_chunks.keys().map(|v| *v).collect();
    epochs.sort();
    assert_eq!(epochs, expected_epochs);

    for (epoch, expected_tag_counts) in &expected_epoch_counts {
      let epoch_map = grouped_msgs.msg_chunks.get(&epoch).unwrap();
      let mut tag_counts: Vec<usize> = epoch_map.values().map(|v| v.new_msgs.len()).collect();
      tag_counts.sort();
      assert_eq!(&tag_counts, expected_tag_counts);
    }

    let store_conns = Arc::new(DBStorageConnections::new(&db_pool, true).await.unwrap());
    grouped_msgs
      .store_new_pending_msgs(&store_conns, profiler.clone())
      .await
      .unwrap();
    drop(store_conns);

    grouped_msgs = GroupedMessages::default();

    let msg_infos = vec![(0, "a|0"), (1, "a|2"), (2, "a|7"), (3, "a|4")];

    for (epoch, measurement) in msg_infos {
      grouped_msgs.add(
        generate_test_message(epoch, &vec![measurement.as_bytes().to_vec()], &fetcher),
        None,
      );
    }

    // Should fetch pending messages for new message tags
    grouped_msgs
      .fetch_pending(db_pool.clone(), &mut rec_msgs, profiler.clone())
      .await
      .unwrap();

    let mut epochs: Vec<u8> = grouped_msgs.msg_chunks.keys().map(|v| *v).collect();
    epochs.sort();
    assert_eq!(epochs, expected_epochs);

    let expected_epoch_counts = vec![(0, vec![3]), (1, vec![2]), (2, vec![1]), (3, vec![2])];

    for (epoch, expected_tag_counts) in &expected_epoch_counts {
      let epoch_map = grouped_msgs.msg_chunks.get(&epoch).unwrap();
      let mut tag_counts: Vec<usize> = epoch_map
        .values()
        .map(|v| v.new_msgs.len() + v.pending_msgs.len())
        .collect();
      tag_counts.sort();
      assert_eq!(&tag_counts, expected_tag_counts);
    }
  }

  #[test]
  fn chunk_split() {
    let mut grouped_msgs = GroupedMessages::default();
    let epoch_tag_counts = vec![(0, 42), (1, 38), (2, 52), (3, 60)];

    let fetcher = LocalFetcher::new();
    for (epoch, tag_count) in &epoch_tag_counts {
      for i in 0..*tag_count {
        for _ in 0..2 {
          grouped_msgs.add(
            generate_test_message(
              *epoch,
              &vec![format!("a|{}", i).as_bytes().to_vec()],
              &fetcher,
            ),
            None,
          );
        }
      }
    }

    let split_grouped_msgs = grouped_msgs.split(7);

    for grouped_msgs in &split_grouped_msgs {
      let mut epochs: Vec<u8> = grouped_msgs.msg_chunks.keys().map(|v| *v).collect();
      epochs.sort();
      assert_eq!(epochs, vec![0, 1, 2, 3]);
      for epoch_map in grouped_msgs.msg_chunks.values() {
        assert!(epoch_map.len() >= 5 && epoch_map.len() <= 12);
        for tag_val in epoch_map.values() {
          assert_eq!(tag_val.new_msgs.len(), 2);
        }
      }
    }

    for (epoch, expected_tag_count) in &epoch_tag_counts {
      assert_eq!(
        split_grouped_msgs
          .iter()
          .fold(0, |acc, g| acc + g.msg_chunks.get(epoch).unwrap().len()),
        *expected_tag_count
      );
    }
  }

  #[tokio::test]
  async fn fetch_recovered() {
    dotenv().ok();
    let mut grouped_msgs = GroupedMessages::default();
    let fetcher = LocalFetcher::new();
    let profiler = Arc::new(Profiler::default());

    let msg_infos = vec![(0, "a|1"), (1, "a|1"), (2, "a|2")];

    for (epoch, measurement) in msg_infos {
      grouped_msgs.add(
        generate_test_message(epoch, &vec![measurement.as_bytes().to_vec()], &fetcher),
        None,
      );
    }

    let new_rec_msgs: Vec<NewRecoveredMessage> = (0..2)
      .map(|i| NewRecoveredMessage {
        msg_tag: grouped_msgs
          .msg_chunks
          .get(&i)
          .unwrap()
          .keys()
          .next()
          .unwrap()
          .to_vec(),
        epoch_tag: i as i16,
        metric_name: "a".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 1,
        key: Vec::new(),
        has_children: true,
      })
      .collect();

    let db_pool = Arc::new(DBPool::new(DBConnectionType::Test));
    let conn = Arc::new(Mutex::new(db_pool.get().await.unwrap()));
    new_rec_msgs
      .insert_batch(conn.clone(), profiler.clone())
      .await
      .unwrap();
    drop(conn);

    let mut recovered_msgs = RecoveredMessages::default();

    grouped_msgs
      .fetch_recovered(db_pool, &mut recovered_msgs, profiler.clone())
      .await
      .unwrap();

    assert_eq!(recovered_msgs.map.get(&0).unwrap().len(), 1);
    assert_eq!(recovered_msgs.map.get(&1).unwrap().len(), 1);
    assert!(!recovered_msgs.map.contains_key(&2));
  }
}
