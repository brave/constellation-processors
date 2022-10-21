use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::RecordStreamArc;
use crate::star::parse_message;
use futures::future::try_join_all;
use nested_sta_rs::api::NestedMessage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;

const RECV_TIMEOUT_MS: u64 = 2500;

pub async fn consume_and_group(
  rec_streams: &Vec<RecordStreamArc>,
  msg_collect_count: usize,
) -> Result<(GroupedMessages, usize), AggregatorError> {
  let msg_count = Arc::new(Mutex::new(0));
  let mut grouped_msgs = GroupedMessages::default();

  let (parsed_tx, mut parsed_rx) = mpsc::unbounded_channel::<NestedMessage>();

  let parsing_tasks = rec_streams
    .iter()
    .map(|_| {
      let parsed_tx = parsed_tx.clone();
      let (raw_tx, mut raw_rx) = mpsc::unbounded_channel::<Vec<u8>>();
      let task = tokio::spawn(async move {
        while let Some(msg) = raw_rx.recv().await {
          parsed_tx.send(parse_message(&msg)?).unwrap();
        }
        info!("Parsing task finished");
        Ok(())
      });
      (raw_tx, task)
    })
    .collect::<Vec<(
      mpsc::UnboundedSender<Vec<u8>>,
      JoinHandle<Result<(), AggregatorError>>,
    )>>();
  drop(parsed_tx);

  let mut task_handles = parsing_tasks
    .iter()
    .zip(rec_streams.iter().cloned().into_iter())
    .map(|((raw_tx, _), rec_stream)| {
      let raw_tx = raw_tx.clone();
      let msg_count = msg_count.clone();
      tokio::spawn(async move {
        loop {
          tokio::select! {
            msg_res = rec_stream.consume() => {
              raw_tx.send(msg_res?).unwrap();

              let mut msg_count = msg_count.lock().await;
              *msg_count += 1;
              if *msg_count >= msg_collect_count {
                break;
              }
              drop(msg_count);
            },
            _ = sleep(Duration::from_millis(RECV_TIMEOUT_MS)) => {
              break;
            }
          }
        }
        info!("Kafka consume task finished");
        Ok(())
      })
    })
    .collect::<Vec<JoinHandle<Result<(), AggregatorError>>>>();

  task_handles.extend(
    parsing_tasks
      .into_iter()
      .map(|(_, handle)| handle)
      .collect::<Vec<JoinHandle<Result<(), AggregatorError>>>>(),
  );

  while let Some(parsed_msg) = parsed_rx.recv().await {
    grouped_msgs.add(parsed_msg, None);
  }

  for handle in try_join_all(task_handles).await? {
    if let Err(e) = handle {
      return Err(e);
    }
  }

  info!("Messages grouped");

  let msg_count = *msg_count.lock().await;

  Ok((grouped_msgs, msg_count))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::record_stream::TestRecordStream;
  use crate::star::tests::generate_test_message;
  use nested_sta_rs::api::SerializableNestedMessage;
  use nested_sta_rs::randomness::testing::LocalFetcher;

  #[tokio::test]
  async fn consume_and_group_all() {
    let record_stream = prepare_record_stream().await;

    let (grouped_msgs, count) = consume_and_group(&record_stream, 1024).await.unwrap();

    assert_eq!(count, 7);
    assert_eq!(grouped_msgs.msg_chunks.get(&4).unwrap().len(), 1);
    assert_eq!(grouped_msgs.msg_chunks.get(&5).unwrap().len(), 2);
    assert_eq!(grouped_msgs.msg_chunks.get(&6).unwrap().len(), 2);
  }

  #[tokio::test]
  async fn consume_and_group_some() {
    let record_stream = prepare_record_stream().await;

    let (grouped_msgs, count) = consume_and_group(&record_stream, 3).await.unwrap();

    assert_eq!(count, 3);
    assert_eq!(grouped_msgs.msg_chunks.get(&4).unwrap().len(), 1);
    assert_eq!(grouped_msgs.msg_chunks.get(&5).unwrap().len(), 1);
    assert!(grouped_msgs.msg_chunks.get(&6).is_none());
  }

  async fn prepare_record_stream() -> Vec<RecordStreamArc> {
    let record_stream = Arc::new(TestRecordStream::default());

    let fetcher = LocalFetcher::new();

    let msg_infos = vec![
      (4, "test|5"),
      (4, "test|5"),
      (5, "test|1"),
      (5, "test|3"),
      (5, "test|3"),
      (6, "test|2"),
      (6, "test|1"),
    ];

    let mut records_to_consume = record_stream.records_to_consume.lock().await;
    for (epoch, measurement) in msg_infos {
      let msg = bincode::serialize(&SerializableNestedMessage::from(generate_test_message(
        epoch,
        &vec![measurement.as_bytes().to_vec()],
        &fetcher,
      )))
      .unwrap();
      records_to_consume.push(msg);
    }
    drop(records_to_consume);
    vec![record_stream]
  }
}
