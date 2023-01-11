use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::RecordStreamArc;
use crate::star::parse_message;
use futures::future::try_join_all;
use star_constellation::api::NestedMessage;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const MIN_RECV_TIMEOUT_MS_ENV_KEY: &str = "MIN_RECV_TIMEOUT_MS";
const DEFAULT_MIN_RECV_TIMEOUT_MS: &str = "2500";
const MAX_RECV_TIMEOUT_MS_ENV_KEY: &str = "MAX_RECV_TIMEOUT_MS";
const DEFAULT_MAX_RECV_TIMEOUT_MS: &str = "30000";

fn create_recv_tasks(
  rec_streams: &Vec<RecordStreamArc>,
  parsing_tasks: &Vec<(
    mpsc::UnboundedSender<Vec<u8>>,
    JoinHandle<Result<(), AggregatorError>>,
  )>,
  msg_count: Arc<Mutex<usize>>,
  msgs_to_collect_count: usize,
) -> Vec<JoinHandle<Result<(), AggregatorError>>> {
  let min_recv_timeout = Duration::from_millis(
    u64::from_str(
      &env::var(MIN_RECV_TIMEOUT_MS_ENV_KEY).unwrap_or(DEFAULT_MIN_RECV_TIMEOUT_MS.to_string()),
    )
    .unwrap(),
  );
  let max_recv_timeout = Duration::from_millis(
    u64::from_str(
      &env::var(MAX_RECV_TIMEOUT_MS_ENV_KEY).unwrap_or(DEFAULT_MAX_RECV_TIMEOUT_MS.to_string()),
    )
    .unwrap(),
  );
  parsing_tasks
    .iter()
    .zip(rec_streams.iter().cloned().into_iter())
    .map(|((raw_tx, _), rec_stream)| {
      let raw_tx = raw_tx.clone();
      let msg_count = msg_count.clone();
      tokio::spawn(async move {
        let mut total_wait_time = Duration::from_secs(0);
        loop {
          tokio::select! {
            msg_res = rec_stream.consume() => {
              raw_tx.send(msg_res?).unwrap();

              let mut msg_count = msg_count.lock().await;
              *msg_count += 1;
              if *msg_count >= msgs_to_collect_count {
                break;
              }
              drop(msg_count);
            },
            _ = sleep(min_recv_timeout) => {
              total_wait_time += min_recv_timeout;
              // if there are assigned partitions, wait only until the min_recv_timeout for a message.
              // if there are not assigned partitions, wait until the max_recv_timeout to give
              // the broker a chance to assign partitions.
              if rec_stream.has_assigned_partitions()? || total_wait_time >= max_recv_timeout {
                break;
              }
            },
          }
        }
        info!("Kafka consume task finished");
        Ok(())
      })
    })
    .collect()
}

fn create_parsing_tasks(
  task_count: usize,
  parsed_tx: UnboundedSender<NestedMessage>,
) -> Vec<(
  mpsc::UnboundedSender<Vec<u8>>,
  JoinHandle<Result<(), AggregatorError>>,
)> {
  (0..task_count)
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
    .collect()
}

pub async fn consume_and_group(
  rec_streams: &Vec<RecordStreamArc>,
  msgs_to_collect_count: usize,
) -> Result<(GroupedMessages, usize), AggregatorError> {
  let mut grouped_msgs = GroupedMessages::default();

  let (parsed_tx, mut parsed_rx) = mpsc::unbounded_channel::<NestedMessage>();
  let msg_count = Arc::new(Mutex::new(0));

  let parsing_tasks = create_parsing_tasks(rec_streams.len(), parsed_tx);
  let recv_tasks = create_recv_tasks(
    rec_streams,
    &parsing_tasks,
    msg_count.clone(),
    msgs_to_collect_count,
  );

  let mut task_handles = recv_tasks;
  task_handles.extend(parsing_tasks.into_iter().map(|(_, handle)| handle));

  while let Some(parsed_msg) = parsed_rx.recv().await {
    grouped_msgs.add(parsed_msg, None);
  }

  try_join_all(task_handles)
    .await?
    .into_iter()
    .collect::<Result<Vec<()>, AggregatorError>>()?;

  info!("Messages grouped");

  let msg_count = *msg_count.lock().await;
  Ok((grouped_msgs, msg_count))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::record_stream::TestRecordStream;
  use crate::star::tests::generate_test_message;
  use star_constellation::api::SerializableNestedMessage;
  use star_constellation::randomness::testing::LocalFetcher;

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
