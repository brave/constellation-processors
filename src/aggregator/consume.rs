use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::RecordStreamArc;
use crate::star::parse_message;
use crate::util::parse_env_var;
use futures::future::try_join_all;
use futures::FutureExt;
use star_constellation::api::NestedMessage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const MAX_INIT_RECV_TIMEOUT_MS_ENV_KEY: &str = "MAX_INIT_RECV_TIMEOUT_MS";
const DEFAULT_MAX_INIT_RECV_TIMEOUT_MS: &str = "30000";
const MIN_RECV_RATE_ENV_KEY: &str = "MIN_RECV_RATE_PER_SEC";
const DEFAULT_MIN_RECV_RATE: &str = "100";

const RATE_CHECK_INTERVAL_SECS: u64 = 5;

async fn run_recv_task(
  rec_stream: RecordStreamArc,
  parsing_task_tx: mpsc::UnboundedSender<Vec<u8>>,
  msg_count: Arc<Mutex<usize>>,
  msgs_to_collect_count: usize,
) -> Result<(), AggregatorError> {
  let max_init_recv_timeout = Duration::from_millis(parse_env_var::<u64>(
    MAX_INIT_RECV_TIMEOUT_MS_ENV_KEY,
    DEFAULT_MAX_INIT_RECV_TIMEOUT_MS,
  ));
  let min_recv_rate = parse_env_var::<u64>(MIN_RECV_RATE_ENV_KEY, DEFAULT_MIN_RECV_RATE);
  let rate_check_interval = Duration::from_secs(RATE_CHECK_INTERVAL_SECS);

  let mut total_init_wait_time = Duration::from_secs(0);
  let mut rate_frame_sleep = sleep(rate_check_interval).boxed();
  let mut stream_started = false;
  let mut msgs_recvd_in_frame = 0;

  loop {
    tokio::select! {
      msg_res = rec_stream.consume() => {
        parsing_task_tx.send(msg_res?).unwrap();

        let mut msg_count = msg_count.lock().await;
        *msg_count += 1;
        if *msg_count >= msgs_to_collect_count {
          break;
        }
        drop(msg_count);

        msgs_recvd_in_frame += 1;
        if !stream_started {
          // If the stream has just started (aka the first message was just received),
          // reset the sleep so that we get a proper, unskewed rate measurement.
          rate_frame_sleep = sleep(rate_check_interval).boxed();
          stream_started = true;
        }
      },
      _ = &mut rate_frame_sleep => {
        rate_frame_sleep = sleep(rate_check_interval).boxed();
        if !rec_stream.has_assigned_partitions()? {
          // If there are no assigned partitions, assume we are waiting
          // for a parititon to be assigned. If no partition is available before
          // the max_init_recv_timeout, then assume that partitions are not available
          // and stop the task.
          total_init_wait_time += rate_check_interval;
          if total_init_wait_time >= max_init_recv_timeout {
            break;
          }
        } else {
          // If partitions are assigned, and the msgs/second rate is less than the
          // defined minimum, stop receiving. We are probably near the end of the stream.
          if (msgs_recvd_in_frame / RATE_CHECK_INTERVAL_SECS) < min_recv_rate {
            break;
          }
          msgs_recvd_in_frame = 0;
        }
      },
    }
  }
  info!("Kafka consume task finished");
  Ok(())
}

fn create_recv_tasks(
  rec_streams: &Vec<RecordStreamArc>,
  parsing_tasks: &Vec<(
    mpsc::UnboundedSender<Vec<u8>>,
    JoinHandle<Result<(), AggregatorError>>,
  )>,
  msg_count: Arc<Mutex<usize>>,
  msgs_to_collect_count: usize,
) -> Vec<JoinHandle<Result<(), AggregatorError>>> {
  parsing_tasks
    .iter()
    .zip(rec_streams.iter().cloned().into_iter())
    .map(|((parsing_task_tx, _), rec_stream)| {
      let parsing_task_tx = parsing_task_tx.clone();
      let msg_count = msg_count.clone();
      tokio::spawn(async move {
        run_recv_task(
          rec_stream,
          parsing_task_tx,
          msg_count,
          msgs_to_collect_count,
        )
        .await
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
