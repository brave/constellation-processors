use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::DynRecordStream;
use crate::star::parse_message;
use std::time::Duration;
use tokio::time::sleep;

const RECV_TIMEOUT_MS: u64 = 2500;

pub async fn consume_and_group(
  rec_stream: &DynRecordStream,
  msg_collect_count: usize,
) -> Result<(GroupedMessages, usize), AggregatorError> {
  let mut count = 0;
  let mut grouped_msgs = GroupedMessages::default();

  loop {
    tokio::select! {
      msg_res = rec_stream.consume() => {
        let msg = msg_res?;
        let msg_data = parse_message(&msg)?;

        grouped_msgs.add(msg_data, None);
        count += 1;
        if count >= msg_collect_count {
          break;
        }
      },
      _ = sleep(Duration::from_millis(RECV_TIMEOUT_MS)) => {
        break;
      }
    }
  }

  Ok((grouped_msgs, count))
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

  async fn prepare_record_stream() -> TestRecordStream {
    let record_stream = TestRecordStream::default();

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
    record_stream
  }
}
