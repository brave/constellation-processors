use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::RecordStream;
use crate::star::parse_message;
use std::time::Duration;
use tokio::time::sleep;

const RECV_TIMEOUT_MS: u64 = 2500;

pub async fn consume_and_group(
  rec_stream: &RecordStream,
  msg_collect_count: usize,
) -> Result<(GroupedMessages, usize), AggregatorError> {
  let mut count = 0;
  let mut grouped_msgs = GroupedMessages::default();

  loop {
    tokio::select! {
      msg_res = rec_stream.consume() => {
        let msg = msg_res?;
        let msg_data = parse_message(&msg)?;

        grouped_msgs.add(msg_data.msg, None);
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
