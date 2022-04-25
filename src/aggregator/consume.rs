use super::group::GroupedMessages;
use super::AggregatorError;
use crate::record_stream::RecordStream;
use crate::star::parse_message;
use std::time::Duration;
use tokio::time::sleep;

const MAX_MESSAGES_TO_COLLECT: usize = 650000;
const RECV_TIMEOUT_MS: u64 = 2500;

pub async fn consume_and_group() -> Result<(GroupedMessages, RecordStream, usize), AggregatorError>
{
  let mut count = 0;
  let mut grouped_msgs = GroupedMessages::default();

  let rec_stream = RecordStream::new(false, true, false);
  loop {
    tokio::select! {
      msg_res = rec_stream.consume() => {
        let msg = msg_res?;
        let msg_data = parse_message(&msg)?;

        grouped_msgs.add(msg_data.msg, None);
        count += 1;
        if count >= MAX_MESSAGES_TO_COLLECT {
          break;
        }
      },
      _ = sleep(Duration::from_millis(RECV_TIMEOUT_MS)) => {
        break;
      }
    }
  }

  Ok((grouped_msgs, rec_stream, count))
}
