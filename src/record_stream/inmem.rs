use super::{RecordStream, RecordStreamError};
use std::collections::VecDeque;
use std::ops::Range;
use std::cmp::min;
use async_trait::async_trait;
use tokio::sync::{Notify, Mutex};

const MAX_POLL_SIZE: usize = 64;

#[derive(Default)]
struct InMemStreamData {
  queue: VecDeque<String>,
  next_index_to_consume: Option<usize>,
  last_consume_end_index: Option<usize>
}

#[derive(Default)]
pub struct InMemRecordStream {
  data: Mutex<InMemStreamData>,
  notify: Notify
}

impl InMemStreamData {
  fn get_consume_range(&self, use_last_consume: bool) -> Range<usize> {
    let start = self.next_index_to_consume.unwrap_or(0);
    let end = if use_last_consume {
      self.last_consume_end_index.unwrap_or(0)
    } else {
      min(start + MAX_POLL_SIZE, self.queue.len())
    };
    start..end
  }
}

#[async_trait]
impl RecordStream for InMemRecordStream {
  async fn produce(&self, record: &str) -> Result<(), RecordStreamError> {
    let mut data = self.data.lock().await;
    data.queue.push_front(record.to_string());
    self.notify.notify_one();
    Ok(())
  }

  async fn consume(&self) -> Result<Vec<String>, RecordStreamError> {
    let mut data = self.data.lock().await;
    let mut range = data.get_consume_range(false);
    if (range.end - range.start) == 0 {
      drop(data);
      self.notify.notified().await;
      data = self.data.lock().await;
      range = data.get_consume_range(false);
    }

    data.last_consume_end_index = Some(range.end);
    Ok(data.queue.range(range).cloned().collect())
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let mut data = self.data.lock().await;
    let range = data.get_consume_range(true);

    data.queue.drain(range.clone());

    data.next_index_to_consume = Some(range.end - (range.end - range.start));
    data.last_consume_end_index = None;
    Ok(())
  }

}
