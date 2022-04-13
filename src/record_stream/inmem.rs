use super::{RecordStream, RecordStreamError};
use async_trait::async_trait;
use std::collections::VecDeque;
use tokio::sync::{Mutex, Notify};

#[derive(Default)]
pub struct InMemRecordStream {
  queue: Mutex<VecDeque<String>>,
  notify: Notify,
  consume_count: Mutex<usize>,
}

#[async_trait]
impl RecordStream for InMemRecordStream {
  async fn produce(&self, record: &str) -> Result<(), RecordStreamError> {
    let mut queue = self.queue.lock().await;
    queue.push_back(record.to_string());
    self.notify.notify_one();
    Ok(())
  }

  async fn consume(&self) -> Result<String, RecordStreamError> {
    let mut queue = self.queue.lock().await;
    let c_count = *self.consume_count.lock().await;
    while c_count >= queue.len() {
      drop(queue);
      self.notify.notified().await;
      queue = self.queue.lock().await;
    }
    let res = queue.get(c_count).unwrap().clone();
    *self.consume_count.lock().await += 1;
    Ok(res)
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let mut queue = self.queue.lock().await;

    let mut c_count = self.consume_count.lock().await;
    queue.drain(..*c_count);
    *c_count = 0;

    Ok(())
  }
}
