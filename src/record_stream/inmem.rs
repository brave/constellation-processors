use super::{RecordStream, RecordStreamError};
use std::collections::VecDeque;
use async_trait::async_trait;
use tokio::sync::{Notify, Mutex};

#[derive(Default)]
pub struct InMemRecordStream {
  queue: Mutex<VecDeque<String>>,
  notify: Notify
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
    while queue.is_empty() {
      drop(queue);
      self.notify.notified().await;
      queue = self.queue.lock().await;
    }
    Ok(queue.front().unwrap().clone())
  }

  async fn commit_last_consume(&self) -> Result<(), RecordStreamError> {
    let mut queue = self.queue.lock().await;

    queue.pop_front();

    Ok(())
  }

}
