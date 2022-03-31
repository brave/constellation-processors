use super::RecordStream;
use std::collections::VecDeque;
use std::ops::Range;
use std::error::Error;
use std::cmp::min;

const MAX_POLL_SIZE: usize = 64;

#[derive(Default)]
pub struct InMemRecordStream {
  queue: VecDeque<String>,
  next_index_to_consume: Option<usize>,
  last_consume_end_index: Option<usize>
}

impl InMemRecordStream {
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

impl RecordStream for InMemRecordStream {
  fn produce(&mut self, record: &str) -> Result<(), Box<dyn Error>> {
    self.queue.push_front(record.to_string());
    Ok(())
  }

  fn consume(&mut self) -> Result<Vec<String>, Box<dyn Error>> {
    let range = self.get_consume_range(false);
    self.last_consume_end_index = Some(range.end);
    Ok(self.queue.range(range).cloned().collect())
  }

  fn commit_last_consume(&mut self) -> Result<(), Box<dyn Error>> {
    let range = self.get_consume_range(true);

    self.queue.drain(range.clone());

    self.next_index_to_consume = Some(range.end - (range.end - range.start));
    self.last_consume_end_index = None;
    Ok(())
  }

}
