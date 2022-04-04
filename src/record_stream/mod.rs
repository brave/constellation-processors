use async_trait::async_trait;
use derive_more::{Display, Error};

mod inmem;
mod kafka;

pub use inmem::*;
pub use kafka::*;

#[derive(Debug, Display, Error)]
#[display(fmt = "Record stream error: {}", description)]
pub struct RecordStreamError {
  description: String
}

impl From<String> for RecordStreamError {
  fn from(description: String) -> Self {
    Self {
      description
    }
  }
}

#[async_trait]
pub trait RecordStream {
  async fn produce(&self, record: &str) -> Result<(), RecordStreamError>;
  async fn consume(&self) -> Result<String, RecordStreamError>;
  async fn commit_last_consume(&self) -> Result<(), RecordStreamError>;
}
