use async_trait::async_trait;
use derive_more::{Display, Error};

mod inmem;

pub use inmem::*;

#[derive(Debug, Display, Error)]
#[display(fmt = "Record stream error: {}", description)]
pub struct RecordStreamError {
  description: String
}

#[async_trait]
pub trait RecordStream {
  async fn produce(&self, record: &str) -> Result<(), RecordStreamError>;
  async fn consume(&self) -> Result<Vec<String>, RecordStreamError>;
  async fn commit_last_consume(&self) -> Result<(), RecordStreamError>;
}
