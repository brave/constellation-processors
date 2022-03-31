use std::error::Error;

mod inmem;

pub use inmem::*;

pub trait RecordStream {
  fn produce(&mut self, record: &str) -> Result<(), Box<dyn Error>>;
  fn consume(&mut self) -> Result<Vec<String>, Box<dyn Error>>;
  fn commit_last_consume(&mut self) -> Result<(), Box<dyn Error>>;
}
