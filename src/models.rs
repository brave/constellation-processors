use std::fmt;
use crate::record_stream::RecordStream;
use tokio::sync::Mutex;

#[derive(Default, Hash, PartialEq, Eq, Clone)]
pub struct MsgInfo {
  pub epoch_tag: String,
  pub msg_tag: Vec<u8>,
  pub layer: usize
}

impl fmt::Display for MsgInfo {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let msg_tag_hex = hex::encode(&self.msg_tag);
    write!(f, "epoch={}/layer={}/tag={}",
      self.epoch_tag, self.layer, msg_tag_hex)
  }
}

pub struct AppState {
  pub rec_stream: Mutex<Box<dyn RecordStream + Send>>
}
