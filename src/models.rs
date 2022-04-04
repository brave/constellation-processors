use std::fmt;

#[derive(Default, Hash, PartialEq, Eq, Clone)]
pub struct MsgInfo {
  pub epoch_tag: u8,
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
