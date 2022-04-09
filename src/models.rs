use std::fmt;
use std::str::FromStr;

#[derive(Default, Hash, PartialEq, Eq, Clone, Debug)]
pub struct MsgInfo {
  pub epoch_tag: u8,
  pub msg_tags: Vec<Vec<u8>>
}

impl fmt::Display for MsgInfo {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let tag_str = self.msg_tags.iter()
      .map(|v| format!("tag={}", hex::encode(v)))
      .collect::<Vec<String>>()
      .join("/");
    write!(f, "epoch={}/{}", self.epoch_tag, tag_str)
  }
}

impl From<&str> for MsgInfo {
  fn from(v: &str) -> MsgInfo {
    let mut result: MsgInfo = Default::default();
    v.split("/").for_each(|v| {
      let parts: Vec<&str> = v.split("=").collect();
      if parts.len() != 2 {
        return;
      }
      match parts[0] {
        "epoch" => {
          result.epoch_tag = u8::from_str(parts[1]).unwrap();
        },
        "tag" => {
          result.msg_tags.push(hex::decode(parts[1]).unwrap());
        },
        _ => ()
      };
    });
    result
  }
}
