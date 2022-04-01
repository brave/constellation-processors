use nested_sta_rs::api::{SerializableNestedMessage, NestedMessage};
use derive_more::{Error, From, Display};

#[derive(Error, From, Display, Debug)]
pub enum AppSTARError {
  #[display(fmt = "failed to decode base64")]
  Base64(base64::DecodeError),
  #[display(fmt = "failed to decode bincode")]
  Bincode(bincode::Error)
}

pub fn parse_message(record: &str) -> Result<NestedMessage, AppSTARError> {
  let bincode_msg = base64::decode(record)?;
  let smsg: SerializableNestedMessage = bincode::deserialize(&bincode_msg)?;
  Ok(NestedMessage::from(smsg))
}

