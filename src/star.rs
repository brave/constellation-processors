use nested_sta_rs::api::{SerializableNestedMessage, NestedMessage};
use derive_more::{Error, From, Display};

#[derive(Error, From, Display, Debug)]
pub enum AppSTARError {
  #[display(fmt = "failed to decode base64")]
  Base64(base64::DecodeError),
  #[display(fmt = "failed to decode bincode")]
  Bincode(bincode::Error)
}

pub struct ParsedMessageData {
  pub msg: NestedMessage,
  pub bincode_msg: Vec<u8>
}

pub fn parse_message(record: &str) -> Result<ParsedMessageData, AppSTARError> {
  let bincode_msg = base64::decode(record)?;
  let smsg: SerializableNestedMessage = bincode::deserialize(&bincode_msg)?;
  Ok(ParsedMessageData {
    msg: NestedMessage::from(smsg),
    bincode_msg
  })
}

pub fn serialize_message(message: NestedMessage) -> Result<String, AppSTARError> {
  let smsg = SerializableNestedMessage::from(message);
  let bincode_msg = bincode::serialize(&smsg)?;
  Ok(base64::encode(&bincode_msg))
}

