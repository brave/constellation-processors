use derive_more::{Display, Error, From};
use nested_sta_rs::api::{
  key_recover, recover, NestedMessage, PartialMeasurement, SerializableNestedMessage,
};
use nested_sta_rs::errors::NestedSTARError;
use std::cmp::min;
use std::str::{from_utf8, Utf8Error};

#[derive(Error, From, Display, Debug)]
pub enum AppSTARError {
  #[display(fmt = "failed to decode base64")]
  Base64(base64::DecodeError),
  #[display(fmt = "failed to decode bincode")]
  Bincode(bincode::Error),
  #[display(fmt = "failed to decode utf8")]
  Utf8(Utf8Error),
  #[display(fmt = "failed to split measurement by delimiter")]
  Delimiter,
  #[display(fmt = "failed to recover messages")]
  Recovery(NestedSTARError),
}

pub struct ParsedMessageData {
  pub msg: NestedMessage,
  pub bincode_msg: Vec<u8>,
}

pub struct MsgRecoveryInfo {
  pub measurement: (String, String),
  pub next_layer_messages: Option<Vec<NestedMessage>>,
}

pub fn parse_message(record: &str) -> Result<ParsedMessageData, AppSTARError> {
  let bincode_msg = base64::decode(record)?;
  Ok(ParsedMessageData {
    msg: parse_message_bincode(&bincode_msg)?,
    bincode_msg,
  })
}

pub fn parse_message_bincode(bincode_msg: &[u8]) -> Result<NestedMessage, AppSTARError> {
  let smsg: SerializableNestedMessage = bincode::deserialize(bincode_msg)?;
  Ok(NestedMessage::from(smsg))
}

pub fn serialize_message_bincode(message: NestedMessage) -> Result<Vec<u8>, AppSTARError> {
  let smsg = SerializableNestedMessage::from(message);
  Ok(bincode::serialize(&smsg)?)
}

fn get_measurement_contents(m: &PartialMeasurement) -> Result<(String, String), AppSTARError> {
  let mstr = from_utf8(m.measurement.0.get(0).unwrap().as_slice())?;
  let mstr_spl: Vec<&str> = mstr.split('|').collect();
  if mstr_spl.len() != 2 {
    Err(AppSTARError::Delimiter)
  } else {
    Ok((
      mstr_spl[0].to_string(),
      mstr_spl[1].trim_matches(char::from(0)).to_string(),
    ))
  }
}

pub fn recover_key(
  messages: &[NestedMessage],
  epoch_tag: u8,
  k_threshold: usize,
) -> Result<Vec<u8>, AppSTARError> {
  let msgs_to_use = min(k_threshold + (k_threshold / 3), messages.len());
  let unencrypted_layers: Vec<_> = messages[..msgs_to_use]
    .iter()
    .map(|v| &v.unencrypted_layer)
    .collect();

  Ok(key_recover(&unencrypted_layers, epoch_tag)?)
}

pub fn recover_msgs(
  messages: Vec<NestedMessage>,
  key: &[u8],
) -> Result<MsgRecoveryInfo, AppSTARError> {
  let unencrypted_layers: Vec<_> = messages.iter().map(|v| &v.unencrypted_layer).collect();

  let pms = recover(&unencrypted_layers, key)?;
  let has_next_layer = pms.iter().any(|v| v.get_next_layer_key().is_some());
  let next_layer_messages = if has_next_layer {
    Some(
      messages
        .into_iter()
        .zip(pms.iter())
        .filter(|(_, pm)| pm.get_next_layer_key().as_ref().is_some())
        .map(|(mut msg, pm)| {
          let layer_key = pm.get_next_layer_key().as_ref().unwrap();
          msg.decrypt_next_layer(layer_key);
          msg
        })
        .collect(),
    )
  } else {
    None
  };

  Ok(MsgRecoveryInfo {
    measurement: get_measurement_contents(&pms[0])?,
    next_layer_messages,
  })
}
