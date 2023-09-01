use std::{collections::HashMap, env};

pub fn get_data_channel_map_from_env(env_key: &str, default: &str) -> HashMap<String, String> {
  let env_encoded = env::var(env_key).unwrap_or_else(|_| default.to_string());

  let mut map = HashMap::new();
  for encoded_channel in env_encoded.split(",") {
    let mut encoded_channel_split = encoded_channel.split("=");
    let channel_name = encoded_channel_split
      .next()
      .unwrap_or_else(|| {
        panic!(
          "should be able to parse name from {} env channel setting entry: {}",
          env_key, encoded_channel
        )
      })
      .to_string();
    let channel_value = encoded_channel_split
      .next()
      .unwrap_or_else(|| {
        panic!(
          "should be able to parse value from {} env channel setting entry: {}",
          env_key, encoded_channel
        )
      })
      .to_string();
    map.insert(channel_name, channel_value);
  }

  map
}

pub fn get_data_channel_value_from_env(env_key: &str, default: &str, channel_name: &str) -> String {
  get_data_channel_map_from_env(env_key, default)
    .get(channel_name)
    .cloned()
    .unwrap_or_else(|| {
      panic!(
        "{} env channel setting must contain entry for channel: {}",
        env_key, channel_name
      );
    })
}
