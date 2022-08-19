use serde::Deserialize;
use std::env;

const FIRST_EPOCH: u8 = 0u8;
const LAST_EPOCH: u8 = 255u8;

const EPOCH_LIFETIME_WEEKS: usize = 4;

const RANDOMNESS_HOST_ENV_KEY: &str = "RANDOMNESS_HOST";
const DISABLE_RANDOMNESS_TLS_VALIDATION_ENV_KEY: &str = "DISABLE_RANDOMNESS_TLS_VALIDATION";
const TEST_EPOCH_ENV_KEY: &str = "TEST_EPOCH";

#[derive(Deserialize)]
struct RandomnessInfoResponse {
  #[serde(rename = "currentEpoch")]
  current_epoch: u8,
}

pub async fn get_current_epoch() -> u8 {
  if let Ok(epoch_val) = env::var(TEST_EPOCH_ENV_KEY) {
    return epoch_val.parse::<u8>().unwrap();
  }
  let mut client_builder = reqwest::ClientBuilder::new();
  if env::var(DISABLE_RANDOMNESS_TLS_VALIDATION_ENV_KEY).unwrap_or("".to_string()) == "true" {
    client_builder = client_builder.danger_accept_invalid_certs(true);
  }
  let client = client_builder.build().unwrap();
  let randomness_info_url = reqwest::Url::parse(
    &env::var(RANDOMNESS_HOST_ENV_KEY)
      .expect(&format!("{} env var not defined", RANDOMNESS_HOST_ENV_KEY)),
  )
  .unwrap()
  .join("info")
  .unwrap();
  let resp: RandomnessInfoResponse = client
    .get(randomness_info_url)
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
  resp.current_epoch
}

pub fn is_epoch_expired(epoch: u8, mut current_epoch: u8) -> bool {
  let mut diff = 0;
  if !(FIRST_EPOCH..=LAST_EPOCH).contains(&current_epoch) {
    return true;
  }
  while current_epoch != epoch {
    if current_epoch == FIRST_EPOCH {
      current_epoch = LAST_EPOCH;
    } else {
      current_epoch -= 1;
    }

    diff += 1;
  }
  diff >= EPOCH_LIFETIME_WEEKS
}
