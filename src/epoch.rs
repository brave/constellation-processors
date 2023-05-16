use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use std::env;

const FIRST_EPOCH: u8 = 0u8;
const LAST_EPOCH: u8 = 255u8;

const EPOCH_LIFETIME_WEEKS: usize = 4;

const RANDOMNESS_HOST_ENV_KEY: &str = "RANDOMNESS_HOST";
const DISABLE_RANDOMNESS_TLS_VALIDATION_ENV_KEY: &str = "DISABLE_RANDOMNESS_TLS_VALIDATION";

#[derive(Deserialize)]
pub struct CurrentEpochInfo {
  #[serde(rename = "currentEpoch")]
  pub epoch: u8,
  #[serde(rename = "nextEpochTime")]
  pub next_epoch_time: DateTime<Utc>,
}

impl CurrentEpochInfo {
  pub async fn retrieve() -> Self {
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
    client
      .get(randomness_info_url)
      .send()
      .await
      .expect("should be able to retrieve epoch info from randomness server")
      .json()
      .await
      .expect("should be able to parse info json from randomness server")
  }

  pub fn test_info(test_epoch: u8, epoch_duration: Duration) -> Self {
    Self {
      epoch: test_epoch,
      next_epoch_time: Utc::now() + epoch_duration,
    }
  }
}

pub fn is_epoch_expired(config: &EpochConfig, epoch: u8) -> bool {
  let mut diff = 0;
  let mut current_epoch = config.current_epoch.epoch;
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

pub struct EpochConfig {
  pub current_epoch: CurrentEpochInfo,
  pub epoch_date_field_name: String,
  pub epoch_length: Duration,
}

pub fn get_epoch_survey_date(config: &EpochConfig, epoch: u8) -> String {
  let current_epoch_start = config.current_epoch.next_epoch_time - config.epoch_length;
  let epoch_delta = config.current_epoch.epoch.wrapping_sub(epoch);
  let epoch_start_date = current_epoch_start - (config.epoch_length * epoch_delta as i32);
  epoch_start_date.date_naive().to_string()
}

#[cfg(test)]
mod tests {
  use super::{get_epoch_survey_date, CurrentEpochInfo, EpochConfig};
  use chrono::{DateTime, Duration};

  #[test]
  fn survey_date() {
    let epoch_length = Duration::weeks(1);
    let epoch_config = EpochConfig {
      current_epoch: CurrentEpochInfo {
        epoch: 2,
        next_epoch_time: DateTime::parse_from_rfc3339("2023-05-08T13:00:00.000Z")
          .unwrap()
          .into(),
      },
      epoch_date_field_name: "wos".to_string(),
      epoch_length,
    };

    assert_eq!(get_epoch_survey_date(&epoch_config, 2), "2023-05-01");
    assert_eq!(get_epoch_survey_date(&epoch_config, 1), "2023-04-24");
    assert_eq!(get_epoch_survey_date(&epoch_config, 0), "2023-04-17");
    assert_eq!(get_epoch_survey_date(&epoch_config, 255), "2023-04-10");
    assert_eq!(get_epoch_survey_date(&epoch_config, 254), "2023-04-03");
  }
}
