use calendar_duration::CalendarDuration;
use serde::Deserialize;
use std::env;
use time::OffsetDateTime;

use crate::channel::get_data_channel_value_from_env;

const FIRST_EPOCH: u8 = 0u8;
const LAST_EPOCH: u8 = 255u8;

const EPOCH_LIFETIMES_ENV_KEY: &str = "EPOCH_LIFETIMES";
const DEFAULT_EPOCH_LIFETIMES: &str = "typical=3";

const RANDOMNESS_HOST_ENV_KEY: &str = "RANDOMNESS_HOST";
const DISABLE_RANDOMNESS_TLS_VALIDATION_ENV_KEY: &str = "DISABLE_RANDOMNESS_TLS_VALIDATION";

const EPOCH_LENGTHS_ENV_KEY: &str = "EPOCH_LENGTHS";
const DEFAULT_EPOCH_LENGTHS: &str = "typical=1w";
const EPOCH_DATE_FIELD_NAMES_ENV_KEY: &str = "EPOCH_DATE_FIELD_NAMES";
const DEFAULT_EPOCH_DATE_FIELD_NAMES: &str = "typical=wos";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentEpochInfo {
  pub epoch: u8,
  pub next_epoch_time: OffsetDateTime,
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

  pub fn test_info(test_epoch: u8, epoch_duration: CalendarDuration) -> Self {
    Self {
      epoch: test_epoch,
      next_epoch_time: OffsetDateTime::now_utc() + epoch_duration,
    }
  }
}

pub struct EpochConfig {
  pub current_epoch: CurrentEpochInfo,
  pub epoch_date_field_name: String,
  pub epoch_length: CalendarDuration,
  pub epoch_lifetime_count: usize,
}

impl EpochConfig {
  pub async fn new(test_epoch: Option<u8>, channel_name: &str) -> Self {
    let epoch_length = CalendarDuration::from(
      get_data_channel_value_from_env(EPOCH_LENGTHS_ENV_KEY, DEFAULT_EPOCH_LENGTHS, channel_name)
        .as_str(),
    );
    let epoch_lifetime_count = get_data_channel_value_from_env(
      EPOCH_LIFETIMES_ENV_KEY,
      DEFAULT_EPOCH_LIFETIMES,
      channel_name,
    )
    .parse::<usize>()
    .expect("epoch lifetime should be an unsigned integer");
    assert!(
      !epoch_length.is_zero(),
      "epoch length for main channel should not be zero"
    );
    let epoch_date_field_name = get_data_channel_value_from_env(
      EPOCH_DATE_FIELD_NAMES_ENV_KEY,
      DEFAULT_EPOCH_DATE_FIELD_NAMES,
      channel_name,
    );
    let current_epoch = match test_epoch {
      Some(epoch) => CurrentEpochInfo::test_info(epoch, epoch_length),
      None => CurrentEpochInfo::retrieve().await,
    };
    Self {
      current_epoch,
      epoch_date_field_name,
      epoch_length,
      epoch_lifetime_count,
    }
  }

  pub fn is_epoch_expired(&self, epoch: u8) -> bool {
    let mut diff = 0;
    let mut current_epoch = self.current_epoch.epoch;
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
    diff >= self.epoch_lifetime_count
  }

  pub fn get_epoch_survey_date(&self, epoch: u8) -> String {
    let current_epoch_start = self.current_epoch.next_epoch_time - self.epoch_length;
    let epoch_delta = self.current_epoch.epoch.wrapping_sub(epoch);

    let mut epoch_start_date = current_epoch_start;
    for _ in 0..epoch_delta {
      epoch_start_date = epoch_start_date - self.epoch_length;
    }
    epoch_start_date.date().to_string()
  }
}

#[cfg(test)]
mod tests {
  use super::{CurrentEpochInfo, EpochConfig};
  use calendar_duration::CalendarDuration;
  use time::{format_description::well_known::Rfc3339, OffsetDateTime};

  fn get_epoch_config() -> EpochConfig {
    let epoch_length = CalendarDuration::from("1w");
    EpochConfig {
      current_epoch: CurrentEpochInfo {
        epoch: 2,
        next_epoch_time: OffsetDateTime::parse("2023-05-08T13:00:00.000Z", &Rfc3339)
          .unwrap()
          .into(),
      },
      epoch_date_field_name: "wos".to_string(),
      epoch_length,
      epoch_lifetime_count: 5,
    }
  }

  #[test]
  fn survey_date() {
    let epoch_config = get_epoch_config();

    assert_eq!(epoch_config.get_epoch_survey_date(2), "2023-05-01");
    assert_eq!(epoch_config.get_epoch_survey_date(1), "2023-04-24");
    assert_eq!(epoch_config.get_epoch_survey_date(0), "2023-04-17");
    assert_eq!(epoch_config.get_epoch_survey_date(255), "2023-04-10");
    assert_eq!(epoch_config.get_epoch_survey_date(254), "2023-04-03");
  }

  #[test]
  fn epoch_expiry() {
    let epoch_config = get_epoch_config();

    assert!(!epoch_config.is_epoch_expired(2));
    assert!(!epoch_config.is_epoch_expired(0));
    assert!(!epoch_config.is_epoch_expired(254));
    assert!(epoch_config.is_epoch_expired(253));
    assert!(epoch_config.is_epoch_expired(120));
  }
}
