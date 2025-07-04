use reqwest::{Client, StatusCode};
use std::{
  env,
  time::{Duration, Instant},
};

use crate::util::parse_env_var;

use super::AggregatorError;

const IMDS_ENDPOINT_ENV_KEY: &str = "IMDS_ENDPOINT";
const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";
const CHECK_SPOT_TERMINATION_ENV_KEY: &str = "CHECK_SPOT_TERMINATION";
const DEFAULT_CHECK_SPOT_TERMINATION: &str = "false";
const SPOT_EVICTION_ENDPOINT_ENV_KEY: &str = "SPOT_EVICTION_ENDPOINT";

const IMDS_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const IMDS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const IMDS_TOKEN_LIFETIME: Duration = Duration::from_secs(6 * 60 * 60); // 6 hours
const IMDS_TOKEN_REFRESH_THRESHOLD: Duration = Duration::from_secs(5 * 60 * 60); // 5 hours

const AWS_TOKEN_TTL_HEADER: &str = "X-aws-ec2-metadata-token-ttl-seconds";
const AWS_TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";

const IMDS_TOKEN_PATH: &str = "/latest/api/token";
const IMDS_TERMINATION_PATH: &str = "/latest/meta-data/spot/termination-time";

async fn get_imds_token(client: &Client, endpoint: &str) -> Result<String, AggregatorError> {
  let token_endpoint = format!("{}{}", endpoint, IMDS_TOKEN_PATH);
  match client
    .put(token_endpoint)
    .header(
      AWS_TOKEN_TTL_HEADER,
      IMDS_TOKEN_LIFETIME.as_secs().to_string(),
    )
    .send()
    .await
  {
    Ok(response) => {
      if response.status().is_success() {
        Ok(response.text().await.map_err(|e| {
          error!("Failed to get token from IMDS response: {}", e);
          AggregatorError::IMDSRequestFail
        })?)
      } else {
        error!(
          "IMDS token endpoint returned unexpected status code: {}",
          response.status()
        );
        Err(AggregatorError::IMDSRequestFail)
      }
    }
    Err(e) => {
      error!("Error generating IMDS token: {}", e);
      Err(AggregatorError::IMDSRequestFail)
    }
  }
}

pub async fn check_spot_termination_status(
  loop_enabled: bool,
  channel_name: &str,
) -> Result<(), AggregatorError> {
  let check_enabled: bool = parse_env_var(
    CHECK_SPOT_TERMINATION_ENV_KEY,
    DEFAULT_CHECK_SPOT_TERMINATION,
  );
  let client = Client::builder()
    .timeout(IMDS_REQUEST_TIMEOUT)
    .build()
    .unwrap();

  let endpoint: String = parse_env_var(IMDS_ENDPOINT_ENV_KEY, DEFAULT_IMDS_ENDPOINT);
  let term_time_endpoint = format!("{}{}", endpoint, IMDS_TERMINATION_PATH);

  let mut token: Option<String> = None;
  let mut token_age = Instant::now();

  loop {
    if check_enabled {
      if token.is_none() || Instant::now().duration_since(token_age) > IMDS_TOKEN_REFRESH_THRESHOLD
      {
        token = Some(get_imds_token(&client, &endpoint).await?);
        token_age = Instant::now();
      }

      match client
        .get(&term_time_endpoint)
        .header(AWS_TOKEN_HEADER, token.as_ref().unwrap())
        .send()
        .await
      {
        Ok(response) => {
          if response.status().is_success() {
            warn!("Spot instance scheduled for termination, stopping");

            // Send request to eviction endpoint if configured
            if let Some(eviction_url) = env::var(SPOT_EVICTION_ENDPOINT_ENV_KEY).ok() {
              let eviction_url = format!("{}/{}", eviction_url, channel_name);
              info!("Sending spot eviction notification to: {}", eviction_url);
              let _ = client.post(eviction_url).send().await;
            }

            return Err(AggregatorError::SpotTermination);
          } else if response.status() != StatusCode::NOT_FOUND {
            error!(
              "Spot termination endpoint returned unexpected status code: {}",
              response.status()
            );
            return Err(AggregatorError::IMDSRequestFail);
          }
        }
        Err(e) => {
          error!("Error checking spot termination status: {}", e);
          return Err(AggregatorError::IMDSRequestFail);
        }
      }
    }

    if !loop_enabled {
      return Ok(());
    }
    tokio::time::sleep(IMDS_CHECK_INTERVAL).await;
  }
}
