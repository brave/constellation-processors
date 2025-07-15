use anyhow::{anyhow, Result};
use log::warn;
use reqwest::Client;
use serde_json::json;
use std::env;

const SLACK_AUTH_TOKEN_ENV_KEY: &str = "SLACK_AUTH_TOKEN";
const SLACK_CHANNEL_ENV_KEY: &str = "SLACK_CHANNEL";
const SLACK_API_URL: &str = "https://slack.com/api/chat.postMessage";

pub struct SlackClient {
  client: Client,
  auth_token: Option<String>,
  channel: Option<String>,
}

impl SlackClient {
  pub fn new() -> Self {
    let auth_token = env::var(SLACK_AUTH_TOKEN_ENV_KEY).ok();
    let channel = env::var(SLACK_CHANNEL_ENV_KEY).ok();

    if auth_token.is_none() {
      warn!("SLACK_AUTH_TOKEN environment variable not set. Slack notifications will be disabled.");
    }

    if channel.is_none() {
      warn!("SLACK_CHANNEL environment variable not set. Slack notifications will be disabled.");
    }

    Self {
      client: Client::new(),
      auth_token,
      channel,
    }
  }

  pub async fn send_message(&self, message: &str) -> Result<()> {
    let (token, channel) = match (&self.auth_token, &self.channel) {
      (Some(token), Some(channel)) => (token, channel),
      _ => {
        warn!(
          "Slack configuration not available. Skipping message: {}",
          message
        );
        return Ok(());
      }
    };

    let payload = json!({
      "channel": channel,
      "text": message
    });

    let response = self
      .client
      .post(SLACK_API_URL)
      .bearer_auth(token)
      .json(&payload)
      .send()
      .await?;

    if response.status().is_success() {
      let response_json: serde_json::Value = response.json().await?;

      if !response_json
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
      {
        let error_msg = response_json
          .get("error")
          .and_then(|v| v.as_str())
          .unwrap_or("unknown error");
        return Err(anyhow!("Slack API error: {}", error_msg));
      }
    } else {
      return Err(anyhow!("HTTP error: {}", response.status()));
    }

    Ok(())
  }
}
