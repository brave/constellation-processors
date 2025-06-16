use aws_config::SdkConfig;
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::{
  http_request::{self, SignableBody, SignableRequest, SignatureLocation, SigningSettings},
  sign::v4,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use std::{
  error::Error,
  thread,
  time::{Duration as StdDuration, SystemTime},
};
use time::{macros::format_description, Duration, UtcDateTime};
use tokio::runtime::Runtime;
use url::Url;

const ACTION_TYPE: &str = "Action";
const ACTION_NAME: &str = "kafka-cluster:Connect";
const SIGNING_NAME: &str = "kafka-cluster";
const USER_AGENT_KEY: &str = "User-Agent";
const DATE_QUERY_KEY: &str = "X-Amz-Date";
const EXPIRES_QUERY_KEY: &str = "X-Amz-Expires";
const DEFAULT_EXPIRY_SECONDS: u64 = 900;

const USER_AGENT_VALUE: &str = "constellation-processors";

type IAMResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone)]
pub struct TokenInfo {
  pub token: String,
  pub expiration_time: UtcDateTime,
}
pub struct MSKIAMAuthManager {
  token_info: Option<TokenInfo>,
}

impl MSKIAMAuthManager {
  pub fn new() -> Self {
    Self { token_info: None }
  }

  pub fn get_auth_token(&mut self) -> IAMResult<TokenInfo> {
    if let Some(token_info) = &self.token_info {
      if token_info.expiration_time > UtcDateTime::now() {
        return Ok(token_info.clone());
      }
    }

    let token_info = thread::spawn(|| {
      Runtime::new()
        .unwrap()
        .block_on(generate_auth_token_async())
    })
    .join()
    .unwrap()?;
    self.token_info = Some(token_info.clone());
    Ok(token_info)
  }
}

async fn generate_auth_token_async() -> IAMResult<TokenInfo> {
  debug!("Generating MSK IAM auth token");
  let config = aws_config::from_env().load().await;

  let mut url = build_request_url(&config)?;

  sign_request_url(&mut url, &config).await?;

  let expiration_time = get_expiration_time(&url)?;

  url
    .query_pairs_mut()
    .append_pair(USER_AGENT_KEY, USER_AGENT_VALUE);

  let encoded = URL_SAFE_NO_PAD.encode(url.as_str().as_bytes());

  Ok(TokenInfo {
    token: encoded,
    expiration_time,
  })
}

fn build_request_url(config: &SdkConfig) -> IAMResult<Url> {
  let endpoint_url = format!(
    "https://kafka.{}.amazonaws.com/",
    config
      .region()
      .ok_or_else(|| "AWS region is not set")?
      .to_string()
  );
  let mut url = Url::parse(&endpoint_url)?;

  {
    let mut query_pairs = url.query_pairs_mut();
    query_pairs.append_pair(ACTION_TYPE, ACTION_NAME);
  }

  Ok(url)
}

async fn sign_request_url(url: &mut Url, config: &SdkConfig) -> IAMResult<()> {
  let credentials_provider = config
    .credentials_provider()
    .ok_or_else(|| "AWS credentials provider is not set")?;
  let credentials = credentials_provider.provide_credentials().await?;

  let signable_request = SignableRequest::new(
    "GET",
    url.as_str(),
    std::iter::empty(),
    SignableBody::Bytes(&[]),
  )?;

  let identity = credentials.into();
  let region = config
    .region()
    .ok_or_else(|| "AWS region is not set")?
    .to_string();
  let mut signing_settings = SigningSettings::default();
  signing_settings.signature_location = SignatureLocation::QueryParams;
  signing_settings.expires_in = Some(StdDuration::from_secs(DEFAULT_EXPIRY_SECONDS));
  let signing_params = v4::SigningParams::builder()
    .identity(&identity)
    .region(&region)
    .name(SIGNING_NAME)
    .time(SystemTime::now())
    .settings(signing_settings)
    .build()?
    .into();

  let signing_output = http_request::sign(signable_request, &signing_params)?;

  for (key, value) in signing_output.output().params() {
    url.query_pairs_mut().append_pair(key, value);
  }

  Ok(())
}

fn get_expiration_time(url: &Url) -> IAMResult<UtcDateTime> {
  let date_str = url
    .query_pairs()
    .find_map(|(k, v)| {
      if k == DATE_QUERY_KEY {
        Some(v.to_string())
      } else {
        None
      }
    })
    .ok_or_else(|| "failed to find AWS signed date parameter")?;

  let date_format_description = format_description!("[year][month][day]T[hour][minute][second]Z");
  let date = UtcDateTime::parse(&date_str, date_format_description)?;

  let expiry_duration_seconds = url
    .query_pairs()
    .find_map(|(k, v)| {
      if k == EXPIRES_QUERY_KEY {
        Some(v.to_string())
      } else {
        None
      }
    })
    .ok_or_else(|| "failed to find AWS signed expiry parameter")?
    .parse::<i64>()?;

  let expiry_duration = Duration::seconds(expiry_duration_seconds);
  let expiry_time = date + expiry_duration;

  Ok(expiry_time)
}
