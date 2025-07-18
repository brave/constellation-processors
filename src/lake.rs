use aws_sdk_s3::{
  config::http::HttpResponse, error::SdkError, operation::put_object::PutObjectError,
  primitives::ByteStream, Client,
};
use derive_more::{Display, Error, From};
use rand::random;
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use time::OffsetDateTime;

const S3_ENDPOINT_ENV_VAR: &str = "S3_ENDPOINT";
const OUTPUT_S3_BUCKET_ENV_KEY: &str = "S3_OUTPUT_BUCKET";
const INCLUDE_YMD_IN_LAKE_ENV_KEY: &str = "INCLUDE_YMD_IN_LAKE";
const DEFAULT_OUTPUT_BUCKET_NAME: &str = "p3a-star-recovered";
const YMD_KEY: &str = "ymd";

#[derive(From, Error, Display, Debug)]
pub enum DataLakeError {
  #[display(fmt = "Upload error: {}", _0)]
  Upload(SdkError<PutObjectError, HttpResponse>),
  #[display(fmt = "JSON parsing error: {}", _0)]
  JsonParsing(serde_json::Error),
}

pub struct DataLake {
  s3: Client,
  bucket_name: String,
  include_ymd_channels: HashSet<String>,
}

impl DataLake {
  pub async fn new() -> Self {
    let endpoint = env::var(S3_ENDPOINT_ENV_VAR).ok();
    let aws_config = aws_config::from_env().load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(endpoint) = endpoint {
      s3_config_builder = s3_config_builder
        .endpoint_url(endpoint)
        .force_path_style(true);
    }
    let s3_config = s3_config_builder.build();

    let s3 = Client::from_conf(s3_config);

    let include_ymd_channels = env::var(INCLUDE_YMD_IN_LAKE_ENV_KEY)
      .map(|channels| {
        channels
          .split(',')
          .map(|s| s.trim().to_string())
          .filter(|s| !s.is_empty())
          .collect()
      })
      .unwrap_or_default();

    Self {
      s3,
      bucket_name: env::var(OUTPUT_S3_BUCKET_ENV_KEY)
        .unwrap_or(DEFAULT_OUTPUT_BUCKET_NAME.to_string()),
      include_ymd_channels,
    }
  }

  pub async fn store(&self, channel_name: &str, contents: &str) -> Result<(), DataLakeError> {
    let rand_key: u64 = random();
    let current_date = OffsetDateTime::now_utc().date();
    let full_key = format!(
      "{}/{}/{}.jsonl",
      current_date,
      channel_name,
      hex::encode(rand_key.to_le_bytes())
    );

    let processed_contents = if self.include_ymd_channels.contains(channel_name) {
      self.add_ymd_to_jsonl(contents, &current_date)?
    } else {
      contents.as_bytes().to_vec()
    };

    self
      .s3
      .put_object()
      .acl(aws_sdk_s3::types::ObjectCannedAcl::BucketOwnerFullControl)
      .body(ByteStream::from(processed_contents))
      .bucket(&self.bucket_name)
      .key(full_key)
      .send()
      .await?;
    Ok(())
  }

  fn add_ymd_to_jsonl(
    &self,
    contents: &str,
    current_date: &time::Date,
  ) -> Result<Vec<u8>, DataLakeError> {
    let mut processed_lines = Vec::new();

    for line in contents.lines() {
      if line.trim().is_empty() {
        continue;
      }

      let mut json_value: Value = serde_json::from_str(line)?;

      if let Value::Object(ref mut map) = json_value {
        map.insert(YMD_KEY.to_string(), current_date.to_string().into());
      }

      processed_lines.push(serde_json::to_string(&json_value)?);
    }

    Ok(processed_lines.join("\n").as_bytes().to_vec())
  }
}
