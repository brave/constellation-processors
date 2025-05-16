use aws_sdk_s3::{
  config::http::HttpResponse, error::SdkError, operation::put_object::PutObjectError,
  primitives::ByteStream, Client,
};
use derive_more::{Display, Error, From};
use rand::random;
use std::env;
use time::OffsetDateTime;

const S3_ENDPOINT_ENV_VAR: &str = "S3_ENDPOINT";
const OUTPUT_S3_BUCKET_ENV_KEY: &str = "S3_OUTPUT_BUCKET";
const DEFAULT_OUTPUT_BUCKET_NAME: &str = "p3a-star-recovered";

#[derive(From, Error, Display, Debug)]
pub enum DataLakeError {
  #[display(fmt = "Upload error: {}", _0)]
  Upload(SdkError<PutObjectError, HttpResponse>),
}

pub struct DataLake {
  s3: Client,
  bucket_name: String,
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

    Self {
      s3,
      bucket_name: env::var(OUTPUT_S3_BUCKET_ENV_KEY)
        .unwrap_or(DEFAULT_OUTPUT_BUCKET_NAME.to_string()),
    }
  }

  pub async fn store(&self, channel_name: &str, contents: &str) -> Result<(), DataLakeError> {
    let rand_key: u64 = random();
    let full_key = format!(
      "{}/{}/{}.jsonl",
      OffsetDateTime::now_utc().date(),
      channel_name,
      hex::encode(rand_key.to_le_bytes())
    );
    let contents = contents.as_bytes().to_vec();
    self
      .s3
      .put_object()
      .acl(aws_sdk_s3::types::ObjectCannedAcl::BucketOwnerFullControl)
      .body(ByteStream::from(contents))
      .bucket(&self.bucket_name)
      .key(full_key)
      .send()
      .await?;
    Ok(())
  }
}
