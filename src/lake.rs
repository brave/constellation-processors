use derive_more::{Display, Error, From};
use rand::random;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_s3::{PutObjectError, PutObjectRequest, S3Client, S3};
use std::env;
use time::OffsetDateTime;

const S3_ENDPOINT_ENV_VAR: &str = "S3_ENDPOINT";
const OUTPUT_S3_BUCKET_ENV_KEY: &str = "S3_OUTPUT_BUCKET";
const WEB_IDENTITY_ENV_VAR: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const DEFAULT_OUTPUT_BUCKET_NAME: &str = "p3a-star-recovered";

#[derive(From, Error, Display, Debug)]
pub enum DataLakeError {
  #[display(fmt = "Upload error: {}", _0)]
  Upload(RusotoError<PutObjectError>),
}

pub struct DataLake {
  s3: S3Client,
  bucket_name: String,
}

impl DataLake {
  pub fn new() -> Self {
    let region = match env::var(S3_ENDPOINT_ENV_VAR) {
      Ok(endpoint) => Region::Custom {
        name: "us-west-2".to_string(),
        endpoint,
      },
      Err(_) => Default::default(),
    };
    let s3 = if env::var(WEB_IDENTITY_ENV_VAR).is_ok() {
      let provider = rusoto_credential::AutoRefreshingProvider::new(
        rusoto_sts::WebIdentityProvider::from_k8s_env(),
      )
      .unwrap();
      S3Client::new_with(rusoto_core::HttpClient::new().unwrap(), provider, region)
    } else {
      S3Client::new(region)
    };

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
      channel_name,
      OffsetDateTime::now_utc().date(),
      hex::encode(rand_key.to_le_bytes())
    );
    let contents = contents.as_bytes().to_vec();
    self
      .s3
      .put_object(PutObjectRequest {
        acl: Some("bucket-owner-full-control".to_string()),
        body: Some(ByteStream::from(contents)),
        bucket: self.bucket_name.clone(),
        key: full_key,
        ..Default::default()
      })
      .await?;
    Ok(())
  }
}
