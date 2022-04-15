use std::env;
use rand::random;
use rusoto_s3::{S3, S3Client, PutObjectRequest, PutObjectError};
use rusoto_core::{Region, RusotoError, ByteStream};
use derive_more::{From, Error, Display};
use chrono::prelude::Utc;

const S3_ENDPOINT_ENV_VAR: &str = "S3_ENDPOINT";
const OUTPUT_S3_BUCKET_ENV_KEY: &str = "S3_OUTPUT_BUCKET";
const DEFAULT_OUTPUT_BUCKET_NAME: &str = "p3a-star-recovered";

#[derive(From, Error, Display, Debug)]
pub enum DataLakeError {
  #[display(fmt = "Upload error: {}", _0)]
  Upload(RusotoError<PutObjectError>),
}

pub struct DataLake {
  s3: S3Client,
  bucket_name: String
}

impl DataLake {
  pub fn new() -> Self {
    let region = match env::var(S3_ENDPOINT_ENV_VAR) {
      Ok(endpoint) => {
        Region::Custom {
          name: "us-west-2".to_string(),
          endpoint
        }
      },
      Err(_) => Default::default()
    };
    Self {
      s3: S3Client::new(region),
      bucket_name: env::var(
        OUTPUT_S3_BUCKET_ENV_KEY
      ).unwrap_or(DEFAULT_OUTPUT_BUCKET_NAME.to_string())
    }
  }

  pub async fn store(&self, contents: &str) -> Result<(), DataLakeError> {
    let rand_key: u64 = random();
    let full_key = format!(
      "{}/{}.jsonl",
      Utc::today().naive_utc().to_string(),
      hex::encode(rand_key.to_le_bytes())
    );
    let contents = contents.as_bytes().to_vec();
    self.s3.put_object(PutObjectRequest {
      body: Some(ByteStream::from(contents)),
      bucket: self.bucket_name.clone(),
      key: full_key,
      ..Default::default()
    }).await?;
    Ok(())
  }
}
