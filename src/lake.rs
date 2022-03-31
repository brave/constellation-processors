use std::env;
use std::path::Path;
use rusoto_s3::{S3, S3Client, PutObjectRequest, PutObjectError};
use rusoto_core::{Region, RusotoError, ByteStream};
use tokio::{fs::File, io};
use tokio::io::AsyncReadExt;
use derive_more::{From, Error, Display};

const S3_ENDPOINT_ENV_VAR: &str = "S3_ENDPOINT";
const PARTITIONED_S3_BUCKET_NAME: &str = "S3_PARTITIONED_BUCKET";
const FINAL_S3_BUCKET_NAME: &str = "S3_FINAL_BUCKET";
const DEFAULT_PARTITIONED_BUCKET_NAME: &str = "p3a-partitioned";
const DEFAULT_FINAL_BUCKET_NAME: &str = "p3a-final";

#[derive(From, Error, Display, Debug)]
pub enum DataLakeError {
  #[display(fmt = "IO error: {}", _0)]
  IO(io::Error),
  #[display(fmt = "Upload error: {}", _0)]
  Upload(RusotoError<PutObjectError>)
}

pub struct DataLake {
  s3: S3Client,
  partitioned_bucket_name: String,
  final_bucket_name: String
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
      partitioned_bucket_name: env::var(
        PARTITIONED_S3_BUCKET_NAME
      ).unwrap_or(DEFAULT_PARTITIONED_BUCKET_NAME.to_string()),
      final_bucket_name: env::var(
        FINAL_S3_BUCKET_NAME
      ).unwrap_or(DEFAULT_FINAL_BUCKET_NAME.to_string())
    }
  }

  pub async fn store_file(&self, file_loc: &Path, dest_path: &str,
    in_partitioned_bucket: bool) -> Result<(), DataLakeError> {
    let mut file = File::open(file_loc).await?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;

    let bucket = if in_partitioned_bucket {
      self.partitioned_bucket_name.clone()
    } else {
      self.final_bucket_name.clone()
    };

    self.s3.put_object(PutObjectRequest {
      body: Some(ByteStream::from(contents)),
      bucket,
      key: dest_path.to_string(),
      ..Default::default()
    }).await?;
    Ok(())
  }
}
