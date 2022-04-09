use std::env;
use std::path::Path;
use std::pin::Pin;
use rusoto_s3::{S3, S3Client, PutObjectRequest,
  PutObjectError, ListObjectsV2Request, ListObjectsV2Error,
  GetObjectRequest, GetObjectError};
use rusoto_core::{Region, RusotoError, ByteStream};
use tokio::{fs::File, io};
use tokio::io::AsyncReadExt;
use futures_core::stream::Stream;
use async_stream::stream;
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
  Upload(RusotoError<PutObjectError>),
  #[display(fmt = "Object list error: {}", _0)]
  List(RusotoError<ListObjectsV2Error>),
  #[display(fmt = "Download error: {}", _0)]
  Download(RusotoError<GetObjectError>)
}

pub struct DataLake {
  s3: S3Client,
  partitioned_bucket_name: String,
  final_bucket_name: String
}

#[derive(Debug)]
pub struct LakeFile {
  pub key: String,
  pub size: usize
}

pub struct LakeListing {
  pub prefixes: Vec<String>,
  pub files: Vec<LakeFile>
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

  pub fn list_partitioned_files<'a>(&'a self,
    prefix: Option<String>) -> Pin<Box<dyn Stream<Item = Result<LakeListing, DataLakeError>> + 'a>> {
    Box::pin(stream! {
      let mut continuation_token = None;
      loop {
        let output = self.s3.list_objects_v2(ListObjectsV2Request {
          bucket: self.partitioned_bucket_name.clone(),
          continuation_token: continuation_token.clone(),
          prefix: prefix.clone(),
          delimiter: Some("/".to_string()),
          ..Default::default()
        }).await;
        match output {
          Err(e) => yield Err(DataLakeError::from(e)),
          Ok(output) => {
            continuation_token = output.continuation_token;
            yield Ok(LakeListing {
              files: output.contents.unwrap_or_default().into_iter().map(|v| LakeFile {
                key: v.key.unwrap_or_default(),
                size: v.size.unwrap_or_default() as usize
              }).collect(),
              prefixes: output.common_prefixes.unwrap_or_default().into_iter().map(|v| {
                v.prefix.unwrap_or_default()
              }).collect(),
            });
          }
        };
        if continuation_token.is_none() {
          break;
        }
      }
    })
  }

  pub async fn download_file_to_string(&self, key: &str,
    in_partitioned_bucket: bool) -> Result<String, DataLakeError> {
    let bucket = if in_partitioned_bucket {
      self.partitioned_bucket_name.clone()
    } else {
      self.final_bucket_name.clone()
    };

    let resp = self.s3.get_object(GetObjectRequest {
      bucket,
      key: key.to_string(),
      ..Default::default()
    }).await?;

    let mut output = String::new();
    resp.body.unwrap().into_async_read().read_to_string(&mut output).await?;

    Ok(output)
  }
}
