use std::env;
use std::str::FromStr;
use tokio::fs::{OpenOptions, File, create_dir_all, remove_file};
use tokio::io::{self, AsyncWriteExt};
use std::path::Path;
use derive_more::{From, Display, Error};
use lru::LruCache;
use crate::models::MsgInfo;
use std::collections::HashMap;

const BUFFER_DIR_ENV_VAR: &str = "BUFFER_DIR";
const DEFAULT_BUFFER_DIR: &str = "buffer";
const BUFFER_SIZE_ENV_VAR: &str = "BUFFER_SIZE";
const DEFAULT_BUFFER_SIZE: usize = 15 * 1024 * 1024 * 1024;
const MAX_OPEN_FILES: usize = 256;
const BAKED_FILE_SIZE_BYTES: usize = 524288;
const UNBAKED_THRESHOLD: f64 = 0.6f64;

#[derive(Clone)]
pub struct BakedFile {
  key: String,
  size: usize
}

pub struct RecordBuffer {
  open_files: LruCache<MsgInfo, File>,
  latest_buffer_ids: HashMap<MsgInfo, String>,
  latest_buffer_sizes: HashMap<MsgInfo, usize>,
  baked_files: Vec<BakedFile>,
  total_unbaked_size: usize,
  total_baked_size: usize,
  buffer_dir: String,
  buffer_size: usize
}

#[derive(From, Error, Display, Debug)]
pub enum RecordBufferError {
  #[display(fmt = "IO error: {}", _0)]
  IO(io::Error),
  #[display(fmt = "Can't operate on unbaked file")]
  NotBaked
}

impl RecordBuffer {
  pub fn new() -> Self {
    let buffer_dir = env::var(BUFFER_DIR_ENV_VAR)
      .unwrap_or(DEFAULT_BUFFER_DIR.to_string());
    let buffer_size = match env::var(BUFFER_SIZE_ENV_VAR) {
      Ok(v) => usize::from_str(&v).expect("Buffer size must be positive integer"),
      Err(_) => DEFAULT_BUFFER_SIZE
    };
    Self {
      open_files: LruCache::new(MAX_OPEN_FILES),
      latest_buffer_ids: Default::default(),
      latest_buffer_sizes: Default::default(),
      baked_files: Default::default(),
      total_unbaked_size: 0,
      total_baked_size: 0,
      buffer_dir,
      buffer_size
    }
  }

  fn get_buffer_key(&mut self, msg_info: &MsgInfo) -> String {
    let buffer_id = self.latest_buffer_ids.entry(msg_info.clone()).or_insert_with(|| {
      let id_buf = rand::random::<[u8; 8]>();
      hex::encode(id_buf)
    });
    let msg_info_str = msg_info.to_string();
    format!("{}/{}.b64l", msg_info_str, buffer_id)
  }

  fn bake_file(&mut self, msg_info: &MsgInfo) {
    self.open_files.pop(msg_info);
    let buffer_size = *self.latest_buffer_sizes.get(msg_info).unwrap_or(&0);
    let key = self.get_buffer_key(msg_info);
    self.baked_files.push(BakedFile {
      key,
      size: buffer_size
    });
    self.latest_buffer_sizes.remove(msg_info);
    self.latest_buffer_ids.remove(msg_info);

    self.total_unbaked_size -= buffer_size;
    self.total_baked_size += buffer_size;
  }

  fn update_sizes_and_maybe_bake(&mut self, msg_info: &MsgInfo,
    added_size: usize) {
    let buffer_size = self.latest_buffer_sizes.entry(msg_info.clone()).or_insert(0);
    *buffer_size += added_size;

    self.total_unbaked_size += *buffer_size;

    if *buffer_size >= BAKED_FILE_SIZE_BYTES {
      self.bake_file(msg_info);
    }
  }

  pub async fn send_to_buffer(&mut self, msg_info: &MsgInfo,
    record: &str) -> Result<(), RecordBufferError> {
    if !self.open_files.contains(msg_info) {
      let buffer_path_str = self.get_buffer_key(msg_info);
      let path = Path::new(&self.buffer_dir).join(&buffer_path_str);
      if let Some(parent) = path.parent() {
        if !parent.exists() {
          create_dir_all(parent).await?;
        }
      }
      let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path).await?;
      self.open_files.put(msg_info.clone(), file);
    }

    let file = self.open_files.get_mut(msg_info).unwrap();
    file.write_all(record.as_bytes()).await?;
    file.write_u8('\n' as u8).await?;

    self.update_sizes_and_maybe_bake(msg_info, record.len() + 1);

    Ok(())
  }

  pub async fn retrieve_baked_file_paths(&mut self) -> Vec<BakedFile> {
    let baked_files = self.baked_files.iter().cloned().collect();
    self.baked_files.clear();
    baked_files
  }

  pub async fn remove_baked_buffer_file(&mut self,
    baked_file: BakedFile) -> Result<(), RecordBufferError> {
    self.total_baked_size -= baked_file.size;
    let path = Path::new(&self.buffer_dir).join(&baked_file.key);
    remove_file(path).await?;
    Ok(())
  }

  pub fn get_bytes_used(&self) -> usize {
    self.total_baked_size + self.total_unbaked_size
  }

  pub fn is_buffer_full(&self) -> bool {
    self.get_bytes_used() >= self.buffer_size
  }

  pub fn maybe_bake_everything(&mut self) -> bool {
    if !self.is_buffer_full() {
      return false;
    }
    let unbaked_threshold = (UNBAKED_THRESHOLD * self.buffer_size as f64) as usize;
    if self.total_unbaked_size >= unbaked_threshold {
      let msg_infos: Vec<MsgInfo> = self.latest_buffer_sizes.keys().cloned().collect();
      for msg_info in msg_infos {
        self.bake_file(&msg_info);
      }
      true
    } else {
      false
    }
  }

  pub fn get_buffer_dir(&self) -> String {
    self.buffer_dir.clone()
  }

  pub fn close_files(&mut self) {
    self.open_files.clear();
  }
}
