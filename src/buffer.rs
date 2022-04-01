use std::env;
use std::str::FromStr;
use std::time::{Instant, Duration};
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
const BUFFER_EXPIRY_TIME_SECS: u64 = 36 * 3600;
const UNBAKED_THRESHOLD: f64 = 0.6f64;

#[derive(Clone)]
pub struct BakedFile {
  pub key: String,
  pub size: usize
}

#[derive(Clone)]
struct BufferInfo {
  id: String,
  size: usize,
  creation_time: Instant
}

pub struct RecordBuffer {
  open_files: LruCache<MsgInfo, File>,
  latest_buffers: HashMap<MsgInfo, BufferInfo>,
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
      latest_buffers: Default::default(),
      baked_files: Default::default(),
      total_unbaked_size: 0,
      total_baked_size: 0,
      buffer_dir,
      buffer_size
    }
  }

  fn format_buffer_key(msg_info: &MsgInfo, buffer_info: &BufferInfo) -> String {
    let msg_info_str = msg_info.to_string();
    format!("{}/{}.b64l", msg_info_str, buffer_info.id)
  }

  fn get_buffer_key(&mut self, msg_info: &MsgInfo) -> String {
    let buffer_info = self.latest_buffers.entry(msg_info.clone()).or_insert_with(|| {
      let id_bytes = rand::random::<[u8; 8]>();
      let id = hex::encode(id_bytes);
      let buffer_info = BufferInfo {
        id,
        size: 0,
        creation_time: Instant::now()
      };
      buffer_info
    });
    Self::format_buffer_key(msg_info, buffer_info)
  }

  fn bake_file(&mut self, msg_info: Option<&MsgInfo>) {
    // if none specified, bake all files
    let entries = match msg_info {
      Some(msg_info) => vec![(msg_info.clone(), self.latest_buffers.remove(msg_info).unwrap())],
      None => self.latest_buffers.drain().collect()
    };
    for (msg_info, buffer_info) in entries {
      self.open_files.pop(&msg_info);
      let key = Self::format_buffer_key(&msg_info, &buffer_info);
      self.baked_files.push(BakedFile {
        key,
        size: buffer_info.size
      });

      debug!("{} {} {}", buffer_info.size, self.total_baked_size, self.total_unbaked_size);
      self.total_unbaked_size -= buffer_info.size;
      self.total_baked_size += buffer_info.size;
      self.latest_buffers.remove(&msg_info);
    }
  }

  fn update_sizes_and_maybe_bake(&mut self, msg_info: &MsgInfo,
    added_size: usize) {
    let buffer_info = self.latest_buffers.get_mut(msg_info).unwrap();
    buffer_info.size += added_size;
    self.total_unbaked_size += added_size;

    if buffer_info.size >= BAKED_FILE_SIZE_BYTES {
      self.bake_file(Some(msg_info));
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

  pub fn retrieve_baked_file_paths(&mut self) -> Vec<BakedFile> {
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

  pub fn bake_everything_if_full(&mut self) {
    if !self.is_buffer_full() {
      return;
    }
    let unbaked_threshold = (UNBAKED_THRESHOLD * self.buffer_size as f64) as usize;
    if self.total_unbaked_size >= unbaked_threshold {
      self.bake_file(None);
    }
  }

  pub fn bake_expired_buffers(&mut self) {
    let time_threshold = Duration::from_secs(BUFFER_EXPIRY_TIME_SECS);
    let msg_infos: Vec<MsgInfo> = self.latest_buffers.iter().filter_map(|(msg_info, buffer_info)| {
      if buffer_info.creation_time.elapsed() >= time_threshold {
        Some(msg_info.clone())
      } else {
        None
      }
    }).collect();
    for msg_info in msg_infos {
      self.bake_file(Some(&msg_info));
    }
  }

  pub fn get_buffer_dir(&self) -> String {
    self.buffer_dir.clone()
  }
}
