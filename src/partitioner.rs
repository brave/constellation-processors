use actix_web::web::Data;
use std::path::Path;
use std::process;
use std::io;
use crate::lake::{DataLake, DataLakeError};
use crate::models::MsgInfo;
use crate::state::AppState;
use crate::buffer::{RecordBuffer, RecordBufferError, BakedFile};
use crate::record_stream::RecordStreamError;
use crate::star::{parse_message, AppSTARError};
use tokio::fs;
use tokio::{try_join, select};
use tokio::time::{sleep, Duration, Instant};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use futures::future::BoxFuture;
use derive_more::{From, Error, Display};

const EXPIRED_CHECK_INTERVAL_SECS: u64 = 60 * 10;
const EMPTY_DIR_CHECK_INTERVAL_SECS: u64 = 60 * 60;

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Partitioner error: {}")]
pub enum PartitionerError {
  STAR(AppSTARError),
  RecordBuffer(RecordBufferError),
  DataLake(DataLakeError),
  RecordStream(RecordStreamError),
  IO(io::Error)
}

async fn process_lake(lake: DataLake, buffer_dir: String,
  mut from_buffering: UnboundedReceiver<BakedFile>,
  to_buffering: UnboundedSender<BakedFile>) -> Result<(), PartitionerError> {

  loop {
    match from_buffering.recv().await {
      None => {
        info!("Lake task ending, while receiving baked file from buffer task");
        return Ok(());
      },
      Some(baked_file) => {
        debug!("Storing {} in data lake", baked_file.key);
        lake.store_file(&Path::new(&buffer_dir).join(&baked_file.key),
          &baked_file.key, true).await?;
        if let Err(_) = to_buffering.send(baked_file) {
          info!("Lake task ending, while sending baked file to buffer task");
          return Ok(());
        }
      }
    }
  }
}

async fn process_buffer(state: Data<AppState>,
  mut buffer: RecordBuffer, mut from_lake: UnboundedReceiver<BakedFile>,
  to_lake: UnboundedSender<BakedFile>) -> Result<(), PartitionerError> {

  let expired_check_interval = Duration::from_secs(EXPIRED_CHECK_INTERVAL_SECS);
  let empty_dir_check_interval = Duration::from_secs(EMPTY_DIR_CHECK_INTERVAL_SECS);
  let mut last_expired_check = Instant::now();
  let mut last_empty_dir_check = Instant::now();

  loop {
    if buffer.is_buffer_full() {
      error!("Buffer is full! sleeping for a while, hoping baked files will get uploaded soon");
      sleep(Duration::from_secs(30)).await;
      continue;
    }

    if last_expired_check.elapsed() >= expired_check_interval {
      buffer.bake_expired_buffers();
      last_expired_check = Instant::now();
    }

    if last_empty_dir_check.elapsed() >= empty_dir_check_interval {
      buffer.cleanup_empty_dirs(None).await?;
      last_empty_dir_check = Instant::now();
    }

    select! {
      res = state.rec_stream.consume() => {
        let record = res?;
        // TODO: consider producing/consuming bincode instead of base64
        // for better performance
        match parse_message(&record) {
          Err(e) => debug!("failed to parse message: {}", e),
          Ok(msg) => {
            let msg_info = MsgInfo {
              epoch_tag: msg.epoch,
              msg_tags: vec![msg.unencrypted_layer.tag.clone()]
            };
            debug!("Sending {} to buffer", msg_info.to_string());
            buffer.send_to_buffer(&msg_info, &record).await?;
          }
        }

        buffer.bake_everything_if_full();

        for baked_file in buffer.retrieve_baked_file_paths() {
          if let Err(_) = to_lake.send(baked_file) {
            info!("Buffering task ending, while sending baked file to lake task");
            return Ok(());
          }
        }

        state.rec_stream.commit_last_consume().await?;
      },
      res = from_lake.recv() => {
        match res {
          None => {
            info!("Buffering task ending, while receiving baked file from lake task");
            return Ok(());
          },
          Some(baked_file) => {
            debug!("Removing local file {}", baked_file.key);
            buffer.remove_baked_buffer_file(baked_file).await?;
          }
        };
      }
    }
  }
}

fn upload_and_delete_prev_buffer<'a>(data_lake: &'a DataLake, buffer_dir: &'a str,
  curr_path: Option<&'a Path>) -> BoxFuture<'a, Result<(), PartitionerError>> {
  Box::pin(async move {
    let mut entries = fs::read_dir(curr_path.unwrap_or(Path::new(buffer_dir))).await?;
    while let Some(entry) = entries.next_entry().await? {
      let metadata = entry.metadata().await?;
      let path = &entry.path();
      let extension = path.extension().unwrap_or_default().to_str().unwrap();
      if !metadata.is_dir() && extension == "b64l" {
        let key = path.strip_prefix(buffer_dir).unwrap().to_str().unwrap();
        debug!("Storing prev buffer {} in data lake", key);
        data_lake.store_file(&entry.path(), key, true).await?;
        fs::remove_file(path).await?;
      } else if metadata.is_dir() {
        upload_and_delete_prev_buffer(data_lake, buffer_dir, Some(path)).await?;
        fs::remove_dir(path).await?;
      } else {
        fs::remove_file(path).await?;
      }
    }

    Ok(())
  })
}

pub async fn start_partitioner(state: Data<AppState>) {
  let lake = DataLake::new();
  let buffer = RecordBuffer::new();
  let buffer_dir = buffer.get_buffer_dir();

  info!("Uploading buffered files from previous process, if any");
  upload_and_delete_prev_buffer(&lake, &buffer_dir, None).await.unwrap();

  let (to_lake, from_buffering) = mpsc::unbounded_channel::<BakedFile>();
  let (to_buffering, from_lake) = mpsc::unbounded_channel::<BakedFile>();
  let res = try_join!(
    tokio::spawn(async move {
      info!("Starting partitioner data lake task...");
      process_lake(lake, buffer_dir, from_buffering, to_buffering).await
    }),
    tokio::spawn(async move {
      info!("Starting partitioner buffering task...");
      process_buffer(state, buffer, from_lake, to_lake).await
    })
  );
  match res {
    Err(e) => {
      error!("partitioner join error: {:?}", e);
      process::exit(1);
    },
    Ok((l, b)) => {
      if let Err(e) = l {
        error!("lake task error: {}", e);
        process::exit(1);
      }
      if let Err(e) = b {
        error!("buffer task error: {}", e);
        process::exit(1);
      }
    }
  }
}
