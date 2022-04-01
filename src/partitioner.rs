use actix_web::web::Data;
use std::path::Path;
use std::process;
use std::num::Wrapping;
use crate::lake::{DataLake, DataLakeError};
use crate::models::MsgInfo;
use crate::state::AppState;
use crate::buffer::{RecordBuffer, RecordBufferError, BakedFile};
use crate::record_stream::RecordStreamError;
use crate::star::{parse_message, AppSTARError};
use tokio::{try_join, select};
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use derive_more::{From, Error, Display};

const EXPIRED_CHECK_INTERVAL: Wrapping<usize> = Wrapping(10);
const WR_ONE: Wrapping<usize> = Wrapping(1);
const WR_ZERO: Wrapping<usize> = Wrapping(0);

#[derive(Error, From, Display, Debug)]
#[display(fmt = "Partitioner error: {}")]
pub enum PartitionerError {
  STAR(AppSTARError),
  RecordBuffer(RecordBufferError),
  DataLake(DataLakeError),
  RecordStream(RecordStreamError)
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

async fn process_buffer(state: Data<AppState>, mut buffer: RecordBuffer,
  mut from_lake: UnboundedReceiver<BakedFile>,
  to_lake: UnboundedSender<BakedFile>) -> Result<(), PartitionerError> {

  let mut it_count = Wrapping(0usize);

  loop {
    if buffer.is_buffer_full() {
      error!("Buffer is full! sleeping for a while, hoping baked files will get uploaded soon");
      sleep(Duration::from_secs(30)).await;
      continue;
    }

    select! {
      res = state.rec_stream.consume() => {
        let records = res?;
        for record in records {
          match parse_message(&record) {
            Err(e) => debug!("failed to parse message: {}", e),
            Ok(msg) => {
              let msg_info = MsgInfo {
                epoch_tag: msg.epoch,
                layer: 0,
                msg_tag: msg.unencrypted_layer.tag.clone()
              };
              debug!("Sending {} to buffer", msg_info.to_string());
              buffer.send_to_buffer(&msg_info, &record).await?;
            }
          }
        }

        buffer.bake_everything_if_full();
        if it_count % EXPIRED_CHECK_INTERVAL == WR_ZERO {
          buffer.bake_expired_buffers();
        }
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

    it_count += WR_ONE;
  }
}

pub async fn start_partitioner(state: Data<AppState>) {
  let (to_lake, from_buffering) = mpsc::unbounded_channel::<BakedFile>();
  let (to_buffering, from_lake) = mpsc::unbounded_channel::<BakedFile>();
  let buffer = RecordBuffer::new();
  let lake = DataLake::new();
  let buffer_dir = buffer.get_buffer_dir();
  let res = try_join!(
    tokio::spawn(async {
      info!("Starting partitioner data lake task...");
      process_lake(lake, buffer_dir, from_buffering, to_buffering).await
    }),
    tokio::spawn(async {
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
