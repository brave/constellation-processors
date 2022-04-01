use tokio::sync::Mutex;
use crate::record_stream::RecordStream;

pub struct AppState {
  pub rec_stream: Mutex<Box<dyn RecordStream + Send>>
}
