use crate::record_stream::RecordStream;

pub struct AppState {
  pub rec_stream: Box<dyn RecordStream + Send + Sync>
}
