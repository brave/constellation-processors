use crate::record_stream::RecordStream;
use crate::star::{parse_message, AppSTARError};
use actix_web::{
  error::ResponseError,
  http::{header::ContentType, StatusCode},
  post,
  web::{self, Data},
  App, HttpResponse, HttpServer, Responder,
};
use derive_more::{Display, Error, From};
use std::str::{from_utf8, Utf8Error};

#[derive(From, Error, Display, Debug)]
pub enum WebError {
  #[display(fmt = "Failed to decode utf8: {}", _0)]
  Utf8(Utf8Error),
  #[display(fmt = "Failed to decode STAR message: {}", _0)]
  STARDecode(AppSTARError),
  #[display(fmt = "Internal server error")]
  Internal,
}

pub struct ServerState {
  pub rec_stream: RecordStream,
}

impl ResponseError for WebError {
  fn error_response(&self) -> HttpResponse {
    HttpResponse::build(self.status_code())
      .insert_header(ContentType::plaintext())
      .body(self.to_string())
  }

  fn status_code(&self) -> StatusCode {
    match *self {
      WebError::STARDecode(_) | WebError::Utf8(_) => StatusCode::BAD_REQUEST,
      WebError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
  }
}

#[post("/")]
async fn main_handler(
  body: web::Bytes,
  state: Data<ServerState>,
) -> Result<impl Responder, WebError> {
  // TODO: add proper error handling, check if body is valid base64
  let body_str = from_utf8(&body)?.trim();
  parse_message(body_str)?;
  if let Err(e) = state.rec_stream.produce(body_str).await {
    error!("Failed to push message: {}", e);
    Err(WebError::Internal)
  } else {
    Ok("")
  }
}

pub async fn start_server(worker_count: usize) -> std::io::Result<()> {
  let state = Data::new(ServerState {
    rec_stream: RecordStream::new(true, false, false),
  });
  info!("Starting server...");
  HttpServer::new(move || App::new().app_data(state.clone()).service(main_handler))
    .workers(worker_count)
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
