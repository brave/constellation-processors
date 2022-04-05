use std::str::{Utf8Error, from_utf8};
use actix_web::{web::{self, Data}, post, App, HttpResponse,
  HttpServer, Responder, error::ResponseError,
  http::{header::ContentType, StatusCode}};
use crate::state::AppState;
use crate::star::{parse_message, AppSTARError};
use derive_more::{From, Error, Display};

#[derive(From, Error, Display, Debug)]
pub enum WebError {
  #[display(fmt = "Failed to decode utf8: {}", _0)]
  Utf8(Utf8Error),
  #[display(fmt = "Failed to decode STAR message: {}", _0)]
  STARDecode(AppSTARError),
  #[display(fmt = "Internal server error")]
  Internal
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
      WebError::Internal => StatusCode::INTERNAL_SERVER_ERROR
    }
  }
}

#[post("/")]
async fn main_handler(body: web::Bytes,
  state: Data<AppState>) -> Result<impl Responder, WebError> {
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

pub async fn start_server(state: Data<AppState>) -> std::io::Result<()> {
  info!("Starting server...");
  HttpServer::new(move || {
    App::new()
      .app_data(state.clone())
      .service(main_handler)
  }).bind(("0.0.0.0", 8080))?
    .run()
    .await
}
