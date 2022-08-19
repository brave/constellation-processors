use crate::prometheus::{create_metric_server, WebMetrics};
use crate::record_stream::RecordStream;
use crate::star::{parse_message, AppSTARError};
use actix_web::{
  dev::Service,
  error::ResponseError,
  http::{header::ContentType, StatusCode},
  post,
  web::{self, Data},
  App, HttpResponse, HttpServer, Responder,
};
use derive_more::{Display, Error, From};
use futures::{future::try_join, FutureExt, TryFutureExt};
use prometheus_client::registry::Registry;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::time::Instant;

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
  pub web_metrics: Arc<WebMetrics>,
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
    web_metrics: Arc::new(WebMetrics::new()),
  });

  let mut registry = <Registry>::default();
  state.web_metrics.register_metrics(&mut registry);
  let metric_server = create_metric_server(registry)?;

  info!("Starting server...");
  let main_server = HttpServer::new(move || {
    App::new()
      .app_data(state.clone())
      .wrap_fn(|req, srv| {
        let web_metrics = req
          .app_data::<Data<ServerState>>()
          .unwrap()
          .web_metrics
          .clone();
        let web_metrics_err = web_metrics.clone();

        web_metrics.request_start();
        let start_time = Instant::now();
        srv
          .call(req)
          .map(move |res| {
            web_metrics.request_end(Some(start_time.elapsed()));
            res
          })
          .map_err(move |err| {
            web_metrics_err.request_end(None);
            err
          })
      })
      .service(main_handler)
  })
  .workers(worker_count)
  .bind(("0.0.0.0", 8080))?
  .run();

  try_join(metric_server, main_server).await.map(|_| ())
}
