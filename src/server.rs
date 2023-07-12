use crate::prometheus::{
  create_metric_server, InflightMetricLabels, TotalMetricLabels, WebMetrics,
};
use crate::record_stream::{KafkaRecordStream, RecordStreamArc};
use crate::star::{parse_message, AppSTARError};
use actix_web::{
  dev::Service,
  error::ResponseError,
  get,
  http::{header::ContentType, StatusCode},
  post,
  web::{self, Data},
  App, HttpResponse, HttpServer, Responder,
};
use base64::{engine::general_purpose as base64_engine, Engine as _};
use derive_more::{Display, Error, From};
use futures::{future::try_join, FutureExt};
use prometheus_client::registry::Registry;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::time::Instant;

#[derive(From, Error, Display, Debug)]
pub enum WebError {
  #[display(fmt = "failed to decode base64")]
  Base64(base64::DecodeError),
  #[display(fmt = "Failed to decode utf8: {}", _0)]
  Utf8(Utf8Error),
  #[display(fmt = "Failed to decode STAR message: {}", _0)]
  STARDecode(AppSTARError),
  #[display(fmt = "Internal server error")]
  Internal,
}

pub struct ServerState {
  pub rec_stream: RecordStreamArc,
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
      WebError::STARDecode(_) | WebError::Utf8(_) | WebError::Base64(_) => StatusCode::BAD_REQUEST,
      WebError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
  }
}

#[get("/")]
/// Return an station identification message for data transparency
async fn ident_handler() -> Result<impl Responder, WebError> {
    Ok("STAR Constellation aggregation endpoint. See https://github.com/brave/constellation-processors for more information.")
}

#[post("/")]
async fn main_handler(
  body: web::Bytes,
  state: Data<ServerState>,
) -> Result<impl Responder, WebError> {
  let body_str = from_utf8(&body)?.trim();
  let bincode_msg = base64_engine::STANDARD.decode(body_str)?;
  parse_message(&bincode_msg)?;
  if let Err(e) = state.rec_stream.produce(&bincode_msg).await {
    error!("Failed to push message: {}", e);
    Err(WebError::Internal)
  } else {
    Ok(HttpResponse::NoContent().finish())
  }
}

pub async fn start_server(worker_count: usize) -> std::io::Result<()> {
  let state = Data::new(ServerState {
    rec_stream: Arc::new(KafkaRecordStream::new(true, false, false)),
    web_metrics: Arc::new(WebMetrics::new()),
  });

  let mut registry = <Registry>::default();
  state.web_metrics.register_metrics(&mut registry);
  let metric_server = create_metric_server(registry, 9090)?;

  info!("Starting server...");
  let main_server = HttpServer::new(move || {
    App::new()
      .app_data(state.clone())
      .wrap_fn(|request, srv| {
        let web_metrics = request
          .app_data::<Data<ServerState>>()
          .unwrap()
          .web_metrics
          .clone();

        let inflight_metric_labels = InflightMetricLabels::from(&request);
        web_metrics.request_start(&inflight_metric_labels);

        let start_time = Instant::now();

        srv.call(request).map(move |result| {
          let status_code = match result.as_ref() {
            Ok(response) => response.status(),
            Err(err) => err.as_response_error().status_code(),
          };
          let total_metric_labels = TotalMetricLabels::from((&inflight_metric_labels, status_code));
          web_metrics.request_end(
            &inflight_metric_labels,
            &total_metric_labels,
            start_time.elapsed(),
          );

          result
        })
      })
      .service(ident_handler)
      .service(main_handler)
  })
  .workers(worker_count)
  .bind(("0.0.0.0", 8080))?
  .run();

  try_join(metric_server, main_server).await.map(|_| ())
}
