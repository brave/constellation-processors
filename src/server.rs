use crate::channel::get_data_channel_map_from_env;
use crate::prometheus::{
  create_metric_server, InflightMetricLabels, TotalMetricLabels, WebMetrics,
};
use crate::record_stream::{
  get_data_channel_topic_map_from_env, KafkaRecordStream, KafkaRecordStreamConfig, RecordStream,
};
use crate::star::{parse_message, AppSTARError};
use actix_web::HttpRequest;
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
use reqwest::header::HeaderName;
use std::collections::HashMap;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::time::Instant;

const MIN_CHANNEL_REVISIONS_ENV_KEY: &str = "MIN_CHANNEL_REVISIONS";
const REVISION_HEADER: &str = "brave-p3a-version";

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
  pub channel_rec_streams: HashMap<String, KafkaRecordStream>,
  pub web_metrics: Arc<WebMetrics>,
  pub main_channel: String,
  pub min_revision_map: HashMap<String, usize>,
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

/// Return an station identification message for data transparency
#[get("/")]
async fn ident_handler() -> Result<impl Responder, WebError> {
  Ok(concat!(
    "STAR Constellation aggregation endpoint.\n",
    "See https://github.com/brave/constellation-processors for more information.\n"
  ))
}

async fn handle_measurement_submit(
  body: web::Bytes,
  request: HttpRequest,
  state: &ServerState,
  channel_name: &String,
) -> Result<impl Responder, WebError> {
  match state.channel_rec_streams.get(channel_name) {
    None => Ok(HttpResponse::NotFound().finish()),
    Some(rec_stream) => {
      let body_str = from_utf8(&body)?.trim();
      let bincode_msg = base64_engine::STANDARD.decode(body_str)?;
      parse_message(&bincode_msg)?;

      if let Some(min_revision) = state.min_revision_map.get(channel_name) {
        let req_revision = request
          .headers()
          .get(HeaderName::from_static(REVISION_HEADER))
          .map(|v| {
            v.to_str()
              .unwrap_or_default()
              .parse::<usize>()
              .unwrap_or_default()
          })
          .unwrap_or_default();
        if req_revision < *min_revision {
          // Just ignore older requests gracefully
          return Ok(HttpResponse::NoContent().finish());
        }
      }

      match rec_stream.produce(&bincode_msg).await {
        Err(e) => {
          error!("Failed to push message: {}", e);
          Err(WebError::Internal)
        }
        Ok(_) => Ok(HttpResponse::NoContent().finish()),
      }
    }
  }
}

#[post("/{channel}")]
async fn channel_handler(
  body: web::Bytes,
  request: HttpRequest,
  state: Data<ServerState>,
  channel: web::Path<String>,
) -> Result<impl Responder, WebError> {
  handle_measurement_submit(body, request, state.as_ref(), channel.as_ref()).await
}

#[post("/")]
async fn main_handler(
  body: web::Bytes,
  request: HttpRequest,
  state: Data<ServerState>,
) -> Result<impl Responder, WebError> {
  handle_measurement_submit(body, request, state.as_ref(), &state.main_channel).await
}

pub async fn start_server(worker_count: usize, main_channel: String) -> std::io::Result<()> {
  let channel_rec_streams = get_data_channel_topic_map_from_env(false)
    .into_iter()
    .map(|(channel_name, topic)| {
      (
        channel_name,
        KafkaRecordStream::new(KafkaRecordStreamConfig {
          enable_producer: true,
          enable_consumer: false,
          topic,
          use_output_group_id: false,
        }),
      )
    })
    .collect();

  let min_revision_map = get_data_channel_map_from_env(MIN_CHANNEL_REVISIONS_ENV_KEY, "")
    .into_iter()
    .map(|(channel, value)| {
      (
        channel,
        value
          .parse::<usize>()
          .expect("minimum channel revision should be non-negative integer"),
      )
    })
    .collect();

  let state = Data::new(ServerState {
    channel_rec_streams,
    web_metrics: Arc::new(WebMetrics::new()),
    main_channel,
    min_revision_map,
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
      .service(channel_handler)
      .service(main_handler)
  })
  .workers(worker_count)
  .bind(("0.0.0.0", 8080))?
  .run();

  try_join(metric_server, main_server).await.map(|_| ())
}
