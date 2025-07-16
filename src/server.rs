use crate::channel::get_data_channel_map_from_env;
use crate::prometheus::{
  create_metric_server, InflightMetricLabels, RequestDurationLabels, TotalMetricLabels, WebMetrics,
};
use crate::record_stream::{
  get_data_channel_topic_map_from_env, KafkaRecordStream, KafkaRecordStreamConfig,
  KafkaRecordStreamFactory, RecordStream,
};
use crate::star::{parse_message, AppSTARError};
use crate::util::parse_env_var;
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
use std::ops::RangeInclusive;
use std::str::{from_utf8, FromStr, Utf8Error};
use std::sync::Arc;
use std::time::Instant;

const MIN_CHANNEL_REVISIONS_ENV_KEY: &str = "MIN_CHANNEL_REVISIONS";
const MIN_REQUEST_K_THRESHOLD_ENV_KEY: &str = "MIN_REQUEST_K_THRESHOLD";
const MIN_REQUEST_K_THRESHOLD_DEFAULT: &str = "20";
const MAX_REQUEST_K_THRESHOLD_ENV_KEY: &str = "MAX_REQUEST_K_THRESHOLD";
const MAX_REQUEST_K_THRESHOLD_DEFAULT: &str = "50";
const REVISION_HEADER: &str = "brave-p3a-version";
const THRESHOLD_HEADER: &str = "brave-p3a-constellation-threshold";

#[derive(From, Error, Display, Debug)]
pub enum WebError {
  #[display(fmt = "failed to decode base64")]
  Base64(base64::DecodeError),
  #[display(fmt = "Failed to decode utf8: {}", _0)]
  Utf8(Utf8Error),
  #[display(fmt = "Failed to decode STAR message: {}", _0)]
  STARDecode(AppSTARError),
  #[display(fmt = "Bad k threshold in request header")]
  BadThreshold,
  #[display(fmt = "Internal server error")]
  Internal,
}

pub struct ServerState {
  pub channel_rec_streams: HashMap<String, KafkaRecordStream>,
  pub web_metrics: Arc<WebMetrics>,
  pub main_channel: String,
  pub min_revision_map: HashMap<String, usize>,
  pub request_threshold_range: RangeInclusive<usize>,
}

struct EpochExt(u8);

impl ResponseError for WebError {
  fn error_response(&self) -> HttpResponse {
    HttpResponse::build(self.status_code())
      .insert_header(ContentType::plaintext())
      .body(self.to_string())
  }

  fn status_code(&self) -> StatusCode {
    match *self {
      WebError::STARDecode(_)
      | WebError::Utf8(_)
      | WebError::Base64(_)
      | WebError::BadThreshold => StatusCode::BAD_REQUEST,
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

fn extract_and_parse_header<T: FromStr>(
  request: &HttpRequest,
  header_name: &'static str,
) -> Option<T> {
  request
    .headers()
    .get(HeaderName::from_static(header_name))
    .and_then(|v| v.to_str().unwrap_or_default().parse::<T>().ok())
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
      let message = parse_message(&bincode_msg)?;

      if let Some(min_revision) = state.min_revision_map.get(channel_name) {
        let req_revision: usize =
          extract_and_parse_header(&request, REVISION_HEADER).unwrap_or_default();
        if req_revision < *min_revision {
          // Just ignore older requests gracefully
          return Ok(HttpResponse::NoContent().finish());
        }
      }
      let threshold: Option<usize> = extract_and_parse_header(&request, THRESHOLD_HEADER);
      if let Some(threshold) = threshold {
        if !state.request_threshold_range.contains(&threshold) {
          return Err(WebError::BadThreshold);
        }
      }

      match rec_stream.produce(&bincode_msg, threshold).await {
        Err(e) => {
          error!("Failed to push message: {}", e);
          Err(WebError::Internal)
        }
        Ok(_) => {
          let mut response = HttpResponse::NoContent();
          response.extensions_mut().insert(EpochExt(message.epoch));
          Ok(response.finish())
        }
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
  let rec_stream_factory = KafkaRecordStreamFactory::new();
  let channel_rec_streams = get_data_channel_topic_map_from_env(false)
    .into_iter()
    .map(|(channel_name, topic)| {
      (
        channel_name.clone(),
        rec_stream_factory.create_record_stream(KafkaRecordStreamConfig {
          enable_producer: true,
          enable_consumer: false,
          topic,
          use_output_group_id: false,
          channel_name,
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

  let min_request_threshold = parse_env_var(
    MIN_REQUEST_K_THRESHOLD_ENV_KEY,
    MIN_REQUEST_K_THRESHOLD_DEFAULT,
  );
  let max_request_threshold = parse_env_var(
    MAX_REQUEST_K_THRESHOLD_ENV_KEY,
    MAX_REQUEST_K_THRESHOLD_DEFAULT,
  );

  let state = Data::new(ServerState {
    channel_rec_streams,
    web_metrics: Arc::new(WebMetrics::new()),
    main_channel,
    min_revision_map,
    request_threshold_range: min_request_threshold..=max_request_threshold,
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
          let (epoch, status_code) = match result.as_ref() {
            Ok(response) => (
              response
                .response()
                .extensions()
                .get::<EpochExt>()
                .map(|ext| ext.0),
              response.status(),
            ),
            Err(err) => (None, err.as_response_error().status_code()),
          };
          let total_metric_labels =
            TotalMetricLabels::new(&inflight_metric_labels, status_code, epoch);
          let duration_labels = RequestDurationLabels::new(&inflight_metric_labels, status_code);
          web_metrics.request_end(
            &inflight_metric_labels,
            &total_metric_labels,
            &duration_labels,
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
