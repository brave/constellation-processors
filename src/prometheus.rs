use actix_web::dev::ServiceRequest;
use actix_web::error::InternalError;
use actix_web::{dev::Server, web, App, HttpResponse, HttpServer};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use reqwest::StatusCode;
use std::io;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct WebMetrics {
  total_requests: Family<TotalMetricLabels, Counter>,
  in_flight_requests: Family<InflightMetricLabels, Gauge>,
  request_duration: Family<RequestDurationLabels, Histogram>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct InflightMetricLabels {
  method: String,
  path: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TotalMetricLabels {
  method: String,
  path: String,
  epoch: Option<u8>,
  status: u16,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestDurationLabels {
  method: String,
  path: String,
  status: u16,
}

impl From<&ServiceRequest> for InflightMetricLabels {
  fn from(request: &ServiceRequest) -> Self {
    Self {
      method: request.method().to_string(),
      path: request.path().to_string(),
    }
  }
}

impl TotalMetricLabels {
  pub fn new(
    inflight_labels: &InflightMetricLabels,
    status: StatusCode,
    epoch: Option<u8>,
  ) -> Self {
    Self {
      method: inflight_labels.method.clone(),
      path: inflight_labels.path.clone(),
      epoch,
      status: status.as_u16(),
    }
  }
}

impl RequestDurationLabels {
  pub fn new(inflight_labels: &InflightMetricLabels, status: StatusCode) -> Self {
    Self {
      method: inflight_labels.method.clone(),
      path: inflight_labels.path.clone(),
      status: status.as_u16(),
    }
  }
}

impl WebMetrics {
  pub fn new() -> Self {
    Self {
      total_requests: Family::default(),
      in_flight_requests: Family::default(),
      request_duration: Family::new_with_constructor(|| {
        Histogram::new(exponential_buckets(0.01, 2., 8))
      }),
    }
  }

  pub fn request_start(&self, labels: &InflightMetricLabels) {
    self.in_flight_requests.get_or_create(labels).inc();
  }

  pub fn request_end(
    &self,
    inflight_labels: &InflightMetricLabels,
    total_labels: &TotalMetricLabels,
    duration_labels: &RequestDurationLabels,
    request_duration: Duration,
  ) {
    match total_labels.status == 404 {
      true => {
        // Remove unknown labels from metrics to avoid pollution
        self.in_flight_requests.remove(inflight_labels);
      }
      false => {
        self.in_flight_requests.get_or_create(inflight_labels).dec();
        self
          .request_duration
          .get_or_create(duration_labels)
          .observe(request_duration.as_secs_f64());
        self.total_requests.get_or_create(total_labels).inc();
      }
    };
  }

  pub fn register_metrics(&self, registry: &mut Registry) {
    registry.register(
      "api_requests",
      "Number of total requests",
      self.total_requests.clone(),
    );
    registry.register(
      "in_flight_requests",
      "Number of requests currently being served",
      self.in_flight_requests.clone(),
    );
    registry.register(
      "request_duration_seconds",
      "Histogram of latencies for requests",
      self.request_duration.clone(),
    );
  }
}

#[derive(Default)]
pub struct DataLakeMetrics {
  records_saved_total: Counter,
  batch_record_total: Gauge,
}

impl DataLakeMetrics {
  pub fn record_received(&self) {
    self.batch_record_total.inc();
  }

  pub fn records_flushed(&self, count: usize) {
    self.records_saved_total.inc_by(count as u64);
    self.batch_record_total.dec_by(count as i64);
  }

  pub fn register_metrics(&self, registry: &mut Registry) {
    registry.register(
      "records_saved_total",
      "Number of total records saved to the data lake",
      self.records_saved_total.clone(),
    );
    registry.register(
      "batch_record_total",
      "Number of total records stored in memory, waiting to be saved to data lake",
      self.batch_record_total.clone(),
    );
  }
}

async fn metrics_handler(state: web::Data<Mutex<Registry>>) -> actix_web::Result<HttpResponse> {
  let registry = state.lock().await;
  let mut body: String = String::new();
  encode(&mut body, &registry)
    .map_err(|v| InternalError::new(v, StatusCode::INTERNAL_SERVER_ERROR))?;
  Ok(
    HttpResponse::Ok()
      .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
      .body(body),
  )
}

async fn health_check_handler() -> io::Result<HttpResponse> {
  Ok(HttpResponse::NoContent().finish())
}

pub fn create_metric_server(registry: Registry, port: u16) -> io::Result<Server> {
  let state = web::Data::new(Mutex::new(registry));
  Ok(
    HttpServer::new(move || {
      App::new()
        .app_data(state.clone())
        .service(web::resource("/metrics").route(web::get().to(metrics_handler)))
        .service(web::resource("/health").route(web::get().to(health_check_handler)))
    })
    .bind(("0.0.0.0", port))?
    .run(),
  )
}
