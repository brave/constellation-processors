use actix_web::{dev::Server, web, App, HttpResponse, HttpServer};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;
use std::io;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct WebMetrics {
  total_requests: Counter,
  in_flight_requests: Gauge,
  request_duration: Histogram,
}

impl WebMetrics {
  pub fn new() -> Self {
    Self {
      total_requests: Counter::default(),
      in_flight_requests: Gauge::default(),
      request_duration: Histogram::new(exponential_buckets(0.01, 2., 8)),
    }
  }

  pub fn request_start(&self) {
    self.total_requests.inc();
    self.in_flight_requests.inc();
  }

  pub fn request_end(&self, success_duration: Option<Duration>) {
    self.in_flight_requests.dec();
    if let Some(d) = success_duration {
      self.request_duration.observe(d.as_secs_f64());
    }
  }

  pub fn register_metrics(&self, registry: &mut Registry) {
    registry.register(
      "api_requests_total",
      "Number of total requests",
      Box::new(self.total_requests.clone()),
    );
    registry.register(
      "in_flight_requests",
      "Number of requests currently being served",
      Box::new(self.in_flight_requests.clone()),
    );
    registry.register(
      "request_duration_seconds",
      "Histogram of latencies for requests",
      Box::new(self.request_duration.clone()),
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
    self.batch_record_total.dec_by(count as u64);
  }

  pub fn register_metrics(&self, registry: &mut Registry) {
    registry.register(
      "records_saved_total",
      "Number of total records saved to the data lake",
      Box::new(self.records_saved_total.clone()),
    );
    registry.register(
      "batch_record_total",
      "Number of total records stored in memory, waiting to be saved to data lake",
      Box::new(self.batch_record_total.clone()),
    );
  }
}

async fn metrics_handler(state: web::Data<Mutex<Registry>>) -> io::Result<HttpResponse> {
  let registry = state.lock().await;
  let mut buffer = vec![];
  encode(&mut buffer, &registry)?;
  let body = std::str::from_utf8(buffer.as_slice()).unwrap().to_string();
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
