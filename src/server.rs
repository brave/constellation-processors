use actix_web::{web::{self, Data}, post, App, HttpServer, Responder};
use crate::state::AppState;

#[post("/")]
async fn main_handler(body: web::Bytes, state: Data<AppState>) -> impl Responder {
  // TODO: add proper error handling, check if body is valid base64
  state.rec_stream.produce(
    std::str::from_utf8(&body).unwrap().trim()
  ).await.unwrap();
  ""
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
