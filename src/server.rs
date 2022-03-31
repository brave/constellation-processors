use actix_web::{web::{self, Data}, post, App, HttpServer, Responder};
use crate::models::AppState;

#[post("/")]
async fn main_handler(body: web::Bytes, state: Data<AppState>) -> impl Responder {
  let mut stream = state.rec_stream.lock().await;
  // TODO: add proper error handling, check if body is valid base64
  stream.produce(std::str::from_utf8(&body).unwrap()).unwrap();
  ""
}

pub async fn start_server(state: web::Data<AppState>) -> std::io::Result<()> {
  HttpServer::new(move || {
    App::new()
      .app_data(state.clone())
      .service(main_handler)
  })
  .bind(("0.0.0.0", 8080))?
  .run()
  .await
}
