mod lake;
mod buffer;
mod models;
mod record_stream;
mod server;

use actix_web::web::Data;
use dotenv::dotenv;
use env_logger::Env;
use record_stream::InMemRecordStream;
use tokio::sync::Mutex;
use server::start_server;
use clap::Parser;

use models::{MsgInfo, AppState};

#[macro_use]
extern crate log;

#[derive(Parser, Debug)]
#[clap(version, about)]
struct CliArgs {
  #[clap(short, long)]
  server: bool
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
  let cli_args = CliArgs::parse();

  if !cli_args.server {
    panic!("Must select process mode! Use -h flag for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(
    Env::default().default_filter_or("info")
  ).init();

  let state = Data::new(AppState {
    rec_stream: Mutex::new(Box::new(InMemRecordStream::default()))
  });

  if cli_args.server {
    return start_server(state).await;
  }
  Ok(())
}

