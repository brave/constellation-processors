mod lake;
mod buffer;
mod models;
mod record_stream;
mod server;
mod state;
mod partitioner;
mod star;

use actix_web::web::Data;
use dotenv::dotenv;
use env_logger::Env;
use record_stream::InMemRecordStream;
use server::start_server;
use clap::Parser;
use partitioner::start_partitioner;

use state::AppState;

#[macro_use]
extern crate log;

#[derive(Parser, Debug)]
#[clap(version, about)]
struct CliArgs {
  #[clap(short, long)]
  server: bool,

  #[clap(short, long)]
  partitioner: bool
}

#[tokio::main]
async fn main() {
  // TODO: sigint-triggered graceful shutdown
  let cli_args = CliArgs::parse();

  if !cli_args.server && !cli_args.partitioner {
    panic!("Must select process mode! Use -h flag for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(
    Env::default().default_filter_or("info")
  ).init();

  let state = Data::new(AppState {
    rec_stream: Box::new(InMemRecordStream::default())
  });

  if cli_args.partitioner {
    if cli_args.server {
      tokio::spawn(start_partitioner(state.clone()));
    } else {
      start_partitioner(state.clone()).await;
    }
  }
  if cli_args.server {
    start_server(state).await.unwrap();
  }
}

