mod lake;
mod buffer;
mod models;
mod record_stream;
mod server;
mod state;
mod partitioner;
mod star;
mod aggregator;

use actix_web::web::Data;
use dotenv::dotenv;
use env_logger::Env;
use record_stream::{InMemRecordStream, KafkaRecordStream};
use server::start_server;
use clap::Parser;
use partitioner::start_partitioner;
use aggregator::Aggregator;

use state::AppState;

#[macro_use]
extern crate log;

#[derive(Parser, Debug)]
#[clap(version, about)]
struct CliArgs {
  #[clap(short, long)]
  server: bool,

  #[clap(short, long)]
  partitioner: bool,

  #[clap(short, long)]
  aggregator: bool,

  #[clap(long)]
  use_in_mem_stream: bool
}

#[tokio::main]
async fn main() {
  // TODO: sigint-triggered graceful shutdown
  let cli_args = CliArgs::parse();

  if !cli_args.server && !cli_args.partitioner && !cli_args.aggregator {
    panic!("Must select process mode! Use -h switch for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(
    Env::default().default_filter_or("info")
  ).init();

  if cli_args.aggregator {
    let mut aggregator = Aggregator::new();
    aggregator.aggregate().await.unwrap();
    return;
  }

  let state = Data::new(AppState {
    rec_stream: if cli_args.use_in_mem_stream {
      Box::new(InMemRecordStream::default())
    } else {
      Box::new(KafkaRecordStream::new(cli_args.server, cli_args.partitioner))
    }
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

