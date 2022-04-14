mod aggregator;
mod dbsink;
mod epoch;
mod models;
mod record_stream;
mod schema;
mod server;
mod star;
mod state;

use actix_web::web::Data;
use aggregator::start_aggregation;
use clap::Parser;
use dbsink::start_dbsink;
use dotenv::dotenv;
use env_logger::Env;
use futures::future::try_join_all;
use record_stream::{InMemRecordStream, KafkaRecordStream, RecordStream};
use server::start_server;
use std::process;

use state::AppState;

#[macro_use]
extern crate log;

#[macro_use]
extern crate diesel;

#[derive(Parser, Debug, Clone)]
#[clap(version, about)]
struct CliArgs {
  #[clap(short, long)]
  server: bool,

  #[clap(short, long)]
  db_sink: bool,

  #[clap(short, long)]
  aggregator: bool,

  #[clap(long)]
  use_in_mem_stream: bool,

  #[clap(long)]
  output_measurements_to_stdout: bool,

  #[clap(long, default_value = "2")]
  consumer_count: usize,
}

fn create_rec_stream(
  cli_args: &CliArgs,
  create_for_state: bool,
) -> Box<dyn RecordStream + Send + Sync> {
  if cli_args.use_in_mem_stream {
    Box::new(InMemRecordStream::default())
  } else {
    let consumer_enabled = cli_args.db_sink && (!create_for_state || cli_args.server);
    let producer_enabled = (cli_args.server && create_for_state) || cli_args.aggregator;
    Box::new(KafkaRecordStream::new(
      producer_enabled,
      consumer_enabled,
      cli_args.aggregator,
    ))
  }
}

#[tokio::main]
async fn main() {
  // TODO: sigint-triggered graceful shutdown
  let cli_args = CliArgs::parse();

  if !cli_args.server && !cli_args.db_sink && !cli_args.aggregator {
    panic!("Must select process mode! Use -h switch for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

  if cli_args.aggregator {
    let out_stream = if cli_args.output_measurements_to_stdout {
      None
    } else {
      Some(create_rec_stream(&cli_args, false))
    };
    start_aggregation(out_stream.as_ref()).await.unwrap();
    return;
  }

  let state = Data::new(AppState {
    rec_stream: create_rec_stream(&cli_args, true),
  });

  if cli_args.db_sink {
    if !cli_args.server {
      let tasks: Vec<_> = (0..cli_args.consumer_count)
        .map(|_| {
          let cli_args = cli_args.clone();
          tokio::spawn(async move {
            let rec_stream = create_rec_stream(&cli_args, false);
            start_dbsink(rec_stream.as_ref()).await
          })
        })
        .collect();
      try_join_all(tasks).await.unwrap();
    } else {
      let state_for_sink = state.clone();
      tokio::spawn(async move {
        if let Err(e) = start_dbsink(state_for_sink.rec_stream.as_ref()).await {
          error!("DB sink task failed: {:?}", e);
          process::exit(1);
        }
      });
    }
  }
  if cli_args.server {
    start_server(state).await.unwrap();
  }
}
