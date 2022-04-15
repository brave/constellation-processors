mod aggregator;
mod dbsink;
mod epoch;
mod models;
mod record_stream;
mod schema;
mod server;
mod star;
mod state;
mod lake;
mod lakesink;

use actix_web::web::Data;
use aggregator::start_aggregation;
use clap::Parser;
use dbsink::start_dbsink;
use dotenv::dotenv;
use env_logger::Env;
use futures::future::try_join_all;
use record_stream::RecordStream;
use server::start_server;
use std::process;
use tokio_util::sync::CancellationToken;
use lakesink::start_lakesink;

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
  lake_sink: bool,

  #[clap(short, long)]
  aggregator: bool,

  #[clap(long)]
  output_measurements_to_stdout: bool,

  #[clap(long, default_value = "2")]
  consumer_count: usize,
}

#[tokio::main]
async fn main() {
  // TODO: sigint-triggered graceful shutdown
  let cli_args = CliArgs::parse();

  if !cli_args.server && !cli_args.db_sink && !cli_args.aggregator && !cli_args.lake_sink {
    panic!("Must select process mode! Use -h switch for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

  let mut tasks = Vec::new();

  let mut lakesink_cancel_tokens = Vec::new();
  if cli_args.lake_sink {
    for _ in 0..cli_args.consumer_count {
      let cancel_token = CancellationToken::new();
      let cloned_token = cancel_token.clone();
      tasks.push(tokio::spawn(async move {
        let res = start_lakesink(RecordStream::new(false, true, true),
          cloned_token.clone()).await;
        if let Err(e) = res {
          error!("Lake sink task failed: {:?}", e);
          process::exit(1);
        }
      }));
      lakesink_cancel_tokens.push(cancel_token);
    }
  }

  if cli_args.aggregator {
    let out_stream = if cli_args.output_measurements_to_stdout {
      None
    } else {
      Some(RecordStream::new(true, false, true))
    };
    start_aggregation(out_stream).await.unwrap();
    lakesink_cancel_tokens.iter().for_each(|t| t.cancel());
    try_join_all(tasks).await.unwrap();
    return;
  }

  if cli_args.db_sink {
    for _ in 0..cli_args.consumer_count {
      tasks.push(tokio::spawn(async move {
        let res = start_dbsink(RecordStream::new(false, true, false)).await;
        if let Err(e) = res {
          error!("DB sink task failed: {:?}", e);
          process::exit(1);
        }
      }));
    }
  }

  if cli_args.server {
    let state = Data::new(AppState {
      rec_stream: RecordStream::new(true, false, false),
    });
    start_server(state).await.unwrap();
  } else if !tasks.is_empty() {
    try_join_all(tasks).await.unwrap();
  }
}
