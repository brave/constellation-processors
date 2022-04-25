mod aggregator;
mod epoch;
mod lake;
mod lakesink;
mod models;
mod record_stream;
mod schema;
mod server;
mod star;

use aggregator::start_aggregation;
use clap::Parser;
use dotenv::dotenv;
use env_logger::Env;
use epoch::get_current_epoch;
use futures::future::try_join_all;
use lakesink::start_lakesink;
use server::start_server;
use std::process;
use tokio_util::sync::CancellationToken;

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
  lake_sink: bool,

  #[clap(short, long)]
  aggregator: bool,

  #[clap(long)]
  output_measurements_to_stdout: bool,

  #[clap(long, default_value = "16")]
  agg_worker_count: usize,

  #[clap(long, default_value = "650000")]
  agg_msg_collect_count: usize,

  #[clap(long, default_value = "3")]
  agg_iterations: usize,

  #[clap(long, default_value = "16")]
  server_worker_count: usize,

  #[clap(long, default_value = "2")]
  lakesink_consumer_count: usize,
}

#[tokio::main]
async fn main() {
  // TODO: sigint-triggered graceful shutdown
  let cli_args = CliArgs::parse();

  if !cli_args.server && !cli_args.aggregator && !cli_args.lake_sink {
    panic!("Must select process mode! Use -h switch for more details.");
  }

  dotenv().ok();
  env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

  info!("Current epoch is {}", get_current_epoch());

  let mut tasks = Vec::new();

  let mut lakesink_cancel_tokens = Vec::new();
  if cli_args.lake_sink {
    for _ in 0..cli_args.lakesink_consumer_count {
      let cancel_token = CancellationToken::new();
      let cloned_token = cancel_token.clone();
      tasks.push(tokio::spawn(async move {
        let res = start_lakesink(cloned_token.clone()).await;
        if let Err(e) = res {
          error!("Lake sink task failed: {:?}", e);
          process::exit(1);
        }
      }));
      lakesink_cancel_tokens.push(cancel_token);
    }
  }

  if cli_args.aggregator {
    start_aggregation(
      cli_args.agg_worker_count,
      cli_args.agg_msg_collect_count,
      cli_args.agg_iterations,
      cli_args.output_measurements_to_stdout,
    )
    .await
    .unwrap();
    lakesink_cancel_tokens.iter().for_each(|t| t.cancel());
    try_join_all(tasks).await.unwrap();
    return;
  }

  if cli_args.server {
    start_server(cli_args.server_worker_count).await.unwrap();
  } else if !tasks.is_empty() {
    try_join_all(tasks).await.unwrap();
  }
}
