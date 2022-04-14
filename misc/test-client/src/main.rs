use clap::Parser;
use futures::future::try_join_all;
use nested_sta_rs::api::*;
use nested_sta_rs::randomness::testing::LocalFetcher as RandomnessFetcher;
use rand::{thread_rng, Rng};
use std::time::Instant;
use tokio::task::JoinHandle;

#[derive(Parser, Clone)]
#[clap(version, about)]
struct CliArgs {
  #[clap(long, default_value = "http://localhost:8080/")]
  url: String,

  #[clap(short, long, default_value = "10")]
  unique_count: usize,

  #[clap(short, long, default_value = "1")]
  epoch: u8,

  #[clap(short, long, default_value = "7")]
  layer_count: usize,

  #[clap(long, default_value = "100")]
  threshold: u32,

  #[clap(short, long, default_value = "30")]
  task_count: usize,
}

#[tokio::main]
async fn main() {
  let cli_args = CliArgs::parse();

  println!("Generating messages...");

  let gen_tasks: Vec<JoinHandle<Vec<String>>> = (0..cli_args.unique_count)
    .map(|i| {
      let cli_args = cli_args.clone();
      tokio::spawn(async move {
        let mut rng = thread_rng();
        let rnd_fetcher = RandomnessFetcher::new();

        println!("Generating unique set {}", i);

        let measurement_layers: Vec<_> = (0..cli_args.layer_count)
          .map(|i| {
            let r: u32 = rng.gen();
            format!("layer{}|{}", i, r).as_bytes().to_vec()
          })
          .collect();
        println!("{}", measurement_layers.iter().map(|v| std::str::from_utf8(v).unwrap()).collect::<Vec<_>>().join(" "));
        let example_aux = vec![];

        (0..cli_args.threshold)
          .map(|_| {
            let rsf = client::format_measurement(&measurement_layers, cli_args.epoch).unwrap();
            let mgf = client::sample_randomness(&rnd_fetcher, rsf, &None).unwrap();
            let msg = client::construct_message(mgf, &example_aux, cli_args.threshold).unwrap();

            msg
          })
          .collect()
      })
    })
    .collect();

  let mut messages: Vec<String> = Vec::new();
  let gen_tasks_results = try_join_all(gen_tasks).await.unwrap();
  for res in gen_tasks_results {
    messages.extend(res);
  }

  println!("Shuffling/splitting messages...");

  let message_count = messages.len();

  let message_chunks: Vec<Vec<String>> = messages
    .chunks(message_count / cli_args.task_count)
    .map(|v| v.to_vec())
    .collect();

  println!("Sending requests...");

  let start_time = Instant::now();

  let tasks: Vec<_> = message_chunks
    .into_iter()
    .map(|chunk| {
      let url = cli_args.url.clone();
      tokio::spawn(async move {
        let client = reqwest::Client::new();
        for msg in chunk {
          client.post(&url).body(msg).send().await.unwrap();
        }
      })
    })
    .collect();

  try_join_all(tasks).await.unwrap();

  let time_taken = start_time.elapsed().as_secs_f64();
  let rate = message_count as f64 / time_taken;

  println!(
    "Sent {} messages, took {} seconds, rate {} reqs/s",
    message_count, time_taken, rate
  );
}
