use base64::{engine::general_purpose as base64_engine, Engine as _};
use clap::Parser;
use futures::future::try_join_all;
use rand::{thread_rng, Rng};
use star_constellation::api::*;
use star_constellation::randomness::testing::{LocalFetcher, LocalFetcherResponse};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const DATA_GEN_TASKS: usize = 128;

#[derive(Parser, Clone)]
#[clap(version, about)]
struct CliArgs {
  #[clap(long, default_value = "http://localhost:8080/")]
  url: String,

  #[clap(short, long, default_value = "10")]
  unique_count: usize,

  #[clap(long)]
  gen_data_file: Option<String>,

  #[clap(long)]
  messages_file: Option<String>,

  #[clap(short, long, default_value = "1")]
  epoch: u8,

  #[clap(short, long, default_value = "7")]
  layer_count: usize,

  #[clap(long, default_value = "100")]
  threshold: u32,

  #[clap(short, long, default_value = "30")]
  task_count: usize,
}

fn generate_messages(layers: &[Vec<u8>], cli_args: &CliArgs, count: usize) -> Vec<String> {
  let rnd_fetcher = LocalFetcher::new();
  let example_aux = vec![];
  (0..count)
    .map(|_| {
      let rrs = client::prepare_measurement(layers, cli_args.epoch).unwrap();
      let req = client::construct_randomness_request(&rrs);

      let req_slice_vec: Vec<&[u8]> = req.iter().map(|v| v.as_slice()).collect();
      let LocalFetcherResponse {
        serialized_points, ..
      } = rnd_fetcher.eval(&req_slice_vec, cli_args.epoch).unwrap();

      let points_slice_vec: Vec<&[u8]> = serialized_points.iter().map(|v| v.as_slice()).collect();
      base64_engine::STANDARD.encode(
        client::construct_message(
          &points_slice_vec,
          None,
          &rrs,
          &None,
          &example_aux,
          cli_args.threshold,
        )
        .unwrap(),
      )
    })
    .collect()
}

async fn gen_random_msgs(cli_args: &CliArgs) -> Vec<String> {
  let gen_tasks: Vec<JoinHandle<Vec<String>>> = (0..cli_args.unique_count)
    .map(|i| {
      let cli_args = cli_args.clone();
      tokio::spawn(async move {
        let mut rng = thread_rng();

        println!("Generating unique set {}", i);

        let measurement_layers: Vec<_> = (0..cli_args.layer_count)
          .map(|i| {
            let r: u32 = rng.gen();
            format!("layer{}|{}", i, r).as_bytes().to_vec()
          })
          .collect();

        generate_messages(&measurement_layers, &cli_args, cli_args.threshold as usize)
      })
    })
    .collect();

  let mut messages = Vec::new();
  let gen_tasks_results = try_join_all(gen_tasks).await.unwrap();
  for res in gen_tasks_results {
    messages.extend(res);
  }
  messages
}

async fn gen_msgs_from_data_and_save(csv_path: &str, cli_args: &CliArgs) {
  let mut rdr = csv::Reader::from_path(csv_path).unwrap();
  let header: Vec<String> = rdr
    .headers()
    .unwrap()
    .iter()
    .map(|v| v.to_string())
    .collect();
  let records: Vec<Vec<String>> = rdr
    .records()
    .map(|rec_res| {
      let rec = rec_res.unwrap();
      rec.iter().map(|v| v.trim().to_string()).collect()
    })
    .collect();

  let mut new_path = PathBuf::from(csv_path);
  new_path.set_extension("b64l");
  let file = Arc::new(Mutex::new(
    OpenOptions::new()
      .create(true)
      .append(true)
      .open(new_path)
      .await
      .unwrap(),
  ));

  let rec_chunks: Vec<Vec<Vec<String>>> = records
    .chunks(std::cmp::max(records.len() / DATA_GEN_TASKS, 1))
    .map(|v| v.to_vec())
    .collect();
  let rec_chunks_len = rec_chunks.len();

  let gen_tasks: Vec<JoinHandle<()>> = rec_chunks
    .into_iter()
    .enumerate()
    .map(|(i, rec_chunk)| {
      let header = header.clone();
      let cli_args = cli_args.clone();
      let file = file.clone();
      tokio::spawn(async move {
        let mut task_msgs = Vec::new();
        let chunk_len = rec_chunk.len();
        for (j, rec) in rec_chunk.into_iter().enumerate() {
          let total = usize::from_str(rec.last().unwrap()).unwrap();
          let layers: Vec<Vec<u8>> = header[..header.len() - 1]
            .iter()
            .zip(rec[..rec.len() - 1].iter())
            .map(|(name, value)| format!("{}|{}", name, value).as_bytes().to_vec())
            .collect();

          task_msgs.extend(generate_messages(&layers, &cli_args, total));

          if j % 100 == 0 {
            println!(
              "Chunk {}/{}: generated {}/{} unique msgs in chunk",
              i, rec_chunks_len, j, chunk_len
            );
          }
        }
        let mut file_guard = file.lock().await;
        file_guard
          .write_all(task_msgs.join("\n").as_bytes())
          .await
          .unwrap();
        file_guard.write_all(b"\n").await.unwrap();
        file_guard.flush().await.unwrap();
      })
    })
    .collect();

  try_join_all(gen_tasks).await.unwrap();
}

async fn send_random_messages(cli_args: &CliArgs) {
  println!("Generating random messages...");
  let messages = gen_random_msgs(cli_args).await;

  println!("Splitting messages...");

  let message_count = messages.len();

  let message_chunks = messages
    .chunks(message_count / cli_args.task_count)
    .map(|v| v.to_vec());

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

  calc_and_output_time(message_count, start_time);
}

async fn send_messages_from_file(cli_args: &CliArgs, messages_file: &str) {
  let file = File::open(messages_file).await.unwrap();
  let reader = Arc::new(Mutex::new(BufReader::new(file)));

  println!("Sending requests...");

  let start_time = Instant::now();

  let tasks: Vec<_> = (0..cli_args.task_count)
    .map(|_| {
      let url = cli_args.url.clone();
      let reader = reader.clone();
      tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut count = 0;
        loop {
          let mut msg = String::new();
          if reader.lock().await.read_line(&mut msg).await.unwrap() == 0 {
            break;
          }
          if msg.is_empty() {
            continue;
          }
          client.post(&url).body(msg).send().await.unwrap();
          count += 1;
        }
        count
      })
    })
    .collect();

  let task_results = try_join_all(tasks).await.unwrap();
  let message_count = task_results.iter().sum();

  calc_and_output_time(message_count, start_time);
}

fn calc_and_output_time(message_count: usize, start_time: Instant) {
  let time_taken = start_time.elapsed().as_secs_f64();
  let rate = message_count as f64 / time_taken;

  println!(
    "Sent {} messages, took {} seconds, rate {} reqs/s",
    message_count, time_taken, rate
  );
}

#[tokio::main]
async fn main() {
  let cli_args = CliArgs::parse();

  if let Some(gen_data_file) = cli_args.gen_data_file.as_ref() {
    println!("Generating messages from data file...");
    gen_msgs_from_data_and_save(gen_data_file, &cli_args).await;
    return;
  }

  if let Some(messages_file) = cli_args.messages_file.as_ref() {
    send_messages_from_file(&cli_args, messages_file).await;
  } else {
    println!("Generating random messages...");
    send_random_messages(&cli_args).await;
  }
}
