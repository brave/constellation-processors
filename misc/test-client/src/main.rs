use clap::Parser;
use futures::future::try_join_all;
use nested_sta_rs::api::*;
use nested_sta_rs::randomness::testing::LocalFetcher as RandomnessFetcher;
use rand::{thread_rng, Rng};
use std::time::Instant;
use std::sync::Arc;
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tokio::sync::Mutex;
use tokio::fs::{File, OpenOptions};

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
  let rnd_fetcher = RandomnessFetcher::new();
  let example_aux = vec![];
  (0..count)
    .map(|_| {
      let rsf = client::format_measurement(layers, cli_args.epoch).unwrap();
      let mgf = client::sample_randomness(&rnd_fetcher, rsf, &None).unwrap();
      let msg = client::construct_message(mgf, &example_aux, cli_args.threshold).unwrap();

      msg
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
  let header: Vec<String> = rdr.headers().unwrap().iter().map(|v| v.to_string()).collect();
  let records: Vec<Vec<String>> = rdr.records().map(|rec_res| {
    let rec = rec_res.unwrap();
    rec.iter().map(|v| v.trim().to_string()).collect()
  }).collect();

  let mut new_path = PathBuf::from(csv_path);
  new_path.set_extension("b64l");
  let file = Arc::new(Mutex::new(
    OpenOptions::new().create(true).append(true).open(new_path).await.unwrap()
  ));

  let rec_chunks: Vec<Vec<Vec<String>>> =
    records.chunks(records.len() / DATA_GEN_TASKS).map(|v| v.to_vec()).collect();
  let rec_chunks_len = rec_chunks.len();

  let gen_tasks: Vec<JoinHandle<()>> = rec_chunks.into_iter().enumerate().map(|(i, rec_chunk)| {
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

        task_msgs.extend(
          generate_messages(&layers, &cli_args, total)
        );

        if j % 100 == 0 {
          println!("Chunk {}/{}: generated {}/{} unique msgs in chunk",
            i, rec_chunks_len, j, chunk_len);
        }
      }
      file.lock().await.write_all(
        task_msgs.join("\n").as_bytes()
      ).await.unwrap();
    })
  }).collect();

  try_join_all(gen_tasks).await.unwrap();
}

#[tokio::main]
async fn main() {
  let cli_args = CliArgs::parse();

  if let Some(gen_data_file) = cli_args.gen_data_file.as_ref() {
    println!("Generating messages from data file...");
    gen_msgs_from_data_and_save(gen_data_file, &cli_args).await;
    return;
  }

  let messages = if let Some(messages_file) = cli_args.messages_file {
    let mut file = File::open(messages_file).await.unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).await.unwrap();
    contents.split('\n').filter_map(|v| if !v.is_empty() {
      Some(v.to_string())
    } else {
      None
    }).collect()
  } else {
    println!("Generating random messages...");
    gen_random_msgs(&cli_args).await
  };

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
