use derive_more::Display;
use std::{collections::HashMap, ops::DerefMut, time::Instant};
use tokio::sync::{Mutex, RwLock};

#[derive(Display)]
enum StatInfo {
  #[display(
    fmt = "min = {:.5}{}, max = {:.5}{}, avg = {:.5}{}, sum = {:.5}{}, count = {}",
    min,
    unit,
    max,
    unit,
    "sum / (*count as f64)",
    unit,
    sum,
    unit,
    count
  )]
  Range {
    unit: &'static str,
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
  },
  #[display(fmt = "{:.5}s", _0)]
  Seconds(f64),
}

#[derive(Copy, Clone, Display, Hash, PartialEq, Eq)]
pub enum ProfilerStat {
  DownloadTime,
  TaskProcessingTime,
  TotalProcessingTime,
  PendingMsgGet,
  PendingMsgInsert,
  PendingMsgDelete,
  RecoveredMsgGet,
  RecoveredMsgUpdate,
  RecoveredMsgInsert,
  TagsPerTask,
}

#[derive(Default)]
pub struct Profiler {
  stats: RwLock<HashMap<ProfilerStat, Mutex<StatInfo>>>,
}

impl Profiler {
  pub async fn record_total_time(&self, key: ProfilerStat, start_instant: Instant) {
    let seconds = start_instant.elapsed().as_secs_f64();
    let mut stats = self.stats.write().await;
    assert!(!stats.contains_key(&key), "Total time stat already exists");
    stats.insert(key, Mutex::new(StatInfo::Seconds(seconds)));
  }

  pub async fn record_range(&self, key: ProfilerStat, value: f64, unit: &'static str) {
    if !self.stats.read().await.contains_key(&key) {
      let mut stats = self.stats.write().await;
      stats.insert(
        key,
        Mutex::new(StatInfo::Range {
          unit,
          min: f64::MAX,
          max: 0f64,
          sum: 0f64,
          count: 0,
        }),
      );
    }
    let stats = self.stats.read().await;
    let mut stat_info = stats.get(&key).unwrap().lock().await;
    match stat_info.deref_mut() {
      StatInfo::Range {
        min,
        max,
        sum,
        count,
        ..
      } => {
        if value < *min {
          *min = value;
        }
        if value > *max {
          *max = value;
        }
        *sum += value;
        *count += 1;
      }
      _ => {
        panic!("Another stat type already exists for key")
      }
    };
  }

  pub async fn record_range_time(&self, key: ProfilerStat, start_instant: Instant) {
    let seconds = start_instant.elapsed().as_secs_f64();
    self.record_range(key, seconds, "s").await;
  }

  pub async fn summary(&self) -> String {
    let mut stats = self.stats.write().await;
    let mut lines = stats
      .drain()
      .map(|(k, v)| format!("{}: {}", k, v.into_inner()))
      .collect::<Vec<String>>();
    lines.sort();
    lines.join("\n")
  }
}
