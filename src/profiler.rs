use std::{
  collections::{BinaryHeap, HashMap},
  fmt::{Display, Formatter},
  ops::DerefMut,
  time::Instant,
};
use tokio::sync::{Mutex, RwLock};

const STAT_PERCENTILES: [f32; 4] = [0.5, 0.75, 0.9, 0.99];

enum StatInfo {
  Range {
    unit: &'static str,
    min: u32,
    max: u32,
    sum: u32,
    entries: BinaryHeap<u32>,
  },
  Seconds(f64),
}

impl Display for StatInfo {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      StatInfo::Range {
        unit,
        min,
        max,
        sum,
        entries,
      } => {
        let entries_len = entries.len();
        let mut percentile_labels = None;
        if !entries.is_empty() {
          let entries = entries.clone().into_sorted_vec();
          // Generate label/description for each predefined
          // precentile.
          percentile_labels = Some(
            STAT_PERCENTILES
              .iter()
              .map(|percent_def| {
                let target_index = (entries_len as f32 * percent_def).floor() as usize;
                format!(
                  "{}th = {}{}",
                  (percent_def * 100.0) as usize,
                  entries[target_index],
                  unit
                )
              })
              .collect::<Vec<_>>()
              .join(", "),
          );
        }
        write!(
          f,
          "min = {}{}, {}, max = {}{}, sum = {}{}, count = {}",
          min,
          unit,
          percentile_labels.unwrap_or_default(),
          max,
          unit,
          sum,
          unit,
          entries.len(),
        )
      }
      StatInfo::Seconds(seconds) => write!(f, "{:.5}s", seconds),
    }
  }
}

#[derive(Copy, Clone, derive_more::Display, Hash, PartialEq, Eq)]
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
  RecoveredMsgDelete,
  TagsPerTask,
  OutStreamProduceTime,
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

  pub async fn record_range(&self, key: ProfilerStat, value: u32, unit: &'static str) {
    if !self.stats.read().await.contains_key(&key) {
      let mut stats = self.stats.write().await;
      // Check key again to handle potential race condition
      if !stats.contains_key(&key) {
        stats.insert(
          key,
          Mutex::new(StatInfo::Range {
            unit,
            min: u32::MAX,
            max: 0,
            sum: 0,
            entries: BinaryHeap::with_capacity(2000),
          }),
        );
      }
    }
    let stats = self.stats.read().await;
    let mut stat_info = stats.get(&key).unwrap().lock().await;
    match stat_info.deref_mut() {
      StatInfo::Range {
        min,
        max,
        sum,
        entries,
        ..
      } => {
        if value < *min {
          *min = value;
        }
        if value > *max {
          *max = value;
        }
        *sum += value;
        entries.push(value);
      }
      _ => {
        panic!("Another stat type already exists for key")
      }
    };
  }

  pub async fn record_range_time(&self, key: ProfilerStat, start_instant: Instant) {
    let millis = start_instant.elapsed().as_millis();
    self.record_range(key, millis as u32, "ms").await;
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
