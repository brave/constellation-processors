use super::recovered::RecoveredMessages;
use super::AggregatorError;
use crate::epoch::{get_epoch_survey_date, EpochConfig};
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::DynRecordStream;
use futures::future::{BoxFuture, FutureExt};
use serde_json::Value;
use std::collections::HashMap;
use std::str::from_utf8;
use std::time::Instant;

pub struct MeasurementReporter<'a> {
  epoch_config: &'a EpochConfig,
  out_stream: Option<&'a DynRecordStream>,
  profiler: &'a Profiler,
  epoch: u8,
  epoch_start_date: String,
  partial_report: bool,
}

fn build_full_measurement_json(
  metric_chain: Vec<(String, Value)>,
  epoch_date_field_name: &str,
  epoch_start_date: &str,
  count: i64,
) -> Result<Vec<u8>, AggregatorError> {
  let mut full_measurement = HashMap::new();
  for metric in metric_chain {
    full_measurement.insert(metric.0, metric.1);
  }
  full_measurement.insert("total".to_string(), count.into());
  full_measurement.insert(
    epoch_date_field_name.to_string(),
    epoch_start_date.to_string().into(),
  );
  Ok(serde_json::to_vec(&full_measurement)?)
}

impl<'a> MeasurementReporter<'a> {
  pub fn new(
    epoch_config: &'a EpochConfig,
    out_stream: Option<&'a DynRecordStream>,
    profiler: &'a Profiler,
    epoch: u8,
    partial_report: bool,
  ) -> Self {
    let epoch_start_date = get_epoch_survey_date(&epoch_config, epoch);
    Self {
      epoch_config,
      out_stream,
      profiler,
      epoch,
      epoch_start_date,
      partial_report,
    }
  }

  fn report_recursive(
    &'a self,
    rec_msgs: &'a mut RecoveredMessages,
    metric_chain: Vec<(String, Value)>,
    parent_msg_tag: Option<Vec<u8>>,
  ) -> BoxFuture<'a, Result<i64, AggregatorError>> {
    async move {
      let tags = rec_msgs.get_tags_by_parent(self.epoch, parent_msg_tag);

      let mut recovered_count = 0;

      for tag in tags {
        let mut msg = rec_msgs.get_mut(self.epoch, &tag).unwrap().clone();
        if msg.count == 0 {
          continue;
        }

        let mut metric_chain = metric_chain.clone();
        metric_chain.push((msg.metric_name.clone(), msg.metric_value.clone().into()));

        // is_msmt_final: true if the current measurement should be reported right now
        // i.e. all layers have been recovered, or a partial report was requested for an old epoch
        let is_msmt_final = if msg.has_children {
          let children_rec_count = self
            .report_recursive(rec_msgs, metric_chain.clone(), Some(tag))
            .await?;

          msg.count -= children_rec_count;

          if msg.count > 0 && self.partial_report {
            // partial_report is typically true during an expired epoch report.
            // If the count for the current tag is non-zero, and child tags cannot be recovered,
            // report the partial measurements now.
            true
          } else {
            recovered_count += children_rec_count;
            false
          }
        } else {
          // if there are no children, we have recovered all tags in the metric chain;
          // we can safely report the final measurement
          true
        };

        if is_msmt_final {
          recovered_count += msg.count;
          let full_msmt = build_full_measurement_json(
            metric_chain,
            &self.epoch_config.epoch_date_field_name,
            &self.epoch_start_date,
            msg.count,
          )?;
          let start_instant = Instant::now();
          match self.out_stream {
            Some(o) => o.queue_produce(full_msmt).await?,
            None => println!("{}", from_utf8(&full_msmt)?),
          };
          self
            .profiler
            .record_range_time(ProfilerStat::OutStreamProduceTime, start_instant)
            .await;
          msg.count = 0;
        }
        rec_msgs.add(msg);
      }

      Ok(recovered_count)
    }
    .boxed()
  }

  pub async fn report(
    &'a self,
    rec_msgs: &'a mut RecoveredMessages,
  ) -> Result<i64, AggregatorError> {
    self.report_recursive(rec_msgs, Vec::new(), None).await
  }
}

#[cfg(test)]
mod tests {
  use chrono::{Duration, Utc};
  use serde_json::json;

  use super::*;
  use crate::epoch::CurrentEpochInfo;
  use crate::models::RecoveredMessage;
  use crate::record_stream::TestRecordStream;

  fn test_epoch_config(epoch: u8) -> EpochConfig {
    let epoch_length = Duration::seconds(604800);
    EpochConfig {
      current_epoch: CurrentEpochInfo::test_info(epoch, epoch_length),
      epoch_date_field_name: "wos".to_string(),
      epoch_length,
    }
  }

  fn expected_date() -> String {
    Utc::now().date_naive().to_string()
  }

  #[tokio::test]
  async fn full_report() {
    let record_stream = TestRecordStream::default();
    let mut recovered_msgs = RecoveredMessages::default();
    let profiler = Profiler::default();

    let new_rec_msgs = vec![
      RecoveredMessage {
        id: 0,
        msg_tag: vec![51; 20],
        epoch_tag: 1,
        metric_name: "a".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 22,
        key: vec![88; 32],
        has_children: false,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![51; 20],
        epoch_tag: 2,
        metric_name: "a".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 72,
        key: vec![88; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![52; 20],
        epoch_tag: 2,
        metric_name: "b".to_string(),
        metric_value: "2".to_string(),
        parent_recovered_msg_tag: Some(vec![51; 20]),
        count: 25,
        key: vec![77; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![53; 20],
        epoch_tag: 2,
        metric_name: "c".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: Some(vec![52; 20]),
        count: 7,
        key: vec![99; 32],
        has_children: false,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![54; 20],
        epoch_tag: 2,
        metric_name: "c".to_string(),
        metric_value: "4".to_string(),
        parent_recovered_msg_tag: Some(vec![52; 20]),
        count: 10,
        key: vec![99; 32],
        has_children: false,
      },
    ];

    for rec_msg in new_rec_msgs {
      recovered_msgs.add(rec_msg);
    }
    let epoch_config = test_epoch_config(2);
    let reporter =
      MeasurementReporter::new(&epoch_config, Some(&record_stream), &profiler, 2, false);
    let rec_count = reporter.report(&mut recovered_msgs).await.unwrap();

    assert_eq!(rec_count, 17);
    let records = parse_and_sort_records(record_stream.records_produced.into_inner());

    let date = expected_date();
    assert_eq!(records.len(), 2);
    assert_eq!(
      records[0],
      json!({ "a": "1", "b": "2", "c": "3", "total": 7, "wos": date })
    );
    assert_eq!(
      records[1],
      json!({ "a": "1", "b": "2", "c": "4", "total": 10, "wos": date })
    );

    let rec_epoch_map = recovered_msgs.map.get(&1).unwrap();
    assert_eq!(rec_epoch_map.get(&vec![51; 20]).unwrap().count, 22);
    let rec_epoch_map = recovered_msgs.map.get(&2).unwrap();
    assert_eq!(rec_epoch_map.get(&vec![51; 20]).unwrap().count, 55);
    assert_eq!(rec_epoch_map.get(&vec![52; 20]).unwrap().count, 8);
    assert_eq!(rec_epoch_map.get(&vec![53; 20]).unwrap().count, 0);
    assert_eq!(rec_epoch_map.get(&vec![54; 20]).unwrap().count, 0);
  }

  #[tokio::test]
  async fn partial_report() {
    let record_stream = TestRecordStream::default();
    let mut recovered_msgs = RecoveredMessages::default();
    let profiler = Profiler::default();

    let new_rec_msgs = vec![
      RecoveredMessage {
        id: 0,
        msg_tag: vec![51; 20],
        epoch_tag: 2,
        metric_name: "a".to_string(),
        metric_value: "1".to_string(),
        parent_recovered_msg_tag: None,
        count: 82,
        key: vec![88; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![52; 20],
        epoch_tag: 2,
        metric_name: "b".to_string(),
        metric_value: "2".to_string(),
        parent_recovered_msg_tag: Some(vec![51; 20]),
        count: 27,
        key: vec![77; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![53; 20],
        epoch_tag: 2,
        metric_name: "b".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: Some(vec![51; 20]),
        count: 25,
        key: vec![77; 32],
        has_children: true,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![54; 20],
        epoch_tag: 2,
        metric_name: "c".to_string(),
        metric_value: "3".to_string(),
        parent_recovered_msg_tag: Some(vec![52; 20]),
        count: 0,
        key: vec![99; 32],
        has_children: false,
      },
      RecoveredMessage {
        id: 0,
        msg_tag: vec![55; 20],
        epoch_tag: 2,
        metric_name: "c".to_string(),
        metric_value: "4".to_string(),
        parent_recovered_msg_tag: Some(vec![53; 20]),
        count: 0,
        key: vec![99; 32],
        has_children: false,
      },
    ];

    for rec_msg in new_rec_msgs {
      recovered_msgs.add(rec_msg);
    }
    let epoch_config = test_epoch_config(2);
    let reporter =
      MeasurementReporter::new(&epoch_config, Some(&record_stream), &profiler, 2, true);
    reporter.report(&mut recovered_msgs).await.unwrap();

    let records = parse_and_sort_records(record_stream.records_produced.into_inner());

    let date = expected_date();
    assert_eq!(records.len(), 3);
    assert_eq!(
      records[0],
      json!({ "a": "1", "b": "3", "total": 25, "wos": date }),
    );
    assert_eq!(
      records[1],
      json!({ "a": "1", "b": "2", "total": 27, "wos": date })
    );
    assert_eq!(records[2], json!({ "a": "1", "total": 30, "wos": date }));

    let rec_epoch_map = recovered_msgs.map.get(&2).unwrap();
    assert_eq!(rec_epoch_map.get(&vec![51; 20]).unwrap().count, 0);
    assert_eq!(rec_epoch_map.get(&vec![52; 20]).unwrap().count, 0);
    assert_eq!(rec_epoch_map.get(&vec![53; 20]).unwrap().count, 0);
  }

  fn parse_and_sort_records(records: Vec<Vec<u8>>) -> Vec<serde_json::Value> {
    let mut result: Vec<serde_json::Value> = records
      .iter()
      .map(|v| serde_json::from_slice(&v).unwrap())
      .collect();
    result.sort_by(|a, b| {
      let a_num = a.get("total").unwrap().as_i64().unwrap();
      let b_num = b.get("total").unwrap().as_i64().unwrap();
      a_num.cmp(&b_num)
    });
    result
  }
}
