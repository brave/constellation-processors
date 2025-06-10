use super::recovered::RecoveredMessages;
use super::AggregatorError;
use crate::epoch::EpochConfig;
use crate::profiler::{Profiler, ProfilerStat};
use crate::record_stream::DynRecordStream;
use futures::future::{BoxFuture, FutureExt};
use serde_json::Value;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Instant;

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

fn report_measurements_recursive<'a>(
  rec_msgs: &'a mut RecoveredMessages,
  epoch: u8,
  epoch_date_field_name: &'a str,
  epoch_start_date: &'a str,
  partial_report: bool,
  out_stream: Option<&'a DynRecordStream>,
  metric_chain: Vec<(String, Value)>,
  parent_msg_tag: Option<Vec<u8>>,
  profiler: Arc<Profiler>,
) -> BoxFuture<'a, Result<i64, AggregatorError>> {
  async move {
    let tags = rec_msgs.get_tags_by_parent(epoch, parent_msg_tag);

    let mut recovered_count = 0;

    for tag in tags {
      let mut msg = rec_msgs.get_mut(epoch, &tag).unwrap().clone();
      if msg.count == 0 {
        continue;
      }

      let mut metric_chain = metric_chain.clone();
      metric_chain.push((msg.metric_name.clone(), msg.metric_value.clone().into()));

      // is_msmt_final: true if the current measurement should be reported right now
      // i.e. all layers have been recovered
      let is_msmt_final = if msg.has_children {
        let children_rec_count = report_measurements_recursive(
          rec_msgs,
          epoch,
          epoch_date_field_name,
          epoch_start_date,
          partial_report,
          out_stream,
          metric_chain.clone(),
          Some(tag),
          profiler.clone(),
        )
        .await?;

        msg.count -= children_rec_count;
        recovered_count += children_rec_count;

        if msg.count > 0 && partial_report {
          // partial_report is typically true during an expired epoch report.
          // If the count for the current tag is non-zero, and child tags cannot be recovered,
          // report the partial measurements now.
          true
        } else {
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
          epoch_date_field_name,
          epoch_start_date,
          msg.count,
        )?;
        let start_instant = Instant::now();
        match out_stream {
          Some(o) => o.queue_produce(full_msmt).await?,
          None => println!("{}", from_utf8(&full_msmt)?),
        };
        profiler
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

pub async fn report_measurements(
  rec_msgs: &mut RecoveredMessages,
  epoch_config: &EpochConfig,
  epoch: u8,
  partial_report: bool,
  out_stream: Option<&DynRecordStream>,
  profiler: Arc<Profiler>,
) -> Result<i64, AggregatorError> {
  let epoch_start_date = epoch_config.get_epoch_survey_date(epoch);
  Ok(
    report_measurements_recursive(
      rec_msgs,
      epoch,
      &epoch_config.epoch_date_field_name,
      &epoch_start_date,
      partial_report,
      out_stream,
      Vec::new(),
      None,
      profiler,
    )
    .await?,
  )
}

#[cfg(test)]
mod tests {
  use std::sync::Mutex;

  use calendar_duration::CalendarDuration;
  use serde_json::json;
  use time::OffsetDateTime;

  use super::*;
  use crate::epoch::CurrentEpochInfo;
  use crate::models::RecoveredMessage;
  use crate::record_stream::TestRecordStream;

  fn test_epoch_config(epoch: u8) -> EpochConfig {
    let epoch_length = CalendarDuration::from("1w");
    EpochConfig {
      current_epoch_info: Mutex::new(CurrentEpochInfo::test_info(epoch, epoch_length)),
      epoch_date_field_name: "wos".to_string(),
      epoch_length,
      epoch_lifetime_count: 3,
    }
  }

  fn expected_date() -> String {
    OffsetDateTime::now_utc().date().to_string()
  }

  #[tokio::test]
  async fn full_report() {
    let record_stream = TestRecordStream::default();
    let mut recovered_msgs = RecoveredMessages::default();
    let profiler = Arc::new(Profiler::default());

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
    let rec_count = report_measurements(
      &mut recovered_msgs,
      &test_epoch_config(2),
      2,
      false,
      Some(&record_stream),
      profiler,
    )
    .await
    .unwrap();

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
    let profiler = Arc::new(Profiler::default());

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
    report_measurements(
      &mut recovered_msgs,
      &test_epoch_config(2),
      2,
      true,
      Some(&record_stream),
      profiler,
    )
    .await
    .unwrap();

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
