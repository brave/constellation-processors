use super::AggregatorError;
use super::recovered::RecoveredMessages;
use crate::record_stream::RecordStream;
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;

fn build_full_measurement_json(
  metric_chain: Vec<(String, String)>,
  count: i64,
) -> Result<String, AggregatorError> {
  let mut full_measurement = HashMap::new();
  for metric in metric_chain {
    full_measurement.insert(metric.0, metric.1);
  }
  full_measurement.insert("count".to_string(), count.to_string());
  Ok(serde_json::to_string(&full_measurement)?)
}

fn report_measurements_recursive<'a>(
  rec_msgs: &'a mut RecoveredMessages,
  epoch: u8,
  partial_report: bool,
  out_stream: Option<&'a RecordStream>,
  metric_chain: Vec<(String, String)>,
  parent_msg_tag: Option<Vec<u8>>,
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
      metric_chain.push((msg.metric_name.clone(), msg.metric_value.clone()));

      let is_final = if msg.has_children {
        let children_rec_count = report_measurements_recursive(
          rec_msgs,
          epoch,
          partial_report,
          out_stream,
          metric_chain.clone(),
          Some(tag),
        )
        .await?;

        msg.count -= children_rec_count;

        if msg.count > 0 && partial_report {
          true
        } else {
          recovered_count += children_rec_count;
          false
        }
      } else {
        true
      };

      if is_final {
        recovered_count += msg.count;
        let full_msmt = build_full_measurement_json(metric_chain, msg.count)?;
        match out_stream {
          Some(o) => o.produce(&full_msmt).await?,
          None => println!("{}", full_msmt),
        };
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
  epoch: u8,
  partial_report: bool,
  out_stream: Option<&RecordStream>,
) -> Result<i64, AggregatorError> {
  Ok(
    report_measurements_recursive(rec_msgs, epoch, partial_report,
      out_stream, Vec::new(), None).await?
  )
}
