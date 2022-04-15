use super::AggregatorError;
use crate::models::{DBPool, RecoveredMessage};
use crate::record_stream::RecordStream;
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;
use std::sync::Arc;

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
  db_pool: &'a Arc<DBPool>,
  partial_report_epoch: Option<i16>,
  out_stream: Option<&'a RecordStream>,
  metric_chain: Vec<(String, String)>,
  parent_msg_id: Option<i64>,
) -> BoxFuture<'a, Result<i64, AggregatorError>> {
  async move {
    let msgs =
      RecoveredMessage::find_by_parent(db_pool.clone(), parent_msg_id, partial_report_epoch)
        .await?;

    let mut recovered_count = 0;

    for mut msg in msgs {
      if msg.count == 0 {
        continue;
      }

      let mut metric_chain = metric_chain.clone();
      metric_chain.push((msg.metric_name, msg.metric_value));

      let is_final = if msg.has_children {
        let children_rec_count = report_measurements_recursive(
          db_pool,
          partial_report_epoch,
          out_stream,
          metric_chain.clone(),
          Some(msg.id),
        )
        .await?;

        msg.count -= children_rec_count;

        if msg.count > 0 && partial_report_epoch.is_some() {
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
        RecoveredMessage::update_count(db_pool.clone(), msg.id, 0).await?;
      } else {
        RecoveredMessage::update_count(db_pool.clone(), msg.id, msg.count).await?;
      }
    }

    Ok(recovered_count)
  }
  .boxed()
}

pub async fn report_measurements(
  db_pool: Arc<DBPool>,
  partial_report_epoch: Option<i16>,
  out_stream: Option<&RecordStream>,
) -> Result<(), AggregatorError> {
  info!(
    "Reporting {} measurements...",
    if partial_report_epoch.is_some() {
      "partial"
    } else {
      "full"
    }
  );
  let count =
    report_measurements_recursive(&db_pool, partial_report_epoch,
      out_stream, Vec::new(), None).await?;
  info!("Reported {} measurements", count);
  Ok(())
}
