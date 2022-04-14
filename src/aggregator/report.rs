use super::AggregatorError;
use crate::models::{DBPool, RecoveredMessage};
use std::collections::HashMap;
use std::sync::Arc;

fn build_full_measurement_json(msgs: &[&RecoveredMessage]) -> Result<String, AggregatorError> {
  let mut full_measurement = HashMap::new();
  for msg in msgs {
    full_measurement.insert(msg.metric_name.clone(), msg.metric_value.clone());
  }
  full_measurement.insert("count".to_string(), msgs.last().unwrap().count.to_string());
  Ok(serde_json::to_string(&full_measurement)?)
}

struct StackNode {
  msgs: Vec<RecoveredMessage>,
  next_index: usize,
  child_count: i64,
}

pub async fn report_measurements(
  db_pool: Arc<DBPool>,
  report_partial: bool,
) -> Result<(), AggregatorError> {
  let mut stack = vec![StackNode {
    msgs: RecoveredMessage::find_by_parent(db_pool.clone(), None).await?,
    next_index: 0,
    child_count: 0,
  }];
  while !stack.is_empty() {
    let mut last_node = stack.last_mut().unwrap();
    if last_node.next_index >= last_node.msgs.len() {
      let child_count = last_node.child_count;
      let stack_len = stack.len();
      if stack_len > 1 {
        let parent_node = &mut stack[stack_len - 2];
        let parent_msg = &parent_node.msgs[parent_node.next_index - 1];
        RecoveredMessage::update_count(
          db_pool.clone(),
          parent_msg.id,
          parent_msg.count - child_count,
        )
        .await?;
        parent_node.child_count += child_count;
      }
      stack.pop();
      continue;
    }

    let curr_index = last_node.next_index;
    last_node.next_index += 1;
    let msg = &stack.last().unwrap().msgs[curr_index];

    if !msg.has_children {
      let msg_id = msg.id;
      let msg_count = msg.count;

      let msg_chain: Vec<_> = stack.iter().map(|v| &v.msgs[v.next_index - 1]).collect();

      let full_msmt = build_full_measurement_json(&msg_chain)?;
      stack.last_mut().unwrap().child_count += msg_count;
      println!("{}", full_msmt);

      RecoveredMessage::update_count(db_pool.clone(), msg_id, 0).await?;
    } else {
      let child_msgs = RecoveredMessage::find_by_parent(db_pool.clone(), Some(msg.id)).await?;
      stack.push(StackNode {
        msgs: child_msgs,
        child_count: 0,
        next_index: 0,
      });
    }
  }

  Ok(())
}
