use super::{BatchInsert, DBConnection};
use crate::models::PgStoreError;
use crate::profiler::{Profiler, ProfilerStat};
use crate::schema::recovered_msgs;
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task;

#[derive(Queryable, Clone)]
pub struct RecoveredMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub metric_name: String,
  pub metric_value: String,
  pub parent_recovered_msg_tag: Option<Vec<u8>>,
  pub count: i64,
  pub key: Vec<u8>,
  pub has_children: bool,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = recovered_msgs)]
pub struct NewRecoveredMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub metric_name: String,
  pub metric_value: String,
  pub parent_recovered_msg_tag: Option<Vec<u8>>,
  pub count: i64,
  pub key: Vec<u8>,
  pub has_children: bool,
}

impl From<RecoveredMessage> for NewRecoveredMessage {
  fn from(msg: RecoveredMessage) -> Self {
    Self {
      msg_tag: msg.msg_tag,
      epoch_tag: msg.epoch_tag,
      metric_name: msg.metric_name,
      metric_value: msg.metric_value,
      parent_recovered_msg_tag: msg.parent_recovered_msg_tag,
      count: msg.count,
      key: msg.key,
      has_children: msg.has_children,
    }
  }
}

impl RecoveredMessage {
  pub async fn list(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    filter_msg_tags: Vec<Vec<u8>>,
    profiler: Arc<Profiler>,
  ) -> Result<Vec<RecoveredMessage>, PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let mut conn = conn.lock().unwrap();
      Ok(
        recovered_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq_any(filter_msg_tags))
          .load(conn.deref_mut())?,
      )
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::RecoveredMsgGet, start_instant)
      .await;
    result
  }

  pub async fn update_count(
    conn: Arc<Mutex<DBConnection>>,
    curr_id: i64,
    new_count: i64,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let mut conn = conn.lock().unwrap();
      diesel::update(recovered_msgs.filter(id.eq(curr_id)))
        .set(count.eq(new_count))
        .execute(conn.deref_mut())?;

      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::RecoveredMsgUpdate, start_instant)
      .await;
    result
  }

  pub async fn list_with_nonzero_count(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    profiler: Arc<Profiler>,
  ) -> Result<Vec<Self>, PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let mut conn = conn.lock().unwrap();
      let result = recovered_msgs
        .filter(epoch_tag.eq(filter_epoch_tag))
        .filter(count.gt(0))
        .load(conn.deref_mut())?;
      Ok(result)
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::RecoveredMsgGet, start_instant)
      .await;
    result
  }

  pub async fn list_distinct_epochs(
    conn: Arc<Mutex<DBConnection>>,
  ) -> Result<Vec<i16>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let mut conn = conn.lock().unwrap();
      Ok(
        recovered_msgs
          .select(epoch_tag)
          .distinct()
          .load::<i16>(conn.deref_mut())?,
      )
    })
    .await?
  }

  pub async fn delete_epoch(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let mut conn = conn.lock().unwrap();
      diesel::delete(recovered_msgs.filter(epoch_tag.eq(filter_epoch_tag)))
        .execute(conn.deref_mut())?;
      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::RecoveredMsgDelete, start_instant)
      .await;
    result
  }
}

#[async_trait]
impl BatchInsert<NewRecoveredMessage> for Vec<NewRecoveredMessage> {
  async fn insert_batch(
    self,
    conn: Arc<Mutex<DBConnection>>,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      let mut conn = conn.lock().unwrap();
      diesel::insert_into(recovered_msgs::table)
        .values(self)
        .execute(conn.deref_mut())?;
      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::RecoveredMsgInsert, start_instant)
      .await;
    result
  }
}
