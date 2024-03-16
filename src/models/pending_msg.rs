use super::{BatchInsert, DBConnection};
use crate::models::PgStoreError;
use crate::profiler::{Profiler, ProfilerStat};
use crate::schema::pending_msgs;
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task;

#[derive(Queryable, Debug, Clone)]
pub struct PendingMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub message: Vec<u8>,
  pub threshold: i16,
}

#[derive(Insertable, Clone)]
#[diesel(table_name = pending_msgs)]
pub struct NewPendingMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub message: Vec<u8>,
  pub threshold: i16,
}

impl PendingMessage {
  pub async fn list(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    filter_msg_tag: Vec<u8>,
    profiler: Arc<Profiler>,
  ) -> Result<Vec<Self>, PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let mut conn = conn.lock().unwrap();
      Ok(
        pending_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq(filter_msg_tag))
          .load(conn.deref_mut())?,
      )
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::PendingMsgGet, start_instant)
      .await;
    result
  }

  pub async fn delete_epoch(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let mut conn = conn.lock().unwrap();
      diesel::delete(pending_msgs.filter(epoch_tag.eq(filter_epoch_tag)))
        .execute(conn.deref_mut())?;
      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::PendingMsgDelete, start_instant)
      .await;
    result
  }

  pub async fn delete_tag(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
    filter_msg_tag: Vec<u8>,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let mut conn = conn.lock().unwrap();
      diesel::delete(
        pending_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq(filter_msg_tag)),
      )
      .execute(conn.deref_mut())?;
      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::PendingMsgDelete, start_instant)
      .await;
    result
  }
}

#[async_trait]
impl BatchInsert<NewPendingMessage> for Vec<NewPendingMessage> {
  async fn insert_batch(
    self,
    conn: Arc<Mutex<DBConnection>>,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError> {
    let start_instant = Instant::now();
    let result = task::spawn_blocking(move || {
      let mut conn = conn.lock().unwrap();
      diesel::insert_into(pending_msgs::table)
        .values(self)
        .execute(conn.deref_mut())?;
      Ok(())
    })
    .await?;
    profiler
      .record_range_time(ProfilerStat::PendingMsgInsert, start_instant)
      .await;
    result
  }
}
