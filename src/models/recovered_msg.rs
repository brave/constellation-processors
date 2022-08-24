use super::{BatchInsert, DBConnection};
use crate::models::PgStoreError;
use crate::schema::recovered_msgs;
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
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
#[table_name = "recovered_msgs"]
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
  ) -> Result<Vec<RecoveredMessage>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = conn.lock().unwrap();
      Ok(
        recovered_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq_any(filter_msg_tags))
          .load(conn.deref())?,
      )
    })
    .await?
  }

  pub async fn update_count(
    conn: Arc<Mutex<DBConnection>>,
    curr_id: i64,
    new_count: i64,
  ) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = conn.lock().unwrap();
      diesel::update(recovered_msgs.filter(id.eq(curr_id)))
        .set(count.eq(new_count))
        .execute(conn.deref())?;

      Ok(())
    })
    .await?
  }

  pub async fn list_with_nonzero_count(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
  ) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = conn.lock().unwrap();
      Ok(
        recovered_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(count.gt(0))
          .load(conn.deref())?,
      )
    })
    .await?
  }

  pub async fn list_distinct_epochs(
    conn: Arc<Mutex<DBConnection>>,
  ) -> Result<Vec<i16>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = conn.lock().unwrap();
      Ok(
        recovered_msgs
          .select(epoch_tag)
          .distinct()
          .load::<i16>(conn.deref())?,
      )
    })
    .await?
  }

  pub async fn delete_epoch(
    conn: Arc<Mutex<DBConnection>>,
    filter_epoch_tag: i16,
  ) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = conn.lock().unwrap();
      diesel::delete(recovered_msgs.filter(epoch_tag.eq(filter_epoch_tag)))
        .execute(conn.deref())?;
      Ok(())
    })
    .await?
  }
}

#[async_trait]
impl BatchInsert<NewRecoveredMessage> for Vec<NewRecoveredMessage> {
  async fn insert_batch(self, conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      let conn = conn.lock().unwrap();
      diesel::insert_into(recovered_msgs::table)
        .values(self)
        .execute(conn.deref())?;
      Ok(())
    })
    .await?
  }
}
