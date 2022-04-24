use super::{DBPool, BatchInsert};
use crate::models::PgStoreError;
use crate::schema::recovered_msgs;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::sync::Arc;
use tokio::task;
use async_trait::async_trait;

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
      has_children: msg.has_children
    }
  }
}

impl RecoveredMessage {
  pub async fn list(
    db_pool: Arc<DBPool>,
    filter_epoch_tag: i16,
    filter_msg_tags: Vec<Vec<u8>>
  ) -> Result<Vec<RecoveredMessage>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;
      let conn = db_pool.get()?;
      Ok(
        recovered_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq_any(filter_msg_tags))
          .load(&conn)?
      )
    })
    .await?
  }

  pub async fn update_count(
    db_pool: Arc<DBPool>,
    curr_id: i64,
    new_count: i64,
  ) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = db_pool.get()?;
      diesel::update(recovered_msgs.filter(id.eq(curr_id)))
        .set(count.eq(new_count))
        .execute(&conn)?;

      Ok(())
    })
    .await?
  }

  pub async fn list_with_nonzero_count(
    db_pool: Arc<DBPool>,
    filter_epoch_tag: i16,
  ) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = db_pool.get()?;
      Ok(
        recovered_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(count.gt(0))
          .load(&conn)?
      )
    })
    .await?
  }

  pub async fn list_distinct_epochs(db_pool: Arc<DBPool>) -> Result<Vec<i16>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = db_pool.get()?;
      Ok(
        recovered_msgs
          .select(epoch_tag)
          .distinct()
          .load::<i16>(&conn)?,
      )
    })
    .await?
  }

  pub async fn delete_epoch(pool: Arc<DBPool>, filter_epoch_tag: i16) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;
      let conn = pool.get()?;
      diesel::delete(recovered_msgs.filter(epoch_tag.eq(filter_epoch_tag))).execute(&conn)?;
      Ok(())
    })
    .await?
  }
}

#[async_trait]
impl BatchInsert<NewRecoveredMessage> for Vec<NewRecoveredMessage> {
  async fn insert_batch(self, db_pool: Arc<DBPool>) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      let conn = db_pool.get()?;
      diesel::insert_into(recovered_msgs::table)
        .values(self)
        .execute(&conn)?;
      Ok(())
    })
    .await?
  }
}
