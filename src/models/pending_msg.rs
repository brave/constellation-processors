use super::DBPool;
use super::BatchInsert;
use crate::models::PgStoreError;
use crate::schema::pending_msgs;
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::sync::Arc;
use tokio::task;

#[derive(Queryable, Debug, Clone)]
pub struct PendingMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub message: Vec<u8>,
}

#[derive(Insertable, Clone)]
#[table_name = "pending_msgs"]
pub struct NewPendingMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub message: Vec<u8>,
}

impl PendingMessage {
  pub async fn list(
    pool: Arc<DBPool>,
    filter_epoch_tag: i16,
    filter_msg_tag: Vec<u8>
  ) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let conn = pool.get()?;
      Ok(
        pending_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq(filter_msg_tag))
          .load(&conn)?
      )
    })
    .await?
  }

  pub async fn delete_epoch(pool: Arc<DBPool>, filter_epoch_tag: i16) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let conn = pool.get()?;
      diesel::delete(pending_msgs.filter(epoch_tag.eq(filter_epoch_tag))).execute(&conn)?;
      Ok(())
    })
    .await?
  }

  pub async fn delete_tag(
    pool: Arc<DBPool>,
    filter_epoch_tag: i16,
    filter_msg_tag: Vec<u8>
  ) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let conn = pool.get()?;
      diesel::delete(
        pending_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq(filter_msg_tag))
      ).execute(&conn)?;
      Ok(())
    })
    .await?
  }
}

#[async_trait]
impl BatchInsert<NewPendingMessage> for Vec<NewPendingMessage> {
  async fn insert_batch(self, pool: Arc<DBPool>) -> Result<(), PgStoreError> {
    task::spawn_blocking(move || {
      let conn = pool.get()?;
      diesel::insert_into(pending_msgs::table)
        .values(self)
        .execute(&conn)?;
      Ok(())
    })
    .await?
  }
}
