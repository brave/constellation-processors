use super::DBPool;
use super::{BatchDelete, BatchInsert};
use crate::models::PgStoreError;
use crate::schema::pending_msgs;
use async_trait::async_trait;
use diesel::pg::types::sql_types::Bytea;
use diesel::sql_types::{BigInt, Nullable, SmallInt};
use diesel::{sql_query, ExpressionMethods, QueryDsl, RunQueryDsl};
use std::sync::Arc;
use tokio::task;

#[derive(Queryable, Debug)]
pub struct PendingMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub parent_recovered_msg_id: Option<i64>,
  pub message: Vec<u8>,
}

#[derive(Insertable)]
#[table_name = "pending_msgs"]
pub struct NewPendingMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub parent_recovered_msg_id: Option<i64>,
  pub message: Vec<u8>,
}

#[derive(QueryableByName, Debug, Clone)]
pub struct PendingMessageCount {
  #[sql_type = "SmallInt"]
  pub epoch_tag: i16,
  #[sql_type = "Bytea"]
  pub msg_tag: Vec<u8>,
  #[sql_type = "BigInt"]
  pub count: i64,
  #[sql_type = "Nullable<BigInt>"]
  pub recovered_msg_id: Option<i64>,
  #[sql_type = "Nullable<Bytea>"]
  pub key: Option<Vec<u8>>,
  #[sql_type = "Nullable<BigInt>"]
  pub recovered_count: Option<i64>,
}

impl PendingMessage {
  pub async fn list(
    pool: Arc<DBPool>,
    filter_epoch_tag: i16,
    filter_msg_tag: Vec<u8>,
  ) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let conn = pool.get()?;
      Ok(
        pending_msgs
          .filter(epoch_tag.eq(filter_epoch_tag))
          .filter(msg_tag.eq(filter_msg_tag))
          .load(&conn)?,
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

#[async_trait]
impl BatchDelete<PendingMessage> for Vec<PendingMessage> {
  async fn delete_batch(&self, pool: Arc<DBPool>) -> Result<(), PgStoreError> {
    let msg_ids: Vec<i64> = self.iter().map(|v| v.id).collect();
    task::spawn_blocking(move || {
      use crate::schema::pending_msgs::dsl::*;
      let conn = pool.get()?;
      diesel::delete(pending_msgs.filter(id.eq_any(msg_ids))).execute(&conn)?;
      Ok(())
    })
    .await?
  }
}

impl PendingMessageCount {
  pub async fn count_new(pool: Arc<DBPool>, min_count: usize) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      let conn = pool.get()?;
      Ok(
        sql_query(
          "SELECT p.epoch_tag, p.msg_tag, COUNT(*) as count, \
            null as recovered_msg_id, null as key, null as recovered_count \
          FROM pending_msgs p \
          LEFT JOIN recovered_msgs r ON r.epoch_tag = p.epoch_tag AND r.msg_tag = p.msg_tag \
          WHERE r.id IS NULL \
          GROUP BY p.epoch_tag, p.msg_tag \
          HAVING COUNT(*) >= $1",
        )
        .bind::<BigInt, _>(min_count as i64)
        .get_results(&conn)?,
      )
    })
    .await?
  }

  pub async fn count_recovered(pool: Arc<DBPool>) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      let conn = pool.get()?;
      Ok(
        sql_query(
          "SELECT p.epoch_tag, p.msg_tag, COUNT(*) as count, \
            r.id as recovered_msg_id, r.key, r.count as recovered_count \
          FROM pending_msgs p \
          JOIN recovered_msgs r ON r.epoch_tag = p.epoch_tag AND r.msg_tag = p.msg_tag \
          GROUP BY p.epoch_tag, p.msg_tag, r.id",
        )
        .get_results(&conn)?,
      )
    })
    .await?
  }
}
