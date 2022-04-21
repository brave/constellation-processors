use super::DBPool;
use crate::models::PgStoreError;
use crate::schema::recovered_msgs;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use std::sync::Arc;
use tokio::task;

#[derive(Queryable)]
pub struct RecoveredMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub metric_name: String,
  pub metric_value: String,
  pub parent_recovered_msg_id: Option<i64>,
  pub count: i64,
  pub key: Vec<u8>,
  pub has_children: bool,
}

#[derive(Insertable)]
#[table_name = "recovered_msgs"]
pub struct NewRecoveredMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub metric_name: String,
  pub metric_value: String,
  pub parent_recovered_msg_id: Option<i64>,
  pub count: i64,
  pub key: Vec<u8>,
  pub has_children: bool,
}

impl RecoveredMessage {
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

  pub async fn find_by_parent(
    db_pool: Arc<DBPool>,
    parent_id: Option<i64>,
    filter_epoch_tag: Option<i16>,
  ) -> Result<Vec<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = db_pool.get()?;
      let mut q = recovered_msgs.into_boxed().filter(count.gt(0));
      q = match parent_id {
        Some(parent_id) => q.filter(parent_recovered_msg_id.eq(parent_id)),
        None => q.filter(parent_recovered_msg_id.is_null()),
      };
      if let Some(filter_epoch_tag) = filter_epoch_tag {
        q = q.filter(epoch_tag.eq(filter_epoch_tag));
      }
      Ok(q.load(&conn)?)
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

impl NewRecoveredMessage {
  pub async fn insert(self, db_pool: Arc<DBPool>) -> Result<i64, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::id;
      let conn = db_pool.get()?;
      Ok(
        diesel::insert_into(recovered_msgs::table)
          .values(&self)
          .returning(id)
          .get_result::<i64>(&conn)?,
      )
    })
    .await?
  }
}
