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
