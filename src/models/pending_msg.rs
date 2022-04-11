use std::sync::Arc;
use crate::schema::pending_msgs;
use tokio::task;
use diesel::RunQueryDsl;
use async_trait::async_trait;
use crate::models::PgStoreError;
use super::BatchInsert;
use super::DBPool;

#[derive(Queryable)]
pub struct PendingMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub parent_recovered_msg_id: Option<i64>,
  pub message: Vec<u8>
}

#[derive(Insertable)]
#[table_name="pending_msgs"]
pub struct NewPendingMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: i16,
  pub parent_recovered_msg_id: Option<i64>,
  pub message: Vec<u8>
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
    }).await?
  }
}
