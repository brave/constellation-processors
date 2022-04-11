use crate::state::DBPool;
use crate::schema::recovered_msgs;
use tokio::task;
use actix_web::web;
use crate::state::AppState;
use crate::models::PgStoreError;
use diesel::result::{Error, DatabaseErrorKind};
use diesel::{QueryDsl, ExpressionMethods, RunQueryDsl, Connection};

#[derive(Queryable)]
pub struct RecoveredMessage {
  pub id: i64,
  pub msg_tag: Vec<u8>,
  pub epoch_tag: String,
  pub metric_name: String,
  pub metric_value: String,
  pub key: Vec<u8>
}

#[derive(Insertable)]
#[table_name="recovered_msgs"]
pub struct NewRecoveredMessage {
  pub msg_tag: Vec<u8>,
  pub epoch_tag: String,
  pub metric_name: String,
  pub metric_value: String,
  pub key: Vec<u8>
}

impl RecoveredMessage {
  pub async fn find(app_st: web::Data<AppState>, search_epoch_tag: String,
    search_msg_tag: Vec<u8>) -> Result<Option<Self>, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::*;

      let conn = app_st.db_pool.get()?;
      let recovered_msg = recovered_msgs
        .filter(msg_tag.eq(&search_msg_tag))
        .filter(epoch_tag.eq(&search_epoch_tag))
        .first::<Self>(&conn);

      match recovered_msg {
        Ok(r) => Ok(Some(r)),
        Err(e) => match e {
          diesel::result::Error::NotFound => Ok(None),
          _ => Err(PgStoreError::from(e))
        }
      }
    }).await?
  }
}

impl NewRecoveredMessage {
  pub async fn insert(self, app_st: web::Data<AppState>) -> Result<i64, PgStoreError> {
    task::spawn_blocking(move || {
      use crate::schema::recovered_msgs::dsl::id;

      let conn = app_st.db_pool.get()?;
      let insert_res = diesel::insert_into(recovered_msgs::table)
        .values(&self)
        .returning(id)
        .get_result::<i64>(&conn);
      if let Err(e) = insert_res.as_ref() {
        if let Error::DatabaseError(de, _) = e {
          if let DatabaseErrorKind::UniqueViolation = de {
            use crate::schema::recovered_msgs::dsl::*;
            return Ok(recovered_msgs
              .filter(msg_tag.eq(self.msg_tag))
              .filter(epoch_tag.eq(self.epoch_tag))
              .select(id)
              .first::<i64>(&conn)?);
          }
        }
      }
      Ok(insert_res?)
    }).await?
  }
}
