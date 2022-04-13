mod error;
mod pending_msg;
mod recovered_msg;

pub use error::*;
pub use pending_msg::*;
pub use recovered_msg::*;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::env;
use std::str::FromStr;
use std::sync::Arc;

pub type DBPool = Pool<ConnectionManager<PgConnection>>;

pub fn create_db_pool() -> DBPool {
  let db_url = env::var("DATABASE_URL").expect("DATABASE_URL env var must be defined");
  let pool_max_size = u32::from_str(&env::var("DATABASE_MAX_CONN").unwrap_or("16".to_string()))
    .expect("DATABASE_MAX_CONN must be a positive integer");

  let db_mgr = ConnectionManager::new(db_url);
  Pool::builder()
    .max_size(pool_max_size)
    .build(db_mgr)
    .expect("Failed to create db pool")
}

#[async_trait]
pub trait BatchInsert<T> {
  async fn insert_batch(self, pool: Arc<DBPool>) -> Result<(), PgStoreError>;
}

#[async_trait]
pub trait BatchDelete<T> {
  async fn delete_batch(&self, pool: Arc<DBPool>) -> Result<(), PgStoreError>;
}
