mod error;
mod pending_msg;
mod recovered_msg;

pub use error::*;
pub use pending_msg::*;
pub use recovered_msg::*;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use std::env;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

const DATABASE_URL_ENV_KEY: &str = "DATABASE_URL";
const MAX_CONN_ENV_KEY: &str = "DATABASE_MAX_CONN";
const MAX_CONN_DEFAULT: &str = "100";

pub type DBPool = Pool<ConnectionManager<PgConnection>>;
pub type DBConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn create_db_pool() -> DBPool {
  let db_url = env::var(DATABASE_URL_ENV_KEY)
    .unwrap_or_else(|_| panic!("{} env var must be defined", DATABASE_URL_ENV_KEY));
  let pool_max_size =
    u32::from_str(&env::var(MAX_CONN_ENV_KEY).unwrap_or(MAX_CONN_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", MAX_CONN_ENV_KEY));

  let db_mgr = ConnectionManager::new(db_url);
  Pool::builder()
    .max_size(pool_max_size)
    .build(db_mgr)
    .expect("Failed to create db pool")
}

#[async_trait]
pub trait BatchInsert<T> {
  async fn insert_batch(self, conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError>;
}
