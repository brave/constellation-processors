mod error;
mod pending_msg;
mod recovered_msg;

pub use error::*;
pub use pending_msg::*;
pub use recovered_msg::*;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool, PooledConnection};
use std::env;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

const DATABASE_URL_ENV_KEY: &str = "DATABASE_URL";
const TEST_DATABASE_URL_ENV_KEY: &str = "TEST_DATABASE_URL";
const MAX_CONN_ENV_KEY: &str = "DATABASE_MAX_CONN";
const MAX_CONN_DEFAULT: &str = "100";

pub type DBPool = Pool<ConnectionManager<PgConnection>>;
pub type DBConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn create_db_pool(use_test_db: bool) -> DBPool {
  let env_key = if use_test_db {
    TEST_DATABASE_URL_ENV_KEY
  } else {
    DATABASE_URL_ENV_KEY
  };
  let db_url = env::var(env_key).unwrap_or_else(|_| panic!("{} env var must be defined", env_key));
  let pool_max_size =
    u32::from_str(&env::var(MAX_CONN_ENV_KEY).unwrap_or(MAX_CONN_DEFAULT.to_string()))
      .unwrap_or_else(|_| panic!("{} must be a positive integer", MAX_CONN_ENV_KEY));

  let db_mgr = ConnectionManager::new(db_url);
  let mut builder = Pool::builder();
  if use_test_db {
    builder = builder
      .connection_customizer(Box::new(TestConnectionCustomizer))
      .min_idle(Some(1))
      .max_size(1);
  } else {
    builder = builder.max_size(pool_max_size);
  }
  builder.build(db_mgr).expect("Failed to create db pool")
}

#[async_trait]
pub trait BatchInsert<T> {
  async fn insert_batch(self, conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError>;
}

#[derive(Debug)]
pub struct TestConnectionCustomizer;

impl<C, E> CustomizeConnection<C, E> for TestConnectionCustomizer
where
  C: diesel::Connection,
{
  fn on_acquire(&self, conn: &mut C) -> Result<(), E> {
    conn
      .begin_test_transaction()
      .expect("Failed to begin test transaction");
    Ok(())
  }
}
