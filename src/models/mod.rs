mod error;
mod pending_msg;
mod recovered_msg;

use diesel::connection::TransactionManager;
use diesel::Connection;
pub use error::*;
pub use pending_msg::*;
pub use recovered_msg::*;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool, PooledConnection};
use std::env;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::profiler::Profiler;

const DATABASE_URL_ENV_KEY: &str = "DATABASE_URL";
const TEST_DATABASE_URL_ENV_KEY: &str = "TEST_DATABASE_URL";
const MAX_CONN_ENV_KEY: &str = "DATABASE_MAX_CONN";
const MAX_CONN_DEFAULT: &str = "100";
const DB_POOL_TIMEOUT_SECS: u64 = 3600;
const DB_POOL_POLL_MS: u64 = 100;

pub type DBConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub struct DBPool {
  inner_pool: Pool<ConnectionManager<PgConnection>>,
}

impl DBPool {
  pub fn new(use_test_db: bool) -> Self {
    let env_key = if use_test_db {
      TEST_DATABASE_URL_ENV_KEY
    } else {
      DATABASE_URL_ENV_KEY
    };
    let db_url =
      env::var(env_key).unwrap_or_else(|_| panic!("{} env var must be defined", env_key));
    let pool_max_size =
      u32::from_str(&env::var(MAX_CONN_ENV_KEY).unwrap_or(MAX_CONN_DEFAULT.to_string()))
        .unwrap_or_else(|_| panic!("{} must be a positive integer", MAX_CONN_ENV_KEY));

    let db_mgr = ConnectionManager::new(db_url);
    let mut builder = Pool::builder();
    builder = if use_test_db {
      builder
        .connection_customizer(Box::new(TestConnectionCustomizer))
        .min_idle(Some(1))
        .max_size(1)
    } else {
      builder.max_size(pool_max_size)
    };

    Self {
      inner_pool: builder.build(db_mgr).expect("Failed to create db pool"),
    }
  }

  pub async fn get(&self) -> Result<DBConnection, PgStoreError> {
    let timeout_duration = Duration::from_secs(DB_POOL_TIMEOUT_SECS);
    let poll_duration = Duration::from_millis(DB_POOL_POLL_MS);
    let start_instant = Instant::now();
    while start_instant.elapsed() < timeout_duration {
      if let Some(conn) = self.inner_pool.try_get() {
        return Ok(conn);
      }
      sleep(poll_duration).await;
    }
    Err(PgStoreError::PoolTimeout)
  }
}

pub fn begin_transaction(store_conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError> {
  let store_conn_lock = store_conn.lock().unwrap();
  Ok(
    store_conn_lock
      .transaction_manager()
      .begin_transaction(store_conn_lock.deref())
      .map_err(|e| PgStoreError::from(e))?,
  )
}

pub fn commit_transaction(store_conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError> {
  let store_conn_lock = store_conn.lock().unwrap();
  Ok(
    store_conn_lock
      .transaction_manager()
      .commit_transaction(store_conn_lock.deref())
      .map_err(|e| PgStoreError::from(e))?,
  )
}

#[async_trait]
pub trait BatchInsert<T> {
  async fn insert_batch(
    self,
    conn: Arc<Mutex<DBConnection>>,
    profiler: Arc<Profiler>,
  ) -> Result<(), PgStoreError>;
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
