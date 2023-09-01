mod error;
mod pending_msg;
mod recovered_msg;

use diesel::connection::TransactionManager;
use diesel::Connection;
pub use error::*;
pub use pending_msg::*;
use r2d2::ManageConnection;
pub use recovered_msg::*;

use async_trait::async_trait;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, CustomizeConnection, Pool, PooledConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use rand::{seq::SliceRandom, thread_rng};
use std::env;
use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::channel::get_data_channel_value_from_env;
use crate::profiler::Profiler;

const DATABASE_URL_ENV_KEY: &str = "DATABASE_URL";
const TEST_DATABASE_URL_ENV_KEY: &str = "TEST_DATABASE_URL";
const DATABASE_NAMES_ENV_KEY: &str = "DATABASE_NAMES";
const DEFAULT_DATABASE_NAMES: &str = "typical=postgres";
const MAX_CONN_ENV_KEY: &str = "DATABASE_MAX_CONN";
const MAX_CONN_DEFAULT: &str = "100";
const MAX_WRITE_CONN_ENV_KEY: &str = "DATABASE_MAX_WRITE_CONN";
const MAX_WRITE_CONN_DEFAULT: &str = "8";
const DB_POOL_TIMEOUT_SECS: u64 = 3600;
const DB_POOL_POLL_MS: u64 = 100;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub type DBConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub enum DBConnectionType<'a> {
  #[allow(dead_code)]
  Test,
  Normal {
    channel_name: &'a str,
  },
}

pub struct DBPool {
  inner_pool: Pool<ConnectionManager<PgConnection>>,
}

fn get_channel_db_url<'a>(conn_type: &DBConnectionType<'a>) -> String {
  let env_key = match conn_type {
    DBConnectionType::Test => TEST_DATABASE_URL_ENV_KEY,
    DBConnectionType::Normal { .. } => DATABASE_URL_ENV_KEY,
  };
  let db_url = env::var(env_key).unwrap_or_else(|_| panic!("{} env var must be defined", env_key));
  match conn_type {
    DBConnectionType::Test => db_url,
    DBConnectionType::Normal { channel_name } => {
      let database_name = get_data_channel_value_from_env(
        DATABASE_NAMES_ENV_KEY,
        DEFAULT_DATABASE_NAMES,
        channel_name,
      );
      format!("{}/{}", db_url, database_name)
    }
  }
}

impl DBPool {
  pub fn new<'a>(conn_type: DBConnectionType<'a>) -> Self {
    let db_url = get_channel_db_url(&conn_type);
    let pool_max_size =
      u32::from_str(&env::var(MAX_CONN_ENV_KEY).unwrap_or(MAX_CONN_DEFAULT.to_string()))
        .unwrap_or_else(|_| panic!("{} must be a positive integer", MAX_CONN_ENV_KEY));

    let db_mgr: ConnectionManager<PgConnection> = ConnectionManager::new(db_url);

    db_mgr
      .connect()
      .expect("could not connect to db it run migrations")
      .run_pending_migrations(MIGRATIONS)
      .expect("failed to run migrations");

    let mut builder = Pool::builder();
    builder = if let DBConnectionType::Test = conn_type {
      builder
        .connection_customizer(Box::new(TestConnectionCustomizer))
        .min_idle(Some(1))
        .max_size(1)
    } else {
      builder
        .min_idle(Some(pool_max_size))
        .max_size(pool_max_size)
        .max_lifetime(None)
        .idle_timeout(None)
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

pub struct DBStorageConnections {
  conns: Vec<Arc<Mutex<DBConnection>>>,
}

impl DBStorageConnections {
  pub async fn new(db_pool: &Arc<DBPool>, using_test_db: bool) -> Result<Self, PgStoreError> {
    let conn_count = if using_test_db {
      1
    } else {
      usize::from_str(
        &env::var(MAX_WRITE_CONN_ENV_KEY).unwrap_or(MAX_WRITE_CONN_DEFAULT.to_string()),
      )
      .unwrap_or_else(|_| panic!("{} must be a positive integer", MAX_WRITE_CONN_ENV_KEY))
    };
    let mut conns = Vec::new();
    for _ in 0..conn_count {
      let conn = Arc::new(Mutex::new(db_pool.get().await?));
      begin_db_transaction(conn.clone())?;
      conns.push(conn);
    }
    Ok(Self { conns })
  }

  pub fn get(&self) -> Arc<Mutex<DBConnection>> {
    self.conns.choose(&mut thread_rng()).unwrap().clone()
  }

  pub fn commit(&self) -> Result<(), PgStoreError> {
    for conn in &self.conns {
      commit_db_transaction(conn.clone())?;
    }
    Ok(())
  }
}

pub fn begin_db_transaction(conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError> {
  let mut conn_lock = conn.lock().unwrap();
  Ok(<DBConnection as Connection>::TransactionManager::begin_transaction(conn_lock.deref_mut())?)
}

pub fn commit_db_transaction(conn: Arc<Mutex<DBConnection>>) -> Result<(), PgStoreError> {
  let mut conn_lock = conn.lock().unwrap();
  Ok(<DBConnection as Connection>::TransactionManager::commit_transaction(conn_lock.deref_mut())?)
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
