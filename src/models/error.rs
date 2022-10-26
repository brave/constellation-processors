use derive_more::{Display, Error, From};
use tokio::task::JoinError;

#[derive(From, Error, Debug, Display)]
pub enum PgStoreError {
  #[display(fmt = "diesel error: {}", "_0")]
  Diesel(diesel::result::Error),
  #[display(fmt = "r2d2 error: {}", "_0")]
  R2D2(r2d2::Error),
  #[display(fmt = "error joining task result: {}", "_0")]
  Join(JoinError),
  #[display(fmt = "DB pool timeout")]
  PoolTimeout,
}
