use std::fmt::Debug;
use std::{env, str::FromStr};

pub fn parse_env_var<F>(env_key: &str, default: &str) -> F
where
  F: FromStr,
  <F as FromStr>::Err: Debug,
{
  F::from_str(&env::var(env_key).unwrap_or_else(|_| default.to_string())).unwrap()
}
