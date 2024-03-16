use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::{env, str::FromStr};

pub fn parse_env_var<F>(env_key: &str, default: &str) -> F
where
  F: FromStr,
  <F as FromStr>::Err: Debug,
{
  F::from_str(&env::var(env_key).unwrap_or_else(|_| default.to_string())).unwrap()
}

pub fn most_common_value<T, I>(iterator: I) -> Option<T>
where
  T: Hash + Eq,
  I: Iterator<Item = T>,
{
  let mut occurrences: HashMap<T, usize> = HashMap::new();

  for value in iterator {
    *occurrences.entry(value).or_insert(0) += 1;
  }

  occurrences
    .into_iter()
    .max_by_key(|(_, v)| *v)
    .map(|(k, _)| k)
}
