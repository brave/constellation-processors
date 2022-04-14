use std::time::{Duration, SystemTime};

const SEC_IN_DAY: u64 = 86400;
const SEC_IN_WEEK: u64 = SEC_IN_DAY * 7;
// Our first epoch will start on Saturday, Jan 3, 1970, 9:00 UTC.
const FIRST_EPOCH_OFFSET_SEC: u64 = (86400 * 2) + (3600 * 9);

const FIRST_EPOCH: u8 = 0u8;
const LAST_EPOCH: u8 = 7u8;
const TOTAL_EPOCHS: u64 = (LAST_EPOCH - FIRST_EPOCH + 1) as u64;

const EPOCH_LIFETIME_WEEKS: usize = 4;

pub fn get_current_epoch() -> u8 {
  let first_epoch_start_time = SystemTime::UNIX_EPOCH
    .checked_add(Duration::from_secs(FIRST_EPOCH_OFFSET_SEC))
    .unwrap();
  let sec_since_first_epoch = SystemTime::now()
    .duration_since(first_epoch_start_time)
    .unwrap()
    .as_secs();

  let weeks_since_first_epoch = sec_since_first_epoch / SEC_IN_WEEK;
  let epoch_index = (weeks_since_first_epoch % TOTAL_EPOCHS) as u8;
  FIRST_EPOCH + epoch_index
}

pub fn is_epoch_expired(epoch: u8, mut current_epoch: u8) -> bool {
  let mut diff = 0;
  if current_epoch < FIRST_EPOCH || current_epoch > LAST_EPOCH {
    return true;
  }
  while current_epoch != epoch {
    if current_epoch == FIRST_EPOCH {
      current_epoch = LAST_EPOCH;
    } else {
      current_epoch -= 1;
    }

    diff += 1;
  }
  diff >= EPOCH_LIFETIME_WEEKS
}
