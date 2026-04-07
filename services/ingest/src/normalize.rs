use std::time::{SystemTime, UNIX_EPOCH};

/// Returns current time as unix milliseconds.
pub fn local_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_millis() as u64
}
