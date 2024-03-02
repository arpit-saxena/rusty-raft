use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use thiserror::Error;

pub struct AtomicDuration {
    duration_nanos: AtomicU64,
}

#[derive(Error, Debug)]
pub enum AtomicDurationError {
    #[error("Can only represent durations whose nanoseconds fit into 64 bits")]
    DurationNotSupported,
}

impl AtomicDuration {
    fn duration_to_nanos(duration: Duration) -> Result<u64, AtomicDurationError> {
        let nanos = duration.as_nanos();
        if nanos > u64::MAX as u128 {
            return Err(AtomicDurationError::DurationNotSupported);
        }
        Ok(nanos as u64)
    }

    pub fn new(duration: Duration) -> Result<AtomicDuration, AtomicDurationError> {
        let nanos = Self::duration_to_nanos(duration)?;
        Ok(AtomicDuration {
            duration_nanos: AtomicU64::new(nanos),
        })
    }

    pub fn load(&self, ordering: Ordering) -> Duration {
        let nanos = self.duration_nanos.load(ordering);
        Duration::from_nanos(nanos)
    }

    pub fn store(&self, duration: Duration, ordering: Ordering) -> Result<(), AtomicDurationError> {
        let nanos = Self::duration_to_nanos(duration)?;
        self.duration_nanos.store(nanos, ordering);
        Ok(())
    }
}
