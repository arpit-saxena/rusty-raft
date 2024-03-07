use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};

pub struct AtomicDuration {
    duration_nanos: AtomicU64,
}

#[derive(Error, Debug)]
pub enum AtomicTimeError {
    #[error("Can only represent durations whose nanoseconds fit into 64 bits")]
    DurationNotSupported,
    #[error("Can only store Instants which are greater than or equal to the initial Instant")]
    InstantNotSupported,
}

impl AtomicDuration {
    fn duration_to_nanos(duration: Duration) -> Result<u64, AtomicTimeError> {
        let nanos = duration.as_nanos();
        if nanos > u64::MAX as u128 {
            return Err(AtomicTimeError::DurationNotSupported);
        }
        Ok(nanos as u64)
    }

    pub fn new(duration: Duration) -> Result<AtomicDuration, AtomicTimeError> {
        let nanos = Self::duration_to_nanos(duration)?;
        Ok(AtomicDuration {
            duration_nanos: AtomicU64::new(nanos),
        })
    }

    pub fn load(&self, ordering: Ordering) -> Duration {
        let nanos = self.duration_nanos.load(ordering);
        Duration::from_nanos(nanos)
    }

    pub fn store(&self, duration: Duration, ordering: Ordering) -> Result<(), AtomicTimeError> {
        let nanos = Self::duration_to_nanos(duration)?;
        self.duration_nanos.store(nanos, ordering);
        Ok(())
    }
}

pub struct AtomicInstant {
    base: Instant,
    offset: AtomicDuration,
}

#[derive(Error, Debug)]
pub enum AtomicInstantError {}

impl AtomicInstant {
    pub fn new(instant: Instant) -> Result<AtomicInstant, AtomicTimeError> {
        Ok(AtomicInstant {
            base: instant,
            offset: AtomicDuration::new(Duration::ZERO)?,
        })
    }

    pub fn load(&self, ordering: Ordering) -> Instant {
        self.base + self.offset.load(ordering)
    }

    pub fn store(&self, instant: Instant, ordering: Ordering) -> Result<(), AtomicTimeError> {
        if instant < self.base {
            return Err(AtomicTimeError::InstantNotSupported);
        }

        let offset = instant - self.base;
        self.offset.store(offset, ordering)?;
        Ok(())
    }
}

/// Implement new function for tokio::sync::watch::Sender that has an easier interface than the provided
/// send_ conditional functions.
pub trait UpdateIfNew {
    type Item;
    fn send_if_new(&self, item: Self::Item) -> Self::Item;
}

impl<T> UpdateIfNew for watch::Sender<T>
where
    T: Copy + PartialEq,
{
    type Item = T;

    fn send_if_new(&self, item: Self::Item) -> Self::Item {
        let mut old_item = *self.borrow();
        self.send_if_modified(|current_item| {
            old_item = std::mem::replace(current_item, item);
            item != old_item
        });
        old_item
    }
}
