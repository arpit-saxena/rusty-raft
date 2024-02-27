use std::{
    fmt::Debug,
    sync::atomic::{AtomicI32, AtomicU32, Ordering},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/******************************* Generic Byte IO ********************************/
pub trait GenericByteIO<T> {
    async fn write_little_endian(&mut self, data: T) -> std::io::Result<()>;
    async fn read_little_endian(&mut self) -> std::io::Result<T>;
}

macro_rules! impl_byte_writer {
    ($type:ty, $read_method:ident, $write_method:ident) => {
        impl<StateFile> GenericByteIO<$type> for StateFile
        where
            StateFile: super::StateFile,
        {
            async fn write_little_endian(&mut self, data: $type) -> std::io::Result<()> {
                self.$write_method(data).await?;
                Ok(())
            }
            async fn read_little_endian(&mut self) -> std::io::Result<$type> {
                let data = self.$read_method().await?;
                Ok(data)
            }
        }
    };
}

impl_byte_writer!(u32, read_u32_le, write_u32_le);
impl_byte_writer!(i32, read_i32_le, write_i32_le);

/******************************* Atomic<T> ********************************/
// Taken from a comment on https://stackoverflow.com/a/57075792/5585431
pub trait Atomize {
    type Atom;

    fn atomize(self) -> Self::Atom;
    fn load(this: &Self::Atom, ordering: Ordering) -> Self;
    fn store(this: &Self::Atom, val: Self, ordering: Ordering);
    fn swap(this: &Self::Atom, val: Self, ordering: Ordering) -> Self;
    /* ... */
}

macro_rules! impl_has_atomic {
    ($t:ty, $atom:ty) => {
        impl Atomize for $t {
            type Atom = $atom;

            fn atomize(self) -> Self::Atom {
                Self::Atom::from(self)
            }

            fn load(this: &Self::Atom, ordering: Ordering) -> Self {
                this.load(ordering)
            }

            fn store(this: &Self::Atom, val: Self, ordering: Ordering) {
                this.store(val, ordering)
            }

            fn swap(this: &Self::Atom, val: Self, ordering: Ordering) -> Self {
                this.swap(val, ordering)
            }
        }
    };
}

impl_has_atomic!(i32, AtomicI32);
impl_has_atomic!(u32, AtomicU32);

pub struct Atomic<T: Atomize + Debug>(T::Atom);

impl<T: Atomize + Debug> Atomic<T> {
    pub fn from(value: T) -> Self {
        Atomic(value.atomize())
    }

    pub fn load(&self, ordering: Ordering) -> T {
        T::load(&self.0, ordering)
    }

    pub fn store(&self, val: T, ordering: Ordering) {
        T::store(&self.0, val, ordering)
    }

    pub fn swap(&self, val: T, ordering: Ordering) -> T {
        T::swap(&self.0, val, ordering)
    }
}

impl<T: Atomize + Debug> Debug for Atomic<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Atomic")
            .field(&self.load(Ordering::SeqCst))
            .finish()
    }
}
