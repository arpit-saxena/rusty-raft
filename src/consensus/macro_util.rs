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
