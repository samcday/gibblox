use alloc::boxed::Box;
use core::fmt;

use crate::{BlockReader, ByteReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

/// Explicit Byte->Block boundary adapter.
///
/// This is the canonical place where a byte stream's tail is zero-extended to
/// satisfy fixed-size logical block reads.
pub struct BlockByteReader<S> {
    inner: S,
    block_size: u32,
}

impl<S> BlockByteReader<S> {
    pub fn new(inner: S, block_size: u32) -> GibbloxResult<Self> {
        if block_size == 0 || !block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero power of two",
            ));
        }

        Ok(Self { inner, block_size })
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

#[async_trait::async_trait]
impl<S> BlockReader for BlockByteReader<S>
where
    S: ByteReader,
{
    fn block_size(&self) -> u32 {
        self.block_size
    }

    async fn total_blocks(&self) -> GibbloxResult<u64> {
        Ok(self
            .inner
            .size_bytes()
            .await?
            .div_ceil(self.block_size as u64))
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("block-byte:(")?;
        self.inner.write_identity(out)?;
        write!(out, "):block_size={}", self.block_size)
    }

    async fn read_blocks(
        &self,
        lba: u64,
        buf: &mut [u8],
        ctx: ReadContext,
    ) -> GibbloxResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if !buf.len().is_multiple_of(self.block_size as usize) {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "buffer length must align to block size",
            ));
        }

        let total_blocks = self.total_blocks().await?;
        if lba >= total_blocks {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "requested block out of range",
            ));
        }

        let offset = lba.checked_mul(self.block_size as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "lba overflow")
        })?;
        let size_bytes = self.inner.size_bytes().await?;
        let available = size_bytes.checked_sub(offset).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset out of range")
        })?;
        let read_len = (buf.len() as u64).min(available) as usize;
        let read = self
            .inner
            .read_at(offset, &mut buf[..read_len], ctx)
            .await?;
        if read != read_len {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short read from byte source",
            ));
        }
        if read_len < buf.len() {
            buf[read_len..].fill(0);
        }
        Ok(buf.len())
    }
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};
    use core::fmt;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::executor::block_on;

    use crate::{BlockReader, ByteReader, GibbloxErrorKind, GibbloxResult, ReadContext};

    struct RecordingByteReader {
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    #[async_trait]
    impl ByteReader for RecordingByteReader {
        async fn size_bytes(&self) -> GibbloxResult<u64> {
            Ok(self.data.len() as u64)
        }

        fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
            out.write_str("recording-byte")
        }

        async fn read_at(
            &self,
            offset: u64,
            out: &mut [u8],
            _ctx: ReadContext,
        ) -> GibbloxResult<usize> {
            self.requests
                .lock()
                .expect("lock request log")
                .push((offset, out.len()));

            if out.is_empty() {
                return Ok(0);
            }
            if offset >= self.data.len() as u64 {
                return Ok(0);
            }

            let read_len = (out.len() as u64).min(self.data.len() as u64 - offset) as usize;
            let start = offset as usize;
            out[..read_len].copy_from_slice(&self.data[start..start + read_len]);
            Ok(read_len)
        }
    }

    #[test]
    fn block_byte_reader_reads_and_zero_pads_tail() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(4097);
        let reader = super::BlockByteReader::new(
            RecordingByteReader {
                data: data.clone(),
                requests: Arc::clone(&requests),
            },
            512,
        )
        .expect("create reader");

        let mut out = vec![0u8; 512];
        let read =
            block_on(reader.read_blocks(8, &mut out, ReadContext::FOREGROUND)).expect("read");
        assert_eq!(read, 512);
        assert_eq!(out[0], data[4096]);
        assert!(out[1..].iter().all(|byte| *byte == 0));

        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(4096, 1)]);
    }

    #[test]
    fn block_byte_reader_rejects_out_of_range() {
        let reader = super::BlockByteReader::new(
            RecordingByteReader {
                data: patterned_data(1024),
                requests: Arc::new(Mutex::new(Vec::new())),
            },
            512,
        )
        .expect("create reader");

        let mut out = vec![0u8; 512];
        let err = block_on(reader.read_blocks(2, &mut out, ReadContext::FOREGROUND))
            .expect_err("out-of-range read should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::OutOfRange);
    }

    #[test]
    fn block_byte_reader_rejects_invalid_block_size() {
        let err = match super::BlockByteReader::new(
            RecordingByteReader {
                data: patterned_data(1024),
                requests: Arc::new(Mutex::new(Vec::new())),
            },
            0,
        ) {
            Ok(_) => panic!("zero block size should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    fn patterned_data(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (idx, byte) in out.iter_mut().enumerate() {
            *byte = ((idx * 29) % 251) as u8;
        }
        out
    }
}
