use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};

use crate::{BlockReader, ByteReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

#[derive(Clone)]
pub struct AlignedByteReader {
    inner: Arc<dyn BlockReader>,
    block_size: u32,
    size_bytes: u64,
}

impl AlignedByteReader {
    pub async fn new(inner: Arc<dyn BlockReader>) -> GibbloxResult<Self> {
        let block_size = inner.block_size();
        if block_size == 0 || !block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero power of two",
            ));
        }

        let total_blocks = inner.total_blocks().await?;
        let size_bytes = total_blocks.checked_mul(block_size as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "source size overflow")
        })?;

        Ok(Self {
            inner,
            block_size,
            size_bytes,
        })
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub async fn read_vec_at(
        &self,
        offset: u64,
        len: usize,
        ctx: ReadContext,
    ) -> GibbloxResult<Vec<u8>> {
        let mut out = vec![0u8; len];
        self.read_exact_at(offset, &mut out, ctx).await?;
        Ok(out)
    }

    pub async fn read_exact_at(
        &self,
        offset: u64,
        out: &mut [u8],
        ctx: ReadContext,
    ) -> GibbloxResult<()> {
        if out.is_empty() {
            return Ok(());
        }

        let end = offset.checked_add(out.len() as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "range overflow")
        })?;
        if end > self.size_bytes {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "read range exceeds source size",
            ));
        }

        let read = self.read_at(offset, out, ctx).await?;
        if read != out.len() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short read from block source",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ByteReader for AlignedByteReader {
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        Ok(self.size_bytes)
    }

    fn write_identity(&self, out: &mut dyn core::fmt::Write) -> core::fmt::Result {
        self.inner.write_identity(out)
    }

    async fn read_at(&self, offset: u64, out: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if offset >= self.size_bytes {
            return Ok(0);
        }

        let read_len = usize::try_from(self.size_bytes - offset)
            .unwrap_or(usize::MAX)
            .min(out.len());
        if read_len == 0 {
            return Ok(0);
        }

        let block_size_u64 = self.block_size as u64;
        let block_size = self.block_size as usize;

        if offset % block_size_u64 == 0 && read_len % block_size == 0 {
            let read = self
                .inner
                .read_blocks(offset / block_size_u64, &mut out[..read_len], ctx)
                .await?;
            if read != read_len {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    "short aligned read from block source",
                ));
            }
            return Ok(read_len);
        }

        let aligned_start = (offset / block_size_u64) * block_size_u64;
        let end = offset.checked_add(read_len as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "range overflow")
        })?;
        let aligned_end = end
            .div_ceil(block_size_u64)
            .checked_mul(block_size_u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "aligned range overflow")
            })?;
        let aligned_len = usize::try_from(aligned_end - aligned_start).map_err(|_| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "aligned read length exceeds addressable memory",
            )
        })?;

        let mut scratch = vec![0u8; aligned_len];
        let read = self
            .inner
            .read_blocks(aligned_start / block_size_u64, &mut scratch, ctx)
            .await?;
        if read != scratch.len() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short aligned read from block source",
            ));
        }

        let head = usize::try_from(offset - aligned_start).map_err(|_| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "aligned read head offset exceeds addressable memory",
            )
        })?;
        let tail = head.checked_add(read_len).ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "aligned read tail offset overflow",
            )
        })?;

        out[..read_len].copy_from_slice(&scratch[head..tail]);
        Ok(read_len)
    }
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::executor::block_on;

    use crate::{BlockReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

    struct RecordingReader {
        block_size: u32,
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    struct PointerRecordingReader {
        block_size: u32,
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<(u64, usize, usize)>>>,
    }

    #[async_trait]
    impl BlockReader for RecordingReader {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        async fn total_blocks(&self) -> GibbloxResult<u64> {
            Ok(self.data.len().div_ceil(self.block_size as usize) as u64)
        }

        fn write_identity(&self, out: &mut dyn core::fmt::Write) -> core::fmt::Result {
            out.write_str("recording")
        }

        async fn read_blocks(
            &self,
            lba: u64,
            buf: &mut [u8],
            _ctx: ReadContext,
        ) -> GibbloxResult<usize> {
            self.requests
                .lock()
                .expect("lock request log")
                .push((lba, buf.len()));

            let offset = (lba as usize)
                .checked_mul(self.block_size as usize)
                .ok_or_else(|| {
                    GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset overflow")
                })?;
            let end = offset.checked_add(buf.len()).ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "range overflow")
            })?;
            if end > self.data.len() {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "read exceeds backing store",
                ));
            }
            buf.copy_from_slice(&self.data[offset..end]);
            Ok(buf.len())
        }
    }

    #[async_trait]
    impl BlockReader for PointerRecordingReader {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        async fn total_blocks(&self) -> GibbloxResult<u64> {
            Ok(self.data.len().div_ceil(self.block_size as usize) as u64)
        }

        fn write_identity(&self, out: &mut dyn core::fmt::Write) -> core::fmt::Result {
            out.write_str("pointer-recording")
        }

        async fn read_blocks(
            &self,
            lba: u64,
            buf: &mut [u8],
            _ctx: ReadContext,
        ) -> GibbloxResult<usize> {
            self.requests.lock().expect("lock request log").push((
                lba,
                buf.len(),
                buf.as_ptr() as usize,
            ));

            let offset = (lba as usize)
                .checked_mul(self.block_size as usize)
                .ok_or_else(|| {
                    GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset overflow")
                })?;
            let end = offset.checked_add(buf.len()).ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "range overflow")
            })?;
            if end > self.data.len() {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "read exceeds backing store",
                ));
            }
            buf.copy_from_slice(&self.data[offset..end]);
            Ok(buf.len())
        }
    }

    #[test]
    fn aligned_byte_reader_coalesces_unaligned_reads() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(1024 * 1024);
        let source: Arc<dyn BlockReader> = Arc::new(RecordingReader {
            block_size: 512,
            data,
            requests: Arc::clone(&requests),
        });
        let reader = block_on(super::AlignedByteReader::new(source)).expect("create reader");

        let mut out = vec![0u8; 64 * 1024];
        block_on(reader.read_exact_at(123, &mut out, ReadContext::FOREGROUND))
            .expect("read unaligned range");

        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(0, 129 * 512)]);
    }

    #[test]
    fn aligned_byte_reader_uses_caller_buffer_for_aligned_reads() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(1024 * 1024);
        let source: Arc<dyn BlockReader> = Arc::new(PointerRecordingReader {
            block_size: 512,
            data: data.clone(),
            requests: Arc::clone(&requests),
        });
        let reader = block_on(super::AlignedByteReader::new(source)).expect("create reader");

        let mut out = vec![0u8; 64 * 1024];
        let out_ptr = out.as_ptr() as usize;
        block_on(reader.read_exact_at(512, &mut out, ReadContext::FOREGROUND))
            .expect("read aligned range");

        assert_eq!(&out[..], &data[512..512 + out.len()]);

        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(1, out.len(), out_ptr)]);
    }

    #[test]
    fn aligned_byte_reader_rejects_out_of_bounds_read() {
        let source: Arc<dyn BlockReader> = Arc::new(RecordingReader {
            block_size: 512,
            data: patterned_data(4096),
            requests: Arc::new(Mutex::new(Vec::new())),
        });
        let reader = block_on(super::AlignedByteReader::new(source)).expect("create reader");

        let mut out = vec![0u8; 32];
        let err = block_on(reader.read_exact_at(4090, &mut out, ReadContext::FOREGROUND))
            .expect_err("out-of-bounds read should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::OutOfRange);
    }

    #[test]
    fn aligned_byte_reader_rejects_zero_block_size() {
        let source: Arc<dyn BlockReader> = Arc::new(RecordingReader {
            block_size: 0,
            data: patterned_data(4096),
            requests: Arc::new(Mutex::new(Vec::new())),
        });
        let err = match block_on(super::AlignedByteReader::new(source)) {
            Ok(_) => panic!("zero block size should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    fn patterned_data(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (idx, byte) in out.iter_mut().enumerate() {
            *byte = ((idx * 31) % 251) as u8;
        }
        out
    }
}
