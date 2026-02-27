use alloc::{boxed::Box, vec};
use core::fmt;

use crate::{BlockReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

pub struct AlignedBlockReader<S> {
    inner: S,
    block_size: u32,
    inner_block_size: u32,
    size_bytes: u64,
    total_blocks: u64,
}

impl<S> AlignedBlockReader<S>
where
    S: BlockReader,
{
    pub async fn new(inner: S, block_size: u32) -> GibbloxResult<Self> {
        if block_size == 0 || !block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero power of two",
            ));
        }

        let inner_block_size = inner.block_size();
        if inner_block_size == 0 || !inner_block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "inner block size must be non-zero power of two",
            ));
        }

        let inner_total_blocks = inner.total_blocks().await?;
        let size_bytes = inner_total_blocks
            .checked_mul(inner_block_size as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "source size overflow")
            })?;
        let total_blocks = size_bytes.div_ceil(block_size as u64);

        Ok(Self {
            inner,
            block_size,
            inner_block_size,
            size_bytes,
            total_blocks,
        })
    }

    pub fn inner_block_size(&self) -> u32 {
        self.inner_block_size
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[async_trait::async_trait]
impl<S> BlockReader for AlignedBlockReader<S>
where
    S: BlockReader,
{
    fn block_size(&self) -> u32 {
        self.block_size
    }

    async fn total_blocks(&self) -> GibbloxResult<u64> {
        Ok(self.total_blocks)
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("aligned-block:(")?;
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

        let requested_blocks = (buf.len() / self.block_size as usize) as u64;
        let end_lba = lba.checked_add(requested_blocks).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "lba overflow")
        })?;
        if end_lba > self.total_blocks {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "requested block out of range",
            ));
        }

        let offset = lba.checked_mul(self.block_size as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset overflow")
        })?;
        let available = self.size_bytes.checked_sub(offset).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset out of range")
        })?;
        let read_len = (buf.len() as u64).min(available) as usize;

        let inner_bs = self.inner_block_size as u64;
        let aligned_start = (offset / inner_bs) * inner_bs;
        let aligned_end = offset
            .checked_add(read_len as u64)
            .and_then(|end| end.div_ceil(inner_bs).checked_mul(inner_bs))
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
            .read_blocks(aligned_start / inner_bs, &mut scratch, ctx)
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
        buf[..read_len].copy_from_slice(&scratch[head..tail]);
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

    use crate::{BlockReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

    struct RecordingReader {
        block_size: u32,
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    #[async_trait]
    impl BlockReader for RecordingReader {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        async fn total_blocks(&self) -> GibbloxResult<u64> {
            Ok(self.data.len().div_ceil(self.block_size as usize) as u64)
        }

        fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
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

            if !buf.len().is_multiple_of(self.block_size as usize) {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    "buffer length must align to block size",
                ));
            }

            let offset = (lba as usize)
                .checked_mul(self.block_size as usize)
                .ok_or_else(|| {
                    GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset overflow")
                })?;
            if offset >= self.data.len() {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "requested block out of range",
                ));
            }

            let read = (self.data.len() - offset).min(buf.len());
            buf[..read].copy_from_slice(&self.data[offset..offset + read]);
            if read < buf.len() {
                buf[read..].fill(0);
            }

            Ok(buf.len())
        }
    }

    #[test]
    fn aligned_block_reader_downshifts_reads() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(16 * 1024);
        let source = RecordingReader {
            block_size: 4096,
            data: data.clone(),
            requests: Arc::clone(&requests),
        };
        let reader = block_on(super::AlignedBlockReader::new(source, 512)).expect("create reader");

        let mut out = vec![0u8; 512];
        block_on(reader.read_blocks(1, &mut out, ReadContext::FOREGROUND)).expect("read block");

        assert_eq!(&out[..], &data[512..1024]);
        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(0, 4096)]);
    }

    #[test]
    fn aligned_block_reader_rejects_zero_block_size() {
        let source = RecordingReader {
            block_size: 4096,
            data: patterned_data(8192),
            requests: Arc::new(Mutex::new(Vec::new())),
        };

        let err = match block_on(super::AlignedBlockReader::new(source, 0)) {
            Ok(_) => panic!("zero block size should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn aligned_block_reader_rejects_unaligned_buffer() {
        let source = RecordingReader {
            block_size: 4096,
            data: patterned_data(8192),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let reader = block_on(super::AlignedBlockReader::new(source, 512)).expect("create reader");

        let mut out = vec![0u8; 513];
        let err = block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND))
            .expect_err("unaligned buffer should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    fn patterned_data(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (idx, byte) in out.iter_mut().enumerate() {
            *byte = ((idx * 17) % 251) as u8;
        }
        out
    }
}
