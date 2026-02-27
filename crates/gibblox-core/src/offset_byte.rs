use alloc::boxed::Box;
use core::fmt;

use crate::{ByteReader, GibbloxError, GibbloxErrorKind, GibbloxResult, ReadContext};

pub struct OffsetByteReader<S> {
    inner: S,
    offset: u64,
}

impl<S> OffsetByteReader<S> {
    pub fn new(inner: S, offset: u64) -> Self {
        Self { inner, offset }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

#[async_trait::async_trait]
impl<S> ByteReader for OffsetByteReader<S>
where
    S: ByteReader,
{
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        Ok(self.inner.size_bytes().await?.saturating_sub(self.offset))
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("offset-byte:(")?;
        self.inner.write_identity(out)?;
        write!(out, "):offset={}", self.offset)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let visible_size = self.inner.size_bytes().await?.saturating_sub(self.offset);
        if offset >= visible_size {
            return Ok(0);
        }

        let read_len = (buf.len() as u64).min(visible_size - offset) as usize;
        let inner_offset = self.offset.checked_add(offset).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset overflow")
        })?;

        self.inner
            .read_at(inner_offset, &mut buf[..read_len], ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, string::String, sync::Arc, vec, vec::Vec};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::executor::block_on;

    use crate::{
        AlignedBlockReader, BlockByteReader, BlockReader, ByteReader, GibbloxResult,
        OffsetByteReader, ReadContext,
    };

    struct RecordingByteReader {
        data: Vec<u8>,
        requests: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    #[async_trait]
    impl ByteReader for RecordingByteReader {
        async fn size_bytes(&self) -> GibbloxResult<u64> {
            Ok(self.data.len() as u64)
        }

        fn write_identity(&self, out: &mut dyn core::fmt::Write) -> core::fmt::Result {
            out.write_str("recording-byte")
        }

        async fn read_at(
            &self,
            offset: u64,
            buf: &mut [u8],
            _ctx: ReadContext,
        ) -> GibbloxResult<usize> {
            self.requests
                .lock()
                .expect("lock request log")
                .push((offset, buf.len()));

            if buf.is_empty() {
                return Ok(0);
            }
            if offset >= self.data.len() as u64 {
                return Ok(0);
            }

            let read_len = (buf.len() as u64).min(self.data.len() as u64 - offset) as usize;
            let start = offset as usize;
            let end = start + read_len;
            buf[..read_len].copy_from_slice(&self.data[start..end]);
            Ok(read_len)
        }
    }

    #[test]
    fn offset_byte_reader_translates_offsets() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(4096);
        let reader = OffsetByteReader::new(
            RecordingByteReader {
                data: data.clone(),
                requests: Arc::clone(&requests),
            },
            123,
        );

        let mut out = vec![0u8; 64];
        let read = block_on(reader.read_at(5, &mut out, ReadContext::FOREGROUND)).expect("read");

        assert_eq!(read, out.len());
        assert_eq!(&out[..], &data[128..192]);
        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(128, 64)]);
    }

    #[test]
    fn offset_byte_reader_clamps_visible_size() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let reader = OffsetByteReader::new(
            RecordingByteReader {
                data: patterned_data(100),
                requests: Arc::clone(&requests),
            },
            120,
        );

        let size = block_on(reader.size_bytes()).expect("size");
        assert_eq!(size, 0);

        let mut out = [0u8; 16];
        let read = block_on(reader.read_at(0, &mut out, ReadContext::FOREGROUND)).expect("read");
        assert_eq!(read, 0);
        let calls = requests.lock().expect("lock request log");
        assert!(calls.is_empty());
    }

    #[test]
    fn offset_byte_reader_layers_into_block_readers() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = patterned_data(16 * 1024);
        let offset_reader = OffsetByteReader::new(
            RecordingByteReader {
                data: data.clone(),
                requests: Arc::clone(&requests),
            },
            777,
        );

        let blocked = BlockByteReader::new(offset_reader, 512).expect("create block view");
        let aligned = block_on(AlignedBlockReader::new(blocked, 4096)).expect("create");

        let mut out = vec![0u8; 4096];
        block_on(aligned.read_blocks(0, &mut out, ReadContext::FOREGROUND)).expect("read");

        assert_eq!(&out[..], &data[777..777 + 4096]);
        let calls = requests.lock().expect("lock request log");
        assert_eq!(calls.as_slice(), &[(777, 4096)]);

        let mut identity = String::new();
        offset_identity_write(&aligned, &mut identity);
        assert!(identity.contains("offset=777"));
    }

    fn offset_identity_write(reader: &impl BlockReader, out: &mut String) {
        let _ = reader.write_identity(out);
    }

    fn patterned_data(len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (idx, byte) in out.iter_mut().enumerate() {
            *byte = ((idx * 13) % 251) as u8;
        }
        out
    }
}
