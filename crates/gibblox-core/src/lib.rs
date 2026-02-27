#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, string::String, sync::Arc};
use async_trait::async_trait;
use core::{fmt, hash::Hasher};

mod byte_range;
mod gpt;
mod lru;
mod paged;

pub use byte_range::AlignedByteReader;
pub use byte_range::AlignedByteReader as ByteRangeReader;
pub use gpt::{GptBlockReader, GptPartitionSelector};
pub use lru::{LruBlockReader, LruConfig};
pub use paged::{PagedBlockConfig, PagedBlockReader};

pub type GibbloxResult<T> = core::result::Result<T, GibbloxError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GibbloxErrorKind {
    InvalidInput,
    OutOfRange,
    Io,
    Unsupported,
    Other,
}

#[derive(Clone, Debug)]
pub struct GibbloxError {
    kind: GibbloxErrorKind,
    message: Option<String>,
}

impl GibbloxError {
    pub const fn new(kind: GibbloxErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }

    pub fn with_message(kind: GibbloxErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: Some(message.into()),
        }
    }

    pub fn kind(&self) -> GibbloxErrorKind {
        self.kind
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

impl fmt::Display for GibbloxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.message() {
            Some(msg) => write!(f, "{:?}: {}", self.kind, msg),
            None => write!(f, "{:?}", self.kind),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for GibbloxError {}

const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
const FNV_PRIME: u32 = 0x0100_0193;

#[derive(Clone, Copy, Debug)]
pub struct BlockIdentityHasher32 {
    state: u32,
}

impl BlockIdentityHasher32 {
    pub const fn new() -> Self {
        Self {
            state: FNV_OFFSET_BASIS,
        }
    }
}

impl Default for BlockIdentityHasher32 {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for BlockIdentityHasher32 {
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u32::from(*byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    fn finish(&self) -> u64 {
        self.state as u64
    }
}

pub struct HasherFmtWriter<'a, H> {
    hasher: &'a mut H,
}

impl<'a, H: Hasher> HasherFmtWriter<'a, H> {
    pub fn new(hasher: &'a mut H) -> Self {
        Self { hasher }
    }
}

impl<H: Hasher> fmt::Write for HasherFmtWriter<'_, H> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.hasher.write(s.as_bytes());
        Ok(())
    }
}

fn finalize_identity_id(state: u32) -> u32 {
    if state == 0 { 1 } else { state }
}

pub fn derive_block_identity_id_with<H: Hasher>(
    mut hasher: H,
    block_size: u32,
    total_blocks: u64,
    write_identity: impl FnOnce(&mut dyn fmt::Write) -> fmt::Result,
) -> u32 {
    hasher.write_u32(block_size);
    hasher.write_u64(total_blocks);
    let mut writer = HasherFmtWriter::new(&mut hasher);
    let _ = write_identity(&mut writer);
    finalize_identity_id(hasher.finish() as u32)
}

pub fn derive_block_identity_id(
    block_size: u32,
    total_blocks: u64,
    write_identity: impl FnOnce(&mut dyn fmt::Write) -> fmt::Result,
) -> u32 {
    derive_block_identity_id_with(
        BlockIdentityHasher32::new(),
        block_size,
        total_blocks,
        write_identity,
    )
}

pub fn derive_block_identity_id_from_reader<R: BlockReader + ?Sized>(
    reader: &R,
    total_blocks: u64,
) -> u32 {
    derive_block_identity_id(reader.block_size(), total_blocks, |writer| {
        reader.write_identity(writer)
    })
}

pub fn block_identity_string<R: BlockReader + ?Sized>(reader: &R) -> String {
    let mut value = String::new();
    let _ = reader.write_identity(&mut value);
    value
}

pub trait BlockReaderConfigIdentity {
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result;
}

impl<T> BlockReaderConfigIdentity for Arc<T>
where
    T: BlockReaderConfigIdentity + ?Sized,
{
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        (**self).write_identity(out)
    }
}

impl<T> BlockReaderConfigIdentity for &T
where
    T: BlockReaderConfigIdentity + ?Sized,
{
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        (**self).write_identity(out)
    }
}

pub fn config_identity_string<C: BlockReaderConfigIdentity + ?Sized>(config: &C) -> String {
    let mut value = String::new();
    let _ = config.write_identity(&mut value);
    value
}

pub fn derive_config_identity_id_with<H: Hasher, C: BlockReaderConfigIdentity + ?Sized>(
    mut hasher: H,
    config: &C,
) -> u32 {
    let mut writer = HasherFmtWriter::new(&mut hasher);
    let _ = config.write_identity(&mut writer);
    finalize_identity_id(hasher.finish() as u32)
}

pub fn derive_config_identity_id<C: BlockReaderConfigIdentity + ?Sized>(config: &C) -> u32 {
    derive_config_identity_id_with(BlockIdentityHasher32::new(), config)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ReadPriority {
    #[default]
    High,
    Medium,
    Low,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ReadContext {
    pub priority: ReadPriority,
}

impl ReadContext {
    pub const FOREGROUND: Self = Self {
        priority: ReadPriority::High,
    };

    pub const READAHEAD: Self = Self {
        priority: ReadPriority::Medium,
    };

    pub const BACKGROUND: Self = Self {
        priority: ReadPriority::Low,
    };
}

#[async_trait]
pub trait ByteReader: Send + Sync {
    /// Logical block size in bytes used when adapting this byte reader into a block reader.
    fn block_size(&self) -> u32;

    /// Total source size in bytes.
    async fn size_bytes(&self) -> GibbloxResult<u64>;

    /// Write a stable, canonical identity string for this byte reader.
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result;

    /// Read bytes starting at `offset` into `buf`.
    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize>;
}

#[async_trait]
impl<T> BlockReader for T
where
    T: ByteReader + ?Sized,
{
    fn block_size(&self) -> u32 {
        ByteReader::block_size(self)
    }

    async fn total_blocks(&self) -> GibbloxResult<u64> {
        let block_size = ByteReader::block_size(self);
        if block_size == 0 {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero",
            ));
        }
        Ok(ByteReader::size_bytes(self)
            .await?
            .div_ceil(block_size as u64))
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        ByteReader::write_identity(self, out)
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

        let block_size = ByteReader::block_size(self);
        if block_size == 0 {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero",
            ));
        }
        if !buf.len().is_multiple_of(block_size as usize) {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "buffer length must align to block size",
            ));
        }

        let total_blocks = BlockReader::total_blocks(self).await?;
        if lba >= total_blocks {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "requested block out of range",
            ));
        }

        let offset = lba.checked_mul(block_size as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "lba overflow")
        })?;
        let size_bytes = ByteReader::size_bytes(self).await?;
        let available = size_bytes.checked_sub(offset).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset out of range")
        })?;
        let read_len = (buf.len() as u64).min(available) as usize;
        let read = ByteReader::read_at(self, offset, &mut buf[..read_len], ctx).await?;
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

#[async_trait]
pub trait BlockReader: Send + Sync {
    /// Logical block size in bytes.
    fn block_size(&self) -> u32;

    /// Total number of logical blocks available.
    async fn total_blocks(&self) -> GibbloxResult<u64>;

    /// Write a stable, canonical identity string for this block reader.
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result;

    /// Read one or more blocks starting at `lba` into `buf`.
    async fn read_blocks(&self, lba: u64, buf: &mut [u8], ctx: ReadContext)
    -> GibbloxResult<usize>;
}

#[async_trait]
impl<T> BlockReader for Arc<T>
where
    T: BlockReader + ?Sized,
{
    fn block_size(&self) -> u32 {
        (**self).block_size()
    }

    async fn total_blocks(&self) -> GibbloxResult<u64> {
        (**self).total_blocks().await
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        (**self).write_identity(out)
    }

    async fn read_blocks(
        &self,
        lba: u64,
        buf: &mut [u8],
        ctx: ReadContext,
    ) -> GibbloxResult<usize> {
        (**self).read_blocks(lba, buf, ctx).await
    }
}
