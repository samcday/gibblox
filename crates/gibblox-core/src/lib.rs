#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, string::String, sync::Arc};
use async_trait::async_trait;
use core::{fmt, hash::Hasher};

mod block_byte;
mod byte_range;
mod gpt;
mod lru;
mod offset_byte;
mod paged;
mod window_block;

pub use block_byte::BlockByteReader;
pub use byte_range::AlignedByteReader;
pub use gpt::{GptBlockReader, GptBlockReaderConfig, GptPartitionSelector};
pub use lru::{LruBlockReader, LruConfig};
pub use offset_byte::OffsetByteReader;
pub use paged::{PagedBlockConfig, PagedBlockReader};
pub use window_block::WindowBlockReader;

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
    /// Total source size in bytes.
    async fn size_bytes(&self) -> GibbloxResult<u64>;

    /// Write a stable, canonical identity string for this byte reader.
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result;

    /// Read bytes starting at `offset` into `buf`.
    ///
    /// Returns the number of bytes copied into `buf`.
    ///
    /// Callers may request past end-of-source; in that case implementations
    /// return `Ok(0)` (or a shorter length when partially in range).
    ///
    /// For in-range requests, implementations should not silently tolerate
    /// unexpected short reads from the backing transport. Either complete the
    /// requested in-range copy or return an error.
    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize>;
}

#[async_trait]
impl<T> ByteReader for Arc<T>
where
    T: ByteReader + ?Sized,
{
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        (**self).size_bytes().await
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        (**self).write_identity(out)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        (**self).read_at(offset, buf, ctx).await
    }
}

#[async_trait]
impl<T> ByteReader for &T
where
    T: ByteReader + ?Sized,
{
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        (**self).size_bytes().await
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        (**self).write_identity(out)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        (**self).read_at(offset, buf, ctx).await
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
    ///
    /// Implementations should treat in-range short reads as I/O failures and
    /// return an error rather than silently truncating, unless the reader
    /// intentionally exposes synthesized tail bytes (for example, a byte source
    /// viewed as fixed-size blocks).
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
