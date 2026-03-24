extern crate alloc;

use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};
use async_trait::async_trait;
use core::{error::Error, fmt};
use ext4plus::Ext4Read;
use ext4plus::error::Ext4Error as Ext4PlusError;
use ext4plus::prelude::AsyncIterator;
use gibblox_core::{
    BlockByteReader, BlockReader, BlockReaderConfigIdentity, ByteReader, GibbloxError,
    GibbloxErrorKind, GibbloxResult, ReadContext, block_identity_string,
};
use gobblytes_core::{Filesystem, FilesystemEntryType};
use tracing::{info, trace};

pub use ext4plus as ext4plus_rs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ext4EntryType {
    File,
    Directory,
    Symlink,
    Other,
}

impl From<Ext4EntryType> for FilesystemEntryType {
    fn from(value: Ext4EntryType) -> Self {
        match value {
            Ext4EntryType::File => Self::File,
            Ext4EntryType::Directory => Self::Directory,
            Ext4EntryType::Symlink => Self::Symlink,
            Ext4EntryType::Other => Self::Other,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ext4FileReaderConfig {
    pub path: String,
    pub block_size: u32,
    pub source_identity: Option<String>,
}

impl Ext4FileReaderConfig {
    pub fn new(path: &str, block_size: u32) -> GibbloxResult<Self> {
        validate_block_size(block_size)?;
        Ok(Self {
            path: normalize_path(path)?,
            block_size,
            source_identity: None,
        })
    }

    pub fn with_source_identity(mut self, source_identity: impl Into<String>) -> Self {
        self.source_identity = Some(source_identity.into());
        self
    }

    fn validate(&self) -> GibbloxResult<()> {
        validate_block_size(self.block_size)
    }
}

impl BlockReaderConfigIdentity for Ext4FileReaderConfig {
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("ext4-file:(")?;
        out.write_str(
            self.source_identity
                .as_deref()
                .unwrap_or("<unknown-source>"),
        )?;
        write!(out, "):path=len:{}:", self.path.len())?;
        out.write_str(self.path.as_str())?;
        write!(out, ":block_size={}", self.block_size)
    }
}

/// Async-first ext4 filesystem wrapper backed by a gibblox `BlockReader`.
#[derive(Clone)]
pub struct Ext4Fs {
    fs: ext4plus_rs::Ext4,
    source_identity: String,
}

impl Ext4Fs {
    /// Open an ext4 filesystem from a block reader source.
    pub async fn open<S: BlockReader + 'static>(source: S) -> GibbloxResult<Self> {
        let source_block_size = source.block_size();
        if source_block_size == 0 || !source_block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "source block size must be non-zero power of two",
            ));
        }

        let total_blocks = source.total_blocks().await?;
        let source_identity = block_identity_string(&source);
        let source_size_bytes = total_blocks
            .checked_mul(source_block_size as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "source size overflow")
            })?;

        let adapter = AsyncBlockAdapter {
            inner: Arc::new(source),
            block_size: source_block_size as usize,
            size_bytes: source_size_bytes,
        };

        info!(
            source_block_size,
            total_blocks, source_size_bytes, "opening ext4 filesystem"
        );
        let fs = ext4plus_rs::Ext4::load(Box::new(adapter))
            .await
            .map_err(map_ext4_err("open ext4 image"))?;

        Ok(Self {
            fs,
            source_identity,
        })
    }

    pub fn source_identity(&self) -> &str {
        self.source_identity.as_str()
    }

    pub async fn read_all(&self, path: &str) -> GibbloxResult<Vec<u8>> {
        let normalized = normalize_path(path)?;
        trace!(path = normalized, "reading ext4 file");
        self.fs
            .read(normalized.as_str())
            .await
            .map_err(map_ext4_err("read ext4 file"))
    }

    pub async fn read_range(&self, path: &str, offset: u64, len: usize) -> GibbloxResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let normalized = normalize_path(path)?;
        let mut file = self
            .fs
            .open(normalized.as_str())
            .await
            .map_err(map_ext4_err("open ext4 file"))?;
        let mut out = vec![0u8; len];
        let read = file
            .read_bytes_at(&mut out, offset)
            .await
            .map_err(map_ext4_err("read ext4 file range"))?;
        out.truncate(read);
        Ok(out)
    }

    pub async fn read_dir(&self, path: &str) -> GibbloxResult<Vec<String>> {
        let normalized = normalize_path(path)?;
        let mut entries = self
            .fs
            .read_dir(normalized.as_str())
            .await
            .map_err(map_ext4_err("read ext4 directory"))?;

        let mut names = Vec::new();
        while let Some(next) = entries.next().await {
            let entry = next.map_err(map_ext4_err("read ext4 directory entry"))?;
            let name = entry.file_name().as_str().map_err(|err| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("read ext4 directory entry name: {err}"),
                )
            })?;
            if name == "." || name == ".." {
                continue;
            }
            names.push(name.to_string());
        }

        Ok(names)
    }

    pub async fn entry_type(&self, path: &str) -> GibbloxResult<Option<Ext4EntryType>> {
        let normalized = normalize_path(path)?;
        match self.fs.symlink_metadata(normalized.as_str()).await {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                let entry_type = if file_type.is_regular_file() {
                    Ext4EntryType::File
                } else if file_type.is_dir() {
                    Ext4EntryType::Directory
                } else if file_type.is_symlink() {
                    Ext4EntryType::Symlink
                } else {
                    Ext4EntryType::Other
                };
                Ok(Some(entry_type))
            }
            Err(Ext4PlusError::NotFound) => Ok(None),
            Err(err) => Err(map_ext4_err("read ext4 path metadata")(err)),
        }
    }

    pub async fn read_link(&self, path: &str) -> GibbloxResult<String> {
        let normalized = normalize_path(path)?;
        let target = self
            .fs
            .read_link(normalized.as_str())
            .await
            .map_err(map_ext4_err("read ext4 symlink target"))?;
        let target = target.to_str().map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("ext4 symlink target is not UTF-8: {err}"),
            )
        })?;
        Ok(target.to_string())
    }

    pub async fn exists(&self, path: &str) -> GibbloxResult<bool> {
        let normalized = normalize_path(path)?;
        self.fs
            .exists(normalized.as_str())
            .await
            .map_err(map_ext4_err("check ext4 path existence"))
    }
}

impl Filesystem for Ext4Fs {
    type Error = GibbloxError;

    async fn read_all(&self, path: &str) -> Result<Vec<u8>, Self::Error> {
        Ext4Fs::read_all(self, path).await
    }

    async fn read_range(
        &self,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, Self::Error> {
        Ext4Fs::read_range(self, path, offset, len).await
    }

    async fn read_dir(&self, path: &str) -> Result<Vec<String>, Self::Error> {
        Ext4Fs::read_dir(self, path).await
    }

    async fn entry_type(&self, path: &str) -> Result<Option<FilesystemEntryType>, Self::Error> {
        Ext4Fs::entry_type(self, path)
            .await
            .map(|entry_type| entry_type.map(FilesystemEntryType::from))
    }

    async fn read_link(&self, path: &str) -> Result<String, Self::Error> {
        Ext4Fs::read_link(self, path).await
    }

    async fn exists(&self, path: &str) -> Result<bool, Self::Error> {
        Ext4Fs::exists(self, path).await
    }
}

/// File-backed block reader sourced from a file inside an ext4 image.
pub struct Ext4FileReader {
    block_size: u32,
    file_size_bytes: u64,
    file_path: String,
    fs: Ext4Fs,
    config: Ext4FileReaderConfig,
}

impl Ext4FileReader {
    /// Build a block reader for `path` from an ext4 image exposed through `BlockReader`.
    pub async fn new<S: BlockReader + 'static>(
        source: S,
        path: &str,
        block_size: u32,
    ) -> GibbloxResult<Self> {
        Self::open_with_config(source, Ext4FileReaderConfig::new(path, block_size)?).await
    }

    pub async fn open_with_config<S: BlockReader + 'static>(
        source: S,
        config: Ext4FileReaderConfig,
    ) -> GibbloxResult<Self> {
        config.validate()?;
        info!(path = %config.path, block_size = config.block_size, "constructing ext4-backed reader");

        let source: Arc<dyn BlockReader> = Arc::new(source);
        let fs = Ext4Fs::open(source).await?;
        let source_identity = config
            .source_identity
            .clone()
            .unwrap_or_else(|| fs.source_identity().to_string());
        let config = config.with_source_identity(source_identity);
        let file_path = config.path.clone();
        let metadata = fs
            .fs
            .metadata(file_path.as_str())
            .await
            .map_err(map_ext4_err("read ext4 file metadata"))?;
        if !metadata.file_type().is_regular_file() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("ext4 path is not a regular file: {file_path}"),
            ));
        }
        let file_size_bytes = metadata.len();

        info!(path = %file_path, file_size_bytes, "resolved file from ext4");
        Ok(Self {
            block_size: config.block_size,
            file_size_bytes,
            file_path,
            fs,
            config,
        })
    }

    pub fn config(&self) -> &Ext4FileReaderConfig {
        &self.config
    }

    pub fn file_size_bytes(&self) -> u64 {
        self.file_size_bytes
    }
}

#[async_trait]
impl ByteReader for Ext4FileReader {
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        Ok(self.file_size_bytes)
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        self.config.write_identity(out)
    }

    async fn read_at(
        &self,
        offset: u64,
        buf: &mut [u8],
        _ctx: ReadContext,
    ) -> GibbloxResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if offset >= self.file_size_bytes {
            return Ok(0);
        }

        let read_len = ((buf.len() as u64).min(self.file_size_bytes - offset)) as usize;
        let data = self
            .fs
            .read_range(self.file_path.as_str(), offset, read_len)
            .await?;
        if data.len() < read_len {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short read from ext4 file",
            ));
        }
        buf[..read_len].copy_from_slice(&data[..read_len]);
        Ok(read_len)
    }
}

#[async_trait]
impl BlockReader for Ext4FileReader {
    fn block_size(&self) -> u32 {
        self.block_size
    }

    async fn total_blocks(&self) -> GibbloxResult<u64> {
        Ok(self.file_size_bytes.div_ceil(self.block_size as u64))
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        self.config.write_identity(out)
    }

    async fn read_blocks(
        &self,
        lba: u64,
        buf: &mut [u8],
        ctx: ReadContext,
    ) -> GibbloxResult<usize> {
        let adapter = BlockByteReader::new(self, self.block_size)?;
        let read = adapter.read_blocks(lba, buf, ctx).await?;
        trace!(lba, requested = buf.len(), read, "reading ext4 file blocks");
        Ok(read)
    }
}

#[derive(Clone)]
struct AsyncBlockAdapter {
    inner: Arc<dyn BlockReader>,
    block_size: usize,
    size_bytes: u64,
}

#[async_trait]
impl Ext4Read for AsyncBlockAdapter {
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if dst.is_empty() {
            return Ok(());
        }

        let end = start_byte
            .checked_add(dst.len() as u64)
            .ok_or_else(|| adapter_box_error("ext4 adapter read range overflow"))?;
        if end > self.size_bytes {
            return Err(adapter_box_error("ext4 adapter read past end of source"));
        }

        let bs = self.block_size as u64;
        let aligned_start = (start_byte / bs) * bs;
        let aligned_end = end.div_ceil(bs) * bs;
        let aligned_len = (aligned_end - aligned_start) as usize;

        let mut scratch = vec![0u8; aligned_len];
        let mut filled = 0usize;
        while filled < scratch.len() {
            let filled_u64 = u64::try_from(filled)
                .map_err(|_| adapter_box_error("ext4 adapter offset conversion overflow"))?;
            let lba = aligned_start
                .checked_add(filled_u64)
                .ok_or_else(|| adapter_box_error("ext4 adapter read offset overflow"))?
                / bs;
            let read = self
                .inner
                .read_blocks(lba, &mut scratch[filled..], ReadContext::FOREGROUND)
                .await
                .map_err(|err| adapter_box_error(format!("block read failed: {err}")))?;
            if read == 0 {
                return Err(adapter_box_error(
                    "unexpected EOF while servicing ext4 read",
                ));
            }
            if read % self.block_size != 0 && filled + read < scratch.len() {
                return Err(adapter_box_error("unaligned short read from block source"));
            }
            filled += read;
        }

        let head = (start_byte - aligned_start) as usize;
        dst.copy_from_slice(&scratch[head..head + dst.len()]);
        Ok(())
    }
}

fn normalize_path(path: &str) -> GibbloxResult<String> {
    let inner = path.trim_start_matches('/');
    if inner.is_empty() {
        return Ok("/".to_string());
    }
    Ok(format!("/{inner}"))
}

fn validate_block_size(block_size: u32) -> GibbloxResult<()> {
    if block_size == 0 || !block_size.is_power_of_two() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "block size must be non-zero power of two",
        ));
    }
    Ok(())
}

fn map_ext4_err(op: &'static str) -> impl FnOnce(Ext4PlusError) -> GibbloxError {
    move |err| {
        let kind = match &err {
            Ext4PlusError::Io(_) => GibbloxErrorKind::Io,
            Ext4PlusError::Incompatible(_) | Ext4PlusError::Encrypted => {
                GibbloxErrorKind::Unsupported
            }
            Ext4PlusError::Corrupt(_) => GibbloxErrorKind::InvalidInput,
            Ext4PlusError::FileTooLarge => GibbloxErrorKind::OutOfRange,
            _ => GibbloxErrorKind::InvalidInput,
        };
        GibbloxError::with_message(kind, format!("{op}: {err}"))
    }
}

#[derive(Debug)]
struct AdapterError {
    message: String,
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl Error for AdapterError {}

fn adapter_box_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(AdapterError {
        message: message.into(),
    })
}
