#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, format, string::String, string::ToString, sync::Arc, vec::Vec};
use async_trait::async_trait;
use core::fmt;
use gibblox_core::{
    BlockReaderConfigIdentity, ByteReader, GibbloxError, GibbloxErrorKind, GibbloxResult,
    ReadContext,
};
use tracing::{debug, trace};

const TAR_BLOCK_SIZE: u64 = 512;
const TAR_BLOCK_SIZE_USIZE: usize = TAR_BLOCK_SIZE as usize;
const DEFAULT_MAX_METADATA_BYTES: usize = 1024 * 1024;

const TYPE_REGULAR: u8 = b'0';
const TYPE_OLD_REGULAR: u8 = 0;
const TYPE_CONTIGUOUS: u8 = b'7';
const TYPE_PAX_LOCAL: u8 = b'x';
const TYPE_PAX_GLOBAL: u8 = b'g';
const TYPE_GNU_LONG_NAME: u8 = b'L';
const TYPE_GNU_LONG_LINK: u8 = b'K';
const TYPE_GNU_SPARSE: u8 = b'S';

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TarReaderConfig {
    pub max_metadata_bytes: usize,
}

impl Default for TarReaderConfig {
    fn default() -> Self {
        Self {
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TarEntryIndex {
    pub entry_name: String,
    pub header_offset: u64,
    pub data_offset: u64,
    pub size_bytes: u64,
    pub entry_type: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TarEntryByteReaderConfig {
    pub entry_name: String,
    pub reader: TarReaderConfig,
    pub source_identity: Option<String>,
}

impl TarEntryByteReaderConfig {
    pub fn new(entry_name: &str) -> GibbloxResult<Self> {
        Ok(Self {
            entry_name: normalize_entry_name(entry_name)?,
            reader: TarReaderConfig::default(),
            source_identity: None,
        })
    }

    pub fn with_reader_config(mut self, reader: TarReaderConfig) -> Self {
        self.reader = reader;
        self
    }

    pub fn with_source_identity(mut self, source_identity: impl Into<String>) -> Self {
        self.source_identity = Some(source_identity.into());
        self
    }
}

impl BlockReaderConfigIdentity for TarEntryByteReaderConfig {
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("tar-entry:(")?;
        out.write_str(
            self.source_identity
                .as_deref()
                .unwrap_or("<unknown-source>"),
        )?;
        out.write_str("):entry=")?;
        write!(out, "len:{}:", self.entry_name.len())?;
        out.write_str(self.entry_name.as_str())
    }
}

/// Byte reader that exposes the first matching regular file entry from a tar archive.
pub struct TarEntryByteReader {
    source: Arc<dyn ByteReader>,
    source_size_bytes: u64,
    entry: TarEntryIndex,
    config: TarEntryByteReaderConfig,
}

impl TarEntryByteReader {
    pub async fn new(entry_name: &str, source: Arc<dyn ByteReader>) -> GibbloxResult<Self> {
        Self::open_with_config(source, TarEntryByteReaderConfig::new(entry_name)?).await
    }

    pub async fn open_with_config(
        source: Arc<dyn ByteReader>,
        config: TarEntryByteReaderConfig,
    ) -> GibbloxResult<Self> {
        let source_identity = config
            .source_identity
            .clone()
            .unwrap_or_else(|| byte_identity_string(source.as_ref()));
        let config = config.with_source_identity(source_identity);
        let source_size_bytes = source.size_bytes().await?;
        let entry = locate_entry(source.as_ref(), source_size_bytes, &config).await?;

        debug!(
            entry_name = %entry.entry_name,
            header_offset = entry.header_offset,
            data_offset = entry.data_offset,
            size_bytes = entry.size_bytes,
            "tar entry byte reader initialized"
        );

        Ok(Self {
            source,
            source_size_bytes,
            entry,
            config,
        })
    }

    pub async fn open_with_index(
        source: Arc<dyn ByteReader>,
        config: TarEntryByteReaderConfig,
        index: TarEntryIndex,
    ) -> GibbloxResult<Self> {
        let source_identity = config
            .source_identity
            .clone()
            .unwrap_or_else(|| byte_identity_string(source.as_ref()));
        let config = config.with_source_identity(source_identity);
        let source_size_bytes = source.size_bytes().await?;
        validate_index(source.as_ref(), source_size_bytes, &config, &index).await?;

        debug!(
            entry_name = %index.entry_name,
            header_offset = index.header_offset,
            data_offset = index.data_offset,
            size_bytes = index.size_bytes,
            "tar entry byte reader initialized from index"
        );

        Ok(Self {
            source,
            source_size_bytes,
            entry: index,
            config,
        })
    }

    pub fn entry_index(&self) -> &TarEntryIndex {
        &self.entry
    }

    pub fn config(&self) -> &TarEntryByteReaderConfig {
        &self.config
    }

    pub fn entry_size_bytes(&self) -> u64 {
        self.entry.size_bytes
    }
}

#[async_trait]
impl ByteReader for TarEntryByteReader {
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        Ok(self.entry.size_bytes)
    }

    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        self.config.write_identity(out)
    }

    async fn read_at(&self, offset: u64, out: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if offset >= self.entry.size_bytes {
            return Ok(0);
        }

        let available = self.entry.size_bytes - offset;
        let read_len = out.len().min(available as usize);
        let source_offset = self.entry.data_offset.checked_add(offset).ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "tar entry read offset overflow",
            )
        })?;
        let read = self
            .source
            .read_at(source_offset, &mut out[..read_len], ctx)
            .await?;
        if read != read_len {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short read from tar source",
            ));
        }

        trace!(
            offset,
            source_offset,
            requested = out.len(),
            read,
            source_size_bytes = self.source_size_bytes,
            "served tar entry byte read"
        );
        Ok(read)
    }
}

#[derive(Default)]
struct PendingMetadata {
    path: Option<String>,
    size: Option<u64>,
    sparse: bool,
}

#[derive(Clone, Debug)]
struct HeaderMeta {
    path: String,
    size_bytes: u64,
    entry_type: u8,
}

async fn locate_entry(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    config: &TarEntryByteReaderConfig,
) -> GibbloxResult<TarEntryIndex> {
    if source_size_bytes < TAR_BLOCK_SIZE {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar archive is too small to contain a header",
        ));
    }

    let mut cursor = 0u64;
    let mut pending = PendingMetadata::default();
    while cursor
        .checked_add(TAR_BLOCK_SIZE)
        .is_some_and(|end| end <= source_size_bytes)
    {
        let mut header = [0u8; TAR_BLOCK_SIZE_USIZE];
        read_exact_at(
            source,
            source_size_bytes,
            cursor,
            &mut header,
            ReadContext::FOREGROUND,
        )
        .await?;
        if is_zero_block(&header) {
            break;
        }
        verify_header_checksum(&header)?;

        let header_meta = parse_header(&header, &pending)?;
        let data_offset = cursor.checked_add(TAR_BLOCK_SIZE).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "tar data offset overflow")
        })?;
        let data_end = data_offset
            .checked_add(header_meta.size_bytes)
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "tar entry data range overflow",
                )
            })?;
        if data_end > source_size_bytes {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("tar entry '{}' exceeds archive size", header_meta.path),
            ));
        }

        match header_meta.entry_type {
            TYPE_PAX_LOCAL => {
                pending = read_pax_metadata(
                    source,
                    source_size_bytes,
                    data_offset,
                    header_meta.size_bytes,
                    config.reader.max_metadata_bytes,
                )
                .await?;
            }
            TYPE_PAX_GLOBAL => {}
            TYPE_GNU_LONG_NAME => {
                pending.path = Some(
                    read_gnu_long_name(
                        source,
                        source_size_bytes,
                        data_offset,
                        header_meta.size_bytes,
                        config.reader.max_metadata_bytes,
                    )
                    .await?,
                );
            }
            TYPE_GNU_LONG_LINK => {}
            _ => {
                let normalized_path = normalize_entry_name(header_meta.path.as_str())?;
                if normalized_path == config.entry_name {
                    if pending.sparse || header_meta.entry_type == TYPE_GNU_SPARSE {
                        return Err(GibbloxError::with_message(
                            GibbloxErrorKind::Unsupported,
                            format!(
                                "tar entry '{}' is sparse, which is unsupported",
                                header_meta.path
                            ),
                        ));
                    }
                    if !is_regular_entry_type(header_meta.entry_type) {
                        return Err(GibbloxError::with_message(
                            GibbloxErrorKind::Unsupported,
                            format!("tar entry '{}' is not a regular file", header_meta.path),
                        ));
                    }

                    return Ok(TarEntryIndex {
                        entry_name: config.entry_name.clone(),
                        header_offset: cursor,
                        data_offset,
                        size_bytes: header_meta.size_bytes,
                        entry_type: header_meta.entry_type,
                    });
                }
                pending = PendingMetadata::default();
            }
        }

        let padded_size = padded_tar_size(header_meta.size_bytes)?;
        cursor = data_offset.checked_add(padded_size).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "tar cursor overflow")
        })?;
    }

    Err(GibbloxError::with_message(
        GibbloxErrorKind::InvalidInput,
        format!("tar entry '{}' not found", config.entry_name),
    ))
}

async fn validate_index(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    config: &TarEntryByteReaderConfig,
    index: &TarEntryIndex,
) -> GibbloxResult<()> {
    if index.entry_name != config.entry_name {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index entry name does not match requested entry",
        ));
    }
    if !is_regular_entry_type(index.entry_type) {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "tar index entry is not a regular file",
        ));
    }
    if !index.header_offset.is_multiple_of(TAR_BLOCK_SIZE) {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index header offset is not 512-byte aligned",
        ));
    }
    let expected_data_offset =
        index
            .header_offset
            .checked_add(TAR_BLOCK_SIZE)
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "tar index data offset overflow",
                )
            })?;
    if index.data_offset != expected_data_offset {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index data offset does not follow header",
        ));
    }
    let data_end = index
        .data_offset
        .checked_add(index.size_bytes)
        .ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "tar index data range overflow",
            )
        })?;
    if data_end > source_size_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "tar index data range exceeds source size",
        ));
    }

    let mut header = [0u8; TAR_BLOCK_SIZE_USIZE];
    read_exact_at(
        source,
        source_size_bytes,
        index.header_offset,
        &mut header,
        ReadContext::FOREGROUND,
    )
    .await?;
    if is_zero_block(&header) {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index points at end-of-archive marker",
        ));
    }
    verify_header_checksum(&header)?;
    let header_meta = parse_header(&header, &PendingMetadata::default())?;
    if !is_regular_entry_type(header_meta.entry_type) || header_meta.entry_type != index.entry_type
    {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index entry type does not match archive header",
        ));
    }
    if header_meta.size_bytes != index.size_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar index size does not match archive header",
        ));
    }

    Ok(())
}

fn parse_header(
    header: &[u8; TAR_BLOCK_SIZE_USIZE],
    pending: &PendingMetadata,
) -> GibbloxResult<HeaderMeta> {
    let entry_type = header[156];
    let raw_size = parse_numeric_field(&header[124..136])?;
    let size_bytes = pending.size.unwrap_or(raw_size);
    let path = match pending.path.as_deref() {
        Some(path) => path.to_string(),
        None => parse_header_path(header)?,
    };

    Ok(HeaderMeta {
        path,
        size_bytes,
        entry_type,
    })
}

fn parse_header_path(header: &[u8; TAR_BLOCK_SIZE_USIZE]) -> GibbloxResult<String> {
    let name = parse_string_field(&header[0..100])?;
    let prefix = parse_string_field(&header[345..500])?;
    if prefix.is_empty() {
        return Ok(name);
    }
    if name.is_empty() {
        return Ok(prefix);
    }

    let mut out = String::with_capacity(prefix.len() + 1 + name.len());
    out.push_str(prefix.as_str());
    out.push('/');
    out.push_str(name.as_str());
    Ok(out)
}

fn parse_string_field(raw: &[u8]) -> GibbloxResult<String> {
    let len = raw.iter().position(|byte| *byte == 0).unwrap_or(raw.len());
    core::str::from_utf8(&raw[..len])
        .map(str::to_string)
        .map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("tar header path is not UTF-8: {err}"),
            )
        })
}

async fn read_pax_metadata(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    offset: u64,
    size_bytes: u64,
    max_metadata_bytes: usize,
) -> GibbloxResult<PendingMetadata> {
    let payload = read_metadata_payload(
        source,
        source_size_bytes,
        offset,
        size_bytes,
        max_metadata_bytes,
    )
    .await?;
    parse_pax_payload(&payload)
}

async fn read_gnu_long_name(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    offset: u64,
    size_bytes: u64,
    max_metadata_bytes: usize,
) -> GibbloxResult<String> {
    let payload = read_metadata_payload(
        source,
        source_size_bytes,
        offset,
        size_bytes,
        max_metadata_bytes,
    )
    .await?;
    let len = payload
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(payload.len());
    core::str::from_utf8(&payload[..len])
        .map(str::to_string)
        .map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("GNU tar long name is not UTF-8: {err}"),
            )
        })
}

async fn read_metadata_payload(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    offset: u64,
    size_bytes: u64,
    max_metadata_bytes: usize,
) -> GibbloxResult<Vec<u8>> {
    let len = usize::try_from(size_bytes).map_err(|_| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "tar metadata payload does not fit in memory on this platform",
        )
    })?;
    if len > max_metadata_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "tar metadata payload exceeds configured limit",
        ));
    }

    let mut payload = alloc::vec![0u8; len];
    read_exact_at(
        source,
        source_size_bytes,
        offset,
        &mut payload,
        ReadContext::FOREGROUND,
    )
    .await?;
    Ok(payload)
}

fn parse_pax_payload(payload: &[u8]) -> GibbloxResult<PendingMetadata> {
    let mut pos = 0usize;
    let mut pending = PendingMetadata::default();
    while pos < payload.len() {
        let len_start = pos;
        while pos < payload.len() && payload[pos].is_ascii_digit() {
            pos += 1;
        }
        if pos == len_start || pos >= payload.len() || payload[pos] != b' ' {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "invalid PAX record length",
            ));
        }
        let record_len = parse_decimal_usize(&payload[len_start..pos])?;
        if record_len == 0
            || len_start
                .checked_add(record_len)
                .is_none_or(|end| end > payload.len())
        {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "PAX record exceeds metadata payload",
            ));
        }

        let record = &payload[pos + 1..len_start + record_len];
        if record.last().copied() != Some(b'\n') {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "PAX record is not newline terminated",
            ));
        }
        let record = &record[..record.len() - 1];
        if let Some(eq_pos) = record.iter().position(|byte| *byte == b'=') {
            let key = core::str::from_utf8(&record[..eq_pos]).map_err(|err| {
                GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!("PAX key is not UTF-8: {err}"),
                )
            })?;
            let value = &record[eq_pos + 1..];
            match key {
                "path" => {
                    pending.path = Some(core::str::from_utf8(value).map(str::to_string).map_err(
                        |err| {
                            GibbloxError::with_message(
                                GibbloxErrorKind::InvalidInput,
                                format!("PAX path is not UTF-8: {err}"),
                            )
                        },
                    )?);
                }
                "size" => {
                    pending.size = Some(parse_decimal_u64(value)?);
                }
                key if key.starts_with("GNU.sparse") => {
                    pending.sparse = true;
                }
                _ => {}
            }
        }

        pos = len_start + record_len;
    }
    Ok(pending)
}

async fn read_exact_at(
    source: &dyn ByteReader,
    source_size_bytes: u64,
    offset: u64,
    out: &mut [u8],
    ctx: ReadContext,
) -> GibbloxResult<()> {
    if out.is_empty() {
        return Ok(());
    }
    let end = offset.checked_add(out.len() as u64).ok_or_else(|| {
        GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "tar read range overflow")
    })?;
    if end > source_size_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "tar read range exceeds source size",
        ));
    }
    let read = source.read_at(offset, out, ctx).await?;
    if read != out.len() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Io,
            "short read from tar source",
        ));
    }
    Ok(())
}

fn parse_numeric_field(raw: &[u8]) -> GibbloxResult<u64> {
    if raw.first().is_some_and(|byte| (byte & 0x80) != 0) {
        return parse_base256_field(raw);
    }
    parse_octal_field(raw)
}

fn parse_base256_field(raw: &[u8]) -> GibbloxResult<u64> {
    if raw.is_empty() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "empty GNU base-256 field",
        ));
    }
    if (raw[0] & 0x40) != 0 {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            "negative GNU base-256 tar numeric field is unsupported",
        ));
    }

    let mut value = u64::from(raw[0] & 0x7f);
    for byte in &raw[1..] {
        value = value
            .checked_mul(256)
            .and_then(|v| v.checked_add(u64::from(*byte)))
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "GNU base-256 tar numeric field overflow",
                )
            })?;
    }
    Ok(value)
}

fn parse_octal_field(raw: &[u8]) -> GibbloxResult<u64> {
    let mut value = 0u64;
    let mut saw_digit = false;
    for byte in raw {
        match *byte {
            0 | b' ' => {
                if saw_digit {
                    continue;
                }
            }
            b'0'..=b'7' => {
                saw_digit = true;
                value = value
                    .checked_mul(8)
                    .and_then(|v| v.checked_add(u64::from(*byte - b'0')))
                    .ok_or_else(|| {
                        GibbloxError::with_message(
                            GibbloxErrorKind::OutOfRange,
                            "tar octal numeric field overflow",
                        )
                    })?;
            }
            _ => {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    "invalid tar octal numeric field",
                ));
            }
        }
    }
    if !saw_digit {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "empty tar octal numeric field",
        ));
    }
    Ok(value)
}

fn parse_decimal_usize(raw: &[u8]) -> GibbloxResult<usize> {
    let value = parse_decimal_u64(raw)?;
    usize::try_from(value).map_err(|_| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "decimal value overflows usize",
        )
    })
}

fn parse_decimal_u64(raw: &[u8]) -> GibbloxResult<u64> {
    if raw.is_empty() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "empty decimal field",
        ));
    }
    let mut value = 0u64;
    for byte in raw {
        if !byte.is_ascii_digit() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "invalid decimal field",
            ));
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(*byte - b'0')))
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "decimal field overflow")
            })?;
    }
    Ok(value)
}

fn verify_header_checksum(header: &[u8; TAR_BLOCK_SIZE_USIZE]) -> GibbloxResult<()> {
    let expected = parse_octal_field(&header[148..156])?;
    let computed = header
        .iter()
        .enumerate()
        .map(|(idx, byte)| {
            if (148..156).contains(&idx) {
                u64::from(b' ')
            } else {
                u64::from(*byte)
            }
        })
        .sum::<u64>();
    if expected != computed {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            format!("tar header checksum mismatch (expected {expected}, computed {computed})"),
        ));
    }
    Ok(())
}

fn is_zero_block(block: &[u8; TAR_BLOCK_SIZE_USIZE]) -> bool {
    block.iter().all(|byte| *byte == 0)
}

fn is_regular_entry_type(entry_type: u8) -> bool {
    matches!(
        entry_type,
        TYPE_REGULAR | TYPE_OLD_REGULAR | TYPE_CONTIGUOUS
    )
}

fn padded_tar_size(size_bytes: u64) -> GibbloxResult<u64> {
    size_bytes
        .div_ceil(TAR_BLOCK_SIZE)
        .checked_mul(TAR_BLOCK_SIZE)
        .ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "tar padded size overflow")
        })
}

fn normalize_entry_name(entry_name: &str) -> GibbloxResult<String> {
    let mut value = entry_name.trim();
    while let Some(stripped) = value.strip_prefix('/') {
        value = stripped;
    }
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    while let Some(stripped) = value.strip_suffix('/') {
        value = stripped;
    }
    if value.is_empty() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar entry name is empty",
        ));
    }
    if value.contains('\0') {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar entry name contains NUL byte",
        ));
    }
    if value.split('/').any(|component| component == "..") {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "tar entry name must not contain '..' components",
        ));
    }
    Ok(value.to_string())
}

fn byte_identity_string(reader: &dyn ByteReader) -> String {
    let mut value = String::new();
    let _ = reader.write_identity(&mut value);
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::{sync::Arc, vec, vec::Vec};
    use futures::executor::block_on;

    struct MemoryByteReader {
        data: Vec<u8>,
    }

    #[async_trait]
    impl ByteReader for MemoryByteReader {
        async fn size_bytes(&self) -> GibbloxResult<u64> {
            Ok(self.data.len() as u64)
        }

        fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
            write!(out, "memory:{}", self.data.len())
        }

        async fn read_at(
            &self,
            offset: u64,
            out: &mut [u8],
            _ctx: ReadContext,
        ) -> GibbloxResult<usize> {
            if out.is_empty() || offset >= self.data.len() as u64 {
                return Ok(0);
            }
            let start = offset as usize;
            let len = out.len().min(self.data.len() - start);
            out[..len].copy_from_slice(&self.data[start..start + len]);
            Ok(len)
        }
    }

    #[test]
    fn reads_regular_file_entry() {
        let archive = build_tar(&[("rootfs.img", b"hello world".as_slice(), TYPE_REGULAR)]);
        let reader = memory_reader(archive);
        let tar = block_on(TarEntryByteReader::new("/rootfs.img", reader)).expect("open tar entry");

        assert_eq!(tar.entry_size_bytes(), 11);
        let mut out = vec![0u8; 32];
        let read = block_on(tar.read_at(6, &mut out, ReadContext::FOREGROUND)).expect("read");
        assert_eq!(read, 5);
        assert_eq!(&out[..read], b"world");
    }

    #[test]
    fn first_matching_entry_wins() {
        let archive = build_tar(&[
            ("rootfs.img", b"first".as_slice(), TYPE_REGULAR),
            ("rootfs.img", b"second".as_slice(), TYPE_REGULAR),
        ]);
        let reader = memory_reader(archive);
        let tar = block_on(TarEntryByteReader::new("rootfs.img", reader)).expect("open tar entry");

        let mut out = vec![0u8; 8];
        let read = block_on(tar.read_at(0, &mut out, ReadContext::FOREGROUND)).expect("read");
        assert_eq!(read, 5);
        assert_eq!(&out[..read], b"first");
    }

    #[test]
    fn rejects_matching_directory() {
        let archive = build_tar(&[("rootfs.img", b"".as_slice(), b'5')]);
        let reader = memory_reader(archive);
        let err = match block_on(TarEntryByteReader::new("rootfs.img", reader)) {
            Ok(_) => panic!("directory should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::Unsupported);
    }

    #[test]
    fn reports_missing_entry() {
        let archive = build_tar(&[("rootfs.img", b"hello".as_slice(), TYPE_REGULAR)]);
        let reader = memory_reader(archive);
        let err = match block_on(TarEntryByteReader::new("missing.img", reader)) {
            Ok(_) => panic!("missing entry should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn supports_pax_path() {
        let archive = build_pax_path_tar("very/long/path/rootfs.img", b"payload");
        let reader = memory_reader(archive);
        let tar = block_on(TarEntryByteReader::new(
            "/very/long/path/rootfs.img",
            reader,
        ))
        .expect("open pax path entry");

        let mut out = vec![0u8; 16];
        let read = block_on(tar.read_at(0, &mut out, ReadContext::FOREGROUND)).expect("read");
        assert_eq!(read, 7);
        assert_eq!(&out[..read], b"payload");
    }

    #[test]
    fn generated_index_opens_entry() {
        let archive = build_tar(&[("rootfs.img", b"hello".as_slice(), TYPE_REGULAR)]);
        let reader = memory_reader(archive.clone());
        let tar = block_on(TarEntryByteReader::new("rootfs.img", reader)).expect("open tar entry");
        let index = tar.entry_index().clone();

        let reader = memory_reader(archive);
        let config = TarEntryByteReaderConfig::new("rootfs.img").expect("config");
        let tar = block_on(TarEntryByteReader::open_with_index(reader, config, index))
            .expect("open from index");
        assert_eq!(tar.entry_size_bytes(), 5);
    }

    fn memory_reader(data: Vec<u8>) -> Arc<dyn ByteReader> {
        Arc::new(MemoryByteReader { data })
    }

    fn build_tar(entries: &[(&str, &[u8], u8)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (name, payload, entry_type) in entries {
            append_entry(&mut out, name, payload, *entry_type);
        }
        out.extend_from_slice(&[0u8; TAR_BLOCK_SIZE_USIZE]);
        out.extend_from_slice(&[0u8; TAR_BLOCK_SIZE_USIZE]);
        out
    }

    fn build_pax_path_tar(path: &str, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let record = pax_record("path", path);
        append_entry(&mut out, "PaxHeader", record.as_bytes(), TYPE_PAX_LOCAL);
        append_entry(&mut out, "rootfs.img", payload, TYPE_REGULAR);
        out.extend_from_slice(&[0u8; TAR_BLOCK_SIZE_USIZE]);
        out.extend_from_slice(&[0u8; TAR_BLOCK_SIZE_USIZE]);
        out
    }

    fn pax_record(key: &str, value: &str) -> String {
        let mut len = key.len() + value.len() + 4;
        loop {
            let record = format!("{len} {key}={value}\n");
            if record.len() == len {
                return record;
            }
            len = record.len();
        }
    }

    fn append_entry(out: &mut Vec<u8>, name: &str, payload: &[u8], entry_type: u8) {
        let mut header = [0u8; TAR_BLOCK_SIZE_USIZE];
        let name_bytes = name.as_bytes();
        header[..name_bytes.len()].copy_from_slice(name_bytes);
        write_octal(&mut header[100..108], 0o644);
        write_octal(&mut header[108..116], 0);
        write_octal(&mut header[116..124], 0);
        write_octal(&mut header[124..136], payload.len() as u64);
        write_octal(&mut header[136..148], 0);
        header[148..156].fill(b' ');
        header[156] = entry_type;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
        write_octal(&mut header[148..156], checksum);

        out.extend_from_slice(&header);
        out.extend_from_slice(payload);
        let padded = payload.len().div_ceil(TAR_BLOCK_SIZE_USIZE) * TAR_BLOCK_SIZE_USIZE;
        out.resize(out.len() + padded - payload.len(), 0);
    }

    fn write_octal(dst: &mut [u8], value: u64) {
        dst.fill(0);
        let digits = format!("{:0width$o}", value, width = dst.len() - 1);
        dst[..digits.len()].copy_from_slice(digits.as_bytes());
    }
}
