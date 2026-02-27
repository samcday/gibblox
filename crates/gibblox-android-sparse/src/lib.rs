#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, format, string::String, vec, vec::Vec};
use async_lock::Mutex;
use async_trait::async_trait;
use core::fmt;
use gibblox_core::{
    BlockReader, BlockReaderConfigIdentity, GibbloxError, GibbloxErrorKind, GibbloxResult,
    ReadContext,
};
use tracing::{debug, trace};

const SPARSE_HEADER_MAGIC: u32 = 0xed26_ff3a;
const SPARSE_MAJOR_VERSION: u16 = 1;
const SPARSE_HEADER_LEN: usize = 28;
const CHUNK_HEADER_LEN: usize = 12;

const CHUNK_TYPE_RAW: u16 = 0xCAC1;
const CHUNK_TYPE_FILL: u16 = 0xCAC2;
const CHUNK_TYPE_DONT_CARE: u16 = 0xCAC3;
const CHUNK_TYPE_CRC32: u16 = 0xCAC4;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AndroidSparseBlockReaderConfig {
    pub source_identity: Option<String>,
}

impl AndroidSparseBlockReaderConfig {
    pub fn with_source_identity(mut self, source_identity: impl Into<String>) -> Self {
        self.source_identity = Some(source_identity.into());
        self
    }
}

impl BlockReaderConfigIdentity for AndroidSparseBlockReaderConfig {
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        out.write_str("android-sparse:(")?;
        out.write_str(
            self.source_identity
                .as_deref()
                .unwrap_or("<unknown-source>"),
        )?;
        out.write_str(")")
    }
}

/// Read-only block reader over Android sparse image streams.
///
/// The sparse image header is parsed up-front during construction.
/// Chunk headers are parsed and validated lazily as requested reads advance through
/// the sparse image.
pub struct AndroidSparseBlockReader<S> {
    source: S,
    source_block_size: usize,
    source_size_bytes: u64,
    block_size: u32,
    total_blocks: u64,
    expanded_size_bytes: u64,
    total_chunks: usize,
    chunk_header_size: u64,
    declared_chunks: u32,
    image_checksum: u32,
    chunks: Mutex<SparseChunkState>,
    config: AndroidSparseBlockReaderConfig,
}

struct SparseChunkState {
    chunks: Vec<SparseChunk>,
    next_chunk_index: usize,
    next_chunk_cursor: u64,
    output_cursor: u64,
}

impl<S> AndroidSparseBlockReader<S>
where
    S: BlockReader,
{
    /// Parse sparse headers and initialize lazy chunk parsing for `source`.
    pub async fn new(source: S) -> GibbloxResult<Self> {
        Self::new_with_config(source, AndroidSparseBlockReaderConfig::default()).await
    }

    pub async fn new_with_config(
        source: S,
        config: AndroidSparseBlockReaderConfig,
    ) -> GibbloxResult<Self> {
        let source_block_size_u32 = source.block_size();
        if source_block_size_u32 == 0 || !source_block_size_u32.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "source block size must be non-zero power of two",
            ));
        }

        let source_total_blocks = source.total_blocks().await?;
        let source_size_bytes = source_total_blocks
            .checked_mul(source_block_size_u32 as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "source size overflow")
            })?;
        let source_block_size = source_block_size_u32 as usize;

        let mut sparse_header_raw = [0u8; SPARSE_HEADER_LEN];
        read_source_exact(
            &source,
            source_block_size,
            source_size_bytes,
            0,
            &mut sparse_header_raw,
            ReadContext::FOREGROUND,
        )
        .await?;
        let sparse_header = SparseHeader::parse(&sparse_header_raw);

        validate_sparse_header(&sparse_header)?;

        let sparse_block_size = sparse_header.blk_sz;
        let total_blocks = u64::from(sparse_header.total_blks);
        let expanded_size_bytes = total_blocks
            .checked_mul(sparse_block_size as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "sparse expanded image size overflow",
                )
            })?;

        let chunk_header_size = u64::from(sparse_header.chunk_hdr_sz);
        let chunk_cursor = u64::from(sparse_header.file_hdr_sz);
        if chunk_cursor > source_size_bytes {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "sparse file header exceeds source size",
            ));
        }

        let total_chunks_u64 = u64::from(sparse_header.total_chunks);
        let available_chunk_header_bytes = source_size_bytes - chunk_cursor;
        let max_chunk_headers = available_chunk_header_bytes / chunk_header_size;
        if total_chunks_u64 > max_chunk_headers {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!(
                    "declared sparse chunk count {} exceeds maximum {} for available bytes",
                    total_chunks_u64, max_chunk_headers
                ),
            ));
        }

        let total_chunks = usize::try_from(total_chunks_u64).map_err(|_| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "chunk count overflow")
        })?;
        let chunks = Mutex::new(SparseChunkState {
            chunks: Vec::new(),
            next_chunk_index: 0,
            next_chunk_cursor: chunk_cursor,
            output_cursor: 0,
        });

        let source_identity = config
            .source_identity
            .clone()
            .unwrap_or_else(|| gibblox_core::block_identity_string(&source));
        let config = config.with_source_identity(source_identity);

        debug!(
            source_block_size = source_block_size_u32,
            source_total_blocks,
            sparse_block_size,
            total_blocks,
            declared_chunks = sparse_header.total_chunks,
            mapped_chunks = 0,
            "android sparse reader initialized"
        );

        Ok(Self {
            source,
            source_block_size,
            source_size_bytes,
            block_size: sparse_block_size,
            total_blocks,
            expanded_size_bytes,
            total_chunks,
            chunk_header_size,
            declared_chunks: sparse_header.total_chunks,
            image_checksum: sparse_header.image_checksum,
            chunks,
            config,
        })
    }

    pub fn expanded_size_bytes(&self) -> u64 {
        self.expanded_size_bytes
    }

    pub fn declared_chunks(&self) -> u32 {
        self.declared_chunks
    }

    pub fn image_checksum(&self) -> u32 {
        self.image_checksum
    }

    pub fn config(&self) -> &AndroidSparseBlockReaderConfig {
        &self.config
    }

    async fn ensure_chunks_cover_range(
        &self,
        read_end: u64,
        ctx: ReadContext,
    ) -> GibbloxResult<()> {
        if read_end > self.expanded_size_bytes {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "requested range exceeds sparse image",
            ));
        }

        loop {
            let state = self.chunks.lock().await;
            if state.output_cursor >= read_end {
                return Ok(());
            }
            if state.next_chunk_index >= self.total_chunks {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    format!(
                        "sparse output size mismatch (mapped {}, expected {})",
                        state.output_cursor, self.expanded_size_bytes
                    ),
                ));
            }
            drop(state);

            self.parse_next_chunk(ctx).await?;
        }
    }

    async fn parse_next_chunk(&self, ctx: ReadContext) -> GibbloxResult<()> {
        let mut state = self.chunks.lock().await;
        let chunk_index = state.next_chunk_index;
        if chunk_index >= self.total_chunks {
            return Ok(());
        }

        let chunk_cursor = state.next_chunk_cursor;
        let mut chunk_header_raw = [0u8; CHUNK_HEADER_LEN];
        read_source_exact(
            &self.source,
            self.source_block_size,
            self.source_size_bytes,
            chunk_cursor,
            &mut chunk_header_raw,
            ctx,
        )
        .await?;
        let chunk_header = SparseChunkHeader::parse(&chunk_header_raw);

        let chunk_total_size = u64::from(chunk_header.total_sz);
        if chunk_total_size < self.chunk_header_size {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("chunk {chunk_index} total size is smaller than chunk header size"),
            ));
        }

        let chunk_end = chunk_cursor.checked_add(chunk_total_size).ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                format!("chunk {chunk_index} range overflow"),
            )
        })?;
        if chunk_end > self.source_size_bytes {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                format!("chunk {chunk_index} exceeds source size"),
            ));
        }

        let chunk_payload_offset = chunk_cursor
            .checked_add(self.chunk_header_size)
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    format!("chunk {chunk_index} payload offset overflow"),
                )
            })?;
        let chunk_payload_size = chunk_total_size - self.chunk_header_size;
        let chunk_output_bytes = u64::from(chunk_header.chunk_sz)
            .checked_mul(self.block_size as u64)
            .ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    format!("chunk {chunk_index} output size overflow"),
                )
            })?;

        let mapped_chunk = match chunk_header.chunk_type {
            CHUNK_TYPE_RAW => {
                if chunk_payload_size != chunk_output_bytes {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!(
                            "chunk {chunk_index} raw payload size mismatch (expected {chunk_output_bytes}, got {chunk_payload_size})"
                        ),
                    ));
                }
                claim_output_span(
                    &mut state.output_cursor,
                    chunk_output_bytes,
                    self.expanded_size_bytes,
                    chunk_index,
                )?
                .map(|(output_start, output_end)| SparseChunk {
                    output_start,
                    output_end,
                    kind: SparseChunkKind::Raw {
                        source_offset: chunk_payload_offset,
                    },
                })
            }
            CHUNK_TYPE_FILL => {
                if chunk_payload_size != 4 {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!(
                            "chunk {chunk_index} fill payload size mismatch (expected 4, got {chunk_payload_size})"
                        ),
                    ));
                }

                let mut pattern = [0u8; 4];
                read_source_exact(
                    &self.source,
                    self.source_block_size,
                    self.source_size_bytes,
                    chunk_payload_offset,
                    &mut pattern,
                    ctx,
                )
                .await?;

                claim_output_span(
                    &mut state.output_cursor,
                    chunk_output_bytes,
                    self.expanded_size_bytes,
                    chunk_index,
                )?
                .map(|(output_start, output_end)| SparseChunk {
                    output_start,
                    output_end,
                    kind: SparseChunkKind::Fill { pattern },
                })
            }
            CHUNK_TYPE_DONT_CARE => {
                if chunk_payload_size != 0 {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!(
                            "chunk {chunk_index} DONT_CARE payload must be empty (got {chunk_payload_size})"
                        ),
                    ));
                }

                claim_output_span(
                    &mut state.output_cursor,
                    chunk_output_bytes,
                    self.expanded_size_bytes,
                    chunk_index,
                )?
                .map(|(output_start, output_end)| SparseChunk {
                    output_start,
                    output_end,
                    kind: SparseChunkKind::DontCare,
                })
            }
            CHUNK_TYPE_CRC32 => {
                if chunk_header.chunk_sz != 0 {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!("chunk {chunk_index} CRC32 chunk_sz must be zero"),
                    ));
                }
                if chunk_payload_size != 4 {
                    return Err(GibbloxError::with_message(
                        GibbloxErrorKind::InvalidInput,
                        format!(
                            "chunk {chunk_index} CRC32 payload size mismatch (expected 4, got {chunk_payload_size})"
                        ),
                    ));
                }

                let mut crc = [0u8; 4];
                read_source_exact(
                    &self.source,
                    self.source_block_size,
                    self.source_size_bytes,
                    chunk_payload_offset,
                    &mut crc,
                    ctx,
                )
                .await?;
                None
            }
            other => {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Unsupported,
                    format!("unsupported sparse chunk type 0x{other:04X} at chunk {chunk_index}"),
                ));
            }
        };

        if let Some(chunk) = mapped_chunk {
            state.chunks.push(chunk);
        }
        state.next_chunk_index = state.next_chunk_index.checked_add(1).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "chunk index overflow")
        })?;
        state.next_chunk_cursor = chunk_end;

        if state.next_chunk_index == self.total_chunks
            && state.output_cursor != self.expanded_size_bytes
        {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!(
                    "sparse output size mismatch (mapped {}, expected {})",
                    state.output_cursor, self.expanded_size_bytes
                ),
            ));
        }

        trace!(
            chunk_index,
            parsed_chunks = state.next_chunk_index,
            mapped_chunks = state.chunks.len(),
            mapped_output_bytes = state.output_cursor,
            "parsed android sparse chunk"
        );

        Ok(())
    }

    async fn collect_read_plan(
        &self,
        start_offset: u64,
        read_end: u64,
    ) -> GibbloxResult<Vec<PlannedReadSegment>> {
        let state = self.chunks.lock().await;
        let mut cursor = start_offset;
        let mut chunk_index = chunk_index_for_offset(&state.chunks, cursor).ok_or_else(|| {
            GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "sparse mapping does not cover requested offset",
            )
        })?;

        let mut plan = Vec::new();
        while cursor < read_end {
            let chunk = state.chunks.get(chunk_index).copied().ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    "sparse mapping ended before requested range",
                )
            })?;
            if cursor < chunk.output_start || cursor >= chunk.output_end {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    "sparse mapping gap detected",
                ));
            }

            let segment_end = read_end.min(chunk.output_end);
            if segment_end <= cursor {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    "sparse mapping did not advance",
                ));
            }

            plan.push(PlannedReadSegment {
                chunk,
                start: cursor,
                end: segment_end,
            });

            cursor = segment_end;
            if cursor == chunk.output_end {
                chunk_index += 1;
            }
        }

        Ok(plan)
    }
}

struct PlannedReadSegment {
    chunk: SparseChunk,
    start: u64,
    end: u64,
}

fn chunk_index_for_offset(chunks: &[SparseChunk], offset: u64) -> Option<usize> {
    let index = chunks.partition_point(|chunk| chunk.output_end <= offset);
    if index < chunks.len() {
        Some(index)
    } else {
        None
    }
}

#[async_trait]
impl<S> BlockReader for AndroidSparseBlockReader<S>
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
        self.config.write_identity(out)
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

        let blocks = (buf.len() / self.block_size as usize) as u64;
        let end_lba = lba.checked_add(blocks).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "lba overflow")
        })?;
        if end_lba > self.total_blocks {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                "requested range exceeds sparse image",
            ));
        }

        let start_offset = lba.checked_mul(self.block_size as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "read start overflow")
        })?;
        let read_end = start_offset.checked_add(buf.len() as u64).ok_or_else(|| {
            GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "read end overflow")
        })?;

        self.ensure_chunks_cover_range(read_end, ctx).await?;
        let plan = self.collect_read_plan(start_offset, read_end).await?;

        for segment in plan {
            let chunk = segment.chunk;
            let dst_start = usize::try_from(segment.start - start_offset).map_err(|_| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "destination slice offset overflow",
                )
            })?;
            let dst_len = usize::try_from(segment.end - segment.start).map_err(|_| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "destination slice length overflow",
                )
            })?;
            let dst_end = dst_start.checked_add(dst_len).ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "destination slice end overflow",
                )
            })?;
            let dst = buf.get_mut(dst_start..dst_end).ok_or_else(|| {
                GibbloxError::with_message(
                    GibbloxErrorKind::OutOfRange,
                    "destination slice out of bounds",
                )
            })?;

            let chunk_offset = segment.start - chunk.output_start;
            match chunk.kind {
                SparseChunkKind::Raw { source_offset } => {
                    let source_offset =
                        source_offset.checked_add(chunk_offset).ok_or_else(|| {
                            GibbloxError::with_message(
                                GibbloxErrorKind::OutOfRange,
                                "raw chunk source offset overflow",
                            )
                        })?;
                    read_source_exact(
                        &self.source,
                        self.source_block_size,
                        self.source_size_bytes,
                        source_offset,
                        dst,
                        ctx,
                    )
                    .await?;
                }
                SparseChunkKind::Fill { pattern } => {
                    fill_pattern(dst, pattern, chunk_offset);
                }
                SparseChunkKind::DontCare => {
                    dst.fill(0);
                }
            }
        }

        trace!(lba, blocks, bytes = buf.len(), "served android sparse read");
        Ok(buf.len())
    }
}

#[derive(Clone, Copy)]
struct SparseHeader {
    magic: u32,
    major_version: u16,
    file_hdr_sz: u16,
    chunk_hdr_sz: u16,
    blk_sz: u32,
    total_blks: u32,
    total_chunks: u32,
    image_checksum: u32,
}

impl SparseHeader {
    fn parse(raw: &[u8; SPARSE_HEADER_LEN]) -> Self {
        Self {
            magic: u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]),
            major_version: u16::from_le_bytes([raw[4], raw[5]]),
            file_hdr_sz: u16::from_le_bytes([raw[8], raw[9]]),
            chunk_hdr_sz: u16::from_le_bytes([raw[10], raw[11]]),
            blk_sz: u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]),
            total_blks: u32::from_le_bytes([raw[16], raw[17], raw[18], raw[19]]),
            total_chunks: u32::from_le_bytes([raw[20], raw[21], raw[22], raw[23]]),
            image_checksum: u32::from_le_bytes([raw[24], raw[25], raw[26], raw[27]]),
        }
    }
}

#[derive(Clone, Copy)]
struct SparseChunkHeader {
    chunk_type: u16,
    chunk_sz: u32,
    total_sz: u32,
}

impl SparseChunkHeader {
    fn parse(raw: &[u8; CHUNK_HEADER_LEN]) -> Self {
        Self {
            chunk_type: u16::from_le_bytes([raw[0], raw[1]]),
            chunk_sz: u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]),
            total_sz: u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]),
        }
    }
}

#[derive(Clone, Copy)]
struct SparseChunk {
    output_start: u64,
    output_end: u64,
    kind: SparseChunkKind,
}

#[derive(Clone, Copy)]
enum SparseChunkKind {
    Raw { source_offset: u64 },
    Fill { pattern: [u8; 4] },
    DontCare,
}

fn validate_sparse_header(header: &SparseHeader) -> GibbloxResult<()> {
    if header.magic != SPARSE_HEADER_MAGIC {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "invalid Android sparse magic",
        ));
    }
    if header.major_version != SPARSE_MAJOR_VERSION {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Unsupported,
            format!(
                "unsupported Android sparse major version {}",
                header.major_version
            ),
        ));
    }
    if header.file_hdr_sz < SPARSE_HEADER_LEN as u16 {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "sparse file header is smaller than minimum size",
        ));
    }
    if header.chunk_hdr_sz < CHUNK_HEADER_LEN as u16 {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "sparse chunk header is smaller than minimum size",
        ));
    }
    if header.blk_sz == 0 || !header.blk_sz.is_power_of_two() || !header.blk_sz.is_multiple_of(4) {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "sparse block size must be a non-zero power of two and a multiple of 4",
        ));
    }
    Ok(())
}

fn claim_output_span(
    cursor: &mut u64,
    bytes: u64,
    expanded_size_bytes: u64,
    chunk_index: usize,
) -> GibbloxResult<Option<(u64, u64)>> {
    let start = *cursor;
    let end = start.checked_add(bytes).ok_or_else(|| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            format!("chunk {chunk_index} output range overflow"),
        )
    })?;
    if end > expanded_size_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            format!("chunk {chunk_index} output exceeds declared sparse image size"),
        ));
    }

    *cursor = end;
    if bytes == 0 {
        Ok(None)
    } else {
        Ok(Some((start, end)))
    }
}

async fn read_source_exact<S: BlockReader>(
    source: &S,
    source_block_size: usize,
    source_size_bytes: u64,
    offset: u64,
    out: &mut [u8],
    ctx: ReadContext,
) -> GibbloxResult<()> {
    if out.is_empty() {
        return Ok(());
    }

    let end = offset.checked_add(out.len() as u64).ok_or_else(|| {
        GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "source read range overflow")
    })?;
    if end > source_size_bytes {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source read range exceeds source size",
        ));
    }

    let bs = source_block_size as u64;
    if offset.is_multiple_of(bs) && out.len().is_multiple_of(source_block_size) {
        read_source_full_blocks(source, source_block_size, offset / bs, out, ctx).await?;
        return Ok(());
    }

    let aligned_start = (offset / bs) * bs;
    let aligned_end = end.div_ceil(bs).checked_mul(bs).ok_or_else(|| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source aligned range overflow",
        )
    })?;
    let aligned_len = usize::try_from(aligned_end - aligned_start).map_err(|_| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source aligned read exceeds addressable memory",
        )
    })?;

    let mut scratch = vec![0u8; aligned_len];
    read_source_full_blocks(
        source,
        source_block_size,
        aligned_start / bs,
        &mut scratch,
        ctx,
    )
    .await?;

    let head = usize::try_from(offset - aligned_start).map_err(|_| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source aligned head offset overflow",
        )
    })?;
    let tail = head.checked_add(out.len()).ok_or_else(|| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source aligned tail offset overflow",
        )
    })?;
    let src = scratch.get(head..tail).ok_or_else(|| {
        GibbloxError::with_message(
            GibbloxErrorKind::OutOfRange,
            "source aligned slice out of bounds",
        )
    })?;
    out.copy_from_slice(src);
    Ok(())
}

async fn read_source_full_blocks<S: BlockReader>(
    source: &S,
    source_block_size: usize,
    lba: u64,
    out: &mut [u8],
    ctx: ReadContext,
) -> GibbloxResult<()> {
    if out.is_empty() {
        return Ok(());
    }
    if !out.len().is_multiple_of(source_block_size) {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            "unaligned source block read requested",
        ));
    }

    let read = source.read_blocks(lba, out, ctx).await?;
    if read != out.len() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::Io,
            format!(
                "short read from sparse source: expected {}, got {read}",
                out.len()
            ),
        ));
    }
    Ok(())
}

fn fill_pattern(out: &mut [u8], pattern: [u8; 4], start_offset: u64) {
    let mut idx = (start_offset & 3) as usize;
    for byte in out {
        *byte = pattern[idx];
        idx = (idx + 1) & 3;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    struct FakeReader {
        block_size: u32,
        data: Vec<u8>,
        reads: AtomicU64,
    }

    impl FakeReader {
        fn reads(&self) -> u64 {
            self.reads.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl BlockReader for FakeReader {
        fn block_size(&self) -> u32 {
            self.block_size
        }

        async fn total_blocks(&self) -> GibbloxResult<u64> {
            Ok(self.data.len().div_ceil(self.block_size as usize) as u64)
        }

        fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
            write!(out, "fake-sparse:{}:{}", self.block_size, self.data.len())
        }

        async fn read_blocks(
            &self,
            lba: u64,
            buf: &mut [u8],
            _ctx: ReadContext,
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

            self.reads.fetch_add(1, Ordering::Relaxed);

            let offset = (lba as usize)
                .checked_mul(self.block_size as usize)
                .ok_or_else(|| {
                    GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "lba overflow")
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
    fn reads_mixed_raw_fill_and_dont_care_chunks() {
        let mut image = sparse_header(8, 3, 4, 28, 12);
        append_raw_chunk(&mut image, 1, b"ABCDEFGH", 12);
        append_fill_chunk(&mut image, 1, 0x4433_2211, 12);
        append_dont_care_chunk(&mut image, 1, 12);
        append_crc32_chunk(&mut image, 0xDEAD_BEEF, 12, 0);

        let source = Arc::new(FakeReader {
            block_size: 16,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");

        assert_eq!(reader.block_size(), 8);
        assert_eq!(block_on(reader.total_blocks()).expect("total blocks"), 3);
        assert_eq!(reader.expanded_size_bytes(), 24);

        let mut out = vec![0u8; 24];
        let read = block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND))
            .expect("read blocks");
        assert_eq!(read, 24);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"ABCDEFGH");
        expected.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44]);
        expected.extend_from_slice(&[0u8; 8]);
        assert_eq!(out, expected);
    }

    #[test]
    fn supports_extended_sparse_and_chunk_headers() {
        let mut image = sparse_header(4, 1, 1, 32, 16);
        append_raw_chunk(&mut image, 1, &[1, 2, 3, 4], 16);

        let source = Arc::new(FakeReader {
            block_size: 8,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");

        let mut out = vec![0u8; 4];
        let read =
            block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND)).expect("read block");
        assert_eq!(read, 4);
        assert_eq!(out, vec![1, 2, 3, 4]);
    }

    #[test]
    fn opens_without_scanning_all_chunks() {
        let mut image = sparse_header(8, 101, 101, 28, 12);
        append_raw_chunk(&mut image, 1, b"ABCDEFGH", 12);
        for _ in 0..100 {
            append_dont_care_chunk(&mut image, 1, 12);
        }

        let source = Arc::new(FakeReader {
            block_size: 16,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");
        assert_eq!(source.reads(), 1, "open should only read sparse header");

        let mut out = [0u8; 8];
        let read = block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND))
            .expect("read first sparse block");
        assert_eq!(read, out.len());
        assert_eq!(out, *b"ABCDEFGH");
        assert!(
            source.reads() < 10,
            "expected a few reads for first chunk only, got {}",
            source.reads()
        );
    }

    #[test]
    fn defers_chunk_validation_until_requested_range() {
        let mut image = sparse_header(4, 2, 2, 28, 12);
        append_raw_chunk(&mut image, 1, b"ABCD", 12);
        append_chunk_header(&mut image, CHUNK_TYPE_RAW, 1, 15, 12);
        image.extend_from_slice(&[1, 2, 3]);

        let source = Arc::new(FakeReader {
            block_size: 8,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");

        let mut first = [0u8; 4];
        let read = block_on(reader.read_blocks(0, &mut first, ReadContext::FOREGROUND))
            .expect("read first block should succeed");
        assert_eq!(read, first.len());
        assert_eq!(first, *b"ABCD");

        let mut second = [0u8; 4];
        let err = block_on(reader.read_blocks(1, &mut second, ReadContext::FOREGROUND))
            .expect_err("second block should hit invalid chunk");
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_raw_payload_size_mismatch_when_chunk_is_parsed() {
        let mut image = sparse_header(4, 1, 1, 28, 12);
        append_chunk_header(&mut image, CHUNK_TYPE_RAW, 1, 15, 12);
        image.extend_from_slice(&[1, 2, 3]);

        let source = Arc::new(FakeReader {
            block_size: 8,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");
        let mut out = vec![0u8; 4];
        let err = block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND))
            .expect_err("first read should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_crc_chunk_with_non_zero_chunk_size_when_chunk_is_parsed() {
        let mut image = sparse_header(4, 1, 1, 28, 12);
        append_crc32_chunk(&mut image, 0xAABB_CCDD, 12, 1);

        let source = Arc::new(FakeReader {
            block_size: 8,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");
        let mut out = vec![0u8; 4];
        let err = block_on(reader.read_blocks(0, &mut out, ReadContext::FOREGROUND))
            .expect_err("first read should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_chunk_count_that_cannot_fit_chunk_headers() {
        let image = sparse_header(4, 0, 2, 28, 12);

        let source = Arc::new(FakeReader {
            block_size: 8,
            data: image,
            reads: AtomicU64::new(0),
        });
        let err = match block_on(AndroidSparseBlockReader::new(Arc::clone(&source))) {
            Ok(_) => panic!("construction should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), GibbloxErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_out_of_range_reads() {
        let mut image = sparse_header(8, 2, 2, 28, 12);
        append_raw_chunk(&mut image, 1, b"ABCDEFGH", 12);
        append_dont_care_chunk(&mut image, 1, 12);

        let source = Arc::new(FakeReader {
            block_size: 16,
            data: image,
            reads: AtomicU64::new(0),
        });
        let reader = block_on(AndroidSparseBlockReader::new(Arc::clone(&source)))
            .expect("open sparse reader");

        let mut out = vec![0u8; 8];
        let err = block_on(reader.read_blocks(2, &mut out, ReadContext::FOREGROUND))
            .expect_err("out-of-range read should fail");
        assert_eq!(err.kind(), GibbloxErrorKind::OutOfRange);
    }

    fn sparse_header(
        blk_sz: u32,
        total_blks: u32,
        total_chunks: u32,
        file_hdr_sz: u16,
        chunk_hdr_sz: u16,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&SPARSE_HEADER_MAGIC.to_le_bytes());
        out.extend_from_slice(&SPARSE_MAJOR_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&file_hdr_sz.to_le_bytes());
        out.extend_from_slice(&chunk_hdr_sz.to_le_bytes());
        out.extend_from_slice(&blk_sz.to_le_bytes());
        out.extend_from_slice(&total_blks.to_le_bytes());
        out.extend_from_slice(&total_chunks.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.resize(file_hdr_sz as usize, 0);
        out
    }

    fn append_raw_chunk(out: &mut Vec<u8>, blocks: u32, payload: &[u8], chunk_hdr_sz: u16) {
        append_chunk_header(
            out,
            CHUNK_TYPE_RAW,
            blocks,
            (chunk_hdr_sz as u32) + (payload.len() as u32),
            chunk_hdr_sz,
        );
        out.extend_from_slice(payload);
    }

    fn append_fill_chunk(out: &mut Vec<u8>, blocks: u32, fill: u32, chunk_hdr_sz: u16) {
        append_chunk_header(
            out,
            CHUNK_TYPE_FILL,
            blocks,
            (chunk_hdr_sz as u32) + 4,
            chunk_hdr_sz,
        );
        out.extend_from_slice(&fill.to_le_bytes());
    }

    fn append_dont_care_chunk(out: &mut Vec<u8>, blocks: u32, chunk_hdr_sz: u16) {
        append_chunk_header(
            out,
            CHUNK_TYPE_DONT_CARE,
            blocks,
            chunk_hdr_sz as u32,
            chunk_hdr_sz,
        );
    }

    fn append_crc32_chunk(out: &mut Vec<u8>, crc: u32, chunk_hdr_sz: u16, chunk_sz: u32) {
        append_chunk_header(
            out,
            CHUNK_TYPE_CRC32,
            chunk_sz,
            (chunk_hdr_sz as u32) + 4,
            chunk_hdr_sz,
        );
        out.extend_from_slice(&crc.to_le_bytes());
    }

    fn append_chunk_header(
        out: &mut Vec<u8>,
        chunk_type: u16,
        chunk_sz: u32,
        total_sz: u32,
        chunk_hdr_sz: u16,
    ) {
        out.extend_from_slice(&chunk_type.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&chunk_sz.to_le_bytes());
        out.extend_from_slice(&total_sz.to_le_bytes());
        out.resize(out.len() + (chunk_hdr_sz as usize - CHUNK_HEADER_LEN), 0);
    }
}
