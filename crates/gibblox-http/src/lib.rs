//! HTTP-backed read-only gibblox source.
//!
//! Reads are serviced via HTTP range requests. Designed to work on native (reqwest)
//! and wasm32 (fetch) targets.

use async_trait::async_trait;
use gibblox_core::{
    BlockReaderConfigIdentity, ByteReader, GibbloxError, GibbloxErrorKind, GibbloxResult,
    ReadContext,
};
use std::ops::RangeInclusive;
use tracing::debug;
use url::Url;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

/// HTTP-backed read-only gibblox source.
#[derive(Clone, Debug)]
pub struct HttpReaderConfig {
    pub url: Url,
    pub block_size: u32,
    pub size_bytes: Option<u64>,
}

impl HttpReaderConfig {
    pub fn new(url: Url, block_size: u32) -> Self {
        Self {
            url,
            block_size,
            size_bytes: None,
        }
    }

    pub fn with_size(url: Url, block_size: u32, size_bytes: u64) -> Self {
        Self {
            url,
            block_size,
            size_bytes: Some(size_bytes),
        }
    }

    fn validate(&self) -> GibbloxResult<()> {
        if self.block_size == 0 || !self.block_size.is_power_of_two() {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                "block size must be non-zero power of two",
            ));
        }
        Ok(())
    }
}

impl BlockReaderConfigIdentity for HttpReaderConfig {
    fn write_identity(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(out, "http:{}", self.url.as_str())
    }
}

/// HTTP-backed read-only gibblox source.
pub struct HttpReader {
    config: HttpReaderConfig,
    size_bytes: u64,
    inner: HttpClient,
}

impl HttpReader {
    /// Construct a new HTTP source from config.
    pub async fn open(config: HttpReaderConfig) -> GibbloxResult<Self> {
        config.validate()?;
        let client = HttpClient::new()?;
        let size_bytes = if let Some(size_bytes) = config.size_bytes {
            size_bytes
        } else {
            tracing::debug!(url = %config.url, "http read source probe");
            client
                .probe_size(&config.url)
                .await
                .map_err(map_http_err("probe size"))?
        };
        Ok(Self {
            config,
            size_bytes,
            inner: client,
        })
    }

    /// Construct a new HTTP source. `url` must be absolute and point to the backing object.
    pub async fn new(url: Url, block_size: u32) -> GibbloxResult<Self> {
        Self::open(HttpReaderConfig::new(url, block_size)).await
    }

    /// Construct with an explicit size (skips remote probe).
    pub async fn new_with_size(url: Url, block_size: u32, size_bytes: u64) -> GibbloxResult<Self> {
        Self::open(HttpReaderConfig::with_size(url, block_size, size_bytes)).await
    }

    pub fn config(&self) -> &HttpReaderConfig {
        &self.config
    }

    /// Total size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Logical block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.config.block_size
    }

    fn offset_range(&self, offset: u64, len: usize) -> GibbloxResult<RangeInclusive<u64>> {
        if len == 0 {
            return Ok(offset..=offset);
        }
        let end = offset
            .checked_add(len as u64)
            .and_then(|x| x.checked_sub(1))
            .ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "range overflow")
            })?;
        Ok(offset..=end)
    }
}

#[async_trait]
impl ByteReader for HttpReader {
    async fn size_bytes(&self) -> GibbloxResult<u64> {
        Ok(self.size_bytes)
    }

    fn write_identity(&self, out: &mut dyn std::fmt::Write) -> std::fmt::Result {
        self.config.write_identity(out)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8], ctx: ReadContext) -> GibbloxResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if offset >= self.size_bytes {
            return Ok(0);
        }

        let available = self.size_bytes - offset;
        let read_len = (buf.len() as u64).min(available) as usize;
        let range = self.offset_range(offset, read_len)?;
        tracing::trace!(
            url = %self.config.url,
            start = *range.start(),
            end = *range.end(),
            len = read_len,
            "http read range"
        );
        let read = self
            .inner
            .read_range(&self.config.url, range.clone(), &mut buf[..read_len], ctx)
            .await
            .map_err(map_http_err("read range"))?;
        if read != read_len {
            debug!(
                expected = read_len,
                read,
                start = *range.start(),
                end = *range.end(),
                "partial HTTP read"
            );
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::Io,
                "short HTTP read",
            ));
        }
        Ok(read)
    }
}

#[derive(Clone)]
enum HttpClient {
    #[cfg(not(target_arch = "wasm32"))]
    Native(native::Client),
    #[cfg(target_arch = "wasm32")]
    Wasm(wasm::Client),
}

impl HttpClient {
    fn new() -> GibbloxResult<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Ok(HttpClient::Native(native::Client::new()?))
        }
        #[cfg(target_arch = "wasm32")]
        {
            Ok(HttpClient::Wasm(wasm::Client::new()?))
        }
    }

    async fn probe_size(&self, url: &Url) -> Result<u64, HttpError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            HttpClient::Native(c) => c.probe_size(url).await,
            #[cfg(target_arch = "wasm32")]
            HttpClient::Wasm(c) => c.probe_size(url).await,
        }
    }

    async fn read_range(
        &self,
        url: &Url,
        range: RangeInclusive<u64>,
        buf: &mut [u8],
        ctx: ReadContext,
    ) -> Result<usize, HttpError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            HttpClient::Native(c) => c.read_range(url, range, buf, ctx).await,
            #[cfg(target_arch = "wasm32")]
            HttpClient::Wasm(c) => c.read_range(url, range, buf, ctx).await,
        }
    }
}

fn map_http_err(op: &'static str) -> impl FnOnce(HttpError) -> GibbloxError {
    move |err| GibbloxError::with_message(GibbloxErrorKind::Io, format!("{op}: {err}"))
}

#[derive(Debug, thiserror::Error)]
enum HttpError {
    #[error("{0}")]
    Msg(String),
}
