#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use alloc::{boxed::Box, format, string::String};
    use async_trait::async_trait;
    use core::{
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    use gibblox_core::{
        BlockByteReader, BlockReader, BlockReaderConfigIdentity, ByteReader, GibbloxError,
        GibbloxErrorKind, GibbloxResult, ReadContext,
    };
    use js_sys::{Promise, Uint8Array};
    use tracing::{debug, trace};
    use wasm_bindgen::JsValue;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Blob, File};

    const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

    struct SendJsFuture(JsFuture);

    unsafe impl Send for SendJsFuture {}

    impl From<Promise> for SendJsFuture {
        fn from(promise: Promise) -> Self {
            Self(JsFuture::from(promise))
        }
    }

    impl Future for SendJsFuture {
        type Output = Result<JsValue, JsValue>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0).poll(cx)
        }
    }

    #[derive(Clone)]
    struct SendFile(File);

    unsafe impl Send for SendFile {}
    unsafe impl Sync for SendFile {}

    impl SendFile {
        fn as_blob(&self) -> &Blob {
            self.0.as_ref()
        }

        fn name(&self) -> String {
            self.0.name()
        }

        fn last_modified(&self) -> f64 {
            self.0.last_modified()
        }
    }

    #[derive(Clone, Debug)]
    pub struct WebFileBlockReaderConfig {
        pub block_size: u32,
        pub file_name: String,
        pub size_bytes: u64,
        pub last_modified: i64,
    }

    impl WebFileBlockReaderConfig {
        fn from_send_file(file: &SendFile, block_size: u32) -> GibbloxResult<Self> {
            if block_size == 0 || !block_size.is_power_of_two() {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::InvalidInput,
                    "block size must be non-zero power of two",
                ));
            }

            Ok(Self {
                block_size,
                file_name: file.name(),
                size_bytes: f64_to_u64(file.as_blob().size(), "file size")?,
                last_modified: f64_to_i64(file.last_modified(), "file last_modified")?,
            })
        }

        pub fn from_file(file: &File, block_size: u32) -> GibbloxResult<Self> {
            Self::from_send_file(&SendFile(file.clone()), block_size)
        }
    }

    impl BlockReaderConfigIdentity for WebFileBlockReaderConfig {
        fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
            write!(
                out,
                "web-file:{}:{}:{}",
                self.file_name, self.size_bytes, self.last_modified
            )
        }
    }

    pub struct WebFileBlockReader {
        file: SendFile,
        config: WebFileBlockReaderConfig,
    }

    impl WebFileBlockReader {
        pub fn new(file: File, block_size: u32) -> GibbloxResult<Self> {
            let file = SendFile(file);
            let config = WebFileBlockReaderConfig::from_send_file(&file, block_size)?;
            debug!(
                name = %config.file_name,
                size_bytes = config.size_bytes,
                last_modified = config.last_modified,
                block_size = config.block_size,
                "opening web file-backed source"
            );

            Ok(Self { file, config })
        }

        pub fn config(&self) -> &WebFileBlockReaderConfig {
            &self.config
        }

        pub fn size_bytes(&self) -> u64 {
            self.config.size_bytes
        }
    }

    #[async_trait]
    impl ByteReader for WebFileBlockReader {
        async fn size_bytes(&self) -> GibbloxResult<u64> {
            Ok(self.config.size_bytes)
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
            if offset >= self.config.size_bytes {
                return Ok(0);
            }

            let remaining = self.config.size_bytes.checked_sub(offset).ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "offset out of range")
            })?;
            let read_len = (buf.len() as u64).min(remaining) as usize;
            let end = offset.checked_add(read_len as u64).ok_or_else(|| {
                GibbloxError::with_message(GibbloxErrorKind::OutOfRange, "read range overflow")
            })?;

            let promise = self
                .file
                .as_blob()
                .slice_with_f64_and_f64(
                    to_js_number(offset, "read start")?,
                    to_js_number(end, "read end")?,
                )
                .map_err(js_io("slice file"))?
                .array_buffer();
            let buffer = SendJsFuture::from(promise)
                .await
                .map_err(js_io("await file read"))?;
            let bytes = Uint8Array::new(&buffer);

            let available = bytes.length() as usize;
            if available < read_len {
                return Err(GibbloxError::with_message(
                    GibbloxErrorKind::Io,
                    format!(
                        "short read while reading web file slice: expected {read_len}, got {available}"
                    ),
                ));
            }
            bytes
                .subarray(0, read_len as u32)
                .copy_to(&mut buf[..read_len]);

            trace!(offset, requested = read_len, "performed web file byte read");
            Ok(read_len)
        }
    }

    #[async_trait]
    impl BlockReader for WebFileBlockReader {
        fn block_size(&self) -> u32 {
            self.config.block_size
        }

        async fn total_blocks(&self) -> GibbloxResult<u64> {
            Ok(self
                .config
                .size_bytes
                .div_ceil(self.config.block_size as u64))
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
            let adapter = BlockByteReader::new(self, self.config.block_size)?;
            let read = adapter.read_blocks(lba, buf, ctx).await?;
            trace!(
                lba,
                requested = buf.len(),
                read,
                "performed web file block read"
            );
            Ok(read)
        }
    }

    fn to_js_number(value: u64, label: &'static str) -> GibbloxResult<f64> {
        if value > JS_SAFE_INTEGER_MAX {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                format!("{label} exceeds JavaScript safe integer"),
            ));
        }
        Ok(value as f64)
    }

    fn f64_to_u64(value: f64, label: &'static str) -> GibbloxResult<u64> {
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("invalid {label} from web file"),
            ));
        }
        to_js_number(value as u64, label).map(|_| value as u64)
    }

    fn f64_to_i64(value: f64, label: &'static str) -> GibbloxResult<i64> {
        if !value.is_finite() || value.fract() != 0.0 {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("invalid {label} from web file"),
            ));
        }
        if value.abs() > JS_SAFE_INTEGER_MAX as f64
            || value < i64::MIN as f64
            || value > i64::MAX as f64
        {
            return Err(GibbloxError::with_message(
                GibbloxErrorKind::OutOfRange,
                format!("{label} exceeds supported range"),
            ));
        }
        Ok(value as i64)
    }

    fn js_io(op: &'static str) -> impl FnOnce(JsValue) -> GibbloxError {
        move |err| {
            GibbloxError::with_message(
                GibbloxErrorKind::Io,
                format!("{op}: {}", js_value_to_string(err)),
            )
        }
    }

    fn js_value_to_string(value: JsValue) -> String {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|s| s.as_string())
            .unwrap_or_else(|| format!("{value:?}"))
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::*;
