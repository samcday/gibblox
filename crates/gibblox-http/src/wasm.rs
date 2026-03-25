use crate::HttpError;
use futures_util::FutureExt;
use gibblox_core::{GibbloxError, ReadContext, ReadPriority};
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use js_sys::{Promise, Reflect, Uint8Array};
use std::{
    future::Future,
    ops::RangeInclusive,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use url::Url;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestInit, RequestMode, Response, WorkerGlobalScope};

const READ_RANGE_MAX_ATTEMPTS: usize = 3;
const PARTIAL_CONTENT_STATUS: u16 = 206;

/// Wrapper to mark `JsFuture` as `Send` on wasm targets.
struct SendJsFuture(JsFuture);

unsafe impl Send for SendJsFuture {}

impl From<Promise> for SendJsFuture {
    fn from(promise: Promise) -> Self {
        Self(JsFuture::from(promise))
    }
}

impl Future for SendJsFuture {
    type Output = Result<wasm_bindgen::JsValue, wasm_bindgen::JsValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        FutureExt::poll_unpin(&mut self.0, cx)
    }
}

/// Send wrapper around `web_sys::Response` for wasm single-threaded use.
#[derive(Clone)]
struct SendResponse(Response);

unsafe impl Send for SendResponse {}

impl SendResponse {
    fn status(&self) -> u16 {
        self.0.status()
    }

    fn ok(&self) -> bool {
        self.0.ok()
    }

    fn headers(&self) -> Headers {
        self.0.headers()
    }

    fn array_buffer(&self) -> Result<Promise, wasm_bindgen::JsValue> {
        self.0.array_buffer()
    }
}

#[derive(Clone)]
pub struct Client {
    priority_header_enabled: Arc<AtomicBool>,
}

impl Client {
    pub fn new() -> Result<Self, GibbloxError> {
        Ok(Self {
            priority_header_enabled: Arc::new(AtomicBool::new(true)),
        })
    }

    pub async fn probe_size(
        &self,
        url: &Url,
        cors_safelisted_mode: bool,
    ) -> Result<u64, HttpError> {
        if cors_safelisted_mode {
            return self.probe_size_cors_safelisted(url).await;
        }

        let priority_header_enabled = self.priority_header_enabled() && !cors_safelisted_mode;
        if priority_header_enabled {
            match self.probe_size_once(url, true).await {
                Ok(size_bytes) => return Ok(size_bytes),
                Err(priority_err) => {
                    tracing::warn!(
                        %url,
                        error = %priority_err,
                        "http probe with Priority header failed, retrying without Priority header"
                    );
                    self.disable_priority_header();
                    let size_bytes = self
                        .probe_size_once(url, false)
                        .await
                        .map_err(|range_err| {
                            HttpError::Msg(format!(
                                "probe failed with Priority header ({priority_err}); fallback without Priority header also failed ({range_err})"
                            ))
                        })?;
                    tracing::warn!(
                        %url,
                        "Priority header disabled for this HTTP client after probe downgrade"
                    );
                    return Ok(size_bytes);
                }
            }
        }

        self.probe_size_once(url, false).await
    }

    async fn probe_size_cors_safelisted(&self, url: &Url) -> Result<u64, HttpError> {
        let head_start = now_millis();
        tracing::trace!(%url, "http probe cors-safelisted HEAD");
        match self
            .send_request(url, None, "HEAD", ReadContext::FOREGROUND, false)
            .await
        {
            Ok(resp) if resp.ok() => {
                if let Some(len) = parse_content_length_from_headers(&resp.headers()) {
                    tracing::debug!(
                        %url,
                        status = resp.status(),
                        size_bytes = len,
                        elapsed_ms = elapsed_millis_since(head_start),
                        "http probe response completed"
                    );
                    return Ok(len);
                }
            }
            Ok(_) => {}
            Err(err) => {
                tracing::debug!(
                    %url,
                    error = %err,
                    "http probe cors-safelisted HEAD failed; falling back to GET"
                );
            }
        }

        let get_start = now_millis();
        tracing::trace!(%url, "http probe cors-safelisted GET");
        let resp = self
            .send_request(url, None, "GET", ReadContext::FOREGROUND, false)
            .await
            .map_err(|err| HttpError::Msg(format!("probe GET: {err}")))?;
        if resp.ok() {
            if let Some(len) = parse_content_length_from_headers(&resp.headers()) {
                tracing::debug!(
                    %url,
                    status = resp.status(),
                    size_bytes = len,
                    elapsed_ms = elapsed_millis_since(get_start),
                    "http probe response completed"
                );
                return Ok(len);
            }
        }

        Err(HttpError::Msg("unable to determine content length".into()))
    }

    async fn probe_size_once(
        &self,
        url: &Url,
        use_priority_header: bool,
    ) -> Result<u64, HttpError> {
        // Prefer a ranged GET to coax Content-Range, fall back to HEAD/Content-Length.
        tracing::trace!(%url, "http probe range");
        let start = now_millis();
        let resp = self
            .send_request(
                url,
                Some("bytes=0-0"),
                "GET",
                ReadContext::FOREGROUND,
                use_priority_header,
            )
            .await
            .map_err(|err| HttpError::Msg(format!("probe request: {err}")))?;
        let status = resp.status();
        tracing::trace!(%url, status = status, "http probe response");
        let headers = resp.headers();
        if let Ok(Some(val)) = headers.get(CONTENT_RANGE.as_str()) {
            if let Some(len) = parse_content_range_total(&val) {
                tracing::debug!(
                    %url,
                    status = status,
                    size_bytes = len,
                    elapsed_ms = elapsed_millis_since(start),
                    "http probe response completed"
                );
                return Ok(len);
            }
        }
        if resp.ok() && status != PARTIAL_CONTENT_STATUS {
            if let Some(len) = parse_content_length_from_headers(&headers) {
                tracing::debug!(
                    %url,
                    status = status,
                    size_bytes = len,
                    elapsed_ms = elapsed_millis_since(start),
                    "http probe response completed"
                );
                return Ok(len);
            }
        }
        // Final fallback: HEAD (best effort)
        let head_start = now_millis();
        let resp = self
            .send_request(
                url,
                None,
                "HEAD",
                ReadContext::FOREGROUND,
                use_priority_header,
            )
            .await
            .map_err(|err| HttpError::Msg(format!("probe HEAD: {err}")))?;
        if resp.ok() {
            let status = resp.status();
            if let Some(len) = parse_content_length_from_headers(&resp.headers()) {
                tracing::debug!(
                    %url,
                    status = status,
                    size_bytes = len,
                    elapsed_ms = elapsed_millis_since(head_start),
                    "http probe response completed"
                );
                return Ok(len);
            }
        }
        Err(HttpError::Msg("unable to determine content length".into()))
    }

    pub async fn read_range(
        &self,
        url: &Url,
        range: RangeInclusive<u64>,
        buf: &mut [u8],
        ctx: ReadContext,
        cors_safelisted_mode: bool,
    ) -> Result<usize, HttpError> {
        let use_priority_header = self.priority_header_enabled() && !cors_safelisted_mode;
        let start = *range.start();
        let end = *range.end();
        let expected_len = range_len(start, end)?;
        let header = format!("bytes={}-{}", range.start(), range.end());
        let mut last_err = HttpError::Msg("range read did not run".into());

        for attempt in 1..=READ_RANGE_MAX_ATTEMPTS {
            let attempt_start = now_millis();
            let resp = match self
                .send_request(url, Some(&header), "GET", ctx, use_priority_header)
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    last_err = HttpError::Msg(format!("GET: {err}"));
                    tracing::warn!(
                        attempt,
                        max_attempts = READ_RANGE_MAX_ATTEMPTS,
                        start,
                        end,
                        error = %last_err,
                        "http read attempt failed before response"
                    );
                    continue;
                }
            };

            let status = resp.status();
            let (content_range, content_length) = {
                let headers = resp.headers();
                let content_range = match header_value(&headers, CONTENT_RANGE.as_str()) {
                    Ok(value) => value,
                    Err(err) => {
                        last_err = err;
                        tracing::warn!(
                            attempt,
                            max_attempts = READ_RANGE_MAX_ATTEMPTS,
                            start,
                            end,
                            error = %last_err,
                            "http read attempt failed reading Content-Range header"
                        );
                        continue;
                    }
                };
                let content_length = match header_value(&headers, CONTENT_LENGTH.as_str()) {
                    Ok(value) => value,
                    Err(err) => {
                        last_err = err;
                        tracing::warn!(
                            attempt,
                            max_attempts = READ_RANGE_MAX_ATTEMPTS,
                            start,
                            end,
                            error = %last_err,
                            "http read attempt failed reading Content-Length header"
                        );
                        continue;
                    }
                };
                (content_range, content_length)
            };

            tracing::trace!(
                attempt,
                status,
                start,
                end,
                content_range = ?content_range,
                content_length = ?content_length,
                "http read response"
            );

            if let Err(message) = validate_range_response(
                status,
                content_range.as_deref(),
                start,
                end,
                cors_safelisted_mode,
            ) {
                last_err = HttpError::Msg(message);
                tracing::warn!(
                    attempt,
                    max_attempts = READ_RANGE_MAX_ATTEMPTS,
                    start,
                    end,
                    error = %last_err,
                    "http read attempt received invalid range response"
                );
                continue;
            }

            let promise = match resp.array_buffer() {
                Ok(promise) => promise,
                Err(err) => {
                    last_err = HttpError::Msg(format!("array_buffer: {err:?}"));
                    tracing::warn!(
                        attempt,
                        max_attempts = READ_RANGE_MAX_ATTEMPTS,
                        start,
                        end,
                        error = %last_err,
                        "http read attempt failed creating array_buffer"
                    );
                    continue;
                }
            };
            let buffer = match SendJsFuture::from(promise).await {
                Ok(buffer) => buffer,
                Err(err) => {
                    last_err = HttpError::Msg(format!("array_buffer await: {err:?}"));
                    tracing::warn!(
                        attempt,
                        max_attempts = READ_RANGE_MAX_ATTEMPTS,
                        start,
                        end,
                        error = %last_err,
                        "http read attempt failed awaiting array_buffer"
                    );
                    continue;
                }
            };

            let array = Uint8Array::new(&buffer);
            let read = array.length() as usize;
            if read != expected_len {
                last_err = HttpError::Msg(format!(
                    "range body length mismatch: got {read}, expected {expected_len}"
                ));
                tracing::warn!(
                    attempt,
                    max_attempts = READ_RANGE_MAX_ATTEMPTS,
                    start,
                    end,
                    error = %last_err,
                    "http read attempt returned unexpected body length"
                );
                continue;
            }

            array.copy_to(&mut buf[..expected_len]);
            tracing::debug!(
                attempt,
                %url,
                status,
                start,
                end,
                read,
                elapsed_ms = elapsed_millis_since(attempt_start),
                content_range = ?content_range,
                content_length = ?content_length,
                "http read response completed"
            );
            tracing::trace!(
                attempt,
                read = expected_len,
                expected = buf.len(),
                "http read done"
            );
            return Ok(expected_len);
        }

        Err(last_err)
    }

    async fn send_request(
        &self,
        url: &Url,
        range: Option<&str>,
        method: &str,
        ctx: ReadContext,
        use_priority_header: bool,
    ) -> Result<SendResponse, HttpError> {
        let promise = build_request_promise(url, range, method, ctx, use_priority_header)?;
        let resp = SendJsFuture::from(promise)
            .await
            .map_err(|err| HttpError::Msg(format!("fetch await: {err:?}")))?;
        let resp: Response = resp
            .dyn_into()
            .map_err(|err| HttpError::Msg(format!("fetch dyn_into Response: {err:?}")))?;
        Ok(SendResponse(resp))
    }

    fn priority_header_enabled(&self) -> bool {
        self.priority_header_enabled.load(Ordering::Relaxed)
    }

    fn disable_priority_header(&self) {
        self.priority_header_enabled.store(false, Ordering::Relaxed);
    }
}

fn now_millis() -> f64 {
    js_sys::Date::now()
}

fn elapsed_millis_since(start_millis: f64) -> u64 {
    let elapsed = now_millis() - start_millis;
    if elapsed.is_sign_negative() {
        0
    } else {
        elapsed as u64
    }
}

fn build_request_promise(
    url: &Url,
    range: Option<&str>,
    method: &str,
    ctx: ReadContext,
    use_priority_header: bool,
) -> Result<Promise, HttpError> {
    let init = RequestInit::new();
    init.set_method(method);
    init.set_mode(RequestMode::Cors);
    let headers = Headers::new().map_err(|err| HttpError::Msg(format!("{err:?}")))?;
    if let Some(range) = range {
        headers
            .append(RANGE.as_str(), range)
            .map_err(|err| HttpError::Msg(format!("set range: {err:?}")))?;
    }
    if use_priority_header {
        headers
            .append("Priority", priority_header_value(ctx))
            .map_err(|err| HttpError::Msg(format!("set priority: {err:?}")))?;
    }
    init.set_headers(&headers);
    let _ = Reflect::set(
        init.as_ref(),
        &JsValue::from_str("priority"),
        &JsValue::from_str(fetch_priority_value(ctx)),
    );
    let request = Request::new_with_str_and_init(url.as_str(), &init)
        .map_err(|err| HttpError::Msg(format!("build request: {err:?}")))?;
    if let Some(window) = web_sys::window() {
        return Ok(window.fetch_with_request(&request));
    }
    if let Ok(worker) = js_sys::global().dyn_into::<WorkerGlobalScope>() {
        return Ok(worker.fetch_with_request(&request));
    }
    Err(HttpError::Msg("no fetch-capable web global scope".into()))
}

fn priority_header_value(ctx: ReadContext) -> &'static str {
    match ctx.priority {
        ReadPriority::High => "u=0, i",
        ReadPriority::Medium => "u=3",
        ReadPriority::Low => "u=7",
    }
}

fn range_len(start: u64, end: u64) -> Result<usize, HttpError> {
    end.checked_sub(start)
        .and_then(|delta| delta.checked_add(1))
        .and_then(|len| usize::try_from(len).ok())
        .ok_or_else(|| HttpError::Msg("range length overflow".into()))
}

fn header_value(headers: &Headers, name: &str) -> Result<Option<String>, HttpError> {
    headers
        .get(name)
        .map_err(|err| HttpError::Msg(format!("read {name} header: {err:?}")))
}

fn validate_range_response(
    status: u16,
    content_range: Option<&str>,
    expected_start: u64,
    expected_end: u64,
    cors_safelisted_mode: bool,
) -> Result<(), String> {
    if status != PARTIAL_CONTENT_STATUS {
        return Err(format!(
            "GET status {status} (expected {PARTIAL_CONTENT_STATUS} Partial Content)"
        ));
    }

    if cors_safelisted_mode {
        return Ok(());
    }

    let content_range =
        content_range.ok_or_else(|| "missing Content-Range on partial response".to_string())?;
    let (start, end, _) = parse_content_range(content_range)
        .ok_or_else(|| format!("invalid Content-Range header '{content_range}'"))?;
    if start != expected_start || end != expected_end {
        return Err(format!(
            "content-range mismatch: got bytes {start}-{end}, expected bytes {expected_start}-{expected_end}"
        ));
    }

    Ok(())
}

fn fetch_priority_value(ctx: ReadContext) -> &'static str {
    match ctx.priority {
        ReadPriority::High => "high",
        ReadPriority::Medium => "auto",
        ReadPriority::Low => "low",
    }
}

fn parse_content_range_total(hdr: &str) -> Option<u64> {
    parse_content_range(hdr).and_then(|(_, _, total)| total)
}

fn parse_content_length_from_headers(headers: &Headers) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH.as_str())
        .ok()
        .flatten()
        .and_then(|val| val.parse::<u64>().ok())
}

fn parse_content_range(hdr: &str) -> Option<(u64, u64, Option<u64>)> {
    // e.g. "bytes 0-0/12345"
    let hdr = hdr.trim().strip_prefix("bytes ")?;
    let (span, total) = hdr.split_once('/')?;
    let (start, end) = span.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    let total = if total == "*" {
        None
    } else {
        Some(total.parse::<u64>().ok()?)
    };
    Some((start, end, total))
}
