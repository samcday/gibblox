#![cfg(not(target_arch = "wasm32"))]

use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::Response,
    routing::get,
};
use gibblox_pipeline::{
    PipelineSource, PipelineSourceContent, PipelineSourceHttpSource, encode_pipeline,
};
use serde::Deserialize;
use sha2::{Digest, Sha512};
use std::net::SocketAddr;
use std::path::{Component, Path as FsPath, PathBuf};
use std::process::Command;
use tokio::net::TcpListener;

const DEFAULT_PORT: u16 = 4173;
const DEFAULT_NOISE_SEED: u64 = 0xC01D_CAFE_BADC_0FFE;
const DEFAULT_NOISE_SIZE: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    root_dir: PathBuf,
    pkg_dir: PathBuf,
    default_seed: u64,
    default_size: u64,
}

#[derive(Debug, Deserialize)]
struct PipelineQuery {
    seed: Option<u64>,
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct NoiseQuery {
    seed: Option<u64>,
    size: Option<u64>,
}

#[derive(Clone, Copy)]
struct ByteRange {
    start: u64,
    end: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    ensure_wasm_bundle(&root_dir)?;

    let start_port = std::env::var("GIBBLOX_PIPELINE_PUMP_PORT")
        .ok()
        .and_then(|raw| raw.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    let state = AppState {
        pkg_dir: root_dir.join("pkg"),
        root_dir,
        default_seed: DEFAULT_NOISE_SEED,
        default_size: DEFAULT_NOISE_SIZE,
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/index.html", get(index_handler))
        .route("/worker.js", get(worker_handler))
        .route("/pkg/{*path}", get(pkg_handler))
        .route("/pipeline.bin", get(pipeline_handler))
        .route("/noise.bin", get(noise_handler).head(noise_handler))
        .with_state(state);

    let (listener, addr) = bind_listener(start_port).await?;

    println!("gibblox pipeline pump server listening on http://{addr}");
    println!(
        "app: http://{addr}/      generated pipeline: http://{addr}/pipeline.bin?seed={DEFAULT_NOISE_SEED}&size={DEFAULT_NOISE_SIZE}"
    );

    axum::serve(listener, app).await?;
    Ok(())
}

async fn bind_listener(
    start_port: u16,
) -> Result<(TcpListener, SocketAddr), Box<dyn std::error::Error>> {
    let max_attempts = 20u16;
    let mut last_err: Option<std::io::Error> = None;

    for offset in 0..max_attempts {
        let port = start_port.saturating_add(offset);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok((listener, addr)),
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                last_err = Some(err);
            }
            Err(err) => return Err(Box::new(err)),
        }
    }

    match last_err {
        Some(err) => Err(format!(
            "failed to bind local HTTP server starting at port {start_port}: {err}"
        )
        .into()),
        None => Err("failed to bind local HTTP server".into()),
    }
}

async fn index_handler(State(state): State<AppState>) -> Response {
    static_file_response(state.root_dir.join("index.html")).await
}

async fn worker_handler(State(state): State<AppState>) -> Response {
    static_file_response(state.root_dir.join("worker.js")).await
}

async fn pkg_handler(Path(path): Path<String>, State(state): State<AppState>) -> Response {
    let Some(path) = sanitize_relative_path(path.as_str()) else {
        return text_response(StatusCode::BAD_REQUEST, "invalid pkg path");
    };
    static_file_response(state.pkg_dir.join(path)).await
}

async fn pipeline_handler(
    State(state): State<AppState>,
    Query(query): Query<PipelineQuery>,
    headers: HeaderMap,
) -> Response {
    let seed = query.seed.unwrap_or(state.default_seed);
    let size = normalize_size(query.size.unwrap_or(state.default_size));
    let host = request_host(&headers);
    let scheme = request_scheme(&headers);
    let noise_url = format!("{scheme}://{host}/noise.bin?seed={seed}&size={size}");
    let content = match tokio::task::spawn_blocking(move || noise_content(seed, size)).await {
        Ok(content) => content,
        Err(err) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to compute content metadata: {err}").as_str(),
            );
        }
    };

    let pipeline = PipelineSource::Http(PipelineSourceHttpSource {
        http: noise_url,
        cors_safelisted_mode: false,
        content: Some(content),
    });
    let pipeline_bytes = match encode_pipeline(&pipeline) {
        Ok(bytes) => bytes,
        Err(err) => {
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode pipeline: {err}").as_str(),
            );
        }
    };

    let mut response =
        response_with_bytes(StatusCode::OK, "application/octet-stream", pipeline_bytes);
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("inline; filename=\"pipeline.bin\""),
    );
    response
}

async fn noise_handler(
    State(state): State<AppState>,
    Query(query): Query<NoiseQuery>,
    headers: HeaderMap,
    method: Method,
) -> Response {
    let seed = query.seed.unwrap_or(state.default_seed);
    let total_size = normalize_size(query.size.unwrap_or(state.default_size));

    let Some(raw_range) = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
    else {
        if method == Method::HEAD {
            return noise_head_response(total_size);
        }
        return text_response(
            StatusCode::BAD_REQUEST,
            "noise endpoint requires a Range header (for example: bytes=0-4095)",
        );
    };

    let range = match parse_range_header(raw_range, total_size) {
        Ok(range) => range,
        Err(err) => {
            return noise_range_error(total_size, err);
        }
    };

    let length_u64 = range.end.saturating_sub(range.start).saturating_add(1);
    let length = match usize::try_from(length_u64) {
        Ok(length) => length,
        Err(_) => {
            return text_response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "requested range length exceeds this server's limits",
            );
        }
    };

    let mut response = if method == Method::HEAD {
        response_with_bytes(
            StatusCode::PARTIAL_CONTENT,
            "application/octet-stream",
            Vec::new(),
        )
    } else {
        let mut body = vec![0u8; length];
        fill_noise(seed, range.start, &mut body);
        response_with_bytes(
            StatusCode::PARTIAL_CONTENT,
            "application/octet-stream",
            body,
        )
    };

    let content_range = format!("bytes {}-{}/{total_size}", range.start, range.end);
    let content_length = HeaderValue::from_str(length_u64.to_string().as_str())
        .expect("range length should fit valid header value");
    let content_range = HeaderValue::from_str(content_range.as_str())
        .expect("content-range should fit valid header value");

    let headers = response.headers_mut();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_LENGTH, content_length);
    headers.insert(header::CONTENT_RANGE, content_range);
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );

    response
}

fn noise_head_response(total_size: u64) -> Response {
    let mut response = response_with_bytes(StatusCode::OK, "application/octet-stream", Vec::new());
    let headers = response.headers_mut();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(total_size.to_string().as_str())
            .expect("noise size should fit valid header value"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

fn noise_range_error(total_size: u64, message: &str) -> Response {
    let mut response = text_response(StatusCode::RANGE_NOT_SATISFIABLE, message);
    let header_value = format!("bytes */{total_size}");
    if let Ok(value) = HeaderValue::from_str(header_value.as_str()) {
        response.headers_mut().insert(header::CONTENT_RANGE, value);
    }
    response
}

async fn static_file_response(path: PathBuf) -> Response {
    let body = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                return text_response(
                    StatusCode::NOT_FOUND,
                    format!("asset not found: {}", path.display()).as_str(),
                );
            }
            return text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to read asset {}: {err}", path.display()).as_str(),
            );
        }
    };

    let content_type = mime_guess::from_path(&path)
        .first_or_octet_stream()
        .essence_str()
        .to_string();
    response_with_bytes(StatusCode::OK, content_type.as_str(), body)
}

fn response_with_bytes(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response
}

fn text_response(status: StatusCode, message: &str) -> Response {
    response_with_bytes(
        status,
        "text/plain; charset=utf-8",
        message.as_bytes().to_vec(),
    )
}

fn sanitize_relative_path(raw: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for component in FsPath::new(raw).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if out.as_os_str().is_empty() {
        None
    } else {
        Some(out)
    }
}

fn normalize_size(size: u64) -> u64 {
    size.max(1)
}

fn request_host(headers: &HeaderMap) -> String {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|host| !host.is_empty())
        .unwrap_or("127.0.0.1:4173")
        .to_string()
}

fn request_scheme(headers: &HeaderMap) -> &'static str {
    let forwarded = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if forwarded.eq_ignore_ascii_case("https") {
        "https"
    } else {
        "http"
    }
}

fn parse_range_header(raw_range: &str, total_size: u64) -> Result<ByteRange, &'static str> {
    if total_size == 0 {
        return Err("resource has no bytes");
    }

    let spec = raw_range
        .trim()
        .strip_prefix("bytes=")
        .ok_or("Range header must start with 'bytes='")?;

    if spec.contains(',') {
        return Err("multiple ranges are unsupported");
    }

    let (start_raw, end_raw) = spec
        .split_once('-')
        .ok_or("invalid Range header: expected start-end")?;

    if start_raw.is_empty() {
        let suffix = end_raw
            .parse::<u64>()
            .map_err(|_| "invalid suffix Range value")?;
        if suffix == 0 {
            return Err("suffix range must be non-zero");
        }
        let start = total_size.saturating_sub(suffix);
        let end = total_size.saturating_sub(1);
        return Ok(ByteRange { start, end });
    }

    let start = start_raw
        .parse::<u64>()
        .map_err(|_| "invalid range start value")?;
    if start >= total_size {
        return Err("range start exceeds resource size");
    }

    let end = if end_raw.is_empty() {
        total_size.saturating_sub(1)
    } else {
        end_raw
            .parse::<u64>()
            .map_err(|_| "invalid range end value")?
    };

    if end < start {
        return Err("range end precedes range start");
    }

    Ok(ByteRange {
        start,
        end: end.min(total_size.saturating_sub(1)),
    })
}

fn fill_noise(seed: u64, start: u64, out: &mut [u8]) {
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = noise_byte(seed, start.saturating_add(index as u64));
    }
}

fn noise_content(seed: u64, total_size: u64) -> PipelineSourceContent {
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut hasher = Sha512::new();
    let mut offset = 0u64;
    let mut chunk = vec![0u8; CHUNK_SIZE];

    while offset < total_size {
        let remaining = total_size - offset;
        let len = remaining.min(CHUNK_SIZE as u64) as usize;
        fill_noise(seed, offset, &mut chunk[..len]);
        hasher.update(&chunk[..len]);
        offset += len as u64;
    }

    let digest_bytes = hasher.finalize();
    let mut digest = String::from("sha512:");
    digest.reserve(128);
    for byte in digest_bytes {
        digest.push(hex_char(byte >> 4));
        digest.push(hex_char(byte & 0x0f));
    }

    PipelineSourceContent {
        digest,
        size_bytes: total_size,
    }
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + (nibble - 10)) as char,
    }
}

fn noise_byte(seed: u64, offset: u64) -> u8 {
    let mut x = seed.wrapping_add(offset.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x >> 56) as u8
}

fn ensure_wasm_bundle(root_dir: &FsPath) -> Result<(), Box<dyn std::error::Error>> {
    let pkg_js = root_dir.join("pkg/gibblox_pipeline_pump_web.js");
    let pkg_wasm = root_dir.join("pkg/gibblox_pipeline_pump_web_bg.wasm");

    println!("building wasm app bundle with wasm-pack ...");
    let status = Command::new("wasm-pack")
        .args(["build", "--target", "web", "--out-dir", "pkg"])
        .current_dir(root_dir)
        .status();

    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(format!("wasm-pack failed with status {status}").into()),
        Err(err) => {
            if pkg_js.is_file() && pkg_wasm.is_file() {
                eprintln!(
                    "warning: failed to execute wasm-pack ({err}); reusing existing pkg bundle"
                );
                Ok(())
            } else {
                Err(format!("failed to execute wasm-pack: {err}").into())
            }
        }
    }
}
