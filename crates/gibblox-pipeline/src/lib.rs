#![cfg_attr(not(any(feature = "std", feature = "web")), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, string::String, string::ToString, vec::Vec};
use core::fmt;

use gibblox_core::{BlockReaderConfigIdentity, config_identity_string, derive_config_identity_id};
use serde::{Deserialize, Serialize};

use crate::bin::{
    PIPELINE_BIN_FORMAT_VERSION, PIPELINE_BIN_HEADER_LEN, PIPELINE_BIN_MAGIC, PipelineSourceBin,
};

pub use gibblox_schema::bin::{
    PIPELINE_HINTS_BIN_FORMAT_VERSION, PIPELINE_HINTS_BIN_HEADER_LEN, PIPELINE_HINTS_BIN_MAGIC,
};
pub use gibblox_schema::{
    PipelineAndroidSparseChunkIndexHint, PipelineAndroidSparseIndexHint, PipelineContentDigestHint,
    PipelineHint, PipelineHintEntry, PipelineHints, PipelineHintsCodecError,
    PipelineHintsValidationError, PipelineTarEntryIndexHint, decode_pipeline_hints,
    decode_pipeline_hints_prefix, encode_pipeline_hints, pipeline_hints_bin_header_version,
    validate_pipeline_hints,
};

pub mod bin;

#[cfg(any(feature = "std", all(feature = "web", target_arch = "wasm32")))]
mod materialize_common;

#[cfg(feature = "std")]
mod materialize_std;

#[cfg(all(feature = "web", target_arch = "wasm32"))]
mod materialize_web;

#[cfg(feature = "std")]
pub use materialize_std::{OpenPipelineOptions, open_pipeline};

#[cfg(all(feature = "web", target_arch = "wasm32"))]
pub use materialize_web::{
    OpenPipelineOptions as OpenWebPipelineOptions, open_pipeline as open_pipeline_web,
};

#[derive(Debug)]
pub enum PipelineCodecError {
    Decode(postcard::Error),
    InvalidMagic,
    UnsupportedFormatVersion(u16),
}

impl fmt::Display for PipelineCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(err) => write!(f, "decode pipeline: {err}"),
            Self::InvalidMagic => {
                write!(
                    f,
                    "invalid pipeline magic (expected {PIPELINE_BIN_MAGIC:?})"
                )
            }
            Self::UnsupportedFormatVersion(version) => write!(
                f,
                "unsupported pipeline format version {version} (expected {PIPELINE_BIN_FORMAT_VERSION})"
            ),
        }
    }
}

#[cfg(any(feature = "std", feature = "web"))]
impl std::error::Error for PipelineCodecError {}

impl From<postcard::Error> for PipelineCodecError {
    fn from(err: postcard::Error) -> Self {
        Self::Decode(err)
    }
}

pub fn decode_pipeline(bytes: &[u8]) -> Result<PipelineSource, PipelineCodecError> {
    let Some(format_version) = pipeline_bin_header_version(bytes) else {
        return Err(PipelineCodecError::InvalidMagic);
    };

    let payload = &bytes[PIPELINE_BIN_HEADER_LEN..];
    let pipeline = match format_version {
        3 => postcard::from_bytes::<PipelineSourceBin>(payload)?,
        2 => {
            let v2: bin::PipelineSourceBinV2 = postcard::from_bytes(payload)?;
            PipelineSourceBin::from(v2)
        }
        _ => return Err(PipelineCodecError::UnsupportedFormatVersion(format_version)),
    };
    Ok(PipelineSource::from(pipeline))
}

pub fn encode_pipeline(source: &PipelineSource) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(&PipelineSourceBin::from(source.clone()))?;
    let mut out = Vec::with_capacity(PIPELINE_BIN_HEADER_LEN + payload.len());
    out.extend_from_slice(&PIPELINE_BIN_MAGIC);
    out.extend_from_slice(&PIPELINE_BIN_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn pipeline_bin_header_version(bytes: &[u8]) -> Option<u16> {
    if bytes.len() < PIPELINE_BIN_HEADER_LEN {
        return None;
    }
    if bytes[..PIPELINE_BIN_MAGIC.len()] != PIPELINE_BIN_MAGIC {
        return None;
    }
    Some(u16::from_le_bytes([
        bytes[PIPELINE_BIN_MAGIC.len()],
        bytes[PIPELINE_BIN_MAGIC.len() + 1],
    ]))
}

pub fn pipeline_identity_string(source: &PipelineSource) -> String {
    config_identity_string(source)
}

pub fn pipeline_identity_id(source: &PipelineSource) -> u32 {
    derive_config_identity_id(source)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PipelineCachePolicy {
    #[default]
    None,
    Head,
    Tail,
}

impl PipelineCachePolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Head => "head",
            Self::Tail => "tail",
        }
    }
}

impl fmt::Display for PipelineCachePolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipelineValidationError {
    EmptyHttp,
    EmptyFile,
    EmptyCasyncIndex,
    EmptyCasyncChunkStore,
    EmptyTarEntry,
    MissingHttpContent,
    MissingFileContent,
    MissingCasyncContent,
    EmptyContentDigest,
    InvalidContentDigestPrefix { digest: String },
    InvalidContentDigestLength { digest: String, hex_len: usize },
    InvalidContentDigestHex { digest: String },
    UnsupportedCasyncArchiveIndex { index: String },
    PipelineDepthExceeded { max_depth: usize },
    InvalidMbrSelectorCount { selectors: usize },
    EmptyMbrPartuuid,
    InvalidGptSelectorCount { selectors: usize },
    EmptyGptPartlabel,
    EmptyGptPartuuid,
}

impl fmt::Display for PipelineValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyHttp => write!(f, "pipeline http source must not be empty"),
            Self::EmptyFile => write!(f, "pipeline file source must not be empty"),
            Self::EmptyCasyncIndex => write!(f, "pipeline casync.index source must not be empty"),
            Self::EmptyCasyncChunkStore => {
                write!(f, "pipeline casync.chunk_store source must not be empty")
            }
            Self::EmptyTarEntry => write!(f, "pipeline tar.entry must not be empty"),
            Self::MissingHttpContent => {
                write!(f, "pipeline http source must include content metadata")
            }
            Self::MissingFileContent => {
                write!(f, "pipeline file source must include content metadata")
            }
            Self::MissingCasyncContent => {
                write!(f, "pipeline casync source must include content metadata")
            }
            Self::EmptyContentDigest => {
                write!(f, "pipeline content digest must not be empty")
            }
            Self::InvalidContentDigestPrefix { digest } => write!(
                f,
                "pipeline content digest must use 'sha512:' prefix, got '{digest}'"
            ),
            Self::InvalidContentDigestLength { digest, hex_len } => write!(
                f,
                "pipeline content digest '{digest}' must include exactly 128 lowercase hex chars, got {hex_len}"
            ),
            Self::InvalidContentDigestHex { digest } => write!(
                f,
                "pipeline content digest '{digest}' must contain only lowercase hex chars"
            ),
            Self::UnsupportedCasyncArchiveIndex { index } => write!(
                f,
                "unsupported casync archive index (.caidx) in pipeline: {index}; expected casync blob index (.caibx)"
            ),
            Self::PipelineDepthExceeded { max_depth } => {
                write!(f, "pipeline exceeds max depth {max_depth}")
            }
            Self::InvalidMbrSelectorCount { selectors } => write!(
                f,
                "pipeline mbr step must specify exactly one selector (partuuid or index); found {selectors}"
            ),
            Self::EmptyMbrPartuuid => write!(f, "pipeline mbr partuuid must not be empty"),
            Self::InvalidGptSelectorCount { selectors } => write!(
                f,
                "pipeline gpt step must specify exactly one selector (partlabel, partuuid, or index); found {selectors}"
            ),
            Self::EmptyGptPartlabel => write!(f, "pipeline gpt partlabel must not be empty"),
            Self::EmptyGptPartuuid => write!(f, "pipeline gpt partuuid must not be empty"),
        }
    }
}

#[cfg(any(feature = "std", feature = "web"))]
impl std::error::Error for PipelineValidationError {}

pub const MAX_PIPELINE_DEPTH: usize = 16;

pub fn validate_pipeline(source: &PipelineSource) -> Result<(), PipelineValidationError> {
    validate_pipeline_source(source, 0)
}

fn validate_pipeline_source(
    source: &PipelineSource,
    depth: usize,
) -> Result<(), PipelineValidationError> {
    if depth > MAX_PIPELINE_DEPTH {
        return Err(PipelineValidationError::PipelineDepthExceeded {
            max_depth: MAX_PIPELINE_DEPTH,
        });
    }

    match source {
        PipelineSource::Http(source) => {
            if source.http.trim().is_empty() {
                return Err(PipelineValidationError::EmptyHttp);
            }
            let Some(content) = source.content.as_ref() else {
                return Err(PipelineValidationError::MissingHttpContent);
            };
            validate_pipeline_content(content)?;
            Ok(())
        }
        PipelineSource::File(source) => {
            if source.file.trim().is_empty() {
                return Err(PipelineValidationError::EmptyFile);
            }
            let Some(content) = source.content.as_ref() else {
                return Err(PipelineValidationError::MissingFileContent);
            };
            validate_pipeline_content(content)?;
            Ok(())
        }
        PipelineSource::Casync(source) => {
            let index = source.casync.index.trim();
            if index.is_empty() {
                return Err(PipelineValidationError::EmptyCasyncIndex);
            }

            let index_path = strip_query_and_fragment(index);
            if index_path.to_ascii_lowercase().ends_with(".caidx") {
                return Err(PipelineValidationError::UnsupportedCasyncArchiveIndex {
                    index: source.casync.index.clone(),
                });
            }

            if let Some(chunk_store) = source.casync.chunk_store.as_deref() {
                if chunk_store.trim().is_empty() {
                    return Err(PipelineValidationError::EmptyCasyncChunkStore);
                }
            }
            let Some(content) = source.casync.content.as_ref() else {
                return Err(PipelineValidationError::MissingCasyncContent);
            };
            validate_pipeline_content(content)?;
            Ok(())
        }
        PipelineSource::Xz(source) => {
            if let Some(content) = source.content.as_ref() {
                validate_pipeline_content(content)?;
            }
            validate_pipeline_source(source.xz.as_ref(), depth + 1)
        }
        PipelineSource::Tar(source) => {
            if source.tar.entry.trim().is_empty() {
                return Err(PipelineValidationError::EmptyTarEntry);
            }
            if let Some(content) = source.tar.content.as_ref() {
                validate_pipeline_content(content)?;
            }
            validate_pipeline_source(source.tar.source.as_ref(), depth + 1)
        }
        PipelineSource::AndroidSparseImg(source) => {
            if let Some(content) = source.android_sparseimg.content.as_ref() {
                validate_pipeline_content(content)?;
            }
            validate_pipeline_source(source.android_sparseimg.source.as_ref(), depth + 1)
        }
        PipelineSource::Mbr(source) => {
            if let Some(content) = source.mbr.content.as_ref() {
                validate_pipeline_content(content)?;
            }
            let mut selectors = 0usize;
            if let Some(partuuid) = source.mbr.partuuid.as_deref() {
                if partuuid.trim().is_empty() {
                    return Err(PipelineValidationError::EmptyMbrPartuuid);
                }
                selectors += 1;
            }
            if source.mbr.index.is_some() {
                selectors += 1;
            }
            if selectors != 1 {
                return Err(PipelineValidationError::InvalidMbrSelectorCount { selectors });
            }
            validate_pipeline_source(source.mbr.source.as_ref(), depth + 1)
        }
        PipelineSource::Gpt(source) => {
            if let Some(content) = source.gpt.content.as_ref() {
                validate_pipeline_content(content)?;
            }
            let mut selectors = 0usize;
            if let Some(partlabel) = source.gpt.partlabel.as_deref() {
                if partlabel.trim().is_empty() {
                    return Err(PipelineValidationError::EmptyGptPartlabel);
                }
                selectors += 1;
            }
            if let Some(partuuid) = source.gpt.partuuid.as_deref() {
                if partuuid.trim().is_empty() {
                    return Err(PipelineValidationError::EmptyGptPartuuid);
                }
                selectors += 1;
            }
            if source.gpt.index.is_some() {
                selectors += 1;
            }
            if selectors != 1 {
                return Err(PipelineValidationError::InvalidGptSelectorCount { selectors });
            }
            validate_pipeline_source(source.gpt.source.as_ref(), depth + 1)
        }
    }
}

fn validate_pipeline_content(
    content: &PipelineSourceContent,
) -> Result<(), PipelineValidationError> {
    let digest = content.digest.trim();
    if digest.is_empty() {
        return Err(PipelineValidationError::EmptyContentDigest);
    }

    let Some(hex) = digest.strip_prefix("sha512:") else {
        return Err(PipelineValidationError::InvalidContentDigestPrefix {
            digest: digest.to_string(),
        });
    };

    if hex.len() != 128 {
        return Err(PipelineValidationError::InvalidContentDigestLength {
            digest: digest.to_string(),
            hex_len: hex.len(),
        });
    }

    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(PipelineValidationError::InvalidContentDigestHex {
            digest: digest.to_string(),
        });
    }

    Ok(())
}

fn strip_query_and_fragment(value: &str) -> &str {
    let mut end = value.len();
    if let Some(pos) = value.find('?') {
        end = end.min(pos);
    }
    if let Some(pos) = value.find('#') {
        end = end.min(pos);
    }
    &value[..end]
}

impl BlockReaderConfigIdentity for PipelineSource {
    fn write_identity(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        write_pipeline_identity(self, out)
    }
}

fn write_pipeline_identity(source: &PipelineSource, out: &mut dyn fmt::Write) -> fmt::Result {
    match source {
        PipelineSource::Http(source) => {
            out.write_str("http{")?;
            write_string_field(out, "url", source.http.as_str())?;
            write_bool_field(out, "cors_safelisted_mode", source.cors_safelisted_mode)?;
            out.write_str("}")
        }
        PipelineSource::File(source) => {
            out.write_str("file{")?;
            write_string_field(out, "path", source.file.as_str())?;
            out.write_str("}")
        }
        PipelineSource::Casync(source) => {
            out.write_str("casync{")?;
            write_string_field(out, "index", source.casync.index.as_str())?;
            write_opt_string_field(out, "chunk_store", source.casync.chunk_store.as_deref())?;
            out.write_str("}")
        }
        PipelineSource::Xz(source) => {
            out.write_str("xz{source=")?;
            write_pipeline_identity(source.xz.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::Tar(source) => {
            out.write_str("tar{")?;
            write_string_field(out, "entry", source.tar.entry.as_str())?;
            out.write_str("source=")?;
            write_pipeline_identity(source.tar.source.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::AndroidSparseImg(source) => {
            out.write_str("android_sparseimg{source=")?;
            write_pipeline_identity(source.android_sparseimg.source.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::Mbr(source) => {
            out.write_str("mbr{")?;
            write_opt_string_field(out, "partuuid", source.mbr.partuuid.as_deref())?;
            write_opt_u32_field(out, "index", source.mbr.index)?;
            write_opt_u32_field(out, "lba_size", source.mbr.lba_size)?;
            out.write_str("source=")?;
            write_pipeline_identity(source.mbr.source.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::Gpt(source) => {
            out.write_str("gpt{")?;
            write_opt_string_field(out, "partlabel", source.gpt.partlabel.as_deref())?;
            write_opt_string_field(out, "partuuid", source.gpt.partuuid.as_deref())?;
            write_opt_u32_field(out, "index", source.gpt.index)?;
            write_opt_u32_field(out, "lba_size", source.gpt.lba_size)?;
            out.write_str("source=")?;
            write_pipeline_identity(source.gpt.source.as_ref(), out)?;
            out.write_str("}")
        }
    }
}

fn write_string_field(out: &mut dyn fmt::Write, key: &str, value: &str) -> fmt::Result {
    write!(out, "{key}=len:{}:", value.len())?;
    out.write_str(value)?;
    out.write_str(";")
}

fn write_opt_string_field(out: &mut dyn fmt::Write, key: &str, value: Option<&str>) -> fmt::Result {
    match value {
        Some(value) => {
            out.write_str(key)?;
            out.write_str("=some:")?;
            write_string_field(out, "value", value)
        }
        None => {
            out.write_str(key)?;
            out.write_str("=none;")
        }
    }
}

fn write_opt_u32_field(out: &mut dyn fmt::Write, key: &str, value: Option<u32>) -> fmt::Result {
    match value {
        Some(value) => {
            out.write_str(key)?;
            write!(out, "=some:{value};")
        }
        None => {
            out.write_str(key)?;
            out.write_str("=none;")
        }
    }
}

fn write_bool_field(out: &mut dyn fmt::Write, key: &str, value: bool) -> fmt::Result {
    write!(out, "{key}={value};")
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum PipelineSource {
    Casync(PipelineSourceCasyncSource),
    Http(PipelineSourceHttpSource),
    File(PipelineSourceFileSource),
    Xz(PipelineSourceXzSource),
    Tar(PipelineSourceTarSource),
    AndroidSparseImg(PipelineSourceAndroidSparseImgSource),
    Mbr(PipelineSourceMbrSource),
    Gpt(PipelineSourceGptSource),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceHttpSource {
    pub http: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub cors_safelisted_mode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceFileSource {
    pub file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceCasyncSource {
    #[serde(deserialize_with = "deserialize_casync_source")]
    pub casync: PipelineSourceCasync,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceCasyncValue {
    Index(String),
    Source(PipelineSourceCasync),
}

fn deserialize_casync_source<'de, D>(deserializer: D) -> Result<PipelineSourceCasync, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = PipelineSourceCasyncValue::deserialize(deserializer)?;
    Ok(match value {
        PipelineSourceCasyncValue::Index(index) => PipelineSourceCasync {
            index,
            chunk_store: None,
            content: None,
        },
        PipelineSourceCasyncValue::Source(source) => source,
    })
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceCasync {
    pub index: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_store: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceContent {
    pub digest: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceXzSource {
    #[serde(deserialize_with = "deserialize_nested_pipeline_source")]
    pub xz: Box<PipelineSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceTarSource {
    #[serde(deserialize_with = "deserialize_tar_source")]
    pub tar: PipelineSourceTar,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PipelineSourceTar {
    pub entry: String,
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceAndroidSparseImgSource {
    #[serde(deserialize_with = "deserialize_android_sparseimg_source")]
    pub android_sparseimg: PipelineSourceAndroidSparseImg,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PipelineSourceAndroidSparseImg {
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum NestedPipelineSourceValue {
    Direct(Box<PipelineSource>),
    Source { source: Box<PipelineSource> },
}

fn deserialize_nested_pipeline_source<'de, D>(
    deserializer: D,
) -> Result<Box<PipelineSource>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = NestedPipelineSourceValue::deserialize(deserializer)?;
    Ok(match value {
        NestedPipelineSourceValue::Direct(source) => source,
        NestedPipelineSourceValue::Source { source } => source,
    })
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceTarValue {
    Flatten {
        entry: String,
        #[serde(flatten)]
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
    Source {
        entry: String,
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
}

fn deserialize_tar_source<'de, D>(deserializer: D) -> Result<PipelineSourceTar, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = PipelineSourceTarValue::deserialize(deserializer)?;
    Ok(match value {
        PipelineSourceTarValue::Flatten {
            entry,
            source,
            content,
        }
        | PipelineSourceTarValue::Source {
            entry,
            source,
            content,
        } => PipelineSourceTar {
            entry,
            source,
            content,
        },
    })
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceAndroidSparseImgValue {
    Flatten {
        #[serde(flatten)]
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
    Source {
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
}

fn deserialize_android_sparseimg_source<'de, D>(
    deserializer: D,
) -> Result<PipelineSourceAndroidSparseImg, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = PipelineSourceAndroidSparseImgValue::deserialize(deserializer)?;
    Ok(match value {
        PipelineSourceAndroidSparseImgValue::Flatten { source, content }
        | PipelineSourceAndroidSparseImgValue::Source { source, content } => {
            PipelineSourceAndroidSparseImg { source, content }
        }
    })
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceMbrSource {
    #[serde(deserialize_with = "deserialize_mbr_source")]
    pub mbr: PipelineSourceMbr,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PipelineSourceMbr {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partuuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lba_size: Option<u32>,
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceMbrValue {
    Flatten {
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        #[serde(default)]
        lba_size: Option<u32>,
        #[serde(flatten)]
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
    Source {
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        #[serde(default)]
        lba_size: Option<u32>,
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
}

fn deserialize_mbr_source<'de, D>(deserializer: D) -> Result<PipelineSourceMbr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = PipelineSourceMbrValue::deserialize(deserializer)?;
    Ok(match value {
        PipelineSourceMbrValue::Flatten {
            partuuid,
            index,
            lba_size,
            source,
            content,
        }
        | PipelineSourceMbrValue::Source {
            partuuid,
            index,
            lba_size,
            source,
            content,
        } => PipelineSourceMbr {
            partuuid,
            index,
            lba_size,
            source,
            content,
        },
    })
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceGptSource {
    #[serde(deserialize_with = "deserialize_gpt_source")]
    pub gpt: PipelineSourceGpt,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct PipelineSourceGpt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partlabel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partuuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lba_size: Option<u32>,
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<PipelineSourceContent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceGptValue {
    Flatten {
        #[serde(default)]
        partlabel: Option<String>,
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        #[serde(default)]
        lba_size: Option<u32>,
        #[serde(flatten)]
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
    Source {
        #[serde(default)]
        partlabel: Option<String>,
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        #[serde(default)]
        lba_size: Option<u32>,
        source: Box<PipelineSource>,
        #[serde(default)]
        content: Option<PipelineSourceContent>,
    },
}

fn deserialize_gpt_source<'de, D>(deserializer: D) -> Result<PipelineSourceGpt, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = PipelineSourceGptValue::deserialize(deserializer)?;
    Ok(match value {
        PipelineSourceGptValue::Flatten {
            partlabel,
            partuuid,
            index,
            lba_size,
            source,
            content,
        }
        | PipelineSourceGptValue::Source {
            partlabel,
            partuuid,
            index,
            lba_size,
            source,
            content,
        } => PipelineSourceGpt {
            partlabel,
            partuuid,
            index,
            lba_size,
            source,
            content,
        },
    })
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, string::String, vec::Vec};

    use super::{
        MAX_PIPELINE_DEPTH, PipelineCodecError, PipelineSource, PipelineSourceAndroidSparseImg,
        PipelineSourceAndroidSparseImgSource, PipelineSourceCasync, PipelineSourceCasyncSource,
        PipelineSourceContent, PipelineSourceGpt, PipelineSourceGptSource,
        PipelineSourceHttpSource, PipelineSourceMbr, PipelineSourceMbrSource, PipelineSourceTar,
        PipelineSourceTarSource, PipelineSourceXzSource, PipelineValidationError, decode_pipeline,
        encode_pipeline, pipeline_bin_header_version, pipeline_identity_id,
        pipeline_identity_string, validate_pipeline,
    };

    fn sample_content() -> PipelineSourceContent {
        PipelineSourceContent {
            digest: String::from(
                "sha512:11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
            ),
            size_bytes: 123,
        }
    }

    #[test]
    fn validates_and_roundtrips_nested_pipeline() {
        let source = PipelineSource::Gpt(PipelineSourceGptSource {
            gpt: PipelineSourceGpt {
                partlabel: None,
                partuuid: Some(String::from("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")),
                index: None,
                lba_size: None,
                source: Box::new(PipelineSource::AndroidSparseImg(
                    PipelineSourceAndroidSparseImgSource {
                        android_sparseimg: PipelineSourceAndroidSparseImg {
                            source: Box::new(PipelineSource::Xz(PipelineSourceXzSource {
                                xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                                    http: String::from("https://cdn.example.invalid/device.img.xz"),
                                    cors_safelisted_mode: false,
                                    content: Some(sample_content()),
                                })),
                                content: None,
                            })),
                            content: None,
                        },
                    },
                )),
                content: None,
            },
        });

        validate_pipeline(&source).expect("pipeline should validate");
        let encoded = encode_pipeline(&source).expect("encode pipeline");
        let decoded = decode_pipeline(&encoded).expect("decode pipeline");
        assert_eq!(decoded, source);
    }

    #[test]
    fn parses_source_style_yaml_pipeline() {
        let source: PipelineSource = serde_yaml::from_str(
            r#"
gpt:
  partlabel: rootfs
  source:
    android_sparseimg:
      source:
        xz:
          source:
            http: https://cdn.example.invalid/device.img.xz
            content:
              digest: sha512:11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
              size_bytes: 123
"#,
        )
        .expect("parse source-style YAML");

        validate_pipeline(&source).expect("source-style pipeline should validate");
        match source {
            PipelineSource::Gpt(source) => {
                assert_eq!(source.gpt.partlabel.as_deref(), Some("rootfs"));
            }
            other => panic!("expected gpt source, got {other:?}"),
        }
    }

    #[test]
    fn validates_and_roundtrips_tar_pipeline() {
        let source = PipelineSource::Tar(PipelineSourceTarSource {
            tar: PipelineSourceTar {
                entry: String::from("/rootfs.img"),
                source: Box::new(PipelineSource::Xz(PipelineSourceXzSource {
                    xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                        http: String::from("https://cdn.example.invalid/rootfs.tar.xz"),
                        cors_safelisted_mode: false,
                        content: Some(sample_content()),
                    })),
                    content: None,
                })),
                content: None,
            },
        });

        validate_pipeline(&source).expect("tar pipeline should validate");
        let encoded = encode_pipeline(&source).expect("encode pipeline");
        let decoded = decode_pipeline(&encoded).expect("decode pipeline");
        assert_eq!(decoded, source);
    }

    #[test]
    fn parses_tar_source_style_yaml_pipeline() {
        let source: PipelineSource = serde_yaml::from_str(
            r#"
tar:
  entry: /rootfs.img
  source:
    xz:
      source:
        http: https://cdn.example.invalid/rootfs.tar.xz
        content:
          digest: sha512:11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
          size_bytes: 123
"#,
        )
        .expect("parse tar source-style YAML");

        validate_pipeline(&source).expect("tar source-style pipeline should validate");
        match source {
            PipelineSource::Tar(source) => {
                assert_eq!(source.tar.entry.as_str(), "/rootfs.img");
            }
            other => panic!("expected tar source, got {other:?}"),
        }
    }

    #[test]
    fn rejects_casync_archive_index() {
        let source = PipelineSource::Casync(PipelineSourceCasyncSource {
            casync: PipelineSourceCasync {
                index: String::from("https://cdn.example.invalid/indexes/rootfs.caidx?x=1"),
                chunk_store: None,
                content: Some(sample_content()),
            },
        });

        let err = validate_pipeline(&source).expect_err(".caidx should be rejected");
        assert!(matches!(
            err,
            PipelineValidationError::UnsupportedCasyncArchiveIndex { .. }
        ));
    }

    #[test]
    fn rejects_mbr_with_no_selector() {
        let source = PipelineSource::Mbr(PipelineSourceMbrSource {
            mbr: PipelineSourceMbr {
                partuuid: None,
                index: None,
                lba_size: None,
                source: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                    http: String::from("https://cdn.example.invalid/rootfs.img"),
                    cors_safelisted_mode: false,
                    content: Some(sample_content()),
                })),
                content: None,
            },
        });

        let err = validate_pipeline(&source).expect_err("mbr selector cardinality should fail");
        assert_eq!(
            err,
            PipelineValidationError::InvalidMbrSelectorCount { selectors: 0 }
        );
    }

    #[test]
    fn rejects_gpt_with_empty_partlabel() {
        let source = PipelineSource::Gpt(PipelineSourceGptSource {
            gpt: PipelineSourceGpt {
                partlabel: Some(String::from("  ")),
                partuuid: None,
                index: None,
                lba_size: None,
                source: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                    http: String::from("https://cdn.example.invalid/rootfs.img"),
                    cors_safelisted_mode: false,
                    content: Some(sample_content()),
                })),
                content: None,
            },
        });

        let err = validate_pipeline(&source).expect_err("blank partlabel should fail");
        assert_eq!(err, PipelineValidationError::EmptyGptPartlabel);
    }

    #[test]
    fn rejects_depth_over_limit() {
        let mut source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: false,
            content: Some(sample_content()),
        });

        for _ in 0..=MAX_PIPELINE_DEPTH {
            source = PipelineSource::Xz(PipelineSourceXzSource {
                xz: Box::new(source),
                content: None,
            });
        }

        let err = validate_pipeline(&source).expect_err("depth over max should fail");
        assert_eq!(
            err,
            PipelineValidationError::PipelineDepthExceeded {
                max_depth: MAX_PIPELINE_DEPTH,
            }
        );
    }

    #[test]
    fn rejects_invalid_magic() {
        let err = decode_pipeline(b"not-a-pipeline").expect_err("invalid magic should fail");
        assert!(matches!(err, PipelineCodecError::InvalidMagic));
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&crate::bin::PIPELINE_BIN_MAGIC);
        bytes.extend_from_slice(&99u16.to_le_bytes());

        let err = decode_pipeline(&bytes).expect_err("unsupported version should fail");
        assert!(matches!(
            err,
            PipelineCodecError::UnsupportedFormatVersion(99)
        ));
    }

    #[test]
    fn reads_header_version() {
        let source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: false,
            content: Some(sample_content()),
        });
        let bytes = encode_pipeline(&source).expect("encode pipeline");

        assert_eq!(pipeline_bin_header_version(&bytes), Some(3));
    }

    #[test]
    fn decodes_v2_payload_with_lba_size_none() {
        // Build a v2-shaped GPT payload by hand: encode it via the v2 mirror
        // struct in `bin`, slap on the v=2 header, then decode through the
        // public path and assert the lba_size field defaults to None.
        let v2 = crate::bin::PipelineSourceBinV2::Gpt {
            partlabel: None,
            partuuid: Some(String::from("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")),
            index: None,
            source: Box::new(crate::bin::PipelineSourceBinV2::Http {
                url: String::from("https://cdn.example.invalid/rootfs.img"),
                cors_safelisted_mode: false,
                content: Some(sample_content()),
            }),
            content: None,
        };
        let payload = postcard::to_allocvec(&v2).expect("encode v2");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&crate::bin::PIPELINE_BIN_MAGIC);
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&payload);

        let decoded = decode_pipeline(&bytes).expect("decode v2 payload");
        match decoded {
            PipelineSource::Gpt(g) => {
                assert_eq!(g.gpt.lba_size, None);
                assert_eq!(
                    g.gpt.partuuid.as_deref(),
                    Some("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")
                );
            }
            other => panic!("expected gpt, got {other:?}"),
        }
    }

    #[test]
    fn identity_is_stable_for_same_descriptor() {
        let source = PipelineSource::Gpt(PipelineSourceGptSource {
            gpt: PipelineSourceGpt {
                partlabel: None,
                partuuid: Some(String::from("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")),
                index: None,
                lba_size: None,
                source: Box::new(PipelineSource::Xz(PipelineSourceXzSource {
                    xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                        http: String::from("https://cdn.example.invalid/device.img.xz"),
                        cors_safelisted_mode: false,
                        content: Some(sample_content()),
                    })),
                    content: None,
                })),
                content: None,
            },
        });

        assert_eq!(
            pipeline_identity_string(&source),
            pipeline_identity_string(&source)
        );
        assert_eq!(pipeline_identity_id(&source), pipeline_identity_id(&source));
    }

    #[test]
    fn identity_changes_when_descriptor_changes() {
        let a = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs-a.img"),
            cors_safelisted_mode: false,
            content: Some(sample_content()),
        });
        let b = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs-b.img"),
            cors_safelisted_mode: false,
            content: Some(sample_content()),
        });

        assert_ne!(pipeline_identity_string(&a), pipeline_identity_string(&b));
        assert_ne!(pipeline_identity_id(&a), pipeline_identity_id(&b));
    }

    #[test]
    fn identity_changes_when_cors_safelisted_mode_changes() {
        let strict = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: false,
            content: Some(sample_content()),
        });
        let safelisted = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: true,
            content: Some(sample_content()),
        });

        assert_ne!(
            pipeline_identity_string(&strict),
            pipeline_identity_string(&safelisted)
        );
        assert_ne!(
            pipeline_identity_id(&strict),
            pipeline_identity_id(&safelisted)
        );
    }

    #[test]
    fn rejects_http_without_content() {
        let source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: false,
            content: None,
        });

        let err = validate_pipeline(&source).expect_err("terminal content should be required");
        assert_eq!(err, PipelineValidationError::MissingHttpContent);
    }

    #[test]
    fn accepts_missing_wrapper_content() {
        let source = PipelineSource::Xz(PipelineSourceXzSource {
            xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                http: String::from("https://cdn.example.invalid/rootfs.img"),
                cors_safelisted_mode: false,
                content: Some(sample_content()),
            })),
            content: None,
        });

        validate_pipeline(&source).expect("wrapper content is optional");
    }

    #[test]
    fn rejects_invalid_content_digest() {
        let source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
            cors_safelisted_mode: false,
            content: Some(PipelineSourceContent {
                digest: String::from("sha512:NOTHEX"),
                size_bytes: 99,
            }),
        });

        let err = validate_pipeline(&source).expect_err("digest must be lowercase hex");
        assert!(matches!(
            err,
            PipelineValidationError::InvalidContentDigestLength { .. }
        ));
    }

    #[test]
    fn parses_http_cors_safelisted_mode() {
        let source: PipelineSource = serde_yaml::from_str(
            r#"
http: https://cdn.example.invalid/rootfs.img
cors_safelisted_mode: true
content:
  digest: sha512:11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
  size_bytes: 123
"#,
        )
        .expect("parse http pipeline with cors safelisted mode");

        match source {
            PipelineSource::Http(source) => {
                assert!(source.cors_safelisted_mode);
            }
            other => panic!("expected http source, got {other:?}"),
        }
    }
}
