#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::{boxed::Box, string::String, vec::Vec};
use core::fmt;

use gibblox_core::{BlockReaderConfigIdentity, config_identity_string, derive_config_identity_id};
use serde::{Deserialize, Serialize};

use crate::bin::{
    PIPELINE_BIN_FORMAT_VERSION, PIPELINE_BIN_HEADER_LEN, PIPELINE_BIN_MAGIC, PipelineSourceBin,
};

pub mod bin;

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

#[cfg(feature = "std")]
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
    if format_version != PIPELINE_BIN_FORMAT_VERSION {
        return Err(PipelineCodecError::UnsupportedFormatVersion(format_version));
    }

    let payload = &bytes[PIPELINE_BIN_HEADER_LEN..];
    let pipeline: PipelineSourceBin = postcard::from_bytes(payload)?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipelineValidationError {
    EmptyHttp,
    EmptyFile,
    EmptyCasyncIndex,
    EmptyCasyncChunkStore,
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

#[cfg(feature = "std")]
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
            Ok(())
        }
        PipelineSource::File(source) => {
            if source.file.trim().is_empty() {
                return Err(PipelineValidationError::EmptyFile);
            }
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
            Ok(())
        }
        PipelineSource::Xz(source) => validate_pipeline_source(source.xz.as_ref(), depth + 1),
        PipelineSource::AndroidSparseImg(source) => {
            validate_pipeline_source(source.android_sparseimg.as_ref(), depth + 1)
        }
        PipelineSource::Mbr(source) => {
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
        PipelineSource::AndroidSparseImg(source) => {
            out.write_str("android_sparseimg{source=")?;
            write_pipeline_identity(source.android_sparseimg.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::Mbr(source) => {
            out.write_str("mbr{")?;
            write_opt_string_field(out, "partuuid", source.mbr.partuuid.as_deref())?;
            write_opt_u32_field(out, "index", source.mbr.index)?;
            out.write_str("source=")?;
            write_pipeline_identity(source.mbr.source.as_ref(), out)?;
            out.write_str("}")
        }
        PipelineSource::Gpt(source) => {
            out.write_str("gpt{")?;
            write_opt_string_field(out, "partlabel", source.gpt.partlabel.as_deref())?;
            write_opt_string_field(out, "partuuid", source.gpt.partuuid.as_deref())?;
            write_opt_u32_field(out, "index", source.gpt.index)?;
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum PipelineSource {
    Casync(PipelineSourceCasyncSource),
    Http(PipelineSourceHttpSource),
    File(PipelineSourceFileSource),
    Xz(PipelineSourceXzSource),
    AndroidSparseImg(PipelineSourceAndroidSparseImgSource),
    Mbr(PipelineSourceMbrSource),
    Gpt(PipelineSourceGptSource),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceHttpSource {
    pub http: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceFileSource {
    pub file: String,
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
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceXzSource {
    #[serde(deserialize_with = "deserialize_nested_pipeline_source")]
    pub xz: Box<PipelineSource>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceAndroidSparseImgSource {
    #[serde(deserialize_with = "deserialize_nested_pipeline_source")]
    pub android_sparseimg: Box<PipelineSource>,
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
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
enum PipelineSourceMbrValue {
    Flatten {
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        #[serde(flatten)]
        source: Box<PipelineSource>,
    },
    Source {
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        source: Box<PipelineSource>,
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
            source,
        }
        | PipelineSourceMbrValue::Source {
            partuuid,
            index,
            source,
        } => PipelineSourceMbr {
            partuuid,
            index,
            source,
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
    #[serde(flatten)]
    pub source: Box<PipelineSource>,
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
        #[serde(flatten)]
        source: Box<PipelineSource>,
    },
    Source {
        #[serde(default)]
        partlabel: Option<String>,
        #[serde(default)]
        partuuid: Option<String>,
        #[serde(default)]
        index: Option<u32>,
        source: Box<PipelineSource>,
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
            source,
        }
        | PipelineSourceGptValue::Source {
            partlabel,
            partuuid,
            index,
            source,
        } => PipelineSourceGpt {
            partlabel,
            partuuid,
            index,
            source,
        },
    })
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, string::String, vec::Vec};

    use super::{
        MAX_PIPELINE_DEPTH, PipelineCodecError, PipelineSource,
        PipelineSourceAndroidSparseImgSource, PipelineSourceCasync, PipelineSourceCasyncSource,
        PipelineSourceGpt, PipelineSourceGptSource, PipelineSourceHttpSource, PipelineSourceMbr,
        PipelineSourceMbrSource, PipelineSourceXzSource, PipelineValidationError, decode_pipeline,
        encode_pipeline, pipeline_bin_header_version, pipeline_identity_id,
        pipeline_identity_string, validate_pipeline,
    };

    #[test]
    fn validates_and_roundtrips_nested_pipeline() {
        let source = PipelineSource::Gpt(PipelineSourceGptSource {
            gpt: PipelineSourceGpt {
                partlabel: None,
                partuuid: Some(String::from("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")),
                index: None,
                source: Box::new(PipelineSource::AndroidSparseImg(
                    PipelineSourceAndroidSparseImgSource {
                        android_sparseimg: Box::new(PipelineSource::Xz(PipelineSourceXzSource {
                            xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                                http: String::from("https://cdn.example.invalid/device.img.xz"),
                            })),
                        })),
                    },
                )),
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
    fn rejects_casync_archive_index() {
        let source = PipelineSource::Casync(PipelineSourceCasyncSource {
            casync: PipelineSourceCasync {
                index: String::from("https://cdn.example.invalid/indexes/rootfs.caidx?x=1"),
                chunk_store: None,
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
                source: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                    http: String::from("https://cdn.example.invalid/rootfs.img"),
                })),
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
                source: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                    http: String::from("https://cdn.example.invalid/rootfs.img"),
                })),
            },
        });

        let err = validate_pipeline(&source).expect_err("blank partlabel should fail");
        assert_eq!(err, PipelineValidationError::EmptyGptPartlabel);
    }

    #[test]
    fn rejects_depth_over_limit() {
        let mut source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
        });

        for _ in 0..=MAX_PIPELINE_DEPTH {
            source = PipelineSource::Xz(PipelineSourceXzSource {
                xz: Box::new(source),
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
        bytes.extend_from_slice(&2u16.to_le_bytes());

        let err = decode_pipeline(&bytes).expect_err("unsupported version should fail");
        assert!(matches!(
            err,
            PipelineCodecError::UnsupportedFormatVersion(2)
        ));
    }

    #[test]
    fn reads_header_version() {
        let source = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs.img"),
        });
        let bytes = encode_pipeline(&source).expect("encode pipeline");

        assert_eq!(pipeline_bin_header_version(&bytes), Some(1));
    }

    #[test]
    fn identity_is_stable_for_same_descriptor() {
        let source = PipelineSource::Gpt(PipelineSourceGptSource {
            gpt: PipelineSourceGpt {
                partlabel: None,
                partuuid: Some(String::from("31b7f334-6df8-4f95-b4b0-c8653f8f8fbf")),
                index: None,
                source: Box::new(PipelineSource::Xz(PipelineSourceXzSource {
                    xz: Box::new(PipelineSource::Http(PipelineSourceHttpSource {
                        http: String::from("https://cdn.example.invalid/device.img.xz"),
                    })),
                })),
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
        });
        let b = PipelineSource::Http(PipelineSourceHttpSource {
            http: String::from("https://cdn.example.invalid/rootfs-b.img"),
        });

        assert_ne!(pipeline_identity_string(&a), pipeline_identity_string(&b));
        assert_ne!(pipeline_identity_id(&a), pipeline_identity_id(&b));
    }
}
