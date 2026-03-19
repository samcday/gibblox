#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use alloc::{string::String, string::ToString, vec::Vec};
use core::fmt;
use serde::{Deserialize, Serialize};

use crate::bin::{
    PIPELINE_HINTS_BIN_FORMAT_VERSION, PIPELINE_HINTS_BIN_HEADER_LEN, PIPELINE_HINTS_BIN_MAGIC,
};

pub mod bin;

// v0 is intentionally unstable during active development. Prefer clean breaking
// changes over compatibility shims while this format is expected to churn.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineHints {
    pub entries: Vec<PipelineHintEntry>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineHintEntry {
    pub pipeline_identity: String,
    pub hints: Vec<PipelineHint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum PipelineHint {
    AndroidSparseIndex(PipelineAndroidSparseIndexHint),
    ContentDigest(PipelineContentDigestHint),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineContentDigestHint {
    pub digest: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineAndroidSparseIndexHint {
    pub file_hdr_sz: u16,
    pub chunk_hdr_sz: u16,
    pub blk_sz: u32,
    pub total_blks: u32,
    pub total_chunks: u32,
    pub image_checksum: u32,
    pub chunks: Vec<PipelineAndroidSparseChunkIndexHint>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineAndroidSparseChunkIndexHint {
    pub chunk_index: u32,
    pub chunk_type: u16,
    pub chunk_sz: u32,
    pub total_sz: u32,
    pub chunk_offset: u64,
    pub payload_offset: u64,
    pub payload_size: u64,
    pub output_start: Option<u64>,
    pub output_end: Option<u64>,
    pub fill_pattern: Option<[u8; 4]>,
    pub crc32: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipelineHintsValidationError {
    EmptyIdentity,
    DuplicateIdentity {
        pipeline_identity: String,
    },
    UnsortedIdentity {
        previous_identity: String,
        pipeline_identity: String,
    },
    EmptyHints {
        pipeline_identity: String,
    },
    DuplicateHintTypeAndroidSparseIndex {
        pipeline_identity: String,
    },
    DuplicateHintTypeContentDigest {
        pipeline_identity: String,
    },
}

impl fmt::Display for PipelineHintsValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentity => write!(f, "pipeline hint entry has an empty pipeline_identity"),
            Self::DuplicateIdentity { pipeline_identity } => write!(
                f,
                "duplicate pipeline hint identity '{}' is not allowed",
                pipeline_identity
            ),
            Self::UnsortedIdentity {
                previous_identity,
                pipeline_identity,
            } => write!(
                f,
                "pipeline hint identities must be sorted; '{}' appears after '{}'",
                pipeline_identity, previous_identity
            ),
            Self::EmptyHints { pipeline_identity } => write!(
                f,
                "pipeline hint entry '{}' must contain at least one hint",
                pipeline_identity
            ),
            Self::DuplicateHintTypeAndroidSparseIndex { pipeline_identity } => write!(
                f,
                "pipeline hint entry '{}' contains duplicate AndroidSparseIndex hints",
                pipeline_identity
            ),
            Self::DuplicateHintTypeContentDigest { pipeline_identity } => write!(
                f,
                "pipeline hint entry '{}' contains duplicate ContentDigest hints",
                pipeline_identity
            ),
        }
    }
}

#[derive(Debug)]
pub enum PipelineHintsCodecError {
    Decode(postcard::Error),
    InvalidMagic,
    UnsupportedFormatVersion(u16),
    Validate(PipelineHintsValidationError),
}

impl fmt::Display for PipelineHintsCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(err) => write!(f, "decode pipeline hints: {err}"),
            Self::InvalidMagic => {
                write!(
                    f,
                    "invalid pipeline hints magic (expected {PIPELINE_HINTS_BIN_MAGIC:?})"
                )
            }
            Self::UnsupportedFormatVersion(version) => write!(
                f,
                "unsupported pipeline hints format version {version} (expected {PIPELINE_HINTS_BIN_FORMAT_VERSION})"
            ),
            Self::Validate(err) => write!(f, "{err}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for PipelineHintsCodecError {}

impl From<postcard::Error> for PipelineHintsCodecError {
    fn from(err: postcard::Error) -> Self {
        Self::Decode(err)
    }
}

impl From<PipelineHintsValidationError> for PipelineHintsCodecError {
    fn from(err: PipelineHintsValidationError) -> Self {
        Self::Validate(err)
    }
}

pub fn validate_pipeline_hints(hints: &PipelineHints) -> Result<(), PipelineHintsValidationError> {
    let mut previous_identity: Option<&str> = None;
    for entry in &hints.entries {
        let identity = entry.pipeline_identity.as_str();
        if identity.trim().is_empty() {
            return Err(PipelineHintsValidationError::EmptyIdentity);
        }
        if let Some(previous) = previous_identity {
            if previous == identity {
                return Err(PipelineHintsValidationError::DuplicateIdentity {
                    pipeline_identity: identity.to_string(),
                });
            }
            if previous > identity {
                return Err(PipelineHintsValidationError::UnsortedIdentity {
                    previous_identity: previous.to_string(),
                    pipeline_identity: identity.to_string(),
                });
            }
        }
        previous_identity = Some(identity);

        if entry.hints.is_empty() {
            return Err(PipelineHintsValidationError::EmptyHints {
                pipeline_identity: identity.to_string(),
            });
        }

        let mut seen_android_sparse = false;
        let mut seen_content_digest = false;
        for hint in &entry.hints {
            match hint {
                PipelineHint::AndroidSparseIndex(_) => {
                    if seen_android_sparse {
                        return Err(
                            PipelineHintsValidationError::DuplicateHintTypeAndroidSparseIndex {
                                pipeline_identity: identity.to_string(),
                            },
                        );
                    }
                    seen_android_sparse = true;
                }
                PipelineHint::ContentDigest(_) => {
                    if seen_content_digest {
                        return Err(
                            PipelineHintsValidationError::DuplicateHintTypeContentDigest {
                                pipeline_identity: identity.to_string(),
                            },
                        );
                    }
                    seen_content_digest = true;
                }
            }
        }
    }
    Ok(())
}

pub fn encode_pipeline_hints(hints: &PipelineHints) -> Result<Vec<u8>, PipelineHintsCodecError> {
    let mut canonical = hints.clone();
    canonical
        .entries
        .sort_by(|a, b| a.pipeline_identity.cmp(&b.pipeline_identity));
    validate_pipeline_hints(&canonical)?;

    let payload = postcard::to_allocvec(&canonical)?;
    let mut out = Vec::with_capacity(PIPELINE_HINTS_BIN_HEADER_LEN + payload.len());
    out.extend_from_slice(&PIPELINE_HINTS_BIN_MAGIC);
    out.extend_from_slice(&PIPELINE_HINTS_BIN_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn decode_pipeline_hints(bytes: &[u8]) -> Result<PipelineHints, PipelineHintsCodecError> {
    let Some(format_version) = pipeline_hints_bin_header_version(bytes) else {
        return Err(PipelineHintsCodecError::InvalidMagic);
    };
    if format_version != PIPELINE_HINTS_BIN_FORMAT_VERSION {
        return Err(PipelineHintsCodecError::UnsupportedFormatVersion(
            format_version,
        ));
    }

    let payload = &bytes[PIPELINE_HINTS_BIN_HEADER_LEN..];
    let hints: PipelineHints = postcard::from_bytes(payload)?;
    validate_pipeline_hints(&hints)?;
    Ok(hints)
}

pub fn decode_pipeline_hints_prefix(
    bytes: &[u8],
) -> Result<(PipelineHints, usize), PipelineHintsCodecError> {
    let Some(format_version) = pipeline_hints_bin_header_version(bytes) else {
        return Err(PipelineHintsCodecError::InvalidMagic);
    };
    if format_version != PIPELINE_HINTS_BIN_FORMAT_VERSION {
        return Err(PipelineHintsCodecError::UnsupportedFormatVersion(
            format_version,
        ));
    }

    let payload = &bytes[PIPELINE_HINTS_BIN_HEADER_LEN..];
    let (hints, remaining): (PipelineHints, &[u8]) = postcard::take_from_bytes(payload)?;
    validate_pipeline_hints(&hints)?;
    let consumed = PIPELINE_HINTS_BIN_HEADER_LEN
        .checked_add(payload.len() - remaining.len())
        .expect("pipeline hints consumed length overflow");
    Ok((hints, consumed))
}

pub fn pipeline_hints_bin_header_version(bytes: &[u8]) -> Option<u16> {
    if bytes.len() < PIPELINE_HINTS_BIN_HEADER_LEN {
        return None;
    }
    if bytes[..PIPELINE_HINTS_BIN_MAGIC.len()] != PIPELINE_HINTS_BIN_MAGIC {
        return None;
    }
    Some(u16::from_le_bytes([
        bytes[PIPELINE_HINTS_BIN_MAGIC.len()],
        bytes[PIPELINE_HINTS_BIN_MAGIC.len() + 1],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn encode_sorts_entries() {
        let hints = PipelineHints {
            entries: vec![
                PipelineHintEntry {
                    pipeline_identity: String::from("z"),
                    hints: vec![PipelineHint::AndroidSparseIndex(
                        PipelineAndroidSparseIndexHint {
                            file_hdr_sz: 28,
                            chunk_hdr_sz: 12,
                            blk_sz: 4096,
                            total_blks: 1,
                            total_chunks: 1,
                            image_checksum: 0,
                            chunks: Vec::new(),
                        },
                    )],
                },
                PipelineHintEntry {
                    pipeline_identity: String::from("a"),
                    hints: vec![PipelineHint::AndroidSparseIndex(
                        PipelineAndroidSparseIndexHint {
                            file_hdr_sz: 28,
                            chunk_hdr_sz: 12,
                            blk_sz: 4096,
                            total_blks: 1,
                            total_chunks: 1,
                            image_checksum: 0,
                            chunks: Vec::new(),
                        },
                    )],
                },
            ],
        };

        let encoded = encode_pipeline_hints(&hints).expect("encode hints");
        let decoded = decode_pipeline_hints(encoded.as_slice()).expect("decode hints");
        assert_eq!(decoded.entries[0].pipeline_identity, "a");
        assert_eq!(decoded.entries[1].pipeline_identity, "z");
    }

    #[test]
    fn decode_rejects_duplicate_identity() {
        let hints = PipelineHints {
            entries: vec![
                PipelineHintEntry {
                    pipeline_identity: String::from("same"),
                    hints: vec![PipelineHint::AndroidSparseIndex(
                        PipelineAndroidSparseIndexHint {
                            file_hdr_sz: 28,
                            chunk_hdr_sz: 12,
                            blk_sz: 4096,
                            total_blks: 1,
                            total_chunks: 1,
                            image_checksum: 0,
                            chunks: Vec::new(),
                        },
                    )],
                },
                PipelineHintEntry {
                    pipeline_identity: String::from("same"),
                    hints: vec![PipelineHint::AndroidSparseIndex(
                        PipelineAndroidSparseIndexHint {
                            file_hdr_sz: 28,
                            chunk_hdr_sz: 12,
                            blk_sz: 4096,
                            total_blks: 1,
                            total_chunks: 1,
                            image_checksum: 0,
                            chunks: Vec::new(),
                        },
                    )],
                },
            ],
        };

        let payload = postcard::to_allocvec(&hints).expect("encode raw payload");
        let mut raw = Vec::new();
        raw.extend_from_slice(&PIPELINE_HINTS_BIN_MAGIC);
        raw.extend_from_slice(&PIPELINE_HINTS_BIN_FORMAT_VERSION.to_le_bytes());
        raw.extend_from_slice(payload.as_slice());

        let err = decode_pipeline_hints(raw.as_slice()).expect_err("must reject duplicate");
        assert!(matches!(
            err,
            PipelineHintsCodecError::Validate(
                PipelineHintsValidationError::DuplicateIdentity { .. }
            )
        ));
    }
}
