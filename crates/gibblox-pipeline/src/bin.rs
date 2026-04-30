extern crate alloc;

use alloc::{boxed::Box, string::String};

use serde::{Deserialize, Serialize};

use crate::{
    PipelineSource, PipelineSourceAndroidSparseImg, PipelineSourceAndroidSparseImgSource,
    PipelineSourceCasync, PipelineSourceCasyncSource, PipelineSourceContent,
    PipelineSourceFileSource, PipelineSourceGpt, PipelineSourceGptSource, PipelineSourceHttpSource,
    PipelineSourceMbr, PipelineSourceMbrSource, PipelineSourceTar, PipelineSourceTarSource,
    PipelineSourceXzSource,
};

pub const PIPELINE_BIN_MAGIC: [u8; 8] = *b"GBXPIPE0";
pub const PIPELINE_BIN_FORMAT_VERSION: u16 = 0;
pub const PIPELINE_BIN_HEADER_LEN: usize = PIPELINE_BIN_MAGIC.len() + 2;

/// Format versions this build can decode. v0 is intentionally unstable during
/// active development, so only the current shape is accepted.
pub const PIPELINE_BIN_SUPPORTED_VERSIONS: &[u16] = &[PIPELINE_BIN_FORMAT_VERSION];

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum PipelineSourceBin {
    Casync {
        index: String,
        chunk_store: Option<String>,
        content: Option<PipelineSourceContent>,
    },
    Http {
        url: String,
        cors_safelisted_mode: bool,
        content: Option<PipelineSourceContent>,
    },
    File {
        path: String,
        content: Option<PipelineSourceContent>,
    },
    Xz {
        source: Box<PipelineSourceBin>,
        content: Option<PipelineSourceContent>,
    },
    Tar {
        entry: String,
        source: Box<PipelineSourceBin>,
        content: Option<PipelineSourceContent>,
    },
    AndroidSparseImg {
        source: Box<PipelineSourceBin>,
        content: Option<PipelineSourceContent>,
    },
    Mbr {
        partuuid: Option<String>,
        index: Option<u32>,
        lba_size: Option<u32>,
        source: Box<PipelineSourceBin>,
        content: Option<PipelineSourceContent>,
    },
    Gpt {
        partlabel: Option<String>,
        partuuid: Option<String>,
        index: Option<u32>,
        lba_size: Option<u32>,
        source: Box<PipelineSourceBin>,
        content: Option<PipelineSourceContent>,
    },
}

impl From<PipelineSource> for PipelineSourceBin {
    fn from(source: PipelineSource) -> Self {
        match source {
            PipelineSource::Casync(PipelineSourceCasyncSource { casync }) => Self::Casync {
                index: casync.index,
                chunk_store: casync.chunk_store,
                content: casync.content,
            },
            PipelineSource::Http(PipelineSourceHttpSource {
                http,
                cors_safelisted_mode,
                content,
            }) => Self::Http {
                url: http,
                cors_safelisted_mode,
                content,
            },
            PipelineSource::File(PipelineSourceFileSource { file, content }) => Self::File {
                path: file,
                content,
            },
            PipelineSource::Xz(PipelineSourceXzSource { xz, content }) => Self::Xz {
                source: Box::new(PipelineSourceBin::from(*xz)),
                content,
            },
            PipelineSource::Tar(PipelineSourceTarSource {
                tar:
                    PipelineSourceTar {
                        entry,
                        source,
                        content,
                    },
            }) => Self::Tar {
                entry,
                source: Box::new(PipelineSourceBin::from(*source)),
                content,
            },
            PipelineSource::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
                android_sparseimg,
            }) => Self::AndroidSparseImg {
                source: Box::new(PipelineSourceBin::from(*android_sparseimg.source)),
                content: android_sparseimg.content,
            },
            PipelineSource::Mbr(PipelineSourceMbrSource {
                mbr:
                    PipelineSourceMbr {
                        partuuid,
                        index,
                        lba_size,
                        source,
                        content,
                    },
            }) => Self::Mbr {
                partuuid,
                index,
                lba_size,
                source: Box::new(PipelineSourceBin::from(*source)),
                content,
            },
            PipelineSource::Gpt(PipelineSourceGptSource {
                gpt:
                    PipelineSourceGpt {
                        partlabel,
                        partuuid,
                        index,
                        lba_size,
                        source,
                        content,
                    },
            }) => Self::Gpt {
                partlabel,
                partuuid,
                index,
                lba_size,
                source: Box::new(PipelineSourceBin::from(*source)),
                content,
            },
        }
    }
}

impl From<PipelineSourceBin> for PipelineSource {
    fn from(source: PipelineSourceBin) -> Self {
        match source {
            PipelineSourceBin::Casync {
                index,
                chunk_store,
                content,
            } => Self::Casync(PipelineSourceCasyncSource {
                casync: PipelineSourceCasync {
                    index,
                    chunk_store,
                    content,
                },
            }),
            PipelineSourceBin::Http {
                url,
                cors_safelisted_mode,
                content,
            } => Self::Http(PipelineSourceHttpSource {
                http: url,
                cors_safelisted_mode,
                content,
            }),
            PipelineSourceBin::File { path, content } => Self::File(PipelineSourceFileSource {
                file: path,
                content,
            }),
            PipelineSourceBin::Xz { source, content } => Self::Xz(PipelineSourceXzSource {
                xz: Box::new(PipelineSource::from(*source)),
                content,
            }),
            PipelineSourceBin::Tar {
                entry,
                source,
                content,
            } => Self::Tar(PipelineSourceTarSource {
                tar: PipelineSourceTar {
                    entry,
                    source: Box::new(PipelineSource::from(*source)),
                    content,
                },
            }),
            PipelineSourceBin::AndroidSparseImg { source, content } => {
                Self::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
                    android_sparseimg: PipelineSourceAndroidSparseImg {
                        source: Box::new(PipelineSource::from(*source)),
                        content,
                    },
                })
            }
            PipelineSourceBin::Mbr {
                partuuid,
                index,
                lba_size,
                source,
                content,
            } => Self::Mbr(PipelineSourceMbrSource {
                mbr: PipelineSourceMbr {
                    partuuid,
                    index,
                    lba_size,
                    source: Box::new(PipelineSource::from(*source)),
                    content,
                },
            }),
            PipelineSourceBin::Gpt {
                partlabel,
                partuuid,
                index,
                lba_size,
                source,
                content,
            } => Self::Gpt(PipelineSourceGptSource {
                gpt: PipelineSourceGpt {
                    partlabel,
                    partuuid,
                    index,
                    lba_size,
                    source: Box::new(PipelineSource::from(*source)),
                    content,
                },
            }),
        }
    }
}
