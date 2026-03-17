extern crate alloc;

use alloc::{boxed::Box, string::String};

use serde::{Deserialize, Serialize};

use crate::{
    PipelineSource, PipelineSourceAndroidSparseImg, PipelineSourceAndroidSparseImgSource,
    PipelineSourceCasync, PipelineSourceCasyncSource, PipelineSourceFileSource, PipelineSourceGpt,
    PipelineSourceGptSource, PipelineSourceHttpSource, PipelineSourceMbr, PipelineSourceMbrSource,
    PipelineSourceXzSource,
};

pub const PIPELINE_BIN_MAGIC: [u8; 8] = *b"GBXPIPE0";
pub const PIPELINE_BIN_FORMAT_VERSION: u16 = 1;
pub const PIPELINE_BIN_HEADER_LEN: usize = PIPELINE_BIN_MAGIC.len() + 2;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum PipelineSourceBin {
    Casync {
        index: String,
        chunk_store: Option<String>,
    },
    Http {
        url: String,
    },
    File {
        path: String,
    },
    Xz {
        source: Box<PipelineSourceBin>,
    },
    AndroidSparseImg {
        source: Box<PipelineSourceBin>,
    },
    Mbr {
        partuuid: Option<String>,
        index: Option<u32>,
        source: Box<PipelineSourceBin>,
    },
    Gpt {
        partlabel: Option<String>,
        partuuid: Option<String>,
        index: Option<u32>,
        source: Box<PipelineSourceBin>,
    },
}

impl From<PipelineSource> for PipelineSourceBin {
    fn from(source: PipelineSource) -> Self {
        match source {
            PipelineSource::Casync(PipelineSourceCasyncSource { casync }) => Self::Casync {
                index: casync.index,
                chunk_store: casync.chunk_store,
            },
            PipelineSource::Http(PipelineSourceHttpSource { http }) => Self::Http { url: http },
            PipelineSource::File(PipelineSourceFileSource { file }) => Self::File { path: file },
            PipelineSource::Xz(PipelineSourceXzSource { xz }) => Self::Xz {
                source: Box::new(PipelineSourceBin::from(*xz)),
            },
            PipelineSource::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
                android_sparseimg,
            }) => Self::AndroidSparseImg {
                source: Box::new(PipelineSourceBin::from(*android_sparseimg.source)),
            },
            PipelineSource::Mbr(PipelineSourceMbrSource {
                mbr:
                    PipelineSourceMbr {
                        partuuid,
                        index,
                        source,
                    },
            }) => Self::Mbr {
                partuuid,
                index,
                source: Box::new(PipelineSourceBin::from(*source)),
            },
            PipelineSource::Gpt(PipelineSourceGptSource {
                gpt:
                    PipelineSourceGpt {
                        partlabel,
                        partuuid,
                        index,
                        source,
                    },
            }) => Self::Gpt {
                partlabel,
                partuuid,
                index,
                source: Box::new(PipelineSourceBin::from(*source)),
            },
        }
    }
}

impl From<PipelineSourceBin> for PipelineSource {
    fn from(source: PipelineSourceBin) -> Self {
        match source {
            PipelineSourceBin::Casync { index, chunk_store } => {
                Self::Casync(PipelineSourceCasyncSource {
                    casync: PipelineSourceCasync { index, chunk_store },
                })
            }
            PipelineSourceBin::Http { url } => Self::Http(PipelineSourceHttpSource { http: url }),
            PipelineSourceBin::File { path } => Self::File(PipelineSourceFileSource { file: path }),
            PipelineSourceBin::Xz { source } => Self::Xz(PipelineSourceXzSource {
                xz: Box::new(PipelineSource::from(*source)),
            }),
            PipelineSourceBin::AndroidSparseImg { source } => {
                Self::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
                    android_sparseimg: PipelineSourceAndroidSparseImg {
                        source: Box::new(PipelineSource::from(*source)),
                    },
                })
            }
            PipelineSourceBin::Mbr {
                partuuid,
                index,
                source,
            } => Self::Mbr(PipelineSourceMbrSource {
                mbr: PipelineSourceMbr {
                    partuuid,
                    index,
                    source: Box::new(PipelineSource::from(*source)),
                },
            }),
            PipelineSourceBin::Gpt {
                partlabel,
                partuuid,
                index,
                source,
            } => Self::Gpt(PipelineSourceGptSource {
                gpt: PipelineSourceGpt {
                    partlabel,
                    partuuid,
                    index,
                    source: Box::new(PipelineSource::from(*source)),
                },
            }),
        }
    }
}
