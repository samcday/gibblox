extern crate alloc;

use alloc::{boxed::Box, string::String};

use serde::{Deserialize, Serialize};

use crate::{
    PipelineAndroidSparseChunkIndex, PipelineAndroidSparseIndex, PipelineSource,
    PipelineSourceAndroidSparseImg, PipelineSourceAndroidSparseImgSource, PipelineSourceCasync,
    PipelineSourceCasyncSource, PipelineSourceFileSource, PipelineSourceGpt,
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
        index: Option<PipelineAndroidSparseIndexBin>,
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineAndroidSparseIndexBin {
    pub file_hdr_sz: u16,
    pub chunk_hdr_sz: u16,
    pub blk_sz: u32,
    pub total_blks: u32,
    pub total_chunks: u32,
    pub image_checksum: u32,
    pub chunks: Box<[PipelineAndroidSparseChunkIndexBin]>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PipelineAndroidSparseChunkIndexBin {
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
                index: android_sparseimg
                    .index
                    .map(PipelineAndroidSparseIndexBin::from),
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
            PipelineSourceBin::AndroidSparseImg { source, index } => {
                Self::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
                    android_sparseimg: PipelineSourceAndroidSparseImg {
                        index: index.map(PipelineAndroidSparseIndex::from),
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

impl From<PipelineAndroidSparseIndex> for PipelineAndroidSparseIndexBin {
    fn from(index: PipelineAndroidSparseIndex) -> Self {
        Self {
            file_hdr_sz: index.file_hdr_sz,
            chunk_hdr_sz: index.chunk_hdr_sz,
            blk_sz: index.blk_sz,
            total_blks: index.total_blks,
            total_chunks: index.total_chunks,
            image_checksum: index.image_checksum,
            chunks: index
                .chunks
                .into_iter()
                .map(PipelineAndroidSparseChunkIndexBin::from)
                .collect(),
        }
    }
}

impl From<PipelineAndroidSparseIndexBin> for PipelineAndroidSparseIndex {
    fn from(index: PipelineAndroidSparseIndexBin) -> Self {
        Self {
            file_hdr_sz: index.file_hdr_sz,
            chunk_hdr_sz: index.chunk_hdr_sz,
            blk_sz: index.blk_sz,
            total_blks: index.total_blks,
            total_chunks: index.total_chunks,
            image_checksum: index.image_checksum,
            chunks: index
                .chunks
                .into_vec()
                .into_iter()
                .map(PipelineAndroidSparseChunkIndex::from)
                .collect(),
        }
    }
}

impl From<PipelineAndroidSparseChunkIndex> for PipelineAndroidSparseChunkIndexBin {
    fn from(chunk: PipelineAndroidSparseChunkIndex) -> Self {
        Self {
            chunk_index: chunk.chunk_index,
            chunk_type: chunk.chunk_type,
            chunk_sz: chunk.chunk_sz,
            total_sz: chunk.total_sz,
            chunk_offset: chunk.chunk_offset,
            payload_offset: chunk.payload_offset,
            payload_size: chunk.payload_size,
            output_start: chunk.output_start,
            output_end: chunk.output_end,
            fill_pattern: chunk.fill_pattern,
            crc32: chunk.crc32,
        }
    }
}

impl From<PipelineAndroidSparseChunkIndexBin> for PipelineAndroidSparseChunkIndex {
    fn from(chunk: PipelineAndroidSparseChunkIndexBin) -> Self {
        Self {
            chunk_index: chunk.chunk_index,
            chunk_type: chunk.chunk_type,
            chunk_sz: chunk.chunk_sz,
            total_sz: chunk.total_sz,
            chunk_offset: chunk.chunk_offset,
            payload_offset: chunk.payload_offset,
            payload_size: chunk.payload_size,
            output_start: chunk.output_start,
            output_end: chunk.output_end,
            fill_pattern: chunk.fill_pattern,
            crc32: chunk.crc32,
        }
    }
}
