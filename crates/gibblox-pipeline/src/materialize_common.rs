use alloc::{string::String, vec::Vec};

use gibblox_android_sparse::{AndroidSparseChunkIndex, AndroidSparseImageIndex};
use gibblox_tar::TarEntryIndex;
use url::Url;

use crate::{PipelineHint, PipelineHints, PipelineSource, pipeline_identity_string};

pub fn derive_casync_chunk_store_url(index_url: &Url) -> Result<Url, url::ParseError> {
    if let Some(segments) = index_url.path_segments() {
        let segments: Vec<&str> = segments.collect();
        if let Some(index_pos) = segments.iter().rposition(|segment| *segment == "indexes") {
            let mut base_segments = segments[..=index_pos].to_vec();
            base_segments[index_pos] = "chunks";

            let mut url = index_url.clone();
            let mut path = String::from("/");
            path.push_str(base_segments.join("/").as_str());
            if !path.ends_with('/') {
                path.push('/');
            }
            url.set_path(path.as_str());
            url.set_query(None);
            url.set_fragment(None);
            return Ok(url);
        }
    }

    index_url.join("./")
}

pub fn android_sparse_index_hint(
    hints: Option<&PipelineHints>,
    stage: &PipelineSource,
) -> Option<AndroidSparseImageIndex> {
    let identity = pipeline_identity_string(stage);
    let entry = hints?
        .entries
        .iter()
        .find(|entry| entry.pipeline_identity == identity)?;
    entry.hints.iter().find_map(|hint| match hint {
        PipelineHint::AndroidSparseIndex(index) => Some(AndroidSparseImageIndex {
            file_hdr_sz: index.file_hdr_sz,
            chunk_hdr_sz: index.chunk_hdr_sz,
            blk_sz: index.blk_sz,
            total_blks: index.total_blks,
            total_chunks: index.total_chunks,
            image_checksum: index.image_checksum,
            chunks: index
                .chunks
                .iter()
                .map(|chunk| AndroidSparseChunkIndex {
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
                })
                .collect(),
        }),
        _ => None,
    })
}

pub fn tar_entry_index_hint(
    hints: Option<&PipelineHints>,
    stage: &PipelineSource,
) -> Option<TarEntryIndex> {
    let identity = pipeline_identity_string(stage);
    let entry = hints?
        .entries
        .iter()
        .find(|entry| entry.pipeline_identity == identity)?;
    entry.hints.iter().find_map(|hint| match hint {
        PipelineHint::TarEntryIndex(index) => Some(TarEntryIndex {
            entry_name: index.entry_name.clone(),
            header_offset: index.header_offset,
            data_offset: index.data_offset,
            size_bytes: index.size_bytes,
            entry_type: index.entry_type,
        }),
        _ => None,
    })
}
