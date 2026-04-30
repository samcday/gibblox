use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use gibblox_android_sparse::{AndroidSparseBlockReader, AndroidSparseImageIndex};
use gibblox_core::{AlignedByteReader, BlockReader, ByteReader};
use gibblox_pipeline::{
    OpenPipelineOptions, PipelineAndroidSparseChunkIndexHint, PipelineAndroidSparseIndexHint,
    PipelineCachePolicy, PipelineHint, PipelineHintEntry, PipelineHints, PipelineSource,
    PipelineTarEntryIndexHint, open_pipeline, pipeline_identity_string,
};
use gibblox_tar::{TarEntryByteReader, TarEntryByteReaderConfig, TarEntryIndex};
use tracing::info;

#[derive(Clone, Debug)]
pub struct PipelineOptimizeOptions {
    pub image_block_size: u32,
    pub casync_cache_dir: Option<PathBuf>,
    pub cache_policy: PipelineCachePolicy,
}

impl Default for PipelineOptimizeOptions {
    fn default() -> Self {
        Self {
            image_block_size: 512,
            casync_cache_dir: None,
            cache_policy: PipelineCachePolicy::None,
        }
    }
}

pub async fn optimize_pipeline_hints(
    source: &PipelineSource,
    opts: &PipelineOptimizeOptions,
) -> Result<PipelineHints> {
    let mut optimizer = PipelineHintOptimizer {
        entries: BTreeMap::new(),
        visited: BTreeSet::new(),
        opts,
    };
    optimizer.collect(source).await?;
    Ok(PipelineHints {
        entries: optimizer.entries.into_values().collect(),
    })
}

struct PipelineHintOptimizer<'a> {
    entries: BTreeMap<String, PipelineHintEntry>,
    visited: BTreeSet<String>,
    opts: &'a PipelineOptimizeOptions,
}

impl PipelineHintOptimizer<'_> {
    fn collect<'a>(
        &'a mut self,
        source: &'a PipelineSource,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            match source {
                PipelineSource::Http(_) | PipelineSource::File(_) | PipelineSource::Casync(_) => {}
                PipelineSource::Xz(source) => {
                    self.collect(source.xz.as_ref()).await?;
                }
                PipelineSource::AndroidSparseImg(source) => {
                    self.collect(source.android_sparseimg.source.as_ref())
                        .await?;
                    let stage = PipelineSource::AndroidSparseImg(source.clone());
                    let identity = pipeline_identity_string(&stage);
                    if self.visited.insert(identity.clone()) {
                        info!(pipeline_identity = %identity, "materializing android sparse pipeline hint");
                        let upstream = self
                            .open_block_source(source.android_sparseimg.source.as_ref())
                            .await
                            .with_context(|| {
                                format!("open android sparse upstream for {identity}")
                            })?;
                        let reader = AndroidSparseBlockReader::new(upstream)
                            .await
                            .map_err(|err| anyhow!("open android sparse reader: {err}"))?;
                        let index = reader
                            .materialize_index()
                            .await
                            .map_err(|err| anyhow!("materialize android sparse index: {err}"))?;
                        insert_pipeline_hint(
                            &mut self.entries,
                            identity,
                            PipelineHint::AndroidSparseIndex(android_sparse_hint_from_index(index)),
                        );
                    }
                }
                PipelineSource::Tar(source) => {
                    self.collect(source.tar.source.as_ref()).await?;
                    let stage = PipelineSource::Tar(source.clone());
                    let identity = pipeline_identity_string(&stage);
                    if self.visited.insert(identity.clone()) {
                        info!(pipeline_identity = %identity, "materializing tar entry pipeline hint");
                        let upstream = self
                            .open_byte_source(source.tar.source.as_ref())
                            .await
                            .with_context(|| format!("open tar upstream for {identity}"))?;
                        let config = TarEntryByteReaderConfig::new(source.tar.entry.as_str())?
                            .with_source_identity(pipeline_identity_string(
                                source.tar.source.as_ref(),
                            ));
                        let reader = TarEntryByteReader::open_with_config(upstream, config)
                            .await
                            .map_err(|err| anyhow!("open tar entry reader: {err}"))?;
                        insert_pipeline_hint(
                            &mut self.entries,
                            identity,
                            PipelineHint::TarEntryIndex(tar_hint_from_index(
                                reader.entry_index().clone(),
                            )),
                        );
                    }
                }
                PipelineSource::Mbr(source) => {
                    self.collect(source.mbr.source.as_ref()).await?;
                }
                PipelineSource::Gpt(source) => {
                    self.collect(source.gpt.source.as_ref()).await?;
                }
            }
            Ok(())
        })
    }

    async fn open_block_source(&self, source: &PipelineSource) -> Result<Arc<dyn BlockReader>> {
        open_pipeline(source, &self.open_options()).await
    }

    async fn open_byte_source(&self, source: &PipelineSource) -> Result<Arc<dyn ByteReader>> {
        let block_reader = self.open_block_source(source).await?;
        let byte_reader = AlignedByteReader::new(block_reader)
            .await
            .map_err(|err| anyhow!("open aligned byte view: {err}"))?;
        Ok(Arc::new(byte_reader) as Arc<dyn ByteReader>)
    }

    fn open_options(&self) -> OpenPipelineOptions {
        OpenPipelineOptions {
            image_block_size: self.opts.image_block_size,
            casync_cache_dir: self.opts.casync_cache_dir.clone(),
            cache_policy: self.opts.cache_policy,
            pipeline_hints: None,
        }
    }
}

fn insert_pipeline_hint(
    entries: &mut BTreeMap<String, PipelineHintEntry>,
    pipeline_identity: String,
    hint: PipelineHint,
) {
    let entry = entries
        .entry(pipeline_identity.clone())
        .or_insert_with(|| PipelineHintEntry {
            pipeline_identity,
            hints: Vec::new(),
        });
    if !entry
        .hints
        .iter()
        .any(|existing| hint_discriminant(existing) == hint_discriminant(&hint))
    {
        entry.hints.push(hint);
    }
}

fn hint_discriminant(hint: &PipelineHint) -> &'static str {
    match hint {
        PipelineHint::AndroidSparseIndex(_) => "android-sparse-index",
        PipelineHint::TarEntryIndex(_) => "tar-entry-index",
        PipelineHint::ContentDigest(_) => "content-digest",
    }
}

fn android_sparse_hint_from_index(
    index: AndroidSparseImageIndex,
) -> PipelineAndroidSparseIndexHint {
    PipelineAndroidSparseIndexHint {
        file_hdr_sz: index.file_hdr_sz,
        chunk_hdr_sz: index.chunk_hdr_sz,
        blk_sz: index.blk_sz,
        total_blks: index.total_blks,
        total_chunks: index.total_chunks,
        image_checksum: index.image_checksum,
        chunks: index
            .chunks
            .into_iter()
            .map(|chunk| PipelineAndroidSparseChunkIndexHint {
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
    }
}

fn tar_hint_from_index(index: TarEntryIndex) -> PipelineTarEntryIndexHint {
    PipelineTarEntryIndexHint {
        entry_name: index.entry_name,
        header_offset: index.header_offset,
        data_offset: index.data_offset,
        size_bytes: index.size_bytes,
        entry_type: index.entry_type,
    }
}
