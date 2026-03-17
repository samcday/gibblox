use alloc::string::String;
use core::{future::Future, pin::Pin};
use std::sync::Arc;

use gibblox_android_sparse::{AndroidSparseBlockReader, AndroidSparseBlockReaderConfig};
use gibblox_cache::CachedBlockReader;
use gibblox_cache_store_opfs::OpfsCacheOps;
use gibblox_casync::{CasyncBlockReader, CasyncReaderConfig};
use gibblox_casync_web::{WebCasyncChunkStore, WebCasyncChunkStoreConfig, WebCasyncIndexSource};
use gibblox_core::{
    AlignedByteReader, BlockByteReader, BlockReader, ByteReader, GibbloxError, GibbloxErrorKind,
    GibbloxResult, GptBlockReader, GptPartitionSelector,
};
use gibblox_http::{HttpReader, HttpReaderConfig};
use gibblox_mbr::{MbrBlockReader, MbrBlockReaderConfig, MbrPartitionSelector};
use gibblox_xz::{XzBlockReader, XzBlockReaderConfig};
use tracing::warn;
use url::Url;

use crate::materialize_common::derive_casync_chunk_store_url;
use crate::{
    PipelineCachePolicy, PipelineSource, PipelineSourceCasyncSource, pipeline_identity_string,
};

type DynBlockReader = Arc<dyn BlockReader>;
type DynByteReader = Arc<dyn ByteReader>;

#[derive(Clone, Debug)]
pub struct OpenPipelineOptions {
    pub image_block_size: u32,
    pub cache_policy: Option<PipelineCachePolicy>,
    pub cache_http_sources: bool,
}

impl Default for OpenPipelineOptions {
    fn default() -> Self {
        Self {
            image_block_size: 512,
            cache_policy: None,
            cache_http_sources: true,
        }
    }
}

pub async fn open_pipeline(
    source: &PipelineSource,
    opts: &OpenPipelineOptions,
) -> GibbloxResult<DynBlockReader> {
    let reader = resolve_pipeline_source(source, opts).await?;
    maybe_wrap_tail_cache(reader, opts).await
}

fn resolve_pipeline_source<'a>(
    pipeline_source: &'a PipelineSource,
    opts: &'a OpenPipelineOptions,
) -> Pin<Box<dyn Future<Output = GibbloxResult<DynBlockReader>> + 'a>> {
    Box::pin(async move {
        match pipeline_source {
            PipelineSource::Http(source) => resolve_http_source(source, opts).await,
            PipelineSource::File(source) => Err(GibbloxError::with_message(
                GibbloxErrorKind::Unsupported,
                format!(
                    "pipeline file source is unsupported in web worker runtime: {}",
                    source.file
                ),
            )),
            PipelineSource::Casync(source) => {
                resolve_casync_source(source, source_identity(pipeline_source), opts).await
            }
            PipelineSource::Xz(source) => {
                let upstream = resolve_pipeline_byte_source(source.xz.as_ref(), opts).await?;
                let config = XzBlockReaderConfig::default()
                    .with_source_identity(source_identity(source.xz.as_ref()));
                let reader = XzBlockReader::open_with_byte_reader_config(upstream, config).await?;
                let reader = BlockByteReader::new(reader, opts.image_block_size)?;
                let reader: DynBlockReader = Arc::new(reader);
                Ok(reader)
            }
            PipelineSource::AndroidSparseImg(source) => {
                let upstream =
                    resolve_pipeline_source(source.android_sparseimg.source.as_ref(), opts).await?;
                let config = AndroidSparseBlockReaderConfig::default().with_source_identity(
                    source_identity(source.android_sparseimg.source.as_ref()),
                );
                let reader = AndroidSparseBlockReader::new_with_config(upstream, config).await?;
                let reader: Arc<dyn BlockReader> = Arc::new(reader);
                Ok(reader)
            }
            PipelineSource::Mbr(source) => {
                let upstream = resolve_pipeline_source(source.mbr.source.as_ref(), opts).await?;
                let selector = mbr_selector(source)?;
                let config = MbrBlockReaderConfig::new(selector, upstream.block_size())
                    .with_source_identity(source_identity(source.mbr.source.as_ref()));
                let reader = MbrBlockReader::open_with_config(upstream, config).await?;
                let reader: Arc<dyn BlockReader> = Arc::new(reader);
                Ok(reader)
            }
            PipelineSource::Gpt(source) => {
                let upstream = resolve_pipeline_source(source.gpt.source.as_ref(), opts).await?;
                let selector = gpt_selector(source)?;
                let block_size = upstream.block_size();
                let reader = GptBlockReader::new(upstream, selector, block_size).await?;
                let reader: Arc<dyn BlockReader> = Arc::new(reader);
                Ok(reader)
            }
        }
    })
}

fn resolve_pipeline_byte_source<'a>(
    pipeline_source: &'a PipelineSource,
    opts: &'a OpenPipelineOptions,
) -> Pin<Box<dyn Future<Output = GibbloxResult<DynByteReader>> + 'a>> {
    Box::pin(async move {
        match pipeline_source {
            PipelineSource::Http(source) => {
                let url = parse_url(source.http.as_str(), "pipeline http source")?;
                let config = HttpReaderConfig::new(url, opts.image_block_size);
                let reader = HttpReader::open(config).await?;

                if should_cache_head_stage(opts, true) {
                    let block_reader = BlockByteReader::new(reader, opts.image_block_size)?;
                    let block_reader: DynBlockReader = Arc::new(block_reader);
                    let block_reader =
                        maybe_wrap_head_cache(block_reader, opts, true, "http-byte").await?;
                    let byte_reader = AlignedByteReader::new(block_reader).await?;
                    return Ok(Arc::new(byte_reader) as DynByteReader);
                }

                Ok(Arc::new(reader) as DynByteReader)
            }
            PipelineSource::Xz(source) => {
                let upstream = resolve_pipeline_byte_source(source.xz.as_ref(), opts).await?;
                let config = XzBlockReaderConfig::default()
                    .with_source_identity(source_identity(source.xz.as_ref()));
                let reader = XzBlockReader::open_with_byte_reader_config(upstream, config).await?;
                Ok(Arc::new(reader) as DynByteReader)
            }
            _ => {
                let upstream = resolve_pipeline_source(pipeline_source, opts).await?;
                let reader = AlignedByteReader::new(upstream).await?;
                Ok(Arc::new(reader) as DynByteReader)
            }
        }
    })
}

async fn resolve_http_source(
    source: &crate::PipelineSourceHttpSource,
    opts: &OpenPipelineOptions,
) -> GibbloxResult<DynBlockReader> {
    let url = parse_url(source.http.as_str(), "pipeline http source")?;
    let config = HttpReaderConfig::new(url, opts.image_block_size);
    let reader = BlockByteReader::new(HttpReader::open(config).await?, opts.image_block_size)?;
    let reader: Arc<dyn BlockReader> = Arc::new(reader);
    maybe_wrap_head_cache(reader, opts, true, "http").await
}

async fn resolve_casync_source(
    source: &PipelineSourceCasyncSource,
    identity: String,
    opts: &OpenPipelineOptions,
) -> GibbloxResult<Arc<dyn BlockReader>> {
    let index_url = parse_url(source.casync.index.as_str(), "pipeline casync.index")?;
    let chunk_store_url = match source.casync.chunk_store.as_deref() {
        Some(chunk_store) => parse_url(chunk_store, "pipeline casync.chunk_store")?,
        None => derive_casync_chunk_store_url(&index_url).map_err(|err| {
            GibbloxError::with_message(
                GibbloxErrorKind::InvalidInput,
                format!("derive casync chunk store URL from {index_url}: {err}"),
            )
        })?,
    };

    let chunk_store_config = WebCasyncChunkStoreConfig::new(chunk_store_url)?;
    let chunk_store = WebCasyncChunkStore::new(chunk_store_config).await?;
    let config = CasyncReaderConfig {
        block_size: opts.image_block_size,
        strict_verify: false,
        identity: Some(identity),
    };

    let reader =
        CasyncBlockReader::open(WebCasyncIndexSource::new(index_url), chunk_store, config).await?;
    let reader: DynBlockReader = Arc::new(reader);
    maybe_wrap_head_cache(reader, opts, false, "casync").await
}

fn mbr_selector(source: &crate::PipelineSourceMbrSource) -> GibbloxResult<MbrPartitionSelector> {
    if let Some(partuuid) = source.mbr.partuuid.as_deref() {
        return Ok(MbrPartitionSelector::part_uuid(partuuid.to_string()));
    }
    if let Some(index) = source.mbr.index {
        return Ok(MbrPartitionSelector::index(index));
    }

    Err(GibbloxError::with_message(
        GibbloxErrorKind::InvalidInput,
        "pipeline mbr selector missing",
    ))
}

fn gpt_selector(source: &crate::PipelineSourceGptSource) -> GibbloxResult<GptPartitionSelector> {
    if let Some(partlabel) = source.gpt.partlabel.as_deref() {
        return Ok(GptPartitionSelector::part_label(partlabel.to_string()));
    }
    if let Some(partuuid) = source.gpt.partuuid.as_deref() {
        return Ok(GptPartitionSelector::part_uuid(partuuid.to_string()));
    }
    if let Some(index) = source.gpt.index {
        return Ok(GptPartitionSelector::index(index));
    }

    Err(GibbloxError::with_message(
        GibbloxErrorKind::InvalidInput,
        "pipeline gpt selector missing",
    ))
}

fn source_identity(source: &PipelineSource) -> String {
    pipeline_identity_string(source)
}

fn effective_cache_policy(opts: &OpenPipelineOptions) -> PipelineCachePolicy {
    if let Some(policy) = opts.cache_policy {
        return policy;
    }

    if opts.cache_http_sources {
        PipelineCachePolicy::Head
    } else {
        PipelineCachePolicy::None
    }
}

async fn maybe_wrap_head_cache(
    reader: DynBlockReader,
    opts: &OpenPipelineOptions,
    is_http_source: bool,
    stage: &str,
) -> GibbloxResult<DynBlockReader> {
    if !should_cache_head_stage(opts, is_http_source) {
        return Ok(reader);
    }

    let cache = match OpfsCacheOps::open_for_reader(&reader).await {
        Ok(cache) => cache,
        Err(err) => {
            warn!(
                stage,
                error = %err,
                "failed to open OPFS cache for head stage, using uncached reader"
            );
            return Ok(reader);
        }
    };

    match CachedBlockReader::new(Arc::clone(&reader), cache).await {
        Ok(cached) => Ok(Arc::new(cached)),
        Err(err) => {
            warn!(
                stage,
                error = %err,
                "failed to initialize cached head-stage reader, using uncached reader"
            );
            Ok(reader)
        }
    }
}

fn should_cache_head_stage(opts: &OpenPipelineOptions, is_http_source: bool) -> bool {
    if let Some(policy) = opts.cache_policy {
        return policy == PipelineCachePolicy::Head;
    }

    is_http_source && opts.cache_http_sources
}

async fn maybe_wrap_tail_cache(
    reader: Arc<dyn BlockReader>,
    opts: &OpenPipelineOptions,
) -> GibbloxResult<Arc<dyn BlockReader>> {
    if effective_cache_policy(opts) != PipelineCachePolicy::Tail {
        return Ok(reader);
    }

    let cache = match OpfsCacheOps::open_for_reader(&reader).await {
        Ok(cache) => cache,
        Err(err) => {
            warn!(error = %err, "failed to open OPFS cache for pipeline tail, using uncached reader");
            return Ok(reader);
        }
    };

    match CachedBlockReader::new(Arc::clone(&reader), cache).await {
        Ok(cached) => Ok(Arc::new(cached)),
        Err(err) => {
            warn!(error = %err, "failed to initialize cached pipeline tail reader, using uncached reader");
            Ok(reader)
        }
    }
}

fn parse_url(value: &str, context: &str) -> GibbloxResult<Url> {
    let value = value.trim();
    if value.is_empty() {
        return Err(GibbloxError::with_message(
            GibbloxErrorKind::InvalidInput,
            format!("{context} must not be empty"),
        ));
    }

    Url::parse(value).map_err(|err| {
        GibbloxError::with_message(GibbloxErrorKind::InvalidInput, format!("{context}: {err}"))
    })
}
