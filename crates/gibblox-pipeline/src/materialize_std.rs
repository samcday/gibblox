use core::{future::Future, pin::Pin};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use gibblox_android_sparse::AndroidSparseBlockReader;
use gibblox_cache::CachedBlockReader;
use gibblox_cache_store_std::StdCacheOps;
use gibblox_casync::{CasyncBlockReader, CasyncReaderConfig};
use gibblox_casync_std::{
    StdCasyncChunkStore, StdCasyncChunkStoreConfig, StdCasyncChunkStoreLocator,
    StdCasyncIndexLocator, StdCasyncIndexSource,
};
use gibblox_core::{
    AlignedByteReader, BlockByteReader, BlockReader, ByteReader, GptBlockReader,
    GptBlockReaderConfig, GptPartitionSelector,
};
use gibblox_file::FileReader;
use gibblox_http::{HttpReader, HttpReaderConfig};
use gibblox_mbr::{MbrBlockReader, MbrBlockReaderConfig, MbrPartitionSelector};
use gibblox_tar::TarEntryByteReader;
use gibblox_xz::XzBlockReader;
use tracing::warn;
use url::Url;

use crate::materialize_common::derive_casync_chunk_store_url;
use crate::{
    PipelineCachePolicy, PipelineSource, PipelineSourceCasyncSource, pipeline_identity_string,
};

pub type DynBlockReader = Arc<dyn BlockReader>;
type DynByteReader = Arc<dyn ByteReader>;

#[derive(Clone, Debug)]
pub struct OpenPipelineOptions {
    pub image_block_size: u32,
    pub casync_cache_dir: Option<PathBuf>,
    pub cache_policy: PipelineCachePolicy,
}

impl Default for OpenPipelineOptions {
    fn default() -> Self {
        Self {
            image_block_size: 512,
            casync_cache_dir: Some(default_casync_cache_dir()),
            cache_policy: PipelineCachePolicy::None,
        }
    }
}

pub async fn open_pipeline(
    source: &PipelineSource,
    opts: &OpenPipelineOptions,
) -> Result<DynBlockReader> {
    let reader = open_pipeline_source(source, opts).await?;
    maybe_wrap_tail_cache(reader, opts).await
}

pub(crate) fn open_pipeline_source<'a>(
    source: &'a PipelineSource,
    opts: &'a OpenPipelineOptions,
) -> Pin<Box<dyn Future<Output = Result<DynBlockReader>> + 'a>> {
    Box::pin(async move {
        match source {
            PipelineSource::Http(source) => {
                let value = source.http.trim();
                if value.is_empty() {
                    bail!("pipeline http source is empty");
                }

                let url =
                    Url::parse(value).with_context(|| format!("parse HTTP source URL {value}"))?;
                let config = HttpReaderConfig::new(url.clone(), opts.image_block_size)
                    .with_cors_safelisted_mode(source.cors_safelisted_mode);
                let reader = HttpReader::open(config)
                    .await
                    .map_err(|err| anyhow!("open HTTP source {url}: {err}"))?;
                let reader = BlockByteReader::new(reader, opts.image_block_size)
                    .map_err(|err| anyhow!("open HTTP block view {url}: {err}"))?;
                let reader: DynBlockReader = Arc::new(reader);
                maybe_wrap_head_cache(reader, opts, "http").await
            }
            PipelineSource::File(source) => {
                let value = source.file.trim();
                if value.is_empty() {
                    bail!("pipeline file source is empty");
                }

                let reader = FileReader::open(value, opts.image_block_size)
                    .map_err(|err| anyhow!("open file source {value}: {err}"))?;
                let reader: DynBlockReader = Arc::new(reader);
                maybe_wrap_head_cache(reader, opts, "file").await
            }
            PipelineSource::Casync(source) => {
                let reader = open_casync_source(&source.casync, opts).await?;
                maybe_wrap_head_cache(reader, opts, "casync").await
            }
            PipelineSource::Xz(source) => {
                let upstream = open_pipeline_byte_source(source.xz.as_ref(), opts).await?;
                let xz_reader = XzBlockReader::new_from_byte_reader(upstream)
                    .await
                    .map_err(|err| anyhow!("open xz reader: {err}"))?;
                let reader = BlockByteReader::new(xz_reader, opts.image_block_size)
                    .map_err(|err| anyhow!("open xz block view: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::Tar(source) => {
                let upstream = open_pipeline_byte_source(source.tar.source.as_ref(), opts).await?;
                let tar_reader = TarEntryByteReader::new(source.tar.entry.as_str(), upstream)
                    .await
                    .map_err(|err| anyhow!("open tar entry reader: {err}"))?;
                let reader = BlockByteReader::new(tar_reader, opts.image_block_size)
                    .map_err(|err| anyhow!("open tar entry block view: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::AndroidSparseImg(source) => {
                let upstream =
                    open_pipeline_source(source.android_sparseimg.source.as_ref(), opts).await?;
                let reader = AndroidSparseBlockReader::new(upstream)
                    .await
                    .map_err(|err| anyhow!("open android sparse reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::Mbr(source) => {
                let selector = if let Some(partuuid) = source.mbr.partuuid.as_deref() {
                    MbrPartitionSelector::part_uuid(partuuid)
                } else if let Some(index) = source.mbr.index {
                    MbrPartitionSelector::index(index)
                } else {
                    bail!("pipeline mbr source missing selector")
                };

                let upstream = open_pipeline_source(source.mbr.source.as_ref(), opts).await?;
                let mut config = MbrBlockReaderConfig::new(selector, opts.image_block_size);
                if let Some(lba_size) = source.mbr.lba_size {
                    config = config.with_source_lba_size(lba_size);
                }
                let reader = MbrBlockReader::open_with_config(upstream, config)
                    .await
                    .map_err(|err| anyhow!("open mbr reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::Gpt(source) => {
                let selector = if let Some(partlabel) = source.gpt.partlabel.as_deref() {
                    GptPartitionSelector::part_label(partlabel)
                } else if let Some(partuuid) = source.gpt.partuuid.as_deref() {
                    GptPartitionSelector::part_uuid(partuuid)
                } else if let Some(index) = source.gpt.index {
                    GptPartitionSelector::index(index)
                } else {
                    bail!("pipeline gpt source missing selector")
                };

                let upstream = open_pipeline_source(source.gpt.source.as_ref(), opts).await?;
                let mut config = GptBlockReaderConfig::new(selector, opts.image_block_size);
                if let Some(lba_size) = source.gpt.lba_size {
                    config = config.with_source_lba_size(lba_size);
                }
                let reader = GptBlockReader::open_with_config(upstream, config)
                    .await
                    .map_err(|err| anyhow!("open gpt reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
        }
    })
}

fn open_pipeline_byte_source<'a>(
    source: &'a PipelineSource,
    opts: &'a OpenPipelineOptions,
) -> Pin<Box<dyn Future<Output = Result<DynByteReader>> + 'a>> {
    Box::pin(async move {
        match source {
            PipelineSource::Http(source) => {
                let value = source.http.trim();
                if value.is_empty() {
                    bail!("pipeline http source is empty");
                }

                let url =
                    Url::parse(value).with_context(|| format!("parse HTTP source URL {value}"))?;
                let config = HttpReaderConfig::new(url.clone(), opts.image_block_size)
                    .with_cors_safelisted_mode(source.cors_safelisted_mode);
                let reader = HttpReader::open(config)
                    .await
                    .map_err(|err| anyhow!("open HTTP source {url}: {err}"))?;

                if opts.cache_policy == PipelineCachePolicy::Head {
                    let block_reader = BlockByteReader::new(reader, opts.image_block_size)
                        .map_err(|err| anyhow!("open HTTP block view {url}: {err}"))?;
                    let block_reader: DynBlockReader = Arc::new(block_reader);
                    let block_reader =
                        maybe_wrap_head_cache(block_reader, opts, "http-byte").await?;
                    let byte_reader =
                        AlignedByteReader::new(block_reader).await.map_err(|err| {
                            anyhow!("open aligned byte view for HTTP source {url}: {err}")
                        })?;
                    return Ok(Arc::new(byte_reader) as DynByteReader);
                }

                Ok(Arc::new(reader) as DynByteReader)
            }
            PipelineSource::File(source) => {
                let value = source.file.trim();
                if value.is_empty() {
                    bail!("pipeline file source is empty");
                }

                let reader = FileReader::open(value, opts.image_block_size)
                    .map_err(|err| anyhow!("open file source {value}: {err}"))?;

                if opts.cache_policy == PipelineCachePolicy::Head {
                    let block_reader: DynBlockReader = Arc::new(reader);
                    let block_reader =
                        maybe_wrap_head_cache(block_reader, opts, "file-byte").await?;
                    let byte_reader =
                        AlignedByteReader::new(block_reader).await.map_err(|err| {
                            anyhow!("open aligned byte view for file source {value}: {err}")
                        })?;
                    return Ok(Arc::new(byte_reader) as DynByteReader);
                }

                Ok(Arc::new(reader) as DynByteReader)
            }
            PipelineSource::Xz(source) => {
                let upstream = open_pipeline_byte_source(source.xz.as_ref(), opts).await?;
                let reader = XzBlockReader::new_from_byte_reader(upstream)
                    .await
                    .map_err(|err| anyhow!("open xz byte reader: {err}"))?;
                Ok(Arc::new(reader) as DynByteReader)
            }
            PipelineSource::Tar(source) => {
                let upstream = open_pipeline_byte_source(source.tar.source.as_ref(), opts).await?;
                let reader = TarEntryByteReader::new(source.tar.entry.as_str(), upstream)
                    .await
                    .map_err(|err| anyhow!("open tar entry byte reader: {err}"))?;
                Ok(Arc::new(reader) as DynByteReader)
            }
            _ => {
                let upstream = open_pipeline_source(source, opts).await?;
                let reader = AlignedByteReader::new(upstream)
                    .await
                    .map_err(|err| anyhow!("open aligned byte view: {err}"))?;
                Ok(Arc::new(reader) as DynByteReader)
            }
        }
    })
}

async fn maybe_wrap_head_cache(
    reader: DynBlockReader,
    opts: &OpenPipelineOptions,
    stage: &str,
) -> Result<DynBlockReader> {
    if opts.cache_policy != PipelineCachePolicy::Head {
        return Ok(reader);
    }

    let cache = match StdCacheOps::open_default_for_reader(&reader).await {
        Ok(cache) => cache,
        Err(err) => {
            warn!(
                stage,
                error = %err,
                "failed to open std cache for head stage, using uncached reader"
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

async fn maybe_wrap_tail_cache(
    reader: DynBlockReader,
    opts: &OpenPipelineOptions,
) -> Result<DynBlockReader> {
    if opts.cache_policy != PipelineCachePolicy::Tail {
        return Ok(reader);
    }

    let cache = match StdCacheOps::open_default_for_reader(&reader).await {
        Ok(cache) => cache,
        Err(err) => {
            warn!(error = %err, "failed to open std cache for pipeline tail, using uncached reader");
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

async fn open_casync_source(
    source: &crate::PipelineSourceCasync,
    opts: &OpenPipelineOptions,
) -> Result<DynBlockReader> {
    let index = source.index.trim();
    if index.is_empty() {
        bail!("pipeline casync.index source is empty");
    }

    let index_locator = parse_casync_index_locator(index)?;
    let chunk_store_locator =
        resolve_casync_chunk_store_locator(source.chunk_store.as_deref(), &index_locator)?;

    let index_source = StdCasyncIndexSource::new(index_locator)
        .map_err(|err| anyhow!("open casync index source {index}: {err}"))?;

    let mut chunk_store_config = StdCasyncChunkStoreConfig::new(chunk_store_locator);
    chunk_store_config.cache_dir = opts.casync_cache_dir.clone();
    let chunk_store = StdCasyncChunkStore::new(chunk_store_config)
        .map_err(|err| anyhow!("build casync chunk store: {err}"))?;

    let reader = CasyncBlockReader::open(
        index_source,
        chunk_store,
        CasyncReaderConfig {
            block_size: opts.image_block_size,
            strict_verify: false,
            identity: Some(pipeline_identity_string(&PipelineSource::Casync(
                PipelineSourceCasyncSource {
                    casync: source.clone(),
                },
            ))),
        },
    )
    .await
    .map_err(|err| anyhow!("open casync reader {index}: {err}"))?;

    Ok(Arc::new(reader) as DynBlockReader)
}

fn parse_casync_index_locator(index: &str) -> Result<StdCasyncIndexLocator> {
    if let Ok(url) = Url::parse(index) {
        if matches!(url.scheme(), "http" | "https") {
            return Ok(StdCasyncIndexLocator::url(url));
        }
        if url.scheme() == "file" {
            let path = url_to_local_path(&url)
                .with_context(|| format!("parse file URL index source {index}"))?;
            return Ok(StdCasyncIndexLocator::path(path));
        }
    }

    Ok(StdCasyncIndexLocator::path(PathBuf::from(index)))
}

fn resolve_casync_chunk_store_locator(
    chunk_store: Option<&str>,
    index_locator: &StdCasyncIndexLocator,
) -> Result<StdCasyncChunkStoreLocator> {
    if let Some(chunk_store) = chunk_store {
        let chunk_store = chunk_store.trim();
        if chunk_store.is_empty() {
            bail!("pipeline casync.chunk_store source is empty");
        }
        return parse_casync_chunk_store_locator(chunk_store);
    }

    match index_locator {
        StdCasyncIndexLocator::Url(url) => {
            let chunk_url = derive_casync_chunk_store_url(url)
                .with_context(|| format!("derive casync chunk store URL from {url}"))?;
            StdCasyncChunkStoreLocator::url_prefix(chunk_url)
                .map_err(|err| anyhow!("configure casync chunk store URL: {err}"))
        }
        StdCasyncIndexLocator::Path(path) => Ok(StdCasyncChunkStoreLocator::path_prefix(
            derive_casync_chunk_store_path(path),
        )),
    }
}

fn parse_casync_chunk_store_locator(value: &str) -> Result<StdCasyncChunkStoreLocator> {
    if let Ok(url) = Url::parse(value) {
        if matches!(url.scheme(), "http" | "https") {
            return StdCasyncChunkStoreLocator::url_prefix(url)
                .map_err(|err| anyhow!("configure casync chunk store URL {value}: {err}"));
        }
        if url.scheme() == "file" {
            let path = url_to_local_path(&url)
                .with_context(|| format!("parse file URL chunk store {value}"))?;
            return Ok(StdCasyncChunkStoreLocator::path_prefix(path));
        }
    }

    Ok(StdCasyncChunkStoreLocator::path_prefix(PathBuf::from(
        value,
    )))
}

fn url_to_local_path(url: &Url) -> Result<PathBuf> {
    url.to_file_path()
        .map_err(|_| anyhow!("URL is not a valid local file path: {url}"))
}

fn derive_casync_chunk_store_path(index_path: &Path) -> PathBuf {
    if let Some(parent) = index_path.parent() {
        if parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "indexes")
        {
            if let Some(grandparent) = parent.parent() {
                return grandparent.join("chunks");
            }
            return PathBuf::from("chunks");
        }
        return parent.to_path_buf();
    }

    PathBuf::from(".")
}

fn default_casync_cache_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("XDG_CACHE_HOME") {
        if !path.is_empty() {
            return PathBuf::from(path).join("gibblox").join("casync");
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(path) = std::env::var_os("LOCALAPPDATA") {
            if !path.is_empty() {
                return PathBuf::from(path).join("gibblox").join("casync");
            }
        }
    }

    if let Some(path) = std::env::var_os("HOME") {
        if !path.is_empty() {
            return PathBuf::from(path)
                .join(".cache")
                .join("gibblox")
                .join("casync");
        }
    }

    std::env::temp_dir().join("gibblox").join("casync")
}

#[cfg(test)]
mod tests {
    use super::{parse_casync_chunk_store_locator, parse_casync_index_locator};
    use gibblox_casync_std::{StdCasyncChunkStoreLocator, StdCasyncIndexLocator};

    #[test]
    fn parse_file_index_url_as_path_locator() {
        let locator = parse_casync_index_locator("file:///tmp/indexes/rootfs.caibx")
            .expect("parse file URL index locator");
        match locator {
            StdCasyncIndexLocator::Path(path) => {
                assert!(path.ends_with("indexes/rootfs.caibx"));
            }
            StdCasyncIndexLocator::Url(url) => panic!("expected path locator, got URL {url}"),
        }
    }

    #[test]
    fn parse_file_chunk_store_url_as_path_locator() {
        let locator = parse_casync_chunk_store_locator("file:///tmp/chunks/")
            .expect("parse file URL chunk store locator");
        match locator {
            StdCasyncChunkStoreLocator::PathPrefix(path) => {
                assert!(path.ends_with("chunks"));
            }
            StdCasyncChunkStoreLocator::UrlPrefix(url) => {
                panic!("expected path-prefix locator, got URL {url}")
            }
        }
    }
}
