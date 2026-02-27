use core::{future::Future, pin::Pin};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use gibblox_android_sparse::AndroidSparseBlockReader;
use gibblox_casync::{CasyncBlockReader, CasyncReaderConfig};
use gibblox_casync_std::{
    StdCasyncChunkStore, StdCasyncChunkStoreConfig, StdCasyncChunkStoreLocator,
    StdCasyncIndexLocator, StdCasyncIndexSource,
};
use gibblox_core::{BlockReader, GptBlockReader, GptPartitionSelector};
use gibblox_file::StdFileBlockReader;
use gibblox_http::HttpBlockReader;
use gibblox_mbr::{MbrBlockReader, MbrPartitionSelector};
use gibblox_xz::XzBlockReader;
use url::Url;

use crate::materialize_common::derive_casync_chunk_store_url;
use crate::{PipelineSource, PipelineSourceCasyncSource, pipeline_identity_string};

pub type DynBlockReader = Arc<dyn BlockReader>;

#[derive(Clone, Debug)]
pub struct OpenPipelineOptions {
    pub image_block_size: u32,
    pub casync_cache_dir: Option<PathBuf>,
}

impl Default for OpenPipelineOptions {
    fn default() -> Self {
        Self {
            image_block_size: 512,
            casync_cache_dir: Some(default_casync_cache_dir()),
        }
    }
}

pub async fn open_pipeline(
    source: &PipelineSource,
    opts: &OpenPipelineOptions,
) -> Result<DynBlockReader> {
    open_pipeline_source(source, opts).await
}

fn open_pipeline_source<'a>(
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
                let reader = HttpBlockReader::new(url.clone(), opts.image_block_size)
                    .await
                    .map_err(|err| anyhow!("open HTTP source {url}: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::File(source) => {
                let value = source.file.trim();
                if value.is_empty() {
                    bail!("pipeline file source is empty");
                }

                let reader = StdFileBlockReader::open(value, opts.image_block_size)
                    .map_err(|err| anyhow!("open file source {value}: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::Casync(source) => open_casync_source(&source.casync, opts).await,
            PipelineSource::Xz(source) => {
                let upstream = open_pipeline_source(source.xz.as_ref(), opts).await?;
                let reader = XzBlockReader::new(upstream)
                    .await
                    .map_err(|err| anyhow!("open xz reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::AndroidSparseImg(source) => {
                let upstream =
                    open_pipeline_source(source.android_sparseimg.as_ref(), opts).await?;
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
                let reader = MbrBlockReader::new(upstream, selector, opts.image_block_size)
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
                let reader = GptBlockReader::new(upstream, selector, opts.image_block_size)
                    .await
                    .map_err(|err| anyhow!("open gpt reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
        }
    })
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
