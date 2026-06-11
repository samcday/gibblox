use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use gibblox_android_sparse::{AndroidSparseBlockReader, AndroidSparseImageIndex};
use gibblox_core::{AlignedByteReader, BlockReader, ByteReader, ReadContext};
use gibblox_pipeline::{
    LocalArtifactIndex, OpenPipelineOptions, PipelineAndroidSparseChunkIndexHint,
    PipelineAndroidSparseIndexHint, PipelineCachePolicy, PipelineContentDigestHint, PipelineHint,
    PipelineHintEntry, PipelineHints, PipelineSource, PipelineSourceContent,
    PipelineTarEntryIndexHint, open_pipeline, pipeline_identity_string,
};
use gibblox_tar::{TarEntryByteReader, TarEntryByteReaderConfig, TarEntryIndex};
use sha2::{Digest, Sha512};
use tracing::info;

#[derive(Clone, Debug)]
pub struct PipelineOptimizeOptions {
    pub image_block_size: u32,
    pub casync_cache_dir: Option<PathBuf>,
    pub cache_policy: PipelineCachePolicy,
    pub local_artifacts: Option<LocalArtifactIndex>,
    pub content_digests: PipelineContentDigestOptions,
}

#[derive(Clone, Debug, Default)]
pub struct PipelineContentDigestOptions {
    pub enabled: bool,
    pub materialize: bool,
    pub cache_dir: Option<PathBuf>,
}

impl Default for PipelineOptimizeOptions {
    fn default() -> Self {
        Self {
            image_block_size: 512,
            casync_cache_dir: None,
            cache_policy: PipelineCachePolicy::None,
            local_artifacts: None,
            content_digests: PipelineContentDigestOptions::default(),
        }
    }
}

pub async fn optimize_pipeline_hints(
    source: &PipelineSource,
    opts: &PipelineOptimizeOptions,
) -> Result<PipelineHints> {
    let mut optimizer = PipelineHintOptimizer {
        entries: BTreeMap::new(),
        visited_indexes: BTreeSet::new(),
        visited_content: BTreeSet::new(),
        local_artifacts: opts.local_artifacts.clone().unwrap_or_default(),
        materialized_cache: if opts.content_digests.enabled && opts.content_digests.materialize {
            Some(MaterializedCache::new(
                opts.content_digests.cache_dir.clone(),
            )?)
        } else {
            None
        },
        opts,
    };
    optimizer.collect(source).await?;
    Ok(PipelineHints {
        entries: optimizer.entries.into_values().collect(),
    })
}

struct PipelineHintOptimizer<'a> {
    entries: BTreeMap<String, PipelineHintEntry>,
    visited_indexes: BTreeSet<String>,
    visited_content: BTreeSet<String>,
    local_artifacts: LocalArtifactIndex,
    materialized_cache: Option<MaterializedCache>,
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
                    let stage = PipelineSource::Xz(source.clone());
                    self.collect_content_digest(&stage, source.content.as_ref())
                        .await?;
                }
                PipelineSource::AndroidSparseImg(source) => {
                    self.collect(source.android_sparseimg.source.as_ref())
                        .await?;
                    let stage = PipelineSource::AndroidSparseImg(source.clone());
                    let identity = pipeline_identity_string(&stage);
                    if self.visited_indexes.insert(identity.clone()) {
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
                    self.collect_content_digest(&stage, source.android_sparseimg.content.as_ref())
                        .await?;
                }
                PipelineSource::Tar(source) => {
                    self.collect(source.tar.source.as_ref()).await?;
                    let stage = PipelineSource::Tar(source.clone());
                    let identity = pipeline_identity_string(&stage);
                    if self.visited_indexes.insert(identity.clone()) {
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
                    self.collect_content_digest(&stage, source.tar.content.as_ref())
                        .await?;
                }
                PipelineSource::Mbr(source) => {
                    self.collect(source.mbr.source.as_ref()).await?;
                    let stage = PipelineSource::Mbr(source.clone());
                    self.collect_content_digest(&stage, source.mbr.content.as_ref())
                        .await?;
                }
                PipelineSource::Gpt(source) => {
                    self.collect(source.gpt.source.as_ref()).await?;
                    let stage = PipelineSource::Gpt(source.clone());
                    self.collect_content_digest(&stage, source.gpt.content.as_ref())
                        .await?;
                }
            }
            Ok(())
        })
    }

    async fn collect_content_digest(
        &mut self,
        stage: &PipelineSource,
        declared_content: Option<&PipelineSourceContent>,
    ) -> Result<()> {
        if !self.opts.content_digests.enabled || declared_content.is_some() {
            return Ok(());
        }

        let identity = pipeline_identity_string(stage);
        if !self.visited_content.insert(identity.clone()) {
            return Ok(());
        }

        info!(pipeline_identity = %identity, "materializing pipeline content digest hint");
        let reader = self
            .open_block_source(stage)
            .await
            .with_context(|| format!("open pipeline stage for content digest {identity}"))?;
        let materialized = match self.materialized_cache.as_mut() {
            Some(cache) => {
                let materialized =
                    digest_and_materialize_reader_content(reader, identity.as_str(), cache).await?;
                self.local_artifacts.insert_content_path(
                    PipelineSourceContent {
                        digest: materialized.hint.digest.clone(),
                        size_bytes: materialized.hint.size_bytes,
                    },
                    materialized.path,
                )?;
                materialized.hint
            }
            None => digest_reader_content(reader, identity.as_str()).await?,
        };
        insert_pipeline_hint(
            &mut self.entries,
            identity,
            PipelineHint::ContentDigest(materialized),
        );
        Ok(())
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
            pipeline_hints: self.current_hints(),
            local_artifacts: Some(self.local_artifacts.clone()),
        }
    }

    fn current_hints(&self) -> Option<PipelineHints> {
        if self.entries.is_empty() {
            return None;
        }
        Some(PipelineHints {
            entries: self.entries.values().cloned().collect(),
        })
    }
}

struct MaterializedCache {
    cache_dir: PathBuf,
}

struct MaterializedDigest {
    hint: PipelineContentDigestHint,
    path: PathBuf,
}

impl MaterializedCache {
    fn new(cache_dir: Option<PathBuf>) -> Result<Self> {
        let cache_dir = cache_dir.unwrap_or_else(default_materialized_cache_dir);
        fs::create_dir_all(&cache_dir)
            .with_context(|| format!("create materialized cache dir {}", cache_dir.display()))?;
        Ok(Self { cache_dir })
    }

    fn create_temp_writer(&self) -> Result<(PathBuf, BufWriter<fs::File>)> {
        fs::create_dir_all(&self.cache_dir).with_context(|| {
            format!("create materialized cache dir {}", self.cache_dir.display())
        })?;
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|err| anyhow!("materialized cache clock before unix epoch: {err}"))?
            .as_nanos();
        let temp_path = self
            .cache_dir
            .join(format!(".tmp-{}-{nonce}.part", std::process::id()));
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("create materialized temp file {}", temp_path.display()))?;
        Ok((temp_path, BufWriter::new(file)))
    }

    fn finalize_temp_file(&self, temp_path: &Path, digest: &str) -> Result<PathBuf> {
        let final_path = self.path_for_digest(digest)?;
        if final_path.exists() {
            let _ = fs::remove_file(temp_path);
            return Ok(final_path);
        }
        fs::rename(temp_path, &final_path).with_context(|| {
            format!(
                "move materialized cache file {} -> {}",
                temp_path.display(),
                final_path.display()
            )
        })?;
        Ok(final_path)
    }

    fn path_for_digest(&self, digest: &str) -> Result<PathBuf> {
        Ok(self.cache_dir.join(digest_to_cache_filename(digest)?))
    }
}

async fn digest_reader_content(
    reader: Arc<dyn BlockReader>,
    pipeline_identity: &str,
) -> Result<PipelineContentDigestHint> {
    let (digest, size_bytes) =
        read_digest_and_optionally_materialize(reader, pipeline_identity, None).await?;
    Ok(PipelineContentDigestHint { digest, size_bytes })
}

async fn digest_and_materialize_reader_content(
    reader: Arc<dyn BlockReader>,
    pipeline_identity: &str,
    materialized_cache: &MaterializedCache,
) -> Result<MaterializedDigest> {
    let (temp_path, mut writer) = materialized_cache.create_temp_writer()?;
    let read_result = read_digest_and_optionally_materialize(
        reader,
        pipeline_identity,
        Some((&mut writer, temp_path.as_path())),
    )
    .await;
    let (digest, size_bytes) = match read_result {
        Ok(result) => result,
        Err(err) => {
            drop(writer);
            let _ = fs::remove_file(&temp_path);
            return Err(err);
        }
    };
    if let Err(err) = writer
        .flush()
        .with_context(|| format!("flush materialized bytes to {}", temp_path.display()))
    {
        drop(writer);
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    drop(writer);
    let path = match materialized_cache.finalize_temp_file(temp_path.as_path(), digest.as_str()) {
        Ok(path) => path,
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            return Err(err);
        }
    };
    info!(pipeline_identity, digest, size_bytes, cache_path = %path.display(), "materialized pipeline content");
    Ok(MaterializedDigest {
        hint: PipelineContentDigestHint { digest, size_bytes },
        path,
    })
}

async fn read_digest_and_optionally_materialize(
    reader: Arc<dyn BlockReader>,
    pipeline_identity: &str,
    mut materialize: Option<(&mut BufWriter<fs::File>, &Path)>,
) -> Result<(String, u64)> {
    const DIGEST_CHUNK_TARGET_BYTES: usize = 32 * 1024 * 1024;

    let block_size = reader.block_size();
    if block_size == 0 {
        return Err(anyhow!("reader block size is zero"));
    }
    let block_size_usize = block_size as usize;
    let blocks_per_read = core::cmp::max(1, DIGEST_CHUNK_TARGET_BYTES / block_size_usize);
    let total_blocks = reader.total_blocks().await?;
    info!(
        pipeline_identity,
        total_blocks, block_size, blocks_per_read, "digesting pipeline content"
    );

    let mut hasher = Sha512::new();
    let mut size_bytes = 0u64;
    let mut buf = vec![0u8; blocks_per_read * block_size_usize];
    let mut lba = 0u64;
    while lba < total_blocks {
        let remaining_blocks = total_blocks - lba;
        let requested_blocks = core::cmp::min(remaining_blocks, blocks_per_read as u64);
        let requested_bytes = requested_blocks as usize * block_size_usize;
        let read = reader
            .read_blocks(lba, &mut buf[..requested_bytes], ReadContext::BACKGROUND)
            .await?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        if let Some((writer, temp_path)) = materialize.as_mut() {
            writer
                .write_all(&buf[..read])
                .with_context(|| format!("write materialized bytes to {}", temp_path.display()))?;
        }
        size_bytes = size_bytes
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("digest size overflow"))?;
        let consumed_blocks = (read as u64).div_ceil(block_size as u64);
        if consumed_blocks == 0 {
            break;
        }
        lba = lba
            .checked_add(consumed_blocks)
            .ok_or_else(|| anyhow!("digest lba overflow"))?;
        if read < requested_bytes {
            break;
        }
    }
    Ok((format!("sha512:{:x}", hasher.finalize()), size_bytes))
}

fn digest_to_cache_filename(digest: &str) -> Result<String> {
    let hex = digest
        .strip_prefix("sha512:")
        .ok_or_else(|| anyhow!("expected sha512 digest, got {digest}"))?;
    if hex.len() != 128 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("invalid sha512 digest {digest}"));
    }
    Ok(hex.to_ascii_lowercase())
}

fn default_materialized_cache_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("XDG_CACHE_HOME")
        && !path.is_empty()
    {
        return PathBuf::from(path).join("gibblox").join("materialized");
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(path) = std::env::var_os("LOCALAPPDATA")
            && !path.is_empty()
        {
            return PathBuf::from(path).join("gibblox").join("materialized");
        }
    }

    if let Some(path) = std::env::var_os("HOME")
        && !path.is_empty()
    {
        return PathBuf::from(path)
            .join(".cache")
            .join("gibblox")
            .join("materialized");
    }

    std::env::temp_dir().join("gibblox").join("materialized")
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
