use core::{future::Future, pin::Pin};
use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use gibblox_android_sparse::AndroidSparseBlockReader;
use tracing::{debug, info};

use crate::{
    OpenPipelineOptions, PipelineAndroidSparseIndex, PipelineSource, pipeline_identity_string,
};

#[derive(Clone, Debug)]
pub struct OptimizePipelineOptions {
    pub force: bool,
    pub image_block_size: u32,
}

impl Default for OptimizePipelineOptions {
    fn default() -> Self {
        Self {
            force: false,
            image_block_size: 4096,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OptimizePipelineReport {
    pub android_sparse_stages_visited: usize,
    pub android_sparse_indexes_added: usize,
    pub android_sparse_indexes_updated: usize,
    pub android_sparse_indexes_skipped: usize,
}

#[derive(Clone, Debug, Default)]
pub struct OptimizePipelineSession {
    android_sparse_index_cache: HashMap<String, PipelineAndroidSparseIndex>,
}

impl OptimizePipelineSession {
    pub fn new() -> Self {
        Self::default()
    }
}

pub async fn optimize_pipeline(
    source: &mut PipelineSource,
    opts: &OptimizePipelineOptions,
) -> Result<OptimizePipelineReport> {
    let mut session = OptimizePipelineSession::new();
    optimize_pipeline_with_session(source, opts, &mut session).await
}

pub async fn optimize_pipeline_with_session(
    source: &mut PipelineSource,
    opts: &OptimizePipelineOptions,
    session: &mut OptimizePipelineSession,
) -> Result<OptimizePipelineReport> {
    let mut report = OptimizePipelineReport::default();
    optimize_pipeline_source(source, opts, session, &mut report).await?;
    Ok(report)
}

fn optimize_pipeline_source<'a>(
    source: &'a mut PipelineSource,
    opts: &'a OptimizePipelineOptions,
    session: &'a mut OptimizePipelineSession,
    report: &'a mut OptimizePipelineReport,
) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
    Box::pin(async move {
        match source {
            PipelineSource::Xz(stage) => {
                optimize_pipeline_source(stage.xz.as_mut(), opts, session, report).await
            }
            PipelineSource::AndroidSparseImg(stage) => {
                optimize_pipeline_source(
                    stage.android_sparseimg.source.as_mut(),
                    opts,
                    session,
                    report,
                )
                .await?;

                report.android_sparse_stages_visited =
                    report.android_sparse_stages_visited.saturating_add(1);
                if stage.android_sparseimg.index.is_some() && !opts.force {
                    report.android_sparse_indexes_skipped =
                        report.android_sparse_indexes_skipped.saturating_add(1);
                    debug!(
                        force = opts.force,
                        "pipeline optimizer skipped android sparse stage with existing index"
                    );
                    return Ok(());
                }

                let cache_key = pipeline_identity_string(stage.android_sparseimg.source.as_ref());
                if !opts.force {
                    if let Some(index) = session.android_sparse_index_cache.get(&cache_key) {
                        let had_index = stage.android_sparseimg.index.is_some();
                        stage.android_sparseimg.index = Some(index.clone());
                        if had_index {
                            report.android_sparse_indexes_updated =
                                report.android_sparse_indexes_updated.saturating_add(1);
                        } else {
                            report.android_sparse_indexes_added =
                                report.android_sparse_indexes_added.saturating_add(1);
                        }

                        debug!(
                            cache_key = cache_key.as_str(),
                            had_index, "pipeline optimizer cache hit for android sparse index"
                        );
                        return Ok(());
                    }

                    debug!(
                        cache_key = cache_key.as_str(),
                        "pipeline optimizer cache miss for android sparse index"
                    );
                } else {
                    debug!(
                        cache_key = cache_key.as_str(),
                        "pipeline optimizer bypassed android sparse cache due to force"
                    );
                }

                let open_opts = OpenPipelineOptions {
                    image_block_size: opts.image_block_size,
                    cache_policy: crate::PipelineCachePolicy::None,
                    ..OpenPipelineOptions::default()
                };
                let upstream = crate::materialize_std::open_pipeline_source(
                    stage.android_sparseimg.source.as_ref(),
                    &open_opts,
                )
                .await
                .context("open android sparse upstream source")?;
                let reader = AndroidSparseBlockReader::new(upstream)
                    .await
                    .map_err(|err| anyhow!("open android sparse reader: {err}"))?;
                let index = reader
                    .materialize_index()
                    .await
                    .map_err(|err| anyhow!("materialize android sparse index: {err}"))?;

                let had_index = stage.android_sparseimg.index.is_some();
                let index: PipelineAndroidSparseIndex = index.into();
                stage.android_sparseimg.index = Some(index.clone());
                session
                    .android_sparse_index_cache
                    .insert(cache_key.clone(), index);
                if had_index {
                    report.android_sparse_indexes_updated =
                        report.android_sparse_indexes_updated.saturating_add(1);
                } else {
                    report.android_sparse_indexes_added =
                        report.android_sparse_indexes_added.saturating_add(1);
                }

                info!(
                    had_index,
                    force = opts.force,
                    "pipeline optimizer materialized android sparse index"
                );
                debug!(
                    cache_key = cache_key.as_str(),
                    "pipeline optimizer cached android sparse index"
                );

                Ok(())
            }
            PipelineSource::Mbr(stage) => {
                optimize_pipeline_source(stage.mbr.source.as_mut(), opts, session, report).await
            }
            PipelineSource::Gpt(stage) => {
                optimize_pipeline_source(stage.gpt.source.as_mut(), opts, session, report).await
            }
            PipelineSource::Casync(_) | PipelineSource::Http(_) | PipelineSource::File(_) => Ok(()),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        OptimizePipelineOptions, OptimizePipelineSession, optimize_pipeline,
        optimize_pipeline_with_session,
    };
    use crate::{
        PipelineSource, PipelineSourceAndroidSparseImg, PipelineSourceAndroidSparseImgSource,
        PipelineSourceFileSource,
    };
    use alloc::{boxed::Box, format};
    use futures::executor::block_on;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    const SPARSE_MAGIC: u32 = 0xED26_FF3A;
    const SPARSE_MAJOR_VERSION: u16 = 1;
    const CHUNK_TYPE_RAW: u16 = 0xCAC1;
    const CHUNK_TYPE_DONT_CARE: u16 = 0xCAC3;

    #[test]
    fn optimize_populates_android_sparse_index() {
        let path = write_temp_sparse_image();

        let mut source = PipelineSource::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
            android_sparseimg: PipelineSourceAndroidSparseImg {
                index: None,
                source: Box::new(PipelineSource::File(PipelineSourceFileSource {
                    file: path.to_string_lossy().to_string(),
                })),
            },
        });

        let report = block_on(optimize_pipeline(
            &mut source,
            &OptimizePipelineOptions::default(),
        ))
        .expect("optimize pipeline");
        assert_eq!(report.android_sparse_stages_visited, 1);
        assert_eq!(report.android_sparse_indexes_added, 1);
        assert_eq!(report.android_sparse_indexes_updated, 0);
        assert_eq!(report.android_sparse_indexes_skipped, 0);

        let PipelineSource::AndroidSparseImg(source) = source else {
            panic!("expected android_sparseimg source")
        };
        let index = source
            .android_sparseimg
            .index
            .expect("optimizer should embed sparse index");
        assert_eq!(index.total_chunks, 2);
        assert_eq!(index.total_blks, 2);
        assert_eq!(index.chunks.len(), 2);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn optimize_skips_existing_index_without_force() {
        let path = write_temp_sparse_image();

        let mut source = PipelineSource::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
            android_sparseimg: PipelineSourceAndroidSparseImg {
                index: None,
                source: Box::new(PipelineSource::File(PipelineSourceFileSource {
                    file: path.to_string_lossy().to_string(),
                })),
            },
        });

        let first = block_on(optimize_pipeline(
            &mut source,
            &OptimizePipelineOptions::default(),
        ))
        .expect("first optimize");
        assert_eq!(first.android_sparse_indexes_added, 1);

        let second = block_on(optimize_pipeline(
            &mut source,
            &OptimizePipelineOptions::default(),
        ))
        .expect("second optimize");
        assert_eq!(second.android_sparse_stages_visited, 1);
        assert_eq!(second.android_sparse_indexes_added, 0);
        assert_eq!(second.android_sparse_indexes_updated, 0);
        assert_eq!(second.android_sparse_indexes_skipped, 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn optimize_reuses_cached_index_across_pipelines_with_shared_session() {
        let path = write_temp_sparse_image();
        let source_path = path.to_string_lossy().to_string();

        let mut first_source = sparse_file_source(source_path.clone());
        let mut session = OptimizePipelineSession::new();
        let first = block_on(optimize_pipeline_with_session(
            &mut first_source,
            &OptimizePipelineOptions::default(),
            &mut session,
        ))
        .expect("first optimize with shared session");
        assert_eq!(first.android_sparse_indexes_added, 1);

        fs::remove_file(&path).expect("remove source after first optimize");

        let mut second_source = sparse_file_source(source_path);
        let second = block_on(optimize_pipeline_with_session(
            &mut second_source,
            &OptimizePipelineOptions::default(),
            &mut session,
        ))
        .expect("second optimize should reuse cached index");
        assert_eq!(second.android_sparse_stages_visited, 1);
        assert_eq!(second.android_sparse_indexes_added, 1);
        assert_eq!(second.android_sparse_indexes_updated, 0);
        assert_eq!(second.android_sparse_indexes_skipped, 0);

        let PipelineSource::AndroidSparseImg(source) = second_source else {
            panic!("expected android_sparseimg source")
        };
        assert!(source.android_sparseimg.index.is_some());
    }

    #[test]
    fn optimize_force_bypasses_cached_index_in_shared_session() {
        let path = write_temp_sparse_image();
        let source_path = path.to_string_lossy().to_string();

        let mut first_source = sparse_file_source(source_path.clone());
        let mut session = OptimizePipelineSession::new();
        block_on(optimize_pipeline_with_session(
            &mut first_source,
            &OptimizePipelineOptions::default(),
            &mut session,
        ))
        .expect("seed session cache");

        fs::remove_file(&path).expect("remove source before force optimize");

        let mut second_source = sparse_file_source(source_path);
        let err = block_on(optimize_pipeline_with_session(
            &mut second_source,
            &OptimizePipelineOptions {
                force: true,
                ..OptimizePipelineOptions::default()
            },
            &mut session,
        ))
        .expect_err("force optimize should bypass cache and fail on missing source");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("open android sparse upstream source"),
            "unexpected error: {err_text}"
        );
    }

    #[test]
    fn optimize_does_not_reuse_cached_index_for_distinct_fragments() {
        let path = write_temp_sparse_image();
        let mut session = OptimizePipelineSession::new();

        let mut first_source = sparse_file_source(path.to_string_lossy().to_string());
        block_on(optimize_pipeline_with_session(
            &mut first_source,
            &OptimizePipelineOptions::default(),
            &mut session,
        ))
        .expect("seed session cache");

        let _ = fs::remove_file(path);

        let mut distinct_source =
            sparse_file_source(String::from("/nonexistent/distinct-image.simg"));
        let err = block_on(optimize_pipeline_with_session(
            &mut distinct_source,
            &OptimizePipelineOptions::default(),
            &mut session,
        ))
        .expect_err("distinct fragment should not reuse cached index");
        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("open android sparse upstream source"),
            "unexpected error: {err_text}"
        );
    }

    fn sparse_file_source(path: String) -> PipelineSource {
        PipelineSource::AndroidSparseImg(PipelineSourceAndroidSparseImgSource {
            android_sparseimg: PipelineSourceAndroidSparseImg {
                index: None,
                source: Box::new(PipelineSource::File(PipelineSourceFileSource {
                    file: path,
                })),
            },
        })
    }

    fn write_temp_sparse_image() -> PathBuf {
        let mut path = std::env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        path.push(format!("gibblox-pipeline-optimize-{nonce}.simg"));
        fs::write(&path, sparse_image_fixture()).expect("write sparse fixture");
        path
    }

    fn sparse_image_fixture() -> Vec<u8> {
        let mut out = Vec::new();
        append_sparse_header(&mut out, 8, 2, 2, 28, 12);
        append_raw_chunk(&mut out, 1, b"ABCDEFGH", 12);
        append_dont_care_chunk(&mut out, 1, 12);
        out
    }

    fn append_sparse_header(
        out: &mut Vec<u8>,
        blk_sz: u32,
        total_blks: u32,
        total_chunks: u32,
        file_hdr_sz: u16,
        chunk_hdr_sz: u16,
    ) {
        out.extend_from_slice(&SPARSE_MAGIC.to_le_bytes());
        out.extend_from_slice(&SPARSE_MAJOR_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&file_hdr_sz.to_le_bytes());
        out.extend_from_slice(&chunk_hdr_sz.to_le_bytes());
        out.extend_from_slice(&blk_sz.to_le_bytes());
        out.extend_from_slice(&total_blks.to_le_bytes());
        out.extend_from_slice(&total_chunks.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.resize(file_hdr_sz as usize, 0);
    }

    fn append_raw_chunk(out: &mut Vec<u8>, blocks: u32, payload: &[u8], chunk_hdr_sz: u16) {
        append_chunk_header(
            out,
            CHUNK_TYPE_RAW,
            blocks,
            (chunk_hdr_sz as u32) + (payload.len() as u32),
            chunk_hdr_sz,
        );
        out.extend_from_slice(payload);
    }

    fn append_dont_care_chunk(out: &mut Vec<u8>, blocks: u32, chunk_hdr_sz: u16) {
        append_chunk_header(
            out,
            CHUNK_TYPE_DONT_CARE,
            blocks,
            chunk_hdr_sz as u32,
            chunk_hdr_sz,
        );
    }

    fn append_chunk_header(
        out: &mut Vec<u8>,
        chunk_type: u16,
        chunk_sz: u32,
        total_sz: u32,
        chunk_hdr_sz: u16,
    ) {
        out.extend_from_slice(&chunk_type.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&chunk_sz.to_le_bytes());
        out.extend_from_slice(&total_sz.to_le_bytes());
        out.resize(out.len() + (chunk_hdr_sz as usize - 12), 0);
    }
}
