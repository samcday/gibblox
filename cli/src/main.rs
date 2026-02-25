use core::{future::Future, pin::Pin};
use std::{
    fs,
    io::{IsTerminal, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use gibblox_android_sparse::AndroidSparseBlockReader;
use gibblox_casync::{CasyncBlockReader, CasyncReaderConfig};
use gibblox_casync_std::{
    StdCasyncChunkStore, StdCasyncChunkStoreConfig, StdCasyncChunkStoreLocator,
    StdCasyncIndexLocator, StdCasyncIndexSource,
};
use gibblox_core::{BlockReader, GptBlockReader, GptPartitionSelector, ReadContext};
use gibblox_file::StdFileBlockReader;
use gibblox_http::HttpBlockReader;
use gibblox_mbr::{MbrBlockReader, MbrPartitionSelector};
use gibblox_pipeline::{
    PipelineSource, decode_pipeline, encode_pipeline, pipeline_bin_header_version,
    validate_pipeline,
};
use gibblox_xz::XzBlockReader;
use url::Url;

const DEFAULT_IMAGE_BLOCK_SIZE: u32 = 512;
const STREAM_BLOCK_WINDOW: usize = 256;

type DynBlockReader = Arc<dyn BlockReader>;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "gibblox CLI utilities",
    subcommand_precedence_over_arg = true
)]
struct Cli {
    /// Input pipeline document path ("-" for stdin).
    #[arg(value_name = "PIPELINE")]
    pipeline: Option<String>,
    /// Output path for resolved pipeline bytes ("-" for stdout).
    #[arg(short, long, value_name = "OUTPUT", requires = "pipeline")]
    output: Option<String>,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert and validate pipeline definitions.
    Pipeline(PipelineArgs),
}

#[derive(Args)]
struct PipelineArgs {
    #[command(subcommand)]
    command: PipelineCommand,
}

#[derive(Subcommand)]
enum PipelineCommand {
    /// Encode pipeline YAML to binary format.
    Encode(PipelineEncodeArgs),
    /// Decode pipeline binary to YAML.
    Decode(PipelineDecodeArgs),
    /// Validate a pipeline input (YAML by default).
    Validate(PipelineValidateArgs),
}

#[derive(Args)]
struct PipelineEncodeArgs {
    /// Input YAML file.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
    /// Output binary file.
    #[arg(short = 'o', long)]
    output: PathBuf,
}

#[derive(Args)]
struct PipelineDecodeArgs {
    /// Input binary file.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
    /// Output YAML file.
    #[arg(short = 'o', long)]
    output: PathBuf,
}

#[derive(Args)]
struct PipelineValidateArgs {
    /// Input YAML or binary file.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
    /// Parse input as binary pipeline.
    #[arg(long)]
    binary: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.command.is_some() && (cli.pipeline.is_some() || cli.output.is_some()) {
        bail!("PIPELINE and --output cannot be used with subcommands");
    }

    match cli.command {
        Some(Commands::Pipeline(args)) => run_pipeline(args.command),
        None => {
            let input = cli
                .pipeline
                .ok_or_else(|| anyhow!("missing required PIPELINE input"))?;
            let output = cli.output.as_deref().unwrap_or("-");
            run_default_pipeline_execute(&input, output).await
        }
    }
}

async fn run_default_pipeline_execute(input_path: &str, output_path: &str) -> Result<()> {
    validate_binary_output(
        output_path,
        "gibblox pipeline",
        std::io::stdout().is_terminal(),
    )?;

    let input = read_input_bytes(input_path)?;
    let source = parse_pipeline_document(&input, input_path)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline input {}", io_label(input_path)))?;

    let reader = open_pipeline_source(&source).await?;
    write_reader_output(reader.as_ref(), output_path).await
}

fn run_pipeline(command: PipelineCommand) -> Result<()> {
    match command {
        PipelineCommand::Encode(args) => run_pipeline_encode(args),
        PipelineCommand::Decode(args) => run_pipeline_decode(args),
        PipelineCommand::Validate(args) => run_pipeline_validate(args),
    }
}

fn run_pipeline_encode(args: PipelineEncodeArgs) -> Result<()> {
    let source = read_pipeline_yaml(&args.input)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline from YAML input {}", args.input.display()))?;

    let encoded = encode_pipeline(&source).context("encode pipeline binary")?;
    fs::write(&args.output, encoded)
        .with_context(|| format!("write pipeline binary output {}", args.output.display()))?;
    Ok(())
}

fn run_pipeline_decode(args: PipelineDecodeArgs) -> Result<()> {
    let bytes = fs::read(&args.input)
        .with_context(|| format!("read pipeline binary input {}", args.input.display()))?;
    let source = decode_pipeline(&bytes)
        .with_context(|| format!("decode pipeline binary input {}", args.input.display()))?;
    validate_pipeline(&source)
        .with_context(|| format!("validate decoded pipeline from {}", args.input.display()))?;

    let yaml = serde_yaml::to_string(&source).context("serialize pipeline as YAML")?;
    fs::write(&args.output, yaml)
        .with_context(|| format!("write pipeline YAML output {}", args.output.display()))?;
    Ok(())
}

fn run_pipeline_validate(args: PipelineValidateArgs) -> Result<()> {
    if args.binary {
        let bytes = fs::read(&args.input)
            .with_context(|| format!("read pipeline binary input {}", args.input.display()))?;
        let source = decode_pipeline(&bytes)
            .with_context(|| format!("decode pipeline binary input {}", args.input.display()))?;
        validate_pipeline(&source)
            .with_context(|| format!("validate decoded pipeline from {}", args.input.display()))?;
        return Ok(());
    }

    let source = read_pipeline_yaml(&args.input)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline from YAML input {}", args.input.display()))?;
    Ok(())
}

fn read_pipeline_yaml(path: &Path) -> Result<PipelineSource> {
    let input = fs::read_to_string(path)
        .with_context(|| format!("read pipeline YAML input {}", path.display()))?;
    serde_yaml::from_str(&input)
        .with_context(|| format!("parse pipeline YAML input {}", path.display()))
}

fn parse_pipeline_document(input: &[u8], label: &str) -> Result<PipelineSource> {
    if pipeline_bin_header_version(input).is_some() {
        return decode_pipeline(input)
            .with_context(|| format!("decode pipeline binary input {}", io_label(label)));
    }

    serde_yaml::from_slice(input)
        .with_context(|| format!("parse pipeline YAML input {}", io_label(label)))
}

fn open_pipeline_source<'a>(
    source: &'a PipelineSource,
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
                let reader = HttpBlockReader::new(url.clone(), DEFAULT_IMAGE_BLOCK_SIZE)
                    .await
                    .map_err(|err| anyhow!("open HTTP source {url}: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::File(source) => {
                let value = source.file.trim();
                if value.is_empty() {
                    bail!("pipeline file source is empty");
                }

                let reader = StdFileBlockReader::open(value, DEFAULT_IMAGE_BLOCK_SIZE)
                    .map_err(|err| anyhow!("open file source {value}: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::Casync(source) => open_casync_source(&source.casync).await,
            PipelineSource::Xz(source) => {
                let upstream = open_pipeline_source(source.xz.as_ref()).await?;
                let reader = XzBlockReader::new(upstream)
                    .await
                    .map_err(|err| anyhow!("open xz reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
            PipelineSource::AndroidSparseImg(source) => {
                let upstream = open_pipeline_source(source.android_sparseimg.as_ref()).await?;
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

                let upstream = open_pipeline_source(source.mbr.source.as_ref()).await?;
                let reader = MbrBlockReader::new(upstream, selector, DEFAULT_IMAGE_BLOCK_SIZE)
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

                let upstream = open_pipeline_source(source.gpt.source.as_ref()).await?;
                let reader = GptBlockReader::new(upstream, selector, DEFAULT_IMAGE_BLOCK_SIZE)
                    .await
                    .map_err(|err| anyhow!("open gpt reader: {err}"))?;
                Ok(Arc::new(reader) as DynBlockReader)
            }
        }
    })
}

async fn open_casync_source(
    source: &gibblox_pipeline::PipelineSourceCasync,
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
    chunk_store_config.cache_dir = Some(default_casync_cache_dir());
    let chunk_store = StdCasyncChunkStore::new(chunk_store_config)
        .map_err(|err| anyhow!("build casync chunk store: {err}"))?;

    let reader = CasyncBlockReader::open(
        index_source,
        chunk_store,
        CasyncReaderConfig {
            block_size: DEFAULT_IMAGE_BLOCK_SIZE,
            strict_verify: false,
        },
    )
    .await
    .map_err(|err| anyhow!("open casync reader {index}: {err}"))?;

    Ok(Arc::new(reader) as DynBlockReader)
}

fn parse_casync_index_locator(index: &str) -> Result<StdCasyncIndexLocator> {
    if let Ok(url) = Url::parse(index) {
        if matches!(url.scheme(), "http" | "https" | "file") {
            return Ok(StdCasyncIndexLocator::url(url));
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
            let chunk_url = derive_casync_chunk_store_url(url)?;
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
        if matches!(url.scheme(), "http" | "https" | "file") {
            return StdCasyncChunkStoreLocator::url_prefix(url)
                .map_err(|err| anyhow!("configure casync chunk store URL {value}: {err}"));
        }
    }

    Ok(StdCasyncChunkStoreLocator::path_prefix(PathBuf::from(
        value,
    )))
}

fn derive_casync_chunk_store_url(index_url: &Url) -> Result<Url> {
    if let Some(segments) = index_url.path_segments() {
        let segments: Vec<&str> = segments.collect();
        if let Some(index_pos) = segments.iter().rposition(|segment| *segment == "indexes") {
            let mut base_segments = segments[..=index_pos].to_vec();
            base_segments[index_pos] = "chunks";
            let mut url = index_url.clone();
            let mut path = String::from("/");
            path.push_str(&base_segments.join("/"));
            if !path.ends_with('/') {
                path.push('/');
            }
            url.set_path(&path);
            url.set_query(None);
            url.set_fragment(None);
            return Ok(url);
        }
    }

    index_url
        .join("./")
        .with_context(|| format!("derive casync chunk store URL from {index_url}"))
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

async fn write_reader_output(reader: &dyn BlockReader, output_path: &str) -> Result<()> {
    if output_path == "-" {
        let mut stdout = std::io::stdout().lock();
        pump_reader_to_writer(reader, &mut stdout).await?;
        stdout.flush().context("flush stdout")?;
        return Ok(());
    }

    let mut file = fs::File::create(output_path)
        .with_context(|| format!("create output file {}", io_label(output_path)))?;
    pump_reader_to_writer(reader, &mut file).await?;
    file.flush()
        .with_context(|| format!("flush output file {}", io_label(output_path)))
}

async fn pump_reader_to_writer<W: Write>(reader: &dyn BlockReader, writer: &mut W) -> Result<()> {
    let block_size = reader.block_size() as usize;
    if block_size == 0 {
        bail!("pipeline reader block size is zero");
    }
    let total_blocks = reader
        .total_blocks()
        .await
        .map_err(|err| anyhow!("read pipeline total blocks: {err}"))?;

    let mut buffer = vec![0u8; block_size * STREAM_BLOCK_WINDOW];
    let mut lba = 0u64;
    while lba < total_blocks {
        let remaining_blocks = total_blocks - lba;
        let blocks_to_read = remaining_blocks.min(STREAM_BLOCK_WINDOW as u64);
        let read_len = blocks_to_read as usize * block_size;
        let read = reader
            .read_blocks(lba, &mut buffer[..read_len], ReadContext::FOREGROUND)
            .await
            .map_err(|err| anyhow!("read pipeline blocks at lba {lba}: {err}"))?;
        if read < read_len {
            bail!("short pipeline read at lba {lba}: expected {read_len} bytes, got {read}");
        }

        writer
            .write_all(&buffer[..read_len])
            .with_context(|| format!("write output bytes at lba {lba}"))?;
        lba = lba
            .checked_add(blocks_to_read)
            .ok_or_else(|| anyhow!("pipeline block cursor overflow"))?;
    }

    Ok(())
}

fn read_input_bytes(path: &str) -> Result<Vec<u8>> {
    if path == "-" {
        let mut buf = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buf)
            .context("reading input from stdin")?;
        return Ok(buf);
    }

    fs::read(path).with_context(|| format!("read {}", io_label(path)))
}

fn validate_binary_output(path: &str, command: &str, stdout_is_tty: bool) -> Result<()> {
    if path == "-" && stdout_is_tty {
        bail!(
            "{command} output is binary and terminal output is disabled by default; use --output <FILE>"
        );
    }
    Ok(())
}

fn io_label(path: &str) -> String {
    if path == "-" {
        "stdin/stdout".to_string()
    } else {
        path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, parse_pipeline_document, validate_binary_output};
    use clap::Parser;

    #[test]
    fn parse_top_level_pipeline_and_output() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline.yaml", "--output", "out.img"]);
        assert_eq!(cli.pipeline.as_deref(), Some("pipeline.yaml"));
        assert_eq!(cli.output.as_deref(), Some("out.img"));
    }

    #[test]
    fn parse_validate_with_positional_input() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline", "validate", "/tmp/lol"]);
        assert!(matches!(cli.command, Some(super::Commands::Pipeline(_))));
    }

    #[test]
    fn parse_source_style_yaml_pipeline() {
        let source = parse_pipeline_document(
            br#"gpt:
  partlabel: rootfs
  source:
    android_sparseimg:
      source:
        xz:
          source:
            http: https://cdn.example.invalid/device.img.xz
"#,
            "/tmp/lol",
        )
        .expect("parse source-style pipeline YAML");
        assert!(matches!(source, gibblox_pipeline::PipelineSource::Gpt(_)));
    }

    #[test]
    fn rejects_binary_stdout_for_tty() {
        let err = validate_binary_output("-", "gibblox pipeline", true)
            .expect_err("tty stdout should be rejected for binary output");
        let msg = format!("{err}");
        assert!(msg.contains("terminal output is disabled by default"));
    }

    #[test]
    fn allows_binary_stdout_for_non_tty() {
        assert!(
            validate_binary_output("-", "gibblox pipeline", false).is_ok(),
            "non-tty stdout should be allowed"
        );
    }
}
