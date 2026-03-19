use std::{
    fs,
    io::{IsTerminal, Read, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use gibblox_core::{BlockReader, ReadContext, WindowBlockReader};
use gibblox_pipeline::{
    OpenPipelineOptions, PipelineCachePolicy, PipelineSource, decode_pipeline, encode_pipeline,
    open_pipeline, pipeline_bin_header_version, validate_pipeline,
};
use tracing_subscriber::{EnvFilter, fmt};

const DEFAULT_IMAGE_BLOCK_SIZE: u32 = 512;
const DEFAULT_SOURCE_BLOCK_SIZE: u32 = 4096;
const STREAM_BLOCK_WINDOW: usize = 256;

type DynBlockReader = Arc<dyn BlockReader>;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum CliCachePolicy {
    None,
    Head,
    Tail,
}

impl From<CliCachePolicy> for PipelineCachePolicy {
    fn from(value: CliCachePolicy) -> Self {
        match value {
            CliCachePolicy::None => Self::None,
            CliCachePolicy::Head => Self::Head,
            CliCachePolicy::Tail => Self::Tail,
        }
    }
}

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
    /// Start block offset within resolved pipeline output.
    #[arg(long, value_name = "START", requires = "pipeline")]
    start: Option<u64>,
    /// Number of blocks to copy from resolved pipeline output.
    #[arg(long, value_name = "COUNT", requires = "pipeline")]
    count: Option<u64>,
    /// Logical block size exposed by CLI output reads.
    #[arg(long, value_name = "BLOCK_SIZE", requires = "pipeline")]
    blocksize: Option<u32>,
    /// Cache placement policy (`none`, `head`, or `tail`).
    #[arg(long, value_name = "POLICY", value_enum, requires = "pipeline")]
    cache_policy: Option<CliCachePolicy>,
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
    /// Input YAML file ("-" for stdin).
    #[arg(value_name = "INPUT", default_value = "-")]
    input: String,
    /// Output binary file ("-" for stdout).
    #[arg(short = 'o', long, default_value = "-")]
    output: String,
}

#[derive(Args)]
struct PipelineDecodeArgs {
    /// Input binary file ("-" for stdin).
    #[arg(value_name = "INPUT", default_value = "-")]
    input: String,
    /// Output YAML file ("-" for stdout).
    #[arg(short = 'o', long, default_value = "-")]
    output: String,
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
    init_tracing()?;

    let cli = Cli::parse();

    if cli.command.is_some()
        && (cli.pipeline.is_some()
            || cli.output.is_some()
            || cli.start.is_some()
            || cli.count.is_some()
            || cli.blocksize.is_some()
            || cli.cache_policy.is_some())
    {
        bail!(
            "PIPELINE, --output, --start, --count, --blocksize, and --cache-policy cannot be used with subcommands"
        );
    }

    match cli.command {
        Some(Commands::Pipeline(args)) => run_pipeline(args.command).await,
        None => {
            let input = cli
                .pipeline
                .ok_or_else(|| anyhow!("missing required PIPELINE input"))?;
            let output = cli.output.as_deref().unwrap_or("-");
            run_default_pipeline_execute(
                &input,
                output,
                cli.start.unwrap_or(0),
                cli.count,
                cli.blocksize,
                cli.cache_policy,
            )
            .await
        }
    }
}

fn init_tracing() -> Result<()> {
    let filter = match std::env::var("RUST_LOG") {
        Ok(filter) if !filter.trim().is_empty() => {
            EnvFilter::try_from_default_env().context("failed to parse RUST_LOG env filter")?
        }
        _ => EnvFilter::new("off"),
    };

    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing subscriber: {err}"))
}

async fn run_default_pipeline_execute(
    input_path: &str,
    output_path: &str,
    start_blocks: u64,
    count_blocks: Option<u64>,
    requested_block_size: Option<u32>,
    cache_policy: Option<CliCachePolicy>,
) -> Result<()> {
    validate_binary_output(
        output_path,
        "gibblox pipeline",
        std::io::stdout().is_terminal(),
    )?;

    let output_block_size = requested_block_size.unwrap_or(DEFAULT_IMAGE_BLOCK_SIZE);
    validate_image_block_size(output_block_size)?;
    let cache_policy = cache_policy
        .map(PipelineCachePolicy::from)
        .unwrap_or(PipelineCachePolicy::None);

    let input = read_input_bytes(input_path)?;
    let source = parse_pipeline_document(&input, input_path)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline input {}", io_label(input_path)))?;

    let reader = open_pipeline(
        &source,
        &OpenPipelineOptions {
            image_block_size: DEFAULT_SOURCE_BLOCK_SIZE,
            cache_policy,
            ..OpenPipelineOptions::default()
        },
    )
    .await?;
    let reader = shape_output_reader(reader, start_blocks, count_blocks, output_block_size).await?;
    write_reader_output(reader.as_ref(), output_path).await
}

async fn run_pipeline(command: PipelineCommand) -> Result<()> {
    match command {
        PipelineCommand::Encode(args) => run_pipeline_encode(args),
        PipelineCommand::Decode(args) => run_pipeline_decode(args),
        PipelineCommand::Validate(args) => run_pipeline_validate(args),
    }
}

fn run_pipeline_encode(args: PipelineEncodeArgs) -> Result<()> {
    let source = read_pipeline_yaml(&args.input)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline from YAML input {}", args.input))?;

    let encoded = encode_pipeline(&source).context("encode pipeline binary")?;
    write_binary_output(&args.output, &encoded, "gibblox pipeline encode")
}

fn run_pipeline_decode(args: PipelineDecodeArgs) -> Result<()> {
    let bytes = read_input_bytes(&args.input)
        .with_context(|| format!("read pipeline binary input {}", args.input))?;
    let source = decode_pipeline(&bytes)
        .with_context(|| format!("decode pipeline binary input {}", args.input))?;
    validate_pipeline(&source)
        .with_context(|| format!("validate decoded pipeline from {}", args.input))?;

    let yaml = serde_yaml::to_string(&source).context("serialize pipeline as YAML")?;
    write_text_output(&args.output, &yaml)
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

    let input = args.input.to_string_lossy();
    let source = read_pipeline_yaml(&input)?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline from YAML input {}", args.input.display()))?;
    Ok(())
}

fn read_pipeline_yaml(path: &str) -> Result<PipelineSource> {
    let input = read_input_bytes(path)
        .with_context(|| format!("read pipeline YAML input {}", io_label(path)))?;
    serde_yaml::from_slice(&input)
        .with_context(|| format!("parse pipeline YAML input {}", io_label(path)))
}

fn parse_pipeline_document(input: &[u8], label: &str) -> Result<PipelineSource> {
    if pipeline_bin_header_version(input).is_some() {
        return decode_pipeline(input)
            .with_context(|| format!("decode pipeline binary input {}", io_label(label)));
    }

    serde_yaml::from_slice(input)
        .with_context(|| format!("parse pipeline YAML input {}", io_label(label)))
}

fn validate_image_block_size(block_size: u32) -> Result<()> {
    if block_size == 0 || !block_size.is_power_of_two() {
        bail!("pipeline block size must be a non-zero power of two")
    }
    Ok(())
}

fn resolve_block_span(
    total_blocks: u64,
    start_blocks: u64,
    count_blocks: Option<u64>,
) -> Result<(u64, u64)> {
    if start_blocks > total_blocks {
        bail!("pipeline read start block {start_blocks} exceeds available blocks {total_blocks}");
    }

    let blocks = match count_blocks {
        Some(count) => {
            let end = start_blocks
                .checked_add(count)
                .ok_or_else(|| anyhow!("pipeline block range overflow"))?;
            if end > total_blocks {
                bail!(
                    "pipeline read range start={start_blocks} count={count} exceeds available blocks {total_blocks}"
                );
            }
            count
        }
        None => total_blocks - start_blocks,
    };

    Ok((start_blocks, blocks))
}

async fn shape_output_reader(
    reader: DynBlockReader,
    start_blocks: u64,
    count_blocks: Option<u64>,
    output_block_size: u32,
) -> Result<DynBlockReader> {
    let source_block_size = reader.block_size() as u64;
    let source_total_blocks = reader
        .total_blocks()
        .await
        .map_err(|err| anyhow!("read pipeline total blocks: {err}"))?;
    let source_size_bytes = source_total_blocks
        .checked_mul(source_block_size)
        .ok_or_else(|| anyhow!("pipeline source byte size overflow"))?;

    let output_block_size_u64 = output_block_size as u64;
    let output_total_blocks = source_size_bytes.div_ceil(output_block_size_u64);
    let (start_blocks, span_blocks) =
        resolve_block_span(output_total_blocks, start_blocks, count_blocks)?;
    if start_blocks == 0
        && span_blocks == output_total_blocks
        && output_block_size == reader.block_size()
    {
        return Ok(reader);
    }

    let offset_bytes = start_blocks
        .checked_mul(output_block_size_u64)
        .ok_or_else(|| anyhow!("pipeline start byte offset overflow"))?;
    let requested_size_bytes = span_blocks
        .checked_mul(output_block_size_u64)
        .ok_or_else(|| anyhow!("pipeline span byte length overflow"))?;
    let remaining_bytes = source_size_bytes
        .checked_sub(offset_bytes)
        .ok_or_else(|| anyhow!("pipeline start byte offset exceeds source size"))?;
    let size_bytes = requested_size_bytes.min(remaining_bytes);

    let window = WindowBlockReader::new(reader, offset_bytes, size_bytes, output_block_size)
        .await
        .map_err(|err| anyhow!("construct pipeline read window: {err}"))?;
    Ok(Arc::new(window) as DynBlockReader)
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

fn write_binary_output(path: &str, output: &[u8], command: &str) -> Result<()> {
    validate_binary_output(path, command, std::io::stdout().is_terminal())?;
    if path == "-" {
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(output)
            .with_context(|| format!("write {command} output bytes to stdout"))?;
        stdout.flush().context("flush stdout")?;
        return Ok(());
    }

    fs::write(path, output).with_context(|| format!("write {command} output {}", io_label(path)))
}

fn write_text_output(path: &str, output: &str) -> Result<()> {
    if path == "-" {
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(output.as_bytes())
            .context("write pipeline decode output to stdout")?;
        stdout.flush().context("flush stdout")?;
        return Ok(());
    }

    fs::write(path, output).with_context(|| format!("write output {}", io_label(path)))
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
    use super::{
        Cli, PipelineArgs, PipelineCommand, parse_pipeline_document, resolve_block_span,
        validate_binary_output,
    };
    use clap::Parser;

    #[test]
    fn parse_top_level_pipeline_and_output() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline.yaml", "--output", "out.img"]);
        assert_eq!(cli.pipeline.as_deref(), Some("pipeline.yaml"));
        assert_eq!(cli.output.as_deref(), Some("out.img"));
    }

    #[test]
    fn parse_top_level_pipeline_range_and_blocksize() {
        let cli = Cli::parse_from([
            "gibblox-cli",
            "pipeline.yaml",
            "--output",
            "out.img",
            "--start",
            "8",
            "--count",
            "16",
            "--blocksize",
            "4096",
        ]);
        assert_eq!(cli.pipeline.as_deref(), Some("pipeline.yaml"));
        assert_eq!(cli.output.as_deref(), Some("out.img"));
        assert_eq!(cli.start, Some(8));
        assert_eq!(cli.count, Some(16));
        assert_eq!(cli.blocksize, Some(4096));
    }

    #[test]
    fn parse_top_level_pipeline_cache_policy() {
        let cli = Cli::parse_from([
            "gibblox-cli",
            "pipeline.yaml",
            "--output",
            "out.img",
            "--cache-policy",
            "tail",
        ]);
        assert!(matches!(
            cli.cache_policy,
            Some(super::CliCachePolicy::Tail)
        ));
    }

    #[test]
    fn parse_validate_with_positional_input() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline", "validate", "/tmp/lol"]);
        assert!(matches!(cli.command, Some(super::Commands::Pipeline(_))));
    }

    #[test]
    fn parse_pipeline_encode_defaults_to_stdio() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline", "encode"]);
        let super::Commands::Pipeline(PipelineArgs {
            command: PipelineCommand::Encode(args),
        }) = cli.command.expect("command is present")
        else {
            panic!("expected pipeline encode command")
        };
        assert_eq!(args.input, "-");
        assert_eq!(args.output, "-");
    }

    #[test]
    fn parse_pipeline_decode_defaults_to_stdio() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline", "decode"]);
        let super::Commands::Pipeline(PipelineArgs {
            command: PipelineCommand::Decode(args),
        }) = cli.command.expect("command is present")
        else {
            panic!("expected pipeline decode command")
        };
        assert_eq!(args.input, "-");
        assert_eq!(args.output, "-");
    }

    #[test]
    fn parse_pipeline_encode_custom_paths() {
        let cli = Cli::parse_from([
            "gibblox-cli",
            "pipeline",
            "encode",
            "in.yaml",
            "-o",
            "out.bin",
        ]);
        let super::Commands::Pipeline(PipelineArgs {
            command: PipelineCommand::Encode(args),
        }) = cli.command.expect("command is present")
        else {
            panic!("expected pipeline encode command")
        };
        assert_eq!(args.input, "in.yaml");
        assert_eq!(args.output, "out.bin");
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

    #[test]
    fn resolve_block_span_defaults_to_remaining() {
        let (start, blocks) = resolve_block_span(100, 10, None).expect("resolve default span");
        assert_eq!(start, 10);
        assert_eq!(blocks, 90);
    }

    #[test]
    fn resolve_block_span_rejects_out_of_range_count() {
        let err = resolve_block_span(100, 90, Some(11)).expect_err("range should overflow source");
        let msg = format!("{err}");
        assert!(msg.contains("exceeds available blocks"));
    }
}
