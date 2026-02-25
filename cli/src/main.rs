use std::{
    fs,
    io::{IsTerminal, Read, Write},
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use gibblox_pipeline::{PipelineSource, decode_pipeline, encode_pipeline, validate_pipeline};

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
    /// Output compiled pipeline path ("-" for stdout).
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
    input: std::path::PathBuf,
    /// Output binary file.
    #[arg(short = 'o', long)]
    output: std::path::PathBuf,
}

#[derive(Args)]
struct PipelineDecodeArgs {
    /// Input binary file.
    #[arg(value_name = "INPUT")]
    input: std::path::PathBuf,
    /// Output YAML file.
    #[arg(short = 'o', long)]
    output: std::path::PathBuf,
}

#[derive(Args)]
struct PipelineValidateArgs {
    /// Input YAML or binary file.
    #[arg(value_name = "INPUT")]
    input: std::path::PathBuf,
    /// Parse input as binary pipeline.
    #[arg(long)]
    binary: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.command.is_some() {
        if cli.pipeline.is_some() || cli.output.is_some() {
            bail!("PIPELINE and --output cannot be used with subcommands");
        }
    }

    match cli.command {
        Some(Commands::Pipeline(args)) => run_pipeline(args.command),
        None => {
            let input = cli
                .pipeline
                .ok_or_else(|| anyhow!("missing required PIPELINE input"))?;
            let output = cli.output.as_deref().unwrap_or("-");
            run_default_pipeline_encode(&input, output)
        }
    }
}

fn run_default_pipeline_encode(input_path: &str, output_path: &str) -> Result<()> {
    let input = read_input_bytes(input_path)?;
    let source: PipelineSource = serde_yaml::from_slice(&input)
        .with_context(|| format!("parse pipeline YAML input {}", io_label(input_path)))?;
    validate_pipeline(&source)
        .with_context(|| format!("validate pipeline from YAML input {}", io_label(input_path)))?;

    let bytes = encode_pipeline(&source).context("encode pipeline binary")?;
    validate_binary_output(
        output_path,
        "gibblox pipeline",
        std::io::stdout().is_terminal(),
    )?;
    write_output_bytes(output_path, &bytes)
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

fn write_output_bytes(path: &str, bytes: &[u8]) -> Result<()> {
    if path == "-" {
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(bytes)
            .context("writing output to stdout")?;
        stdout.flush().context("flushing stdout")?;
        return Ok(());
    }

    fs::write(path, bytes).with_context(|| format!("write {}", io_label(path)))
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
    use super::{Cli, validate_binary_output};
    use clap::Parser;

    #[test]
    fn parse_top_level_pipeline_and_output() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline.yaml", "--output", "out.gbxp"]);
        assert_eq!(cli.pipeline.as_deref(), Some("pipeline.yaml"));
        assert_eq!(cli.output.as_deref(), Some("out.gbxp"));
    }

    #[test]
    fn parse_validate_with_positional_input() {
        let cli = Cli::parse_from(["gibblox-cli", "pipeline", "validate", "/tmp/lol"]);
        assert!(matches!(cli.command, Some(super::Commands::Pipeline(_))));
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
