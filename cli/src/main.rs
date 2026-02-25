use std::{fs, path::Path};

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use gibblox_pipeline::{PipelineSource, decode_pipeline, encode_pipeline, validate_pipeline};

#[derive(Parser)]
#[command(author, version, about = "gibblox CLI utilities")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
    #[arg(short = 'i', long)]
    input: std::path::PathBuf,
    /// Output binary file.
    #[arg(short = 'o', long)]
    output: std::path::PathBuf,
}

#[derive(Args)]
struct PipelineDecodeArgs {
    /// Input binary file.
    #[arg(short = 'i', long)]
    input: std::path::PathBuf,
    /// Output YAML file.
    #[arg(short = 'o', long)]
    output: std::path::PathBuf,
}

#[derive(Args)]
struct PipelineValidateArgs {
    /// Input YAML or binary file.
    #[arg(short = 'i', long)]
    input: std::path::PathBuf,
    /// Parse input as binary pipeline.
    #[arg(long)]
    binary: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pipeline(args) => run_pipeline(args.command),
    }
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
