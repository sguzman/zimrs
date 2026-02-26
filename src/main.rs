use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use zimrs::config::Config;
use zimrs::db::Database;
use zimrs::export::{ExportOptions, export_json};
use zimrs::release::{build_release_artifacts, create_sample_database};
use zimrs::run_conversion;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Configurable Wiktionary ZIM to SQLite converter and tooling"
)]
struct Cli {
    #[arg(short, long, default_value = "config/wiktionary.toml", global = true)]
    config: PathBuf,

    #[arg(long, global = true)]
    json_logs: bool,

    #[arg(long, global = true)]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Convert(ConvertArgs),
    Reindex(ReindexArgs),
    ExportJson(ExportJsonArgs),
    SampleDb(SampleDbArgs),
    BuildArtifacts(BuildArtifactsArgs),
}

#[derive(Debug, clap::Args)]
struct ConvertArgs {
    #[arg(long)]
    max_entries: Option<u32>,

    #[arg(long)]
    start_index: Option<u32>,

    #[arg(long)]
    overwrite: bool,

    #[arg(long)]
    no_resume: bool,

    #[arg(long)]
    checkpoint_name: Option<String>,

    #[arg(long)]
    extraction_threads: Option<usize>,
}

#[derive(Debug, clap::Args)]
struct ReindexArgs {
    #[arg(long)]
    watermark_name: Option<String>,

    #[arg(long)]
    chunk_size: Option<usize>,
}

#[derive(Debug, clap::Args)]
struct ExportJsonArgs {
    #[arg(long)]
    output: PathBuf,

    #[arg(long)]
    limit: Option<u64>,

    #[arg(long)]
    pretty: bool,

    #[arg(long)]
    include_raw_html: bool,

    #[arg(long)]
    array: bool,

    #[arg(long)]
    batch_size: Option<usize>,
}

#[derive(Debug, clap::Args)]
struct SampleDbArgs {
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, clap::Args)]
struct BuildArtifactsArgs {
    #[arg(long)]
    binary_path: Option<PathBuf>,

    #[arg(long)]
    build_release: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut config = Config::from_toml_path(&cli.config)
        .with_context(|| format!("failed to load config {}", cli.config.display()))?;

    if cli.json_logs {
        config.logging.json = true;
    }

    if let Some(level) = cli.log_level {
        config.logging.level = level;
    }

    init_tracing(&config)?;

    match cli.command.unwrap_or(Commands::Convert(ConvertArgs {
        max_entries: None,
        start_index: None,
        overwrite: false,
        no_resume: false,
        checkpoint_name: None,
        extraction_threads: None,
    })) {
        Commands::Convert(args) => run_convert(args, config, &cli.config),
        Commands::Reindex(args) => run_reindex(args, config),
        Commands::ExportJson(args) => run_export_json(args, config),
        Commands::SampleDb(args) => run_sample_db(args),
        Commands::BuildArtifacts(args) => run_build_artifacts(args, config, &cli.config),
    }
}

fn run_convert(args: ConvertArgs, mut config: Config, config_path: &PathBuf) -> Result<()> {
    if let Some(max_entries) = args.max_entries {
        config.selection.max_entries = Some(max_entries);
    }

    if let Some(start_index) = args.start_index {
        config.selection.start_index = start_index;
    }

    if args.overwrite {
        config.sqlite.overwrite = true;
    }

    if args.no_resume {
        config.checkpoint.resume = false;
    }

    if let Some(name) = args.checkpoint_name {
        config.checkpoint.name = name;
    }

    if let Some(threads) = args.extraction_threads {
        config.workers.extraction_threads = threads.max(1);
    }

    info!(
        config_path = %config_path.display(),
        zim_path = %config.input.zim_path.display(),
        sqlite_path = %config.input.sqlite_path.display(),
        "starting conversion"
    );

    let metrics = run_conversion(&config)?;

    if metrics.ingested_pages == 0 {
        warn!("conversion finished with zero ingested pages");
    }

    info!(
        elapsed_ms = metrics.elapsed_ms(),
        scanned_entries = metrics.scanned_entries,
        filtered_entries = metrics.filtered_entries,
        ingested_pages = metrics.ingested_pages,
        extracted_definitions = metrics.extracted_definitions,
        extracted_relations = metrics.extracted_relations,
        extraction_errors = metrics.extraction_errors,
        checkpoint_updates = metrics.checkpoint_updates,
        resumed_from_checkpoint = metrics.resumed_from_checkpoint,
        "run summary"
    );

    Ok(())
}

fn run_reindex(args: ReindexArgs, mut config: Config) -> Result<()> {
    config.sqlite.overwrite = false;

    if let Some(name) = args.watermark_name {
        config.reindex.watermark_name = name;
    }

    if let Some(chunk_size) = args.chunk_size {
        config.reindex.chunk_size = chunk_size.max(1);
    }

    let db = Database::open(&config)?;
    db.init_schema()?;
    let metrics =
        db.incremental_reindex(&config.reindex.watermark_name, config.reindex.chunk_size)?;

    info!(
        updated_pages = metrics.updated_pages,
        watermark = ?metrics.watermark,
        "incremental reindex complete"
    );

    Ok(())
}

fn run_export_json(args: ExportJsonArgs, mut config: Config) -> Result<()> {
    config.sqlite.overwrite = false;

    let options = ExportOptions {
        sqlite_path: config.input.sqlite_path,
        output_path: args.output,
        pretty: args.pretty || config.export.pretty,
        include_raw_html: args.include_raw_html || config.export.include_raw_html,
        json_lines: if args.array {
            false
        } else {
            config.export.json_lines
        },
        limit: args.limit,
        batch_size: args.batch_size.unwrap_or(config.export.batch_size).max(1),
    };

    let metrics = export_json(&options)?;

    info!(
        output_path = %options.output_path.display(),
        exported_pages = metrics.exported_pages,
        exported_definitions = metrics.exported_definitions,
        exported_relations = metrics.exported_relations,
        exported_aliases = metrics.exported_aliases,
        "json export complete"
    );

    Ok(())
}

fn run_sample_db(args: SampleDbArgs) -> Result<()> {
    create_sample_database(&args.output)?;
    info!(output = %args.output.display(), "sample database created");
    Ok(())
}

fn run_build_artifacts(
    args: BuildArtifactsArgs,
    config: Config,
    config_path: &PathBuf,
) -> Result<()> {
    let binary_path = if args.build_release {
        let status = Command::new("cargo")
            .arg("build")
            .arg("--release")
            .status()
            .context("failed to run cargo build --release")?;
        if !status.success() {
            anyhow::bail!("cargo build --release failed");
        }
        PathBuf::from("target/release/zimrs")
    } else if let Some(path) = args.binary_path {
        path
    } else {
        std::env::current_exe().context("failed to determine current executable path")?
    };

    let metrics = build_release_artifacts(&config, &binary_path, config_path)?;
    info!(
        artifact_dir = %metrics.artifact_dir.display(),
        archive_path = %metrics.archive_path.display(),
        sample_db_path = %metrics.sample_db_path.display(),
        checksum_path = %metrics.checksum_path.display(),
        "release artifacts built"
    );

    Ok(())
}

fn init_tracing(config: &Config) -> Result<()> {
    let default_directive = config
        .logging
        .level
        .parse()
        .context("invalid logging.level directive")?;

    let env_filter = EnvFilter::builder()
        .with_default_directive(default_directive)
        .from_env_lossy();

    let builder = tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true);

    if config.logging.json {
        builder
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to init JSON logger: {error}"))?;
    } else {
        builder
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to init logger: {error}"))?;
    }

    Ok(())
}
