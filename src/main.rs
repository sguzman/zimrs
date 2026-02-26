use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use zimrs::config::Config;
use zimrs::run_conversion;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Configurable Wiktionary ZIM to SQLite converter"
)]
struct Cli {
    #[arg(short, long, default_value = "config/wiktionary.toml")]
    config: PathBuf,

    #[arg(long)]
    max_entries: Option<u32>,

    #[arg(long)]
    start_index: Option<u32>,

    #[arg(long)]
    overwrite: bool,

    #[arg(long)]
    json_logs: bool,

    #[arg(long)]
    log_level: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut config = Config::from_toml_path(&cli.config)
        .with_context(|| format!("failed to load config {}", cli.config.display()))?;

    if let Some(max_entries) = cli.max_entries {
        config.selection.max_entries = Some(max_entries);
    }

    if let Some(start_index) = cli.start_index {
        config.selection.start_index = start_index;
    }

    if cli.overwrite {
        config.sqlite.overwrite = true;
    }

    if cli.json_logs {
        config.logging.json = true;
    }

    if let Some(level) = cli.log_level {
        config.logging.level = level;
    }

    init_tracing(&config)?;

    info!(
        config_path = %cli.config.display(),
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
        extraction_errors = metrics.extraction_errors,
        "run summary"
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
