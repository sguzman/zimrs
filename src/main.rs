use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use zimrs::config::{Config, StorageBackend};
use zimrs::db::Database;
use zimrs::export::{ExportOptions, export_json};
use zimrs::release::{build_release_artifacts, create_sample_database};
use zimrs::run_conversion;
use zimrs::verify::{VerifyOptions, verify_zim_file};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Configurable Wiktionary ZIM converter with Postgres-first and SQLite compatibility backends"
)]
struct Cli {
    #[arg(short, long, default_value = "config/wiktionary.toml", global = true)]
    config: PathBuf,

    #[arg(long, global = true)]
    json_logs: bool,

    #[arg(long, global = true)]
    log_level: Option<String>,

    #[arg(
        long,
        global = true,
        help = "Enable debug-level logs and write a copy to logs/"
    )]
    debug: bool,

    #[arg(long, global = true)]
    backend: Option<StorageBackend>,

    #[arg(long, global = true, help = "Force SQLite backend compatibility mode")]
    sqlite: bool,

    #[arg(long, global = true)]
    pg_host: Option<String>,

    #[arg(long, global = true)]
    pg_port: Option<u16>,

    #[arg(long, global = true)]
    pg_user: Option<String>,

    #[arg(long, global = true)]
    pg_password: Option<String>,

    #[arg(long, global = true)]
    pg_database: Option<String>,

    #[arg(long, global = true)]
    pg_schema: Option<String>,

    #[arg(long, global = true)]
    pg_sslmode: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Convert(ConvertArgs),
    VerifyZim(VerifyZimArgs),
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
struct VerifyZimArgs {
    #[arg(long)]
    path: Option<PathBuf>,

    #[arg(long)]
    skip_checksum: bool,

    #[arg(long, default_value_t = 8192)]
    tail_window_bytes: usize,
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

    if cli.sqlite {
        config.backend = StorageBackend::Sqlite;
    } else if let Some(backend) = cli.backend {
        config.backend = backend;
    }

    if let Some(host) = cli.pg_host {
        config.postgres.host = host;
    }
    if let Some(port) = cli.pg_port {
        config.postgres.port = port;
    }
    if let Some(user) = cli.pg_user {
        config.postgres.user = user;
    }
    if let Some(password) = cli.pg_password {
        config.postgres.password = password;
    }
    if let Some(database) = cli.pg_database {
        config.postgres.database = database;
    }
    if let Some(schema) = cli.pg_schema {
        config.postgres.schema = schema;
    }
    if let Some(sslmode) = cli.pg_sslmode {
        config.postgres.sslmode = sslmode;
    }

    if cli.debug {
        config.logging.level = "debug".to_owned();
    }

    let _log_guard = init_tracing(&config, cli.debug)?;

    match cli.command.unwrap_or(Commands::Convert(ConvertArgs {
        max_entries: None,
        start_index: None,
        overwrite: false,
        no_resume: false,
        checkpoint_name: None,
        extraction_threads: None,
    })) {
        Commands::Convert(args) => run_convert(args, config, &cli.config),
        Commands::VerifyZim(args) => run_verify_zim(args, config),
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
        // Overwrite is backend-agnostic; SQLite removes the DB file, Postgres resets target schema.
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
        backend = ?config.backend,
        pg_host = %config.postgres.host,
        pg_port = config.postgres.port,
        pg_database = %config.postgres.database,
        pg_schema = %config.postgres.schema,
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

fn run_verify_zim(args: VerifyZimArgs, config: Config) -> Result<()> {
    let path = args.path.unwrap_or(config.input.zim_path);
    let options = VerifyOptions {
        checksum: !args.skip_checksum,
        tail_window_bytes: args.tail_window_bytes,
    };

    let report = verify_zim_file(&path, &options)?;
    info!(
        path = %report.path,
        size_bytes = report.size_bytes,
        magic_ok = report.magic_ok,
        tail_all_zero = report.tail_all_zero,
        tail_zero_ratio = report.tail_zero_ratio,
        article_count = report.article_count,
        cluster_count = report.cluster_count,
        checksum_ok = ?report.checksum_ok,
        "zim integrity verification passed"
    );

    Ok(())
}

fn run_export_json(args: ExportJsonArgs, mut config: Config) -> Result<()> {
    config.sqlite.overwrite = false;

    let options = ExportOptions {
        config: config.clone(),
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

fn init_tracing(config: &Config, debug_to_file: bool) -> Result<Option<WorkerGuard>> {
    let default_directive = config
        .logging
        .level
        .parse()
        .context("invalid logging.level directive")?;

    let mut env_filter = EnvFilter::builder()
        .with_default_directive(default_directive)
        .from_env_lossy();

    if debug_to_file {
        // Keep application debug logs, but avoid flooding from DB client internals.
        env_filter = env_filter
            .add_directive("tokio_postgres=warn".parse()?)
            .add_directive("postgres=info".parse()?)
            .add_directive("r2d2=info".parse()?);
    }

    if debug_to_file {
        std::fs::create_dir_all("logs").context("failed to create logs directory")?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before UNIX_EPOCH")?
            .as_millis();
        let file_name = format!("zimrs-{now_ms}.log");
        let file_appender = tracing_appender::rolling::never("logs", &file_name);
        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

        let make_writer = std::io::stdout.and(file_writer);
        let builder = tracing_subscriber::fmt()
            .with_target(true)
            .with_thread_ids(true)
            .with_env_filter(env_filter)
            .with_file(true)
            .with_line_number(true)
            .with_writer(make_writer);

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

        info!(log_file = %format!("logs/{file_name}"), "debug file logging enabled");
        return Ok(Some(guard));
    }

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

    Ok(None)
}
