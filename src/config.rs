use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    Postgres,
    Sqlite,
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::Postgres
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub backend: StorageBackend,
    pub input: InputConfig,
    pub postgres: PostgresConfig,
    pub selection: SelectionConfig,
    pub extraction: ExtractionConfig,
    pub sqlite: SqliteConfig,
    pub logging: LoggingConfig,
    pub checkpoint: CheckpointConfig,
    pub workers: WorkerConfig,
    pub reindex: ReindexConfig,
    pub export: ExportConfig,
    pub release: ReleaseConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: StorageBackend::default(),
            input: InputConfig::default(),
            postgres: PostgresConfig::default(),
            selection: SelectionConfig::default(),
            extraction: ExtractionConfig::default(),
            sqlite: SqliteConfig::default(),
            logging: LoggingConfig::default(),
            checkpoint: CheckpointConfig::default(),
            workers: WorkerConfig::default(),
            reindex: ReindexConfig::default(),
            export: ExportConfig::default(),
            release: ReleaseConfig::default(),
        }
    }
}

impl Config {
    pub fn from_toml_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let parsed: Self =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
        Ok(parsed)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InputConfig {
    pub zim_path: PathBuf,
    pub sqlite_path: PathBuf,
}

impl Default for InputConfig {
    fn default() -> Self {
        Self {
            zim_path: PathBuf::from("tmp/wiktionary_en_all_nopic_2026-02.zim"),
            sqlite_path: PathBuf::from("out/wiktionary.sqlite"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub schema: String,
    pub sslmode: String,
    pub connect_timeout_secs: u64,
    pub max_connection_retries: usize,
    pub retry_backoff_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 5432,
            user: "admin".to_owned(),
            password: "admin".to_owned(),
            database: "data".to_owned(),
            schema: "dictionary".to_owned(),
            sslmode: "disable".to_owned(),
            connect_timeout_secs: 10,
            max_connection_retries: 5,
            retry_backoff_ms: 750,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SelectionConfig {
    pub start_index: u32,
    pub max_entries: Option<u32>,
    pub include_namespaces: Vec<String>,
    pub include_mime_prefixes: Vec<String>,
    pub exclude_url_prefixes: Vec<String>,
    pub exclude_title_prefixes: Vec<String>,
    pub skip_redirects: bool,
    pub require_title: bool,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            start_index: 0,
            max_entries: None,
            include_namespaces: vec!["C".to_owned(), "A".to_owned()],
            include_mime_prefixes: vec!["text/html".to_owned()],
            exclude_url_prefixes: vec!["Special:".to_owned(), "Wiktionary:".to_owned()],
            exclude_title_prefixes: vec!["Appendix:".to_owned(), "Reconstruction:".to_owned()],
            skip_redirects: true,
            require_title: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExtractionConfig {
    pub store_raw_html: bool,
    pub store_plain_text: bool,
    pub parse_language_sections: bool,
    pub parse_relations: bool,
    pub language_allowlist: Vec<String>,
    pub min_definition_chars: usize,
    pub max_definitions_per_language: usize,
    pub relation_types: Vec<String>,
    pub max_relations_per_type: usize,
    pub default_normalizer: String,
    pub language_normalizers: HashMap<String, String>,
    pub nested_list_depth_limit: usize,
    pub confidence_threshold: f64,
    pub include_title_as_alias: bool,
    pub alias_min_length: usize,
}

impl Default for ExtractionConfig {
    fn default() -> Self {
        Self {
            store_raw_html: false,
            store_plain_text: true,
            parse_language_sections: true,
            parse_relations: true,
            language_allowlist: Vec::new(),
            min_definition_chars: 20,
            max_definitions_per_language: 32,
            relation_types: vec![
                "synonyms".to_owned(),
                "antonyms".to_owned(),
                "translations".to_owned(),
            ],
            max_relations_per_type: 48,
            default_normalizer: "identity".to_owned(),
            language_normalizers: HashMap::new(),
            nested_list_depth_limit: 4,
            confidence_threshold: 0.15,
            include_title_as_alias: true,
            alias_min_length: 2,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SqliteConfig {
    pub batch_size: usize,
    pub overwrite: bool,
    pub enable_fts: bool,
    pub journal_mode: String,
    pub synchronous: String,
    pub cache_size_kib: i64,
    pub busy_timeout_ms: u64,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            batch_size: 250,
            overwrite: false,
            enable_fts: true,
            journal_mode: "WAL".to_owned(),
            synchronous: "NORMAL".to_owned(),
            cache_size_kib: 65_536,
            busy_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    pub json: bool,
    pub progress_interval: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            json: false,
            progress_interval: 1_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CheckpointConfig {
    pub enabled: bool,
    pub resume: bool,
    pub name: String,
    pub every_n_entries: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            resume: true,
            name: "default".to_owned(),
            every_n_entries: 10_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    pub extraction_threads: usize,
    pub queue_capacity: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let logical = num_cpus::get().max(1);
        Self {
            extraction_threads: logical.min(8),
            queue_capacity: 2_048,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReindexConfig {
    pub auto_incremental: bool,
    pub watermark_name: String,
    pub chunk_size: usize,
}

impl Default for ReindexConfig {
    fn default() -> Self {
        Self {
            auto_incremental: true,
            watermark_name: "default".to_owned(),
            chunk_size: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ExportConfig {
    pub pretty: bool,
    pub include_raw_html: bool,
    pub json_lines: bool,
    pub batch_size: usize,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            pretty: false,
            include_raw_html: false,
            json_lines: true,
            batch_size: 2_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReleaseConfig {
    pub artifact_dir: PathBuf,
    pub sample_db_name: String,
}

impl Default for ReleaseConfig {
    fn default() -> Self {
        Self {
            artifact_dir: PathBuf::from("dist"),
            sample_db_name: "wiktionary_sample.sqlite".to_owned(),
        }
    }
}
