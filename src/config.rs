use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub input: InputConfig,
    pub selection: SelectionConfig,
    pub extraction: ExtractionConfig,
    pub sqlite: SqliteConfig,
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            input: InputConfig::default(),
            selection: SelectionConfig::default(),
            extraction: ExtractionConfig::default(),
            sqlite: SqliteConfig::default(),
            logging: LoggingConfig::default(),
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
            include_namespaces: vec!["A".to_owned()],
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
    pub language_allowlist: Vec<String>,
    pub min_definition_chars: usize,
    pub max_definitions_per_language: usize,
}

impl Default for ExtractionConfig {
    fn default() -> Self {
        Self {
            store_raw_html: false,
            store_plain_text: true,
            parse_language_sections: true,
            language_allowlist: Vec::new(),
            min_definition_chars: 20,
            max_definitions_per_language: 32,
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
