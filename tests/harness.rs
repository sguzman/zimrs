use std::env;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use anyhow::Result;
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
#[ignore]
fn harness_wiktionary_sample() -> Result<()> {
    let default_zim = PathBuf::from("tmp/wiktionary_en_all_nopic_2026-02.zim");
    let zim_path = env::var("ZIMRS_TEST_ZIM")
        .map(PathBuf::from)
        .unwrap_or(default_zim);

    if !zim_path.exists() {
        eprintln!(
            "skipping harness: zim file not found at {}",
            zim_path.display()
        );
        return Ok(());
    }

    if looks_sparse_or_incomplete(&zim_path)? {
        eprintln!(
            "skipping harness: zim appears incomplete/sparse at {}",
            zim_path.display()
        );
        return Ok(());
    }

    let workdir = tempdir()?;
    let sqlite_path = workdir.path().join("harness.sqlite");
    let config_path = workdir.path().join("harness.toml");

    let config_toml = format!(
        r#"
[input]
zim_path = "{}"
sqlite_path = "{}"

[selection]
start_index = 0
max_entries = 400
include_namespaces = ["A"]
include_mime_prefixes = ["text/html"]
skip_redirects = true
require_title = true
exclude_url_prefixes = ["Special:"]
exclude_title_prefixes = ["Appendix:"]

[extraction]
store_raw_html = false
store_plain_text = true
parse_language_sections = true
language_allowlist = ["English"]
min_definition_chars = 10
max_definitions_per_language = 10

[sqlite]
batch_size = 100
overwrite = true
enable_fts = true
journal_mode = "WAL"
synchronous = "NORMAL"
cache_size_kib = 32768
busy_timeout_ms = 5000

[logging]
level = "info"
json = false
progress_interval = 100
"#,
        zim_path.display(),
        sqlite_path.display()
    );

    fs::write(&config_path, config_toml)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("zimrs");
    cmd.arg("--config").arg(&config_path);
    cmd.assert().success();

    let conn = Connection::open(&sqlite_path)?;
    let page_count: i64 = conn.query_row("SELECT COUNT(*) FROM pages", [], |row| row.get(0))?;
    let definition_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM definitions", [], |row| row.get(0))?;

    assert!(
        page_count > 25,
        "expected > 25 pages from sample extraction, got {page_count}"
    );
    assert!(
        definition_count > 25,
        "expected > 25 definitions from sample extraction, got {definition_count}"
    );

    Ok(())
}

fn looks_sparse_or_incomplete(path: &PathBuf) -> Result<bool> {
    let mut file = fs::File::open(path)?;
    let size = file.metadata()?.len();
    if size < 4096 {
        return Ok(true);
    }

    file.seek(SeekFrom::End(-4096))?;
    let mut buf = vec![0_u8; 4096];
    file.read_exact(&mut buf)?;
    Ok(buf.iter().all(|byte| *byte == 0))
}
