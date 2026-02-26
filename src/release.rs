use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::config::Config;
use crate::db::migrate_connection;

#[derive(Debug, Clone)]
pub struct ReleaseMetrics {
    pub artifact_dir: PathBuf,
    pub archive_path: PathBuf,
    pub sample_db_path: PathBuf,
    pub checksum_path: PathBuf,
}

pub fn create_sample_database(path: &Path) -> Result<()> {
    ensure_parent_dir(path)?;

    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("failed to remove stale sample db {}", path.display()))?;
    }

    let conn = Connection::open(path)
        .with_context(|| format!("failed to open sample db {}", path.display()))?;
    migrate_connection(&conn, true)?;

    conn.execute(
        r#"
        INSERT INTO pages(
            url,
            title,
            namespace,
            mime_type,
            cluster_idx,
            blob_idx,
            redirect_url,
            content_sha256,
            raw_html,
            plain_text,
            extraction_confidence,
            updated_at
        )
        VALUES (?1, ?2, 'A', 'text/html', 0, 0, NULL, 'sample', NULL, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        "#,
        params![
            "Sample:dictionary",
            "Sample",
            "Sample entry used for release artifact validation",
            0.95_f64
        ],
    )?;

    let page_id: i64 = conn.query_row(
        "SELECT id FROM pages WHERE url = 'Sample:dictionary'",
        [],
        |row| row.get(0),
    )?;

    conn.execute(
        r#"
        INSERT INTO definitions(page_id, language, def_order, definition_text, normalized_text, confidence)
        VALUES (?1, 'English', 0, 'A sample definition used for integration checks.', 'sample definition used for integration checks', 0.92)
        "#,
        params![page_id],
    )?;

    conn.execute(
        r#"
        INSERT INTO relations(page_id, language, relation_type, rel_order, source_text, target_term, normalized_target, confidence)
        VALUES (?1, 'English', 'synonyms', 0, 'sample, example', 'example', 'example', 0.80)
        "#,
        params![page_id],
    )?;

    conn.execute(
        r#"
        INSERT INTO lemma_aliases(page_id, language, alias, normalized_alias, source)
        VALUES (?1, 'English', 'Sample', 'sample', 'title')
        "#,
        params![page_id],
    )?;

    conn.execute(
        r#"
        INSERT INTO page_fts(page_id, title, url, plain_text)
        VALUES (?1, 'Sample', 'Sample:dictionary', 'Sample entry used for release artifact validation')
        "#,
        params![page_id],
    )?;

    Ok(())
}

pub fn build_release_artifacts(
    config: &Config,
    binary_path: &Path,
    config_path: &Path,
) -> Result<ReleaseMetrics> {
    let artifact_dir = config.release.artifact_dir.clone();
    fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("failed to create artifact dir {}", artifact_dir.display()))?;

    let package_root = artifact_dir.join("zimrs-release");
    if package_root.exists() {
        fs::remove_dir_all(&package_root)
            .with_context(|| format!("failed to reset {}", package_root.display()))?;
    }
    fs::create_dir_all(&package_root)?;

    let binary_name = binary_path
        .file_name()
        .map(|v| v.to_string_lossy().to_string())
        .unwrap_or_else(|| "zimrs".to_owned());

    fs::copy(binary_path, package_root.join(&binary_name)).with_context(|| {
        format!(
            "failed to copy binary {} into {}",
            binary_path.display(),
            package_root.display()
        )
    })?;

    if Path::new("README.md").exists() {
        fs::copy("README.md", package_root.join("README.md"))?;
    }

    if Path::new("ROADMAP.md").exists() {
        fs::copy("ROADMAP.md", package_root.join("ROADMAP.md"))?;
    }

    fs::copy(config_path, package_root.join("wiktionary.toml"))?;

    let sample_db_path = package_root.join(&config.release.sample_db_name);
    create_sample_database(&sample_db_path)?;

    let archive_path = artifact_dir.join("zimrs-release.tar.gz");
    let tar_status = Command::new("tar")
        .arg("-czf")
        .arg(&archive_path)
        .arg("-C")
        .arg(&artifact_dir)
        .arg("zimrs-release")
        .status()
        .context("failed to invoke tar for release archive")?;

    if !tar_status.success() {
        anyhow::bail!("tar failed to create release archive");
    }

    let archive_bytes = fs::read(&archive_path)?;
    let checksum = sha256_hex(&archive_bytes);
    let checksum_path = artifact_dir.join("zimrs-release.sha256");
    let mut checksum_file = fs::File::create(&checksum_path)?;
    writeln!(checksum_file, "{checksum}  {}", archive_path.display())?;

    Ok(ReleaseMetrics {
        artifact_dir,
        archive_path,
        sample_db_path,
        checksum_path,
    })
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let hash = hasher.finalize();
    format!("{hash:x}")
}
