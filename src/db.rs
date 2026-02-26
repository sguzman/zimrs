use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::config::Config;
use crate::extractor::ExtractedPage;

const TARGET_SCHEMA_VERSION: i64 = 3;

#[derive(Debug, Clone, Default)]
pub struct CheckpointState {
    pub last_processed_index: u32,
    pub ingested_pages: u64,
    pub extracted_definitions: u64,
    pub extracted_relations: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ReindexMetrics {
    pub updated_pages: u64,
    pub watermark: Option<String>,
}

pub struct Database {
    conn: Connection,
    enable_fts: bool,
}

impl Database {
    pub fn open(config: &Config) -> Result<Self> {
        let db_path = &config.input.sqlite_path;
        ensure_parent_dir(db_path)?;

        if config.sqlite.overwrite && db_path.exists() {
            fs::remove_file(db_path)
                .with_context(|| format!("failed to remove {}", db_path.display()))?;
        }

        let conn = Connection::open(db_path)
            .with_context(|| format!("failed to open {}", db_path.display()))?;

        conn.busy_timeout(Duration::from_millis(config.sqlite.busy_timeout_ms))?;
        conn.pragma_update(None, "journal_mode", &config.sqlite.journal_mode)?;
        conn.pragma_update(None, "synchronous", &config.sqlite.synchronous)?;
        conn.pragma_update(None, "cache_size", -config.sqlite.cache_size_kib)?;

        Ok(Self {
            conn,
            enable_fts: config.sqlite.enable_fts,
        })
    }

    pub fn init_schema(&self) -> Result<()> {
        migrate_connection(&self.conn, self.enable_fts)
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.conn.transaction()?)
    }

    pub fn enable_fts(&self) -> bool {
        self.enable_fts
    }

    pub fn load_checkpoint(&self, name: &str) -> Result<Option<CheckpointState>> {
        let checkpoint = self
            .conn
            .query_row(
                r#"
                SELECT
                    last_processed_index,
                    ingested_pages,
                    extracted_definitions,
                    extracted_relations
                FROM ingestion_checkpoints
                WHERE name = ?1
                "#,
                params![name],
                |row| {
                    Ok(CheckpointState {
                        last_processed_index: row.get::<_, i64>(0)? as u32,
                        ingested_pages: row.get::<_, i64>(1)? as u64,
                        extracted_definitions: row.get::<_, i64>(2)? as u64,
                        extracted_relations: row.get::<_, i64>(3)? as u64,
                    })
                },
            )
            .optional()?;

        Ok(checkpoint)
    }

    pub fn save_checkpoint(&self, name: &str, state: &CheckpointState) -> Result<()> {
        let now = unix_now_ms()? as i64;
        self.conn.execute(
            r#"
            INSERT INTO ingestion_checkpoints(
                name,
                last_processed_index,
                updated_unix_ms,
                ingested_pages,
                extracted_definitions,
                extracted_relations,
                metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}')
            ON CONFLICT(name) DO UPDATE SET
                last_processed_index = excluded.last_processed_index,
                updated_unix_ms = excluded.updated_unix_ms,
                ingested_pages = excluded.ingested_pages,
                extracted_definitions = excluded.extracted_definitions,
                extracted_relations = excluded.extracted_relations
            "#,
            params![
                name,
                state.last_processed_index as i64,
                now,
                state.ingested_pages as i64,
                state.extracted_definitions as i64,
                state.extracted_relations as i64,
            ],
        )?;

        Ok(())
    }

    pub fn insert_run_metrics(
        &self,
        started_unix_ms: u128,
        finished_unix_ms: u128,
        scanned_entries: u64,
        filtered_entries: u64,
        ingested_pages: u64,
        extracted_definitions: u64,
        extracted_relations: u64,
        extraction_errors: u64,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO ingestion_runs(
                started_unix_ms,
                finished_unix_ms,
                scanned_entries,
                filtered_entries,
                ingested_pages,
                extracted_definitions,
                extracted_relations,
                extraction_errors
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                started_unix_ms as i64,
                finished_unix_ms as i64,
                scanned_entries as i64,
                filtered_entries as i64,
                ingested_pages as i64,
                extracted_definitions as i64,
                extracted_relations as i64,
                extraction_errors as i64,
            ],
        )?;

        Ok(())
    }

    pub fn incremental_reindex(
        &self,
        watermark_name: &str,
        chunk_size: usize,
    ) -> Result<ReindexMetrics> {
        let mut watermark: Option<String> = self
            .conn
            .query_row(
                "SELECT last_updated_at FROM reindex_state WHERE name = ?1",
                params![watermark_name],
                |row| row.get(0),
            )
            .optional()?;

        if watermark.as_deref() == Some("") {
            watermark = None;
        }

        let mut updated_pages = 0_u64;
        let mut latest_seen = watermark.clone();

        loop {
            let mut stmt = if let Some(_) = watermark {
                self.conn.prepare(
                    r#"
                    SELECT id, title, url, COALESCE(plain_text, ''), updated_at
                    FROM pages
                    WHERE updated_at > ?1
                    ORDER BY updated_at ASC
                    LIMIT ?2
                    "#,
                )?
            } else {
                self.conn.prepare(
                    r#"
                    SELECT id, title, url, COALESCE(plain_text, ''), updated_at
                    FROM pages
                    ORDER BY updated_at ASC
                    LIMIT ?1
                    "#,
                )?
            };

            let mut rows = if let Some(ref current_watermark) = watermark {
                stmt.query(params![current_watermark, chunk_size as i64])?
            } else {
                stmt.query(params![chunk_size as i64])?
            };

            let mut batch_count = 0_u64;
            while let Some(row) = rows.next()? {
                let page_id = row.get::<_, i64>(0)?;
                let title = row.get::<_, String>(1)?;
                let url = row.get::<_, String>(2)?;
                let plain_text = row.get::<_, String>(3)?;
                let updated_at = row.get::<_, String>(4)?;

                if self.enable_fts {
                    self.conn
                        .execute("DELETE FROM page_fts WHERE page_id = ?1", params![page_id])?;
                    self.conn.execute(
                        r#"
                        INSERT INTO page_fts(page_id, title, url, plain_text)
                        VALUES (?1, ?2, ?3, ?4)
                        "#,
                        params![page_id, title, url, plain_text],
                    )?;
                }

                latest_seen = Some(updated_at);
                batch_count += 1;
                updated_pages += 1;
            }

            if batch_count == 0 {
                break;
            }

            watermark = latest_seen.clone();
        }

        if let Some(last_updated_at) = latest_seen.clone() {
            self.conn.execute(
                r#"
                INSERT INTO reindex_state(name, last_updated_at)
                VALUES (?1, ?2)
                ON CONFLICT(name) DO UPDATE SET
                    last_updated_at = excluded.last_updated_at
                "#,
                params![watermark_name, last_updated_at],
            )?;
        }

        Ok(ReindexMetrics {
            updated_pages,
            watermark: latest_seen,
        })
    }
}

pub fn upsert_page(tx: &Transaction<'_>, page: &ExtractedPage, enable_fts: bool) -> Result<()> {
    let page_id: i64 = tx.query_row(
        r#"
        INSERT INTO pages (
            url, title, namespace, mime_type, cluster_idx, blob_idx,
            redirect_url, content_sha256, raw_html, plain_text,
            extraction_confidence, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        ON CONFLICT(url) DO UPDATE SET
            title = excluded.title,
            namespace = excluded.namespace,
            mime_type = excluded.mime_type,
            cluster_idx = excluded.cluster_idx,
            blob_idx = excluded.blob_idx,
            redirect_url = excluded.redirect_url,
            content_sha256 = excluded.content_sha256,
            raw_html = excluded.raw_html,
            plain_text = excluded.plain_text,
            extraction_confidence = excluded.extraction_confidence,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        RETURNING id;
        "#,
        params![
            &page.url,
            &page.title,
            &page.namespace,
            &page.mime_type,
            &page.cluster_idx,
            &page.blob_idx,
            &page.redirect_url,
            &page.content_sha256,
            &page.raw_html,
            &page.plain_text,
            page.extraction_confidence,
        ],
        |row| row.get(0),
    )?;

    tx.execute(
        "DELETE FROM definitions WHERE page_id = ?1",
        params![page_id],
    )?;
    tx.execute("DELETE FROM relations WHERE page_id = ?1", params![page_id])?;
    tx.execute(
        "DELETE FROM lemma_aliases WHERE page_id = ?1",
        params![page_id],
    )?;

    for definition in &page.definitions {
        tx.execute(
            r#"
            INSERT INTO definitions(page_id, language, def_order, definition_text, normalized_text, confidence)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                page_id,
                &definition.language,
                definition.order_in_language,
                &definition.text,
                &definition.normalized_text,
                definition.confidence,
            ],
        )?;
    }

    for relation in &page.relations {
        tx.execute(
            r#"
            INSERT INTO relations(
                page_id,
                language,
                relation_type,
                rel_order,
                source_text,
                target_term,
                normalized_target,
                confidence
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                page_id,
                &relation.language,
                &relation.relation_type,
                relation.order_in_type,
                &relation.source_text,
                &relation.target_term,
                &relation.normalized_target,
                relation.confidence,
            ],
        )?;
    }

    for alias in &page.aliases {
        tx.execute(
            r#"
            INSERT INTO lemma_aliases(page_id, language, alias, normalized_alias, source)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                page_id,
                &alias.language,
                &alias.alias,
                &alias.normalized_alias,
                &alias.source,
            ],
        )?;
    }

    if enable_fts {
        tx.execute("DELETE FROM page_fts WHERE page_id = ?1", params![page_id])?;
        tx.execute(
            r#"
            INSERT INTO page_fts(page_id, title, url, plain_text)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                page_id,
                &page.title,
                &page.url,
                page.plain_text.as_deref().unwrap_or("")
            ],
        )?;
    }

    Ok(())
}

pub fn migrate_connection(conn: &Connection, enable_fts: bool) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    let mut version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if version < 1 {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                namespace TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                cluster_idx INTEGER,
                blob_idx INTEGER,
                redirect_url TEXT,
                content_sha256 TEXT,
                raw_html TEXT,
                plain_text TEXT,
                extraction_confidence REAL NOT NULL DEFAULT 0.0,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

            CREATE TABLE IF NOT EXISTS definitions (
                id INTEGER PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                language TEXT NOT NULL,
                def_order INTEGER NOT NULL,
                definition_text TEXT NOT NULL,
                normalized_text TEXT NOT NULL DEFAULT '',
                confidence REAL NOT NULL DEFAULT 0.0,
                UNIQUE(page_id, language, def_order)
            );

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY,
                started_unix_ms INTEGER NOT NULL,
                finished_unix_ms INTEGER NOT NULL,
                scanned_entries INTEGER NOT NULL,
                filtered_entries INTEGER NOT NULL,
                ingested_pages INTEGER NOT NULL,
                extracted_definitions INTEGER NOT NULL,
                extracted_relations INTEGER NOT NULL DEFAULT 0,
                extraction_errors INTEGER NOT NULL
            );
            "#,
        )?;

        version = 1;
        conn.pragma_update(None, "user_version", version)?;
    }

    if version < 2 {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY,
                started_unix_ms INTEGER NOT NULL,
                finished_unix_ms INTEGER NOT NULL,
                scanned_entries INTEGER NOT NULL,
                filtered_entries INTEGER NOT NULL,
                ingested_pages INTEGER NOT NULL,
                extracted_definitions INTEGER NOT NULL,
                extracted_relations INTEGER NOT NULL DEFAULT 0,
                extraction_errors INTEGER NOT NULL
            );
            "#,
        )?;

        ensure_column(
            conn,
            "pages",
            "extraction_confidence",
            "REAL NOT NULL DEFAULT 0.0",
        )?;
        ensure_column(
            conn,
            "definitions",
            "normalized_text",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            conn,
            "definitions",
            "confidence",
            "REAL NOT NULL DEFAULT 0.0",
        )?;
        ensure_column(
            conn,
            "ingestion_runs",
            "extracted_relations",
            "INTEGER NOT NULL DEFAULT 0",
        )?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS relations (
                id INTEGER PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                language TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                rel_order INTEGER NOT NULL,
                source_text TEXT NOT NULL,
                target_term TEXT NOT NULL,
                normalized_target TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.0,
                UNIQUE(page_id, language, relation_type, rel_order, target_term)
            );

            CREATE TABLE IF NOT EXISTS lemma_aliases (
                id INTEGER PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                language TEXT,
                alias TEXT NOT NULL,
                normalized_alias TEXT NOT NULL,
                source TEXT NOT NULL,
                UNIQUE(page_id, language, alias, source)
            );

            CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                name TEXT PRIMARY KEY,
                last_processed_index INTEGER NOT NULL,
                updated_unix_ms INTEGER NOT NULL,
                ingested_pages INTEGER NOT NULL,
                extracted_definitions INTEGER NOT NULL,
                extracted_relations INTEGER NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS reindex_state (
                name TEXT PRIMARY KEY,
                last_updated_at TEXT NOT NULL DEFAULT ''
            );
            "#,
        )?;

        version = 2;
        conn.pragma_update(None, "user_version", version)?;
    }

    if version < 3 {
        conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_pages_title ON pages(title);
            CREATE INDEX IF NOT EXISTS idx_pages_updated_at ON pages(updated_at);
            CREATE INDEX IF NOT EXISTS idx_definitions_page ON definitions(page_id);
            CREATE INDEX IF NOT EXISTS idx_definitions_language ON definitions(language);
            CREATE INDEX IF NOT EXISTS idx_definitions_norm ON definitions(normalized_text);
            CREATE INDEX IF NOT EXISTS idx_relations_page ON relations(page_id);
            CREATE INDEX IF NOT EXISTS idx_relations_type ON relations(relation_type);
            CREATE INDEX IF NOT EXISTS idx_relations_target ON relations(normalized_target);
            CREATE INDEX IF NOT EXISTS idx_aliases_page ON lemma_aliases(page_id);
            CREATE INDEX IF NOT EXISTS idx_aliases_norm ON lemma_aliases(normalized_alias);
            "#,
        )?;

        version = 3;
        conn.pragma_update(None, "user_version", version)?;
    }

    if enable_fts {
        conn.execute_batch(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS page_fts
            USING fts5(page_id UNINDEXED, title, url, plain_text);
            "#,
        )?;
    }

    if version != TARGET_SCHEMA_VERSION {
        conn.pragma_update(None, "user_version", TARGET_SCHEMA_VERSION)?;
    }

    Ok(())
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    column_type_sql: &str,
) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let existing_name: String = row.get(1)?;
        if existing_name == column {
            return Ok(());
        }
    }

    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN {column} {column_type_sql}"),
        [],
    )?;

    Ok(())
}

fn unix_now_ms() -> Result<u128> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?;
    Ok(now.as_millis())
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn migrates_from_legacy_schema() {
        let workdir = tempdir().expect("tempdir");
        let db_path = workdir.path().join("legacy.sqlite");
        let conn = Connection::open(&db_path).expect("open db");

        conn.execute_batch(
            r#"
            CREATE TABLE pages (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                namespace TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                cluster_idx INTEGER,
                blob_idx INTEGER,
                redirect_url TEXT,
                content_sha256 TEXT,
                raw_html TEXT,
                plain_text TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE definitions (
                id INTEGER PRIMARY KEY,
                page_id INTEGER NOT NULL,
                language TEXT NOT NULL,
                def_order INTEGER NOT NULL,
                definition_text TEXT NOT NULL
            );

            PRAGMA user_version = 1;
            "#,
        )
        .expect("seed legacy schema");

        migrate_connection(&conn, true).expect("migrate");

        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("query version");
        assert_eq!(version, TARGET_SCHEMA_VERSION);

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'relations'")
            .expect("prepare");
        let has_relations = stmt
            .query_row([], |row| row.get::<_, String>(0))
            .optional()
            .expect("query")
            .is_some();
        assert!(has_relations);
    }
}
