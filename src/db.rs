use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, Transaction, params};

use crate::config::Config;
use crate::extractor::ExtractedPage;

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
        self.conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

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
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

            CREATE TABLE IF NOT EXISTS definitions (
                id INTEGER PRIMARY KEY,
                page_id INTEGER NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
                language TEXT NOT NULL,
                def_order INTEGER NOT NULL,
                definition_text TEXT NOT NULL,
                UNIQUE(page_id, language, def_order)
            );

            CREATE INDEX IF NOT EXISTS idx_pages_title ON pages(title);
            CREATE INDEX IF NOT EXISTS idx_definitions_page ON definitions(page_id);
            CREATE INDEX IF NOT EXISTS idx_definitions_language ON definitions(language);

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id INTEGER PRIMARY KEY,
                started_unix_ms INTEGER NOT NULL,
                finished_unix_ms INTEGER NOT NULL,
                scanned_entries INTEGER NOT NULL,
                filtered_entries INTEGER NOT NULL,
                ingested_pages INTEGER NOT NULL,
                extracted_definitions INTEGER NOT NULL,
                extraction_errors INTEGER NOT NULL
            );
            "#,
        )?;

        if self.enable_fts {
            self.conn.execute_batch(
                r#"
                CREATE VIRTUAL TABLE IF NOT EXISTS page_fts
                USING fts5(page_id UNINDEXED, title, url, plain_text);
                "#,
            )?;
        }

        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self.conn.transaction()?)
    }

    pub fn enable_fts(&self) -> bool {
        self.enable_fts
    }

    pub fn insert_run_metrics(
        &self,
        started_unix_ms: u128,
        finished_unix_ms: u128,
        scanned_entries: u64,
        filtered_entries: u64,
        ingested_pages: u64,
        extracted_definitions: u64,
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
                extraction_errors
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                started_unix_ms as i64,
                finished_unix_ms as i64,
                scanned_entries as i64,
                filtered_entries as i64,
                ingested_pages as i64,
                extracted_definitions as i64,
                extraction_errors as i64,
            ],
        )?;

        Ok(())
    }
}

pub fn upsert_page(tx: &Transaction<'_>, page: &ExtractedPage, enable_fts: bool) -> Result<()> {
    let page_id: i64 = tx.query_row(
        r#"
        INSERT INTO pages (
            url, title, namespace, mime_type, cluster_idx, blob_idx,
            redirect_url, content_sha256, raw_html, plain_text, updated_at
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
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
        ],
        |row| row.get(0),
    )?;

    tx.execute(
        "DELETE FROM definitions WHERE page_id = ?1",
        params![page_id],
    )?;

    for definition in &page.definitions {
        tx.execute(
            r#"
            INSERT INTO definitions(page_id, language, def_order, definition_text)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                page_id,
                &definition.language,
                definition.order_in_language,
                &definition.text,
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

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    Ok(())
}
