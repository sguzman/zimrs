use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use postgres::NoTls;
use postgres::error::SqlState;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tracing::{debug, info, warn};

use crate::config::{Config, PostgresConfig, StorageBackend};
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

enum DatabaseInner {
    Sqlite(SqliteBackend),
    Postgres(PostgresBackend),
}

struct SqliteBackend {
    conn: Connection,
    enable_fts: bool,
}

struct PostgresBackend {
    pool: Pool<PostgresConnectionManager<NoTls>>,
    schema: String,
    overwrite: bool,
    enable_fts: bool,
}

pub struct Database {
    inner: DatabaseInner,
}

impl Database {
    pub fn open(config: &Config) -> Result<Self> {
        match config.backend {
            StorageBackend::Sqlite => Self::open_sqlite(config),
            StorageBackend::Postgres => Self::open_postgres(config),
        }
    }

    fn open_sqlite(config: &Config) -> Result<Self> {
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
            inner: DatabaseInner::Sqlite(SqliteBackend {
                conn,
                enable_fts: config.sqlite.enable_fts,
            }),
        })
    }

    fn open_postgres(config: &Config) -> Result<Self> {
        validate_pg_identifier(&config.postgres.database, "postgres.database")?;
        validate_pg_identifier(&config.postgres.schema, "postgres.schema")?;

        ensure_postgres_database(&config.postgres)?;

        let manager = PostgresConnectionManager::new(build_pg_config(&config.postgres)?, NoTls);
        let pool = Pool::builder()
            .max_size((config.workers.extraction_threads.max(2) as u32).min(64))
            .build(manager)
            .context("failed to create postgres connection pool")?;

        Ok(Self {
            inner: DatabaseInner::Postgres(PostgresBackend {
                pool,
                schema: config.postgres.schema.clone(),
                overwrite: config.sqlite.overwrite,
                enable_fts: config.sqlite.enable_fts,
            }),
        })
    }

    pub fn init_schema(&self) -> Result<()> {
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => migrate_connection(&sqlite.conn, sqlite.enable_fts),
            DatabaseInner::Postgres(pg) => pg_init_schema(pg),
        }
    }

    pub fn enable_fts(&self) -> bool {
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => sqlite.enable_fts,
            DatabaseInner::Postgres(pg) => pg.enable_fts,
        }
    }

    pub fn load_checkpoint(&self, name: &str) -> Result<Option<CheckpointState>> {
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => sqlite
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
                .optional()
                .map_err(Into::into),
            DatabaseInner::Postgres(pg) => {
                let mut conn = pg
                    .pool
                    .get()
                    .context("failed to checkout postgres connection")?;
                let sql = format!(
                    "SELECT last_processed_index, ingested_pages, extracted_definitions, extracted_relations FROM {} WHERE name = $1",
                    pg_table(&pg.schema, "ingestion_checkpoints")
                );
                let row = conn.query_opt(&sql, &[&name])?;
                Ok(row.map(|row| CheckpointState {
                    last_processed_index: row.get::<_, i64>(0) as u32,
                    ingested_pages: row.get::<_, i64>(1) as u64,
                    extracted_definitions: row.get::<_, i64>(2) as u64,
                    extracted_relations: row.get::<_, i64>(3) as u64,
                }))
            }
        }
    }

    pub fn save_checkpoint(&self, name: &str, state: &CheckpointState) -> Result<()> {
        let now = unix_now_ms()? as i64;
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => {
                sqlite.conn.execute(
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
            }
            DatabaseInner::Postgres(pg) => {
                let mut conn = pg
                    .pool
                    .get()
                    .context("failed to checkout postgres connection")?;
                let sql = format!(
                    r#"
                    INSERT INTO {}(
                        name,
                        last_processed_index,
                        updated_unix_ms,
                        ingested_pages,
                        extracted_definitions,
                        extracted_relations,
                        metadata_json
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, '{{}}')
                    ON CONFLICT(name) DO UPDATE SET
                        last_processed_index = EXCLUDED.last_processed_index,
                        updated_unix_ms = EXCLUDED.updated_unix_ms,
                        ingested_pages = EXCLUDED.ingested_pages,
                        extracted_definitions = EXCLUDED.extracted_definitions,
                        extracted_relations = EXCLUDED.extracted_relations
                    "#,
                    pg_table(&pg.schema, "ingestion_checkpoints")
                );
                conn.execute(
                    &sql,
                    &[
                        &name,
                        &(state.last_processed_index as i64),
                        &now,
                        &(state.ingested_pages as i64),
                        &(state.extracted_definitions as i64),
                        &(state.extracted_relations as i64),
                    ],
                )?;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => {
                sqlite.conn.execute(
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
            }
            DatabaseInner::Postgres(pg) => {
                let mut conn = pg
                    .pool
                    .get()
                    .context("failed to checkout postgres connection")?;
                let sql = format!(
                    "INSERT INTO {}(started_unix_ms, finished_unix_ms, scanned_entries, filtered_entries, ingested_pages, extracted_definitions, extracted_relations, extraction_errors) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    pg_table(&pg.schema, "ingestion_runs")
                );
                conn.execute(
                    &sql,
                    &[
                        &(started_unix_ms as i64),
                        &(finished_unix_ms as i64),
                        &(scanned_entries as i64),
                        &(filtered_entries as i64),
                        &(ingested_pages as i64),
                        &(extracted_definitions as i64),
                        &(extracted_relations as i64),
                        &(extraction_errors as i64),
                    ],
                )?;
            }
        }

        Ok(())
    }

    pub fn incremental_reindex(
        &self,
        watermark_name: &str,
        chunk_size: usize,
    ) -> Result<ReindexMetrics> {
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => {
                sqlite_incremental_reindex(sqlite, watermark_name, chunk_size)
            }
            DatabaseInner::Postgres(pg) => pg_incremental_reindex(pg, watermark_name, chunk_size),
        }
    }

    pub fn upsert_page(&self, page: &ExtractedPage) -> Result<()> {
        match &self.inner {
            DatabaseInner::Sqlite(sqlite) => {
                let tx = sqlite.conn.unchecked_transaction()?;
                upsert_page_sqlite(&tx, page, sqlite.enable_fts)?;
                tx.commit()?;
            }
            DatabaseInner::Postgres(pg) => {
                let mut conn = pg
                    .pool
                    .get()
                    .context("failed to checkout postgres connection")?;
                upsert_page_postgres(&mut conn, &pg.schema, page, pg.enable_fts)?;
            }
        }

        Ok(())
    }

    pub fn backend_name(&self) -> &'static str {
        match self.inner {
            DatabaseInner::Sqlite(_) => "sqlite",
            DatabaseInner::Postgres(_) => "postgres",
        }
    }
}

fn ensure_postgres_database(pg: &PostgresConfig) -> Result<()> {
    let connect_retries = pg.max_connection_retries.max(1);
    let backoff = Duration::from_millis(pg.retry_backoff_ms.max(1));

    for attempt in 1..=connect_retries {
        let mut target = build_pg_config(pg)?;
        target.dbname(&pg.database);

        match target.connect(NoTls) {
            Ok(_) => {
                if attempt > 1 {
                    info!(attempt, "postgres target database became reachable");
                }
                return Ok(());
            }
            Err(error) => {
                let invalid_catalog = error
                    .as_db_error()
                    .map(|db_error| db_error.code() == &SqlState::INVALID_CATALOG_NAME)
                    .unwrap_or(false);

                if invalid_catalog {
                    info!(database = %pg.database, "target postgres database missing; creating it");
                    create_postgres_database(pg)?;
                    continue;
                }

                if attempt == connect_retries {
                    return Err(error).context("failed to connect to target postgres database");
                }

                warn!(
                    attempt,
                    retries = connect_retries,
                    error = %error,
                    sleep_ms = backoff.as_millis(),
                    "postgres connection attempt failed; backing off"
                );
                thread::sleep(backoff);
            }
        }
    }

    anyhow::bail!("exhausted postgres connection retries")
}

fn create_postgres_database(pg: &PostgresConfig) -> Result<()> {
    let mut admin = build_pg_config(pg)?;
    admin.dbname("postgres");

    let mut client = admin
        .connect(NoTls)
        .context("failed to connect to postgres maintenance database")?;

    let exists = client.query_opt(
        "SELECT 1 FROM pg_database WHERE datname = $1",
        &[&pg.database],
    )?;
    if exists.is_none() {
        let sql = format!("CREATE DATABASE {}", pg_ident(&pg.database));
        client.batch_execute(&sql)?;
        info!(database = %pg.database, "created postgres database");
    }

    Ok(())
}

fn build_pg_config(pg: &PostgresConfig) -> Result<postgres::Config> {
    let mut cfg = postgres::Config::new();
    cfg.host(&pg.host);
    cfg.port(pg.port);
    cfg.user(&pg.user);
    cfg.password(&pg.password);
    cfg.connect_timeout(Duration::from_secs(pg.connect_timeout_secs.max(1)));

    match pg.sslmode.to_lowercase().as_str() {
        "disable" => {}
        other => anyhow::bail!(
            "unsupported postgres.sslmode {other}; only 'disable' is currently supported"
        ),
    }

    Ok(cfg)
}

fn pg_init_schema(pg: &PostgresBackend) -> Result<()> {
    let mut conn = pg
        .pool
        .get()
        .context("failed to checkout postgres connection")?;
    let schema_ident = pg_ident(&pg.schema);

    if pg.overwrite {
        let reset_sql = format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};",
            schema = schema_ident
        );
        conn.batch_execute(&reset_sql)?;
        info!(schema = %pg.schema, "reset postgres schema due to overwrite=true");
    } else {
        conn.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_ident};"))?;
    }

    let pages = pg_table(&pg.schema, "pages");
    let definitions = pg_table(&pg.schema, "definitions");
    let relations = pg_table(&pg.schema, "relations");
    let aliases = pg_table(&pg.schema, "lemma_aliases");
    let runs = pg_table(&pg.schema, "ingestion_runs");
    let checkpoints = pg_table(&pg.schema, "ingestion_checkpoints");
    let reindex_state = pg_table(&pg.schema, "reindex_state");

    let ddl = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {pages} (
            id BIGSERIAL PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            namespace TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            cluster_idx BIGINT,
            blob_idx BIGINT,
            redirect_url TEXT,
            content_sha256 TEXT,
            raw_html TEXT,
            plain_text TEXT,
            extraction_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            updated_at TEXT NOT NULL DEFAULT to_char(timezone('UTC', now()), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        );

        CREATE TABLE IF NOT EXISTS {definitions} (
            id BIGSERIAL PRIMARY KEY,
            page_id BIGINT NOT NULL REFERENCES {pages}(id) ON DELETE CASCADE,
            language TEXT NOT NULL,
            def_order BIGINT NOT NULL,
            definition_text TEXT NOT NULL,
            normalized_text TEXT NOT NULL DEFAULT '',
            confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            UNIQUE(page_id, language, def_order)
        );

        CREATE TABLE IF NOT EXISTS {runs} (
            id BIGSERIAL PRIMARY KEY,
            started_unix_ms BIGINT NOT NULL,
            finished_unix_ms BIGINT NOT NULL,
            scanned_entries BIGINT NOT NULL,
            filtered_entries BIGINT NOT NULL,
            ingested_pages BIGINT NOT NULL,
            extracted_definitions BIGINT NOT NULL,
            extracted_relations BIGINT NOT NULL DEFAULT 0,
            extraction_errors BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS {relations} (
            id BIGSERIAL PRIMARY KEY,
            page_id BIGINT NOT NULL REFERENCES {pages}(id) ON DELETE CASCADE,
            language TEXT NOT NULL,
            relation_type TEXT NOT NULL,
            rel_order BIGINT NOT NULL,
            source_text TEXT NOT NULL,
            target_term TEXT NOT NULL,
            normalized_target TEXT NOT NULL,
            confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            UNIQUE(page_id, language, relation_type, rel_order, target_term)
        );

        CREATE TABLE IF NOT EXISTS {aliases} (
            id BIGSERIAL PRIMARY KEY,
            page_id BIGINT NOT NULL REFERENCES {pages}(id) ON DELETE CASCADE,
            language TEXT,
            alias TEXT NOT NULL,
            normalized_alias TEXT NOT NULL,
            source TEXT NOT NULL,
            UNIQUE(page_id, language, alias, source)
        );

        CREATE TABLE IF NOT EXISTS {checkpoints} (
            name TEXT PRIMARY KEY,
            last_processed_index BIGINT NOT NULL,
            updated_unix_ms BIGINT NOT NULL,
            ingested_pages BIGINT NOT NULL,
            extracted_definitions BIGINT NOT NULL,
            extracted_relations BIGINT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{{}}'
        );

        CREATE TABLE IF NOT EXISTS {reindex_state} (
            name TEXT PRIMARY KEY,
            last_updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_pages_title ON {pages}(title);
        CREATE INDEX IF NOT EXISTS idx_pages_updated_at ON {pages}(updated_at);
        CREATE INDEX IF NOT EXISTS idx_definitions_page ON {definitions}(page_id);
        CREATE INDEX IF NOT EXISTS idx_definitions_language ON {definitions}(language);
        CREATE INDEX IF NOT EXISTS idx_definitions_norm ON {definitions}(normalized_text);
        CREATE INDEX IF NOT EXISTS idx_relations_page ON {relations}(page_id);
        CREATE INDEX IF NOT EXISTS idx_relations_type ON {relations}(relation_type);
        CREATE INDEX IF NOT EXISTS idx_relations_target ON {relations}(normalized_target);
        CREATE INDEX IF NOT EXISTS idx_aliases_page ON {aliases}(page_id);
        CREATE INDEX IF NOT EXISTS idx_aliases_norm ON {aliases}(normalized_alias);
        "#
    );

    conn.batch_execute(&ddl)?;

    if pg.enable_fts {
        let page_fts = pg_table(&pg.schema, "page_fts");
        let fts_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {page_fts} (
                page_id BIGINT PRIMARY KEY REFERENCES {pages}(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                plain_text TEXT NOT NULL,
                search_vector tsvector GENERATED ALWAYS AS (
                    to_tsvector('simple', COALESCE(title, '') || ' ' || COALESCE(plain_text, ''))
                ) STORED
            );
            CREATE INDEX IF NOT EXISTS idx_page_fts_vector ON {page_fts} USING GIN (search_vector);
            "#
        );
        conn.batch_execute(&fts_sql)?;
    }

    Ok(())
}

fn sqlite_incremental_reindex(
    sqlite: &SqliteBackend,
    watermark_name: &str,
    chunk_size: usize,
) -> Result<ReindexMetrics> {
    let mut watermark: Option<String> = sqlite
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
        let mut stmt = if watermark.is_some() {
            sqlite.conn.prepare(
                r#"
                SELECT id, title, url, COALESCE(plain_text, ''), updated_at
                FROM pages
                WHERE updated_at > ?1
                ORDER BY updated_at ASC
                LIMIT ?2
                "#,
            )?
        } else {
            sqlite.conn.prepare(
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

            if sqlite.enable_fts {
                sqlite
                    .conn
                    .execute("DELETE FROM page_fts WHERE page_id = ?1", params![page_id])?;
                sqlite.conn.execute(
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
        sqlite.conn.execute(
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

fn pg_incremental_reindex(
    pg: &PostgresBackend,
    watermark_name: &str,
    chunk_size: usize,
) -> Result<ReindexMetrics> {
    let mut conn = pg
        .pool
        .get()
        .context("failed to checkout postgres connection")?;
    let reindex_state = pg_table(&pg.schema, "reindex_state");
    let pages = pg_table(&pg.schema, "pages");
    let page_fts = pg_table(&pg.schema, "page_fts");

    let mut watermark = conn
        .query_opt(
            &format!("SELECT last_updated_at FROM {reindex_state} WHERE name = $1"),
            &[&watermark_name],
        )?
        .map(|row| row.get::<_, String>(0));

    if watermark.as_deref() == Some("") {
        watermark = None;
    }

    let mut updated_pages = 0_u64;
    let mut latest_seen = watermark.clone();

    loop {
        let rows = if let Some(current_watermark) = watermark.as_ref() {
            conn.query(
                &format!(
                    "SELECT id, title, url, COALESCE(plain_text, ''), updated_at FROM {pages} WHERE updated_at > $1 ORDER BY updated_at ASC LIMIT $2"
                ),
                &[current_watermark, &(chunk_size as i64)],
            )?
        } else {
            conn.query(
                &format!(
                    "SELECT id, title, url, COALESCE(plain_text, ''), updated_at FROM {pages} ORDER BY updated_at ASC LIMIT $1"
                ),
                &[&(chunk_size as i64)],
            )?
        };

        if rows.is_empty() {
            break;
        }

        for row in rows {
            let page_id: i64 = row.get(0);
            let title: String = row.get(1);
            let url: String = row.get(2);
            let plain_text: String = row.get(3);
            let updated_at: String = row.get(4);

            if pg.enable_fts {
                conn.execute(
                    &format!("DELETE FROM {page_fts} WHERE page_id = $1"),
                    &[&page_id],
                )?;
                conn.execute(
                    &format!(
                        "INSERT INTO {page_fts}(page_id, title, url, plain_text) VALUES ($1, $2, $3, $4) ON CONFLICT(page_id) DO UPDATE SET title=EXCLUDED.title, url=EXCLUDED.url, plain_text=EXCLUDED.plain_text"
                    ),
                    &[&page_id, &title, &url, &plain_text],
                )?;
            }

            latest_seen = Some(updated_at);
            updated_pages += 1;
        }

        watermark = latest_seen.clone();
    }

    if let Some(last_updated_at) = latest_seen.clone() {
        conn.execute(
            &format!(
                "INSERT INTO {reindex_state}(name, last_updated_at) VALUES ($1, $2) ON CONFLICT(name) DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at"
            ),
            &[&watermark_name, &last_updated_at],
        )?;
    }

    Ok(ReindexMetrics {
        updated_pages,
        watermark: latest_seen,
    })
}

fn upsert_page_sqlite(tx: &Transaction<'_>, page: &ExtractedPage, enable_fts: bool) -> Result<()> {
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
        let rows_affected = tx.execute(
            r#"
            INSERT OR IGNORE INTO relations(
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
        if rows_affected == 0 {
            debug!(
                page_url = %page.url,
                language = %relation.language,
                relation_type = %relation.relation_type,
                rel_order = relation.order_in_type,
                target_term = %relation.target_term,
                "skipped duplicate relation row"
            );
        }
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

fn upsert_page_postgres(
    conn: &mut r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    schema: &str,
    page: &ExtractedPage,
    enable_fts: bool,
) -> Result<()> {
    let pages = pg_table(schema, "pages");
    let definitions = pg_table(schema, "definitions");
    let relations = pg_table(schema, "relations");
    let aliases = pg_table(schema, "lemma_aliases");
    let page_fts = pg_table(schema, "page_fts");

    let mut tx = conn.transaction()?;

    let cluster_idx = page.cluster_idx.map(i64::from);
    let blob_idx = page.blob_idx.map(i64::from);

    let page_id: i64 = tx
        .query_one(
            &format!(
                "INSERT INTO {pages} (url, title, namespace, mime_type, cluster_idx, blob_idx, redirect_url, content_sha256, raw_html, plain_text, extraction_confidence, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,to_char(timezone('UTC', now()), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')) ON CONFLICT(url) DO UPDATE SET title=EXCLUDED.title, namespace=EXCLUDED.namespace, mime_type=EXCLUDED.mime_type, cluster_idx=EXCLUDED.cluster_idx, blob_idx=EXCLUDED.blob_idx, redirect_url=EXCLUDED.redirect_url, content_sha256=EXCLUDED.content_sha256, raw_html=EXCLUDED.raw_html, plain_text=EXCLUDED.plain_text, extraction_confidence=EXCLUDED.extraction_confidence, updated_at=to_char(timezone('UTC', now()), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') RETURNING id"
            ),
            &[
                &page.url,
                &page.title,
                &page.namespace,
                &page.mime_type,
                &cluster_idx,
                &blob_idx,
                &page.redirect_url,
                &page.content_sha256,
                &page.raw_html,
                &page.plain_text,
                &page.extraction_confidence,
            ],
        )?
        .get(0);

    tx.execute(
        &format!("DELETE FROM {definitions} WHERE page_id = $1"),
        &[&page_id],
    )?;
    tx.execute(
        &format!("DELETE FROM {relations} WHERE page_id = $1"),
        &[&page_id],
    )?;
    tx.execute(
        &format!("DELETE FROM {aliases} WHERE page_id = $1"),
        &[&page_id],
    )?;

    for definition in &page.definitions {
        tx.execute(
            &format!(
                "INSERT INTO {definitions}(page_id, language, def_order, definition_text, normalized_text, confidence) VALUES ($1,$2,$3,$4,$5,$6)"
            ),
            &[
                &page_id,
                &definition.language,
                &definition.order_in_language,
                &definition.text,
                &definition.normalized_text,
                &definition.confidence,
            ],
        )?;
    }

    for relation in &page.relations {
        let rows_affected = tx.execute(
            &format!(
                "INSERT INTO {relations}(page_id, language, relation_type, rel_order, source_text, target_term, normalized_target, confidence) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING"
            ),
            &[
                &page_id,
                &relation.language,
                &relation.relation_type,
                &relation.order_in_type,
                &relation.source_text,
                &relation.target_term,
                &relation.normalized_target,
                &relation.confidence,
            ],
        )?;
        if rows_affected == 0 {
            debug!(
                page_url = %page.url,
                language = %relation.language,
                relation_type = %relation.relation_type,
                rel_order = relation.order_in_type,
                target_term = %relation.target_term,
                "skipped duplicate relation row"
            );
        }
    }

    for alias in &page.aliases {
        tx.execute(
            &format!(
                "INSERT INTO {aliases}(page_id, language, alias, normalized_alias, source) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING"
            ),
            &[
                &page_id,
                &alias.language,
                &alias.alias,
                &alias.normalized_alias,
                &alias.source,
            ],
        )?;
    }

    if enable_fts {
        tx.execute(
            &format!("DELETE FROM {page_fts} WHERE page_id = $1"),
            &[&page_id],
        )?;
        tx.execute(
            &format!(
                "INSERT INTO {page_fts}(page_id, title, url, plain_text) VALUES ($1,$2,$3,$4) ON CONFLICT(page_id) DO UPDATE SET title=EXCLUDED.title, url=EXCLUDED.url, plain_text=EXCLUDED.plain_text"
            ),
            &[
                &page_id,
                &page.title,
                &page.url,
                &page.plain_text.as_deref().unwrap_or(""),
            ],
        )?;
    }

    tx.commit()?;
    Ok(())
}

fn pg_table(schema: &str, table: &str) -> String {
    format!("{}.{}", pg_ident(schema), pg_ident(table))
}

fn pg_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn validate_pg_identifier(value: &str, field: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
        && value
            .chars()
            .next()
            .map(|ch| ch == '_' || ch.is_ascii_alphabetic())
            .unwrap_or(false);

    if !valid {
        anyhow::bail!(
            "invalid {field} '{value}': only [A-Za-z_][A-Za-z0-9_]* identifiers are supported"
        );
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
