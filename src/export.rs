use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use postgres::NoTls;
use rusqlite::{Connection, params};
use serde::Serialize;

use crate::config::{Config, PostgresConfig, StorageBackend};

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub config: Config,
    pub output_path: std::path::PathBuf,
    pub pretty: bool,
    pub include_raw_html: bool,
    pub json_lines: bool,
    pub limit: Option<u64>,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ExportMetrics {
    pub exported_pages: u64,
    pub exported_definitions: u64,
    pub exported_relations: u64,
    pub exported_aliases: u64,
}

#[derive(Debug, Serialize)]
struct ExportDefinition {
    language: String,
    order: i64,
    text: String,
    normalized_text: String,
    confidence: f64,
}

#[derive(Debug, Serialize)]
struct ExportRelation {
    language: String,
    relation_type: String,
    order: i64,
    source_text: String,
    target_term: String,
    normalized_target: String,
    confidence: f64,
}

#[derive(Debug, Serialize)]
struct ExportAlias {
    language: Option<String>,
    alias: String,
    normalized_alias: String,
    source: String,
}

#[derive(Debug, Serialize)]
struct ExportPage {
    id: i64,
    url: String,
    title: String,
    namespace: String,
    mime_type: String,
    redirect_url: Option<String>,
    content_sha256: Option<String>,
    extraction_confidence: f64,
    plain_text: Option<String>,
    raw_html: Option<String>,
    definitions: Vec<ExportDefinition>,
    relations: Vec<ExportRelation>,
    aliases: Vec<ExportAlias>,
}

pub fn export_json(options: &ExportOptions) -> Result<ExportMetrics> {
    ensure_parent_dir(&options.output_path)?;

    let out = File::create(&options.output_path)
        .with_context(|| format!("failed to create {}", options.output_path.display()))?;
    let mut writer = BufWriter::new(out);
    let mut wrote_any_array_item = false;

    if !options.json_lines {
        writer.write_all(b"[")?;
    }

    let metrics = match options.config.backend {
        StorageBackend::Sqlite => {
            let conn = Connection::open(&options.config.input.sqlite_path).with_context(|| {
                format!(
                    "failed to open {}",
                    options.config.input.sqlite_path.display()
                )
            })?;
            export_from_sqlite(&conn, options, &mut writer, &mut wrote_any_array_item)?
        }
        StorageBackend::Postgres => {
            let pg_cfg = build_pg_config(&options.config.postgres)?;
            let mut client = connect_postgres_with_retry(&options.config.postgres, pg_cfg)?;
            export_from_postgres(
                &mut client,
                &options.config.postgres.schema,
                options,
                &mut writer,
                &mut wrote_any_array_item,
            )?
        }
    };

    if !options.json_lines {
        if options.pretty && wrote_any_array_item {
            writer.write_all(b"\n")?;
        }
        writer.write_all(b"]")?;
    }

    writer.flush()?;
    Ok(metrics)
}

fn export_from_sqlite(
    conn: &Connection,
    options: &ExportOptions,
    writer: &mut BufWriter<File>,
    wrote_any_array_item: &mut bool,
) -> Result<ExportMetrics> {
    let mut metrics = ExportMetrics::default();
    let mut offset = 0_u64;
    let batch_size = options.batch_size.max(1) as u64;

    loop {
        let limit = options
            .limit
            .map(|remaining| remaining.saturating_sub(offset).min(batch_size))
            .unwrap_or(batch_size);
        if limit == 0 {
            break;
        }

        let mut stmt = conn.prepare(
            r#"
            SELECT
                id,
                url,
                title,
                namespace,
                mime_type,
                redirect_url,
                content_sha256,
                extraction_confidence,
                plain_text,
                raw_html
            FROM pages
            ORDER BY id ASC
            LIMIT ?1 OFFSET ?2
            "#,
        )?;

        let mut rows = stmt.query(params![limit as i64, offset as i64])?;
        let mut batch_count = 0_u64;

        while let Some(row) = rows.next()? {
            let page_id = row.get::<_, i64>(0)?;
            let definitions = fetch_definitions_sqlite(conn, page_id)?;
            let relations = fetch_relations_sqlite(conn, page_id)?;
            let aliases = fetch_aliases_sqlite(conn, page_id)?;

            metrics.exported_definitions += definitions.len() as u64;
            metrics.exported_relations += relations.len() as u64;
            metrics.exported_aliases += aliases.len() as u64;

            let page = ExportPage {
                id: page_id,
                url: row.get(1)?,
                title: row.get(2)?,
                namespace: row.get(3)?,
                mime_type: row.get(4)?,
                redirect_url: row.get(5)?,
                content_sha256: row.get(6)?,
                extraction_confidence: row.get(7)?,
                plain_text: row.get(8)?,
                raw_html: if options.include_raw_html {
                    row.get(9)?
                } else {
                    None
                },
                definitions,
                relations,
                aliases,
            };

            write_export_page(options, writer, wrote_any_array_item, &page)?;
            metrics.exported_pages += 1;
            batch_count += 1;
        }

        if batch_count == 0 {
            break;
        }

        offset += batch_count;
    }

    Ok(metrics)
}

fn fetch_definitions_sqlite(conn: &Connection, page_id: i64) -> Result<Vec<ExportDefinition>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT language, def_order, definition_text, normalized_text, confidence
        FROM definitions
        WHERE page_id = ?1
        ORDER BY language ASC, def_order ASC
        "#,
    )?;

    let mut rows = stmt.query(params![page_id])?;
    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        out.push(ExportDefinition {
            language: row.get(0)?,
            order: row.get(1)?,
            text: row.get(2)?,
            normalized_text: row.get(3)?,
            confidence: row.get(4)?,
        });
    }

    Ok(out)
}

fn fetch_relations_sqlite(conn: &Connection, page_id: i64) -> Result<Vec<ExportRelation>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            language,
            relation_type,
            rel_order,
            source_text,
            target_term,
            normalized_target,
            confidence
        FROM relations
        WHERE page_id = ?1
        ORDER BY relation_type ASC, rel_order ASC
        "#,
    )?;

    let mut rows = stmt.query(params![page_id])?;
    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        out.push(ExportRelation {
            language: row.get(0)?,
            relation_type: row.get(1)?,
            order: row.get(2)?,
            source_text: row.get(3)?,
            target_term: row.get(4)?,
            normalized_target: row.get(5)?,
            confidence: row.get(6)?,
        });
    }

    Ok(out)
}

fn fetch_aliases_sqlite(conn: &Connection, page_id: i64) -> Result<Vec<ExportAlias>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT language, alias, normalized_alias, source
        FROM lemma_aliases
        WHERE page_id = ?1
        ORDER BY normalized_alias ASC
        "#,
    )?;

    let mut rows = stmt.query(params![page_id])?;
    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        out.push(ExportAlias {
            language: row.get(0)?,
            alias: row.get(1)?,
            normalized_alias: row.get(2)?,
            source: row.get(3)?,
        });
    }

    Ok(out)
}

fn export_from_postgres(
    client: &mut postgres::Client,
    schema: &str,
    options: &ExportOptions,
    writer: &mut BufWriter<File>,
    wrote_any_array_item: &mut bool,
) -> Result<ExportMetrics> {
    let pages = pg_table(schema, "pages");
    let definitions = pg_table(schema, "definitions");
    let relations = pg_table(schema, "relations");
    let aliases = pg_table(schema, "lemma_aliases");

    let mut metrics = ExportMetrics::default();
    let mut offset = 0_u64;
    let batch_size = options.batch_size.max(1) as u64;

    loop {
        let limit = options
            .limit
            .map(|remaining| remaining.saturating_sub(offset).min(batch_size))
            .unwrap_or(batch_size);
        if limit == 0 {
            break;
        }

        let page_rows = client.query(
            &format!(
                "SELECT id, url, title, namespace, mime_type, redirect_url, content_sha256, extraction_confidence, plain_text, raw_html FROM {pages} ORDER BY id ASC LIMIT $1 OFFSET $2"
            ),
            &[&(limit as i64), &(offset as i64)],
        )?;

        if page_rows.is_empty() {
            break;
        }

        for row in &page_rows {
            let page_id: i64 = row.get(0);
            let definitions_rows = client.query(
                &format!(
                    "SELECT language, def_order, definition_text, normalized_text, confidence FROM {definitions} WHERE page_id = $1 ORDER BY language ASC, def_order ASC"
                ),
                &[&page_id],
            )?;
            let relations_rows = client.query(
                &format!(
                    "SELECT language, relation_type, rel_order, source_text, target_term, normalized_target, confidence FROM {relations} WHERE page_id = $1 ORDER BY relation_type ASC, rel_order ASC"
                ),
                &[&page_id],
            )?;
            let aliases_rows = client.query(
                &format!(
                    "SELECT language, alias, normalized_alias, source FROM {aliases} WHERE page_id = $1 ORDER BY normalized_alias ASC"
                ),
                &[&page_id],
            )?;

            let definitions: Vec<ExportDefinition> = definitions_rows
                .into_iter()
                .map(|definition_row| ExportDefinition {
                    language: definition_row.get(0),
                    order: definition_row.get(1),
                    text: definition_row.get(2),
                    normalized_text: definition_row.get(3),
                    confidence: definition_row.get(4),
                })
                .collect();

            let relations: Vec<ExportRelation> = relations_rows
                .into_iter()
                .map(|relation_row| ExportRelation {
                    language: relation_row.get(0),
                    relation_type: relation_row.get(1),
                    order: relation_row.get(2),
                    source_text: relation_row.get(3),
                    target_term: relation_row.get(4),
                    normalized_target: relation_row.get(5),
                    confidence: relation_row.get(6),
                })
                .collect();

            let aliases: Vec<ExportAlias> = aliases_rows
                .into_iter()
                .map(|alias_row| ExportAlias {
                    language: alias_row.get(0),
                    alias: alias_row.get(1),
                    normalized_alias: alias_row.get(2),
                    source: alias_row.get(3),
                })
                .collect();

            metrics.exported_definitions += definitions.len() as u64;
            metrics.exported_relations += relations.len() as u64;
            metrics.exported_aliases += aliases.len() as u64;

            let page = ExportPage {
                id: page_id,
                url: row.get(1),
                title: row.get(2),
                namespace: row.get(3),
                mime_type: row.get(4),
                redirect_url: row.get(5),
                content_sha256: row.get(6),
                extraction_confidence: row.get(7),
                plain_text: row.get(8),
                raw_html: if options.include_raw_html {
                    row.get(9)
                } else {
                    None
                },
                definitions,
                relations,
                aliases,
            };

            write_export_page(options, writer, wrote_any_array_item, &page)?;
            metrics.exported_pages += 1;
        }

        offset += page_rows.len() as u64;
    }

    Ok(metrics)
}

fn write_export_page(
    options: &ExportOptions,
    writer: &mut BufWriter<File>,
    wrote_any_array_item: &mut bool,
    page: &ExportPage,
) -> Result<()> {
    if options.json_lines {
        if options.pretty {
            serde_json::to_writer_pretty(&mut *writer, page)?;
        } else {
            serde_json::to_writer(&mut *writer, page)?;
        }
        writer.write_all(b"\n")?;
        return Ok(());
    }

    if *wrote_any_array_item {
        if options.pretty {
            writer.write_all(b",\n")?;
        } else {
            writer.write_all(b",")?;
        }
    }

    if options.pretty {
        serde_json::to_writer_pretty(&mut *writer, page)?;
    } else {
        serde_json::to_writer(&mut *writer, page)?;
    }

    *wrote_any_array_item = true;
    Ok(())
}

fn connect_postgres_with_retry(
    pg: &PostgresConfig,
    cfg: postgres::Config,
) -> Result<postgres::Client> {
    let retries = pg.max_connection_retries.max(1);
    let backoff = Duration::from_millis(pg.retry_backoff_ms.max(1));

    for attempt in 1..=retries {
        match cfg.connect(NoTls) {
            Ok(client) => return Ok(client),
            Err(error) => {
                if attempt == retries {
                    return Err(error).context("failed to connect to postgres for export");
                }
                tracing::warn!(attempt, retries, error = %error, "retrying postgres export connection");
                thread::sleep(backoff);
            }
        }
    }

    anyhow::bail!("exhausted postgres export retries")
}

fn build_pg_config(pg: &PostgresConfig) -> Result<postgres::Config> {
    let mut cfg = postgres::Config::new();
    cfg.host(&pg.host);
    cfg.port(pg.port);
    cfg.user(&pg.user);
    cfg.password(&pg.password);
    cfg.dbname(&pg.database);
    cfg.connect_timeout(Duration::from_secs(pg.connect_timeout_secs.max(1)));

    match pg.sslmode.to_lowercase().as_str() {
        "disable" => {}
        other => anyhow::bail!(
            "unsupported postgres.sslmode {other}; only 'disable' is currently supported"
        ),
    }

    Ok(cfg)
}

fn pg_table(schema: &str, table: &str) -> String {
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }

    Ok(())
}
