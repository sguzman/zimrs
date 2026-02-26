use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tracing::{debug, info, info_span, trace, warn};
use zim::{DirectoryEntry, MimeType, Target, Zim};

use crate::config::Config;
use crate::db::{Database, upsert_page};
use crate::extractor::{
    ExtractedPage, extract_definitions, html_to_plain_text, mime_type_label, namespace_code,
    sha256_hex,
};

#[derive(Debug, Default, Clone)]
pub struct RunMetrics {
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub scanned_entries: u64,
    pub filtered_entries: u64,
    pub ingested_pages: u64,
    pub extracted_definitions: u64,
    pub extraction_errors: u64,
}

impl RunMetrics {
    pub fn elapsed_ms(&self) -> u128 {
        self.finished_unix_ms.saturating_sub(self.started_unix_ms)
    }
}

#[tracing::instrument(skip(config), fields(zim = %config.input.zim_path.display(), sqlite = %config.input.sqlite_path.display()))]
pub fn run_conversion(config: &Config) -> Result<RunMetrics> {
    ensure_input_exists(&config.input.zim_path)?;

    let started_unix_ms = unix_now_ms()?;

    info!("opening zim archive");
    let zim = Zim::new(&config.input.zim_path)
        .with_context(|| format!("failed to open {}", config.input.zim_path.display()))?;

    info!(
        article_count = zim.header.article_count,
        cluster_count = zim.header.cluster_count,
        version_major = zim.header.version_major,
        version_minor = zim.header.version_minor,
        "zim header loaded"
    );

    let mut db = Database::open(config)?;
    db.init_schema()?;

    let total_articles = zim.header.article_count;
    let start = config.selection.start_index.min(total_articles);
    let max_entries = config
        .selection
        .max_entries
        .unwrap_or(total_articles.saturating_sub(start));
    let end = (u64::from(start) + u64::from(max_entries)).min(u64::from(total_articles)) as u32;

    info!(
        start_index = start,
        end_index = end,
        "starting extraction window"
    );

    let mut metrics = RunMetrics {
        started_unix_ms,
        ..RunMetrics::default()
    };

    let enable_fts = db.enable_fts();
    let batch_size = config.sqlite.batch_size.max(1) as u64;
    let progress_interval = config.logging.progress_interval.max(1);

    let mut tx = db.begin_transaction()?;

    for idx in start..end {
        metrics.scanned_entries += 1;

        let entry = match zim.get_by_url_index(idx) {
            Ok(entry) => entry,
            Err(error) => {
                metrics.extraction_errors += 1;
                warn!(entry_index = idx, error = %error, "failed to decode directory entry");
                continue;
            }
        };

        if !should_select_entry(&entry, config) {
            metrics.filtered_entries += 1;
            continue;
        }

        let span = info_span!(
            "extract_entry",
            entry_index = idx,
            title = %entry.title,
            url = %entry.url
        );
        let _span_guard = span.enter();

        let page = match extract_entry_page(&zim, entry, config) {
            Ok(Some(page)) => page,
            Ok(None) => {
                metrics.filtered_entries += 1;
                continue;
            }
            Err(error) => {
                metrics.extraction_errors += 1;
                warn!(error = %error, "entry extraction failed");
                continue;
            }
        };

        if let Err(error) = upsert_page(&tx, &page, enable_fts) {
            metrics.extraction_errors += 1;
            warn!(error = %error, "database upsert failed");
            continue;
        }

        metrics.ingested_pages += 1;
        metrics.extracted_definitions += page.definitions.len() as u64;

        if metrics.ingested_pages % batch_size == 0 {
            trace!(
                ingested_pages = metrics.ingested_pages,
                "committing batch transaction"
            );
            tx.commit()?;
            tx = db.begin_transaction()?;
        }

        if metrics.scanned_entries % progress_interval == 0 {
            info!(
                scanned_entries = metrics.scanned_entries,
                ingested_pages = metrics.ingested_pages,
                filtered_entries = metrics.filtered_entries,
                extracted_definitions = metrics.extracted_definitions,
                extraction_errors = metrics.extraction_errors,
                "progress"
            );
        }
    }

    tx.commit()?;

    metrics.finished_unix_ms = unix_now_ms()?;

    db.insert_run_metrics(
        metrics.started_unix_ms,
        metrics.finished_unix_ms,
        metrics.scanned_entries,
        metrics.filtered_entries,
        metrics.ingested_pages,
        metrics.extracted_definitions,
        metrics.extraction_errors,
    )?;

    info!(
        elapsed_ms = metrics.elapsed_ms(),
        scanned_entries = metrics.scanned_entries,
        filtered_entries = metrics.filtered_entries,
        ingested_pages = metrics.ingested_pages,
        extracted_definitions = metrics.extracted_definitions,
        extraction_errors = metrics.extraction_errors,
        "conversion complete"
    );

    Ok(metrics)
}

fn ensure_input_exists(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }

    anyhow::bail!("input zim does not exist: {}", path.display());
}

fn unix_now_ms() -> Result<u128> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?;
    Ok(value.as_millis())
}

fn should_select_entry(entry: &DirectoryEntry, config: &Config) -> bool {
    let namespace = namespace_code(entry.namespace);
    if !config.selection.include_namespaces.is_empty()
        && !config
            .selection
            .include_namespaces
            .iter()
            .any(|item| item == namespace)
    {
        debug!(namespace, title = %entry.title, "filtered by namespace");
        return false;
    }

    if config.selection.require_title && entry.title.trim().is_empty() {
        debug!(url = %entry.url, "filtered due to empty title");
        return false;
    }

    if config
        .selection
        .exclude_url_prefixes
        .iter()
        .any(|prefix| entry.url.starts_with(prefix))
    {
        debug!(url = %entry.url, "filtered by url prefix");
        return false;
    }

    if config
        .selection
        .exclude_title_prefixes
        .iter()
        .any(|prefix| entry.title.starts_with(prefix))
    {
        debug!(title = %entry.title, "filtered by title prefix");
        return false;
    }

    let mime = mime_type_label(&entry.mime_type);
    if !config.selection.include_mime_prefixes.is_empty()
        && !config
            .selection
            .include_mime_prefixes
            .iter()
            .any(|prefix| mime.starts_with(prefix))
    {
        debug!(mime, "filtered by mime type");
        return false;
    }

    true
}

#[tracing::instrument(skip(zim, config), fields(url = %entry.url, title = %entry.title))]
fn extract_entry_page(
    zim: &Zim,
    entry: DirectoryEntry,
    config: &Config,
) -> Result<Option<ExtractedPage>> {
    let namespace = namespace_code(entry.namespace).to_owned();
    let mime_type = mime_type_label(&entry.mime_type);

    match entry.target {
        Some(Target::Redirect(redirect_idx)) => {
            if config.selection.skip_redirects {
                trace!(redirect_idx, "skipping redirect entry");
                return Ok(None);
            }

            let redirect_url = zim.get_by_url_index(redirect_idx).ok().map(|e| e.url);
            let url = entry.url;
            let title = if entry.title.is_empty() {
                redirect_url.clone().unwrap_or_else(|| url.clone())
            } else {
                entry.title
            };

            Ok(Some(ExtractedPage {
                url,
                title,
                namespace,
                mime_type,
                cluster_idx: None,
                blob_idx: None,
                redirect_url,
                content_sha256: None,
                raw_html: None,
                plain_text: None,
                definitions: Vec::new(),
            }))
        }
        Some(Target::Cluster(cluster_idx, blob_idx)) => {
            trace!(cluster_idx, "loading cluster");
            let cluster = zim.get_cluster(cluster_idx)?;

            let blob = cluster.get_blob(blob_idx)?;
            let html = String::from_utf8_lossy(blob.as_ref()).into_owned();
            let plain_text = config
                .extraction
                .store_plain_text
                .then(|| html_to_plain_text(&html));
            let definitions = extract_definitions(&html, &config.extraction);
            let content_sha256 = Some(sha256_hex(&html));
            let raw_html = config.extraction.store_raw_html.then_some(html.clone());
            let url = entry.url;
            let title = if entry.title.is_empty() {
                url.clone()
            } else {
                entry.title
            };

            Ok(Some(ExtractedPage {
                url,
                title,
                namespace,
                mime_type,
                cluster_idx: Some(cluster_idx),
                blob_idx: Some(blob_idx),
                redirect_url: None,
                content_sha256,
                raw_html,
                plain_text,
                definitions,
            }))
        }
        None => {
            if matches!(
                entry.mime_type,
                MimeType::DeletedEntry | MimeType::LinkTarget
            ) {
                trace!("skipping entry without target payload");
                return Ok(None);
            }

            warn!("entry had no target payload and was skipped");
            Ok(None)
        }
    }
}
