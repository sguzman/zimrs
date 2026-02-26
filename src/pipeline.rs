use std::path::Path;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};
use tracing::{debug, info, info_span, trace, warn};
use zim::{DirectoryEntry, MimeType, Target, Zim};

use crate::config::Config;
use crate::db::{CheckpointState, Database, upsert_page};
use crate::extractor::{
    ExtractedPage, extract_from_html, mime_type_label, namespace_code, sha256_hex,
};

#[derive(Debug, Default, Clone)]
pub struct RunMetrics {
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub scanned_entries: u64,
    pub filtered_entries: u64,
    pub ingested_pages: u64,
    pub extracted_definitions: u64,
    pub extracted_relations: u64,
    pub extraction_errors: u64,
    pub checkpoint_updates: u64,
    pub resumed_from_checkpoint: bool,
    pub checkpoint_start_index: Option<u32>,
}

impl RunMetrics {
    pub fn elapsed_ms(&self) -> u128 {
        self.finished_unix_ms.saturating_sub(self.started_unix_ms)
    }
}

#[derive(Debug, Clone)]
struct HtmlJobMeta {
    url: String,
    title: String,
    namespace: String,
    mime_type: String,
    cluster_idx: u32,
    blob_idx: u32,
}

#[derive(Debug)]
struct HtmlJob {
    entry_index: u32,
    meta: HtmlJobMeta,
    html: String,
}

#[derive(Debug)]
enum WorkerJob {
    Html(HtmlJob),
    Shutdown,
}

#[derive(Debug)]
struct WorkerResult {
    entry_index: u32,
    page: Option<ExtractedPage>,
    error: Option<String>,
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
    let mut start = config.selection.start_index.min(total_articles);
    let max_entries = config
        .selection
        .max_entries
        .unwrap_or(total_articles.saturating_sub(start));

    let mut metrics = RunMetrics {
        started_unix_ms,
        ..RunMetrics::default()
    };

    if config.checkpoint.enabled && config.checkpoint.resume {
        if let Some(checkpoint) = db.load_checkpoint(&config.checkpoint.name)? {
            let resumed_index = checkpoint.last_processed_index.saturating_add(1);
            if resumed_index > start {
                start = resumed_index.min(total_articles);
                metrics.resumed_from_checkpoint = true;
                metrics.checkpoint_start_index = Some(start);
                info!(
                    checkpoint_name = %config.checkpoint.name,
                    resumed_start_index = start,
                    "resuming from checkpoint"
                );
            }
        }
    }

    let end = (u64::from(start) + u64::from(max_entries)).min(u64::from(total_articles)) as u32;

    info!(
        start_index = start,
        end_index = end,
        extraction_threads = config.workers.extraction_threads,
        "starting extraction window"
    );

    let enable_fts = db.enable_fts();
    let batch_size = config.sqlite.batch_size.max(1) as u64;
    let progress_interval = config.logging.progress_interval.max(1);

    let mut tx = db.begin_transaction()?;
    let mut checkpoint_last_idx = start.saturating_sub(1);

    let extraction_threads = config.workers.extraction_threads.max(1);
    let queue_capacity = config.workers.queue_capacity.max(32);

    let (job_sender, result_receiver, workers) = if extraction_threads > 1 {
        let (job_tx, job_rx) = bounded::<WorkerJob>(queue_capacity);
        let (result_tx, result_rx) = bounded::<WorkerResult>(queue_capacity);

        let handles = spawn_workers(extraction_threads, job_rx, result_tx, config.clone());
        (Some(job_tx), Some(result_rx), handles)
    } else {
        (None, None, Vec::new())
    };

    let mut inflight_jobs = 0_u64;

    for idx in start..end {
        metrics.scanned_entries += 1;
        checkpoint_last_idx = idx;

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

        match entry.target {
            Some(Target::Redirect(redirect_idx)) => {
                if config.selection.skip_redirects {
                    metrics.filtered_entries += 1;
                    continue;
                }

                let redirect_url = zim.get_by_url_index(redirect_idx).ok().map(|e| e.url);
                let url = entry.url;
                let title = if entry.title.is_empty() {
                    redirect_url.clone().unwrap_or_else(|| url.clone())
                } else {
                    entry.title
                };

                let page = ExtractedPage {
                    url,
                    title,
                    namespace: namespace_code(entry.namespace).to_owned(),
                    mime_type: mime_type_label(&entry.mime_type),
                    cluster_idx: None,
                    blob_idx: None,
                    redirect_url,
                    content_sha256: None,
                    raw_html: None,
                    plain_text: None,
                    extraction_confidence: 0.0,
                    definitions: Vec::new(),
                    relations: Vec::new(),
                    aliases: Vec::new(),
                };

                persist_page(&mut tx, &page, &mut metrics, enable_fts)?;
                if metrics.ingested_pages % batch_size == 0 {
                    trace!(
                        ingested_pages = metrics.ingested_pages,
                        "committing batch transaction"
                    );
                    tx.commit()?;
                    tx = db.begin_transaction()?;
                }
            }
            Some(Target::Cluster(cluster_idx, blob_idx)) => {
                let cluster = match zim.get_cluster(cluster_idx) {
                    Ok(cluster) => cluster,
                    Err(error) => {
                        metrics.extraction_errors += 1;
                        warn!(entry_index = idx, error = %error, "failed to load cluster");
                        continue;
                    }
                };

                let blob = match cluster.get_blob(blob_idx) {
                    Ok(blob) => blob,
                    Err(error) => {
                        metrics.extraction_errors += 1;
                        warn!(entry_index = idx, error = %error, "failed to read blob");
                        continue;
                    }
                };

                let html = String::from_utf8_lossy(blob.as_ref()).into_owned();

                let fallback_url = entry.url.clone();
                let meta = HtmlJobMeta {
                    url: entry.url,
                    title: if entry.title.is_empty() {
                        fallback_url
                    } else {
                        entry.title
                    },
                    namespace: namespace_code(entry.namespace).to_owned(),
                    mime_type: mime_type_label(&entry.mime_type),
                    cluster_idx,
                    blob_idx,
                };

                if let Some(job_tx) = &job_sender {
                    inflight_jobs += 1;
                    job_tx
                        .send(WorkerJob::Html(HtmlJob {
                            entry_index: idx,
                            meta,
                            html,
                        }))
                        .with_context(|| {
                            format!(
                                "worker channel send failed at entry index {idx} (threads={extraction_threads})"
                            )
                        })?;
                } else {
                    match build_page_from_html(meta, html, config) {
                        Ok(page) => {
                            persist_page(&mut tx, &page, &mut metrics, enable_fts)?;
                            if metrics.ingested_pages % batch_size == 0 {
                                tx.commit()?;
                                tx = db.begin_transaction()?;
                            }
                        }
                        Err(error) => {
                            metrics.extraction_errors += 1;
                            warn!(entry_index = idx, error = %error, "entry extraction failed");
                        }
                    }
                }
            }
            None => {
                if matches!(
                    entry.mime_type,
                    MimeType::DeletedEntry | MimeType::LinkTarget
                ) {
                    metrics.filtered_entries += 1;
                    continue;
                }

                metrics.extraction_errors += 1;
                warn!(
                    entry_index = idx,
                    "entry had no target payload and was skipped"
                );
            }
        }

        let worker_results = collect_worker_results(&result_receiver, &mut inflight_jobs, false);
        for result in worker_results {
            if let Some(error) = result.error {
                metrics.extraction_errors += 1;
                warn!(entry_index = result.entry_index, error = %error, "worker extraction failed");
                continue;
            }

            if let Some(page) = result.page {
                persist_page(&mut tx, &page, &mut metrics, enable_fts)?;
                if metrics.ingested_pages % batch_size == 0 {
                    tx.commit()?;
                    tx = db.begin_transaction()?;
                }
            }
        }

        if config.checkpoint.enabled
            && config.checkpoint.every_n_entries > 0
            && metrics.scanned_entries % config.checkpoint.every_n_entries == 0
        {
            tx.commit()?;
            db.save_checkpoint(
                &config.checkpoint.name,
                &CheckpointState {
                    last_processed_index: checkpoint_last_idx,
                    ingested_pages: metrics.ingested_pages,
                    extracted_definitions: metrics.extracted_definitions,
                    extracted_relations: metrics.extracted_relations,
                },
            )?;
            metrics.checkpoint_updates += 1;
            tx = db.begin_transaction()?;
        }

        if metrics.scanned_entries % progress_interval == 0 {
            info!(
                scanned_entries = metrics.scanned_entries,
                ingested_pages = metrics.ingested_pages,
                filtered_entries = metrics.filtered_entries,
                extracted_definitions = metrics.extracted_definitions,
                extracted_relations = metrics.extracted_relations,
                extraction_errors = metrics.extraction_errors,
                inflight_jobs,
                "progress"
            );
        }
    }

    if let Some(job_tx) = &job_sender {
        for _ in 0..workers.len() {
            job_tx.send(WorkerJob::Shutdown)?;
        }
    }

    while inflight_jobs > 0 {
        let worker_results = collect_worker_results(&result_receiver, &mut inflight_jobs, true);
        if worker_results.is_empty() {
            break;
        }

        for result in worker_results {
            if let Some(error) = result.error {
                metrics.extraction_errors += 1;
                warn!(entry_index = result.entry_index, error = %error, "worker extraction failed");
                continue;
            }

            if let Some(page) = result.page {
                persist_page(&mut tx, &page, &mut metrics, enable_fts)?;
                if metrics.ingested_pages % batch_size == 0 {
                    tx.commit()?;
                    tx = db.begin_transaction()?;
                }
            }
        }
    }

    for handle in workers {
        if let Err(error) = handle.join() {
            warn!(?error, "worker thread join failed");
        }
    }

    tx.commit()?;

    if config.checkpoint.enabled {
        db.save_checkpoint(
            &config.checkpoint.name,
            &CheckpointState {
                last_processed_index: checkpoint_last_idx,
                ingested_pages: metrics.ingested_pages,
                extracted_definitions: metrics.extracted_definitions,
                extracted_relations: metrics.extracted_relations,
            },
        )?;
        metrics.checkpoint_updates += 1;
    }

    metrics.finished_unix_ms = unix_now_ms()?;

    db.insert_run_metrics(
        metrics.started_unix_ms,
        metrics.finished_unix_ms,
        metrics.scanned_entries,
        metrics.filtered_entries,
        metrics.ingested_pages,
        metrics.extracted_definitions,
        metrics.extracted_relations,
        metrics.extraction_errors,
    )?;

    if config.reindex.auto_incremental {
        let reindex_metrics =
            db.incremental_reindex(&config.reindex.watermark_name, config.reindex.chunk_size)?;
        info!(
            reindexed_pages = reindex_metrics.updated_pages,
            watermark = ?reindex_metrics.watermark,
            "incremental reindex complete"
        );
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
        "conversion complete"
    );

    Ok(metrics)
}

fn spawn_workers(
    extraction_threads: usize,
    receiver: Receiver<WorkerJob>,
    sender: Sender<WorkerResult>,
    config: Config,
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(extraction_threads);

    for worker_id in 0..extraction_threads {
        let rx = receiver.clone();
        let tx = sender.clone();
        let worker_config = config.clone();

        handles.push(thread::spawn(move || {
            loop {
                let Ok(job) = rx.recv() else {
                    break;
                };

                match job {
                    WorkerJob::Shutdown => break,
                    WorkerJob::Html(job) => {
                        let result = match build_page_from_html(job.meta, job.html, &worker_config)
                        {
                            Ok(page) => WorkerResult {
                                entry_index: job.entry_index,
                                page: Some(page),
                                error: None,
                            },
                            Err(error) => WorkerResult {
                                entry_index: job.entry_index,
                                page: None,
                                error: Some(error.to_string()),
                            },
                        };

                        if tx.send(result).is_err() {
                            break;
                        }
                    }
                }
            }

            debug!(worker_id, "worker finished");
        }));
    }

    handles
}

fn collect_worker_results(
    receiver: &Option<Receiver<WorkerResult>>,
    inflight_jobs: &mut u64,
    blocking: bool,
) -> Vec<WorkerResult> {
    let Some(receiver) = receiver else {
        return Vec::new();
    };

    let mut out = Vec::new();

    loop {
        let next = if blocking {
            if *inflight_jobs == 0 {
                None
            } else {
                receiver.recv().ok()
            }
        } else {
            match receiver.try_recv() {
                Ok(value) => Some(value),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
            }
        };

        let Some(result) = next else {
            break;
        };

        *inflight_jobs = inflight_jobs.saturating_sub(1);
        out.push(result);

        if blocking {
            break;
        }
    }

    out
}

fn persist_page(
    tx: &mut rusqlite::Transaction<'_>,
    page: &ExtractedPage,
    metrics: &mut RunMetrics,
    enable_fts: bool,
) -> Result<()> {
    if let Err(error) = upsert_page(tx, page, enable_fts) {
        metrics.extraction_errors += 1;
        warn!(error = %error, "database upsert failed");
        return Ok(());
    }

    metrics.ingested_pages += 1;
    metrics.extracted_definitions += page.definitions.len() as u64;
    metrics.extracted_relations += page.relations.len() as u64;

    Ok(())
}

fn build_page_from_html(meta: HtmlJobMeta, html: String, config: &Config) -> Result<ExtractedPage> {
    let content_sha256 = Some(sha256_hex(&html));
    let extraction = extract_from_html(&meta.title, &html, &config.extraction);
    let raw_html = config.extraction.store_raw_html.then_some(html);

    Ok(ExtractedPage {
        url: meta.url,
        title: meta.title,
        namespace: meta.namespace,
        mime_type: meta.mime_type,
        cluster_idx: Some(meta.cluster_idx),
        blob_idx: Some(meta.blob_idx),
        redirect_url: None,
        content_sha256,
        raw_html,
        plain_text: extraction.plain_text,
        extraction_confidence: extraction.extraction_confidence,
        definitions: extraction.definitions,
        relations: extraction.relations,
        aliases: extraction.aliases,
    })
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
