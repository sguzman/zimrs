# zimrs

`zimrs` converts an English Wiktionary `.zim` archive into a queryable SQLite database for downstream app use (Rust/Tauri, scripts, analytics, etc.).

## What it does

- Opens a ZIM archive with the Rust `zim` crate.
- Applies configurable selection policies (namespace, MIME, prefixes, sampling window).
- Extracts page payloads from clusters and stores normalized page rows.
- Optionally derives plain text and language-section definitions from Wiktionary HTML.
- Writes into SQLite with batched transactions and optional FTS5 index table.
- Emits detailed tracing logs and persists run-level ingestion metrics.

## Project layout

- `src/main.rs`: CLI entrypoint and tracing initialization.
- `src/config.rs`: TOML config model.
- `src/pipeline.rs`: extraction pipeline and filtering logic.
- `src/extractor.rs`: HTML/plain-text/definition extraction helpers.
- `src/db.rs`: SQLite schema + upsert logic.
- `config/wiktionary.toml`: default policy + tuning config.
- `tests/harness.rs`: integration-style harness for real ZIM sampling.

## Requirements

- Rust toolchain (`cargo`)
- A Wiktionary ZIM file (example in this repo context: `tmp/wiktionary_en_all_nopic_2026-02.zim`)

## Quick start

```bash
cargo run --release -- --config config/wiktionary.toml
```

Useful CLI overrides:

```bash
cargo run --release -- \
  --config config/wiktionary.toml \
  --max-entries 2000 \
  --start-index 0 \
  --overwrite \
  --log-level debug
```

## Config tuning

Edit `config/wiktionary.toml` to control behavior:

- `selection.max_entries`: sample-only runs vs full archive conversion.
- `selection.include_namespaces`: target namespace(s), usually `["A"]`.
- `selection.include_mime_prefixes`: typically `text/html` for dictionary pages.
- `extraction.language_allowlist`: keep specific language sections.
- `extraction.max_definitions_per_language`: bound extraction volume.
- `sqlite.batch_size`: transaction granularity.
- `logging.progress_interval`: progress log frequency.

## SQLite schema

Core tables:

- `pages`: one row per retained ZIM page URL.
- `definitions`: extracted definition lines grouped by language.
- `ingestion_runs`: run metrics and timestamps.
- `page_fts` (optional): FTS5 table for fast title/text lookup.

## Test harness

Default unit tests:

```bash
cargo test
```

Real-ZIM ingestion harness (ignored by default):

```bash
cargo test harness_wiktionary_sample -- --ignored --nocapture
```

Harness behavior:

- Uses `tmp/wiktionary_en_all_nopic_2026-02.zim` by default.
- You can override with `ZIMRS_TEST_ZIM=/path/to/file.zim`.
- Runs a bounded conversion sample and validates rows were ingested.
- Skips automatically when the `.zim` tail looks sparse/incomplete (common with interrupted preallocated downloads).

## Notes and limitations

- Wiktionary HTML is complex; the language/definition extraction here is intentionally heuristic and tunable.
- For very large full-archive runs, increase `cache_size_kib`, `batch_size`, and consider disabling `store_raw_html`.
- This project uses a local patched `zim` crate under `vendor/zim` to tolerate sentinel pointer values seen in newer ZIM files.
