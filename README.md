# zimrs

`zimrs` converts an English Wiktionary `.zim` archive into a queryable SQLite dictionary database and includes operational tooling for reindexing, export, benchmarking, and release artifact packaging.

## Features

- Configurable ZIM -> SQLite ingestion with namespace/MIME/prefix filters.
- Resumable checkpointing for long-running archive conversions.
- Optional parallel extraction workers.
- Nested-list-aware definition extraction and relation extraction (`synonyms`, `antonyms`, `translations`).
- Per-language normalization plugins and confidence scoring.
- Alias normalization table for search (`lemma_aliases`).
- Optional FTS5 indexing and incremental reindex command.
- JSON/JSONL export command.
- Release artifact builder with packaged sample database.

## Commands

```bash
# Convert (default command)
cargo run --release -- --config config/wiktionary.toml convert

# Incremental reindex
cargo run --release -- --config config/wiktionary.toml reindex

# Export to JSONL
cargo run --release -- --config config/wiktionary.toml export-json --output out/wiktionary.jsonl

# Build synthetic sample DB
cargo run --release -- --config config/wiktionary.toml sample-db --output out/sample.sqlite

# Build release artifacts bundle (archive + checksum + sample DB)
cargo run --release -- --config config/wiktionary.toml build-artifacts
```

Default command behavior:

```bash
cargo run --release -- --config config/wiktionary.toml
```

is equivalent to:

```bash
cargo run --release -- --config config/wiktionary.toml convert
```

## Config

Main config file: [config/wiktionary.toml](/home/admin/Code/rust/zimrs/config/wiktionary.toml)

Important sections:

- `selection`: entry filtering and extraction window.
- `checkpoint`: resumable ingestion control.
- `workers`: extraction parallelism (`extraction_threads`).
- `extraction`: parser behavior, relation toggles, normalizer mapping, confidence threshold.
- `reindex`: incremental FTS watermark policy.
- `export`: JSON output defaults.
- `release`: artifact directory and sample DB naming.

**Config as Policy**
`config/wiktionary.toml` is the policy document for the Wiktionary archive. Each `[selection]`, `[extraction]`, `[sqlite]`, `[checkpoint]`, and `[workers]` block tunes what data is pulled from the ZIM and how the SQLite build proceeds, so a single file encodes the conversion policy for that specific archive. You can add new policy variants just by cloning the TOML, pointing `--config` at the copy, and tweaking the overlays that control namespaces, relation extraction, FTS repr, and checkpoint cadence.

**Performance Tuning**
The throughput-focused defaults in `config/wiktionary.toml` bias toward fewer disk syncs and bigger batches:
`sqlite.batch_size` = 2 000 commits longer transactions, `sqlite.cache_size_kib` = 131 072 lets SQLite keep more pages in RAM, and `sqlite.busy_timeout_ms` = 30 000 gives the writers time to finish when the DB is locked. `sqlite.journal_mode` and `sqlite.synchronous` are already set to `OFF`, so the import can skip WAL/journal writes; enable the journal again only if you need crash-safety while you keep `checkpoint.enabled = true` to keep resume metadata but only persist it every 100 000 entries. `workers.extraction_threads` = 16 and a 16 384-entry queue keeps all cores busy while the pipeline streams through clusters.

**Run & Verify**
- Generate the SQLite dictionary with the conversion command. For example, on a freshly downloaded ZIM file run:
  ```bash
  cargo run --release -- \
    --config config/wiktionary.toml \
    convert \
    --max-entries 500000 \
    --extraction-threads 16 \
    --overwrite \
    --no-resume
  ```
  `--overwrite` clears `out/wiktionary.sqlite`, `--no-resume` skips the checkpoint resume path, and `--max-entries` lets you throttle the work load. Omit `--overwrite` once you want to keep the previous data or drop `--no-resume` to continue from `checkpoint.name`.

- Verify a downloaded ZIM with the new `verify-zim` subcommand before starting conversion:
  ```bash
  cargo run --release -- --config config/wiktionary.toml verify-zim
  ```
  Add `--skip-checksum` if you need a quick header/tail check without validating the checksum, or adjust `--tail-window-bytes` if the archive is very large.

**SQLite Schema**
- `pages`: canonical entry records (URL, title, namespace, MIME, SHA256, timestamps). `definitions`, `relations`, and `lemma_aliases` reference `pages(id)` to store extracted text, relation targets, and alias metadata. `ingestion_runs` and `ingestion_checkpoints` track run metrics/resume state.
- `relations` and `definitions` both carry a `language` column so every parsed sense preserves which language section it came from; `relations.relation_type` captures synonyms/antonyms/translations. `page_fts` (if enabled) provides a materialized FTS5 index for fast lookups.
- `reindex_state` remembers watermark progress for incremental reindexes.

**Language Coverage**
Wiktionary is multilingual; the importer preserves the source language for each definition/relation and can store aliases in whichever language section produced them. The default `extraction.language_normalizers` mapping seeds normalization plugins for English, French, Spanish, Japanese, and Chinese, and the `language` columns let you query how many distinct language sections made it into each run.

## SQLite schema

Core tables:

- `pages`
- `definitions`
- `relations`
- `lemma_aliases`
- `ingestion_runs`
- `ingestion_checkpoints`
- `reindex_state`
- `page_fts` (when enabled)

## Benchmarks

Suite:

```bash
cargo bench --bench extraction_bench -- --sample-size 20
```

Baseline results are tracked in [benchmarks/BASELINE.md](/home/admin/Code/rust/zimrs/benchmarks/BASELINE.md).

## Tests

```bash
cargo test
```

Real-ZIM harness (ignored by default):

```bash
cargo test harness_wiktionary_sample -- --ignored --nocapture
```

Harness behavior:

- Uses `tmp/wiktionary_en_all_nopic_2026-02.zim` by default.
- Override with `ZIMRS_TEST_ZIM=/path/to/file.zim`.
- Auto-skips when the `.zim` tail looks sparse/incomplete (common with interrupted preallocated downloads).
- As of this release, the harness also asserts that the extracted trial data contains more than 25 pages and 25 definitions so you can treat a passing `cargo test harness_wiktionary_sample -- --ignored --nocapture` as a quick success signal for a real archive.

## Release artifacts

Local artifact script:

```bash
./scripts/build_release_artifacts.sh config/wiktionary.toml
```

Output is placed in `dist/`:

- `zimrs-release.tar.gz`
- `zimrs-release.sha256`
- unpacked staging directory with binary, docs, config, and sample DB

CI workflow for artifact publication:

- [release-artifacts.yml](/home/admin/Code/rust/zimrs/.github/workflows/release-artifacts.yml)

## Notes

- The attached `tmp/wiktionary_en_all_nopic_2026-02.zim` in this workspace appears sparse/incomplete, so full extraction quality should be validated against a completed archive.
- The project uses a local patched `zim` crate under `vendor/zim` to tolerate sentinel pointer values in newer ZIM metadata.
