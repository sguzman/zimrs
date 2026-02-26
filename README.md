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
