# Roadmap

## Conversion core

- [x] Build configurable ZIM -> SQLite pipeline.
- [x] Add namespace/MIME/prefix filtering.
- [x] Add batch writes and resilient upsert handling.
- [x] Add resumable checkpoints for interrupted full-archive runs.
- [x] Add optional parallel extraction workers.

## Extraction quality

- [x] Add plain-text extraction from HTML.
- [x] Add heuristic language-section definition extraction.
- [x] Improve parser fidelity for nested Wiktionary lists/templates.
- [x] Add per-language normalization plugins.
- [x] Add extraction confidence scoring.

## Querying and indexing

- [x] Add FTS5 table support.
- [x] Add lemma normalization table for search aliases.
- [x] Add relation tables (synonyms/antonyms/translations).
- [x] Add incremental reindex tooling.

## Quality and validation

- [x] Add unit tests for extraction helpers.
- [x] Add real-ZIM integration harness.
- [x] Add benchmark suite and performance baselines.
- [x] Add schema migration tests.

## Packaging and UX

- [x] Add project README with usage and tuning notes.
- [x] Provide default policy config TOML.
- [x] Publish release artifacts and sample databases.
- [x] Add JSON export mode for interoperability.
