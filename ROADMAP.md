# Roadmap

## Conversion core

- [x] Build configurable ZIM -> SQLite pipeline.
- [x] Add namespace/MIME/prefix filtering.
- [x] Add batch writes and resilient upsert handling.
- [ ] Add resumable checkpoints for interrupted full-archive runs.
- [ ] Add optional parallel extraction workers.

## Extraction quality

- [x] Add plain-text extraction from HTML.
- [x] Add heuristic language-section definition extraction.
- [ ] Improve parser fidelity for nested Wiktionary lists/templates.
- [ ] Add per-language normalization plugins.
- [ ] Add extraction confidence scoring.

## Querying and indexing

- [x] Add FTS5 table support.
- [ ] Add lemma normalization table for search aliases.
- [ ] Add relation tables (synonyms/antonyms/translations).
- [ ] Add incremental reindex tooling.

## Quality and validation

- [x] Add unit tests for extraction helpers.
- [x] Add real-ZIM integration harness.
- [ ] Add benchmark suite and performance baselines.
- [ ] Add schema migration tests.

## Packaging and UX

- [x] Add project README with usage and tuning notes.
- [x] Provide default policy config TOML.
- [ ] Publish release artifacts and sample databases.
- [ ] Add JSON export mode for interoperability.
