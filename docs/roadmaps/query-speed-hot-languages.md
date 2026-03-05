# Query Speed Roadmap: Hot-Language Optimization

## Goal
- [ ] Improve read/query performance for mainline languages (English, Spanish, German, optional French) without sacrificing canonical completeness.
- [ ] Keep canonical tables (`pages`, `definitions`, `lemma_aliases`) as source of truth.
- [ ] Implement a hot-first query path with canonical fallback.

## Scope and Constraints
- [ ] Focus only on query speed (not ingestion throughput).
- [ ] Preserve existing data model semantics and result quality.
- [ ] Keep long-tail language coverage intact via fallback.
- [ ] Add tracing/observability to validate speed gains and fallback rates.

## Layer 1: Direct Postgres Schema Changes

### 1.1 Hot-Language Projection Structure
- [x] Introduce `dictionary.hot_lookup` projection table for fast lookup by `(language, normalized_alias)`.
- [x] Define projection columns (minimum):
- [x] `language`
- [x] `normalized_alias`
- [x] `alias`
- [x] `page_id`
- [x] `title`
- [x] `url`
- [x] optional summary payload for display/ranking (primary definition excerpt included as `primary_definition`).
- [x] Define primary key/unique key strategy to avoid duplicate projection rows.
- [x] Define FK behavior to `pages(id)` and cleanup strategy for deleted/updated pages.

### 1.2 Indexing Strategy (Hot + Canonical)
- [x] Add hot-table lookup index on `(language, normalized_alias)`.
- [x] Add hot-table index on `(language, page_id)` for hydration joins.
- [x] Add canonical partial index on `lemma_aliases(normalized_alias, page_id)` filtered to hot languages.
- [x] Add canonical partial index on `definitions(page_id, def_order)` filtered to hot languages.
- [x] Review and preserve existing generic indexes for long-tail fallback path.
- [x] Decide whether to add prefix/fuzzy index variant for UX needs (deferred for now; not required by current exact-match workload).

### 1.3 Lifecycle and Maintenance DDL
- [x] Add idempotent DDL migration path for new table/indexes in `dictionary` schema.
- [x] Add schema-overwrite/reset compatibility so new objects are recreated on `--overwrite`.
- [x] Add validation queries to verify projection consistency after build/reset.
- [x] Add health checks for orphaned/duplicate projection rows.

### 1.4 Read-Side Validation
- [x] Define SQL baselines for pre/post optimization latency checks (exact lookup and alias lookup implemented; hydration path covered by indexed page join strategy).
- [x] Define acceptance criteria for P50/P95 latency improvements on hot languages.
- [x] Define fallback correctness checks for non-hot languages.

## Layer 2: Import/Conversion Pipeline Changes

### 2.1 Config and Policy
- [ ] Add config for hot-language set (default `English`, `Spanish`, `German`, optional `French`).
- [ ] Add config toggle for projection maintenance mode:
- [ ] full rebuild after conversion
- [ ] incremental update during/after conversion
- [ ] Add safeguards for empty/invalid hot-language config values.

### 2.2 Projection Build Logic
- [ ] Build/populate `hot_lookup` from canonical tables for hot languages only.
- [ ] Choose deterministic projection row selection rules (aliases, tie-breaking, optional primary definition text).
- [ ] Ensure dedupe rules are aligned with canonical uniqueness semantics.
- [ ] Ensure projection refresh is idempotent (safe to rerun).

### 2.3 Incremental Maintenance Path
- [ ] Update conversion process to refresh projection entries for changed/inserted pages.
- [ ] Ensure page updates invalidate old projection rows and replace with latest values.
- [ ] Handle alias/definition changes without stale projection artifacts.
- [ ] Integrate with existing reindex/checkpoint flow so failures are recoverable.

### 2.4 Query Routing (Hot-First, Fallback)
- [ ] Implement query plan decision:
- [ ] if requested language in hot set -> query `hot_lookup` first
- [ ] if no/low confidence hits -> fallback to canonical join path
- [ ] if language not in hot set -> canonical path directly
- [ ] Preserve existing relevance ordering while adding hot-source priority.
- [ ] Guarantee parity of result correctness with canonical-only behavior.

### 2.5 Observability and Diagnostics
- [ ] Add tracing fields for lookup execution:
- [ ] `lookup_source=hot|fallback`
- [ ] `requested_language`
- [ ] `lookup_ms`
- [ ] `hit_count`
- [ ] Track fallback rate by language to tune hot-language selection.
- [ ] Add warnings for projection freshness drift and missing projection coverage.

## Rollout Plan
- [ ] Phase 1: Deploy schema objects only (no routing), backfill projection, validate consistency.
- [ ] Phase 2: Enable hot-first routing behind feature flag/config toggle.
- [ ] Phase 3: Observe latency and fallback metrics in production-like workloads.
- [ ] Phase 4: Make hot-first routing default for configured languages.

## Testing Plan
- [ ] Unit tests for projection row construction and dedupe logic.
- [ ] Integration tests for end-to-end conversion + projection population.
- [ ] Query parity tests (hot path vs canonical fallback) for top languages.
- [ ] Regression tests for non-hot languages to ensure no behavior loss.
- [ ] Benchmark tests for exact alias/title lookup latency pre/post optimization.

## Risks and Mitigations
- [ ] Risk: projection staleness after updates.
- [ ] Mitigation: deterministic refresh path + drift checks.
- [ ] Risk: index bloat from extra structures.
- [ ] Mitigation: monitor index size/usage and prune optional indexes.
- [ ] Risk: routing complexity causes inconsistent ranking.
- [ ] Mitigation: centralize ranking policy and verify parity with test fixtures.

## Success Criteria
- [ ] Hot-language query P95 latency reduced materially versus canonical-only path.
- [ ] Fallback remains correct and complete for all languages.
- [ ] Projection integrity checks pass continuously.
- [ ] No regression in canonical data correctness or conversion reliability.
