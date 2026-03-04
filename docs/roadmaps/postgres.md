# Postgres Backend Roadmap

## Scope
- [ ] Replace default storage backend from SQLite to PostgreSQL while preserving current ingestion behavior and data model semantics.
- [ ] Keep SQLite support as an explicit compatibility mode via CLI/config flag (for example `--sqlite`).
- [ ] Keep this phase as planning only (no implementation in this document).

## Default Runtime Targets
- [ ] Set default PostgreSQL connection defaults from `tmp/docker-compose.yaml`:
- [ ] `host=127.0.0.1`
- [ ] `port=5432`
- [ ] `user=admin`
- [ ] `password=admin`
- [ ] `database=data`
- [ ] `sslmode=disable`
- [ ] Set default logical target namespace in Postgres to:
- [ ] `database = data`
- [ ] `schema = dictionary`
- [ ] Document override behavior for host/db/schema/credentials via config and CLI.

## Config and CLI Design
- [ ] Introduce backend selection in config (default `postgres`, optional `sqlite`).
- [ ] Add Postgres connection config block with explicit fields (`host`, `port`, `user`, `password`, `database`, `schema`, `sslmode`, pool/tuning).
- [ ] Add CLI overrides for backend + connection + schema (with clear precedence rules: CLI > config > defaults).
- [ ] Add compatibility flag for old behavior (candidate: `--sqlite`), including expected path handling for SQLite DB files.
- [ ] Keep existing SQLite options functional when `backend=sqlite`.

## Schema and DDL Strategy (Postgres)
- [ ] Define canonical DDL for all existing tables in schema `dictionary`:
- [ ] `pages`
- [ ] `definitions`
- [ ] `relations`
- [ ] `lemma_aliases`
- [ ] `ingestion_runs`
- [ ] `ingestion_checkpoints`
- [ ] `reindex_state`
- [ ] Postgres equivalent for search index behavior currently backed by SQLite FTS (choose `tsvector`/GIN strategy and document parity gaps).
- [ ] Ensure all uniqueness constraints and foreign keys match current SQLite semantics.
- [ ] Ensure all required indexes are created and aligned with query patterns.
- [ ] Ensure idempotent schema creation (`CREATE SCHEMA IF NOT EXISTS dictionary`, `CREATE TABLE IF NOT EXISTS ...`).

## Overwrite / Reset Behavior
- [ ] Define overwrite semantics for Postgres when `--overwrite` is set:
- [ ] Preferred: wipe only managed objects in target schema (`dictionary`) without touching other schemas.
- [ ] Candidate implementation strategy: `DROP SCHEMA dictionary CASCADE; CREATE SCHEMA dictionary;` guarded by confirmation/logging in non-automation contexts.
- [ ] Ensure overwrite also resets checkpoints/reindex state consistently.
- [ ] Define non-overwrite behavior for resume/incremental runs.

## Data Access Layer Refactor
- [ ] Introduce storage abstraction so pipeline logic remains backend-agnostic.
- [ ] Implement Postgres writer path preserving transactional batching semantics.
- [ ] Preserve current dedupe/idempotency behavior for definitions/relations/aliases.
- [ ] Keep tracing parity (or better) for all DB operations and batch commits.

## Operational Defaults and Reliability
- [ ] Add connection pooling defaults suitable for bulk ingestion.
- [ ] Add retry/backoff behavior for transient DB failures.
- [ ] Add startup checks:
- [ ] server reachable
- [ ] target database exists
- [ ] schema exists or is creatable
- [ ] role has required privileges
- [ ] Add clear error messages for auth/network/schema-permission failures.

## Migration and Compatibility Plan
- [ ] Document transition path from old SQLite-only config to new backend-aware config.
- [ ] Provide explicit examples:
- [ ] default Postgres flow
- [ ] forced SQLite flow (`--sqlite`)
- [ ] optional data migration/export-import workflow from SQLite to Postgres (if needed).
- [ ] Keep output/export/reindex commands behavior consistent regardless of backend where feasible.

## Testing and Validation
- [ ] Add integration tests for Postgres schema bootstrap and ingestion happy path.
- [ ] Add overwrite tests verifying schema reset and deterministic re-run behavior.
- [ ] Add resume/checkpoint tests in Postgres mode.
- [ ] Add backend parity tests (SQLite vs Postgres) on the same sample corpus for row-level expectations.
- [ ] Add performance smoke tests for large ingest batches against Postgres.
- [ ] Keep full build + tests required in CI before merge.

## Documentation and Rollout
- [ ] Update README/config docs with Postgres-first defaults and `data.dictionary` target.
- [ ] Document docker-compose-derived defaults and local connection assumptions.
- [ ] Add troubleshooting section (auth, permissions, schema creation, port mapping).
- [ ] Add release notes entry describing backend default switch and SQLite fallback flag.

## Open Decisions
- [ ] Choose concrete CLI naming: `--sqlite` only vs `--backend sqlite` + convenience alias.
- [ ] Choose Postgres client/migration stack (latest stable versions).
- [ ] Decide exact FTS parity target and acceptable differences from SQLite FTS.
- [ ] Decide whether to auto-create missing database `data` or require pre-provisioning.
