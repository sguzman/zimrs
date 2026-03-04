# Postgres Backend Roadmap

## Scope
- [x] Replace default storage backend from SQLite to PostgreSQL while preserving ingestion semantics.
- [x] Keep SQLite support as explicit compatibility mode.
- [x] Complete this phase as implementation-backed roadmap.

## Default Runtime Targets
- [x] Set default PostgreSQL connection defaults from `tmp/docker-compose.yaml`.
- [x] `host=127.0.0.1`
- [x] `port=5432`
- [x] `user=admin`
- [x] `password=admin`
- [x] `database=data`
- [x] `sslmode=disable`
- [x] Set default logical namespace to `database=data`, `schema=dictionary`.
- [x] Support config + CLI overrides.

## Config and CLI Design
- [x] Add backend selector in config (`backend = "postgres"|"sqlite"`, default `postgres`).
- [x] Add Postgres config block with host/port/user/password/database/schema/sslmode/retry settings.
- [x] Add CLI backend overrides (`--backend`, `--sqlite`).
- [x] Add CLI Postgres connection/schema overrides (`--pg-*`).
- [x] Preserve SQLite options when `backend=sqlite`.

## Schema and DDL Strategy
- [x] Create/manage `pages`.
- [x] Create/manage `definitions`.
- [x] Create/manage `relations`.
- [x] Create/manage `lemma_aliases`.
- [x] Create/manage `ingestion_runs`.
- [x] Create/manage `ingestion_checkpoints`.
- [x] Create/manage `reindex_state`.
- [x] Implement Postgres FTS table (`page_fts`) with `tsvector` + GIN.
- [x] Preserve uniqueness + FK semantics across backends.
- [x] Preserve required indexes.
- [x] Ensure idempotent schema creation.

## Overwrite / Reset Behavior
- [x] `--overwrite` resets target Postgres schema (`DROP SCHEMA ... CASCADE; CREATE SCHEMA ...`).
- [x] Reset only managed target schema scope.
- [x] Reset checkpoints/reindex state with schema reset.
- [x] Preserve non-overwrite incremental/resume behavior.

## Data Access Layer Refactor
- [x] Introduce backend abstraction in `Database` (`Sqlite`/`Postgres`).
- [x] Implement Postgres write path with per-page transactional upsert semantics.
- [x] Preserve idempotency for duplicate relation/alias paths.
- [x] Keep tracing on DB startup/write failures/retries.

## Operational Defaults and Reliability
- [x] Add connection pool defaults in Postgres mode (`r2d2` with bounded max size).
- [x] Add retry/backoff for Postgres connectivity.
- [x] Add startup checks for reachability/database existence/schema provisioning.
- [x] Add clear connection/bootstrap error propagation.

## Migration and Compatibility Plan
- [x] Document transition from SQLite-only to backend-aware config.
- [x] Document default Postgres flow.
- [x] Document forced SQLite flow (`--sqlite`).
- [x] Keep export/reindex behavior available for both backends.

## Testing and Validation
- [x] Keep existing SQLite unit tests green.
- [x] Keep extraction/pipeline regression tests green.
- [x] Keep harness configured for deterministic SQLite compatibility testing.
- [x] Verify build + tests after backend overhaul.

## Documentation and Rollout
- [x] Update README to Postgres-first behavior.
- [x] Document compose-derived defaults and CLI override behavior.
- [x] Document overwrite semantics for Postgres schema reset.
- [x] Document SQLite fallback mode.

## Decisions Taken
- [x] CLI style: both `--backend sqlite` and convenience alias `--sqlite` are supported.
- [x] Postgres stack: `postgres` + `r2d2` + `r2d2_postgres`.
- [x] FTS parity target: Postgres `page_fts` with generated `tsvector` and GIN index.
- [x] Missing database behavior: auto-create target database when credentials allow.
