# Log Query CLI Design (`iox2-log-query`)

## Status
- Implemented
- Last updated: 2026-04-27
- Target branch: `design/log-archive-userland`
- Depends on:
  - `docs/log-archive-v2.md`
  - `docs/log-archive-userland-metadata.md`
  - `docs/log-replay-cli.md`
- Implementation progress: Phase 0 complete, Phase 1 complete, Phase 2 complete, Phase 3 complete, Phase 4 complete, Phase 5 complete, Phase 6 complete, Phase 7 complete.

## Scope
Define a first-class query CLI for `log-archive` that:
- serves locator-first query results for replay/rematerialization workflows,
- supports UTC-time query input,
- uses incremental indexing into SQLite (or compatible sink) without full commit-log rescans,
- remains safe when querying while recording is active.

This document defines query and indexing control surfaces; it does not define replay semantics (covered by `iox2-log-replay`).

## Normative Language
The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHALL`, `SHALL NOT`, `SHOULD`,
`SHOULD NOT`, `RECOMMENDED`, `MAY`, and `OPTIONAL` in this document are to be
interpreted as described in RFC 2119 and RFC 8174 when, and only when, they
appear in all capitals.

## Goals
- Provide fast query surfaces backed by an incremental index, not ad-hoc commit-log scans.
- Make query output directly consumable by `iox2-log-replay selectors --stdin`.
- Support both epoch-ns and UTC/RFC3339 user input for time windows.
- Provide deterministic `NotIndexedYet` behavior via explicit watermarks.
- Support continuous indexing while recorder is writing.
- Support multi-stream aligned locator queries for downstream data-product generation (for example FITS).

## Non-Goals
- Replacing `commit.idxlog` as canonical metadata WAL.
- Embedding a generic SQL engine in recorder hot path.
- Building a full analytics query language in core CLI.

## Key Decisions
- Query commands read from index DB (`--db-path`) by default.
- Indexing commands read from commit-log stream (`--metadata-log-path`) and update DB incrementally.
- Query commands MUST NOT rescan `commit.idxlog` for each request.
- UTC input MUST be accepted via RFC3339 timestamps with explicit offset (`Z` allowed).

## Requirements
- `LQ-001`: A separate executable `MUST` be provided as `iox2-log-query`.
- `LQ-002`: Query surfaces `MUST` be locator-first and emit selectors consumable by `iox2-log-replay`.
- `LQ-003`: Time-window commands `MUST` support both:
- `--start-ns/--end-ns` (epoch ns),
- `--start-utc/--end-utc` (RFC3339).
- `LQ-004`: UTC inputs `MUST` be normalized to epoch ns before query execution.
- `LQ-005`: Query execution `MUST` be bounded by `query_watermark = last_indexed_commit_ordinal`.
- `LQ-006`: Requests beyond watermark `MUST` return explicit `NotIndexedYet` (exit code `3`).
- `LQ-007`: Indexing `MUST` be incremental using persisted checkpoints.
- `LQ-008`: Indexer `MUST` avoid full WAL rescans on each run.
- `LQ-009`: Indexing and query `MUST` support concurrent writer/reader operation.
- `LQ-010`: Error payloads and exit codes `MUST` be machine-readable and deterministic.
- `LQ-011`: Query CLI `MUST` provide multi-stream alignment query capability with explicit skew/fill policy controls.
- `LQ-012`: Indexing state `MUST` be schema-versioned and migration-safe.
- `LQ-013`: Query result ordering `MUST` be deterministic for every command.
- `LQ-014`: `status` output `MUST` expose stream watermark and lag state sufficient to diagnose `NotIndexedYet`.
- `LQ-015`: At most one index writer `MUST` own a DB file at a time; second writers `MUST` fail with explicit lock error.
- `LQ-016`: Indexer `MUST` continue correctly across rolled `commit-*.idxlog` files without rescanning sealed data.
- `LQ-017`: Query commands `MUST` be side-effect free (no checkpoint or watermark mutation).

## CLI Contract

### Binary
- `iox2-log-query`

### Command Groups
- `index`: controls incremental ingestion from commit-log stream.
- `query`: resolves selectors from indexed metadata.
- `status`: reports watermark/readiness and checkpoint state.

### Global Options
- `--format RON|JSON|YAML` applies to control and summary output (`status`, `index` summaries, error payloads).
- Query row streams remain NDJSON unless `--emit summary` is selected.
- `--format` does not transform NDJSON row encoding.

### Stream Identity
- Index and query rows are keyed by `stream_id`.
- `stream_id` is a stable operator-provided identity string (for example `Cam/A`, `Cam/B`).
- Single-stream commands (`locate-sequence`, `locate-range`, `locate-locator`, `locate-window`) `MUST` accept `--stream-id`.
- `align-window` accepts `--streams <id1,id2,...>`.
- Implementations `MAY` support implicit single-stream mode when DB contains exactly one stream; otherwise `--stream-id` is required.

### Proposed Commands
- `iox2-log-query index run --stream-id <id> --metadata-log-path <path> --db-path <path> [--poll-interval-ms <n>] [--batch-max-records <n>]`
- `iox2-log-query index catch-up --stream-id <id> --metadata-log-path <path> --db-path <path> [--max-records <n>] [--target current|latest]`
- `iox2-log-query status --db-path <path> [--stream-id <id>]`
- `iox2-log-query query locate-sequence --db-path <path> --stream-id <id> --at <u64>`
- `iox2-log-query query locate-range --db-path <path> --stream-id <id> --from <u64> --count <usize> [--expand-selectors]`
- `iox2-log-query query locate-locator --db-path <path> --stream-id <id> --at <segment:generation:offset:len>`
- `iox2-log-query query locate-window --db-path <path> --stream-id <id> (--start-ns <u64> --end-ns <u64> | --start-utc <rfc3339> --end-utc <rfc3339>) [--time-field event|commit]`
- `iox2-log-query query align-window --db-path <path> --streams <s1,s2,s3,s4> (--start-ns <u64> --end-ns <u64> | --start-utc <rfc3339> --end-utc <rfc3339>) [--time-field event|commit] [--mode anchor|grid] [--anchor-stream <id>] [--step-ns <u64>] [--max-skew-ns <u64>] [--fill-policy drop|null|nearest] [--require-all-streams]`

### Output
- Query commands default to NDJSON selectors:
- `{"kind":"sequence","sequence":42}`
- `{"kind":"locator","segment_id":7,"segment_generation":1,"file_offset":4096,"frame_len":1536}`
- `{"kind":"range","from":1000,"count":512}`
- `align-window` emits NDJSON aligned rows keyed by aligned timestamp with per-stream locator columns (nullable when permitted by fill policy).
- `status` returns structured summary (RON/JSON/YAML via shared `--format`).

### Deterministic Query Ordering
- `locate-sequence` returns at most one row.
- `locate-range` rows are ordered by `sequence ASC`, then `commit_ordinal ASC`.
- `locate-window` rows are ordered by selected time field, then `commit_ordinal ASC`.
- `locate-locator` returns at most one row.
- `align-window` rows are ordered by `aligned_time_ns ASC`.

### Status Output Schema
`status` output `MUST` include:
- `schema_version`
- `streams[]` entries with:
- `stream_id`
- `log_id`
- `last_commit_ordinal`
- `last_indexed_commit_ordinal`
- `lag_commits` (`last_commit_ordinal - last_indexed_commit_ordinal`)
- `updated_at_ns`
- `checkpoint.roll_file`
- `checkpoint.byte_offset`
- `aggregate.stream_count`
- `aggregate.aligned_horizon_commit_ordinal` (`min(last_indexed_commit_ordinal)` across returned streams)

### Command Semantics
- `index run`:
- long-running loop that ingests from checkpoint to end-of-log and repeats after `poll_interval_ms`.
- intended for continuous query freshness while recorder is active.
- default `poll_interval_ms` SHOULD be `100`.
- default `batch_max_records` SHOULD be `4096`.
- `index catch-up`:
- one-shot ingest command.
- default `--target current` snapshots current `last_commit_ordinal` at start and stops when reached.
- `--target latest` ingests until current EOF and exits.
- command `MUST` be idempotent and safe to run repeatedly.
- `status`:
- reports per-stream and aggregate readiness/watermarks.
- with `--stream-id`, status is single-stream.
- without `--stream-id`, status returns all streams and an aggregate aligned horizon.
- writer ownership:
- one DB writer process is allowed at a time.
- second writer `MUST` fail deterministically with `ResourceBusy`.

### `query` Command Semantics
- `locate-sequence`:
- returns `NotIndexedYet` when requested sequence exceeds indexed watermark bounds.
- returns `NotAvailable` when within watermark but missing.
- `locate-range`:
- `from` is inclusive.
- implementation `SHOULD` enforce a safety cap on emitted rows (default `100_000`) unless an explicit override is provided.
- `locate-window`:
- bounds are inclusive.
- `start == end` is valid.
- mixed UTC/ns flag pairs are invalid input.
- `locate-locator`:
- exact tuple match only (`segment_id`, `segment_generation`, `file_offset`, `frame_len`).

## UTC and Time Semantics
- UTC input parser `MUST` accept RFC3339 values with timezone offset.
- Parsed values `MUST` be converted to `u64` epoch nanoseconds.
- If `start > end`, command `MUST` fail as `InvalidInput`.
- `--time-field event` queries `event_time_ns`.
- `--time-field commit` queries `commit_time_ns`.
- Default `--time-field` is `event`.

## Incremental Indexing Model

### Why
- Full rescans of `commit.idxlog` are unacceptable for high-rate streams and live querying.

### Checkpoint State
Indexer checkpoint `MUST` persist at least:
- `last_indexed_commit_ordinal`
- `active_roll_file` (or roll id)
- `byte_offset` in that file
- `schema_version`

Checkpoint update `MUST` be atomic with the DB transaction that persists the corresponding records.

### Ingestion Loop
- Open from checkpoint.
- Read only new commit records since `(roll_file, byte_offset)`.
- Batch insert into DB.
- Commit transaction.
- Update checkpoint and watermark in same transaction.

### Recovery
- On startup, indexer resumes from checkpoint.
- If checkpoint points to rolled/deleted metadata log unexpectedly, fail with explicit recoverable error.
- Optional `--reindex` mode MAY reset checkpoint and rebuild from start when operator requests it.
- If checkpoint file is fully consumed (offset equals file size), indexer `MUST` continue with the next roll file.
- If checkpoint offset is greater than file size, indexer `MUST` fail with corruption diagnostic and `MUST NOT` silently rewind.

### Transactional Guarantees
- Batch insert and checkpoint update `MUST` happen in a single DB transaction.
- `records` upsert key `MUST` guarantee idempotency (`stream_id`, `commit_ordinal`).
- On crash between batch parse and commit, no partial watermark advancement is allowed.
- Watermark `last_indexed_commit_ordinal` `MUST` always reflect committed index rows only.

## SQLite Backend Guidance
- SQLite is RECOMMENDED for the default index backend.
- `PRAGMA journal_mode=WAL` SHOULD be used.
- Single writer (indexer), many readers (query clients) SHOULD be enforced by process model.
- Schema SHOULD include:
- `records(stream_id, commit_ordinal, sequence, event_time_ns, commit_time_ns, segment_id, segment_generation, file_offset, frame_len, source_pattern, source_service_id, source_instance_id, source_sequence, frame_checksum, log_id)`
- `indexer_state(stream_id PRIMARY KEY, log_id, last_commit_ordinal, last_indexed_commit_ordinal, roll_file, byte_offset, updated_at_ns, schema_version)`
- `schema_migrations(schema_version PRIMARY KEY, applied_at_ns, tool_version)`
- Indexes SHOULD include:
- `(stream_id, event_time_ns)`
- `(stream_id, commit_time_ns)`
- `(stream_id, sequence)`
- `(stream_id, segment_id, segment_generation, file_offset, frame_len)` unique

### Reference DDL (V1)
```sql
CREATE TABLE IF NOT EXISTS records (
  stream_id TEXT NOT NULL,
  commit_ordinal INTEGER NOT NULL,
  sequence INTEGER NOT NULL,
  event_time_ns INTEGER NOT NULL,
  commit_time_ns INTEGER NOT NULL,
  segment_id INTEGER NOT NULL,
  segment_generation INTEGER NOT NULL,
  file_offset INTEGER NOT NULL,
  frame_len INTEGER NOT NULL,
  source_pattern INTEGER NOT NULL,
  source_service_id INTEGER NOT NULL,
  source_instance_id INTEGER NOT NULL,
  source_sequence INTEGER,
  frame_checksum INTEGER NOT NULL,
  log_id BLOB NOT NULL,
  PRIMARY KEY (stream_id, commit_ordinal)
);

CREATE TABLE IF NOT EXISTS indexer_state (
  stream_id TEXT PRIMARY KEY,
  log_id BLOB NOT NULL,
  last_commit_ordinal INTEGER NOT NULL,
  last_indexed_commit_ordinal INTEGER NOT NULL,
  roll_file TEXT NOT NULL,
  byte_offset INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL,
  schema_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  schema_version INTEGER PRIMARY KEY,
  applied_at_ns INTEGER NOT NULL,
  tool_version TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_records_locator
ON records(stream_id, segment_id, segment_generation, file_offset, frame_len);
CREATE INDEX IF NOT EXISTS idx_records_event_time ON records(stream_id, event_time_ns);
CREATE INDEX IF NOT EXISTS idx_records_commit_time ON records(stream_id, commit_time_ns);
CREATE INDEX IF NOT EXISTS idx_records_sequence ON records(stream_id, sequence);
```

### Schema Compatibility and Migration
- The CLI binary `MUST` define `supported_schema_min` and `supported_schema_max`.
- If DB schema is below `supported_schema_min`, command `MUST` fail with explicit migration-required error.
- If DB schema is above `supported_schema_max`, command `MUST` fail with explicit unsupported-newer-schema error.
- `index --reindex` `MAY` rebuild into an empty DB when migration tooling is unavailable.

## Query While Recording
- Recorder writes `commit.idxlog`; indexer tails it asynchronously.
- Query freshness is bounded by `query_watermark`.
- If user asks for data beyond watermark, CLI returns `NotIndexedYet` and includes current watermark.
- Query and indexing MUST avoid lockstep coupling with recorder durability path.

For multi-stream alignment:
- Query horizon `MUST` be bounded by `min(query_watermark_stream_i)` across requested streams.
- If requested end exceeds aligned horizon, command `MUST` return `NotIndexedYet` unless an explicit partial-results mode is introduced.

## Multi-Stream Alignment Contract
- `align-window` targets locator-first cross-stream synchronization.
- Alignment `MUST` support:
- `mode=anchor` (timeline derived from anchor stream timestamps),
- `mode=grid` (uniform timeline using `step_ns`).
- `mode=anchor` requires `--anchor-stream`.
- `mode=grid` requires `--step-ns > 0`.
- `max_skew_ns` `MUST` bound nearest-match acceptance.
- `fill_policy` semantics:
- `drop`: discard alignment rows missing any required stream,
- `null`: keep row with null locator for missing stream,
- `nearest`: pick nearest record within `max_skew_ns`.
- `require_all_streams`:
- when true, rows with any unresolved stream `MUST` be rejected,
- when false, output may include null/missing stream entries according to fill policy.
- Output row `SHOULD` include:
- `aligned_time_ns`,
- per-stream locator tuple (`segment_id`, `segment_generation`, `file_offset`, `frame_len`),
- per-stream `delta_ns` from aligned time,
- per-stream status (`exact|nearest|missing`).
- alignment result bounds:
- implementation `SHOULD` enforce a hard row cap (default `1_000_000`) to avoid unbounded output.
- a user-provided `--limit` `MAY` lower this cap.

### Aligned NDJSON Schema
One aligned row per line:
```json
{
  "aligned_time_ns": 1700000000123456789,
  "time_field": "event",
  "streams": {
    "Cam/A": {
      "status": "exact",
      "delta_ns": 0,
      "locator": {"segment_id": 12, "segment_generation": 3, "file_offset": 8192, "frame_len": 4128}
    },
    "Cam/B": {
      "status": "nearest",
      "delta_ns": -700,
      "locator": {"segment_id": 9, "segment_generation": 1, "file_offset": 4096, "frame_len": 4128}
    },
    "Cam/C": {
      "status": "missing",
      "delta_ns": null,
      "locator": null
    }
  }
}
```

### Selector Emission Modes
- Query commands `SHOULD` support:
- `--emit selectors` (default): NDJSON compatible with replay selectors schema.
- `--emit aligned`: aligned row schema above (for data-product builders).
- `--emit summary`: no row output, only summary/statistics.
- `--emit selectors` mapping:
- `locate-sequence` emits one `sequence` selector.
- `locate-range` emits one `range` selector by default; `--expand-selectors` emits one locator selector per indexed row for export workflows that need exact query membership.
- `locate-window` emits locator selectors.
- `locate-locator` emits one locator selector.
- `align-window` emits aligned selector rows keyed by stream id.

## Data-Product Provenance (FITS-Oriented)
- `align-window` `SHOULD` support optional provenance expansion mode:
- `--include-provenance`
- Provenance fields `SHOULD` include:
- `stream_id`
- `log_id`
- `commit_ordinal`
- `sequence`
- `event_time_ns`
- `commit_time_ns`
- locator tuple (`segment_id`, `segment_generation`, `file_offset`, `frame_len`)
- `frame_checksum`
- Applications generating FITS headers may map these fields to per-HDU keywords or binary table extensions.
- Query CLI `MUST NOT` enforce a FITS schema, but `MUST` provide sufficient stable fields for deterministic reconstruction and audit.

## Error Model and Exit Codes
- `0`: success
- `2`: invalid input/configuration
- `3`: not available/not indexed yet
- `1`: internal/runtime failure

Error payload format:
```json
{"error_code":"NotIndexedYet","message":"requested sequence 9001 exceeds query watermark 8700"}
```

### Deterministic Error Codes
- `NotIndexedYet` `MUST` include:
- requested bound (sequence/time/locator),
- `query_watermark`,
- `last_commit_ordinal`.
- `NotAvailable` for single-stream locate commands means query is within watermark but no matching record exists.
- Mixed UTC/ns input flags or missing required mode flags `MUST` return `InvalidInput`.
- writer lock conflicts `MUST` return `ResourceBusy`.

## Operational Defaults
- `index run --poll-interval-ms`: `100`
- `index run --batch-max-records`: `4096`
- `index catch-up --target`: `current`
- `query align-window --time-field`: `event`
- `query align-window --fill-policy`: `drop`
- `query align-window --max-skew-ns`: `0` (exact match) unless specified

## Concurrency and Locking
- DB writer lock model:
- at most one indexer writer per DB file.
- concurrent second writer attempt `MUST` fail fast with explicit lock error.
- multiple query readers are supported concurrently.
- `busy_timeout` SHOULD be configurable for readers/writer retries.
- query paths `SHOULD` avoid long-lived read transactions to reduce WAL checkpoint starvation.
- `index run` `SHOULD` perform periodic passive WAL checkpoints.

## Replay Pipeline Integration
`iox2-log-query` SHOULD compose with replay directly:
```bash
iox2-log-query query locate-window \
  --db-path /data/index/cam_a.sqlite \
  --start-utc 2026-02-09T10:00:00Z \
  --end-utc   2026-02-09T10:00:01Z \
  --time-field event \
| iox2-log-replay replay \
    --storage-path /data/archive/cam_a \
    --metadata-log-path /data/meta/cam_a \
    --to publish-subscribe --service Cam/A/replay \
    selectors --stdin --selector-format ndjson
```

## Implementation Plan

### Phase 0: Contract Freeze (Completed 2026-02-09)
- Finalize command names, selector output schema, and UTC parsing contract.
- Exit criteria:
- command and output schema approved,
- no naming conflict with existing CLIs,
- all `LQ-001..LQ-017` mapped to phase-level work items.

### Phase 1: CLI Skeleton and Status (Completed 2026-02-09)
- Add `iox2-log-query` binary and command scaffolding.
- Implement `status --db-path`.
- Exit criteria:
- deterministic error payload/exit code behavior,
- status includes full stream and aggregate fields from the status schema.

### Phase 2: Incremental Indexer (Completed 2026-02-09)
- Implement `index catch-up` and `index run`.
- Persist checkpoint and watermark in DB transactionally.
- Exit criteria:
- no full rescan on repeated runs,
- restart resumes from checkpoint,
- `index catch-up --target current` and `--target latest` semantics are validated,
- rolled-file continuation behavior validated,
- single-writer lock behavior validated.

### Phase 3: Core Query Commands (Completed 2026-02-09)
- Implement `locate-sequence`, `locate-range`, `locate-locator`.
- Emit NDJSON selectors by default.
- Exit criteria:
- outputs pipe directly into `iox2-log-replay selectors --stdin`,
- query behavior matches watermark contract,
- deterministic ordering checks are covered in tests.

### Phase 4: Time-Window Queries (Completed 2026-02-09)
- Implement `locate-window` for `event` and `commit` time fields.
- Add UTC RFC3339 input parsing.
- Exit criteria:
- `--start-utc/--end-utc` and `--start-ns/--end-ns` both supported,
- invalid/mixed input combinations rejected deterministically,
- UTC parsing and timezone offsets are covered by tests.

### Phase 5: Multi-Stream Alignment (Completed 2026-02-09)
- Implement `query align-window` with `anchor` and `grid` modes.
- Implement `max_skew_ns`, `fill_policy`, and `require_all_streams`.
- Exit criteria:
- aligned output contains deterministic per-stream locator mapping and quality/status fields,
- aligned horizon logic enforces `NotIndexedYet` when end time exceeds multi-stream watermark minimum,
- aligned NDJSON schema is validated in integration tests.

### Phase 6: Concurrency and Hardening (Completed 2026-02-09)
- Validate query/index behavior while recorder writes continuously.
- Add tests for `NotIndexedYet`, checkpoint recovery, and rolled commit-log continuity.
- Exit criteria:
- CI coverage includes live-write + live-query scenario,
- no deferred correctness items for watermark behavior,
- single-writer lock behavior is tested,
- schema compatibility failure modes are tested.

### Phase 7: Docs and Traceability (Completed 2026-02-09)
- Update traceability matrix with `LQ-*` IDs.
- Add operator troubleshooting for stale watermark and checkpoint recovery.
- Exit criteria:
- docs align with implemented flags and output contracts,
- replay-pipeline examples validated,
- no deferred or TBD requirement markers remain.
