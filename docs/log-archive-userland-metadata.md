# Log Archive Userland Metadata Integration

## Status
- Draft
- Target branch: `design/log-archive-userland`
- Last updated: 2026-04-27

## Related Documents
- Archive/replay plan: `docs/log-archive-v2.md`
- Pub/sub V1 plan: `docs/log-archive-pubsub-v1-plan.md`
- Historical Log v1 pattern: `historical log-messaging-pattern design` (retired on `design/log-archive-pubsub-v1`)

## Scope
Define how a high-rate userland recorder core integrates with an application-owned metadata system (for example SQLite) without putting database work in the recorder hot path.
The recorder core is pattern-neutral, but the active `design/log-archive-pubsub-v1` scope records `publish_subscribe` only.
`Log` is reserved for historical archive compatibility and is not an active adapter requirement.

This is an extension design for userland archive tooling. It does **not** replace `iceoryx2-userland/record-and-replay`.

## Normative Language
The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHALL`, `SHALL NOT`, `SHOULD`,
`SHOULD NOT`, `RECOMMENDED`, `MAY`, and `OPTIONAL` in this document are to be
interpreted as described in RFC 2119 and RFC 8174 when, and only when, they
appear in all capitals.

## Goals
- Keep recorder write path allocation- and lock-minimal.
- Keep metadata ownership application-defined.
- Emit physical locators at commit time (`segment/generation/offset/len`).
- Make replay-from-metadata a direct locator read with no extra translation step.
- Define a deterministic catch-up source when metadata sinks lag or restart.

## Non-Goals
- Embedding SQLite logic in core `iceoryx2` transport.
- Forcing one metadata schema for all users.

## Integration Model

### Data Plane
- Recorder receives committed entries from supported pattern adapters (`publish_subscribe`; `pipeline` later).
- Source sequence is optional unless a future adapter defines a stable source sequence.
- Recorder writes segment files.

### Metadata Plane
- Recorder emits `CommitRecord` batches into an async queue.
- Separate metadata worker consumes batches and persists to user-owned backend (SQLite, Postgres, etc.).
- Backpressure/drop policy on metadata queue is independent of data durability policy.

### Metadata Delivery Modes
- `DirectSink`:
- recorder enqueues `CommitRecord` batches directly to sink worker
- lowest moving parts, lowest disk overhead
- `CommitLogOnly`:
- recorder appends `CommitRecord` to `commit.idxlog`; external indexer consumes later
- strongest decoupling between data and metadata planes
- `Hybrid`:
- recorder writes to sink worker and `commit.idxlog`
- fastest query freshness plus deterministic backfill source
- Recommended default for balanced deployments: `Hybrid`.
- Recommended mode for maximum ingest throughput: `CommitLogOnly`.

### Pattern Adapter Mapping Contract
- Recorder core ingest `MUST` be pattern-neutral.
- Adapter `MUST` set `source_pattern`:
- `1` = `Log` (reserved/retired on `design/log-archive-pubsub-v1`)
- `2` = `PublishSubscribe`
- `3` = `Pipeline`
- `PublishSubscribe` and `Pipeline` adapters `MAY` set `HAS_SOURCE_SEQUENCE`; when absent they `MUST` write `sequence = 0`.
- No active V1 adapter may require `Log` source-sequence semantics.
- Adapter `MUST` provide stable source-service identity through `log_id` (stream identity field).

### Why Separate Planes
- In throughput-focused configurations, database stalls `MUST NOT` block the recorder data write path.
- Metadata can lag and catch up from recorder commit checkpoints.

### WAL vs Query Surface
- `commit.idxlog` is a metadata WAL and `MUST NOT` be treated as the primary ad-hoc query interface.
- Query-serving systems `MUST` consume/index WAL records into a query-optimized store or index.
- For near-real-time queries, deployments `SHOULD` run a continuous indexer while recording is active.
- Query responses `MUST` be bounded by `query_watermark = last_indexed_commit_ordinal`.

## Locator Identity
- For archive I/O, the canonical record locator is:
  - `(log_id, segment_id, segment_generation, file_offset)`
- `frame_len` is required to bound the read.
- `sequence` remains useful for ordering/gap checks and log-tail UX, but metadata queries do not require sequence->offset mapping.

## Commit Record Contract

```rust
#[derive(Debug, Clone)]
pub struct CommitRecord {
    pub log_id: [u8; 16],                // stable stream identity per recorder adapter instance
    pub source_pattern: u8,              // 1=Log reserved, 2=PublishSubscribe, 3=Pipeline

    // canonical archive locator
    pub segment_id: u64,
    pub segment_generation: u32,
    pub file_offset: u64,                // byte offset to frame header
    pub frame_len: u32,                  // serialized frame length

    // ordering/traceability
    pub commit_ordinal: u64,            // recorder-local monotonic commit counter
    pub sequence: u64,                   // source sequence, or 0 if unavailable
    pub commit_time_ns: u64,             // recorder commit timestamp
    pub event_time_ns: u64,              // optional source timestamp (0 if absent)

    // integrity / decoding
    pub checksum: u64,                   // if enabled in archive profile
    pub checksum_kind: u8,               // 0=None, 1=XxHash64, 2=Crc32c (default)
    pub flags: u32,                      // reserved for future use
}
```

`flags` bits:
- `0x0000_0001` = `HAS_SOURCE_SEQUENCE`

Binary encoding requirements:
- little-endian fixed-width fields
- explicit record magic/version
- record CRC over encoded bytes
- 8-byte alignment for scan speed
- forward-compatible reserved field budget
- file-level header uses `ArchiveFileHeaderV1` from `log-archive-v2.md`
- `commit.idxlog` starts at `major=1, minor=0`

Compatibility behavior:
- metadata indexer `MUST` accept only matching major
- metadata indexer `MUST` accept equal-or-lower minor
- metadata indexer `MUST` fail fast on higher minor with explicit unsupported-format error
- metadata indexer `MUST` fail fast on unknown must-understand flags
- checksum default for v2 archives `SHOULD` be `Crc32c` (`checksum_kind=2`)

`commit.idxlog` record kinds:
- `1` = `CommitRecord`
- `2` = `Checkpoint`
- `3` = `FileSeal`

`CommitRecordBinaryV1` layout (payload for kind `1`):

```rust
#[repr(C)]
pub struct CommitRecordBinaryV1 {
    pub log_id: [u8; 16],
    pub source_pattern: u8,
    pub reserved_pattern: [u8; 3],
    pub segment_id: u64,
    pub segment_generation: u32,
    pub reserved0: u32,
    pub file_offset: u64,
    pub frame_len: u32,
    pub checksum_kind: u8,
    pub reserved1: [u8; 3],
    pub checksum: u64,
    pub commit_ordinal: u64,
    pub sequence: u64,
    pub commit_time_ns: u64,
    pub event_time_ns: u64,
    pub flags: u32,
    pub reserved2: u32,
}
```

`Checkpoint` payload (kind `2`):
- `last_commit_ordinal: u64`
- `stream_offset: u64`
- `checkpoint_crc32c: u32`

`FileSeal` payload (kind `3`):
- `last_commit_ordinal: u64`
- `total_records: u64`
- `file_crc32c: u32`

## Metadata Sink API

```rust
pub trait MetadataSink: Send + 'static {
    type Error: core::fmt::Debug + Send + 'static;

    fn on_batch(&mut self, batch: &[CommitRecord]) -> Result<(), Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}
```

Recorder configuration:
- `metadata_sink: Option<Box<dyn MetadataSink>>`
- `metadata_queue_capacity: usize`
- `metadata_overflow_policy: Block | DropNewest`
- `metadata_delivery_mode: DirectSink | CommitLogOnly | Hybrid`
- `metadata_batch_max_records: usize`
- `metadata_flush_interval_ns: u64`
- `metadata_log_path: PathBuf`
- `metadata_log_roll_bytes: u64`
- `metadata_log_max_bytes: u64`
- `metadata_log_retention_policy: FollowDataRetention | Independent`
- `query_readiness_mode: IndexerBacked | CoreLocatorIndex | Unavailable`
- default: enabled async path with bounded queue and batch writes

Metadata queue semantics:
- For `DirectSink` and `Hybrid`, queue `MUST` be an in-memory bounded channel between recorder ingest and the async metadata worker.
- For `CommitLogOnly`, queue/sink worker `MUST NOT` be constructed.
- Queue element type `MUST` be `CommitRecord` (capacity is counted in records, not bytes).
- `metadata_queue_capacity` `MUST` define the maximum number of pending records waiting to be batched/flushed when queue mode is active.
- `metadata_overflow_policy = Block` means recorder-side enqueue `MUST` wait when the queue is full.
- `metadata_overflow_policy = DropNewest` means newest metadata records `MUST` be dropped when full.
- Queue behavior `MUST NOT` affect payload durability in segment files; it affects only metadata freshness/durability.
- Selecting `Block` is an explicit throughput/latency tradeoff that `MAY` backpressure recorder progress.
- Throughput-oriented `Hybrid` deployments `SHOULD` use `DropNewest` so `commit.idxlog` remains the deterministic catch-up source.

Recommended runtime defaults:
- Implementations `SHOULD` default to:
- `metadata_delivery_mode = Hybrid`
- `metadata_overflow_policy = DropNewest`
- `metadata_queue_capacity = 65536`
- `metadata_batch_max_records = 256`
- `metadata_flush_interval_ns = 100_000_000` (100 ms, or earlier when batch fills)
- `commit_idxlog_checkpoint_interval = 4096`
- `metadata_log_path = data_storage_path`
- `metadata_log_roll_bytes = 1 GiB`
- `metadata_log_max_bytes = 32 GiB`
- `metadata_log_retention_policy = FollowDataRetention`
- `query_readiness_mode = IndexerBacked`

Throughput metadata overrides:
- `metadata_delivery_mode = CommitLogOnly`
- `metadata_queue_capacity = 0` (queue disabled)
- `metadata_batch_max_records = 0` (sink batching disabled)
- `commit_idxlog_checkpoint_interval >= 16384`
- `metadata_log_roll_bytes >= 4 GiB`
- `query_readiness_mode = Unavailable` (until indexer catches up) or `CoreLocatorIndex` (if enabled)

Policy ownership:
- Core `MUST` provide safe operational defaults above.
- Applications `MAY` override all metadata sink policy values.
- Application metadata schema/content `MUST` remain fully application-owned.

Delivery mode validation:
- `DirectSink` `MUST` require `metadata_sink.is_some()`.
- `CommitLogOnly` `MUST` require `metadata_sink.is_none()` and recorder `MUST` bypass metadata queue/sink worker entirely.
- `Hybrid` `MUST` require `metadata_sink.is_some()` and commit-log writing enabled.
- `DirectSink` with `DropNewest` `MUST` be treated as lossy metadata mode and surfaced in status.
- Invalid combinations `MUST` fail recorder startup with explicit configuration error.

### Metadata Throughput Profiles
- `Balanced` (default):
- `metadata_delivery_mode = Hybrid`
- queue + sink enabled for fresher query indexing
- `metadata_overflow_policy = DropNewest`
- `Throughput` (maximum ingest throughput):
- `metadata_delivery_mode = CommitLogOnly`
- metadata sink disabled
- metadata queue path bypassed
- external indexer replays `commit.idxlog` asynchronously
- Implementations `SHOULD` expose profile selection through recorder config/CLI so throughput-first deployments do not pay sink-path overhead.

Failure ordering and durability boundaries:
- Segment append/flush is the source of truth for archived payload durability.
- `commit.idxlog` append in `CommitLogOnly`/`Hybrid` `MUST` happen only after segment frame offset/length are finalized.
- In `Async` mode, recorder `MUST` expose separate progress counters for durable segment commit and durable `commit.idxlog` commit.
- In `Sync` mode, `send()` durability fence `MUST` cover segment durability; it `MUST NOT` wait on external metadata sink I/O.
- If segment durability succeeds but metadata sink delivery fails, recorder state `MUST` become `degraded` while preserving replayability from segment + `commit.idxlog` (when enabled).
- If segment durability succeeds and `CommitLogOnly`/`Hybrid` commit-log write fails, recorder `MUST` report degraded state with explicit metadata-catch-up risk reason.
- In `CommitLogOnly`, metadata sink lag metrics are not applicable and `MUST` be reported as `N/A` (or equivalent explicit state), not as zero.

Metadata log placement and retention:
- Recorder `MUST` allow `metadata_log_path` on a separate volume from data segments.
- Metadata log stream `MUST` support rolling by size and global size cap enforcement.
- `FollowDataRetention` policy `MUST` preserve metadata coverage for all currently retained data segments.
- If metadata coverage falls behind retained data, system status `MUST` become degraded with explicit reason.

## CLI Integration (Planned)
- Metadata operational control `SHOULD` be exposed through `iox2-log-admin` subcommands.
- CLI operations `MUST` interact via recorder/admin APIs and `MUST NOT` modify DB state out-of-band.
- Initial metadata-oriented command behaviors:
- `iox2-log-admin status --service <name>`
- includes `metadata_delivery_mode`, `query_readiness_mode`, queue depth/capacity, `last_commit_ordinal`, `last_indexed_commit_ordinal`, `query_watermark`, lag, and last sink error
- `iox2-log-admin flush --service <name>`
- forces metadata worker flush and reports success/failure
- `iox2-log-admin reindex --service <name> --from-commit-ordinal <n>`
- replays `commit.idxlog` from checkpoint for metadata catch-up/rebuild
- `reindex` `MUST` be idempotent due to locator primary key constraints.

## SQLite Baseline Schema

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version           INTEGER PRIMARY KEY,
  applied_at_ns     INTEGER NOT NULL,
  description       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recorder_state (
  log_id            BLOB PRIMARY KEY,
  last_commit_ordinal INTEGER NOT NULL,
  updated_at_ns     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS indexer_state (
  log_id                    BLOB PRIMARY KEY,
  last_indexed_commit_ordinal INTEGER NOT NULL,
  query_watermark           INTEGER NOT NULL,
  updated_at_ns             INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
  log_id             BLOB NOT NULL,
  source_pattern     INTEGER NOT NULL, -- 1=Log reserved, 2=PublishSubscribe, 3=Pipeline
  segment_id         INTEGER NOT NULL,
  segment_generation INTEGER NOT NULL,
  seq_start          INTEGER NOT NULL,
  seq_end            INTEGER NOT NULL,
  created_at_ns      INTEGER NOT NULL,
  sealed_at_ns       INTEGER,
  checksum_kind      INTEGER NOT NULL,
  PRIMARY KEY (log_id, segment_id, segment_generation)
);

CREATE TABLE IF NOT EXISTS msg_locator (
  log_id             BLOB NOT NULL,
  source_pattern     INTEGER NOT NULL, -- 1=Log reserved, 2=PublishSubscribe, 3=Pipeline
  segment_id         INTEGER NOT NULL,
  segment_generation INTEGER NOT NULL,
  file_offset        INTEGER NOT NULL,
  frame_len          INTEGER NOT NULL,
  commit_ordinal     INTEGER NOT NULL,
  sequence           INTEGER NOT NULL,
  commit_time_ns     INTEGER NOT NULL,
  event_time_ns      INTEGER NOT NULL,
  checksum           INTEGER NOT NULL,
  checksum_kind      INTEGER NOT NULL,
  PRIMARY KEY (log_id, segment_id, segment_generation, file_offset)
);

CREATE UNIQUE INDEX IF NOT EXISTS msg_locator_sequence
  ON msg_locator(log_id, sequence)
  WHERE sequence > 0;

CREATE UNIQUE INDEX IF NOT EXISTS msg_locator_commit_ordinal
  ON msg_locator(log_id, commit_ordinal);

CREATE INDEX IF NOT EXISTS msg_locator_time
  ON msg_locator(log_id, event_time_ns);
```

Notes:
- Application-specific metadata `SHOULD` live in app tables keyed by the locator tuple (or by app key plus locator columns).
- `msg_locator` is sufficient for direct rematerialization.
- App metadata tables `SHOULD` store locator columns denormalized with app keys for fast replay lookup.
- `msg_locator_sequence` is a partial index and applies only when source sequence is present (`sequence > 0`).
- `indexer_state.query_watermark` defines the highest commit ordinal guaranteed queryable by the indexer-backed query surface.
- Migration scripts `MUST` update `PRAGMA user_version` and append a `schema_migrations` row.

## Commit Log Format (Proposed)

`commit.idxlog` logical stream `MUST` be append-only and replayable.

File layout:
- file header:
- magic/version
- log id
- created timestamp
- record stream:
- `RecordHeaderV1` + `CommitRecordBinary`
- `RecordHeaderV1.record_crc32c` covers full record bytes with `record_crc32c` field zeroed during calculation
- periodic checkpoint marker every `N` records with byte offset + commit ordinal
- optional trailer on clean close with final checksum
- default `N = 4096`
- logical stream `MAY` be stored as rolled files `commit-<roll_id>.idxlog`

Indexer validation checks (mandatory):
1. Indexer `MUST` validate `ArchiveFileHeaderV1` from `log-archive-v2.md`.
2. Indexer `MUST` reject unsupported `file_kind` (`CommitIdxLog` required).
3. Indexer `MUST` reject unsupported major/minor.
4. Indexer `MUST` reject non-zero must-understand bits.
5. Indexer `MUST` validate `RecordHeaderV1` and record CRC for each record.
6. Indexer `MUST` validate record kind is `1`, `2`, or `3`.
7. Indexer `MUST` validate monotonic `commit_ordinal` for kind `1`.
8. Indexer `MUST` validate locator bounds fields are non-zero where required.
9. Indexer `MUST` enforce idempotent DB insert keyed by locator primary key.

Operational behavior:
- indexer `MUST` track last applied `commit_ordinal`.
- on restart, indexer `MUST` seek to checkpoint <= last applied and scan forward.
- duplicate inserts `MUST` be ignored via primary key on locator tuple.
- on format mismatch, indexer `MUST` enter degraded state and report actionable error (`expected major/minor`, `found major/minor`).

Query watermark semantics:
- indexer-backed query services `MUST` publish both `last_commit_ordinal` and `last_indexed_commit_ordinal`.
- `query_watermark` `MUST` equal `last_indexed_commit_ordinal`.
- query execution beyond `query_watermark` `MUST` fail with explicit `NotIndexedYet` (or equivalent), not partial silent omission.
- when `CommitLogOnly` is active and no indexer is running, query readiness mode `MUST` be `Unavailable`.
- when optional core locator index is enabled, query readiness mode `MAY` be `CoreLocatorIndex` for sequence/time locator lookup without app DB metadata.

## Offset-First Resolution

Metadata query result `MUST` return locator fields directly:
- `log_id`
- `segment_id`
- `segment_generation`
- `file_offset`
- `frame_len`

Replay `MUST`:
1. Open matching segment generation.
2. `pread()` frame at `file_offset` with `frame_len`.
3. Verify checksum/sequence if enabled.

Metadata replay path `MUST NOT` require sequence->offset translation.

## Write Path (Recorder)
1. Read committed sample metadata from the active pattern adapter (`publish_subscribe`; `pipeline` later).
2. Serialize frame and append to segment file at `current_offset`.
3. Encode and append `CommitRecordBinary` to `commit.idxlog` when delivery mode requires it.
4. Emit `CommitRecord` (with exact offset/len) to metadata queue when delivery mode requires it.
5. Advance `current_offset`.

## Read Path (Replayer)
`read_at(locator)`:
1. Open segment via `(segment_id, segment_generation)`.
2. Read at `file_offset` for `frame_len`.
3. Verify sequence/checksum.
4. Return payload.

## Recovery and Catch-Up
- Recorder persists segment catalog and commit records.
- Metadata worker/indexer resumes from `last_indexed_commit_ordinal` (watermark state).
- If metadata lags, it replays commit records and re-inserts idempotently by locator primary key.

Recovery invariants:
- catch-up replay order `MUST` be strictly increasing by `commit_ordinal`.
- locator uniqueness `MUST` be enforced by DB primary key.
- no metadata row `MUST NOT` exist for a locator outside segment bounds.

## Query Workflow with SQLite
1. App writes app metadata keyed by app entity id + locator fields.
2. Query returns locator rows.
3. Replayer materializes directly from locators.
4. Optional: use `sequence` only for range scans or continuity checks.

## Planning and Tracking
- Canonical implementation plan and progress tracking are maintained in:
- `docs/log-archive-v2.md`
- This document is a companion contract/specification focused on metadata interfaces, file/schema formats, and operational invariants.
- Metadata implementation work maps primarily to:
- `log-archive-v2` Phase 5 (Metadata Integration and Tooling)
- `log-archive-v2` Phase 7 (CLI and Operations UX)
