# Log Archive and Replay Extension (V2)

## Status
- Historical draft, superseded on `design/log-archive-pubsub-v1`
- Target branch: `design/log-archive-userland`
- Last updated: 2026-04-23
- Implementation progress: Phase 0 complete, Phase 1 complete, Phase 2 complete, Phase 3 complete, Phase 4 complete, Phase 5 complete, Phase 6 complete, Phase 7 complete, Phase 8 complete
- Historical dependency: `doc/design-documents/log-messaging-pattern.md` (retired on `design/log-archive-pubsub-v1`)
- Metadata integration note: `doc/design-documents/log-archive-userland-metadata.md`
- Traceability matrix: `doc/design-documents/log-archive-v2-traceability.md`
- Canonical plan/tracking document for archive implementation phases.

## Scope
Define the historical v2 archive core and replay contracts across pattern adapters.
On `design/log-archive-pubsub-v1`, the active scope is pub/sub-only and the core `Log` messaging pattern is retired.
This archive design is not a separate messaging pattern.

## Normative Language
The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHALL`, `SHALL NOT`, `SHOULD`,
`SHOULD NOT`, `RECOMMENDED`, `MAY`, and `OPTIONAL` in this document are to be
interpreted as described in RFC 2119 and RFC 8174 when, and only when, they
appear in all capitals.

## Terminology
- **Recorder**: Optional extension that persists committed pattern entries into segment files.
- **Replayer**: Optional extension that reads persisted entries, including random access by locator and, when available, source sequence.
- **Sequence**: Source ordering id when provided by a pattern adapter.
- **Locator**: Physical storage identity: `segment_id`, `segment_generation`, `file_offset`, `frame_len`.
- **Snapshot token**: Read-consistency handle used to keep query-to-replay behavior deterministic.
- **Metadata service**: Application-owned index service (for example SQLite).
- **Pattern adapter**: Pattern-specific ingest binding that maps supported service samples (`publish_subscribe`; `pipeline` in a later update) into canonical archive records.

### Canonical Field Names
- Canonical locator fields are `segment_id`, `segment_generation`, `file_offset`, `frame_len`.
- `generation`, `offset`, and `length` are shorthand aliases used only in prose.
- `commit.idxlog` is a logical stream and may be stored as rolled files `commit-<roll_id>.idxlog`.

## Goals
- Add durable storage options while preserving v1 semantics for each supported pattern adapter.
- Support one shared archive/recorder core across supported adapters, with `publish_subscribe` as the V1 data plane.
- Add random access replay APIs keyed by sequence.
- Add direct replay APIs keyed by physical locator for metadata-driven rematerialization.
- Support query-driven replay workflows with application-owned metadata.
- Support bounded retention under global capacity and optional tiered storage.
- Detect and report data corruption during recovery and replay.
- Make `Async` mode efficient on Linux systems with `io_uring`.

## Non-Goals
- Introducing a new top-level messaging pattern.
- Keeping or reviving the retired core `Log` messaging pattern on `design/log-archive-pubsub-v1`.
- Embedding SQLite or any metadata query engine in iceoryx2 core.
- Requiring sequence->offset translation in metadata-driven replay paths.
- Requiring all pattern adapters to ship in the same milestone as the pub/sub adapter.

## Architecture
### Extension Components
- **Recorder core**: persists canonical archive records, rolls segments, manages durability and retention metadata.
- **Pattern adapters**: bind core recorder ingest to messaging-pattern semantics (`publish_subscribe`; `pipeline` later).
- **Replayer**: serves replay from persisted segments (`read_at`, `read_range`, `seek`, `read_at_locator`).
- **Admin surface**: controls recorder/replayer lifecycle and retention operations.

### Pattern Adapter Model
- Recorder core `MUST` define a pattern-neutral ingest contract that accepts canonical archive frames plus source metadata.
- Pattern adapters `MUST` provide source identity fields (`source_pattern`, service identity, optional source sequence, timestamps).
- Pub/Sub adapter `MAY` provide source sequence if available; when unavailable it `MUST` set source-sequence-present flag to false.
- Pipeline adapter `MUST` preserve stage/service identity and `MAY` provide stage-local sequence when available.
- Log adapter support is retired on `design/log-archive-pubsub-v1`; source-pattern value `Log` is reserved only for historical archive compatibility.
- Replay/rematerialization contracts remain locator-first and pattern-neutral; pattern-specific replay helpers `MAY` be layered above core APIs.

### Durability Modes
- `Volatile`: v1 behavior, no disk persistence.
- `Async`: in-memory commit first, durability lag allowed.
- `Sync`: `send()` returns only after durable write acknowledgment.
- Default persistence mode is `Async` for throughput-oriented baseline behavior.

### Async Write Engine (io_uring-First)
- Recorder async path is designed around a dedicated I/O worker that owns the submission/completion rings.
- Preferred backend on capable Linux systems is `io_uring`; non-`io_uring` fallback backend is required for portability.
- Segment files are treated as fixed targets for batched append operations.
- Async write pipeline batches records and submits grouped writes with completion-driven durable-position advancement.
- Durability fences in `Sync` mode are expressed as explicit flush barriers in the same backend abstraction.
- Backpressure is driven by bounded in-flight operations and bounded staging memory.
- Completion processing updates recorder metrics and surfaces deterministic write/flush failure states.

### Storage Model
- Segment-file storage with immutable sealed segments plus one active writable segment.
- Segment metadata includes sequence range, timestamps, checksum, payload schema id.
- Recovery uses checkpoint plus tail scan/truncation for partial writes.

### Foundational Throughput Contracts (Architecture-Critical)
- The following constraints are architectural and `MUST` be implemented as part of core recorder/replayer design, not deferred to optional hardening.

- Ack and commit semantics:
- Recorder API `MUST` expose explicit acknowledgment levels:
- `Accepted` (accepted by recorder, not yet durable),
- `DurableData` (segment durability reached),
- `DurableDataAndCommitLog` (segment + `commit.idxlog` durability reached when commit-log mode is active).
- Default `send()` behavior `MUST` be unambiguous by persistence mode (`Async` returns `Accepted`, `Sync` returns `DurableData`).
- APIs waiting for stronger acks `MUST` have explicit timeout/error behavior.
- Ack timeout defaults:
- `wait_durable_data_timeout = 1s`
- `wait_durable_data_and_commitlog_timeout = 2s`
- `ack_poll_interval = 1ms`
- `flush_cli_timeout = 30s`
- Timeout `MUST` return explicit `AckTimeout` with last known durable positions.

- Crash/power-loss contract:
- Process-crash and power-loss behavior `MUST` be explicitly specified per persistence mode and ack level.
- Recovery `MUST` guarantee prefix safety for durable boundaries (no committed hole before reported durable position).
- Recorder/admin status `MUST` surface last durable data sequence and last durable commit-log ordinal separately.

- Out-of-space and preallocation behavior:
- Segment and metadata-log preallocation failures `MUST` transition recorder to explicit degraded/error state.
- Recorder `MUST NOT` silently drop payload data on archive write failure.
- Failure policy (`FailWriter`, `Backpressure`, or equivalent) `MUST` be explicit and operator-configurable.
- Default out-of-space failure policy is `FailWriter`.
- Near-capacity watermarks `SHOULD` be exposed for early operator action before hard out-of-space.

- Replay isolation from ingest:
- Replay I/O and ingest I/O `MUST` be isolated by scheduler/queueing so replay cannot starve recorder durable progress.
- Implementations `SHOULD` enforce configurable replay I/O budget or concurrency caps.
- Recorder throughput metrics `MUST` remain available per recorder/service with replay load present.

- Write amplification and accounting:
- Implementation `MUST` expose bytes-written accounting for segment data, segment metadata, and commit-log streams.
- Operational status `MUST` expose amplification ratio (`total_bytes_written / payload_bytes_committed`).
- Profile guidance `MUST` include expected amplification for `CommitLogOnly` vs `Hybrid`.
- Concrete numeric amplification budgets are intentionally left profile-dependent until post-implementation benchmarking on target profiles.

- Recovery-time envelope:
- Startup/recovery complexity `MUST` be bounded by catalog + active-tail validation, not full rescan of every retained payload byte.
- Recovery-time SLO defaults:
- `target_recovery_time <= 5s + 0.5ms * sealed_segment_count`
- soft operational cap `= 60s`
- degraded threshold `= 2x target_recovery_time`
- Recovery-time SLOs `MUST` be validated as retained-bytes and segment-count scale.

### On-Disk Archive Layout (Proposed)
- Data archive root:
- `<data_storage_path>/<service_id>/<log_id>/catalog.bin`
- `<data_storage_path>/<service_id>/<log_id>/segments/segment-<segment_id>-g<generation>.data`
- `<data_storage_path>/<service_id>/<log_id>/segments/segment-<segment_id>-g<generation>.meta`
- `<data_storage_path>/<service_id>/<log_id>/segments/segment-<segment_id>-g<generation>.idx` (optional accelerator)
- `<data_storage_path>/<service_id>/<log_id>/core-locator.idx` (optional built-in query accelerator)
- Metadata log root:
- `<metadata_log_path>/<service_id>/<log_id>/commit-<roll_id>.idxlog`
- `<metadata_log_path>/<service_id>/<log_id>/indexer.watermark` (optional persisted indexer progress marker)
- `catalog.bin` contains fixed-width segment descriptors for fast startup and range lookup.
- `commit.idxlog` is a logical append-only stream (one or more rolled files) used for metadata catch-up and crash recovery handoff.
- `.data` holds serialized frame records.
- `.idx` optionally accelerates sequence-based replay; locator-based replay does not require it.
- `.meta` holds immutable segment summary plus final checksum and seal marker.
- `core-locator.idx` optionally accelerates built-in sequence/time-range queries without external DB dependency.
- `metadata_log_path` `MAY` be a different volume from `data_storage_path`.

### Format Choice (Binary vs TOML)
- Canonical archive files are binary:
- `catalog.bin`
- `commit-*.idxlog`
- `segment-*.data`
- `segment-*.meta`
- Optional binary accelerator file:
- `segment-*.idx`
- Rationale:
- zero-copy friendly parsing
- fixed-width decode for hot paths
- compact size and lower write amplification
- deterministic recovery scans
- Optional human-readable files are allowed for operators only:
- `archive-manifest.toml` for static configuration and diagnostics
- debug snapshots generated by tools
- Runtime correctness `MUST NOT` depend on TOML sidecars.

### Binary Header and Versioning Contract (V1)
- Every canonical binary file starts with the same fixed header.

```rust
#[repr(C)]
pub struct ArchiveFileHeaderV1 {
    pub magic: [u8; 4],          // b"IOX2"
    pub file_kind: u16,          // see FileKind values below
    pub major: u16,              // incompatible changes
    pub minor: u16,              // additive compatible changes
    pub header_len: u16,         // bytes including extension area
    pub flags: u32,              // low 24 bits optional, high 8 bits must-understand
    pub created_at_ns: u64,
    pub log_id: [u8; 16],
    pub segment_id: u64,         // 0 for non-segment files
    pub segment_generation: u32, // 0 for non-segment files
    pub reserved: [u8; 20],      // future expansion
    pub header_crc32c: u32,      // crc over header except this field
}
```

`file_kind` constants:
- `1` = `Catalog`
- `2` = `CommitIdxLog`
- `3` = `SegmentData`
- `4` = `SegmentIndex`
- `5` = `SegmentMeta`

`flags` layout:
- optional bits (`0x00FF_FFFF`) `MAY` be ignored when unknown
- must-understand bits (`0xFF00_0000`) `MUST` be rejected when unknown
- v1 defines all must-understand bits as reserved (`0`)

Defined optional bits (v1):
- `0x0000_0001` = `HAS_PADDING_RECORDS`
- `0x0000_0002` = `HAS_SEGMENT_CHECKSUM`

Defined must-understand bits (v1):
- none; all values in `0xFF00_0000` are reserved and require reject if non-zero

Initial format versions:
- `catalog.bin`: `major=1, minor=0`
- `commit.idxlog`: `major=1, minor=0`
- `segment.data`: `major=1, minor=0`
- `segment.meta`: `major=1, minor=0`
- `segment.idx`: `major=1, minor=0` (only when accelerator enabled)

Compatibility rules:
- Endianness `MUST` be little-endian.
- Reader `MUST` accept only `major == supported_major`.
- Reader `MUST` accept `minor <= supported_minor`.
- Reader `MUST` reject `minor > supported_minor` with `UnsupportedFormatMinor`.
- Reader `MUST` reject unknown must-understand flag bits.
- `major` increments `MUST` require migration tooling; implementations `MUST NOT` perform implicit in-place rewrite.
- `minor` increments `MAY` append fields after `header_len` or add optional record fields.

Conformance checks (mandatory):
1. Reader `MUST` validate `magic == b"IOX2"`.
2. Reader `MUST` validate `file_kind` is one of `{1,2,3,4,5}`.
3. Reader `MUST` validate `header_len >= size_of::<ArchiveFileHeaderV1>()`.
4. Reader `MUST` validate `major == 1` for v1 files.
5. Reader `MUST` validate `minor <= supported_minor_for(file_kind)`.
6. Reader `MUST` validate `(flags & 0xFF00_0000) == 0` for v1 readers.
7. Reader `MUST` validate `header_crc32c`.
8. Reader `MUST` validate `segment_id == 0 && segment_generation == 0` for `Catalog` and `CommitIdxLog`.
9. Reader `MUST` validate `segment_id > 0` for `SegmentData` and `SegmentMeta`.
10. Reader `MUST` validate that if `file_kind == SegmentIndex`, `segment_id > 0`.

### Segment Data Record Format (Proposed)
- `SegmentHeader` (fixed-size):
- magic/version
- `segment_id`, `generation`
- `created_at_ns`
- checksum policy
- `payload_schema_id`
- reserved bytes for forward compatibility
- `RecordFrame` (variable-size):
- frame magic/version
- `frame_len`, `payload_len`
- `commit_ordinal`
- `source_pattern` (`Log` reserved, `PublishSubscribe`, `Pipeline`)
- `source_sequence` (optional unless a supported adapter defines a stable source sequence)
- source service/stage identity fields
- `event_time_ns`, `commit_time_ns`
- user header bytes length
- payload bytes
- checksum trailer (policy-dependent)
- All record appends are 8-byte aligned to simplify scanning and future mmap-read views.
- Record compatibility:
- record header includes `record_kind`, `record_version`, `record_len`
- unknown optional record kinds are skipped by `record_len`
- unknown required record kinds fail decode

Encoding rule:
- `RecordFrame` fields `MUST` be encoded/decoded with explicit little-endian helpers.
- Implementations `MUST NOT` use raw `repr(C)` transmute/cast decode for on-disk records.

`record_kind` constants:
- `1` = `Sample`
- `2` = `Checkpoint`
- `3` = `Seal`
- `4` = `Padding`

`record_version` policy:
- `record_version` `MUST` be `1` for all v1 records.
- Record versioning `MUST` follow the same major/minor semantics as file header versioning.
- Higher major `MUST` be rejected; higher minor `MUST` be rejected unless explicitly supported.

`RecordHeaderV1` minimal fields:

```rust
#[repr(C)]
pub struct RecordHeaderV1 {
    pub record_magic: [u8; 4],   // b"RCD1"
    pub record_kind: u16,        // see constants above
    pub record_version: u16,     // 1 for v1
    pub record_len: u32,         // bytes including payload and checksum trailer
    pub header_len: u16,         // bytes including future extension fields
    pub flags: u16,              // record-local flags
    pub record_crc32c: u32,      // crc over full record with this field zeroed
}
```

Record conformance checks (mandatory):
1. Reader `MUST` validate `record_magic == b"RCD1"`.
2. Reader `MUST` validate `record_kind` is known or explicitly skippable by policy.
3. Reader `MUST` validate `record_version` is supported.
4. Reader `MUST` validate `record_len >= header_len`.
5. Reader `MUST` validate `record_len % 8 == 0`.
6. Reader `MUST` validate `next_offset = offset + record_len` does not exceed file length.
7. Reader `MUST` validate `record_crc32c`.

### Alignment and I/O Constraints
- Record-level alignment: `record_len` `MUST` be 8-byte aligned.
- File offset alignment: frame start offsets `MUST` be 8-byte aligned.
- Segment size alignment: `segment_bytes` `SHOULD` be rounded up to a 2 MiB boundary.
- Default I/O mode `SHOULD` be buffered file I/O.
- Optional direct-I/O mode `MAY` be enabled only when all buffers, offsets, and lengths satisfy 4 KiB alignment.
- Recorder `MUST` preallocate active segment files to `segment_bytes` before first append.
- Recorder `SHOULD` keep at least one spare preallocated segment for roll handoff.
- Recorder `SHOULD` preallocate the next metadata-log roll file in parallel with active writing.
- Steady-state append path `MUST NOT` require file-size extension syscalls.
- Async backend `SHOULD` prefer `io_uring` on supported Linux systems and `MUST` provide a fallback backend.
- `io_uring` backend `SHOULD` support registered files and completion batching; it `MAY` support registered buffers and `SQPOLL` as optional tuning knobs.

### Data Integrity (Checksums)
- `RecordHeaderV1.record_crc32c` `MUST` be present for all record-framed files and verified during decode.
- Recorder `MUST` always compute/store framing integrity via `record_crc32c`.
- Recorder `MUST` write payload checksum trailers only when payload `checksum` policy is not `None`.
- Replayer `MUST` verify `record_crc32c` always and payload checksum trailers when present.
- Checksum policy is configurable:
- `None` (for explicit performance-only deployments),
- `Crc32c` (default; hardware-accelerated where available, software fallback otherwise),
- `XxHash64` (optional throughput-focused alternative).
- Throughput-oriented deployments `SHOULD` treat `record_crc32c` as required baseline integrity and set payload `checksum = None` unless stronger payload-level verification is required.
- Integrity failures `MUST` return explicit corruption status with segment and sequence context.

### Failure and Recovery Semantics
- Torn write in active segment:
- implementation `MUST` detect via invalid frame magic/len or checksum failure
- implementation `MUST` truncate to last known-good frame boundary
- implementation `MUST` mark a recovery event in metrics and admin status
- Roll crash window:
- if segment `.meta` seal marker is missing, implementation `MUST` treat the segment as active-recoverable
- implementation `MUST` validate tail and either seal or truncate+seal
- `commit.idxlog` replay:
- implementation `MUST` idempotently rebuild missing metadata rows keyed by locator
- implementation `MUST` use `commit_ordinal` for strict replay ordering
- Recovery `MUST NOT` rewrite sealed segment payload bytes.

### Random Access Contract
- Locator API `MUST` be supported for all pattern adapters.
- Sequence API `MUST` be supported when the adapter provides source sequence:
- Sequence API key is `(log_id, source_sequence)`.
- Locator API (metadata-native): keyed by `(log_id, segment_id, segment_generation, file_offset, frame_len)`.
- Replayer APIs `MUST` include:
- `read_at_sequence(sequence)` (when sequence is available)
- `read_range(sequence_start, max_records)` (when sequence is available)
- `seek(sequence)` plus linear `next()` (when sequence is available)
- `read_at_locator(locator)`
- `read_many_locators(&[locator])`
- Out-of-retention sequence `MUST` return explicit not-available status when sequence API is active.
- Locator replay `SHOULD` be treated as the primary high-rate path.
- Sequence replay `MUST` work without `segment.idx` via catalog + bounded frame scan when sequence API is active.
- `segment.idx` `MAY` be used only to accelerate sequence lookups.

### Replay Rate Modes
- `AsFastAsPossible` (default): implementation `MUST` support replay as quickly as downstream accepts.
- `WallClockRate`: implementation `SHOULD` support fixed target throughput (`messages/s` or `bytes/s`).
- `EventTimeRate`: implementation `SHOULD` support replay according to event timestamps with configurable speed factor.
- `BackpressureAware`: implementation `MUST` support replay that advances only when downstream demand is available.

### Metadata Query Integration
- Metadata `MUST` remain fully application-owned.
- Query services `SHOULD` map attributes directly to locators.
- Sequence `MAY` be stored as optional secondary key for range scans and integrity checks.
- Locators `MUST` be treated as authoritative for archived reads after generation and bounds validation.
- Query/read handoff `MAY` use a snapshot token or pin handle for deterministic replay.

### Metadata WAL and Live Indexing Contract
- `commit.idxlog` is the canonical metadata write-ahead-log (WAL); it is optimized for append/recovery, not direct ad-hoc query execution.
- Deployments requiring near-real-time queries `MUST` support a continuous indexer mode that tails `commit.idxlog` while recording is active.
- Query surfaces backed by an indexer `MUST` expose:
- `last_commit_ordinal` (recorder durable metadata boundary),
- `last_indexed_commit_ordinal` (queryable boundary),
- `query_watermark = last_indexed_commit_ordinal`.
- Queries requesting data beyond `query_watermark` `MUST` return explicit `NotIndexedYet` (or equivalent) rather than partial silent results.
- Indexer progress `SHOULD` be persisted (`indexer.watermark` or equivalent durable state) for bounded restart catch-up.
- `reindex` operations `MUST` be idempotent and resume from checkpoint/watermark.

### Metadata Contract (Offset-First)
- Required locator tuple for replay (`MUST`):
- `log_id`, `segment_id`, `segment_generation`, `file_offset`, `frame_len`
- Strongly recommended fields (`SHOULD`):
- `commit_ordinal` (recovery checkpoint and strict ordering)
- `sequence` (continuity diagnostics)
- `checksum` and `checksum_kind` (early verification)
- Replayer `MUST` validate:
- segment generation exists and is not stale
- `file_offset + frame_len` is within segment bounds
- checksum/sequence match when provided by metadata service

### Query Readiness Modes
- `IndexerBacked`:
- queries are served from external metadata index;
- readiness is bounded by `query_watermark`.
- `CoreLocatorIndex`:
- queries are served from optional built-in `core-locator.idx` for sequence/time-range to locator resolution;
- application-owned metadata enrichment remains external.
- Implementations `MUST` report active query-readiness mode and current watermark in admin/CLI status.

### Metadata Log Placement and Retention
- Recorder `MUST` support configuring `metadata_log_path` independently from `data_storage_path`.
- Recorder `MUST` support rolling metadata log files by size (`metadata_log_roll_bytes`).
- Recorder `MUST` support a metadata log global size cap (`metadata_log_max_bytes`).
- Default metadata-log retention policy `SHOULD` be `FollowDataRetention`.
- Under `FollowDataRetention`, metadata log eviction `MUST NOT` remove commit records still required to rematerialize retained data segments.
- If configuration causes metadata coverage to fall behind retained data, recorder/admin status `MUST` report degraded state.

### Runtime Defaults (Initial)
- Implementations `SHOULD` use the following runtime defaults:
- `data_storage_path = "/var/lib/iox2/logs"` (platform-specific equivalent accepted).
- `metadata_log_path = data_storage_path` (same volume by default).
- `segment_bytes = 256 MiB` (rounded up to 2 MiB alignment).
- `segment_preallocate = true`.
- `spare_preallocated_segments = 1`.
- `checksum = Crc32c`.
- `async_io_backend = IoUringPreferred`.
- `io_uring_queue_depth = 256`.
- `io_submit_batch_max = 64`.
- `io_cqe_batch_max = 128`.
- `io_uring_register_files = true` (if supported by kernel/backend).
- `commit_idxlog_checkpoint_interval = 4096`.
- `metadata_log_roll_bytes = 1 GiB`.
- `metadata_log_max_bytes = 32 GiB`.
- `metadata_log_retention_policy = FollowDataRetention`.
- metadata path defaults are defined in `log-archive-userland-metadata.md`.

### Configuration Profiles (Preset + Overrides)
- Recorder configuration `MUST` support named profiles:
- `Durable`
- `Balanced`
- `Throughput`
- `Replay`
- Default profile `MUST` be `Balanced`.
- Profile resolution order `MUST` be:
1. load profile defaults
2. apply explicit per-service overrides
- Effective configuration is therefore `profile defaults + explicit overrides`.
- Recorder startup `MUST` publish resolved effective configuration in status/log output, including active profile id.
- Recorder startup `MUST` fail fast with explicit validation errors for unsafe or contradictory configurations.
- Validation rules `MUST` include at least:
- when `metadata_delivery_mode = Hybrid`, `metadata_queue_capacity > 0`
- `io_submit_batch_max <= io_uring_queue_depth`
- `io_cqe_batch_max <= 2 * io_uring_queue_depth`
- when resolved mode is durability-first (`Durable` profile or explicit equivalent), `metadata_overflow_policy` `MUST NOT` be a dropping policy

Profile baseline intents (initial):
- `Durable`:
- default `persistence_mode = Sync`
- default metadata delivery preference `Hybrid`
- default `metadata_overflow_policy = Block`
- out-of-space policy remains `FailWriter`
- `Balanced`:
- default `persistence_mode = Async`
- uses runtime defaults in `Runtime Defaults (Initial)`
- `Throughput`:
- default `persistence_mode = Async`
- default metadata delivery preference `CommitLogOnly`
- uses high-throughput tuning values from `Throughput Profile`
- `Replay`:
- default `persistence_mode = Async`
- default metadata delivery preference `Hybrid`
- prioritizes replay/query freshness over peak ingest throughput (smaller metadata lag targets, moderate ingest batching)

Queue knob definitions (normative):
- `io_uring_queue_depth` is the maximum in-flight async I/O operations for recorder data path submission/completion.
- `metadata_queue_capacity` is the bounded pending metadata-event queue between recorder ingest and metadata/indexing sink.

### Throughput Profile
- Objective: sustain disk-bound recorder throughput on high-end hosts (for example 64-core systems with >=15 GiB/s aggregate write bandwidth).
- Baseline assumptions:
- recorder is `Async`,
- metadata mode is `CommitLogOnly` for maximum ingest throughput (or `Hybrid` when query freshness is required),
- framing integrity is `record_crc32c`; payload checksum is disabled unless explicitly required,
- segment and metadata-log preallocation are enabled,
- replay and indexing are isolated from recorder I/O workers.
- Recommended high-throughput settings:
- `io_uring_queue_depth >= 1024`
- `io_submit_batch_max >= 256`
- `io_cqe_batch_max >= 512`
- `segment_bytes >= 1 GiB`
- `spare_preallocated_segments >= 2`
- `metadata_log_roll_bytes >= 4 GiB`
- `metadata_queue_capacity >= 262144` (only when `Hybrid`)
- `metadata_batch_max_records >= 1024` (only when `Hybrid`)
- throughput validation is performed with batching flush (`100 ms` or batch-full).
- CPU and NUMA placement are expected to be controlled externally (for example `taskset`/`numactl`, cpuset/cgroup, or service manager affinity settings), not by mandatory recorder-internal APIs.
- Implementations `SHOULD` support external parallelism across multiple independent recorder instances/services; a single recorder stream is not required to saturate full device bandwidth.
- Throughput claims `MUST` be validated with reproducible benchmark scripts on target filesystem/device profiles (XFS and ZFS configurations included in report metadata).
- Throughput acceptance target for `Throughput` `SHOULD` be at least `80%` of the host-measured sequential-write baseline (`fio` or equivalent) under matched durability settings.

### External Parallelism Model
- Internal recorder sharding is out of scope for v2 baseline.
- High-bandwidth deployments `SHOULD` scale via external striping/parallelism (for example RAID/ZFS striping and multiple independent log services/recorders).
- Cross-recorder ordering is best-effort unless reconstructed by metadata/timestamp correlation.
- Admin/CLI `SHOULD` support fleet-level aggregation via external tooling; per-recorder status remains first-class in core APIs.

### Direct-I/O Enablement Criteria
- Buffered I/O remains default for v2 baseline.
- Direct-I/O `MUST` be manually enabled by explicit operator configuration; implementations `MUST NOT` auto-enable it.
- Direct-I/O `MAY` be enabled only when benchmark evidence shows material gain (recommended threshold: `>=10%` sustained throughput increase or materially lower flush tail latency) on target deployment.
- When direct-I/O is enabled, implementation `MUST` enforce alignment constraints and fail fast on invalid buffers/offsets.
- Implementation `MUST` support fallback to buffered I/O with explicit status/error reporting when direct-I/O prerequisites are not met.

### Filesystem and OS Profile Contracts
- Performance qualification `MUST` record filesystem and OS profile used for results (filesystem type, mount/dataset options, kernel version, I/O scheduler, and device topology).
- Reference deployment profiles `SHOULD` include at minimum:
- high-speed NVMe on XFS,
- large SSD pool configuration (for example ZFS) with documented record size and sync policy.
- Throughput claims `MUST` reference the exact profile id used for measurement.
- Implementations `MUST NOT` assume identical behavior across filesystems; profile-specific tuning is expected.

### Retention and Overflow Interaction
- In-memory overflow policy (`Block`, `DropNewest`, `DropOldest`) applies to live log delivery semantics and `MUST NOT` retroactively change already durable archived bytes.
- Global size cap enforcement `MUST` use deterministic oldest-first eviction of sealed segments, unless blocked by active replay pins/snapshots.
- Lagging replay sessions `MUST` be protected by pin/snapshot constraints until released or timed out by policy.
- Retention `MUST` be globally arbitrated across tiers, with one deterministic eviction decision function.

### Tiered Storage
- Tier 0: in-memory ring.
- Tier 1 (`HOT_ATTACHED`): active + sealed segments immediately available for replay/query.
- Tier 2 (`COLD_DETACHED`): sealed segments moved to colder storage and detached from default hot query/replay path.
- Tiering objective in v2 is archive-style cold storage for detached sealed segments (not transparent multi-tier live caching).
- Detach/attach semantics:
- only sealed segments `MAY` be detached;
- detached segments `MUST` keep stable locator identity (`segment_id`, `segment_generation`, `file_offset`);
- hot catalog `MUST` record detached location/tier state;
- attach operation `MUST` make selected detached segments query/replay eligible again;
- delete operation `MUST` apply only to detached segments.
- Replay/query behavior:
- default replay/query scope `SHOULD` be `HOT_ATTACHED` segments;
- default detached access policy is `ExplicitAttachRequired`;
- archive-aware direct reads `MAY` be added as an optional non-default mode in later phases;
- not-attached detached requests `MUST` return explicit not-available/deferred status.
- Detached candidate selection defaults:
- policy name: `ColdOldestUnpinnedFirst`.
- eligibility: segment is `sealed`, not pinned by replay/snapshot, not active, and currently `HOT_ATTACHED`.
- detach trigger watermark: start at `85%` hot-tier capacity.
- detach stop watermark: stop at `75%` hot-tier capacity.
- per-operation detach batch cap: `32` segments.
- default scoring weights:
- `age_weight = 0.6`
- `read_heat_weight = 0.3` (prefer detaching colder segments first)
- `size_weight = 0.1`

### Replay Safety Metadata (Minimum Set)
- `segment_id`
- `segment_generation`
- `sequence_start`
- `sequence_end`
- `created_timestamp`
- `sealed_timestamp` (if sealed)
- `checksum_policy`
- `segment_checksum` (or per-record checksum marker reference)
- `payload_schema_id`
- `tier_location`

## Aeron-Derived Notes
- Keep hot path and archive control path separated.
- Keep logical identity (`sequence`) and physical locator identity distinct.
- Prefer fixed-size segment files with deterministic rolling.
- Maintain compact immutable segment catalog metadata for replay resolution.
- Use explicit persistence commit protocol for crash-safe truncation/recovery.
- Use replay sessions with progress counters for observability/control.
- Base retention on active replay pins/snapshots to prevent invalid deletes.
- Treat locator validation as mandatory (generation + bounds + optional checksum).
- Keep admin commands idempotent (`start_recorder`, `stop_recorder`, `trim_before`, `replay_from`).

## Control Surface Decision
- Recorder/replayer lifecycle and retention controls are exposed through a dedicated log-admin control surface.
- Data-plane builder stays focused on log creation/open and port creation; admin operations are separated to avoid API bloat.

## CLI Control Surface (Implemented 2026-02-09)
- An `iox2` CLI control path `MUST` be provided for recorder lifecycle and retention operations.
- CLI control `MUST` call the log-admin control surface and `MUST NOT` mutate archive files directly.
- CLI commands `MUST` be idempotent and return deterministic exit status.
- A dedicated long-running recorder process `MUST` be provided as a separate executable (`iox2-log-recorder`) so data-plane capture is explicit and decoupled from one-shot control commands.
- Implemented command set (`iox2-log-admin ...`):
- `iox2-log-admin start --service <name> --storage-path <path> [--metadata-log-path <path>] [--profile durable|balanced|throughput|replay] [--mode volatile|async|sync] [--segment-bytes <bytes>] [--spare-preallocated-segments <n>] [--segment-preallocate true|false] [--max-disk-bytes <bytes>]`
- `iox2-log-admin stop --service <name> --storage-path <path> [--metadata-log-path <path>]`
- `iox2-log-admin status --service <name> --storage-path <path> [--metadata-log-path <path>]`
- `iox2-log-admin flush --service <name> --storage-path <path> [--metadata-log-path <path>]`
- `iox2-log-admin trim --service <name> --storage-path <path> [--metadata-log-path <path>] --before-sequence <seq>`
- `iox2-log-admin detach --service <name> --storage-path <path> [--metadata-log-path <path>] --before-sequence <seq>`
- `iox2-log-admin attach --service <name> --storage-path <path> [--metadata-log-path <path>]`
- `iox2-log-admin delete-detached --service <name> --storage-path <path> [--metadata-log-path <path>] [--before-sequence <seq>]`
- Introspection command set (`MUST`):
- `iox2-log-admin list-segments --service <name> --storage-path <path> [--metadata-log-path <path>] [--detached-only]`
- `iox2-log-admin inspect-commit-log --service <name> --storage-path <path> [--metadata-log-path <path>] [--from-ordinal <ordinal>] [--limit <n>]`
- `iox2-log-admin inspect-record --service <name> --storage-path <path> [--metadata-log-path <path>] --at-sequence <seq>`
- `iox2-log-admin inspect-record --service <name> --storage-path <path> [--metadata-log-path <path>] --at-locator <segment_id>:<generation>:<offset>:<len>`
- Implemented long-running command set (`iox2-log-recorder ...`):
- `iox2-log-recorder publish-subscribe --service <name> --storage-path <path> [--metadata-log-path <path>] [--profile durable|balanced|throughput|replay] [--mode volatile|async|sync] [--segment-bytes <bytes>] [--spare-preallocated-segments <n>] [--segment-preallocate true|false] [--max-disk-bytes <bytes>] [--node-name <name>] [--cycle-time-ms <ms>] [--flush-interval-ms <ms>] [--max-messages <n>] [--timeout-ms <ms>] [--ack-level accepted|durable-data|durable-data-and-commit-log] [--subscriber-max-borrowed-samples <n>]`
- `iox2-log-recorder log ...` is retired on `design/log-archive-pubsub-v1`; compatibility surfaces must be absent or return deterministic unsupported errors.
- `status` output `MUST` include:
- operation name and archive path identity (`service`, `storage_path`, `metadata_log_path`)
- recorder profile and persistence mode (`profile`, `persistence_mode`)
- async backend introspection (`configured_async_io_backend`, `effective_async_io_backend`)
- degraded status (`degraded`)
- stats block:
- `committed_records`
- `payload_bytes_committed`
- `data_bytes_written`
- `metadata_bytes_written`
- `rolled_segments`
- `metadata_log_rolls`
- `amplification_ratio`
- retention block:
- `max_disk_bytes`
- `retained_bytes_total`
- `retained_bytes_hot_attached`
- `retained_bytes_cold_detached`
- `segments_hot_attached`
- `segments_cold_detached`
- `pinned_segments`
- Commands support existing CLI output formats (`RON`, `JSON`, `YAML`) through global `--format`.
- `start` and `stop` `MUST` be safe to invoke repeatedly.
- `flush` in `Async` mode `MUST` block until recorder flush completes or fail with explicit error.
- Introspection commands `MUST` return deterministic `NotAvailable` style errors for out-of-retention sequence/locator targets.
- Introspection commands `MUST` support machine-readable output parity with `status` (`RON`, `JSON`) and stable field names.
- Introspection payload rendering `MAY` be truncated by default for terminal safety; machine-readable output `MUST` include full locator and frame-length fields.

## Observability and Metrics (Planned)
- Recorder counters:
- appended records/bytes
- durable records/bytes
- segment roll count
- segment preallocation stall count
- truncation recovery count
- checksum failure count
- metadata enqueue success/drop/block counts
- `io_uring` submit stall count
- `io_uring` completion backlog high-watermark
- Replayer counters:
- locator read count
- sequence read count
- not-available count
- corruption detection count
- Admin gauges:
- active segment id/generation
- retained bytes by tier
- oldest/newest retained sequence
- metadata lag (`last_durable_commit_ordinal - last_indexed_commit_ordinal`)
- `query_watermark` (`last_indexed_commit_ordinal`)
- `query_readiness_mode`
- per-recorder append throughput (`bytes/s`, `records/s`)

## Proposed V2 API Direction
```rust
let service = node
    .service_builder(&"Telemetry/Data".try_into()?)
    .publish_subscribe::<[u8]>()
    .open_or_create()?;

let admin = service.admin_builder().open()?;

let recorder = admin.recorder_builder()
    .persistence_mode(PersistenceMode::Async)
    .profile(RecorderProfile::Throughput)
    .async_io_backend(AsyncIoBackend::IoUringPreferred)
    .storage_path("/var/lib/iox2/logs")
    .segment_bytes(1024 * 1024 * 1024)
    .segment_preallocate(true)
    .spare_preallocated_segments(2)
    .max_disk_bytes(32 * 1024 * 1024 * 1024)
    .checksum(Checksum::None) // payload checksum; framing CRC remains enabled
    .metadata_delivery_mode(MetadataDeliveryMode::CommitLogOnly)
    .io_uring_queue_depth(1024)
    .io_submit_batch_max(256)
    .io_cqe_batch_max(512)
    .start()?;

let replayer = admin.replayer_builder().open()?;
let snapshot = replayer.begin_snapshot()?;
let sample = replayer.read_at_with_snapshot(42, snapshot)?;
let by_locator = replayer.read_at_locator(locator)?;
```

## Validation Plan
- Recovery tests:
- crash during segment write,
- crash during segment roll,
- checkpoint + tail scan correctness.
- checksum mismatch detection and recovery behavior.

- Replay tests:
- `read_at`, `read_range`, `seek` behavior,
- out-of-retention not-available behavior,
- replay under concurrent roll/trim.
- checksum verification failures and error surfacing.

- Metadata integration tests:
- query returns locator set,
- locator validation and stale-generation rejection behavior,
- snapshot token consistency during retention churn.

- Operational tests:
- retention trim under global cap,
- tier promotion/eviction correctness,
- replay lag and pin pressure behavior.
- async backend parity (`io_uring` vs fallback backend) for ordering, durability semantics, and error propagation.
- bounded in-flight behavior under sustained write pressure.
- separate-volume operation (`data_storage_path` != `metadata_log_path`) including failure isolation and degraded-state reporting.
- CLI control tests:
- `start/stop/status/flush/trim` behavior and exit codes
- idempotency under repeated invocations
- degraded-state error reporting

### Performance Validation Targets
- Sustained append throughput in `Async` and `Sync` modes.
- Sustained append throughput in `Throughput` profile with `CommitLogOnly`.
- Relative throughput target: `Throughput` `SHOULD` reach `>=80%` of host sequential-write baseline under matched durability settings.
- Ack-level latency envelopes:
- `Accepted`, `DurableData`, and `DurableDataAndCommitLog` p50/p99 under steady load.
- Replay latency p50/p99 for:
- `read_at_sequence(sequence)` (for sequence-capable adapters)
- `read_at_locator(locator)`
- Recovery time objective:
- cold start with 1 active + N sealed segments
- metadata catch-up from `commit.idxlog`
- Recovery-time scaling envelope:
- measured as retained-bytes and segment-count increase.
- Backpressure behavior:
- bounded memory under stalled storage backend
- bounded memory under stalled metadata backend
- Metadata amplification delta:
- compare recorder throughput for `CommitLogOnly` vs `Hybrid` on identical ingest workload.
- Write amplification reporting:
- verify amplification ratio accounting in status/metrics for each profile.
- Preallocation correctness:
- no steady-state append path calls that extend segment size after warm-up.
- Replay isolation:
- recorder throughput/latency under concurrent replay load with configured replay I/O budget.
- Multi-recorder external scaling:
- aggregate throughput across multiple independent recorders on striped storage.

## Decisions to Freeze Before Implementation
- [x] Recorder scope across messaging patterns:
- recorder core is pattern-neutral; current branch support is `publish_subscribe` only, with `pipeline` deferred and `Log` retired.
- [x] Internal recorder sharding in core v2:
- out of v2 core scope; v2 uses external parallelism/striping and multiple independent recorders.
- [x] Direct-I/O policy:
- manual explicit opt-in only; never auto-enabled.
- [x] Tiering semantics:
- cold storage is modeled as detached sealed segments with explicit attach/detach/delete lifecycle.
- [x] Metadata query readiness:
- query watermark contract (`last_commit_ordinal`, `last_indexed_commit_ordinal`, `NotIndexedYet`) is required.
- [x] Default out-of-space failure policy:
- `FailWriter` is the default for v2.
- [x] Default detached access behavior:
- `ExplicitAttachRequired` is the default; detached reads require explicit attach.
- [x] Recovery-time SLO numbers:
- `target_recovery_time <= 5s + 0.5ms * sealed_segment_count`, soft cap `60s`, degraded at `2x target`.
- [x] Ack timeout defaults:
- `wait_durable_data_timeout=1s`, `wait_durable_data_and_commitlog_timeout=2s`, `ack_poll_interval=1ms`, `flush_cli_timeout=30s`.
- [x] Detached-segment selection heuristic:
- `ColdOldestUnpinnedFirst` with 85%/75% hysteresis and default weights (`age=0.6`, `read_heat=0.3`, `size=0.1`).

## Detailed Implementation Plan

### Phase 0 - File Format and Contracts
- Define binary structs for:
- segment header
- frame header
- optional accelerator index entry (`segment.idx`)
- catalog entry
- commit-log entry
- Define forward-compatibility/versioning policy.
- Freeze locator validation rules.
- Freeze pattern-adapter ingest contract (`publish_subscribe`; `pipeline` later) and source-identity fields.
- Freeze ack-level contract (`Accepted`, `DurableData`, `DurableDataAndCommitLog`) and timeout/error semantics.
- Freeze crash/power-loss contract by persistence mode and ack level.
- Freeze out-of-space failure policy and degraded-state transitions.
- Freeze runtime defaults and replay-rate mode semantics.

**Exit Criteria**
- All Phase 0 requirement IDs tracked in `log-archive-v2-traceability.md` are `Covered`; none are `Partial` or `Gap`.
- Canonical binary header fixtures and decode/validation tests are committed and passing in CI.
- Major/minor compatibility and must-understand flag behavior is covered by automated tests.
- Format contracts are frozen at explicit major/minor values and incompatible changes require major bump plus migration note.
- Any non-Phase-0 requirements are tracked in their corresponding phase requirement IDs and are not counted as open Phase 0 gaps.

### Phase 1 - Recorder Core (Completed 2026-02-08)
- Implement segment `.data` writer with aligned append.
- Implement rolling and `.meta` seal flow.
- Implement segment and metadata-log preallocation with spare-segment handoff.
- Implement `Volatile`, `Async`, `Sync` durability modes.
- Add checksum write path.
- Implement failure-policy actions for out-of-space and preallocation failure.
- Implement bytes-written accounting and amplification ratio metrics.
- Implement `publish_subscribe` adapter on top of recorder core ingest contract.

**Exit Criteria**
- All `LA2-P1-*` requirement IDs are `Covered` in `log-archive-v2-traceability.md`.
- `cargo test -p iox2-log-archive-core -- --nocapture` passes, including recorder/replayer integration tests and recorder unit tests.
- Deterministic out-of-space behavior is verified through fault-injection style tests (`ENOSPC` path) with explicit degraded/error state checks.
- Metadata-log preallocation behavior is verified by tests that confirm replay correctness with preallocated zero-tail bytes.
- Recorder stats/accounting fields are asserted in tests for payload/data/metadata bytes and amplification ratio behavior.

### Phase 2 - Replayer Core (Completed 2026-02-08)
- Implement sequence replay APIs (`read_at_sequence`, `read_range`, `seek/next`) for sequence-capable adapters.
- Implement `read_at_locator` and `read_many_locators`.
- Implement checksum verification and corruption error types.
- Implement replay I/O budget controls required for ingest isolation.

**Exit Criteria**
- All `LA2-P2-*` requirement IDs are `Covered` in `log-archive-v2-traceability.md`.
- `cargo test -p iox2-log-archive-core -- --nocapture` passes with sequence replay, locator replay, order-preserving `read_many_locators`, checksum validation, and replay budget tests.
- Negative locator-path tests verify missing segment and frame-bounds/length mismatch handling with explicit errors.
- Replay-vs-ingest performance envelope requirements are covered by Phase 6 hardening/performance gates.

### Phase 3 - Recovery and Checkpointing (Completed 2026-02-08)
- Implement startup recovery:
- catalog load
- active segment tail scan+truncate
- commit-log replay hooks
- Implement deterministic recovery metrics and admin status.

**Exit Criteria**
- Recovery crash matrix is automated and passing for all supported persistence modes (`Volatile`, `Async`, `Sync`) across at least:
- crash during append
- crash during roll/seal
- crash during commit-log write
- Recovery validates prefix safety: no hole or reordering before reported durable boundary.
- Recovery emits deterministic status fields (last durable sequence/ordinal, truncation events, degraded reason when applicable).
- Recovery-time SLO evidence is captured and committed for multiple segment counts and retained byte sizes, with pass/fail against the documented formula.

### Phase 4 - Retention and Tier Arbitration (Completed 2026-02-08)
- Implement global retention arbiter with size cap.
- Add detached cold-segment lifecycle (detach/attach/delete) and tier-state tracking.
- Enforce replay pin/snapshot constraints during trim.

**Exit Criteria**
- Retention arbiter tests prove deterministic oldest-first behavior for equivalent inputs.
- Attach/detach/delete lifecycle tests prove idempotency and correct state transitions across retries.
- Replay pin/snapshot safety tests prove pinned segments are not deleted or detached in ways that violate replay contract.
- Tier-state and retention outcomes are observable via admin status fields and verified by automated tests.
- Capacity enforcement is verified under sustained ingest with deterministic policy behavior at/over watermarks.

### Phase 5 - Metadata Integration and Tooling (Completed 2026-02-08)
- Publish metadata schema contract keyed by locator.
- Implement continuous `commit.idxlog` live-indexer mode with durable watermark tracking.
- Implement query watermark reporting (`last_commit_ordinal`, `last_indexed_commit_ordinal`).
- Implement optional `core-locator.idx` path for immediate locator queries without external DB dependency.
- Provide SQLite reference sink as an external userland adapter crate for `commit.idxlog` ingestion.
- Add end-to-end query-to-replay example and troubleshooting guidance.

**Exit Criteria**
- Metadata schema contract (locator-first fields, versions, and compatibility notes) is published and checked into the repo.
- Live indexer catch-up and restart tests pass with monotonic watermark progression and idempotent reindex behavior.
- Queries beyond watermark return explicit `NotIndexedYet` (or equivalent) in automated tests; no silent partial success.
- `commit.idxlog`-to-index ingestion path supports both offline and continuous modes and is covered by tests.
- End-to-end query-to-replay example is committed, runnable, and validated in CI.

**Troubleshooting Guidance (Phase 5)**
- `NotIndexedYet` query result:
- verify `last_commit_ordinal` and `last_indexed_commit_ordinal` from indexer/admin status.
- run indexer catch-up until `query_watermark` reaches requested sequence/locator boundary.
- Sequence is below watermark but still unavailable:
- treat as out-of-retention (`NotAvailable`) and validate retained segment tier state.
- confirm detached/trimmed lifecycle actions for the target sequence/locator.
- Restart/catch-up drift:
- ensure `indexer.watermark` is writable and persisted on each successful catch-up cycle.
- run idempotent `reindex` when watermark or index files are suspected stale/corrupted.

### Phase 6 - Hardening and Performance (Completed 2026-02-08)
- Completed in this milestone:
- recorder runtime now uses a backend abstraction that makes `io_uring` the preferred Linux `Async` path with mandatory blocking fallback.
- backend selection is explicitly configurable (`AsyncIoBackend`) and runtime-resolved backend is observable (`EffectiveAsyncIoBackend`).
- recorder data-path writes/flush/sync operations are routed through the backend abstraction.
- backend selection and validation tests cover explicit blocking mode, preferred-mode fallback behavior, and queue-depth validation.
- backend parity coverage validates replay-equivalent semantics between blocking and required-`io_uring` backends.
- sustained-ingest soak coverage validates large append batches and end-to-end replay integrity.
- ack-level contracts are implemented with explicit wait APIs, timeout semantics, and durable-position tracking.
- concurrent replay-vs-ingest isolation coverage validates recorder durable progress under replay budget limits.
- corruption-injection coverage validates deterministic checksum mismatch detection.
- reproducible throughput benchmark harness is provided via:
- `crates/core/examples/throughput_profile_benchmark.rs`
- `crates/core/scripts/run_throughput_profile_benchmark.sh`
- reproducible storage baseline (`fio`) harness is provided via:
- `crates/core/scripts/fio_baseline_sequential_write.fio`
- `crates/core/scripts/run_fio_baseline.sh`
- benchmark reports include host/filesystem metadata required by this spec.

**Exit Criteria**
- Backend parity suite passes for `io_uring` and fallback backend with equivalent correctness semantics.
- Corruption-injection tests pass with explicit corruption detection and deterministic error reporting.
- Soak tests complete for sustained ingest and replay validation without data-loss/corruption regressions in automated coverage.
- Throughput profile benchmarks are reproducible from checked-in scripts and include environment metadata (filesystem, kernel, hardware profile).
- Throughput acceptance checks (`>=80%` of baseline where applicable) are executed using the benchmark harness and deployment profile metadata.
- No open Sev-1/Sev-2 recorder/replayer correctness bugs at phase exit.

### Phase 7 - CLI and Operations UX (Completed 2026-02-09)
- Add `iox2-log-admin` executable on top of log-admin APIs.
- Add `iox2-log-recorder` executable as the long-running reference data-plane recorder process.
- Implement `start`, `stop`, `status`, `flush`, `trim`, `detach`, `attach`, and `delete-detached`.
- Implement `list-segments`, `inspect-commit-log`, and `inspect-record` introspection commands.
- Implement live ingest loop for `publish-subscribe` pattern capture in `iox2-log-recorder`.
- Add machine-readable output schemas for `status`.
- Add end-to-end CLI tests and help text/documentation.

**Exit Criteria**
- End-to-end CLI tests pass for `start`, `stop`, `status`, `flush`, `trim`, `detach`, `attach`, and `delete-detached`.
- End-to-end CLI tests pass for `list-segments`, `inspect-commit-log`, `inspect-record --at-sequence`, and `inspect-record --at-locator`.
- End-to-end CLI tests pass for `iox2-log-recorder publish-subscribe` bounded recording flows.
- `iox2-log-recorder log` is absent or returns deterministic unsupported errors on `design/log-archive-pubsub-v1`.
- Idempotency tests pass for repeated lifecycle and retention operations.
- Machine-readable output schema is versioned, validated by tests, and stable for documented fields.
- CLI exit codes are deterministic and mapped to documented error classes.
- CLI tests verify deterministic not-available errors for out-of-retention sequence/locator introspection requests.
- Operator documentation is updated with command semantics, examples, and failure-mode troubleshooting.

### Phase 8 - Additional Pattern Adapters (Completed 2026-02-08)
- Implement `publish_subscribe` adapter on recorder core ingest contract.
- Implement `pipeline` adapter on recorder core ingest contract.
- Add adapter-specific metadata/source-identity mapping tests.
- Add end-to-end record/replay validation for `publish_subscribe` and `pipeline`.
- Implement `publish_subscribe` adapter-out rematerialization helper on top of replay APIs.
- Add first-class recorder APIs backed by adapter mapping:
- `append_publish_subscribe_record(PublishSubscribeRecordInput)`
- `append_pipeline_record(PipelineRecordInput)`
- Add deterministic adapter metadata prefix helpers:
- `encode_adapter_user_header(ArchiveSourceMetadata, user_header)`
- `decode_adapter_user_header(user_header)`
- Add first-class pub-sub adapter-out API:
- `PubSubRematerializerBuilder`
- `PubSubRematerializer::rematerialize_{sequence,locator,range,locators}`
- Add source-pattern dispatching adapter-out API:
- `ArchiveRematerializerBuilder`
- `ArchiveRematerializer::rematerialize_{sequence,locator,range,locators}`
- `ArchiveRematerializer` routes by recorded source metadata and returns deterministic missing-pattern errors unless explicitly configured to skip unconfigured patterns.

**Exit Criteria**
- Adapter conformance suites pass for both `publish_subscribe` and `pipeline`.
- Source-identity and metadata mapping tests prove each adapter populates canonical archive fields correctly.
- Cross-pattern replay behavior is validated for sequence-capable and locator-only paths.
- Query/readiness reporting is consistent across adapters and verified by automated tests.
- Adapter-out rematerialization tests pass for:
- pub-sub rematerialization payload/user-header recovery and pattern filtering.
- deterministic unsupported behavior for retired Log destinations when exposed by compatibility CLI surfaces.
- source-pattern dispatching and deterministic missing-pattern error behavior for configured adapters.
- Traceability matrix includes adapter-specific requirement IDs and marks them `Covered` at phase exit.

## Resolved Decisions
- Default persistence mode is `Async`; `Sync` is opt-in for stricter durability.
- Recorder/replayer controls live in a dedicated log-admin control surface.
- Retention is globally arbitrated across tiers.
- Minimum replay-safety segment metadata is defined in `Replay Safety Metadata (Minimum Set)`.
- Mmap-based replay views are intentionally out of current scope until functional replay stability.
- Checksum policy is configured per service instance in v2 (not per segment generation).
- Default checksum policy is `Crc32c` with runtime hardware acceleration and software fallback.
- Async recorder implementation is `io_uring`-first on Linux with mandatory fallback backend for non-supporting environments.
- `RecordFrame` encoding uses explicit little-endian helpers (no raw struct casts for on-disk decode).
- `segment.idx` is optional and disabled by default in v2 baseline.
- `read_many_locators` preserves input order.
- Default replay mode is `AsFastAsPossible`; paced replay modes are opt-in.
- Recorder control is exposed through `iox2-log-admin` commands backed by log-admin APIs.
- Recorder supports independent `metadata_log_path` (including separate volume) with independent roll and size-cap controls.
- High-throughput `Throughput` profile prioritizes recorder ingest throughput (`Async`, preallocation, deep batching) over immediate metadata freshness.
- Maximum ingest recommendation is `CommitLogOnly`; `Hybrid` is available when metadata freshness is required.
- Record framing integrity (`record_crc32c`) is mandatory; payload checksum policy is independently configurable.
- Segment growth in steady state is prevented via preallocation requirements.
- Direct-I/O is optional, manual-only, and enabled only with benchmark-validated benefit and strict alignment conformance.
- Ack-level semantics, crash/power-loss semantics, out-of-space policy, replay isolation, and write-amplification accounting are treated as core architecture contracts.
- `commit.idxlog` is the canonical metadata WAL; query readiness is governed by explicit indexing watermarks.
- Internal recorder sharding is intentionally out of current v2 scope; scaling is via external parallelism/striping and multiple recorder instances.
- Tiering in v2 is based on detached sealed segments for cold storage management.
- Default detached access policy is `ExplicitAttachRequired`.
- Default out-of-space failure policy is `FailWriter`.
- Recorder core is pattern-neutral; the active branch uses `publish_subscribe` first, defers `pipeline`, and retires `Log`.
- Pattern-specific ingest is exposed as first-class recorder APIs while still mapping through a shared adapter contract internally.
- Rematerialization supports pattern-faithful adapter-out via dedicated pattern rematerializers and a source-pattern dispatching facade.

## Open Items
- None.
