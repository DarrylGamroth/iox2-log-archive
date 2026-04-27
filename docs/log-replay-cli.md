# Log Replay CLI Design (`iox2-log-replay`)

## Status
- Implemented
- Last updated: 2026-04-23
- Target branch: `design/log-archive-userland`
- Depends on: `doc/design-documents/log-archive-v2.md`
- Active scope note: narrowed by `doc/design-documents/log-archive-pubsub-v1-plan.md`; Log rematerialization is retired on `design/log-archive-pubsub-v1`.
- Implementation progress: Phase 0 complete, Phase 1 complete, Phase 2 complete, Phase 3 complete, Phase 4 complete, Phase 5 complete.

## Scope
Define a first-class replay CLI for `log-archive` that can:
- replay by sequence/range/locator,
- accept selector streams over stdin for pipeline integration,
- rematerialize to `publish_subscribe` services,
- optionally stream replayed records to stdout for tooling.

This design does not add metadata query semantics into the replay CLI.

## Normative Language
The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHALL`, `SHALL NOT`, `SHOULD`,
`SHOULD NOT`, `RECOMMENDED`, `MAY`, and `OPTIONAL` in this document are to be
interpreted as described in RFC 2119 and RFC 8174 when, and only when, they
appear in all capitals.

## Goals
- Provide a dedicated replay binary with stable operator semantics.
- Keep replay decoupled from recorder daemon/control CLIs.
- Support shell-pipeline workflows where selectors are piped from external tools.
- Preserve archive bytes exactly (payload/user-header) during rematerialization.
- Support replay pacing modes (`fast`, `recorded`, `fixed`).

## Non-Goals
- Implementing a query language in replay CLI.
- Embedding SQLite/Polars/DuckDB in replay CLI.
- Replacing metadata/indexer tooling.
- Changing archive on-disk formats.

## Terminology
- **Selector**: An instruction that identifies one or more records to replay.
- **Locator selector**: Selector keyed by `segment_id`, `segment_generation`, `file_offset`, `frame_len`.
- **Sequence selector**: Selector keyed by archive sequence values.
- **Recorded-rate replay**: Pacing based on `event_time_ns` delta between adjacent emitted records.

## Requirements
- `LRC-001`: A separate executable `MUST` be provided as `iox2-log-replay`.
- `LRC-002`: The CLI `MUST` support one-shot replay operations (no background daemon required).
- `LRC-003`: Selector input `MUST` be expressed as explicit selector subcommands and `MUST` support:
  - `sequence --at <u64>`,
  - `range --from <u64> --count <usize>`,
  - `locator --at <segment_id>:<segment_generation>:<file_offset>:<frame_len>`,
  - `selectors --stdin --selector-format <ndjson|csv>`.
- `LRC-004`: Streamed selectors `MUST` support line-delimited `ndjson`; `csv` support is `SHOULD`.
- `LRC-005`: Replay destination `MUST` support:
  - `publish-subscribe` rematerialization,
  - stdout output for tool chaining.
- `LRC-006`: Default rate mode `MUST` be `fast`.
- `LRC-007`: Optional rate modes `MUST` include `recorded` and `fixed`.
- `LRC-008`: CLI errors and exit codes `MUST` be deterministic and machine-readable.
- `LRC-009`: Replay summary `MUST` include emitted count, skipped count, byte count, and elapsed time.
- `LRC-010`: For stdin selectors, parsing and replay `SHOULD` be incremental (streaming) to avoid loading all selectors into memory.
- `LRC-011`: Snapshot replay MUST remain the default. Live replay MUST be explicit via follow mode.
- `LRC-012`: Follow mode MUST refresh `commit.idxlog` and expose a visible sequence/commit watermark in the replay summary.

## CLI Contract

### Binary
- `iox2-log-replay`

### Primary Command
```text
iox2-log-replay replay [COMMON_OPTIONS] <SELECTOR_COMMAND>
```

### Required Archive Inputs
- `--storage-path <path>`
- `--metadata-log-path <path>` (optional; defaults to storage path)

### Selector Commands
- `sequence --at <u64>`
- `range --from <u64> --count <usize>`
- `locator --at <segment_id>:<segment_generation>:<file_offset>:<frame_len>`
- `selectors --stdin --selector-format ndjson|csv`
- `selectors --file <path> --selector-format ndjson|csv`
- `all`

Design intent:
- A selector command is always required.
- Selector commands are structurally exclusive by CLI shape (no flag-level exclusion matrix).
- `all` replays every available record in archive sequence order.
- `selectors` is the only mode that accepts external multi-record selector streams.

### Live Follow Mode
- `--follow` refreshes `commit.idxlog` while replay is active.
- `--follow` is supported for `all`, `sequence`, and `range`.
- `locator` and selector-stream replay remain snapshot operations.
- `--follow-poll-ms <n>` controls polling interval.
- `--follow-idle-timeout-ms <n>` exits after no new visible records appear for the configured duration.
- Without an idle timeout, `all --follow` is intended to run until interrupted.

### Destinations
- `--to publish-subscribe --service <name>`
- `--to stdout`
- `--to log --service <name>` is retired and absent on `design/log-archive-pubsub-v1`.
- `--to recorded-pattern` (future extension; initial scope MAY defer if pipeline output adapter is unavailable)

### Rate Modes
- `--rate fast` (default)
- `--rate recorded`
- `--rate fixed --messages-per-sec <u64>`

### Additional Controls
- `--skip-missing` (default false)
- `--max-errors <usize>` (default `1` unless `--skip-missing`)
- `--format RON|JSON|YAML` for final summary output

### Examples
```bash
# Replay one sequence to stdout
iox2-log-replay replay \
  --storage-path /data/archive \
  --metadata-log-path /data/meta \
  --to stdout \
  sequence --at 42

# Replay a range to publish-subscribe
iox2-log-replay replay \
  --storage-path /data/archive \
  --metadata-log-path /data/meta \
  --to publish-subscribe --service Cam/A/replay \
  range --from 1000 --count 512

# Replay one locator to publish-subscribe
iox2-log-replay replay \
  --storage-path /data/archive \
  --metadata-log-path /data/meta \
  --to publish-subscribe --service Cam/A/replay \
  locator --at 7:1:4096:1536
```

## Selector Schemas

### NDJSON Selector Schema
One selector per line.

Sequence selector:
```json
{"kind":"sequence","sequence":42}
```

Locator selector:
```json
{"kind":"locator","segment_id":7,"segment_generation":1,"file_offset":4096,"frame_len":1536}
```

Range selector (optional for stdin):
```json
{"kind":"range","from":1000,"count":512}
```

### CSV Selector Schema
Header:
```text
kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len
```

Rules:
- `kind=sequence`: requires `sequence`.
- `kind=range`: requires `from,count`.
- `kind=locator`: requires locator fields.

## Replay Semantics
- For sequence/range selectors, records are read via `ArchiveReplayer` sequence APIs.
- For locator selectors, records are read via `read_at_locator`.
- Ordering `MUST` preserve selector order for locator/stdin modes.
- For range selectors, ordering `MUST` be monotonically increasing sequence order.
- In `recorded` rate mode, inter-message delay `MUST` derive from positive `event_time_ns` deltas and clamp pathological gaps by configurable max sleep.
- In `fixed` mode, pacing is based on target messages/sec using monotonic clock.

## Error Model and Exit Codes
- `0`: success.
- `2`: invalid input (selector syntax, missing required option, illegal mode combinations).
- `3`: not available (missing sequence/locator/service).
- `1`: internal/runtime failure.

Machine-readable error payload should follow current CLI conventions:
```json
{"error_code":"NotAvailable","message":"..."}
```

## Summary Output Contract
Example summary payload:
```json
{
  "operation": "replay",
  "storage_path": "/tmp/archive",
  "metadata_log_path": "/tmp/meta",
  "destination": "publish-subscribe",
  "service": "My/Replay/Service",
  "selector_source": "stdin:ndjson",
  "rate_mode": "fast",
  "selected": 1024,
  "emitted": 1020,
  "skipped_missing": 4,
  "errors": 0,
  "bytes_emitted": 16777216,
  "elapsed_ms": 245
}
```

## Pipeline Integration
The replay CLI is explicitly designed to be fed by external query tools:

```bash
iox2-log-query query locate-window \
  --db-path /data/index/cam_a.sqlite \
  --stream-id Cam/A \
  --start-ns 1700000000000000000 \
  --end-ns 1700000001000000000 \
| iox2-log-replay replay \
    --storage-path /data/archive/Cam-A \
    --metadata-log-path /data/meta/Cam-A \
    --to publish-subscribe --service Cam/A/replay \
    --rate fast \
    selectors --stdin --selector-format ndjson
```

## Implementation Plan

### Phase 0: Contract Freeze (Completed 2026-02-09)
- Add this document and lock command semantics.
- Add selector schema fixtures (`ndjson`, `csv`) under CLI test assets.
- Exit criteria:
  - command options and selector schemas approved,
  - no unresolved naming collisions with existing CLIs.

### Phase 1: CLI Skeleton + Parsing (Completed 2026-02-09)
- Add binary `iceoryx2-cli/iox2-log-replay` with `cli.rs`, `command.rs`, `main.rs`.
- Implement selector subcommands (`sequence`, `range`, `locator`, `selectors`).
- Implement stdin/file parser for NDJSON.
- Exit criteria:
  - deterministic parse errors with exit code `2`,
  - unit tests for selector subcommand parsing and validation.

### Phase 2: Replay Core (Completed 2026-02-09)
- Wire `ArchiveReplayerBuilder` and selection execution.
- Implement destination `stdout` first (no rematerialization dependency).
- Implement summary output payload.
- Exit criteria:
  - replay by sequence/range/locator succeeds on fixture archive,
  - summary fields populated and stable.

### Phase 3: Rematerialization Destinations (Completed 2026-02-09)
- Integrate `PubSubRematerializerBuilder`.
- Add destination-specific validation (`--service` required for service destinations).
- Exit criteria:
  - e2e tests rematerialize to `publish-subscribe` services,
  - retired `log` destination returns deterministic unsupported errors if exposed,
  - payload/user-header bytes match archived frames.

### Phase 4: Rate Control + Streaming Robustness (Completed 2026-02-09; refreshed 2026-04-27)
- Implement `fast`, `recorded`, `fixed` pacing.
- Stream stdin/file selectors incrementally without loading the full selector set into memory.
- Add `--skip-missing` and `--max-errors` handling.
- Exit criteria:
  - recorded/fixed pacing tests pass within tolerance,
  - long stdin streams run without unbounded memory growth.

### Phase 5: Hardening and Docs (Completed 2026-02-09)
- Add CLI integration tests for piped selectors and mixed selector types.
- Add README examples and troubleshooting.
- Update traceability matrix with `LRC-*` requirement IDs.
- Exit criteria:
  - replay CLI tests pass in CI,
  - docs show direct + piped replay workflows.

## Initial Test Matrix
- Direct sequence replay to stdout.
- Direct range replay to stdout.
- Direct locator replay to stdout.
- NDJSON stdin selectors replay to stdout.
- NDJSON stdin selectors rematerialize to log.
- NDJSON stdin selectors rematerialize to publish-subscribe.
- Missing selector behavior with and without `--skip-missing`.
- Deterministic exit codes and machine-readable errors.

## Open Items
- None for the implemented v1 scope.
