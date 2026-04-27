# Log Archive PubSub-Only V1 Plan

## Status
- Implemented
- Last updated: 2026-04-27
- Branch: `design/log-archive-pubsub-v1`
- Base: `main` (upstream HEAD)

## Objective
Deliver a pure userland high-rate archive recorder/replay stack that works with `publish_subscribe` only.
V1 intentionally does not introduce, depend on, or preserve a core `Log` messaging pattern.

## Scope
### In Scope (V1)
- Recorder data-plane ingest from `publish_subscribe`.
- Archive file format and commit metadata WAL (`commit.idxlog`).
- Incremental indexing and query CLI.
- Replay CLI with:
- selector replay to `stdout`
- rematerialization to `publish_subscribe`
- Deterministic machine-readable errors and status outputs.

### Retired / Out of Scope
- Core `Log` messaging pattern, including `Appender`, `Tailer`, and `MessagingPattern::Log`.
- Recorder ingest from `log` (`tailer`) services.
- Replay/rematerialize to a `log` destination.

### Deferred (Later Updates)
- Pipeline ingest adapters and pipeline rematerialization.

## Normative Language
The key words `MUST`, `SHOULD`, `MAY` are to be interpreted as described in RFC 2119 / RFC 8174 when capitalized.

## V1 Requirements
- `PSV1-001`: Implementation MUST remain userland-only.
- `PSV1-002`: Recorder MUST ingest `publish_subscribe` dynamic payloads and user headers.
- `PSV1-003`: Recorder MUST write append-only archive data and `commit.idxlog` metadata WAL.
- `PSV1-004`: Query/index MUST support incremental checkpointed indexing.
- `PSV1-005`: Replay MUST support selectors (`sequence`, `range`, `locator`, `stdin` selectors).
- `PSV1-006`: Replay MUST support destinations `stdout` and `publish_subscribe`.
- `PSV1-007`: Query output MUST pipe directly into replay (`query | replay selectors --stdin`).
- `PSV1-008`: CLI error model MUST be deterministic with stable exit codes.
- `PSV1-009`: Retired or deferred adapters MUST be absent or fail explicitly as unsupported in V1.

## Implementation Phases
### Phase 0: Contract Freeze
- Define V1 boundaries and deferred adapters in docs and CLI help text.
- Exit criteria:
- V1/deferred scope is explicit and unambiguous.

### Phase 1: Core Archive Runtime (Userland)
- Implement/port userland archive writer/reader core and commit WAL handling.
- Ensure `publish_subscribe` input mapping into canonical archive records.
- Exit criteria:
- Record + read roundtrip passes for pubsub-origin records.

### Phase 2: Recorder CLI (PubSub Only)
- Implement long-running recorder daemon for `publish_subscribe`.
- Remove or gate non-V1 ingest patterns with explicit unsupported errors.
- Exit criteria:
- Recorder reliably captures live pubsub traffic with deterministic summary output.

### Phase 3: Query + Index CLI
- Implement checkpointed indexer, status, and locate/align query surfaces.
- Add schema compatibility checks and single-writer lock behavior.
- Exit criteria:
- Query/index tests pass, including lock contention and schema guard behavior.

### Phase 4: Replay CLI (Stdout + PubSub)
- Implement replay selectors and rematerialization to pubsub.
- Remove or gate non-V1 replay destinations with explicit unsupported errors.
- Exit criteria:
- Replay tests pass for selectors, pacing, missing-record policy, and pubsub destination.

### Phase 5: End-to-End + Robustness
- Add e2e shell-pipe tests (`iox2-log-query ... | iox2-log-replay ...`).
- Validate deterministic error payloads and operator-facing summaries.
- Exit criteria:
- End-to-end tests pass in CI for V1 scope.

### Phase 6: Documentation + Traceability
- Publish V1 README/ops docs and update traceability matrix (`PSV1-*`).
- Document retired Log surfaces and forward roadmap for pipeline adapters.
- Exit criteria:
- No unresolved V1 requirements; deferred items tracked explicitly.

## Progress Tracker
- [x] Phase 0 complete
- [x] Phase 1 complete
- [x] Phase 2 complete
- [x] Phase 3 complete
- [x] Phase 4 complete
- [x] Phase 5 complete
- [x] Phase 6 complete

## Open Decisions
- Resolved: compatibility target is upstream `main` / HEAD.
- Resolved: keep dedicated binaries (`iox2-log-recorder`, `iox2-log-admin`, `iox2-log-query`, `iox2-log-replay`) and gate deferred paths with explicit unsupported errors.
- Resolved: retire the core `Log` messaging pattern from this branch; pub/sub is the V1 data plane.
