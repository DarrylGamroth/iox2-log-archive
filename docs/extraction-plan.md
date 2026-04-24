# Extraction Plan

## Status

- Implemented as standalone workspace.
- Current dependency baseline: upstream `eclipse-iceoryx/iceoryx2` pinned to `3107941ba2a40f2897c395289447d0f93664ad8c`.
- Next dependency target: released `iceoryx2` crate version.

## Workspace Shape

- `crates/core`: archive format, segment writer, metadata WAL, replay, retention, and metadata contracts.
- `crates/iceoryx2`: iceoryx2 integration adapter surface.
- `crates/sqlite`: SQLite metadata indexing/query backend.
- `crates/cli`: recorder, control, admin, query, and replay binaries.

## Remaining Decoupling Work

- Replace the pinned upstream git dependency with the selected released crate version.
- Track the dynamic type-detail API gap in `docs/iceoryx2-api-audit.md` and replace it once upstream exposes a stable hook.
- `iox2-log-archive-orchestrator` consumes these external binaries through configurable recorder/control binary paths.

## Operational Status

- Active transport scope is pub-sub only.
- Recorder shutdown is cooperative for control `stop` and process `SIGINT`/`SIGTERM`.
- Production tuning overrides are exposed in `iox2-log-recorder` and can be passed through the orchestrator desired state.
- Release gates and compatibility policy are documented in `docs/release-readiness.md`.

## Verification

```bash
cargo check --workspace
cargo test --workspace --no-run
cargo test -p iox2-log-archive-core --tests
cargo test -p iox2-log-archive-sqlite --tests
cargo test -p iox2-log-archive-iceoryx2 --tests
```
