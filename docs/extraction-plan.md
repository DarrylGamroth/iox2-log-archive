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
- Audit direct use of dynamic type-detail internals and either avoid them or upstream a small stable API hook.
- Update `iox2-log-archive-orchestrator` release packaging to consume these external binaries.

## Verification

```bash
cargo check --workspace
cargo test --workspace --no-run
cargo test -p iox2-log-archive-core --tests
cargo test -p iox2-log-archive-sqlite --tests
cargo test -p iox2-log-archive-iceoryx2 --tests
```
