# Extraction Plan

## Status

- Implemented as standalone workspace.
- Current dependency baseline: adjacent `../iceoryx2/iceoryx2` path dependency.
- Next dependency target: released `iceoryx2` crate or pinned upstream git tag.

## Workspace Shape

- `crates/core`: archive format, segment writer, metadata WAL, replay, retention, and metadata contracts.
- `crates/iceoryx2`: iceoryx2 integration adapter surface.
- `crates/sqlite`: SQLite metadata indexing/query backend.
- `crates/cli`: recorder, control, admin, query, and replay binaries.

## Remaining Decoupling Work

- Replace the path dependency on `../iceoryx2/iceoryx2` with the selected stable upstream dependency.
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
