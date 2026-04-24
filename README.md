# iox2-log-archive

Standalone high-rate recorder/replay tooling for iceoryx2 data.

This repository is intentionally outside the upstream `iceoryx2` workspace. The
archive core is userland code; iceoryx2 integration is treated as an adapter over
the public `iceoryx2` API.

## Workspace

- `crates/core`: archive format, segment writer, metadata WAL, replay, retention, benchmarks. This crate has no `iceoryx2` dependency.
- `crates/iceoryx2`: iceoryx2 publish-subscribe integration adapters.
- `crates/sqlite`: SQLite metadata indexing and query backend.
- `crates/cli`: recorder, control, admin, query, and replay binaries.
- `docs`: extracted design documents from the original fork branch.

## Build

```bash
cargo check --workspace
```

The extraction currently uses a path dependency on `../iceoryx2/iceoryx2`.
Replace that dependency with a released crate or pinned upstream git revision
when the supported iceoryx2 baseline is selected.
