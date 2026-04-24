# iox2-log-archive

Standalone high-rate recorder/replay tooling for iceoryx2 data.

This repository is intentionally outside the upstream `iceoryx2` workspace. The
archive core is userland code; iceoryx2 integration is treated as an adapter over
the public `iceoryx2` API.

The supported recorder transport is iceoryx2 `publish_subscribe`. The retired
core `Log` messaging pattern is intentionally not part of this repository's
active scope.

## Workspace

- `crates/core`: archive format, segment writer, metadata WAL, replay, retention, benchmarks. This crate has no `iceoryx2` dependency.
- `crates/iceoryx2`: iceoryx2 publish-subscribe integration adapters.
- `crates/sqlite`: SQLite metadata indexing and query backend.
- `crates/cli`: recorder, control, admin, query, and replay binaries.
- `docs`: extracted design documents from the original fork branch.

## Build

```bash
cargo check --workspace
cargo test --workspace
```

The extraction currently pins `iceoryx2` to upstream commit
`3107941ba2a40f2897c395289447d0f93664ad8c`. Replace that dependency with a
released crate version when the required public APIs are available in a release.

## Recorder

The primary binary is `iox2-log-recorder`:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-recorder -- \
  --format JSON \
  publish-subscribe \
  --service My/Camera/Frames \
  --storage-path /var/lib/iox2-log-archive/storage \
  --metadata-log-path /var/lib/iox2-log-archive/metadata \
  --profile balanced \
  --mode async
```

The recorder handles `SIGINT`/`SIGTERM` cooperatively: it exits the ingest loop,
finalizes the archive, and emits the normal summary payload with
`stop_reason = "ShutdownRequested"`.

Production tuning options are exposed as optional overrides. If omitted, the
selected profile supplies defaults:

- `--async-io-backend io-uring-preferred|io-uring-required|blocking`
- `--io-uring-queue-depth <n>`
- `--io-submit-batch-max <n>`
- `--io-cqe-batch-max <n>`
- `--io-uring-register-files true|false`
- `--checksum-mode none|crc32c`
- `--out-of-space-policy fail-writer`
- `--metadata-log-roll-bytes <bytes>`
- `--metadata-log-max-bytes <bytes>`

## Operations

See:

- `docs/operator-guide.md` for recorder/control/query/replay operation.
- `docs/release-readiness.md` for compatibility, packaging, and release gates.
- `docs/iceoryx2-api-audit.md` for the remaining upstream API stabilization
  dependency.
