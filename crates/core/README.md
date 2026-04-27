# Log Archive (Userland)

`iox2-log-archive-core` is the high-rate recorder/replayer crate for
iceoryx2 data, implemented in userland.

Location: `crates/core`

## Capabilities

- Persistent archive layout: `catalog.bin`, `segments/`, `detached/`, `commit.idxlog`
- Recorder profiles: `durable`, `balanced`, `throughput`, `replay`
- Persistence modes: `volatile`, `async`, `sync`
- Async backend policy: io_uring preferred/required, or blocking fallback
- Recorder/replay surfaces for canonical archive records; iceoryx2 `publish_subscribe` capture and rematerialization live in adapter/CLI crates
- `log` and `pipeline` adapters are deferred to later updates
- Metadata indexer (`commit.idxlog` tail + persisted watermark)
- CLI tools: `iox2-log-recorder`, `iox2-log-control`, `iox2-log-admin`, `iox2-log-query`, `iox2-log-replay`

## Build

From repository root:

```bash
cargo build -p iox2-log-archive-core
cargo build -p iox2-log-archive-cli --bin iox2-log-recorder
cargo build -p iox2-log-archive-cli --bin iox2-log-control
cargo build -p iox2-log-archive-cli --bin iox2-log-admin
cargo build -p iox2-log-archive-cli --bin iox2-log-query
cargo build -p iox2-log-archive-cli --bin iox2-log-replay
```

## Runtime Model

- `iox2-log-recorder` is the long-running reference recorder process for live ingest (`publish_subscribe`).
- The recorder data plane is also available directly through the Rust API in this crate.
- `iox2-log-control` is the live daemon control CLI (`status`, `flush`, `pause`, `resume`, `stop`) via request-response.
- `iox2-log-admin` is the one-shot archive maintenance/inspection CLI.
- Each `iox2-log-admin` invocation opens the archive, performs one operation, then exits.

Recorder lifecycle:
- `Running <-> Paused -> Stopping`
- `pause` is idempotent and keeps the first pause timestamp.
- `resume` is idempotent and clears the pause timestamp.
- `stop` requests graceful shutdown from either `Running` or `Paused`.

## Operator Runbook (PubSub)

Data flow:
- Publisher writes live `publish_subscribe` samples.
- `iox2-log-recorder` captures samples and appends archive frames.
- `iox2-log-query` indexes `commit.idxlog` into SQLite and emits replay selectors.
- `iox2-log-replay` consumes selectors and rematerializes to `publish_subscribe` or `stdout`.
- Export tools should consume the same selector stream; use expanded locator selectors
  when exact query membership matters.

Example variables:

```bash
export SERVICE="My/Camera/Service"
export STREAM_ID="$SERVICE"
export STORAGE_PATH="/tmp/iox2-archive/storage"
export METADATA_PATH="/tmp/iox2-archive/metadata"
export DB_PATH="/tmp/iox2-archive/index.sqlite"
```

Optional archive initialization/control-plane call:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  start \
  --service "$SERVICE" \
  --storage-path "$STORAGE_PATH" \
  --metadata-log-path "$METADATA_PATH" \
  --profile throughput \
  --mode async
```

Terminal A, start live recorder (runs until stopped, timeout, or `--max-messages` reached):

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-recorder -- \
  --format JSON \
  publish-subscribe \
  --service "$SERVICE" \
  --storage-path "$STORAGE_PATH" \
  --metadata-log-path "$METADATA_PATH" \
  --profile throughput \
  --mode async
```

Terminal B, index what has been recorded so far:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-query -- \
  index catch-up \
  --stream-id "$STREAM_ID" \
  --metadata-log-path "$METADATA_PATH" \
  --db-path "$DB_PATH"
```

Terminal B, query selectors and replay them to `publish_subscribe`:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-query -- \
  --format JSON \
  query locate-range \
  --db-path "$DB_PATH" \
  --stream-id "$STREAM_ID" \
  --from 1 \
  --count 10 \
  --emit selectors \
| cargo run -p iox2-log-archive-cli --bin iox2-log-replay -- \
    --format JSON \
    replay \
    --storage-path "$STORAGE_PATH" \
    --metadata-log-path "$METADATA_PATH" \
    --to publish-subscribe \
    --service "$SERVICE" \
    selectors --stdin --selector-format ndjson
```

Replay every available record in sequence order:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-replay -- \
  --format JSON \
  replay \
  --storage-path "$STORAGE_PATH" \
  --metadata-log-path "$METADATA_PATH" \
  --to publish-subscribe \
  --service "$SERVICE" \
  all
```

Terminal B, or replay same selectors to `stdout` for inspection:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-query -- \
  --format JSON \
  query locate-range \
  --db-path "$DB_PATH" \
  --stream-id "$STREAM_ID" \
  --from 1 \
  --count 3 \
  --emit selectors \
| cargo run -p iox2-log-archive-cli --bin iox2-log-replay -- \
    --format JSON \
    replay \
    --storage-path "$STORAGE_PATH" \
    --metadata-log-path "$METADATA_PATH" \
    --to stdout \
    selectors --stdin --selector-format ndjson
```

For export/data-product tools, keep query and archive reads decoupled with the
same pipe boundary. Range queries can emit exact locator selectors; a future
FITS exporter can consume that stream directly:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-query -- \
  --format JSON \
  query locate-range \
  --db-path "$DB_PATH" \
  --stream-id "$STREAM_ID" \
  --from 1 \
  --count 100 \
  --emit selectors \
  --expand-selectors \
| iox2-log-export-fits \
    --storage-path "$STORAGE_PATH" \
    --metadata-log-path "$METADATA_PATH" \
    --output-dir /data/fits \
    --selectors-stdin
```

## Quickstart Example

Run the end-to-end example (`record -> index -> replay`):

```bash
cargo run -p iox2-log-archive-core --example query_to_replay
```

Optional output location:

```bash
IOX2_LOG_ARCHIVE_EXAMPLE_ROOT=/tmp/iox2-log-archive-demo \
  cargo run -p iox2-log-archive-core --example query_to_replay
```

## Rust API Usage

Minimal record + replay:

```rust
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, ArchiveReplayerBuilder, ChecksumMode, PersistenceMode,
    PublishSubscribeRecordInput,
};

let storage_path = std::path::Path::new("/tmp/iox2-log-archive/storage");
let metadata_path = std::path::Path::new("/tmp/iox2-log-archive/metadata");

let mut recorder = ArchiveRecorderBuilder::new(storage_path)
    .metadata_log_path(metadata_path)
    .segment_bytes(1024 * 1024)
    .persistence_mode(PersistenceMode::Async)
    .checksum_mode(ChecksumMode::Crc32c)
    .create()?;

recorder.append_publish_subscribe_record(PublishSubscribeRecordInput {
    event_time_ns: 1_000,
    source_service_id: 0xA11CE,
    source_publisher_id: 0xBEEF,
    source_sequence: Some(1),
    user_header: &[0xA1, 0x01],
    payload: b"frame-1",
})?;

recorder.finalize()?;

let replayer = ArchiveReplayerBuilder::new(storage_path)
    .metadata_log_path(metadata_path)
    .open()?;

let frame = replayer.read_at_sequence(1)?.expect("sequence exists");
assert_eq!(frame.payload, b"frame-1");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## CLI Usage

Long-running live recorder process (`iox2-log-recorder`):

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-recorder -- \
  --format JSON \
  publish-subscribe \
  --service My/Camera/Service \
  --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata \
  --profile throughput \
  --mode async
```

Daemon control (`iox2-log-control`) and archive inspection (`iox2-log-admin`):

Build once:

```bash
cargo build -p iox2-log-archive-cli --bin iox2-log-recorder
cargo build -p iox2-log-archive-cli --bin iox2-log-control
cargo build -p iox2-log-archive-cli --bin iox2-log-admin
```

Query live daemon status:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-control -- \
  --format JSON \
  status \
  --service My/Camera/Service \
  --timeout-ms 2000
```

Pause/resume, flush, and stop a live daemon:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-control -- \
  pause --service My/Camera/Service --timeout-ms 2000

cargo run -p iox2-log-archive-cli --bin iox2-log-control -- \
  resume --service My/Camera/Service --timeout-ms 2000

cargo run -p iox2-log-archive-cli --bin iox2-log-control -- \
  flush --service My/Camera/Service --timeout-ms 2000

cargo run -p iox2-log-archive-cli --bin iox2-log-control -- \
  stop --service My/Camera/Service --timeout-ms 2000
```

While paused, the recorder keeps running but drops incoming live samples. `status`
reports `is_paused`, `dropped_while_paused`, and `paused_since_ns`.

Optional archive open/recover (offline, no daemon required):

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  start \
  --service My/Camera/Service \
  --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata \
  --profile throughput \
  --mode async
```

Inspect metadata and records (offline via archive files):

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  --format JSON \
  inspect-commit-log \
  --service My/Camera/Service \
  --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata \
  --from-ordinal 1 \
  --limit 10

cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  --format JSON \
  inspect-record \
  --service My/Camera/Service \
  --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata \
  --at-sequence 1
```

Segment retention/tiering operations:

```bash
cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  detach --service My/Camera/Service --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata --before-sequence 1000

cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  attach --service My/Camera/Service --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata

cargo run -p iox2-log-archive-cli --bin iox2-log-admin -- \
  trim --service My/Camera/Service --storage-path /tmp/iox2-archive/storage \
  --metadata-log-path /tmp/iox2-archive/metadata --before-sequence 1000
```

## Tests

Run crate tests:

```bash
cargo test -p iox2-log-archive-core --tests -- --nocapture
```

Run CLI integration tests:

```bash
cargo test -p iceoryx2-cli --test iox2_log_control_cli_tests -- --nocapture
cargo test -p iceoryx2-cli --test iox2_log_recorder_cli_tests -- --nocapture
```

## Throughput and Storage Baselines

Recorder throughput benchmark:

```bash
crates/core/scripts/run_throughput_profile_benchmark.sh /tmp/log-archive-bench
```

`fio` sequential-write baseline (requires `fio`):

```bash
crates/core/scripts/run_fio_baseline.sh /tmp/log-archive-fio
```

Both scripts emit JSON reports with host and storage metadata.

## Current Limits

- Live recording is available via Rust API and `iox2-log-recorder`.
- Live daemon control is available via `iox2-log-control`.
- Archive maintenance/inspection is available via `iox2-log-admin`.
- Dedicated C/C++/Python FFI for log-archive control is not implemented yet.

## Design References

- `doc/design-documents/log-archive-v2.md`
- `doc/design-documents/log-archive-v2-traceability.md`
