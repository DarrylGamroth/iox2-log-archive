# Orchestrator Integration

`iox2-log-archive` owns the recorder data path and archive tools.
`iox2-log-archive-orchestrator` owns single-host desired-state management for
many recorder workers.

The orchestrator is intentionally a control-plane package. It does not replace
the recorder, query indexer, replay tool, or archive admin tool from this
repository.

## Roles

`iox2-log-archive` provides:

- `iox2-log-recorder`: subscribes to iceoryx2 pub-sub and writes archive data.
- `iox2-log-control`: sends live status, pause, resume, flush, and stop requests.
- `iox2-log-admin`: performs offline archive inspection and retention actions.
- `iox2-log-query`: builds and queries the SQLite metadata index.
- `iox2-log-replay`: replays records to stdout or pub-sub.

`iox2-log-archive-orchestrator` provides:

- `iox2-log-orchestrator serve`: long-running daemon.
- Desired state stored as TOML.
- One subprocess per recorder worker.
- Periodic and command-triggered reconciliation.
- Worker status and stop operations mediated through `iox2-log-control`.

## Data And Control Flow

```text
live publishers
  -> iceoryx2 publish-subscribe service
  -> iox2-log-recorder worker
  -> archive segments + metadata WAL

iox2-log-orchestrator CLI
  -> orchestrator request-response control service
  -> orchestrator daemon
  -> iox2-log-recorder subprocesses

orchestrator daemon
  -> iox2-log-control
  -> recorder worker control service

iox2-log-query / iox2-log-replay / iox2-log-admin
  -> archive files after or during recording
```

The orchestrator is not in the data path between publishers and recorders. That
keeps hot-path throughput governed by the recorder configuration and the
iceoryx2 source service, not by control-plane reconciliation.

## Build And Install

Build the archive binaries:

```bash
cd /path/to/iox2-log-archive
cargo build --release -p iox2-log-archive-cli --bins
```

Build the orchestrator from the sibling repository:

```bash
cd /path/to/iox2-log-archive-orchestrator
cargo build --release
```

Point the orchestrator at stable archive binary paths:

```bash
export IOX2_LOG_ORCH_RECORDER_BIN=/opt/iox2-log-archive/bin/iox2-log-recorder
export IOX2_LOG_ORCH_CONTROL_BIN=/opt/iox2-log-archive/bin/iox2-log-control
```

You can also pass the recorder and control binary paths through orchestrator
CLI/config flags when that is preferable for a deployment.

## Quickstart

Use shared environment for daemon and client commands:

```bash
export IOX2_LOG_ORCH_STATE_PATH=/var/lib/iox2-log-orchestrator/state.toml
export IOX2_LOG_ORCH_CONTROL_SERVICE=iox2/log/archive/orchestrator/control
```

Start the daemon:

```bash
iox2-log-orchestrator --format JSON \
  serve
```

Enable a recorder-managed service:

```bash
iox2-log-orchestrator --format JSON \
  enable \
  --service My/Camera/Frames \
  --instance default \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --profile throughput \
  --mode async \
  --async-io-backend io-uring-required \
  --io-uring-queue-depth 256 \
  --io-submit-batch-max 256 \
  --io-cqe-batch-max 512
```

Inspect and control the managed worker:

```bash
iox2-log-orchestrator --format JSON status --service My/Camera/Frames
iox2-log-orchestrator --format JSON pause --service My/Camera/Frames
iox2-log-orchestrator --format JSON resume --service My/Camera/Frames
iox2-log-orchestrator --format JSON stop --service My/Camera/Frames
iox2-log-orchestrator --format JSON shutdown
```

If you omit `--state-path` or `--control-service` from client commands, make
sure the same values are supplied through environment variables or config.

## Desired State

The orchestrator persists intent in `state.toml`. A representative service
entry looks like this:

```toml
version = 1

[services."My/Camera/Frames"]
enabled = true
paused = false
instance = "default"
generation = 1
storage_path = "/var/lib/iox2-log-archive/My_Camera_Frames/storage"
metadata_log_path = "/var/lib/iox2-log-archive/My_Camera_Frames/metadata"
profile = "throughput"
mode = "async"
cycle_time_ms = 10
flush_interval_ms = 100
async_io_backend = "io-uring-required"
io_uring_queue_depth = 256
io_submit_batch_max = 256
io_cqe_batch_max = 512
io_uring_register_files = true
checksum_mode = "crc32c"
out_of_space_policy = "fail-writer"
metadata_log_roll_bytes = 4294967296
metadata_log_max_bytes = 34359738368
```

The daemon reconciles this desired state by spawning a recorder subprocess with
the equivalent `iox2-log-recorder publish-subscribe` arguments.

## Current Compatibility Notes

The orchestrator supports the recorder profiles
`durable|balanced|throughput|replay`. Use `profile = "throughput"` for high-rate
large-payload services, then size borrowed-sample capacity explicitly for the
application if the external-payload fast path is required.

The orchestrator also does not currently pass
`--subscriber-max-borrowed-samples`, `--source-service-id`, `--segment-bytes`,
`--spare-preallocated-segments`, or `--segment-preallocate` to the recorder.
That means it can manage high-throughput services through `profile =
"throughput"` using the profile defaults, but it cannot express per-service
overrides for those fields yet.

For workloads that need non-default fast-path or segment sizing, use one of
these approaches:

- Extend the orchestrator service spec to pass
  `--subscriber-max-borrowed-samples` and the segment/preallocation overrides.
- Ensure the already-created source service exposes enough borrowed samples for
  the recorder's fast-path capacity check.

Query, replay, export, and archive maintenance remain separate file-oriented
workflows. The orchestrator should own recorder lifecycle; it should not own
SQL query semantics or FITS/materialization policy.

For large payload streams, do not hide borrowed-sample sizing in a generic
deployment preset. In iceoryx2, borrowed-sample capacity contributes to
publisher data-segment capacity and is multiplied by configured
`max_subscribers`. A 1000 FPS camera workload may legitimately need deep
borrowed-sample capacity, but that should be declared in the service spec once
the orchestrator supports the field.

## Operational Pattern

Use this split in production:

- Use the orchestrator to keep the desired set of recorder workers running.
- Use `iox2-log-control` directly when diagnosing one live worker.
- Use `iox2-log-query index run` as a separate long-lived service if low-latency
  query availability is required.
- Use `iox2-log-replay` or future exporters by piping selectors from
  `iox2-log-query`.
- Use `iox2-log-admin` for retention and archive inspection.

This keeps the write path simple: publishers, iceoryx2 pub-sub, recorder,
archive files. Everything else is a control-plane or read-side concern.
