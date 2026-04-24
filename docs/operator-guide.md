# Operator Guide

## Scope

`iox2-log-archive` records iceoryx2 `publish_subscribe` traffic. The retired
core `Log` messaging pattern is intentionally unsupported.

## Binaries

- `iox2-log-recorder`: long-running pub-sub recorder worker.
- `iox2-log-control`: live recorder status/pause/resume/flush/stop client.
- `iox2-log-query`: incremental metadata indexing and selector queries.
- `iox2-log-replay`: stdout replay and pub-sub rematerialization.
- `iox2-log-admin`: archive status, retention, and introspection.

## Recorder Shutdown

The recorder installs a signal handler for `SIGINT` and `SIGTERM`. A signal sets
a cooperative shutdown flag; the recorder stops ingesting, finalizes the archive,
and prints its normal summary. Operators should prefer this path over process
kill because it preserves the archive recovery boundary and summary output.

Control-plane stop is also supported:

```bash
iox2-log-control --format JSON stop --service My/Camera/Frames
```

## Recorder Tuning

Profiles provide safe defaults. Override individual knobs only when a workload
or platform requires it:

```bash
iox2-log-recorder --format JSON publish-subscribe \
  --service My/Camera/Frames \
  --storage-path /var/lib/iox2-log-archive/storage \
  --metadata-log-path /var/lib/iox2-log-archive/metadata \
  --profile throughput \
  --mode async \
  --async-io-backend io-uring-required \
  --io-uring-queue-depth 1024 \
  --io-submit-batch-max 256 \
  --io-cqe-batch-max 512 \
  --io-uring-register-files true \
  --metadata-log-roll-bytes 4294967296 \
  --metadata-log-max-bytes 34359738368
```

Use `--async-io-backend blocking` for portability tests or platforms where
`io_uring` is unavailable. Use `io-uring-required` only when failing fast is
preferable to silently falling back.

## Query And Replay

Index metadata into SQLite:

```bash
iox2-log-query --format JSON index catch-up \
  --stream-id My/Camera/Frames \
  --metadata-log-path /var/lib/iox2-log-archive/metadata \
  --db-path /var/lib/iox2-log-archive/query.sqlite
```

Query selectors and rematerialize them to a pub-sub service:

```bash
iox2-log-query --format JSON query locate-range \
  --db-path /var/lib/iox2-log-archive/query.sqlite \
  --stream-id My/Camera/Frames \
  --from 1 \
  --count 100 |
iox2-log-replay --format JSON selectors \
  --storage-path /var/lib/iox2-log-archive/storage \
  --metadata-log-path /var/lib/iox2-log-archive/metadata \
  --stdin \
  --to publish-subscribe \
  --service My/Camera/Frames/Replay
```

## Orchestrator

The recommended multi-service deployment model is process-per-recorder managed
by `iox2-log-orchestrator` from the sibling `iox2-log-archive-orchestrator`
repository. Configure `IOX2_LOG_ORCH_RECORDER_BIN` and
`IOX2_LOG_ORCH_CONTROL_BIN` to point at the installed binaries from this
repository.
