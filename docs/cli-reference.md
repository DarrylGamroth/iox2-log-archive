# CLI Reference

This is the practical reference for the `iox2-log-archive` command-line tools.
Use each binary's `--help` output as the final source of truth for exact flag
spelling.

## Common Conventions

All tools support structured output:

```bash
--format JSON
```

Use JSON for scripts and orchestration. RON and YAML are intended for human
inspection.

Common path terms:

- `--service`: iceoryx2 service name, for example `My/Camera/Frames`.
- `--storage-path`: archive segment directory.
- `--metadata-log-path`: append-only metadata log directory.
- `--db-path`: SQLite query index path.

Keep storage, metadata, and query index paths separate per recorded service
unless a higher-level deployment tool owns the layout.

## Recorder

Binary:

```bash
iox2-log-recorder
```

The recorder subscribes to an iceoryx2 publish-subscribe service and writes an
archive segment stream plus metadata WAL.

High-throughput preset:

```bash
iox2-log-recorder --format JSON publish-subscribe \
  --service My/Camera/Frames \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --profile throughput
```

Main recorder options:

| Option | Purpose |
| --- | --- |
| `--service <name>` | Source publish-subscribe service to record. |
| `--storage-path <path>` | Segment directory. Required. |
| `--metadata-log-path <path>` | Metadata WAL directory. If omitted, metadata follows the storage layout defaults. |
| `--profile durable|balanced|throughput|replay` | Selects recorder defaults. |
| `--mode volatile|async|sync` | Overrides persistence mode selected by the profile. |
| `--segment-bytes <bytes>` | Overrides segment size. |
| `--spare-preallocated-segments <n>` | Keeps spare segment files ready for rollover. |
| `--segment-preallocate true|false` | Enables or disables file preallocation. |
| `--max-disk-bytes <bytes>` | Fails recording before exceeding the configured storage budget. |
| `--checksum-mode none|crc32c` | Enables or disables per-frame checksums. |
| `--subscriber-max-borrowed-samples <n>` | Requests subscriber capacity for the external-payload fast path. |
| `--source-service-id <id>` | Stamps a stable source id into metadata. |
| `--max-messages <n>` | Test/benchmark limit. Stops after `n` records. |
| `--timeout-ms <ms>` | Test/benchmark wall-clock limit. |
| `--flush-interval-ms <ms>` | Periodic flush cadence. |
| `--ack-level <level>` | Configures subscriber acknowledgement behavior. |

Async I/O options:

| Option | Purpose |
| --- | --- |
| `--async-io-backend io-uring-preferred|io-uring-required|blocking` | Selects backend behavior. |
| `--io-uring-queue-depth <n>` | Submission/completion queue depth. |
| `--io-submit-batch-max <n>` | Max writes submitted per flush cycle. |
| `--io-cqe-batch-max <n>` | Max completions reaped per cycle. |
| `--io-uring-register-files true|false` | Registers segment files with io_uring where supported. |

Profile guidance:

| Profile | Intended use |
| --- | --- |
| `durable` | Stronger persistence behavior for moderate rates. |
| `balanced` | General default for development and moderate production streams. |
| `throughput` | High-throughput async path with 1 GiB segments and two spare preallocated segments. Borrowed-sample capacity for the external-payload fast path remains an explicit workload tuning knob. |
| `replay` | Archive generation tuned for replay-heavy workflows. |

For the external-payload fast path, the source service must be configured with
enough borrowed-sample capacity. If it is not, the recorder still works but
falls back to the compatible copied path.

### Borrowed-Sample Sizing

Do not treat `--subscriber-max-borrowed-samples` as a harmless queue-depth
setting for large payloads. In iceoryx2, publisher data-segment capacity scales
approximately with:

```text
max_subscribers * (subscriber_max_buffer_size + subscriber_max_borrowed_samples)
  + history_size
  + publisher_max_loaned_samples
```

That means borrowed-sample capacity is effectively multiplied by the configured
subscriber capacity of the service. With 1 MiB frames, 512 borrowed samples is
roughly 516 MiB per publisher when `max_subscribers = 1` and defaults for buffer
and publisher loan counts are small. With `max_subscribers = 8`, the same
setting can require roughly 4 GiB per publisher.

For large camera-like payloads, tune borrowed samples from expected frame rate,
storage tail latency, and acceptable shared-memory budget. For example, a
100 FPS stream with 50 ms storage latency needs only about 5 in-flight payloads
plus margin. A 1000 FPS application may need a much deeper setting, but that
should be an explicit application-level choice.

## Control

Binary:

```bash
iox2-log-control
```

The control tool talks to a live recorder control service. It is for runtime
operations, not offline archive mutation.

Examples:

```bash
iox2-log-control --format JSON status --service My/Camera/Frames
iox2-log-control --format JSON flush --service My/Camera/Frames
iox2-log-control --format JSON pause --service My/Camera/Frames
iox2-log-control --format JSON resume --service My/Camera/Frames
iox2-log-control --format JSON stop --service My/Camera/Frames
```

Use `stop` or process `SIGINT`/`SIGTERM` for cooperative shutdown. Avoid
unconditional process kill unless recovery from a failed process is the goal.

## Admin

Binary:

```bash
iox2-log-admin
```

The admin tool operates on archive files and recorder lifecycle helpers.

Commands:

| Command | Purpose |
| --- | --- |
| `start` | Start a managed recorder helper. |
| `stop` | Stop a managed recorder helper. |
| `status` | Inspect archive status. |
| `flush` | Force pending archive state to disk where applicable. |
| `trim` | Apply retention trimming. |
| `detach` | Detach a segment from the active archive set. |
| `attach` | Attach a detached segment. |
| `delete-detached` | Delete detached segment data. |
| `list-segments` | List archive segments. |
| `inspect-commit-log` | Inspect metadata commit records. |
| `inspect-record` | Inspect an individual archive record. |

Use admin commands for offline inspection and retention. Use `iox2-log-control`
for live recorder control.

## Query

Binary:

```bash
iox2-log-query
```

The query tool indexes metadata into SQLite and emits selectors that can be
piped to replay or export tools.

Index a finite archive:

```bash
iox2-log-query --format JSON index catch-up \
  --stream-id My/Camera/Frames \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite
```

Run a long-lived incremental indexer:

```bash
iox2-log-query --format JSON index run \
  --stream-id My/Camera/Frames \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite \
  --poll-interval-ms 100 \
  --batch-max-records 4096
```

Query examples:

```bash
iox2-log-query --format JSON query locate-sequence \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite \
  --stream-id My/Camera/Frames \
  --at 42
```

```bash
iox2-log-query --format JSON query locate-range \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite \
  --stream-id My/Camera/Frames \
  --from 1000 \
  --count 512 \
  --expand-selectors
```

```bash
iox2-log-query --format JSON query locate-window \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite \
  --stream-id My/Camera/Frames \
  --start-utc 2026-04-27T12:00:00Z \
  --end-utc 2026-04-27T12:05:00Z \
  --time-field event \
  --emit selectors
```

`locate-range --expand-selectors` emits one locator selector per matching
record. Use that form when downstream tools need exact query membership rather
than a compact contiguous range.

The query tool can report `NotIndexedYet` when the SQLite index has not caught
up to the requested metadata. Re-run `index catch-up`, keep `index run` active,
or retry later.

## Replay

Binary:

```bash
iox2-log-replay
```

The replay tool reads archive records and writes them either to stdout or to an
iceoryx2 publish-subscribe service.

Replay everything:

```bash
iox2-log-replay --format JSON replay \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --to publish-subscribe \
  --service My/Camera/Frames/Replay \
  all
```

Follow a recorder and replay newly committed records until idle:

```bash
iox2-log-replay --format JSON replay \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --to publish-subscribe \
  --service My/Camera/Frames/Replay \
  --follow \
  --follow-poll-ms 100 \
  --follow-idle-timeout-ms 5000 \
  all
```

Replay a range:

```bash
iox2-log-replay --format JSON replay \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --to stdout \
  range --from 1000 --count 512
```

Pipe query selectors to replay:

```bash
iox2-log-query --format JSON query locate-range \
  --db-path /var/lib/iox2-log-archive/My_Camera_Frames/query.sqlite \
  --stream-id My/Camera/Frames \
  --from 1000 \
  --count 512 \
  --expand-selectors |
iox2-log-replay --format JSON replay \
  --storage-path /var/lib/iox2-log-archive/My_Camera_Frames/storage \
  --metadata-log-path /var/lib/iox2-log-archive/My_Camera_Frames/metadata \
  --to publish-subscribe \
  --service My/Camera/Frames/Replay \
  selectors --stdin --selector-format ndjson
```

Selector forms:

| Selector | Purpose |
| --- | --- |
| `all` | Replay every available record in archive order. |
| `sequence --at <n>` | Replay one record by sequence. |
| `range --from <n> --count <n>` | Replay a contiguous sequence range. |
| `locator --at <segment>:<generation>:<offset>:<frame_len>` | Replay one exact frame location. |
| `selectors --stdin --selector-format ndjson` | Stream selectors from stdin. |
| `selectors --file <path> --selector-format ndjson|csv` | Stream selectors from a file. |

NDJSON selector examples:

```json
{"kind":"sequence","sequence":42}
{"kind":"range","from":1000,"count":512}
{"kind":"locator","segment_id":7,"segment_generation":1,"file_offset":4096,"frame_len":1536}
```

CSV selector files use this header:

```csv
kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len
```

Replay rate options:

| Option | Purpose |
| --- | --- |
| `--rate fast` | Replay as fast as the reader and sink allow. |
| `--rate recorded` | Preserve recorded timing gaps, capped by `--max-recorded-gap-ms`. |
| `--rate fixed --messages-per-sec <n>` | Emit at a fixed message rate. |
| `--skip-missing` | Continue past missing selectors. |
| `--max-errors <n>` | Bound tolerated replay errors. |
| `--follow` | Refresh `commit.idxlog` and follow newly committed records for `all`, `sequence`, and `range` selectors. |
| `--follow-poll-ms <n>` | Poll interval for `--follow`; default `100`. |
| `--follow-idle-timeout-ms <n>` | Stop `--follow` after this many milliseconds without new visible records. |

Follow mode sees records after complete archive metadata is externally visible.
It also pins the visible unread replay window so retention trim does not remove
records under the live replay cursor.

## Tool Boundaries

Use this split for production workflows:

- `iox2-log-recorder`: live data path.
- `iox2-log-control`: live recorder control.
- `iox2-log-admin`: archive maintenance and inspection.
- `iox2-log-query`: metadata indexing and selector generation.
- `iox2-log-replay`: record rematerialization to stdout or pub-sub.

The future FITS exporter should consume the same selector stream used by replay.
FITS header generation is a domain-specific policy layer and should remain
outside the recorder hot path.
