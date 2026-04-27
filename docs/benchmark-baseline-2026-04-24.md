# Benchmark Baseline - 2026-04-24

## Environment

- Host: AMD Ryzen 7 6800H with Radeon Graphics
- Kernel: `6.12.57+deb13-amd64`
- OS/arch: `Linux x86_64`
- Filesystem: `ext2/ext3`
- Mount point: `/`
- Mount options: `rw,relatime,errors=remount-ro`
- Build profile: `release`

These numbers are a local development-machine baseline, not a release
acceptance target for other hosts.

## Storage Baseline

Command:

```bash
RUNTIME_SEC=10 \
RAMP_TIME_SEC=2 \
FILE_SIZE=1g \
BLOCK_SIZE=1m \
IODEPTH=64 \
NUMJOBS=1 \
DIRECT_IO=0 \
IOENGINE=io_uring \
crates/core/scripts/run_fio_baseline.sh target/benchmarks/fio-seqwrite-1g-10s
```

Result:

- Sequential write bandwidth: `1,071,877,688 B/s` (`~1.07 GB/s`)
- Write IOPS: `1015.873`
- Runtime: `10080 ms`
- Full report: `target/benchmarks/fio-seqwrite-1g-10s/report.json`

## Core Recorder Synthetic Baseline

This bypasses iceoryx2 transport and measures archive core append throughput
with synthetic in-process payloads.

Command:

```bash
RECORDS=100000 \
PAYLOAD_BYTES=4096 \
SEGMENT_BYTES=67108864 \
BACKEND=auto \
PROFILE=throughput \
crates/core/scripts/run_throughput_profile_benchmark.sh \
  target/benchmarks/core-throughput-100k-4k-auto
```

Result:

- Records: `100000`
- Payload bytes: `409,600,000`
- Elapsed: `0.494792 s`
- Records/s: `202,105`
- Payload throughput: `827,822,197 B/s` (`~827.8 MB/s`)
- Effective backend: `IoUring`
- Write amplification: `1.052739`
- Full report: `target/benchmarks/core-throughput-100k-4k-auto/report.json`

Core recorder throughput is `~77.2%` of the fio sequential-write baseline on
this host for the tested 4 KiB synthetic payload profile.

## Live Pub-Sub Recorder Synthetic Baseline

This measures the full live path:

`synthetic pub-sub publisher -> iceoryx2 pub-sub -> recorder adapter -> archive core`

The current synthetic publisher uses a non-overflowing/blocking pub-sub service
and sends exactly the requested record count. Older checkpoint numbers in this
document may show `sent_messages` far above recorded messages; those runs also
measured publisher flood pressure and should not be used as storage-focused
baselines.

Command:

```bash
RECORDS=50000 \
PAYLOAD_BYTES=4096 \
SEGMENT_BYTES=67108864 \
BACKEND=auto \
PROFILE=throughput \
crates/iceoryx2/scripts/run_synthetic_pubsub_record_benchmark.sh \
  target/benchmarks/pubsub-throughput-50k-4k-auto
```

Result:

- Records: `50000`
- Payload bytes: `204,800,000`
- Sent messages: `2,133,152`
- Recorder elapsed: `0.467388 s`
- Wall elapsed: `0.471119 s`
- Recorder records/s: `106,978`
- Wall records/s: `106,130`
- Recorder payload throughput: `438,179,939 B/s` (`~438.2 MB/s`)
- Wall payload throughput: `434,709,506 B/s` (`~434.7 MB/s`)
- Effective backend: `IoUring`
- Write amplification: `1.050786`
- Stop reason: `MaxMessages`
- Full report: `target/benchmarks/pubsub-throughput-50k-4k-auto/report.json`

Live pub-sub recorder throughput is `~40.6%` of the fio sequential-write
baseline and `~52.5%` of the direct core recorder baseline on this host for the
tested 4 KiB synthetic payload profile.

## Optimization Checkpoint

The first backend optimization removed the extra clone from encoded archive
frames into the io_uring pending-write queue. With `io_uring_required` and the
same 50k x 4 KiB live pub-sub profile, wall throughput improved from the prior
`~464.8 MB/s` required-io_uring baseline to `~576.5 MB/s`.

Result:

- Records: `50000`
- Payload bytes: `204,800,000`
- Sent messages: `1,528,915`
- Recorder elapsed: `0.351567 s`
- Wall elapsed: `0.355277 s`
- Recorder records/s: `142,221`
- Wall records/s: `140,735`
- Recorder payload throughput: `582,535,281 B/s` (`~582.5 MB/s`)
- Wall payload throughput: `576,452,362 B/s` (`~576.5 MB/s`)
- Effective backend: `IoUring`

`strace -f -c` on a 20k x 4 KiB run confirmed the recorder is using io_uring:
the run issued one `io_uring_setup`, three `io_uring_register` calls, and a few
hundred `io_uring_enter` calls for tens of thousands of archive writes. That
means io_uring is batching effectively; the remaining gap is mostly per-sample
CPU work above the storage backend.

## Large-Payload Storage-Focused Checkpoint

Host `spiders`, `/mnt/datadrive/tmp`, fio storage ceiling:

- `ioengine=io_uring`, `direct=1`, `iodepth=64`, 1 MiB sequential write:
  `~1.62 GB/s`
- `ioengine=io_uring`, `direct=0`, `iodepth=64`, 1 MiB sequential write:
  `~1.23 GB/s`

Recorder changes measured against this ceiling:

- Added checksum mode `none` for archive frames.
- Added an external-payload `writev` fast path for pub-sub recording. The
  iceoryx2 sample is retained until the io_uring CQE completes, so the payload
  is not copied into an encoded frame buffer.
- Added vectored CRC32C for the external-payload path, avoiding the old
  checksummed fallback that rebuilt each frame as one contiguous buffer.
- Added commit-index batching for async mode. Durable ack/sync paths flush the
  batch before fsync, preserving requested durability semantics.
- Changed throughput-profile io_uring queue depth from `1024` to `256`; the
  1024-deep path retained too many 1 MiB samples and regressed throughput.
- Changed the synthetic pub-sub benchmark to non-overflowing/blocking delivery
  and exactly `records` sends, avoiding publisher-flood contamination.
- Changed the storage-focused payload matrix default segment size to `1 GiB`.

64 GiB, 1 MiB payload, checksum-none, corrected source model:

| io_uring depth | Payload throughput |
| ---: | ---: |
| 16 | `~0.888 GB/s` |
| 64 | `~0.936 GB/s` |
| 128 | `~0.968 GB/s` |
| 256 | `~0.969 GB/s` |
| 512 | `~0.942 GB/s` |
| 1024 | `~0.873 GB/s` |

Default throughput profile after tuning uses depth `256`, submit batch `256`,
and CQE batch `512`. A no-override verification run produced `~0.896 GB/s`;
the spread indicates remaining host/run variability, but the corrected path is
consistently above the prior 1 MiB live pub-sub checksum-none baseline of
`~0.713 GB/s`.

Post-batching and vectored-CRC checkpoint, 64 GiB, 1 MiB payload, `1 GiB`
segments, depth `256`:

| Checksum | Publish mode | Payload throughput |
| --- | --- | ---: |
| `None` | `copy` | `~0.869 GB/s` |
| `Crc32c` | `copy` | `~0.735 GB/s` |
| `None` | `loan` | `~0.803 GB/s` |

Raw CRC32C throughput on `spiders` over the same 64 GiB byte count was
`~7.06 GB/s`, so CRC computation itself is not the dominant cost. The observed
checked-recording gap is mostly the extra memory read/cache pressure plus
remaining recorder/transport overhead, not a slow checksum implementation.

## Reproduce

The checked-in benchmark entry points are:

- `crates/core/scripts/run_throughput_profile_benchmark.sh`
- `crates/iceoryx2/scripts/run_synthetic_pubsub_record_benchmark.sh`
- `crates/iceoryx2/scripts/run_synthetic_pubsub_payload_matrix.sh`
- `crates/core/scripts/run_fio_baseline.sh`

For release comparisons, rerun all three with the same payload size, backend,
profile, filesystem, and power/performance settings.

For storage-focused live pub-sub sweeps, use the payload matrix script. It runs
`256 B`, `4 KiB`, `16 KiB`, and `1 MiB` payloads by default, computes record
counts from `TARGET_PAYLOAD_BYTES`, writes one JSON report per payload, appends
those reports to `summary.jsonl`, and removes per-run archive data after each
successful run unless `CLEANUP_ARCHIVE=false`.

Example:

```bash
TARGET_PAYLOAD_BYTES=8589934592 \
BACKEND=io_uring_required \
PROFILE=throughput \
PUBLISH_MODE=copy \
crates/iceoryx2/scripts/run_synthetic_pubsub_payload_matrix.sh \
  target/benchmarks/pubsub-payload-matrix-8g
```

Increase `TARGET_PAYLOAD_BYTES` on hosts with enough free space when the goal is
to exceed memory/cache effects and measure sustained storage behavior.
