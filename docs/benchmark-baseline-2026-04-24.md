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

The synthetic publisher intentionally overdrives the pub-sub service so the
recorder is saturated. `sent_messages` is therefore higher than recorded
messages; recorded throughput is the primary metric.

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

## Reproduce

The checked-in benchmark entry points are:

- `crates/core/scripts/run_throughput_profile_benchmark.sh`
- `crates/iceoryx2/scripts/run_synthetic_pubsub_record_benchmark.sh`
- `crates/core/scripts/run_fio_baseline.sh`

For release comparisons, rerun all three with the same payload size, backend,
profile, filesystem, and power/performance settings.
