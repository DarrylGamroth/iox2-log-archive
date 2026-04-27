#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${1:-${REPO_ROOT}/target/log-archive-synthetic-pubsub-benchmark}"
RECORDS="${RECORDS:-100000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-4096}"
SEGMENT_BYTES="${SEGMENT_BYTES:-67108864}"
BACKEND="${BACKEND:-auto}"
PROFILE="${PROFILE:-throughput}"
PUBLISH_MODE="${PUBLISH_MODE:-copy}"
CLEANUP_ARCHIVE="${CLEANUP_ARCHIVE:-false}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
CHECKSUM_MODE="${CHECKSUM_MODE:-crc32c}"
IO_URING_QUEUE_DEPTH="${IO_URING_QUEUE_DEPTH:-}"
IO_SUBMIT_BATCH_MAX="${IO_SUBMIT_BATCH_MAX:-}"
IO_CQE_BATCH_MAX="${IO_CQE_BATCH_MAX:-}"
SUBSCRIBER_MAX_BORROWED_SAMPLES="${SUBSCRIBER_MAX_BORROWED_SAMPLES:-}"

STORAGE_PATH="${OUT_DIR}/storage"
METADATA_PATH="${OUT_DIR}/metadata"
LOG_PATH="${OUT_DIR}/benchmark.log"
REPORT_PATH="${OUT_DIR}/report.json"

mkdir -p "${OUT_DIR}"

CPU_MODEL="$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -n 1 | tr '"' "'" || true)"
if [[ -z "${CPU_MODEL}" ]]; then
  CPU_MODEL="unknown"
fi

FS_TYPE="$(stat -f -c %T "${OUT_DIR}")"
MOUNT_POINT="$(df -P "${OUT_DIR}" | awk 'NR==2 {print $6}')"
MOUNT_OPTIONS="$(findmnt -no OPTIONS --target "${OUT_DIR}" 2>/dev/null || echo "unknown")"

EXTRA_ARGS=()
if [[ -n "${IO_URING_QUEUE_DEPTH}" ]]; then
  EXTRA_ARGS+=(--io-uring-queue-depth "${IO_URING_QUEUE_DEPTH}")
fi
if [[ -n "${IO_SUBMIT_BATCH_MAX}" ]]; then
  EXTRA_ARGS+=(--io-submit-batch-max "${IO_SUBMIT_BATCH_MAX}")
fi
if [[ -n "${IO_CQE_BATCH_MAX}" ]]; then
  EXTRA_ARGS+=(--io-cqe-batch-max "${IO_CQE_BATCH_MAX}")
fi
if [[ -n "${SUBSCRIBER_MAX_BORROWED_SAMPLES}" ]]; then
  EXTRA_ARGS+=(--subscriber-max-borrowed-samples "${SUBSCRIBER_MAX_BORROWED_SAMPLES}")
fi

BENCH_OUTPUT="$(
  cargo run -p iox2-log-archive-iceoryx2 \
    --example synthetic_pubsub_record_benchmark \
    --release -- \
    --storage-path "${STORAGE_PATH}" \
    --metadata-log-path "${METADATA_PATH}" \
    --records "${RECORDS}" \
    --payload-bytes "${PAYLOAD_BYTES}" \
    --segment-bytes "${SEGMENT_BYTES}" \
    --backend "${BACKEND}" \
    --profile "${PROFILE}" \
    --publish-mode "${PUBLISH_MODE}" \
    --timeout-seconds "${TIMEOUT_SECONDS}" \
    --checksum-mode "${CHECKSUM_MODE}" \
    "${EXTRA_ARGS[@]}" \
    2>&1 | tee "${LOG_PATH}"
)"
BENCH_JSON="$(printf '%s\n' "${BENCH_OUTPUT}" | tail -n 1)"

if [[ "${BENCH_JSON}" != \{* ]]; then
  echo "benchmark did not produce JSON output; inspect ${LOG_PATH}" >&2
  exit 1
fi

cat > "${REPORT_PATH}" <<EOF
{
  "timestamp_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "host": {
    "kernel": "$(uname -r)",
    "os": "$(uname -s)",
    "arch": "$(uname -m)",
    "cpu_model": "${CPU_MODEL}"
  },
  "storage": {
    "filesystem_type": "${FS_TYPE}",
    "mount_point": "${MOUNT_POINT}",
    "mount_options": "${MOUNT_OPTIONS}"
  },
  "benchmark_input": {
    "records": ${RECORDS},
    "payload_bytes": ${PAYLOAD_BYTES},
    "segment_bytes": ${SEGMENT_BYTES},
    "backend": "${BACKEND}",
    "profile": "${PROFILE}",
    "publish_mode": "${PUBLISH_MODE}",
    "timeout_seconds": ${TIMEOUT_SECONDS},
    "checksum_mode": "${CHECKSUM_MODE}",
    "io_uring_queue_depth": "${IO_URING_QUEUE_DEPTH}",
    "io_submit_batch_max": "${IO_SUBMIT_BATCH_MAX}",
    "io_cqe_batch_max": "${IO_CQE_BATCH_MAX}",
    "subscriber_max_borrowed_samples": "${SUBSCRIBER_MAX_BORROWED_SAMPLES}"
  },
  "benchmark_result": ${BENCH_JSON}
}
EOF

echo "benchmark log: ${LOG_PATH}"
echo "benchmark report: ${REPORT_PATH}"

if [[ "${CLEANUP_ARCHIVE}" == "true" ]]; then
  rm -rf "${STORAGE_PATH}" "${METADATA_PATH}"
  echo "removed benchmark archive data: ${STORAGE_PATH} ${METADATA_PATH}"
fi
