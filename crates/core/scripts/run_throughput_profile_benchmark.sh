#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${1:-${REPO_ROOT}/target/log-archive-throughput-benchmark}"
RECORDS="${RECORDS:-250000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-4096}"
SEGMENT_BYTES="${SEGMENT_BYTES:-67108864}"
BACKEND="${BACKEND:-auto}"
PROFILE="${PROFILE:-throughput}"

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

BENCH_OUTPUT="$(
  cargo run -p iox2-log-archive-core \
    --example throughput_profile_benchmark \
    --release -- \
    --storage-path "${STORAGE_PATH}" \
    --metadata-log-path "${METADATA_PATH}" \
    --records "${RECORDS}" \
    --payload-bytes "${PAYLOAD_BYTES}" \
    --segment-bytes "${SEGMENT_BYTES}" \
    --backend "${BACKEND}" \
    --profile "${PROFILE}" \
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
    "profile": "${PROFILE}"
  },
  "benchmark_result": ${BENCH_JSON}
}
EOF

echo "benchmark log: ${LOG_PATH}"
echo "benchmark report: ${REPORT_PATH}"
