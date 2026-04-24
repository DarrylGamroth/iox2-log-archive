#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${1:-${REPO_ROOT}/target/log-archive-fio-baseline}"
TARGET_DIR="${TARGET_DIR:-${OUT_DIR}/fio-data}"
JOB_FILE="${JOB_FILE:-${REPO_ROOT}/crates/core/scripts/fio_baseline_sequential_write.fio}"

IOENGINE="${IOENGINE:-io_uring}"
RUNTIME_SEC="${RUNTIME_SEC:-60}"
RAMP_TIME_SEC="${RAMP_TIME_SEC:-5}"
BLOCK_SIZE="${BLOCK_SIZE:-1m}"
IODEPTH="${IODEPTH:-64}"
NUMJOBS="${NUMJOBS:-1}"
FILE_SIZE="${FILE_SIZE:-16g}"
DIRECT_IO="${DIRECT_IO:-0}"

FIO_OUTPUT_JSON="${OUT_DIR}/fio_result.json"
FIO_LOG="${OUT_DIR}/fio.log"
REPORT_PATH="${OUT_DIR}/report.json"

if ! command -v fio >/dev/null 2>&1; then
  echo "fio is not installed. Install fio and retry." >&2
  exit 1
fi

if [[ ! -f "${JOB_FILE}" ]]; then
  echo "fio job file not found: ${JOB_FILE}" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"
mkdir -p "${TARGET_DIR}"

CPU_MODEL="$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -n 1 | tr '"' "'" || true)"
if [[ -z "${CPU_MODEL}" ]]; then
  CPU_MODEL="unknown"
fi

FS_TYPE="$(stat -f -c %T "${TARGET_DIR}")"
MOUNT_POINT="$(df -P "${TARGET_DIR}" | awk 'NR==2 {print $6}')"
MOUNT_OPTIONS="$(findmnt -no OPTIONS --target "${TARGET_DIR}" 2>/dev/null || echo "unknown")"

export TARGET_DIR
export IOENGINE
export RUNTIME_SEC
export RAMP_TIME_SEC
export BLOCK_SIZE
export IODEPTH
export NUMJOBS
export FILE_SIZE
export DIRECT_IO

fio "${JOB_FILE}" \
  --output-format=json \
  --output="${FIO_OUTPUT_JSON}" \
  2>&1 | tee "${FIO_LOG}"

FIO_JSON="$(cat "${FIO_OUTPUT_JSON}")"

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
  "fio_input": {
    "job_file": "${JOB_FILE}",
    "target_dir": "${TARGET_DIR}",
    "ioengine": "${IOENGINE}",
    "runtime_sec": ${RUNTIME_SEC},
    "ramp_time_sec": ${RAMP_TIME_SEC},
    "block_size": "${BLOCK_SIZE}",
    "iodepth": ${IODEPTH},
    "numjobs": ${NUMJOBS},
    "file_size": "${FILE_SIZE}",
    "direct_io": ${DIRECT_IO}
  },
  "fio_result": ${FIO_JSON}
}
EOF

echo "fio log: ${FIO_LOG}"
echo "fio result: ${FIO_OUTPUT_JSON}"
echo "baseline report: ${REPORT_PATH}"
