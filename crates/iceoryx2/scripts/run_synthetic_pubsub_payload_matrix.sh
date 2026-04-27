#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="${1:-${REPO_ROOT}/target/log-archive-synthetic-pubsub-payload-matrix}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-256 4096 16384 1048576}"
TARGET_PAYLOAD_BYTES="${TARGET_PAYLOAD_BYTES:-8589934592}"
SEGMENT_BYTES="${SEGMENT_BYTES:-1073741824}"
BACKEND="${BACKEND:-io_uring_required}"
PROFILE="${PROFILE:-throughput}"
PUBLISH_MODE="${PUBLISH_MODE:-copy}"
CLEANUP_ARCHIVE="${CLEANUP_ARCHIVE:-true}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
CHECKSUM_MODE="${CHECKSUM_MODE:-crc32c}"
IO_URING_QUEUE_DEPTH="${IO_URING_QUEUE_DEPTH:-}"
IO_SUBMIT_BATCH_MAX="${IO_SUBMIT_BATCH_MAX:-}"
IO_CQE_BATCH_MAX="${IO_CQE_BATCH_MAX:-}"
SUBSCRIBER_MAX_BORROWED_SAMPLES="${SUBSCRIBER_MAX_BORROWED_SAMPLES:-}"

mkdir -p "${OUT_DIR}"

SUMMARY_PATH="${OUT_DIR}/summary.jsonl"
: > "${SUMMARY_PATH}"

required_free_bytes=$(( TARGET_PAYLOAD_BYTES + TARGET_PAYLOAD_BYTES / 5 + SEGMENT_BYTES ))

for payload_bytes in ${PAYLOAD_SIZES}; do
  if [[ "${payload_bytes}" -le 0 ]]; then
    echo "invalid payload size: ${payload_bytes}" >&2
    exit 1
  fi

  records=$(( (TARGET_PAYLOAD_BYTES + payload_bytes - 1) / payload_bytes ))
  run_dir="${OUT_DIR}/payload-${payload_bytes}"

  echo "running payload_bytes=${payload_bytes} records=${records} target_payload_bytes=${TARGET_PAYLOAD_BYTES}"

  available_bytes="$(df -PB1 "${OUT_DIR}" | awk 'NR==2 {print $4}')"
  if [[ "${available_bytes}" -lt "${required_free_bytes}" ]]; then
    echo "insufficient free space for payload_bytes=${payload_bytes}: available=${available_bytes} required=${required_free_bytes}" >&2
    exit 1
  fi

  RECORDS="${records}" \
  PAYLOAD_BYTES="${payload_bytes}" \
  SEGMENT_BYTES="${SEGMENT_BYTES}" \
  BACKEND="${BACKEND}" \
  PROFILE="${PROFILE}" \
  PUBLISH_MODE="${PUBLISH_MODE}" \
  CLEANUP_ARCHIVE="${CLEANUP_ARCHIVE}" \
  TIMEOUT_SECONDS="${TIMEOUT_SECONDS}" \
  CHECKSUM_MODE="${CHECKSUM_MODE}" \
  IO_URING_QUEUE_DEPTH="${IO_URING_QUEUE_DEPTH}" \
  IO_SUBMIT_BATCH_MAX="${IO_SUBMIT_BATCH_MAX}" \
  IO_CQE_BATCH_MAX="${IO_CQE_BATCH_MAX}" \
  SUBSCRIBER_MAX_BORROWED_SAMPLES="${SUBSCRIBER_MAX_BORROWED_SAMPLES}" \
    "${REPO_ROOT}/crates/iceoryx2/scripts/run_synthetic_pubsub_record_benchmark.sh" "${run_dir}"

  tr -d '\n' < "${run_dir}/report.json" >> "${SUMMARY_PATH}"
  printf '\n' >> "${SUMMARY_PATH}"
done

echo "matrix summary: ${SUMMARY_PATH}"
