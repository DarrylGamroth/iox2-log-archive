#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-target/release/examples/synthetic_pubsub_record_benchmark}"
OUT_DIR="${1:?usage: run_synthetic_pubsub_payload_matrix_binary.sh <out-dir>}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-256 4096 16384 1048576}"
TARGET_PAYLOAD_BYTES="${TARGET_PAYLOAD_BYTES:-68719476736}"
SEGMENT_BYTES="${SEGMENT_BYTES:-1073741824}"
BACKEND="${BACKEND:-io_uring_required}"
PROFILE="${PROFILE:-throughput}"
PUBLISH_MODE="${PUBLISH_MODE:-copy}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-1800}"
CLEANUP_ARCHIVE="${CLEANUP_ARCHIVE:-true}"
CHECKSUM_MODE="${CHECKSUM_MODE:-crc32c}"
IO_URING_QUEUE_DEPTH="${IO_URING_QUEUE_DEPTH:-}"
IO_SUBMIT_BATCH_MAX="${IO_SUBMIT_BATCH_MAX:-}"
IO_CQE_BATCH_MAX="${IO_CQE_BATCH_MAX:-}"
SUBSCRIBER_MAX_BORROWED_SAMPLES="${SUBSCRIBER_MAX_BORROWED_SAMPLES:-}"

mkdir -p "${OUT_DIR}"

SUMMARY_PATH="${OUT_DIR}/summary.jsonl"
: > "${SUMMARY_PATH}"

CPU_MODEL="$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -n 1 | tr '"' "'" || true)"
if [[ -z "${CPU_MODEL}" ]]; then
  CPU_MODEL="unknown"
fi

FS_TYPE="$(stat -f -c %T "${OUT_DIR}")"
MOUNT_POINT="$(df -P "${OUT_DIR}" | awk 'NR==2 {print $6}')"
MOUNT_OPTIONS="$(findmnt -no OPTIONS --target "${OUT_DIR}" 2>/dev/null || echo "unknown")"
required_free_bytes=$((TARGET_PAYLOAD_BYTES + TARGET_PAYLOAD_BYTES / 5 + SEGMENT_BYTES))

for payload_bytes in ${PAYLOAD_SIZES}; do
  if [[ "${payload_bytes}" -le 0 ]]; then
    echo "invalid payload size: ${payload_bytes}" >&2
    exit 1
  fi

  records=$(((TARGET_PAYLOAD_BYTES + payload_bytes - 1) / payload_bytes))
  run_dir="${OUT_DIR}/payload-${payload_bytes}"
  storage_path="${run_dir}/storage"
  metadata_path="${run_dir}/metadata"
  log_path="${run_dir}/benchmark.log"
  report_path="${run_dir}/report.json"
  mkdir -p "${run_dir}"

  available_bytes="$(df -PB1 "${OUT_DIR}" | awk 'NR==2 {print $4}')"
  if [[ "${available_bytes}" -lt "${required_free_bytes}" ]]; then
    echo "insufficient free space for payload_bytes=${payload_bytes}: available=${available_bytes} required=${required_free_bytes}" >&2
    exit 1
  fi

  echo "running payload_bytes=${payload_bytes} records=${records} target_payload_bytes=${TARGET_PAYLOAD_BYTES} out=${OUT_DIR}"
  extra_args=()
  if [[ -n "${IO_URING_QUEUE_DEPTH}" ]]; then
    extra_args+=(--io-uring-queue-depth "${IO_URING_QUEUE_DEPTH}")
  fi
  if [[ -n "${IO_SUBMIT_BATCH_MAX}" ]]; then
    extra_args+=(--io-submit-batch-max "${IO_SUBMIT_BATCH_MAX}")
  fi
  if [[ -n "${IO_CQE_BATCH_MAX}" ]]; then
    extra_args+=(--io-cqe-batch-max "${IO_CQE_BATCH_MAX}")
  fi
  if [[ -n "${SUBSCRIBER_MAX_BORROWED_SAMPLES}" ]]; then
    extra_args+=(--subscriber-max-borrowed-samples "${SUBSCRIBER_MAX_BORROWED_SAMPLES}")
  fi
  bench_output="$(
    "${BIN}" \
      --storage-path "${storage_path}" \
      --metadata-log-path "${metadata_path}" \
      --records "${records}" \
      --payload-bytes "${payload_bytes}" \
      --segment-bytes "${SEGMENT_BYTES}" \
      --backend "${BACKEND}" \
      --profile "${PROFILE}" \
      --publish-mode "${PUBLISH_MODE}" \
      --timeout-seconds "${TIMEOUT_SECONDS}" \
      --checksum-mode "${CHECKSUM_MODE}" \
      "${extra_args[@]}" \
      2>&1 | tee "${log_path}"
  )"
  bench_json="$(printf '%s\n' "${bench_output}" | tail -n 1)"
  if [[ "${bench_json}" != \{* ]]; then
    echo "benchmark did not produce JSON output; inspect ${log_path}" >&2
    exit 1
  fi

  cat > "${report_path}" <<EOF
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
    "records": ${records},
    "payload_bytes": ${payload_bytes},
    "target_payload_bytes": ${TARGET_PAYLOAD_BYTES},
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
  "benchmark_result": ${bench_json}
}
EOF

  tr -d '\n' < "${report_path}" >> "${SUMMARY_PATH}"
  printf '\n' >> "${SUMMARY_PATH}"

  if [[ "${CLEANUP_ARCHIVE}" == "true" ]]; then
    rm -rf "${storage_path}" "${metadata_path}"
    echo "removed benchmark archive data: ${storage_path} ${metadata_path}"
  fi
done

echo "matrix summary: ${SUMMARY_PATH}"
