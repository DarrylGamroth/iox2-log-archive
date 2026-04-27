// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::{Context, anyhow};
use iox2_log_archive_cli::Format;
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderError, AsyncIoBackend, ChecksumMode, EffectiveAsyncIoBackend, OutOfSpacePolicy,
    PersistenceMode, RecorderAckLevel, RecorderProfile,
};
use iox2_log_archive_iceoryx2::{
    PubSubRecorderConfig, PubSubRecorderError, PubSubRecorderStopReason,
    record_publish_subscribe as record_iceoryx2_publish_subscribe,
};
use serde::Serialize;

use crate::cli::{
    CliAsyncIoBackend, CliChecksumMode, CliOutOfSpacePolicy, CliPersistenceMode,
    CliRecorderAckLevel, CliRecorderProfile, LogRecordAction, LogRecordArchiveOptions,
    LogRecordPublishSubscribeOptions,
};

#[derive(Debug, Clone, Copy)]
struct CliProfileDefaults {
    persistence_mode: CliPersistenceMode,
    segment_bytes: usize,
    spare_preallocated_segments: usize,
    segment_preallocate: bool,
    subscriber_max_borrowed_samples: Option<usize>,
}

#[derive(Debug)]
pub(crate) enum LogRecordCommandError {
    InvalidInput(String),
    NotAvailable(String),
    Internal(anyhow::Error),
}

impl LogRecordCommandError {
    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::Internal(_) => 1,
            Self::InvalidInput(_) => 2,
            Self::NotAvailable(_) => 3,
        }
    }

    pub(crate) fn to_formatted_error(&self, format: Format) -> String {
        #[derive(Serialize)]
        struct ErrorPayload<'a> {
            error_code: &'a str,
            message: &'a str,
        }

        let payload = match self {
            LogRecordCommandError::InvalidInput(message) => ErrorPayload {
                error_code: "InvalidInput",
                message,
            },
            LogRecordCommandError::NotAvailable(message) => ErrorPayload {
                error_code: "NotAvailable",
                message,
            },
            LogRecordCommandError::Internal(error) => ErrorPayload {
                error_code: "Internal",
                message: &format!("{error:#}"),
            },
        };

        format
            .as_string(&payload)
            .unwrap_or_else(|_| format!("{:?}", payload.error_code))
    }
}

impl core::fmt::Display for LogRecordCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "{message}"),
            Self::NotAvailable(message) => write!(f, "{message}"),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for LogRecordCommandError {}

#[derive(Debug, Clone)]
struct ArchivePaths {
    service: String,
    storage_path: PathBuf,
    metadata_log_path: PathBuf,
}

impl ArchivePaths {
    fn from_options(options: &LogRecordArchiveOptions) -> Result<Self, LogRecordCommandError> {
        if options.service.trim().is_empty() {
            return Err(LogRecordCommandError::InvalidInput(
                "--service must not be empty".to_string(),
            ));
        }

        let storage_path = options.storage_path.clone();
        let metadata_log_path = options
            .metadata_log_path
            .clone()
            .unwrap_or_else(|| storage_path.clone());

        Ok(Self {
            service: options.service.clone(),
            storage_path,
            metadata_log_path,
        })
    }
}

#[derive(Serialize)]
struct PathPayload<'a> {
    service: &'a str,
    storage_path: String,
    metadata_log_path: String,
}

#[derive(Serialize)]
struct RecordSummary<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    profile: &'static str,
    persistence_mode: &'static str,
    configured_async_io_backend: &'static str,
    effective_async_io_backend: &'static str,
    default_ack_level: &'static str,
    requested_ack_level: Option<&'static str>,
    stop_reason: &'static str,
    source_service_id: Option<u64>,
    io_uring_queue_depth: u32,
    io_submit_batch_max: u32,
    io_cqe_batch_max: u32,
    io_uring_register_files: bool,
    checksum_mode: &'static str,
    subscriber_max_borrowed_samples: usize,
    external_payload_fast_path: bool,
    out_of_space_policy: &'static str,
    metadata_log_roll_bytes: u64,
    metadata_log_max_bytes: u64,
    flush_interval_ms: u64,
    max_messages: Option<u64>,
    timeout_ms: Option<u64>,
    messages_recorded: u64,
    dropped_while_paused: u64,
    elapsed_ms: u128,
    committed_records: u64,
    payload_bytes_committed: u64,
    data_bytes_written: u64,
    metadata_bytes_written: u64,
    rolled_segments: u64,
    preallocated_segments: u64,
    out_of_space_events: u64,
    metadata_log_rolls: u64,
    write_amplification_ratio: f64,
    last_durable_data_sequence: Option<u64>,
    last_durable_commit_ordinal: Option<u64>,
    paused_at_shutdown: bool,
    paused_since_ns_at_shutdown: Option<u64>,
    degraded: bool,
}

pub(crate) fn log_record(
    action: LogRecordAction,
    format: Format,
) -> Result<(), LogRecordCommandError> {
    match action {
        LogRecordAction::PublishSubscribe(options) => record_publish_subscribe(options, format),
    }
}

fn record_publish_subscribe(
    options: LogRecordPublishSubscribeOptions,
    format: Format,
) -> Result<(), LogRecordCommandError> {
    validate_runtime_options(&options.archive, &options.runtime.common)?;
    if options.runtime.subscriber_max_borrowed_samples == Some(0) {
        return Err(LogRecordCommandError::InvalidInput(
            "--subscriber-max-borrowed-samples must be greater than 0".to_string(),
        ));
    }
    let paths = ArchivePaths::from_options(&options.archive)?;
    let requested_ack_level = options.runtime.common.ack_level.map(ack_level_from_cli);
    let shutdown_requested = install_shutdown_handler()?;
    let defaults = cli_profile_defaults(options.archive.profile);
    let persistence_mode = options.archive.mode.unwrap_or(defaults.persistence_mode);
    let segment_bytes = options
        .archive
        .segment_bytes
        .unwrap_or(defaults.segment_bytes);
    let spare_preallocated_segments = options
        .archive
        .spare_preallocated_segments
        .unwrap_or(defaults.spare_preallocated_segments);
    let segment_preallocate = options
        .archive
        .segment_preallocate
        .unwrap_or(defaults.segment_preallocate);
    let subscriber_max_borrowed_samples = options
        .runtime
        .subscriber_max_borrowed_samples
        .or(defaults.subscriber_max_borrowed_samples);

    let summary = record_iceoryx2_publish_subscribe(PubSubRecorderConfig {
        service: paths.service.clone(),
        node_name: options.runtime.common.node_name.clone(),
        storage_path: paths.storage_path.clone(),
        metadata_log_path: paths.metadata_log_path.clone(),
        profile: recorder_profile(options.archive.profile),
        persistence_mode: persistence_mode_from_cli(persistence_mode),
        segment_bytes,
        spare_preallocated_segments,
        segment_preallocate,
        max_disk_bytes: options.archive.max_disk_bytes,
        async_io_backend: options
            .archive
            .async_io_backend
            .map(async_io_backend_from_cli),
        io_uring_queue_depth: options.archive.io_uring_queue_depth,
        io_submit_batch_max: options.archive.io_submit_batch_max,
        io_cqe_batch_max: options.archive.io_cqe_batch_max,
        io_uring_register_files: options.archive.io_uring_register_files,
        checksum_mode: options.archive.checksum_mode.map(checksum_mode_from_cli),
        subscriber_max_borrowed_samples,
        out_of_space_policy: options
            .archive
            .out_of_space_policy
            .map(out_of_space_policy_from_cli),
        metadata_log_roll_bytes: options.archive.metadata_log_roll_bytes,
        metadata_log_max_bytes: options.archive.metadata_log_max_bytes,
        source_service_id: options.runtime.source_service_id,
        cycle_time: Duration::from_millis(options.runtime.common.cycle_time_ms),
        max_messages: options.runtime.common.max_messages,
        timeout: options.runtime.common.timeout_ms.map(Duration::from_millis),
        flush_interval: non_zero_duration(options.runtime.common.flush_interval_ms),
        ack_level: requested_ack_level,
        shutdown_requested: Some(shutdown_requested),
    })
    .map_err(map_pubsub_recorder_error)?;

    let summary = RecordSummary {
        operation: "record-publish-subscribe",
        path: path_payload(&paths),
        profile: cli_recorder_profile_label(options.archive.profile),
        persistence_mode: persistence_mode_label(summary.persistence_mode),
        configured_async_io_backend: async_backend_label(summary.configured_async_io_backend),
        effective_async_io_backend: effective_async_backend_label(
            summary.effective_async_io_backend,
        ),
        default_ack_level: ack_level_label(summary.default_ack_level),
        requested_ack_level: summary.requested_ack_level.map(ack_level_label),
        stop_reason: stop_reason_label(summary.stop_reason),
        source_service_id: Some(summary.source_service_id),
        io_uring_queue_depth: summary.io_uring_queue_depth,
        io_submit_batch_max: summary.io_submit_batch_max,
        io_cqe_batch_max: summary.io_cqe_batch_max,
        io_uring_register_files: summary.io_uring_register_files,
        checksum_mode: checksum_mode_label(summary.checksum_mode),
        subscriber_max_borrowed_samples: summary.subscriber_max_borrowed_samples,
        external_payload_fast_path: summary.external_payload_fast_path,
        out_of_space_policy: out_of_space_policy_label(summary.out_of_space_policy),
        metadata_log_roll_bytes: summary.metadata_log_roll_bytes,
        metadata_log_max_bytes: summary.metadata_log_max_bytes,
        flush_interval_ms: options.runtime.common.flush_interval_ms,
        max_messages: summary.max_messages,
        timeout_ms: summary
            .timeout
            .map(|value| value.as_millis().min(u64::MAX as u128) as u64),
        messages_recorded: summary.messages_recorded,
        dropped_while_paused: summary.dropped_while_paused,
        elapsed_ms: summary.elapsed.as_millis(),
        committed_records: summary.committed_records,
        payload_bytes_committed: summary.payload_bytes_committed,
        data_bytes_written: summary.data_bytes_written,
        metadata_bytes_written: summary.metadata_bytes_written,
        rolled_segments: summary.rolled_segments,
        preallocated_segments: summary.preallocated_segments,
        out_of_space_events: summary.out_of_space_events,
        metadata_log_rolls: summary.metadata_log_rolls,
        write_amplification_ratio: summary.write_amplification_ratio,
        last_durable_data_sequence: summary.last_durable_data_sequence,
        last_durable_commit_ordinal: summary.last_durable_commit_ordinal,
        paused_at_shutdown: summary.paused_at_shutdown,
        paused_since_ns_at_shutdown: summary.paused_since_ns_at_shutdown,
        degraded: summary.degraded,
    };

    print_output(&summary, format)
}

fn validate_runtime_options(
    archive: &LogRecordArchiveOptions,
    runtime: &crate::cli::LogRecordRuntimeOptions,
) -> Result<(), LogRecordCommandError> {
    if archive.service.trim().is_empty() {
        return Err(LogRecordCommandError::InvalidInput(
            "--service must not be empty".to_string(),
        ));
    }

    if runtime.cycle_time_ms == 0 {
        return Err(LogRecordCommandError::InvalidInput(
            "--cycle-time-ms must be greater than 0".to_string(),
        ));
    }
    if archive.segment_bytes == Some(0) {
        return Err(LogRecordCommandError::InvalidInput(
            "--segment-bytes must be greater than 0".to_string(),
        ));
    }
    if archive.io_uring_queue_depth == Some(0) {
        return Err(LogRecordCommandError::InvalidInput(
            "--io-uring-queue-depth must be greater than 0".to_string(),
        ));
    }
    if archive.io_submit_batch_max == Some(0) {
        return Err(LogRecordCommandError::InvalidInput(
            "--io-submit-batch-max must be greater than 0".to_string(),
        ));
    }
    if archive.io_cqe_batch_max == Some(0) {
        return Err(LogRecordCommandError::InvalidInput(
            "--io-cqe-batch-max must be greater than 0".to_string(),
        ));
    }

    Ok(())
}

fn install_shutdown_handler() -> Result<Arc<AtomicBool>, LogRecordCommandError> {
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let handler_flag = Arc::clone(&shutdown_requested);
    ctrlc::set_handler(move || {
        handler_flag.store(true, Ordering::SeqCst);
    })
    .with_context(|| "failed to install SIGINT/SIGTERM shutdown handler")
    .map_err(LogRecordCommandError::Internal)?;
    Ok(shutdown_requested)
}

fn non_zero_duration(value_ms: u64) -> Option<Duration> {
    if value_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(value_ms))
    }
}

fn map_recorder_error(error: ArchiveRecorderError) -> LogRecordCommandError {
    match error {
        ArchiveRecorderError::SequenceNotMonotonic { previous, next } => {
            LogRecordCommandError::InvalidInput(format!(
                "monotonic sequence violation: sequence {next} is not greater than previous sequence {previous}",
            ))
        }
        other => LogRecordCommandError::Internal(anyhow!(other)),
    }
}

fn map_pubsub_recorder_error(error: PubSubRecorderError) -> LogRecordCommandError {
    match error {
        PubSubRecorderError::InvalidInput(message) => LogRecordCommandError::InvalidInput(message),
        PubSubRecorderError::NotAvailable(message) => LogRecordCommandError::NotAvailable(message),
        PubSubRecorderError::Recorder(error) => map_recorder_error(error),
        PubSubRecorderError::Iceoryx2(message) => LogRecordCommandError::Internal(anyhow!(message)),
    }
}

fn recorder_profile(value: CliRecorderProfile) -> RecorderProfile {
    match value {
        CliRecorderProfile::Durable => RecorderProfile::Durable,
        CliRecorderProfile::Balanced => RecorderProfile::Balanced,
        CliRecorderProfile::Throughput => RecorderProfile::Throughput,
        CliRecorderProfile::Replay => RecorderProfile::Replay,
    }
}

fn cli_recorder_profile_label(value: CliRecorderProfile) -> &'static str {
    match value {
        CliRecorderProfile::Durable => "Durable",
        CliRecorderProfile::Balanced => "Balanced",
        CliRecorderProfile::Throughput => "Throughput",
        CliRecorderProfile::Replay => "Replay",
    }
}

fn cli_profile_defaults(value: CliRecorderProfile) -> CliProfileDefaults {
    match value {
        CliRecorderProfile::Durable => CliProfileDefaults {
            persistence_mode: CliPersistenceMode::Sync,
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            segment_preallocate: true,
            subscriber_max_borrowed_samples: None,
        },
        CliRecorderProfile::Balanced => CliProfileDefaults {
            persistence_mode: CliPersistenceMode::Async,
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            segment_preallocate: true,
            subscriber_max_borrowed_samples: None,
        },
        CliRecorderProfile::Throughput => CliProfileDefaults {
            persistence_mode: CliPersistenceMode::Async,
            segment_bytes: 1024 * 1024 * 1024,
            spare_preallocated_segments: 2,
            segment_preallocate: true,
            subscriber_max_borrowed_samples: None,
        },
        CliRecorderProfile::Replay => CliProfileDefaults {
            persistence_mode: CliPersistenceMode::Async,
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            segment_preallocate: true,
            subscriber_max_borrowed_samples: None,
        },
    }
}

fn persistence_mode_from_cli(value: CliPersistenceMode) -> PersistenceMode {
    match value {
        CliPersistenceMode::Volatile => PersistenceMode::Volatile,
        CliPersistenceMode::Async => PersistenceMode::Async,
        CliPersistenceMode::Sync => PersistenceMode::Sync,
    }
}

fn persistence_mode_label(value: PersistenceMode) -> &'static str {
    match value {
        PersistenceMode::Volatile => "Volatile",
        PersistenceMode::Async => "Async",
        PersistenceMode::Sync => "Sync",
    }
}

fn ack_level_from_cli(value: CliRecorderAckLevel) -> RecorderAckLevel {
    match value {
        CliRecorderAckLevel::Accepted => RecorderAckLevel::Accepted,
        CliRecorderAckLevel::DurableData => RecorderAckLevel::DurableData,
        CliRecorderAckLevel::DurableDataAndCommitLog => RecorderAckLevel::DurableDataAndCommitLog,
    }
}

fn ack_level_label(value: RecorderAckLevel) -> &'static str {
    match value {
        RecorderAckLevel::Accepted => "Accepted",
        RecorderAckLevel::DurableData => "DurableData",
        RecorderAckLevel::DurableDataAndCommitLog => "DurableDataAndCommitLog",
    }
}

fn async_io_backend_from_cli(value: CliAsyncIoBackend) -> AsyncIoBackend {
    match value {
        CliAsyncIoBackend::IoUringPreferred => AsyncIoBackend::IoUringPreferred,
        CliAsyncIoBackend::IoUringRequired => AsyncIoBackend::IoUringRequired,
        CliAsyncIoBackend::Blocking => AsyncIoBackend::Blocking,
    }
}

fn async_backend_label(value: AsyncIoBackend) -> &'static str {
    match value {
        AsyncIoBackend::IoUringPreferred => "IoUringPreferred",
        AsyncIoBackend::IoUringRequired => "IoUringRequired",
        AsyncIoBackend::Blocking => "Blocking",
    }
}

fn effective_async_backend_label(value: EffectiveAsyncIoBackend) -> &'static str {
    match value {
        EffectiveAsyncIoBackend::IoUring => "IoUring",
        EffectiveAsyncIoBackend::Blocking => "Blocking",
    }
}

fn checksum_mode_from_cli(value: CliChecksumMode) -> ChecksumMode {
    match value {
        CliChecksumMode::None => ChecksumMode::None,
        CliChecksumMode::Crc32c => ChecksumMode::Crc32c,
    }
}

fn checksum_mode_label(value: ChecksumMode) -> &'static str {
    match value {
        ChecksumMode::None => "None",
        ChecksumMode::Crc32c => "Crc32c",
    }
}

fn out_of_space_policy_from_cli(value: CliOutOfSpacePolicy) -> OutOfSpacePolicy {
    match value {
        CliOutOfSpacePolicy::FailWriter => OutOfSpacePolicy::FailWriter,
    }
}

fn out_of_space_policy_label(value: OutOfSpacePolicy) -> &'static str {
    match value {
        OutOfSpacePolicy::FailWriter => "FailWriter",
    }
}

fn stop_reason_label(value: PubSubRecorderStopReason) -> &'static str {
    match value {
        PubSubRecorderStopReason::ControlStop => "ControlStop",
        PubSubRecorderStopReason::ShutdownRequested => "ShutdownRequested",
        PubSubRecorderStopReason::MaxMessages => "MaxMessages",
        PubSubRecorderStopReason::Timeout => "Timeout",
        PubSubRecorderStopReason::WaitInterrupted => "WaitInterrupted",
    }
}

fn path_payload(paths: &ArchivePaths) -> PathPayload<'_> {
    PathPayload {
        service: &paths.service,
        storage_path: paths.storage_path.display().to_string(),
        metadata_log_path: paths.metadata_log_path.display().to_string(),
    }
}

fn print_output<T: Serialize>(payload: &T, format: Format) -> Result<(), LogRecordCommandError> {
    let serialized = format
        .as_string(payload)
        .with_context(|| "failed to serialize log-recorder output")
        .map_err(LogRecordCommandError::Internal)?;
    println!("{serialized}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throughput_profile_does_not_assume_large_payload_borrow_capacity() {
        let defaults = cli_profile_defaults(CliRecorderProfile::Throughput);

        assert_eq!(defaults.persistence_mode, CliPersistenceMode::Async);
        assert_eq!(defaults.segment_bytes, 1024 * 1024 * 1024);
        assert_eq!(defaults.spare_preallocated_segments, 2);
        assert!(defaults.segment_preallocate);
        assert_eq!(defaults.subscriber_max_borrowed_samples, None);
    }
}
