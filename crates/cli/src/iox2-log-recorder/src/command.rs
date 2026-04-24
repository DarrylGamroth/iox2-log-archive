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
use std::time::Duration;

use anyhow::{Context, anyhow};
use iox2_log_archive_cli::Format;
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderError, EffectiveAsyncIoBackend, PersistenceMode, RecorderAckLevel,
    RecorderProfile,
};
use iox2_log_archive_iceoryx2::{
    PubSubRecorderConfig, PubSubRecorderError,
    record_publish_subscribe as record_iceoryx2_publish_subscribe,
};
use serde::Serialize;

use crate::cli::{
    CliPersistenceMode, CliRecorderAckLevel, CliRecorderProfile, LogRecordAction,
    LogRecordArchiveOptions, LogRecordPublishSubscribeOptions,
};

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
    source_service_id: Option<u64>,
    flush_interval_ms: u64,
    max_messages: Option<u64>,
    timeout_ms: Option<u64>,
    messages_recorded: u64,
    dropped_while_paused: u64,
    elapsed_ms: u128,
    committed_records: u64,
    payload_bytes_committed: u64,
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
    let paths = ArchivePaths::from_options(&options.archive)?;
    let requested_ack_level = options.runtime.common.ack_level.map(ack_level_from_cli);

    let summary = record_iceoryx2_publish_subscribe(PubSubRecorderConfig {
        service: paths.service.clone(),
        node_name: options.runtime.common.node_name.clone(),
        storage_path: paths.storage_path.clone(),
        metadata_log_path: paths.metadata_log_path.clone(),
        profile: recorder_profile(options.archive.profile),
        persistence_mode: persistence_mode(options.archive.mode),
        segment_bytes: options.archive.segment_bytes,
        spare_preallocated_segments: options.archive.spare_preallocated_segments,
        segment_preallocate: options.archive.segment_preallocate,
        max_disk_bytes: options.archive.max_disk_bytes,
        source_service_id: options.runtime.source_service_id,
        cycle_time: Duration::from_millis(options.runtime.common.cycle_time_ms),
        max_messages: options.runtime.common.max_messages,
        timeout: options.runtime.common.timeout_ms.map(Duration::from_millis),
        flush_interval: non_zero_duration(options.runtime.common.flush_interval_ms),
        ack_level: requested_ack_level,
    })
    .map_err(map_pubsub_recorder_error)?;

    let summary = RecordSummary {
        operation: "record-publish-subscribe",
        path: path_payload(&paths),
        profile: recorder_profile_label(summary.profile),
        persistence_mode: persistence_mode_label(summary.persistence_mode),
        configured_async_io_backend: async_backend_label(summary.configured_async_io_backend),
        effective_async_io_backend: effective_async_backend_label(
            summary.effective_async_io_backend,
        ),
        default_ack_level: ack_level_label(summary.default_ack_level),
        requested_ack_level: summary.requested_ack_level.map(ack_level_label),
        source_service_id: Some(summary.source_service_id),
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

    Ok(())
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

fn recorder_profile_label(value: RecorderProfile) -> &'static str {
    match value {
        RecorderProfile::Durable => "Durable",
        RecorderProfile::Balanced => "Balanced",
        RecorderProfile::Throughput => "Throughput",
        RecorderProfile::Replay => "Replay",
    }
}

fn persistence_mode(value: CliPersistenceMode) -> PersistenceMode {
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

fn async_backend_label(value: iox2_log_archive_core::log_archive::AsyncIoBackend) -> &'static str {
    match value {
        iox2_log_archive_core::log_archive::AsyncIoBackend::IoUringPreferred => "IoUringPreferred",
        iox2_log_archive_core::log_archive::AsyncIoBackend::IoUringRequired => "IoUringRequired",
        iox2_log_archive_core::log_archive::AsyncIoBackend::Blocking => "Blocking",
    }
}

fn effective_async_backend_label(value: EffectiveAsyncIoBackend) -> &'static str {
    match value {
        EffectiveAsyncIoBackend::IoUring => "IoUring",
        EffectiveAsyncIoBackend::Blocking => "Blocking",
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
