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

use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use iox2_log_archive_cli::Format;
use iox2_log_archive_core::log_archive::{
    ArchiveCommitLogEntry, ArchiveLocator, ArchiveRecorderBuilder, ArchiveRecorderError,
    ArchiveReplayError, ArchiveReplayerBuilder, ArchiveSegmentTier, PersistenceMode,
    RecorderProfile,
};
use serde::Serialize;

use crate::cli::{
    CliPersistenceMode, CliRecorderProfile, LogRecorderAction, LogRecorderArchiveOptions,
    LogRecorderStartOptions,
};

#[derive(Debug)]
pub(crate) enum LogRecorderCommandError {
    InvalidInput(String),
    NotAvailable(String),
    Internal(anyhow::Error),
}

impl LogRecorderCommandError {
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
            LogRecorderCommandError::InvalidInput(message) => ErrorPayload {
                error_code: "InvalidInput",
                message,
            },
            LogRecorderCommandError::NotAvailable(message) => ErrorPayload {
                error_code: "NotAvailable",
                message,
            },
            LogRecorderCommandError::Internal(error) => ErrorPayload {
                error_code: "Internal",
                message: &format!("{error:#}"),
            },
        };

        format
            .as_string(&payload)
            .unwrap_or_else(|_| format!("{:?}", payload.error_code))
    }
}

impl core::fmt::Display for LogRecorderCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "{message}"),
            Self::NotAvailable(message) => write!(f, "{message}"),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for LogRecorderCommandError {}

#[derive(Debug, Clone)]
struct ArchivePaths {
    service: String,
    storage_path: PathBuf,
    metadata_log_path: PathBuf,
}

impl ArchivePaths {
    fn from_options(options: &LogRecorderArchiveOptions) -> Result<Self, LogRecorderCommandError> {
        if options.service.trim().is_empty() {
            return Err(LogRecorderCommandError::InvalidInput(
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

    fn ensure_archive_exists(&self) -> Result<(), LogRecorderCommandError> {
        if !self.storage_path.join("catalog.bin").exists() {
            return Err(LogRecorderCommandError::NotAvailable(format!(
                "archive not found for service '{}' at {}",
                self.service,
                self.storage_path.display()
            )));
        }
        if !self.metadata_log_path.join("commit.idxlog").exists() {
            return Err(LogRecorderCommandError::NotAvailable(format!(
                "commit.idxlog not found for service '{}' at {}",
                self.service,
                self.metadata_log_path.display()
            )));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct PathPayload<'a> {
    service: &'a str,
    storage_path: String,
    metadata_log_path: String,
}

#[derive(Serialize)]
struct OperationResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    affected_segments: u64,
}

#[derive(Serialize)]
struct StartResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    profile: &'static str,
    persistence_mode: &'static str,
    recovered_existing_archive: bool,
}

#[derive(Serialize)]
struct StatusResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    profile: &'static str,
    persistence_mode: &'static str,
    configured_async_io_backend: &'static str,
    effective_async_io_backend: &'static str,
    degraded: bool,
    stats: StatusStats,
    retention: StatusRetention,
}

#[derive(Serialize)]
struct StatusStats {
    committed_records: u64,
    payload_bytes_committed: u64,
    data_bytes_written: u64,
    metadata_bytes_written: u64,
    rolled_segments: u64,
    metadata_log_rolls: u64,
    last_durable_data_sequence: Option<u64>,
    last_durable_commit_ordinal: Option<u64>,
    amplification_ratio: f64,
}

#[derive(Serialize)]
struct StatusRetention {
    max_disk_bytes: Option<u64>,
    retained_bytes_total: u64,
    retained_bytes_hot_attached: u64,
    retained_bytes_cold_detached: u64,
    segments_hot_attached: usize,
    segments_cold_detached: usize,
    pinned_segments: usize,
}

#[derive(Serialize)]
struct SegmentStatePayload {
    segment_id: u64,
    segment_generation: u32,
    sequence_start: u64,
    sequence_end: u64,
    records: u64,
    data_bytes_used: u64,
    tier: &'static str,
    pinned: bool,
}

#[derive(Serialize)]
struct ListSegmentsResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    segments: Vec<SegmentStatePayload>,
}

#[derive(Serialize)]
struct CommitLogEntryPayload {
    commit_ordinal: u64,
    sequence: u64,
    segment_id: u64,
    segment_generation: u32,
    file_offset: u64,
    frame_len: u32,
    frame_checksum: u32,
    event_time_ns: u64,
    commit_time_ns: u64,
    source_pattern: &'static str,
    source_service_id: u64,
    source_instance_id: u64,
    source_sequence: Option<u64>,
    hot_attached: bool,
}

#[derive(Serialize)]
struct InspectCommitLogResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    from_ordinal: u64,
    limit: usize,
    entries: Vec<CommitLogEntryPayload>,
}

#[derive(Serialize)]
struct RecordPayloadPreview {
    total_len: usize,
    preview_len: usize,
    truncated: bool,
    hex: String,
}

#[derive(Serialize)]
struct RecordResult {
    commit_ordinal: u64,
    sequence: u64,
    event_time_ns: u64,
    commit_time_ns: u64,
    segment_id: u64,
    segment_generation: u32,
    file_offset: u64,
    frame_len: u32,
    user_header: RecordPayloadPreview,
    payload: RecordPayloadPreview,
}

#[derive(Serialize)]
struct InspectRecordResult<'a> {
    operation: &'a str,
    #[serde(flatten)]
    path: PathPayload<'a>,
    query: String,
    record: RecordResult,
}

pub(crate) fn log_recorder(
    action: LogRecorderAction,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    match action {
        LogRecorderAction::Start(options) => start(options, format),
        LogRecorderAction::Stop(options) => stop(options.archive, format),
        LogRecorderAction::Status(options) => status(options.archive, format),
        LogRecorderAction::Flush(options) => flush(options.archive, format),
        LogRecorderAction::Trim(options) => trim(options.archive, options.before_sequence, format),
        LogRecorderAction::Detach(options) => {
            detach(options.archive, options.before_sequence, format)
        }
        LogRecorderAction::Attach(options) => attach(options.archive, format),
        LogRecorderAction::DeleteDetached(options) => delete_detached(
            options.archive,
            options.before_sequence.unwrap_or(u64::MAX),
            format,
        ),
        LogRecorderAction::ListSegments(options) => {
            list_segments(options.archive, options.detached_only, format)
        }
        LogRecorderAction::InspectCommitLog(options) => {
            inspect_commit_log(options.archive, options.from_ordinal, options.limit, format)
        }
        LogRecorderAction::InspectRecord(options) => inspect_record(
            options.archive,
            options.at_sequence,
            options.at_locator,
            options.preview_bytes,
            format,
        ),
    }
}

fn start(options: LogRecorderStartOptions, format: Format) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options.archive)?;
    let mut builder = ArchiveRecorderBuilder::new(&paths.storage_path)
        .metadata_log_path(&paths.metadata_log_path)
        .profile(recorder_profile(options.profile))
        .persistence_mode(persistence_mode(options.mode))
        .segment_bytes(options.segment_bytes)
        .spare_preallocated_segments(options.spare_preallocated_segments)
        .segment_preallocate(options.segment_preallocate);

    if let Some(max_disk_bytes) = options.max_disk_bytes {
        builder = builder.max_disk_bytes(max_disk_bytes);
    }

    let recorder = builder.open_or_recover().map_err(map_recorder_error)?;
    let output = StartResult {
        operation: "start",
        path: path_payload(&paths),
        profile: recorder_profile_label(recorder.profile()),
        persistence_mode: persistence_mode_label(recorder.persistence_mode()),
        recovered_existing_archive: recorder.recovery_status().recovered_existing_archive,
    };
    print_output(&output, format)
}

fn stop(options: LogRecorderArchiveOptions, format: Format) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    recorder.finalize().map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "stop",
        path: path_payload(&paths),
        affected_segments: 0,
    };
    print_output(&output, format)
}

fn status(
    options: LogRecorderArchiveOptions,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let recorder = open_existing_recorder(&paths)?;
    let stats = recorder.stats();
    let retention = recorder.retention_status().map_err(map_recorder_error)?;
    let output = StatusResult {
        operation: "status",
        path: path_payload(&paths),
        profile: recorder_profile_label(recorder.profile()),
        persistence_mode: persistence_mode_label(recorder.persistence_mode()),
        configured_async_io_backend: async_backend_label(recorder.configured_async_io_backend()),
        effective_async_io_backend: effective_async_backend_label(
            recorder.effective_async_io_backend(),
        ),
        degraded: recorder.is_degraded(),
        stats: StatusStats {
            committed_records: stats.committed_records,
            payload_bytes_committed: stats.payload_bytes_committed,
            data_bytes_written: stats.data_bytes_written,
            metadata_bytes_written: stats.metadata_bytes_written,
            rolled_segments: stats.rolled_segments,
            metadata_log_rolls: stats.metadata_log_rolls,
            last_durable_data_sequence: recorder.last_durable_data_sequence(),
            last_durable_commit_ordinal: recorder.last_durable_commit_ordinal(),
            amplification_ratio: stats.amplification_ratio(),
        },
        retention: StatusRetention {
            max_disk_bytes: retention.max_disk_bytes,
            retained_bytes_total: retention.retained_bytes_total,
            retained_bytes_hot_attached: retention.retained_bytes_hot_attached,
            retained_bytes_cold_detached: retention.retained_bytes_cold_detached,
            segments_hot_attached: retention.segments_hot_attached,
            segments_cold_detached: retention.segments_cold_detached,
            pinned_segments: retention.pinned_segments,
        },
    };
    print_output(&output, format)
}

fn flush(
    options: LogRecorderArchiveOptions,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    recorder.flush().map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "flush",
        path: path_payload(&paths),
        affected_segments: 0,
    };
    print_output(&output, format)
}

fn trim(
    options: LogRecorderArchiveOptions,
    before_sequence: u64,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    let trimmed = recorder
        .trim_before_sequence(before_sequence)
        .map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "trim",
        path: path_payload(&paths),
        affected_segments: trimmed,
    };
    print_output(&output, format)
}

fn detach(
    options: LogRecorderArchiveOptions,
    before_sequence: u64,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    let detached = recorder
        .detach_before_sequence(before_sequence)
        .map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "detach",
        path: path_payload(&paths),
        affected_segments: detached,
    };
    print_output(&output, format)
}

fn attach(
    options: LogRecorderArchiveOptions,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    let attached = recorder.attach_all_detached().map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "attach",
        path: path_payload(&paths),
        affected_segments: attached,
    };
    print_output(&output, format)
}

fn delete_detached(
    options: LogRecorderArchiveOptions,
    before_sequence: u64,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let mut recorder = open_existing_recorder(&paths)?;
    let deleted = recorder
        .delete_detached_before_sequence(before_sequence)
        .map_err(map_recorder_error)?;
    let output = OperationResult {
        operation: "delete-detached",
        path: path_payload(&paths),
        affected_segments: deleted,
    };
    print_output(&output, format)
}

fn list_segments(
    options: LogRecorderArchiveOptions,
    detached_only: bool,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let recorder = open_existing_recorder(&paths)?;
    let mut segments = recorder.list_segments().map_err(map_recorder_error)?;
    if detached_only {
        segments.retain(|segment| segment.tier == ArchiveSegmentTier::ColdDetached);
    }

    let output = ListSegmentsResult {
        operation: "list-segments",
        path: path_payload(&paths),
        segments: segments
            .into_iter()
            .map(|segment| SegmentStatePayload {
                segment_id: segment.segment_id,
                segment_generation: segment.segment_generation,
                sequence_start: segment.sequence_start,
                sequence_end: segment.sequence_end,
                records: segment.records,
                data_bytes_used: segment.data_bytes_used,
                tier: match segment.tier {
                    ArchiveSegmentTier::HotAttached => "HotAttached",
                    ArchiveSegmentTier::ColdDetached => "ColdDetached",
                },
                pinned: segment.pinned,
            })
            .collect(),
    };
    print_output(&output, format)
}

fn inspect_commit_log(
    options: LogRecorderArchiveOptions,
    from_ordinal: u64,
    limit: usize,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    if limit == 0 {
        return Err(LogRecorderCommandError::InvalidInput(
            "--limit must be > 0".to_string(),
        ));
    }

    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let replayer = ArchiveReplayerBuilder::new(&paths.storage_path)
        .metadata_log_path(&paths.metadata_log_path)
        .open()
        .map_err(map_replay_error)?;

    let entries = replayer.inspect_commit_log_entries(
        from_ordinal,
        NonZeroUsize::new(limit).expect("limit was validated as > 0"),
    );

    let output = InspectCommitLogResult {
        operation: "inspect-commit-log",
        path: path_payload(&paths),
        from_ordinal,
        limit,
        entries: entries.into_iter().map(commit_log_entry_payload).collect(),
    };
    print_output(&output, format)
}

fn inspect_record(
    options: LogRecorderArchiveOptions,
    at_sequence: Option<u64>,
    at_locator: Option<String>,
    preview_bytes: usize,
    format: Format,
) -> Result<(), LogRecorderCommandError> {
    let paths = ArchivePaths::from_options(&options)?;
    paths.ensure_archive_exists()?;
    let replayer = ArchiveReplayerBuilder::new(&paths.storage_path)
        .metadata_log_path(&paths.metadata_log_path)
        .open()
        .map_err(map_replay_error)?;

    let (query, frame) = if let Some(sequence) = at_sequence {
        let frame = replayer
            .read_at_sequence(sequence)
            .map_err(map_replay_error)?
            .ok_or_else(|| {
                LogRecorderCommandError::NotAvailable(format!(
                    "sequence {sequence} is not available"
                ))
            })?;
        (format!("sequence:{sequence}"), frame)
    } else if let Some(locator) = at_locator {
        let locator = parse_locator(&locator)?;
        let query = format!(
            "locator:{}:{}:{}:{}",
            locator.segment_id, locator.segment_generation, locator.file_offset, locator.frame_len
        );
        let frame = replayer
            .read_at_locator(locator)
            .map_err(map_replay_error)?;
        (query, frame)
    } else {
        return Err(LogRecorderCommandError::InvalidInput(
            "either --at-sequence or --at-locator is required".to_string(),
        ));
    };

    let output = InspectRecordResult {
        operation: "inspect-record",
        path: path_payload(&paths),
        query,
        record: RecordResult {
            commit_ordinal: frame.commit_ordinal,
            sequence: frame.sequence,
            event_time_ns: frame.event_time_ns,
            commit_time_ns: frame.commit_time_ns,
            segment_id: frame.locator.segment_id,
            segment_generation: frame.locator.segment_generation,
            file_offset: frame.locator.file_offset,
            frame_len: frame.locator.frame_len,
            user_header: preview_bytes_as_hex(&frame.user_header, preview_bytes),
            payload: preview_bytes_as_hex(&frame.payload, preview_bytes),
        },
    };
    print_output(&output, format)
}

fn commit_log_entry_payload(entry: ArchiveCommitLogEntry) -> CommitLogEntryPayload {
    CommitLogEntryPayload {
        commit_ordinal: entry.commit_ordinal,
        sequence: entry.sequence,
        segment_id: entry.locator.segment_id,
        segment_generation: entry.locator.segment_generation,
        file_offset: entry.locator.file_offset,
        frame_len: entry.locator.frame_len,
        frame_checksum: entry.frame_checksum,
        event_time_ns: entry.event_time_ns,
        commit_time_ns: entry.commit_time_ns,
        source_pattern: match entry.source_pattern {
            iox2_log_archive_core::log_archive::ArchiveSourcePattern::Log => "Log",
            iox2_log_archive_core::log_archive::ArchiveSourcePattern::PublishSubscribe => {
                "PublishSubscribe"
            }
            iox2_log_archive_core::log_archive::ArchiveSourcePattern::Pipeline => "Pipeline",
        },
        source_service_id: entry.source_service_id,
        source_instance_id: entry.source_instance_id,
        source_sequence: entry.source_sequence,
        hot_attached: entry.hot_attached,
    }
}

fn preview_bytes_as_hex(payload: &[u8], preview_bytes: usize) -> RecordPayloadPreview {
    let preview_len = payload.len().min(preview_bytes);
    let mut hex = String::with_capacity(preview_len * 2);
    for byte in payload.iter().take(preview_len) {
        use core::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }

    RecordPayloadPreview {
        total_len: payload.len(),
        preview_len,
        truncated: preview_len < payload.len(),
        hex,
    }
}

fn parse_locator(value: &str) -> Result<ArchiveLocator, LogRecorderCommandError> {
    let mut parts = value.split(':');
    let segment_id = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u64>()
        .map_err(|_| invalid_locator(value))?;
    let segment_generation = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u32>()
        .map_err(|_| invalid_locator(value))?;
    let file_offset = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u64>()
        .map_err(|_| invalid_locator(value))?;
    let frame_len = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u32>()
        .map_err(|_| invalid_locator(value))?;
    if parts.next().is_some() {
        return Err(invalid_locator(value));
    }
    if segment_generation == 0 {
        return Err(LogRecorderCommandError::InvalidInput(
            "invalid locator segment generation '0', expected > 0".to_string(),
        ));
    }

    Ok(ArchiveLocator {
        segment_id,
        segment_generation,
        file_offset,
        frame_len,
    })
}

fn invalid_locator(value: &str) -> LogRecorderCommandError {
    LogRecorderCommandError::InvalidInput(format!(
        "invalid locator '{value}', expected <segment_id>:<generation>:<offset>:<frame_len>"
    ))
}

fn open_existing_recorder(
    paths: &ArchivePaths,
) -> Result<iox2_log_archive_core::log_archive::ArchiveRecorder, LogRecorderCommandError> {
    ArchiveRecorderBuilder::new(&paths.storage_path)
        .metadata_log_path(&paths.metadata_log_path)
        .open_or_recover()
        .map_err(map_recorder_error)
}

fn recorder_profile(value: CliRecorderProfile) -> RecorderProfile {
    match value {
        CliRecorderProfile::Durable => RecorderProfile::Durable,
        CliRecorderProfile::Balanced => RecorderProfile::Balanced,
        CliRecorderProfile::Throughput => RecorderProfile::Throughput,
        CliRecorderProfile::Replay => RecorderProfile::Replay,
    }
}

fn persistence_mode(value: CliPersistenceMode) -> PersistenceMode {
    match value {
        CliPersistenceMode::Volatile => PersistenceMode::Volatile,
        CliPersistenceMode::Async => PersistenceMode::Async,
        CliPersistenceMode::Sync => PersistenceMode::Sync,
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

fn persistence_mode_label(value: PersistenceMode) -> &'static str {
    match value {
        PersistenceMode::Volatile => "Volatile",
        PersistenceMode::Async => "Async",
        PersistenceMode::Sync => "Sync",
    }
}

fn async_backend_label(value: iox2_log_archive_core::log_archive::AsyncIoBackend) -> &'static str {
    match value {
        iox2_log_archive_core::log_archive::AsyncIoBackend::IoUringPreferred => "IoUringPreferred",
        iox2_log_archive_core::log_archive::AsyncIoBackend::IoUringRequired => "IoUringRequired",
        iox2_log_archive_core::log_archive::AsyncIoBackend::Blocking => "Blocking",
    }
}

fn effective_async_backend_label(
    value: iox2_log_archive_core::log_archive::EffectiveAsyncIoBackend,
) -> &'static str {
    match value {
        iox2_log_archive_core::log_archive::EffectiveAsyncIoBackend::Blocking => "Blocking",
        iox2_log_archive_core::log_archive::EffectiveAsyncIoBackend::IoUring => "IoUring",
    }
}

fn map_recorder_error(error: ArchiveRecorderError) -> LogRecorderCommandError {
    match error {
        ArchiveRecorderError::MissingArchiveComponent(path)
        | ArchiveRecorderError::ArchiveAlreadyExists(path)
        | ArchiveRecorderError::OutOfSpace(path) => LogRecorderCommandError::NotAvailable(format!(
            "archive component unavailable: {}",
            path.display()
        )),
        ArchiveRecorderError::InvalidConfiguration(message) => {
            LogRecorderCommandError::InvalidInput(message.to_string())
        }
        other => LogRecorderCommandError::Internal(anyhow!("{other:?}")),
    }
}

fn map_replay_error(error: ArchiveReplayError) -> LogRecorderCommandError {
    match error {
        ArchiveReplayError::MissingCommitLog(path) | ArchiveReplayError::MissingSegment(path) => {
            LogRecorderCommandError::NotAvailable(format!(
                "requested data is not available: {}",
                path.display()
            ))
        }
        ArchiveReplayError::InvalidConfiguration(message)
        | ArchiveReplayError::InvalidCommitEntry(message)
        | ArchiveReplayError::InvalidPinState(message) => {
            LogRecorderCommandError::InvalidInput(message.to_string())
        }
        other => LogRecorderCommandError::Internal(anyhow!("{other:?}")),
    }
}

fn path_payload(paths: &ArchivePaths) -> PathPayload<'_> {
    PathPayload {
        service: &paths.service,
        storage_path: paths.storage_path.display().to_string(),
        metadata_log_path: paths.metadata_log_path.display().to_string(),
    }
}

fn print_output<T: Serialize>(payload: &T, format: Format) -> Result<(), LogRecorderCommandError> {
    let output = format
        .as_string(payload)
        .with_context(|| "failed to serialize log-admin output")
        .map_err(LogRecorderCommandError::Internal)?;
    println!("{output}");
    Ok(())
}
