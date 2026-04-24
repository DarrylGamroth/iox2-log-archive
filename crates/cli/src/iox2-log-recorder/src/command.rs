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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow};
use iceoryx2::prelude::*;
use iceoryx2::sample::Sample as PubSubSample;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::TypeDetail;
use iox2_log_archive_cli::{
    Format, LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP, LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
    LOG_RECORDER_CONTROL_STATE_PAUSED, LOG_RECORDER_CONTROL_STATE_RUNNING,
    LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR, LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST,
    LogRecorderControlRequest, LogRecorderControlResponse, encode_optional_u64,
    log_recorder_control_service_name,
};
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, ArchiveRecorderError, DEFAULT_WAIT_DURABLE_DATA_AND_COMMIT_LOG_TIMEOUT,
    DEFAULT_WAIT_DURABLE_DATA_TIMEOUT, EffectiveAsyncIoBackend, PersistenceMode,
    PublishSubscribeRecordInput, RecorderAckLevel, RecorderProfile,
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

#[derive(Debug, Clone, Copy)]
struct ServiceTypes {
    payload: TypeDetail,
    user_header: TypeDetail,
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

    let node = NodeBuilder::new()
        .name(
            &NodeName::new(&options.runtime.common.node_name)
                .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?,
        )
        .create::<ipc::Service>()
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;

    let service_name = ServiceName::new(&paths.service)
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;
    let service_types = get_pubsub_service_types(&service_name, &node)?;

    let service = unsafe {
        node.service_builder(&service_name)
            .publish_subscribe::<[CustomPayloadMarker]>()
            .user_header::<CustomHeaderMarker>()
            .__internal_set_payload_type_details(&service_types.payload)
            .__internal_set_user_header_type_details(&service_types.user_header)
            .open_or_create()
    }
    .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;

    let subscriber = service
        .subscriber_builder()
        .create()
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;

    let control_service_name = ServiceName::new(&log_recorder_control_service_name(&paths.service))
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;
    let control_service = node
        .service_builder(&control_service_name)
        .request_response::<LogRecorderControlRequest, LogRecorderControlResponse>()
        .open_or_create()
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;
    let control_server = control_service
        .server_builder()
        .create()
        .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?;

    let mut recorder = recorder_builder(&options.archive, &paths)?
        .open_or_recover()
        .map_err(map_recorder_error)?;

    let cycle_time = Duration::from_millis(options.runtime.common.cycle_time_ms);
    let timeout = options.runtime.common.timeout_ms.map(Duration::from_millis);
    let flush_interval = non_zero_duration(options.runtime.common.flush_interval_ms);
    let source_service_id = options
        .runtime
        .source_service_id
        .unwrap_or_else(|| stable_service_id(&paths.service));

    let start = Instant::now();
    let mut last_flush = Instant::now();
    let mut messages_recorded = 0u64;
    let mut dropped_while_paused = 0u64;
    let mut is_paused = false;
    let mut paused_since_ns = None;
    let mut stop_requested = false;

    let poll_control_requests =
        |recorder: &mut iox2_log_archive_core::log_archive::ArchiveRecorder,
         is_paused: &mut bool,
         paused_since_ns: &mut Option<u64>,
         dropped_while_paused: &mut u64,
         stop_requested: &mut bool| {
            while let Some(active_request) = control_server
                .receive()
                .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?
            {
                let request = *active_request;
                let response = if request.protocol_version != LOG_RECORDER_CONTROL_PROTOCOL_VERSION
                {
                    LogRecorderControlResponse::error(LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST)
                } else {
                    match request.command {
                        LOG_RECORDER_CONTROL_CMD_STATUS => control_response_for_recorder(
                            recorder,
                            *is_paused,
                            *paused_since_ns,
                            *dropped_while_paused,
                        ),
                        LOG_RECORDER_CONTROL_CMD_FLUSH => match recorder.flush() {
                            Ok(()) => control_response_for_recorder(
                                recorder,
                                *is_paused,
                                *paused_since_ns,
                                *dropped_while_paused,
                            ),
                            Err(_) => LogRecorderControlResponse::error(
                                LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR,
                            ),
                        },
                        LOG_RECORDER_CONTROL_CMD_PAUSE => {
                            if !*is_paused {
                                *is_paused = true;
                                *paused_since_ns = Some(unix_time_now_ns());
                            }
                            control_response_for_recorder(
                                recorder,
                                *is_paused,
                                *paused_since_ns,
                                *dropped_while_paused,
                            )
                        }
                        LOG_RECORDER_CONTROL_CMD_RESUME => {
                            *is_paused = false;
                            *paused_since_ns = None;
                            control_response_for_recorder(
                                recorder,
                                *is_paused,
                                *paused_since_ns,
                                *dropped_while_paused,
                            )
                        }
                        LOG_RECORDER_CONTROL_CMD_STOP => {
                            *stop_requested = true;
                            control_response_for_recorder(
                                recorder,
                                *is_paused,
                                *paused_since_ns,
                                *dropped_while_paused,
                            )
                        }
                        _ => LogRecorderControlResponse::error(
                            LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST,
                        ),
                    }
                };

                let _ = active_request.send_copy(response);
            }

            Ok::<(), LogRecordCommandError>(())
        };

    'record_loop: loop {
        poll_control_requests(
            &mut recorder,
            &mut is_paused,
            &mut paused_since_ns,
            &mut dropped_while_paused,
            &mut stop_requested,
        )?;
        if stop_requested {
            break;
        }

        while let Some(sample) = unsafe { subscriber.receive_custom_payload() }
            .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?
        {
            let (_system_header, user_header, payload) =
                extract_pubsub_payload(&sample, &service_types.user_header);

            if is_paused {
                dropped_while_paused = dropped_while_paused.saturating_add(1);
            } else {
                let input = PublishSubscribeRecordInput {
                    event_time_ns: unix_time_now_ns(),
                    source_service_id,
                    source_publisher_id: fold_u128_to_u64(sample.origin().value()),
                    source_sequence: None,
                    user_header,
                    payload,
                };

                if let Some(level) = requested_ack_level {
                    recorder
                        .append_publish_subscribe_record_with_ack(input, level, ack_timeout(level))
                        .map_err(map_recorder_error)?;
                } else {
                    recorder
                        .append_publish_subscribe_record(input)
                        .map_err(map_recorder_error)?;
                }

                messages_recorded = messages_recorded.saturating_add(1);
            }

            poll_control_requests(
                &mut recorder,
                &mut is_paused,
                &mut paused_since_ns,
                &mut dropped_while_paused,
                &mut stop_requested,
            )?;
            if stop_requested {
                break 'record_loop;
            }

            if should_stop(
                messages_recorded,
                options.runtime.common.max_messages,
                start.elapsed(),
                timeout,
            ) {
                break 'record_loop;
            }

            if let Some(interval) = flush_interval {
                if last_flush.elapsed() >= interval {
                    recorder.flush().map_err(map_recorder_error)?;
                    last_flush = Instant::now();
                }
            }
        }

        if should_stop(
            messages_recorded,
            options.runtime.common.max_messages,
            start.elapsed(),
            timeout,
        ) {
            break;
        }

        if stop_requested {
            break;
        }

        if let Some(interval) = flush_interval {
            if last_flush.elapsed() >= interval {
                recorder.flush().map_err(map_recorder_error)?;
                last_flush = Instant::now();
            }
        }

        if node.wait(cycle_time).is_err() {
            break;
        }
    }

    recorder.finalize().map_err(map_recorder_error)?;

    let stats = recorder.stats();
    let summary = RecordSummary {
        operation: "record-publish-subscribe",
        path: path_payload(&paths),
        profile: recorder_profile_label(recorder.profile()),
        persistence_mode: persistence_mode_label(recorder.persistence_mode()),
        configured_async_io_backend: async_backend_label(recorder.configured_async_io_backend()),
        effective_async_io_backend: effective_async_backend_label(
            recorder.effective_async_io_backend(),
        ),
        default_ack_level: ack_level_label(recorder.default_ack_level()),
        requested_ack_level: requested_ack_level.map(ack_level_label),
        source_service_id: Some(source_service_id),
        flush_interval_ms: options.runtime.common.flush_interval_ms,
        max_messages: options.runtime.common.max_messages,
        timeout_ms: options.runtime.common.timeout_ms,
        messages_recorded,
        dropped_while_paused,
        elapsed_ms: start.elapsed().as_millis(),
        committed_records: stats.committed_records,
        payload_bytes_committed: stats.payload_bytes_committed,
        last_durable_data_sequence: recorder.last_durable_data_sequence(),
        last_durable_commit_ordinal: recorder.last_durable_commit_ordinal(),
        paused_at_shutdown: is_paused,
        paused_since_ns_at_shutdown: paused_since_ns,
        degraded: recorder.is_degraded(),
    };

    print_output(&summary, format)
}

fn recorder_builder(
    options: &LogRecordArchiveOptions,
    paths: &ArchivePaths,
) -> Result<ArchiveRecorderBuilder, LogRecordCommandError> {
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

    Ok(builder)
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

fn get_pubsub_service_types(
    service_name: &ServiceName,
    node: &Node<ipc::Service>,
) -> Result<ServiceTypes, LogRecordCommandError> {
    let service_details = match ipc::Service::details(
        service_name,
        node.config(),
        MessagingPattern::PublishSubscribe,
    )
    .map_err(|error| LogRecordCommandError::Internal(anyhow!(error)))?
    {
        Some(details) => details,
        None => {
            return Err(LogRecordCommandError::NotAvailable(format!(
                "unable to access publish-subscribe service \"{service_name}\", does it exist?"
            )));
        }
    };

    let details = service_details
        .static_details
        .publish_subscribe()
        .message_type_details();

    Ok(ServiceTypes {
        payload: details.payload,
        user_header: details.user_header,
    })
}

fn extract_pubsub_payload<'a>(
    sample: &'a PubSubSample<ipc::Service, [CustomPayloadMarker], CustomHeaderMarker>,
    user_header_type: &TypeDetail,
) -> (&'a [u8], &'a [u8], &'a [u8]) {
    let system_header = unsafe {
        core::slice::from_raw_parts(
            (sample.header() as *const iceoryx2::service::header::publish_subscribe::Header).cast(),
            core::mem::size_of::<iceoryx2::service::header::publish_subscribe::Header>(),
        )
    };
    let user_header = unsafe {
        core::slice::from_raw_parts(
            (sample.user_header() as *const CustomHeaderMarker).cast(),
            user_header_type.size(),
        )
    };
    let payload = unsafe {
        core::slice::from_raw_parts(
            sample.payload().as_ptr().cast::<u8>(),
            sample.payload().len(),
        )
    };

    (system_header, user_header, payload)
}

fn should_stop(
    messages_recorded: u64,
    max_messages: Option<u64>,
    elapsed: Duration,
    timeout: Option<Duration>,
) -> bool {
    if let Some(max_messages) = max_messages {
        if messages_recorded >= max_messages {
            return true;
        }
    }

    if let Some(timeout) = timeout {
        if elapsed >= timeout {
            return true;
        }
    }

    false
}

fn non_zero_duration(value_ms: u64) -> Option<Duration> {
    if value_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(value_ms))
    }
}

fn unix_time_now_ns() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn fold_u128_to_u64(value: u128) -> u64 {
    let lower = value as u64;
    let upper = (value >> 64) as u64;
    lower ^ upper
}

fn stable_service_id(service: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in service.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn ack_timeout(level: RecorderAckLevel) -> Duration {
    match level {
        RecorderAckLevel::Accepted => Duration::ZERO,
        RecorderAckLevel::DurableData => DEFAULT_WAIT_DURABLE_DATA_TIMEOUT,
        RecorderAckLevel::DurableDataAndCommitLog => {
            DEFAULT_WAIT_DURABLE_DATA_AND_COMMIT_LOG_TIMEOUT
        }
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

fn control_response_for_recorder(
    recorder: &iox2_log_archive_core::log_archive::ArchiveRecorder,
    is_paused: bool,
    paused_since_ns: Option<u64>,
    dropped_while_paused: u64,
) -> LogRecorderControlResponse {
    let stats = recorder.stats();
    LogRecorderControlResponse::ok(
        if is_paused {
            LOG_RECORDER_CONTROL_STATE_PAUSED
        } else {
            LOG_RECORDER_CONTROL_STATE_RUNNING
        },
        stats.committed_records,
        stats.payload_bytes_committed,
        stats.data_bytes_written,
        stats.metadata_bytes_written,
        encode_optional_u64(recorder.last_durable_data_sequence()),
        encode_optional_u64(recorder.last_durable_commit_ordinal()),
        dropped_while_paused,
        encode_optional_u64(paused_since_ns),
    )
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
