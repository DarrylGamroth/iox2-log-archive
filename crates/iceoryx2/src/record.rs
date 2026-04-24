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

//! Publish-subscribe recording adapter for iceoryx2.

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iceoryx2::prelude::*;
use iceoryx2::sample::Sample as PubSubSample;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::TypeDetail;
use iox2_log_archive_core::log_archive::{
    ArchiveRecorder, ArchiveRecorderBuilder, ArchiveRecorderError, AsyncIoBackend, ChecksumMode,
    DEFAULT_WAIT_DURABLE_DATA_AND_COMMIT_LOG_TIMEOUT, DEFAULT_WAIT_DURABLE_DATA_TIMEOUT,
    EffectiveAsyncIoBackend, OutOfSpacePolicy, PersistenceMode, PublishSubscribeRecordInput,
    RecorderAckLevel, RecorderProfile,
};

use crate::{
    LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP, LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
    LOG_RECORDER_CONTROL_STATE_PAUSED, LOG_RECORDER_CONTROL_STATE_RUNNING,
    LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR, LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST,
    LogRecorderControlRequest, LogRecorderControlResponse, encode_optional_u64,
    log_recorder_control_service_name,
};

/// Configuration for recording one iceoryx2 publish-subscribe service.
#[derive(Debug, Clone)]
pub struct PubSubRecorderConfig {
    /// Logical service name to record.
    pub service: String,
    /// Recorder node name.
    pub node_name: String,
    /// Archive storage root.
    pub storage_path: PathBuf,
    /// Metadata log root.
    pub metadata_log_path: PathBuf,
    /// Recorder profile.
    pub profile: RecorderProfile,
    /// Persistence mode.
    pub persistence_mode: PersistenceMode,
    /// Segment size in bytes.
    pub segment_bytes: usize,
    /// Number of spare preallocated segments.
    pub spare_preallocated_segments: usize,
    /// Whether active segments are preallocated.
    pub segment_preallocate: bool,
    /// Optional max on-disk archive bytes.
    pub max_disk_bytes: Option<u64>,
    /// Optional async data-path backend override.
    pub async_io_backend: Option<AsyncIoBackend>,
    /// Optional `io_uring` queue-depth override.
    pub io_uring_queue_depth: Option<u32>,
    /// Optional `io_uring` submit batch override.
    pub io_submit_batch_max: Option<u32>,
    /// Optional `io_uring` completion batch override.
    pub io_cqe_batch_max: Option<u32>,
    /// Optional `io_uring` registered-file mode override.
    pub io_uring_register_files: Option<bool>,
    /// Optional frame checksum mode override.
    pub checksum_mode: Option<ChecksumMode>,
    /// Optional out-of-space policy override.
    pub out_of_space_policy: Option<OutOfSpacePolicy>,
    /// Optional active metadata-log roll threshold override.
    pub metadata_log_roll_bytes: Option<u64>,
    /// Optional global metadata-log size-cap override.
    pub metadata_log_max_bytes: Option<u64>,
    /// Optional stable source service identity override.
    pub source_service_id: Option<u64>,
    /// Wait interval when no data is available.
    pub cycle_time: Duration,
    /// Optional max captured messages.
    pub max_messages: Option<u64>,
    /// Optional wall-clock timeout.
    pub timeout: Option<Duration>,
    /// Optional periodic flush interval.
    pub flush_interval: Option<Duration>,
    /// Optional per-record ack level.
    pub ack_level: Option<RecorderAckLevel>,
    /// Optional cooperative shutdown flag for signal handlers or embedders.
    pub shutdown_requested: Option<Arc<AtomicBool>>,
}

/// Reason a recorder run stopped.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PubSubRecorderStopReason {
    /// A control-plane stop command was received.
    ControlStop,
    /// The cooperative shutdown flag was set.
    ShutdownRequested,
    /// Configured max-message bound was reached.
    MaxMessages,
    /// Configured wall-clock timeout was reached.
    Timeout,
    /// The iceoryx2 node wait operation was interrupted.
    WaitInterrupted,
}

/// Summary returned after a recorder run completes.
#[derive(Debug, Clone)]
pub struct PubSubRecorderSummary {
    /// Recorder profile used.
    pub profile: RecorderProfile,
    /// Persistence mode used.
    pub persistence_mode: PersistenceMode,
    /// Configured async backend.
    pub configured_async_io_backend: AsyncIoBackend,
    /// Effective async backend.
    pub effective_async_io_backend: EffectiveAsyncIoBackend,
    /// Default ack level.
    pub default_ack_level: RecorderAckLevel,
    /// Requested ack level.
    pub requested_ack_level: Option<RecorderAckLevel>,
    /// Stop reason.
    pub stop_reason: PubSubRecorderStopReason,
    /// Source service id.
    pub source_service_id: u64,
    /// Configured `io_uring` queue depth.
    pub io_uring_queue_depth: u32,
    /// Configured `io_uring` submit batch size.
    pub io_submit_batch_max: u32,
    /// Configured `io_uring` completion batch size.
    pub io_cqe_batch_max: u32,
    /// Configured `io_uring` registered-file mode.
    pub io_uring_register_files: bool,
    /// Configured frame checksum mode.
    pub checksum_mode: ChecksumMode,
    /// Configured out-of-space policy.
    pub out_of_space_policy: OutOfSpacePolicy,
    /// Configured metadata-log roll threshold.
    pub metadata_log_roll_bytes: u64,
    /// Configured metadata-log size cap.
    pub metadata_log_max_bytes: u64,
    /// Flush interval.
    pub flush_interval: Option<Duration>,
    /// Max messages.
    pub max_messages: Option<u64>,
    /// Timeout.
    pub timeout: Option<Duration>,
    /// Messages recorded.
    pub messages_recorded: u64,
    /// Samples dropped while paused.
    pub dropped_while_paused: u64,
    /// Elapsed duration.
    pub elapsed: Duration,
    /// Committed records.
    pub committed_records: u64,
    /// Payload bytes committed.
    pub payload_bytes_committed: u64,
    /// Data bytes written.
    pub data_bytes_written: u64,
    /// Metadata bytes written.
    pub metadata_bytes_written: u64,
    /// Segment roll count.
    pub rolled_segments: u64,
    /// Preallocated segment count.
    pub preallocated_segments: u64,
    /// Out-of-space event count.
    pub out_of_space_events: u64,
    /// Metadata-log roll count.
    pub metadata_log_rolls: u64,
    /// Write amplification ratio.
    pub write_amplification_ratio: f64,
    /// Last durable data sequence.
    pub last_durable_data_sequence: Option<u64>,
    /// Last durable commit ordinal.
    pub last_durable_commit_ordinal: Option<u64>,
    /// Whether recorder was paused at shutdown.
    pub paused_at_shutdown: bool,
    /// Pause timestamp at shutdown.
    pub paused_since_ns_at_shutdown: Option<u64>,
    /// Whether recorder ended degraded.
    pub degraded: bool,
}

/// Error returned by the iceoryx2 pub-sub recorder adapter.
#[derive(Debug)]
pub enum PubSubRecorderError {
    /// Invalid adapter configuration.
    InvalidInput(String),
    /// Service is unavailable.
    NotAvailable(String),
    /// Recorder core error.
    Recorder(ArchiveRecorderError),
    /// iceoryx2 adapter error.
    Iceoryx2(String),
}

impl core::fmt::Display for PubSubRecorderError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) | Self::NotAvailable(message) | Self::Iceoryx2(message) => {
                f.write_str(message)
            }
            Self::Recorder(error) => write!(f, "{error:?}"),
        }
    }
}

impl std::error::Error for PubSubRecorderError {}

impl From<ArchiveRecorderError> for PubSubRecorderError {
    fn from(value: ArchiveRecorderError) -> Self {
        Self::Recorder(value)
    }
}

#[derive(Debug, Clone, Copy)]
struct ServiceTypes {
    payload: TypeDetail,
    user_header: TypeDetail,
}

/// Records one iceoryx2 publish-subscribe service into an archive.
pub fn record_publish_subscribe(
    config: PubSubRecorderConfig,
) -> Result<PubSubRecorderSummary, PubSubRecorderError> {
    validate_config(&config)?;

    let node = NodeBuilder::new()
        .name(&NodeName::new(&config.node_name).map_err(to_iox2_error)?)
        .create::<ipc::Service>()
        .map_err(to_iox2_error)?;

    let service_name = ServiceName::new(&config.service).map_err(to_iox2_error)?;
    let service_types = get_pubsub_service_types(&service_name, &node)?;

    let service = unsafe {
        node.service_builder(&service_name)
            .publish_subscribe::<[CustomPayloadMarker]>()
            .user_header::<CustomHeaderMarker>()
            .__internal_set_payload_type_details(&service_types.payload)
            .__internal_set_user_header_type_details(&service_types.user_header)
            .open_or_create()
    }
    .map_err(to_iox2_error)?;

    let subscriber = service
        .subscriber_builder()
        .create()
        .map_err(to_iox2_error)?;

    let control_service_name =
        ServiceName::new(&log_recorder_control_service_name(&config.service))
            .map_err(to_iox2_error)?;
    let control_service = node
        .service_builder(&control_service_name)
        .request_response::<LogRecorderControlRequest, LogRecorderControlResponse>()
        .open_or_create()
        .map_err(to_iox2_error)?;
    let control_server = control_service
        .server_builder()
        .create()
        .map_err(to_iox2_error)?;

    let mut recorder = recorder_builder(&config).open_or_recover()?;
    let source_service_id = config
        .source_service_id
        .unwrap_or_else(|| stable_service_id(&config.service));

    let start = Instant::now();
    let mut last_flush = Instant::now();
    let mut messages_recorded = 0u64;
    let mut dropped_while_paused = 0u64;
    let mut is_paused = false;
    let mut paused_since_ns = None;
    let mut stop_requested = false;
    let mut stop_reason = None;

    let mut poll_control_requests =
        |recorder: &mut ArchiveRecorder,
         is_paused: &mut bool,
         paused_since_ns: &mut Option<u64>,
         dropped_while_paused: &mut u64,
         stop_requested: &mut bool| {
            while let Some(active_request) = control_server.receive().map_err(to_iox2_error)? {
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
                            stop_reason = Some(PubSubRecorderStopReason::ControlStop);
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

            Ok::<(), PubSubRecorderError>(())
        };

    'record_loop: loop {
        if shutdown_requested(&config) {
            stop_reason = Some(PubSubRecorderStopReason::ShutdownRequested);
            break;
        }

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

        while let Some(sample) =
            unsafe { subscriber.receive_custom_payload() }.map_err(to_iox2_error)?
        {
            if shutdown_requested(&config) {
                stop_reason = Some(PubSubRecorderStopReason::ShutdownRequested);
                break 'record_loop;
            }

            let (user_header, payload) =
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

                if let Some(level) = config.ack_level {
                    recorder.append_publish_subscribe_record_with_ack(
                        input,
                        level,
                        ack_timeout(level),
                    )?;
                } else {
                    recorder.append_publish_subscribe_record(input)?;
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

            if let Some(reason) = bounded_stop_reason(
                messages_recorded,
                config.max_messages,
                start.elapsed(),
                config.timeout,
            ) {
                stop_reason = Some(reason);
                break 'record_loop;
            }

            if let Some(interval) = config.flush_interval {
                if last_flush.elapsed() >= interval {
                    recorder.flush()?;
                    last_flush = Instant::now();
                }
            }
        }

        if let Some(reason) = bounded_stop_reason(
            messages_recorded,
            config.max_messages,
            start.elapsed(),
            config.timeout,
        ) {
            stop_reason = Some(reason);
            break;
        }

        if stop_requested {
            break;
        }

        if let Some(interval) = config.flush_interval {
            if last_flush.elapsed() >= interval {
                recorder.flush()?;
                last_flush = Instant::now();
            }
        }

        if shutdown_requested(&config) {
            stop_reason = Some(PubSubRecorderStopReason::ShutdownRequested);
            break;
        }

        if node.wait(config.cycle_time).is_err() {
            stop_reason = Some(PubSubRecorderStopReason::WaitInterrupted);
            break;
        }
    }

    recorder.finalize()?;

    let stats = recorder.stats();
    Ok(PubSubRecorderSummary {
        profile: recorder.profile(),
        persistence_mode: recorder.persistence_mode(),
        configured_async_io_backend: recorder.configured_async_io_backend(),
        effective_async_io_backend: recorder.effective_async_io_backend(),
        default_ack_level: recorder.default_ack_level(),
        requested_ack_level: config.ack_level,
        stop_reason: stop_reason.unwrap_or(PubSubRecorderStopReason::ControlStop),
        source_service_id,
        io_uring_queue_depth: recorder.io_uring_queue_depth(),
        io_submit_batch_max: recorder.io_submit_batch_max(),
        io_cqe_batch_max: recorder.io_cqe_batch_max(),
        io_uring_register_files: recorder.io_uring_register_files(),
        checksum_mode: recorder.checksum_mode(),
        out_of_space_policy: recorder.out_of_space_policy(),
        metadata_log_roll_bytes: recorder.metadata_log_roll_bytes(),
        metadata_log_max_bytes: recorder.metadata_log_max_bytes(),
        flush_interval: config.flush_interval,
        max_messages: config.max_messages,
        timeout: config.timeout,
        messages_recorded,
        dropped_while_paused,
        elapsed: start.elapsed(),
        committed_records: stats.committed_records,
        payload_bytes_committed: stats.payload_bytes_committed,
        data_bytes_written: stats.data_bytes_written,
        metadata_bytes_written: stats.metadata_bytes_written,
        rolled_segments: stats.rolled_segments,
        preallocated_segments: stats.preallocated_segments,
        out_of_space_events: stats.out_of_space_events,
        metadata_log_rolls: stats.metadata_log_rolls,
        write_amplification_ratio: stats.amplification_ratio(),
        last_durable_data_sequence: recorder.last_durable_data_sequence(),
        last_durable_commit_ordinal: recorder.last_durable_commit_ordinal(),
        paused_at_shutdown: is_paused,
        paused_since_ns_at_shutdown: paused_since_ns,
        degraded: recorder.is_degraded(),
    })
}

fn recorder_builder(config: &PubSubRecorderConfig) -> ArchiveRecorderBuilder {
    let mut builder = ArchiveRecorderBuilder::new(&config.storage_path)
        .metadata_log_path(&config.metadata_log_path)
        .profile(config.profile)
        .persistence_mode(config.persistence_mode)
        .segment_bytes(config.segment_bytes)
        .spare_preallocated_segments(config.spare_preallocated_segments)
        .segment_preallocate(config.segment_preallocate);

    if let Some(async_io_backend) = config.async_io_backend {
        builder = builder.async_io_backend(async_io_backend);
    }
    if let Some(io_uring_queue_depth) = config.io_uring_queue_depth {
        builder = builder.io_uring_queue_depth(io_uring_queue_depth);
    }
    if let Some(io_submit_batch_max) = config.io_submit_batch_max {
        builder = builder.io_submit_batch_max(io_submit_batch_max);
    }
    if let Some(io_cqe_batch_max) = config.io_cqe_batch_max {
        builder = builder.io_cqe_batch_max(io_cqe_batch_max);
    }
    if let Some(io_uring_register_files) = config.io_uring_register_files {
        builder = builder.io_uring_register_files(io_uring_register_files);
    }
    if let Some(checksum_mode) = config.checksum_mode {
        builder = builder.checksum_mode(checksum_mode);
    }
    if let Some(out_of_space_policy) = config.out_of_space_policy {
        builder = builder.out_of_space_policy(out_of_space_policy);
    }
    if let Some(metadata_log_roll_bytes) = config.metadata_log_roll_bytes {
        builder = builder.metadata_log_roll_bytes(metadata_log_roll_bytes);
    }
    if let Some(metadata_log_max_bytes) = config.metadata_log_max_bytes {
        builder = builder.metadata_log_max_bytes(metadata_log_max_bytes);
    }
    if let Some(max_disk_bytes) = config.max_disk_bytes {
        builder = builder.max_disk_bytes(max_disk_bytes);
    }

    builder
}

fn validate_config(config: &PubSubRecorderConfig) -> Result<(), PubSubRecorderError> {
    if config.service.trim().is_empty() {
        return Err(PubSubRecorderError::InvalidInput(
            "service must not be empty".to_string(),
        ));
    }
    if config.node_name.trim().is_empty() {
        return Err(PubSubRecorderError::InvalidInput(
            "node_name must not be empty".to_string(),
        ));
    }
    if config.cycle_time.is_zero() {
        return Err(PubSubRecorderError::InvalidInput(
            "cycle_time must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn get_pubsub_service_types(
    service_name: &ServiceName,
    node: &Node<ipc::Service>,
) -> Result<ServiceTypes, PubSubRecorderError> {
    let service_details = match ipc::Service::details(
        service_name,
        node.config(),
        MessagingPattern::PublishSubscribe,
    )
    .map_err(to_iox2_error)?
    {
        Some(details) => details,
        None => {
            return Err(PubSubRecorderError::NotAvailable(format!(
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
) -> (&'a [u8], &'a [u8]) {
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

    (user_header, payload)
}

fn bounded_stop_reason(
    messages_recorded: u64,
    max_messages: Option<u64>,
    elapsed: Duration,
    timeout: Option<Duration>,
) -> Option<PubSubRecorderStopReason> {
    if max_messages.is_some_and(|max| messages_recorded >= max) {
        return Some(PubSubRecorderStopReason::MaxMessages);
    }
    if timeout.is_some_and(|timeout| elapsed >= timeout) {
        return Some(PubSubRecorderStopReason::Timeout);
    }
    None
}

fn shutdown_requested(config: &PubSubRecorderConfig) -> bool {
    config
        .shutdown_requested
        .as_ref()
        .is_some_and(|flag| flag.load(Ordering::Relaxed))
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

fn control_response_for_recorder(
    recorder: &ArchiveRecorder,
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

fn to_iox2_error(error: impl core::fmt::Debug) -> PubSubRecorderError {
    PubSubRecorderError::Iceoryx2(format!("{error:?}"))
}
