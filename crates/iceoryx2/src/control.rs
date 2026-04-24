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

//! iceoryx2 request-response control protocol for live recorder workers.

use std::time::{Duration, Instant};

use iceoryx2::prelude::ZeroCopySend;
use iceoryx2::prelude::*;

/// Recorder control protocol version.
pub const LOG_RECORDER_CONTROL_PROTOCOL_VERSION: u16 = 3;
/// Sentinel used for absent optional `u64` values.
pub const LOG_RECORDER_CONTROL_NONE: u64 = u64::MAX;

/// Query worker status.
pub const LOG_RECORDER_CONTROL_CMD_STATUS: u16 = 1;
/// Flush the recorder.
pub const LOG_RECORDER_CONTROL_CMD_FLUSH: u16 = 2;
/// Request graceful worker shutdown.
pub const LOG_RECORDER_CONTROL_CMD_STOP: u16 = 3;
/// Pause ingestion.
pub const LOG_RECORDER_CONTROL_CMD_PAUSE: u16 = 4;
/// Resume ingestion.
pub const LOG_RECORDER_CONTROL_CMD_RESUME: u16 = 5;

/// Request succeeded.
pub const LOG_RECORDER_CONTROL_STATUS_OK: u16 = 0;
/// Request was malformed or unsupported.
pub const LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST: u16 = 1;
/// Request failed inside the recorder.
pub const LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR: u16 = 2;

/// Recorder is ingesting samples.
pub const LOG_RECORDER_CONTROL_STATE_RUNNING: u16 = 0;
/// Recorder is alive but dropping incoming samples.
pub const LOG_RECORDER_CONTROL_STATE_PAUSED: u16 = 1;

/// Suffix appended to a recorded service name for the control service.
pub const LOG_RECORDER_CONTROL_SUFFIX: &str = "_log_recorder_control";

/// Request payload for the recorder control protocol.
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub struct LogRecorderControlRequest {
    /// Protocol version.
    pub protocol_version: u16,
    /// Command discriminator.
    pub command: u16,
    /// Reserved for future flags.
    pub reserved: u32,
}

impl LogRecorderControlRequest {
    /// Creates a request with the current protocol version.
    pub const fn new(command: u16) -> Self {
        Self {
            protocol_version: LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
            command,
            reserved: 0,
        }
    }
}

/// Response payload for the recorder control protocol.
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub struct LogRecorderControlResponse {
    /// Protocol version.
    pub protocol_version: u16,
    /// Status discriminator.
    pub status: u16,
    /// Recorder state discriminator.
    pub state: u16,
    /// Reserved for future flags.
    pub reserved: u16,
    /// Number of committed records.
    pub committed_records: u64,
    /// Number of committed payload bytes.
    pub payload_bytes_committed: u64,
    /// Number of archive data bytes written.
    pub data_bytes_written: u64,
    /// Number of metadata bytes written.
    pub metadata_bytes_written: u64,
    /// Last durable data sequence, or [`LOG_RECORDER_CONTROL_NONE`].
    pub last_durable_data_sequence: u64,
    /// Last durable commit ordinal, or [`LOG_RECORDER_CONTROL_NONE`].
    pub last_durable_commit_ordinal: u64,
    /// Samples dropped while paused.
    pub dropped_while_paused: u64,
    /// Pause start timestamp in ns, or [`LOG_RECORDER_CONTROL_NONE`].
    pub paused_since_ns: u64,
}

impl LogRecorderControlResponse {
    /// Creates an error response.
    pub const fn error(status: u16) -> Self {
        Self {
            protocol_version: LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
            status,
            state: LOG_RECORDER_CONTROL_STATE_RUNNING,
            reserved: 0,
            committed_records: 0,
            payload_bytes_committed: 0,
            data_bytes_written: 0,
            metadata_bytes_written: 0,
            last_durable_data_sequence: LOG_RECORDER_CONTROL_NONE,
            last_durable_commit_ordinal: LOG_RECORDER_CONTROL_NONE,
            dropped_while_paused: 0,
            paused_since_ns: LOG_RECORDER_CONTROL_NONE,
        }
    }

    /// Creates a successful response.
    #[allow(clippy::too_many_arguments)]
    pub const fn ok(
        state: u16,
        committed_records: u64,
        payload_bytes_committed: u64,
        data_bytes_written: u64,
        metadata_bytes_written: u64,
        last_durable_data_sequence: u64,
        last_durable_commit_ordinal: u64,
        dropped_while_paused: u64,
        paused_since_ns: u64,
    ) -> Self {
        Self {
            protocol_version: LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
            status: LOG_RECORDER_CONTROL_STATUS_OK,
            state,
            reserved: 0,
            committed_records,
            payload_bytes_committed,
            data_bytes_written,
            metadata_bytes_written,
            last_durable_data_sequence,
            last_durable_commit_ordinal,
            dropped_while_paused,
            paused_since_ns,
        }
    }
}

/// Returns the control service name for a recorded service.
pub fn log_recorder_control_service_name(recorded_service: &str) -> String {
    format!(
        "{}/{LOG_RECORDER_CONTROL_SUFFIX}",
        recorded_service.trim_end_matches('/')
    )
}

/// Encodes an optional `u64` for zero-copy control payloads.
pub const fn encode_optional_u64(value: Option<u64>) -> u64 {
    match value {
        Some(value) => value,
        None => LOG_RECORDER_CONTROL_NONE,
    }
}

/// Decodes an optional `u64` from zero-copy control payloads.
pub const fn decode_optional_u64(value: u64) -> Option<u64> {
    if value == LOG_RECORDER_CONTROL_NONE {
        None
    } else {
        Some(value)
    }
}

/// Configuration for one recorder control request.
#[derive(Debug, Clone)]
pub struct LogRecorderControlClientConfig {
    /// Recorded service name.
    pub service: String,
    /// Control client node name.
    pub node_name: String,
    /// Response timeout.
    pub timeout: Duration,
}

/// Response data for one recorder control request.
#[derive(Debug, Clone)]
pub struct LogRecorderControlResult {
    /// Control service name used.
    pub control_service: String,
    /// Daemon status.
    pub daemon_status: LogRecorderDaemonStatus,
    /// Whether the recorder is paused.
    pub is_paused: bool,
    /// Samples dropped while paused.
    pub dropped_while_paused: u64,
    /// Pause start timestamp in ns.
    pub paused_since_ns: Option<u64>,
    /// Number of committed records.
    pub committed_records: u64,
    /// Number of committed payload bytes.
    pub payload_bytes_committed: u64,
    /// Number of archive data bytes written.
    pub data_bytes_written: u64,
    /// Number of metadata bytes written.
    pub metadata_bytes_written: u64,
    /// Last durable data sequence.
    pub last_durable_data_sequence: Option<u64>,
    /// Last durable commit ordinal.
    pub last_durable_commit_ordinal: Option<u64>,
}

/// Normalized daemon status.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogRecorderDaemonStatus {
    /// Request completed successfully.
    Ok,
}

/// Error returned by recorder control clients.
#[derive(Debug)]
pub enum LogRecorderControlError {
    /// Invalid input.
    InvalidInput(String),
    /// Control service or daemon is unavailable.
    NotAvailable(String),
    /// iceoryx2 client failure.
    Iceoryx2(String),
}

impl core::fmt::Display for LogRecorderControlError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) | Self::NotAvailable(message) | Self::Iceoryx2(message) => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for LogRecorderControlError {}

/// Sends one command to a running recorder daemon.
pub fn request_recorder_control(
    config: LogRecorderControlClientConfig,
    command: u16,
) -> Result<LogRecorderControlResult, LogRecorderControlError> {
    validate_control_config(&config)?;

    let control_service = log_recorder_control_service_name(&config.service);
    let node = NodeBuilder::new()
        .name(&NodeName::new(&config.node_name).map_err(to_control_iox2_error)?)
        .create::<ipc::Service>()
        .map_err(to_control_iox2_error)?;

    let control_service_name = ServiceName::new(&control_service).map_err(to_control_iox2_error)?;

    let request_response = node
        .service_builder(&control_service_name)
        .request_response::<LogRecorderControlRequest, LogRecorderControlResponse>()
        .open()
        .map_err(|_| {
            LogRecorderControlError::NotAvailable(format!(
                "recorder daemon control service '{control_service}' is not available",
            ))
        })?;

    let client = request_response
        .client_builder()
        .create()
        .map_err(to_control_iox2_error)?;

    let response = send_request(
        &node,
        &client,
        LogRecorderControlRequest::new(command),
        config.timeout,
    )?;

    let daemon_status = match response.status {
        LOG_RECORDER_CONTROL_STATUS_OK => LogRecorderDaemonStatus::Ok,
        LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST => {
            return Err(LogRecorderControlError::InvalidInput(
                "daemon rejected command as invalid".to_string(),
            ));
        }
        LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR => {
            return Err(LogRecorderControlError::NotAvailable(
                "daemon failed to execute command".to_string(),
            ));
        }
        status => {
            return Err(LogRecorderControlError::Iceoryx2(format!(
                "daemon returned unknown status code {status}",
            )));
        }
    };

    let is_paused = match response.state {
        LOG_RECORDER_CONTROL_STATE_RUNNING => false,
        LOG_RECORDER_CONTROL_STATE_PAUSED => true,
        state => {
            return Err(LogRecorderControlError::Iceoryx2(format!(
                "daemon returned unknown state code {state}",
            )));
        }
    };

    Ok(LogRecorderControlResult {
        control_service,
        daemon_status,
        is_paused,
        dropped_while_paused: response.dropped_while_paused,
        paused_since_ns: decode_optional_u64(response.paused_since_ns),
        committed_records: response.committed_records,
        payload_bytes_committed: response.payload_bytes_committed,
        data_bytes_written: response.data_bytes_written,
        metadata_bytes_written: response.metadata_bytes_written,
        last_durable_data_sequence: decode_optional_u64(response.last_durable_data_sequence),
        last_durable_commit_ordinal: decode_optional_u64(response.last_durable_commit_ordinal),
    })
}

fn validate_control_config(
    config: &LogRecorderControlClientConfig,
) -> Result<(), LogRecorderControlError> {
    if config.service.trim().is_empty() {
        return Err(LogRecorderControlError::InvalidInput(
            "service must not be empty".to_string(),
        ));
    }
    if config.node_name.trim().is_empty() {
        return Err(LogRecorderControlError::InvalidInput(
            "node_name must not be empty".to_string(),
        ));
    }
    if config.timeout.is_zero() {
        return Err(LogRecorderControlError::InvalidInput(
            "timeout must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn send_request(
    node: &Node<ipc::Service>,
    client: &iceoryx2::port::client::Client<
        ipc::Service,
        LogRecorderControlRequest,
        (),
        LogRecorderControlResponse,
        (),
    >,
    request: LogRecorderControlRequest,
    timeout: Duration,
) -> Result<LogRecorderControlResponse, LogRecorderControlError> {
    let pending_response = client.send_copy(request).map_err(to_control_iox2_error)?;

    if pending_response.number_of_server_connections() == 0 {
        return Err(LogRecorderControlError::NotAvailable(
            "recorder daemon is not connected to control service".to_string(),
        ));
    }

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(response) = pending_response.receive().map_err(to_control_iox2_error)? {
            if response.protocol_version != LOG_RECORDER_CONTROL_PROTOCOL_VERSION {
                return Err(LogRecorderControlError::NotAvailable(format!(
                    "daemon protocol version mismatch: expected {}, got {}",
                    LOG_RECORDER_CONTROL_PROTOCOL_VERSION, response.protocol_version
                )));
            }

            return Ok(*response);
        }

        if Instant::now() >= deadline {
            return Err(LogRecorderControlError::NotAvailable(
                "timed out waiting for recorder daemon response".to_string(),
            ));
        }

        if node.wait(Duration::from_millis(2)).is_err() {
            return Err(LogRecorderControlError::NotAvailable(
                "control client wait interrupted while awaiting response".to_string(),
            ));
        }
    }
}

fn to_control_iox2_error(error: impl core::fmt::Debug) -> LogRecorderControlError {
    LogRecorderControlError::Iceoryx2(format!("{error:?}"))
}
