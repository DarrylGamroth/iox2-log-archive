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

use iceoryx2::prelude::ZeroCopySend;

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
