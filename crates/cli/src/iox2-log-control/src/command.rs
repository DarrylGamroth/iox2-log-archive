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

use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use iceoryx2::prelude::*;
use iox2_log_archive_cli::{
    Format, LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP, LOG_RECORDER_CONTROL_PROTOCOL_VERSION,
    LOG_RECORDER_CONTROL_STATE_PAUSED, LOG_RECORDER_CONTROL_STATE_RUNNING,
    LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR, LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST,
    LOG_RECORDER_CONTROL_STATUS_OK, LogRecorderControlRequest, LogRecorderControlResponse,
    decode_optional_u64, log_recorder_control_service_name,
};
use serde::Serialize;

use crate::cli::{LogControlAction, LogControlOptions};

#[derive(Debug)]
pub(crate) enum LogControlCommandError {
    InvalidInput(String),
    NotAvailable(String),
    Internal(anyhow::Error),
}

impl LogControlCommandError {
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
            LogControlCommandError::InvalidInput(message) => ErrorPayload {
                error_code: "InvalidInput",
                message,
            },
            LogControlCommandError::NotAvailable(message) => ErrorPayload {
                error_code: "NotAvailable",
                message,
            },
            LogControlCommandError::Internal(error) => ErrorPayload {
                error_code: "Internal",
                message: &format!("{error:#}"),
            },
        };

        format
            .as_string(&payload)
            .unwrap_or_else(|_| format!("{:?}", payload.error_code))
    }
}

impl core::fmt::Display for LogControlCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "{message}"),
            Self::NotAvailable(message) => write!(f, "{message}"),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for LogControlCommandError {}

#[derive(Serialize)]
struct ControlResult<'a> {
    operation: &'a str,
    service: &'a str,
    control_service: String,
    daemon_status: &'static str,
    is_paused: bool,
    dropped_while_paused: u64,
    paused_since_ns: Option<u64>,
    committed_records: u64,
    payload_bytes_committed: u64,
    data_bytes_written: u64,
    metadata_bytes_written: u64,
    last_durable_data_sequence: Option<u64>,
    last_durable_commit_ordinal: Option<u64>,
}

pub(crate) fn log_control(
    action: LogControlAction,
    format: Format,
) -> Result<(), LogControlCommandError> {
    match action {
        LogControlAction::Status(options) => {
            execute(options, "status", LOG_RECORDER_CONTROL_CMD_STATUS, format)
        }
        LogControlAction::Flush(options) => {
            execute(options, "flush", LOG_RECORDER_CONTROL_CMD_FLUSH, format)
        }
        LogControlAction::Stop(options) => {
            execute(options, "stop", LOG_RECORDER_CONTROL_CMD_STOP, format)
        }
        LogControlAction::Pause(options) => {
            execute(options, "pause", LOG_RECORDER_CONTROL_CMD_PAUSE, format)
        }
        LogControlAction::Resume(options) => {
            execute(options, "resume", LOG_RECORDER_CONTROL_CMD_RESUME, format)
        }
    }
}

fn execute(
    options: LogControlOptions,
    operation: &'static str,
    command: u16,
    format: Format,
) -> Result<(), LogControlCommandError> {
    validate_options(&options)?;

    let control_service = log_recorder_control_service_name(&options.service);
    let node = NodeBuilder::new()
        .name(
            &NodeName::new(&options.node_name)
                .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?,
        )
        .create::<ipc::Service>()
        .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?;

    let control_service_name = ServiceName::new(&control_service)
        .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?;

    let request_response = node
        .service_builder(&control_service_name)
        .request_response::<LogRecorderControlRequest, LogRecorderControlResponse>()
        .open()
        .map_err(|_| {
            LogControlCommandError::NotAvailable(format!(
                "recorder daemon control service '{}' is not available",
                control_service
            ))
        })?;

    let client = request_response
        .client_builder()
        .create()
        .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?;

    let response = send_request(
        &node,
        &client,
        LogRecorderControlRequest::new(command),
        Duration::from_millis(options.timeout_ms),
    )?;

    let daemon_status = match response.status {
        LOG_RECORDER_CONTROL_STATUS_OK => "Ok",
        LOG_RECORDER_CONTROL_STATUS_INVALID_REQUEST => {
            return Err(LogControlCommandError::InvalidInput(format!(
                "daemon rejected {operation} command as invalid"
            )));
        }
        LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR => {
            return Err(LogControlCommandError::NotAvailable(format!(
                "daemon failed to execute {operation} command"
            )));
        }
        status => {
            return Err(LogControlCommandError::Internal(anyhow!(
                "daemon returned unknown status code {status}"
            )));
        }
    };

    let is_paused = match response.state {
        LOG_RECORDER_CONTROL_STATE_RUNNING => false,
        LOG_RECORDER_CONTROL_STATE_PAUSED => true,
        state => {
            return Err(LogControlCommandError::Internal(anyhow!(
                "daemon returned unknown state code {state}"
            )));
        }
    };

    let payload = ControlResult {
        operation,
        service: &options.service,
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
    };

    print_output(&payload, format)
}

fn validate_options(options: &LogControlOptions) -> Result<(), LogControlCommandError> {
    if options.service.trim().is_empty() {
        return Err(LogControlCommandError::InvalidInput(
            "--service must not be empty".to_string(),
        ));
    }

    if options.timeout_ms == 0 {
        return Err(LogControlCommandError::InvalidInput(
            "--timeout-ms must be greater than 0".to_string(),
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
) -> Result<LogRecorderControlResponse, LogControlCommandError> {
    let pending_response = client
        .send_copy(request)
        .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?;

    if pending_response.number_of_server_connections() == 0 {
        return Err(LogControlCommandError::NotAvailable(
            "recorder daemon is not connected to control service".to_string(),
        ));
    }

    let deadline = Instant::now() + timeout;
    loop {
        if let Some(response) = pending_response
            .receive()
            .map_err(|error| LogControlCommandError::Internal(anyhow!(error)))?
        {
            if response.protocol_version != LOG_RECORDER_CONTROL_PROTOCOL_VERSION {
                return Err(LogControlCommandError::NotAvailable(format!(
                    "daemon protocol version mismatch: expected {}, got {}",
                    LOG_RECORDER_CONTROL_PROTOCOL_VERSION, response.protocol_version
                )));
            }

            return Ok(*response);
        }

        if Instant::now() >= deadline {
            return Err(LogControlCommandError::NotAvailable(
                "timed out waiting for recorder daemon response".to_string(),
            ));
        }

        if node.wait(Duration::from_millis(2)).is_err() {
            return Err(LogControlCommandError::NotAvailable(
                "control client wait interrupted while awaiting response".to_string(),
            ));
        }
    }
}

fn print_output<T: Serialize>(payload: &T, format: Format) -> Result<(), LogControlCommandError> {
    let output = format
        .as_string(payload)
        .with_context(|| "failed to serialize log-control output")
        .map_err(LogControlCommandError::Internal)?;
    println!("{output}");
    Ok(())
}
