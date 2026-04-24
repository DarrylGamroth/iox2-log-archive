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

use std::time::Duration;

use anyhow::{Context, anyhow};
use iox2_log_archive_cli::{
    Format, LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP,
};
use iox2_log_archive_iceoryx2::{
    LogRecorderControlClientConfig, LogRecorderControlError, LogRecorderDaemonStatus,
    request_recorder_control,
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

    let result = request_recorder_control(
        LogRecorderControlClientConfig {
            service: options.service.clone(),
            node_name: options.node_name.clone(),
            timeout: Duration::from_millis(options.timeout_ms),
        },
        command,
    )
    .map_err(|error| map_control_error(error, operation))?;

    let payload = ControlResult {
        operation,
        service: &options.service,
        control_service: result.control_service,
        daemon_status: daemon_status_label(result.daemon_status),
        is_paused: result.is_paused,
        dropped_while_paused: result.dropped_while_paused,
        paused_since_ns: result.paused_since_ns,
        committed_records: result.committed_records,
        payload_bytes_committed: result.payload_bytes_committed,
        data_bytes_written: result.data_bytes_written,
        metadata_bytes_written: result.metadata_bytes_written,
        last_durable_data_sequence: result.last_durable_data_sequence,
        last_durable_commit_ordinal: result.last_durable_commit_ordinal,
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

fn map_control_error(
    error: LogRecorderControlError,
    operation: &'static str,
) -> LogControlCommandError {
    match error {
        LogRecorderControlError::InvalidInput(message) => {
            LogControlCommandError::InvalidInput(if message.contains("daemon rejected") {
                format!("daemon rejected {operation} command as invalid")
            } else {
                message
            })
        }
        LogRecorderControlError::NotAvailable(message) => {
            LogControlCommandError::NotAvailable(if message.contains("daemon failed") {
                format!("daemon failed to execute {operation} command")
            } else {
                message
            })
        }
        LogRecorderControlError::Iceoryx2(message) => {
            LogControlCommandError::Internal(anyhow!(message))
        }
    }
}

fn daemon_status_label(value: LogRecorderDaemonStatus) -> &'static str {
    match value {
        LogRecorderDaemonStatus::Ok => "Ok",
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
