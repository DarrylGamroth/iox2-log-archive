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

mod pubsub;

pub use pubsub::*;

use iox2_log_archive_core::log_archive::ArchiveReplayError;

/// Default node name used by pub-sub rematerialization helpers.
pub const DEFAULT_PUBSUB_REMATERIALIZER_NODE_NAME: &str = "iox2-log-archive-rematerializer";

/// Error returned by archive rematerialization helpers.
#[derive(Debug)]
pub enum ArchiveRematerializeError {
    /// Invalid configuration value.
    InvalidConfiguration(&'static str),
    /// Replayer failed to retrieve archived records.
    Replay(ArchiveReplayError),
    /// Node name was invalid.
    InvalidNodeName(String),
    /// Service name was invalid.
    InvalidServiceName(String),
    /// Type-name validation failed.
    InvalidTypeName(String),
    /// Unable to create rematerializer node.
    NodeCreation(String),
    /// Unable to open/create publish-subscribe service.
    ServiceCreation(String),
    /// Unable to create publisher endpoint.
    PublisherCreation(String),
    /// Unable to loan payload memory from publisher.
    Loan(String),
    /// Unable to send rematerialized sample.
    Send(String),
    /// Archived frame user-header length does not match rematerializer contract.
    IncompatibleUserHeaderSize {
        /// Expected user-header length configured for output service.
        expected: usize,
        /// Actual user-header length decoded from archived frame.
        actual: usize,
        /// Archived sequence used for diagnostics.
        sequence: u64,
    },
    /// Unexpected payload length returned by publisher loan.
    UnexpectedLoanedPayloadSize {
        /// Requested payload length.
        expected: usize,
        /// Actual payload length in loaned sample.
        actual: usize,
        /// Archived sequence used for diagnostics.
        sequence: u64,
    },
}

impl core::fmt::Display for ArchiveRematerializeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveRematerializeError::{self:?}")
    }
}

impl std::error::Error for ArchiveRematerializeError {}

impl From<ArchiveReplayError> for ArchiveRematerializeError {
    fn from(value: ArchiveReplayError) -> Self {
        Self::Replay(value)
    }
}
