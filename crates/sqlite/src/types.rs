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

/// Default stream id used by [`crate::SqliteMetadataSink::open`].
pub const DEFAULT_STREAM_ID: &str = "__default__";

/// SQLite schema version for query/index contracts.
pub const SQLITE_SCHEMA_VERSION: u32 = 1;

/// Time field used by window queries.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SqliteTimeField {
    /// Query against `event_time_ns`.
    Event,
    /// Query against `commit_time_ns`.
    Commit,
}

/// Persisted indexer state row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SqliteIndexerState {
    /// Stable stream identity.
    pub stream_id: String,
    /// Stable log identity.
    pub log_id: [u8; 16],
    /// Recorder durable metadata boundary.
    pub last_commit_ordinal: u64,
    /// Queryable metadata boundary.
    pub last_indexed_commit_ordinal: u64,
    /// Active roll-file name for checkpointing.
    pub roll_file: String,
    /// Byte offset checkpoint in roll-file.
    pub byte_offset: u64,
    /// Last update timestamp in nanoseconds.
    pub updated_at_ns: u64,
    /// Schema version of row.
    pub schema_version: u32,
}

/// SQLite-backed implementation of [`iox2_log_archive_core::log_archive::ArchiveMetadataSink`].
#[derive(Debug, Clone)]
pub struct SqliteMetadataSink {
    pub(crate) db_path: PathBuf,
    pub(crate) stream_id: String,
}
