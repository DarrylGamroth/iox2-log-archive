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

use std::path::Path;

use iox2_log_archive_core::log_archive::ArchiveMetadataSinkError;

use crate::conversion::{now_ns, u64_to_i64};
use crate::types::SQLITE_SCHEMA_VERSION;

pub(crate) fn open_connection(
    path: &Path,
) -> Result<rusqlite::Connection, ArchiveMetadataSinkError> {
    let connection = rusqlite::Connection::open(path).map_err(|err| {
        ArchiveMetadataSinkError::new(format!("open sqlite connection failed: {err}"))
    })?;
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("configure sqlite pragmas failed: {err}"))
        })?;
    Ok(connection)
}

pub(crate) fn initialize_schema(
    connection: &rusqlite::Connection,
) -> Result<(), ArchiveMetadataSinkError> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS records (
                stream_id TEXT NOT NULL,
                commit_ordinal INTEGER NOT NULL,
                log_id BLOB NOT NULL,
                sequence INTEGER NOT NULL,
                segment_id INTEGER NOT NULL,
                segment_generation INTEGER NOT NULL,
                file_offset INTEGER NOT NULL,
                frame_len INTEGER NOT NULL,
                frame_checksum INTEGER NOT NULL,
                event_time_ns INTEGER NOT NULL DEFAULT 0,
                commit_time_ns INTEGER NOT NULL DEFAULT 0,
                source_pattern INTEGER NOT NULL DEFAULT 1,
                source_service_id INTEGER NOT NULL DEFAULT 0,
                source_instance_id INTEGER NOT NULL DEFAULT 0,
                source_sequence INTEGER,
                PRIMARY KEY (stream_id, commit_ordinal)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_records_sequence
                ON records(stream_id, sequence);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_records_locator
                ON records(stream_id, segment_id, segment_generation, file_offset, frame_len);
            CREATE INDEX IF NOT EXISTS idx_records_event_time
                ON records(stream_id, event_time_ns);
            CREATE INDEX IF NOT EXISTS idx_records_commit_time
                ON records(stream_id, commit_time_ns);
            CREATE TABLE IF NOT EXISTS indexer_state (
                stream_id TEXT PRIMARY KEY,
                log_id BLOB NOT NULL,
                last_commit_ordinal INTEGER NOT NULL,
                last_indexed_commit_ordinal INTEGER NOT NULL,
                roll_file TEXT NOT NULL,
                byte_offset INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                schema_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS schema_migrations (
                schema_version INTEGER PRIMARY KEY,
                applied_at_ns INTEGER NOT NULL,
                tool_version TEXT NOT NULL
            );",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("initialize sqlite schema failed: {err}"))
        })?;

    let now = now_ns();
    connection
        .execute(
            "INSERT OR IGNORE INTO schema_migrations(schema_version, applied_at_ns, tool_version)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![
                SQLITE_SCHEMA_VERSION as i64,
                u64_to_i64(now, "applied_at_ns")?,
                env!("CARGO_PKG_VERSION")
            ],
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("initialize schema migration row failed: {err}"))
        })?;

    Ok(())
}
