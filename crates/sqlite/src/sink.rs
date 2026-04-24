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

use iox2_log_archive_core::log_archive::{
    ArchiveLocator, ArchiveMetadataSink, ArchiveMetadataSinkError, MetadataCommitRecord,
};
use rusqlite::OptionalExtension;

use crate::conversion::i64_to_u32;
use crate::query::{
    decode_indexer_state_row, decode_record_row, insert_records_batch, query_indexer_state,
    query_max_timestamp_ns, query_range_by_sequence, query_record_by_locator,
    query_record_by_sequence, query_window, upsert_indexer_state_row,
};
use crate::schema::{initialize_schema, open_connection};
use crate::types::{DEFAULT_STREAM_ID, SqliteIndexerState, SqliteMetadataSink, SqliteTimeField};

impl SqliteMetadataSink {
    /// Opens or creates a SQLite sink database for [`DEFAULT_STREAM_ID`].
    pub fn open(path: &Path) -> Result<Self, ArchiveMetadataSinkError> {
        Self::open_for_stream(path, DEFAULT_STREAM_ID)
    }

    /// Opens or creates a SQLite sink database for a stream id.
    pub fn open_for_stream(path: &Path, stream_id: &str) -> Result<Self, ArchiveMetadataSinkError> {
        if stream_id.trim().is_empty() {
            return Err(ArchiveMetadataSinkError::new("stream_id must not be empty"));
        }

        let sink = Self {
            db_path: path.to_path_buf(),
            stream_id: stream_id.to_string(),
        };
        sink.initialize_schema()?;
        Ok(sink)
    }

    /// Returns sink stream id.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Returns sink database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Returns number of indexed rows for this sink stream.
    pub fn record_count(&self) -> Result<u64, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        let mut stmt = connection
            .prepare("SELECT COUNT(*) FROM records WHERE stream_id = ?1")
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("prepare count query failed: {err}"))
            })?;
        let count: i64 = stmt
            .query_row([&self.stream_id], |row| row.get(0))
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("run count query failed: {err}"))
            })?;
        if count < 0 {
            return Err(ArchiveMetadataSinkError::new(
                "sqlite count query returned negative value",
            ));
        }
        Ok(count as u64)
    }

    /// Returns one metadata row by sequence for this sink stream.
    pub fn query_by_sequence(
        &self,
        sequence: u64,
    ) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_record_by_sequence(&connection, &self.stream_id, sequence)
    }

    /// Returns one metadata row by locator for this sink stream.
    pub fn query_by_locator(
        &self,
        locator: ArchiveLocator,
    ) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_record_by_locator(&connection, &self.stream_id, locator)
    }

    /// Returns a sequence-ordered record range for this sink stream.
    pub fn query_range_by_sequence(
        &self,
        from: u64,
        count: usize,
    ) -> Result<Vec<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_range_by_sequence(&connection, &self.stream_id, from, count)
    }

    /// Returns a time-window range for this sink stream.
    pub fn query_window(
        &self,
        start_ns: u64,
        end_ns: u64,
        time_field: SqliteTimeField,
        limit: usize,
    ) -> Result<Vec<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_window(
            &connection,
            &self.stream_id,
            start_ns,
            end_ns,
            time_field,
            limit,
        )
    }

    /// Returns the latest record by commit ordinal for this sink stream.
    pub fn latest_record(&self) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        let mut stmt = connection
            .prepare(
                "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence
                 FROM records
                 WHERE stream_id = ?1
                 ORDER BY commit_ordinal DESC
                 LIMIT 1",
            )
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("prepare latest record query failed: {err}"))
            })?;
        let row = stmt
            .query_row([&self.stream_id], decode_record_row)
            .optional()
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("run latest record query failed: {err}"))
            })?;
        Ok(row)
    }

    /// Returns maximum timestamp for this stream by selected time field.
    pub fn max_timestamp_ns(
        &self,
        time_field: SqliteTimeField,
    ) -> Result<Option<u64>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_max_timestamp_ns(&connection, &self.stream_id, time_field)
    }

    /// Upserts indexer state for this sink stream.
    pub fn upsert_indexer_state(
        &self,
        state: &SqliteIndexerState,
    ) -> Result<(), ArchiveMetadataSinkError> {
        if state.stream_id != self.stream_id {
            return Err(ArchiveMetadataSinkError::new(
                "indexer state stream_id does not match sink stream_id",
            ));
        }

        let connection = self.open_connection()?;
        upsert_indexer_state_row(&connection, state)
    }

    /// Loads indexer state for this sink stream.
    pub fn load_indexer_state(
        &self,
    ) -> Result<Option<SqliteIndexerState>, ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        query_indexer_state(&connection, &self.stream_id)
    }

    /// Clears indexed rows and state for this sink stream.
    pub fn clear_stream(&self) -> Result<(), ArchiveMetadataSinkError> {
        let mut connection = self.open_connection()?;
        let tx = connection.transaction().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("begin sqlite clear transaction failed: {err}"))
        })?;
        tx.execute(
            "DELETE FROM records WHERE stream_id = ?1",
            [&self.stream_id],
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("clear stream records failed: {err}"))
        })?;
        tx.execute(
            "DELETE FROM indexer_state WHERE stream_id = ?1",
            [&self.stream_id],
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("clear stream state failed: {err}"))
        })?;
        tx.commit().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("commit sqlite clear transaction failed: {err}"))
        })?;
        Ok(())
    }

    /// Returns all indexer state rows in the database.
    pub fn list_indexer_states(
        path: &Path,
    ) -> Result<Vec<SqliteIndexerState>, ArchiveMetadataSinkError> {
        let connection = open_connection(path)?;
        let mut stmt = connection
            .prepare(
                "SELECT stream_id, log_id, last_commit_ordinal, last_indexed_commit_ordinal, roll_file, byte_offset, updated_at_ns, schema_version
                 FROM indexer_state
                 ORDER BY stream_id",
            )
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!(
                    "prepare list indexer_state query failed: {err}"
                ))
            })?;
        let rows = stmt
            .query_map([], decode_indexer_state_row)
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("run list indexer_state query failed: {err}"))
            })?;

        let mut states = Vec::new();
        for row in rows {
            states.push(row.map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode indexer_state row failed: {err}"))
            })?);
        }
        Ok(states)
    }

    /// Returns all distinct stream ids that currently have indexed rows.
    pub fn list_stream_ids(path: &Path) -> Result<Vec<String>, ArchiveMetadataSinkError> {
        let connection = open_connection(path)?;
        let mut stmt = connection
            .prepare("SELECT DISTINCT stream_id FROM records ORDER BY stream_id")
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!(
                    "prepare list stream ids query failed: {err}"
                ))
            })?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|err| {
                ArchiveMetadataSinkError::new(format!("run list stream ids query failed: {err}"))
            })?;
        let mut stream_ids = Vec::new();
        for row in rows {
            stream_ids.push(row.map_err(|err| {
                ArchiveMetadataSinkError::new(format!("decode stream id row failed: {err}"))
            })?);
        }
        Ok(stream_ids)
    }

    /// Returns sorted unique schema versions observed in the database.
    ///
    /// Versions are collected from `schema_migrations` and `indexer_state`.
    /// When both tables are absent (fresh/empty DB), returns an empty vector.
    pub fn list_schema_versions(path: &Path) -> Result<Vec<u32>, ArchiveMetadataSinkError> {
        let connection = open_connection(path)?;
        let has_schema_migrations = table_exists(&connection, "schema_migrations")?;
        let has_indexer_state = table_exists(&connection, "indexer_state")?;

        if !has_schema_migrations && !has_indexer_state {
            return Ok(Vec::new());
        }

        if !has_schema_migrations {
            return Err(ArchiveMetadataSinkError::new(
                "schema_migrations table is missing",
            ));
        }

        let mut versions = Vec::<u32>::new();
        {
            let mut stmt = connection
                .prepare("SELECT schema_version FROM schema_migrations ORDER BY schema_version")
                .map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "prepare schema_migrations query failed: {err}"
                    ))
                })?;
            let rows = stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "run schema_migrations query failed: {err}"
                    ))
                })?;
            for row in rows {
                let raw = row.map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "decode schema_migrations row failed: {err}"
                    ))
                })?;
                versions.push(i64_to_u32(raw, "schema_migrations.schema_version")?);
            }
        }

        if has_indexer_state {
            let mut stmt = connection
                .prepare("SELECT DISTINCT schema_version FROM indexer_state")
                .map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "prepare indexer_state schema query failed: {err}"
                    ))
                })?;
            let rows = stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "run indexer_state schema query failed: {err}"
                    ))
                })?;
            for row in rows {
                let raw = row.map_err(|err| {
                    ArchiveMetadataSinkError::new(format!(
                        "decode indexer_state schema row failed: {err}"
                    ))
                })?;
                versions.push(i64_to_u32(raw, "indexer_state.schema_version")?);
            }
        }

        versions.sort_unstable();
        versions.dedup();
        Ok(versions)
    }

    /// Creates a sink handle by stream id without writing to records.
    pub fn handle(path: &Path, stream_id: &str) -> Result<Self, ArchiveMetadataSinkError> {
        Self::open_for_stream(path, stream_id)
    }

    fn open_connection(&self) -> Result<rusqlite::Connection, ArchiveMetadataSinkError> {
        open_connection(&self.db_path)
    }

    fn initialize_schema(&self) -> Result<(), ArchiveMetadataSinkError> {
        let connection = self.open_connection()?;
        initialize_schema(&connection)
    }
}

fn table_exists(
    connection: &rusqlite::Connection,
    table_name: &str,
) -> Result<bool, ArchiveMetadataSinkError> {
    let mut stmt = connection
        .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1")
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!(
                "prepare sqlite_master query for '{table_name}' failed: {err}"
            ))
        })?;
    let count: i64 = stmt
        .query_row([table_name], |row| row.get(0))
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!(
                "run sqlite_master query for '{table_name}' failed: {err}"
            ))
        })?;
    Ok(count > 0)
}

impl ArchiveMetadataSink for SqliteMetadataSink {
    fn on_records(
        &mut self,
        records: &[MetadataCommitRecord],
    ) -> Result<(), ArchiveMetadataSinkError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut connection = self.open_connection()?;
        let tx = connection.transaction().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("begin sqlite transaction failed: {err}"))
        })?;

        insert_records_batch(&tx, &self.stream_id, records)?;

        tx.commit().map_err(|err| {
            ArchiveMetadataSinkError::new(format!("commit sqlite transaction failed: {err}"))
        })?;
        Ok(())
    }
}
