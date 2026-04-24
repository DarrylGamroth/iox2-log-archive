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

use iox2_log_archive_core::log_archive::{
    ArchiveLocator, ArchiveMetadataSinkError, MetadataCommitRecord,
};
use rusqlite::OptionalExtension;

use crate::conversion::{
    i64_to_source_pattern, i64_to_u32, i64_to_u64, option_u64_to_option_i64, source_pattern_to_i64,
    u32_to_i64, u64_to_i64, usize_to_i64,
};
use crate::types::{SqliteIndexerState, SqliteTimeField};

pub(crate) fn query_record_by_sequence(
    connection: &rusqlite::Connection,
    stream_id: &str,
    sequence: u64,
) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
    let sequence_i64 = u64_to_i64(sequence, "sequence")?;
    let mut stmt = connection
        .prepare(
            "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence
             FROM records
             WHERE stream_id = ?1 AND sequence = ?2
             LIMIT 1",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("prepare sequence query failed: {err}"))
        })?;
    let row = stmt
        .query_row(
            rusqlite::params![stream_id, sequence_i64],
            decode_record_row,
        )
        .optional()
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("run sequence query failed: {err}"))
        })?;
    Ok(row)
}

pub(crate) fn query_record_by_locator(
    connection: &rusqlite::Connection,
    stream_id: &str,
    locator: ArchiveLocator,
) -> Result<Option<MetadataCommitRecord>, ArchiveMetadataSinkError> {
    let mut stmt = connection
        .prepare(
            "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence
             FROM records
             WHERE stream_id = ?1
               AND segment_id = ?2
               AND segment_generation = ?3
               AND file_offset = ?4
               AND frame_len = ?5
             LIMIT 1",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("prepare locator query failed: {err}"))
        })?;
    let row = stmt
        .query_row(
            rusqlite::params![
                stream_id,
                u64_to_i64(locator.segment_id, "segment_id")?,
                u32_to_i64(locator.segment_generation),
                u64_to_i64(locator.file_offset, "file_offset")?,
                u32_to_i64(locator.frame_len),
            ],
            decode_record_row,
        )
        .optional()
        .map_err(|err| ArchiveMetadataSinkError::new(format!("run locator query failed: {err}")))?;
    Ok(row)
}

pub(crate) fn query_range_by_sequence(
    connection: &rusqlite::Connection,
    stream_id: &str,
    from: u64,
    count: usize,
) -> Result<Vec<MetadataCommitRecord>, ArchiveMetadataSinkError> {
    let mut stmt = connection
        .prepare(
            "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence
             FROM records
             WHERE stream_id = ?1 AND sequence >= ?2
             ORDER BY sequence ASC, commit_ordinal ASC
             LIMIT ?3",
        )
        .map_err(|err| ArchiveMetadataSinkError::new(format!("prepare range query failed: {err}")))?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                stream_id,
                u64_to_i64(from, "from")?,
                usize_to_i64(count, "count")?
            ],
            decode_record_row,
        )
        .map_err(|err| ArchiveMetadataSinkError::new(format!("run range query failed: {err}")))?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(|err| {
            ArchiveMetadataSinkError::new(format!("decode range row failed: {err}"))
        })?);
    }
    Ok(result)
}

pub(crate) fn query_window(
    connection: &rusqlite::Connection,
    stream_id: &str,
    start_ns: u64,
    end_ns: u64,
    time_field: SqliteTimeField,
    limit: usize,
) -> Result<Vec<MetadataCommitRecord>, ArchiveMetadataSinkError> {
    let (column, order_column) = match time_field {
        SqliteTimeField::Event => ("event_time_ns", "event_time_ns"),
        SqliteTimeField::Commit => ("commit_time_ns", "commit_time_ns"),
    };
    let sql = format!(
        "SELECT log_id, commit_ordinal, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence
         FROM records
         WHERE stream_id = ?1 AND {column} >= ?2 AND {column} <= ?3
         ORDER BY {order_column} ASC, commit_ordinal ASC
         LIMIT ?4"
    );
    let mut stmt = connection.prepare(&sql).map_err(|err| {
        ArchiveMetadataSinkError::new(format!("prepare window query failed: {err}"))
    })?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                stream_id,
                u64_to_i64(start_ns, "start_ns")?,
                u64_to_i64(end_ns, "end_ns")?,
                usize_to_i64(limit, "limit")?
            ],
            decode_record_row,
        )
        .map_err(|err| ArchiveMetadataSinkError::new(format!("run window query failed: {err}")))?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(|err| {
            ArchiveMetadataSinkError::new(format!("decode window row failed: {err}"))
        })?);
    }
    Ok(result)
}

pub(crate) fn upsert_indexer_state_row(
    connection: &rusqlite::Connection,
    state: &SqliteIndexerState,
) -> Result<(), ArchiveMetadataSinkError> {
    connection
        .execute(
            "INSERT OR REPLACE INTO indexer_state
             (stream_id, log_id, last_commit_ordinal, last_indexed_commit_ordinal, roll_file, byte_offset, updated_at_ns, schema_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                state.stream_id,
                state.log_id.as_slice(),
                u64_to_i64(state.last_commit_ordinal, "last_commit_ordinal")?,
                u64_to_i64(state.last_indexed_commit_ordinal, "last_indexed_commit_ordinal")?,
                state.roll_file,
                u64_to_i64(state.byte_offset, "byte_offset")?,
                u64_to_i64(state.updated_at_ns, "updated_at_ns")?,
                u32_to_i64(state.schema_version),
            ],
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("upsert indexer_state row failed: {err}"))
        })?;
    Ok(())
}

pub(crate) fn query_indexer_state(
    connection: &rusqlite::Connection,
    stream_id: &str,
) -> Result<Option<SqliteIndexerState>, ArchiveMetadataSinkError> {
    let mut stmt = connection
        .prepare(
            "SELECT stream_id, log_id, last_commit_ordinal, last_indexed_commit_ordinal, roll_file, byte_offset, updated_at_ns, schema_version
             FROM indexer_state
             WHERE stream_id = ?1
             LIMIT 1",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("prepare indexer_state query failed: {err}"))
        })?;
    let row = stmt
        .query_row([stream_id], decode_indexer_state_row)
        .optional()
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("run indexer_state query failed: {err}"))
        })?;
    Ok(row)
}

pub(crate) fn decode_record_row(
    row: &rusqlite::Row<'_>,
) -> Result<MetadataCommitRecord, rusqlite::Error> {
    let log_id_blob: Vec<u8> = row.get(0)?;
    let log_id = decode_log_id_blob(log_id_blob, 0, "records.log_id")?;

    let source_pattern_raw: i64 = row.get(10)?;
    let source_pattern =
        i64_to_source_pattern(source_pattern_raw, "source_pattern").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?;

    Ok(MetadataCommitRecord {
        log_id,
        commit_ordinal: i64_to_u64(row.get::<_, i64>(1)?, "commit_ordinal").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        sequence: i64_to_u64(row.get::<_, i64>(2)?, "sequence").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        locator: ArchiveLocator {
            segment_id: i64_to_u64(row.get::<_, i64>(3)?, "segment_id").map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            })?,
            segment_generation: i64_to_u32(row.get::<_, i64>(4)?, "segment_generation").map_err(
                |err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Integer,
                        Box::new(err),
                    )
                },
            )?,
            file_offset: i64_to_u64(row.get::<_, i64>(5)?, "file_offset").map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            })?,
            frame_len: i64_to_u32(row.get::<_, i64>(6)?, "frame_len").map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            })?,
        },
        frame_checksum: i64_to_u32(row.get::<_, i64>(7)?, "frame_checksum").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        event_time_ns: i64_to_u64(row.get::<_, i64>(8)?, "event_time_ns").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        commit_time_ns: i64_to_u64(row.get::<_, i64>(9)?, "commit_time_ns").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                9,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        source_pattern,
        source_service_id: i64_to_u64(row.get::<_, i64>(11)?, "source_service_id").map_err(
            |err| {
                rusqlite::Error::FromSqlConversionFailure(
                    11,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            },
        )?,
        source_instance_id: i64_to_u64(row.get::<_, i64>(12)?, "source_instance_id").map_err(
            |err| {
                rusqlite::Error::FromSqlConversionFailure(
                    12,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            },
        )?,
        source_sequence: row
            .get::<_, Option<i64>>(13)?
            .map(|value| i64_to_u64(value, "source_sequence"))
            .transpose()
            .map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    13,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            })?,
    })
}

pub(crate) fn decode_indexer_state_row(
    row: &rusqlite::Row<'_>,
) -> Result<SqliteIndexerState, rusqlite::Error> {
    let log_id_blob: Vec<u8> = row.get(1)?;
    let log_id = decode_log_id_blob(log_id_blob, 1, "indexer_state.log_id")?;

    Ok(SqliteIndexerState {
        stream_id: row.get(0)?,
        log_id,
        last_commit_ordinal: i64_to_u64(row.get::<_, i64>(2)?, "last_commit_ordinal").map_err(
            |err| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::new(err),
                )
            },
        )?,
        last_indexed_commit_ordinal: i64_to_u64(
            row.get::<_, i64>(3)?,
            "last_indexed_commit_ordinal",
        )
        .map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        roll_file: row.get(4)?,
        byte_offset: i64_to_u64(row.get::<_, i64>(5)?, "byte_offset").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        updated_at_ns: i64_to_u64(row.get::<_, i64>(6)?, "updated_at_ns").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
        schema_version: i64_to_u32(row.get::<_, i64>(7)?, "schema_version").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Integer,
                Box::new(err),
            )
        })?,
    })
}

pub(crate) fn decode_log_id_blob(
    blob: Vec<u8>,
    column: usize,
    field: &'static str,
) -> Result<[u8; 16], rusqlite::Error> {
    if blob.len() != 16 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            Box::new(ArchiveMetadataSinkError::new(format!(
                "invalid {field} length {}; expected 16 bytes",
                blob.len()
            ))),
        ));
    }

    let mut log_id = [0u8; 16];
    log_id.copy_from_slice(&blob);
    Ok(log_id)
}

pub(crate) fn insert_records_batch(
    tx: &rusqlite::Transaction<'_>,
    stream_id: &str,
    records: &[MetadataCommitRecord],
) -> Result<(), ArchiveMetadataSinkError> {
    let mut stmt = tx
        .prepare(
            "INSERT OR REPLACE INTO records
            (stream_id, commit_ordinal, log_id, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        )
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("prepare sqlite insert statement failed: {err}"))
        })?;

    for record in records {
        stmt.execute(rusqlite::params![
            stream_id,
            u64_to_i64(record.commit_ordinal, "commit_ordinal")?,
            record.log_id.as_slice(),
            u64_to_i64(record.sequence, "sequence")?,
            u64_to_i64(record.locator.segment_id, "segment_id")?,
            u32_to_i64(record.locator.segment_generation),
            u64_to_i64(record.locator.file_offset, "file_offset")?,
            u32_to_i64(record.locator.frame_len),
            u32_to_i64(record.frame_checksum),
            u64_to_i64(record.event_time_ns, "event_time_ns")?,
            u64_to_i64(record.commit_time_ns, "commit_time_ns")?,
            source_pattern_to_i64(record.source_pattern),
            u64_to_i64(record.source_service_id, "source_service_id")?,
            u64_to_i64(record.source_instance_id, "source_instance_id")?,
            option_u64_to_option_i64(record.source_sequence, "source_sequence")?,
        ])
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("insert sqlite record failed: {err}"))
        })?;
    }

    Ok(())
}

pub(crate) fn query_max_timestamp_ns(
    connection: &rusqlite::Connection,
    stream_id: &str,
    time_field: SqliteTimeField,
) -> Result<Option<u64>, ArchiveMetadataSinkError> {
    let column = match time_field {
        SqliteTimeField::Event => "event_time_ns",
        SqliteTimeField::Commit => "commit_time_ns",
    };
    let sql = format!("SELECT MAX({column}) FROM records WHERE stream_id = ?1");
    let mut stmt = connection.prepare(&sql).map_err(|err| {
        ArchiveMetadataSinkError::new(format!("prepare max timestamp query failed: {err}"))
    })?;
    let value = stmt
        .query_row([stream_id], |row| row.get::<_, Option<i64>>(0))
        .map_err(|err| {
            ArchiveMetadataSinkError::new(format!("run max timestamp query failed: {err}"))
        })?;
    value
        .map(|raw| i64_to_u64(raw, "max_timestamp_ns"))
        .transpose()
}
