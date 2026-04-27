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
    ArchiveLocator, ArchiveMetadataIndexerBuilder, ArchiveMetadataSink, ArchiveRecorderBuilder,
    ArchiveSourcePattern, ChecksumMode, MetadataCommitRecord, PersistenceMode,
    PublishSubscribeRecordInput,
};
use iox2_log_archive_sqlite::{
    SQLITE_SCHEMA_VERSION, SqliteIndexerState, SqliteMetadataSink, SqliteTimeField,
    SqliteWriterLock,
};

fn metadata_record(sequence: u64, commit_ordinal: u64) -> MetadataCommitRecord {
    MetadataCommitRecord {
        log_id: [0xAB; 16],
        commit_ordinal,
        sequence,
        locator: ArchiveLocator {
            segment_id: 1,
            segment_generation: 1,
            file_offset: sequence * 64,
            frame_len: 64,
        },
        frame_checksum: 0x1234,
        event_time_ns: sequence * 100,
        commit_time_ns: sequence * 100 + 10,
        source_pattern: ArchiveSourcePattern::Log,
        source_service_id: 11,
        source_instance_id: 22,
        source_sequence: Some(sequence),
    }
}

#[test]
fn sqlite_metadata_sink_materializes_commitlog_records() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("metadata.sqlite");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    for sequence in 1..=4u64 {
        recorder
            .append_publish_subscribe_record(PublishSubscribeRecordInput {
                event_time_ns: sequence * 100,
                source_service_id: 11,
                source_publisher_id: 22,
                source_sequence: Some(sequence),
                user_header: &[0x1, 0x2],
                payload: &[sequence as u8; 8],
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let sink = SqliteMetadataSink::open(&db_path).unwrap();
    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .sink(Box::new(sink))
        .open()
        .unwrap();
    let processed = indexer.catch_up_once().unwrap();
    assert_eq!(processed, 4);

    let verifier = SqliteMetadataSink::open(&db_path).unwrap();
    assert_eq!(verifier.record_count().unwrap(), 4);
    let record = verifier.query_by_sequence(3).unwrap().unwrap();
    assert_eq!(record.sequence, 3);
    assert_eq!(record.commit_ordinal, 3);
}

#[test]
fn sqlite_writer_lock_is_exclusive_and_releases_on_drop() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");

    let lock = SqliteWriterLock::acquire(&db_path).unwrap();
    assert!(lock.lock_path().exists());

    let second = SqliteWriterLock::acquire(&db_path).unwrap_err();
    assert!(second.details.contains("writer lock is already held"));

    drop(lock);
    let lock_after_drop = SqliteWriterLock::acquire(&db_path).unwrap();
    assert!(lock_after_drop.lock_path().exists());
}

#[test]
fn sqlite_stream_isolation_and_state_roundtrip_work() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");

    let mut sink_a = SqliteMetadataSink::open_for_stream(&db_path, "Cam/A").unwrap();
    let mut sink_b = SqliteMetadataSink::open_for_stream(&db_path, "Cam/B").unwrap();

    sink_a.on_records(&[metadata_record(10, 1)]).unwrap();
    sink_b.on_records(&[metadata_record(20, 1)]).unwrap();

    let state_a = SqliteIndexerState {
        stream_id: "Cam/A".to_string(),
        log_id: [0x1; 16],
        last_commit_ordinal: 8,
        last_indexed_commit_ordinal: 7,
        roll_file: "commit.idxlog".to_string(),
        byte_offset: 1024,
        updated_at_ns: 11,
        schema_version: 1,
    };
    sink_a.upsert_indexer_state(&state_a).unwrap();

    let state_b = SqliteIndexerState {
        stream_id: "Cam/B".to_string(),
        log_id: [0x2; 16],
        last_commit_ordinal: 3,
        last_indexed_commit_ordinal: 3,
        roll_file: "commit.idxlog".to_string(),
        byte_offset: 512,
        updated_at_ns: 12,
        schema_version: 1,
    };
    sink_b.upsert_indexer_state(&state_b).unwrap();

    let all_streams = SqliteMetadataSink::list_stream_ids(&db_path).unwrap();
    assert_eq!(all_streams, vec!["Cam/A".to_string(), "Cam/B".to_string()]);

    assert!(sink_a.query_by_sequence(10).unwrap().is_some());
    assert!(sink_a.query_by_sequence(20).unwrap().is_none());
    assert!(sink_b.query_by_sequence(20).unwrap().is_some());

    let loaded_a = sink_a.load_indexer_state().unwrap().unwrap();
    assert_eq!(loaded_a, state_a);
    let loaded_b = sink_b.load_indexer_state().unwrap().unwrap();
    assert_eq!(loaded_b, state_b);

    let states = SqliteMetadataSink::list_indexer_states(&db_path).unwrap();
    assert_eq!(states.len(), 2);

    let window_a = sink_a
        .query_window(0, i64::MAX as u64, SqliteTimeField::Event, 10)
        .unwrap();
    assert_eq!(window_a.len(), 1);
    assert_eq!(window_a[0].sequence, 10);
}

#[test]
fn sqlite_queries_cover_locators_windows_limits_and_empty_results() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");

    let mut sink_a = SqliteMetadataSink::open_for_stream(&db_path, "Cam/A").unwrap();
    let mut sink_b = SqliteMetadataSink::open_for_stream(&db_path, "Cam/B").unwrap();

    let records_a = (1..=5)
        .map(|sequence| metadata_record(sequence, sequence))
        .collect::<Vec<_>>();
    let records_b = vec![metadata_record(1, 10), metadata_record(2, 11)];
    sink_a.on_records(&records_a).unwrap();
    sink_b.on_records(&records_b).unwrap();

    let locator = records_a[2].locator;
    let by_locator_a = sink_a.query_by_locator(locator).unwrap().unwrap();
    assert_eq!(by_locator_a.sequence, 3);
    assert_eq!(by_locator_a.commit_ordinal, 3);
    let by_locator_b = sink_b.query_by_locator(locator).unwrap();
    assert!(by_locator_b.is_none());

    let range = sink_a.query_range_by_sequence(2, 3).unwrap();
    assert_eq!(
        range
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );

    let event_window = sink_a
        .query_window(200, 500, SqliteTimeField::Event, 2)
        .unwrap();
    assert_eq!(
        event_window
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    let commit_window = sink_a
        .query_window(210, 410, SqliteTimeField::Commit, 10)
        .unwrap();
    assert_eq!(
        commit_window
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );

    let empty = sink_a
        .query_window(9_000, 10_000, SqliteTimeField::Event, 10)
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        sink_a.max_timestamp_ns(SqliteTimeField::Event).unwrap(),
        Some(500)
    );
    assert_eq!(
        sink_a.max_timestamp_ns(SqliteTimeField::Commit).unwrap(),
        Some(510)
    );
    assert_eq!(sink_a.latest_record().unwrap().unwrap().sequence, 5);
    assert_eq!(sink_b.record_count().unwrap(), 2);
}

#[test]
fn sqlite_queries_reject_values_outside_sqlite_integer_range() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");
    let sink = SqliteMetadataSink::open_for_stream(&db_path, "Cam/A").unwrap();

    let too_large = (i64::MAX as u64) + 1;
    let sequence_error = sink.query_by_sequence(too_large).unwrap_err();
    assert!(sequence_error.details.contains("sequence"));

    let window_error = sink
        .query_window(too_large, too_large, SqliteTimeField::Event, 10)
        .unwrap_err();
    assert!(window_error.details.contains("start_ns"));
}

#[test]
fn sqlite_sink_rejects_malformed_log_id_blobs() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");

    let sink = SqliteMetadataSink::open(&db_path).unwrap();
    let connection = rusqlite::Connection::open(&db_path).unwrap();

    connection
        .execute(
            "INSERT INTO records
            (stream_id, commit_ordinal, log_id, sequence, segment_id, segment_generation, file_offset, frame_len, frame_checksum, event_time_ns, commit_time_ns, source_pattern, source_service_id, source_instance_id, source_sequence)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            rusqlite::params![
                "__default__",
                1i64,
                vec![0xAAu8; 8],
                1i64,
                1i64,
                1i64,
                0i64,
                64i64,
                0i64,
                0i64,
                0i64,
                1i64,
                0i64,
                0i64,
                Option::<i64>::None,
            ],
        )
        .unwrap();

    let error = sink.query_by_sequence(1).unwrap_err();
    assert!(error.details.contains("invalid records.log_id length"));
}

#[test]
fn sqlite_schema_versions_include_migration_and_stream_state_versions() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("query.sqlite");

    let sink = SqliteMetadataSink::open_for_stream(&db_path, "Cam/A").unwrap();
    let versions = SqliteMetadataSink::list_schema_versions(&db_path).unwrap();
    assert_eq!(versions, vec![SQLITE_SCHEMA_VERSION]);

    let state = SqliteIndexerState {
        stream_id: "Cam/A".to_string(),
        log_id: [0x1; 16],
        last_commit_ordinal: 2,
        last_indexed_commit_ordinal: 1,
        roll_file: "commit.idxlog".to_string(),
        byte_offset: 128,
        updated_at_ns: 7,
        schema_version: SQLITE_SCHEMA_VERSION + 1,
    };
    sink.upsert_indexer_state(&state).unwrap();

    let versions = SqliteMetadataSink::list_schema_versions(&db_path).unwrap();
    assert_eq!(
        versions,
        vec![SQLITE_SCHEMA_VERSION, SQLITE_SCHEMA_VERSION + 1]
    );
}
