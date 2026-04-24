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

use iceoryx2_bb_testing::assert_that;
use iox2_log_archive_core::log_archive::{
    ArchiveMetadataIndexerBuilder, ArchiveRecorderBuilder, ArchiveReplayerBuilder,
    ArchiveSourcePattern, ChecksumMode, MetadataQueryError, PersistenceMode,
    PublishSubscribeRecordInput, QueryReadinessMode, decode_adapter_user_header,
};

#[derive(Debug, Clone, Copy)]
struct LogRecordInput<'a> {
    sequence: u64,
    event_time_ns: u64,
    user_header: &'a [u8],
    payload: &'a [u8],
}

trait LegacyLogAppendExt {
    fn append_log_record(
        &mut self,
        input: LogRecordInput<'_>,
    ) -> Result<
        iox2_log_archive_core::log_archive::RecordedCommit,
        iox2_log_archive_core::log_archive::ArchiveRecorderError,
    >;
}

impl LegacyLogAppendExt for iox2_log_archive_core::log_archive::ArchiveRecorder {
    fn append_log_record(
        &mut self,
        input: LogRecordInput<'_>,
    ) -> Result<
        iox2_log_archive_core::log_archive::RecordedCommit,
        iox2_log_archive_core::log_archive::ArchiveRecorderError,
    > {
        self.append_publish_subscribe_record(PublishSubscribeRecordInput {
            event_time_ns: input.event_time_ns,
            source_service_id: 1,
            source_publisher_id: 1,
            source_sequence: Some(input.sequence),
            user_header: input.user_header,
            payload: input.payload,
        })
    }
}

fn create_recorder(
    storage_path: &std::path::Path,
    metadata_path: &std::path::Path,
) -> iox2_log_archive_core::log_archive::ArchiveRecorder {
    ArchiveRecorderBuilder::new(storage_path)
        .metadata_log_path(metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap()
}

#[test]
fn metadata_indexer_catch_up_restart_and_reindex_are_monotonic_and_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = create_recorder(&storage_path, &metadata_path);
    for sequence in 1..=3u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 10,
                user_header: &[0x10, 0x11],
                payload: &[sequence as u8; 12],
            })
            .unwrap();
    }
    recorder.flush().unwrap();

    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()
        .unwrap();
    let processed_initial = indexer.catch_up_once().unwrap();
    assert_that!(processed_initial, eq 3);
    let status_initial = indexer.status();
    assert_that!(status_initial.watermark.last_commit_ordinal, eq 3);
    assert_that!(status_initial.watermark.last_indexed_commit_ordinal, eq 3);
    assert_that!(
        status_initial.query_readiness_mode,
        eq QueryReadinessMode::CoreLocatorIndex
    );
    assert_that!(
        storage_path.join("core-locator.idx").exists(),
        eq true
    );

    for sequence in 4..=5u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 10,
                user_header: &[0x20, 0x21],
                payload: &[sequence as u8; 16],
            })
            .unwrap();
    }
    recorder.flush().unwrap();

    let processed_delta = indexer.catch_up_once().unwrap();
    assert_that!(processed_delta, eq 2);
    let status_delta = indexer.status();
    assert_that!(status_delta.watermark.last_commit_ordinal, eq 5);
    assert_that!(status_delta.watermark.last_indexed_commit_ordinal, eq 5);
    drop(indexer);

    let mut reopened = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()
        .unwrap();
    let status_reopened = reopened.status();
    assert_that!(status_reopened.watermark.last_indexed_commit_ordinal, eq 5);
    let processed_none = reopened.catch_up_once().unwrap();
    assert_that!(processed_none, eq 0);

    reopened.reindex().unwrap();
    let status_reindexed = reopened.status();
    assert_that!(status_reindexed.watermark.last_indexed_commit_ordinal, eq 5);
    assert_that!(status_reindexed.watermark.last_commit_ordinal, eq 5);
    let metadata = reopened.query_by_sequence(5).unwrap();
    assert_that!(metadata.sequence, eq 5);
    recorder.finalize().unwrap();
}

#[test]
fn metadata_indexer_queries_beyond_watermark_return_not_indexed_yet() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = create_recorder(&storage_path, &metadata_path);
    let mut commits = Vec::new();
    for sequence in 1..=4u64 {
        commits.push(
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence * 100,
                    user_header: &[0x30],
                    payload: &[sequence as u8; 8],
                })
                .unwrap(),
        );
    }
    recorder.flush().unwrap();

    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()
        .unwrap();
    let processed = indexer.catch_up_with_limit(Some(2)).unwrap();
    assert_that!(processed, eq 2);
    let status = indexer.status();
    assert_that!(status.watermark.last_commit_ordinal, eq 4);
    assert_that!(status.watermark.last_indexed_commit_ordinal, eq 2);

    let beyond = indexer.query_by_sequence(4);
    assert_that!(
        matches!(
            beyond,
            Err(MetadataQueryError::NotIndexedYet {
                requested_sequence: Some(4),
                requested_locator: None,
                query_watermark: 2,
                last_commit_ordinal: 4
            })
        ),
        eq true
    );
    let available = indexer.query_by_sequence(2).unwrap();
    assert_that!(available.sequence, eq 2);

    let by_locator = indexer.query_by_locator(commits[3].locator);
    assert_that!(
        matches!(
            by_locator,
            Err(MetadataQueryError::NotIndexedYet {
                requested_sequence: None,
                requested_locator: Some(_),
                query_watermark: 2,
                last_commit_ordinal: 4
            })
        ),
        eq true
    );
    recorder.finalize().unwrap();
}

#[test]
fn metadata_indexer_supports_offline_and_continuous_commitlog_ingestion() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    {
        let mut recorder = create_recorder(&storage_path, &metadata_path);
        for sequence in 1..=5u64 {
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence,
                    user_header: &[0x44, 0x45],
                    payload: &[sequence as u8; 10],
                })
                .unwrap();
        }
        recorder.finalize().unwrap();
    }

    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()
        .unwrap();
    let processed_offline = indexer.catch_up_once().unwrap();
    assert_that!(processed_offline, eq 5);
    assert_that!(indexer.status().watermark.query_watermark(), eq 5);

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .open_or_recover()
        .unwrap();

    for sequence in 6..=10u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence,
                user_header: &[0x55, 0x56],
                payload: &[sequence as u8; 10],
            })
            .unwrap();
        recorder.flush().unwrap();
        let processed_live = indexer.catch_up_with_limit(Some(1)).unwrap();
        assert_that!(processed_live, eq 1);
    }
    recorder.finalize().unwrap();

    let status = indexer.status();
    assert_that!(status.watermark.last_commit_ordinal, eq 10);
    assert_that!(status.watermark.last_indexed_commit_ordinal, eq 10);
}

#[test]
fn metadata_indexer_query_to_replay_path_is_end_to_end_functional() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = create_recorder(&storage_path, &metadata_path);
    for sequence in 1..=4u64 {
        let payload = vec![sequence as u8; 6 + (sequence as usize)];
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 1_000,
                user_header: &[0xAA, sequence as u8],
                payload: &payload,
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()
        .unwrap();
    let _ = indexer.catch_up_once().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    let replayed = indexer.replay_by_sequence(&replayer, 3).unwrap();
    assert_that!(replayed.sequence, eq 3);
    assert_that!(replayed.payload, eq vec![3u8; 9]);
    let decoded = decode_adapter_user_header(&replayed.user_header).unwrap();
    assert_that!(
        decoded.source_metadata.source_pattern,
        eq ArchiveSourcePattern::PublishSubscribe
    );
    assert_that!(decoded.user_header.to_vec(), eq vec![0xAA, 3u8]);

    let metadata = indexer.query_by_sequence(4).unwrap();
    let by_locator = replayer.read_at_locator(metadata.locator).unwrap();
    assert_that!(by_locator.sequence, eq 4);
}
