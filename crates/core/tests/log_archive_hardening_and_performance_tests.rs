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

use core::num::NonZeroUsize;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use iceoryx2_bb_testing::assert_that;
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, ArchiveRecorderError, ArchiveReplayerBuilder, AsyncIoBackend,
    ChecksumMode, EffectiveAsyncIoBackend, PersistenceMode, PublishSubscribeRecordInput,
    RecorderProfile, ReplayBudget,
};

#[derive(Debug, Clone, Copy)]
struct LogRecordInput<'a> {
    sequence: u64,
    event_time_ns: u64,
    user_header: &'a [u8],
    payload: &'a [u8],
}

type ReplayedRecord = (u64, Vec<u8>, Vec<u8>);
type RecordAndReplayResult = (EffectiveAsyncIoBackend, Vec<ReplayedRecord>);

trait LegacyLogAppendExt {
    fn append_log_record(
        &mut self,
        input: LogRecordInput<'_>,
    ) -> Result<iox2_log_archive_core::log_archive::RecordedCommit, ArchiveRecorderError>;
}

impl LegacyLogAppendExt for iox2_log_archive_core::log_archive::ArchiveRecorder {
    fn append_log_record(
        &mut self,
        input: LogRecordInput<'_>,
    ) -> Result<iox2_log_archive_core::log_archive::RecordedCommit, ArchiveRecorderError> {
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

fn payload_byte(sequence: u64, index: usize) -> u8 {
    let seed = (sequence as u8).wrapping_mul(31).wrapping_add(17);
    seed.wrapping_add(index as u8)
}

fn fill_payload(sequence: u64, payload: &mut [u8]) {
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = payload_byte(sequence, index);
    }
}

fn record_and_replay(
    storage_path: &Path,
    metadata_path: &Path,
    backend: AsyncIoBackend,
    records: u64,
    payload_len: usize,
) -> RecordAndReplayResult {
    let mut recorder = ArchiveRecorderBuilder::new(storage_path)
        .metadata_log_path(metadata_path)
        .profile(RecorderProfile::Throughput)
        .segment_bytes(256 * 1024)
        .segment_preallocate(true)
        .spare_preallocated_segments(2)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .async_io_backend(backend)
        .io_uring_queue_depth(256)
        .io_submit_batch_max(64)
        .io_cqe_batch_max(128)
        .create()
        .unwrap();
    let effective_backend = recorder.effective_async_io_backend();

    let mut payload = vec![0u8; payload_len];
    for sequence in 1..=records {
        fill_payload(sequence, &mut payload);
        let user_header = [sequence as u8, (sequence >> 8) as u8, 0xAA, 0x55];
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 100,
                user_header: &user_header,
                payload: &payload,
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(storage_path)
        .metadata_log_path(metadata_path)
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(records as usize).unwrap())
        .unwrap()
        .into_iter()
        .map(|frame| (frame.sequence, frame.user_header, frame.payload))
        .collect();

    (effective_backend, replayed)
}

#[test]
fn log_archive_phase6_io_uring_required_selection_is_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let result = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .async_io_backend(AsyncIoBackend::IoUringRequired)
        .io_uring_queue_depth(8)
        .io_submit_batch_max(8)
        .io_cqe_batch_max(16)
        .create();

    #[cfg(target_os = "linux")]
    {
        let io_uring_available = io_uring::IoUring::new(8).is_ok();
        match (io_uring_available, result) {
            (true, Ok(recorder)) => {
                assert_that!(
                    recorder.effective_async_io_backend(),
                    eq EffectiveAsyncIoBackend::IoUring
                );
            }
            (false, Err(ArchiveRecorderError::InvalidConfiguration(message))) => {
                assert_that!(message, eq "io_uring backend required but unavailable");
            }
            (true, Err(error)) => {
                panic!("expected io_uring required backend creation to succeed, got {error:?}");
            }
            (false, Ok(_)) => {
                panic!("expected io_uring required backend creation to fail when unavailable");
            }
            (_, Err(error)) => {
                panic!("unexpected error for io_uring required backend: {error:?}");
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        assert_that!(
            matches!(
                result,
                Err(ArchiveRecorderError::InvalidConfiguration(message))
                if message == "io_uring backend required but unavailable"
            ),
            eq true
        );
    }
}

#[test]
fn log_archive_phase6_backend_parity_between_blocking_and_io_uring() {
    #[cfg(not(target_os = "linux"))]
    {
        return;
    }

    #[cfg(target_os = "linux")]
    {
        if io_uring::IoUring::new(8).is_err() {
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let records = 768u64;
        let payload_len = 1536usize;

        let (blocking_backend, blocking_replay) = record_and_replay(
            &temp.path().join("archive_blocking"),
            &temp.path().join("metadata_blocking"),
            AsyncIoBackend::Blocking,
            records,
            payload_len,
        );
        let (io_uring_backend, io_uring_replay) = record_and_replay(
            &temp.path().join("archive_io_uring"),
            &temp.path().join("metadata_io_uring"),
            AsyncIoBackend::IoUringRequired,
            records,
            payload_len,
        );

        assert_that!(blocking_backend, eq EffectiveAsyncIoBackend::Blocking);
        assert_that!(io_uring_backend, eq EffectiveAsyncIoBackend::IoUring);
        assert_that!(io_uring_replay, eq blocking_replay);
    }
}

#[test]
fn log_archive_phase6_sustained_ingest_soak_preserves_integrity() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let record_count = 4096u64;
    let payload_len = 2048usize;

    let backend = {
        #[cfg(target_os = "linux")]
        {
            if io_uring::IoUring::new(32).is_ok() {
                AsyncIoBackend::IoUringRequired
            } else {
                AsyncIoBackend::Blocking
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            AsyncIoBackend::Blocking
        }
    };

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .profile(RecorderProfile::Throughput)
        .segment_bytes(2 * 1024 * 1024)
        .segment_preallocate(true)
        .spare_preallocated_segments(2)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .async_io_backend(backend)
        .io_uring_queue_depth(256)
        .io_submit_batch_max(64)
        .io_cqe_batch_max(128)
        .create()
        .unwrap();

    let mut payload = vec![0u8; payload_len];
    for sequence in 1..=record_count {
        fill_payload(sequence, &mut payload);
        let user_header = [sequence as u8, (sequence >> 8) as u8, 0x5A, 0xA5];
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 10,
                user_header: &user_header,
                payload: &payload,
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let stats = recorder.stats();
    assert_that!(stats.committed_records, eq record_count);
    assert_that!(stats.payload_bytes_committed, eq record_count * payload_len as u64);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .replay_budget(ReplayBudget {
            max_records_per_call: record_count as usize,
            max_bytes_per_call: (record_count as usize) * (payload_len + 512),
        })
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(record_count as usize).unwrap())
        .unwrap();

    assert_that!(replayed.len(), eq record_count as usize);

    for (index, frame) in replayed.iter().enumerate().step_by(257) {
        let sequence = index as u64 + 1;
        assert_that!(frame.sequence, eq sequence);
        assert_that!(frame.payload.len(), eq payload_len);
        assert_that!(frame.payload[0], eq payload_byte(sequence, 0));
        assert_that!(
            frame.payload[payload_len / 2],
            eq payload_byte(sequence, payload_len / 2)
        );
        assert_that!(
            frame.payload[payload_len - 1],
            eq payload_byte(sequence, payload_len - 1)
        );
    }

    let last = replayed.last().unwrap();
    assert_that!(last.sequence, eq record_count);
    assert_that!(last.payload[0], eq payload_byte(record_count, 0));
    assert_that!(
        last.payload[payload_len - 1],
        eq payload_byte(record_count, payload_len - 1)
    );
}

#[test]
fn log_archive_phase6_replay_budget_isolates_ingest_progress_under_concurrent_replay() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .profile(RecorderProfile::Throughput)
        .segment_bytes(2 * 1024 * 1024)
        .segment_preallocate(true)
        .spare_preallocated_segments(2)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .async_io_backend(AsyncIoBackend::Blocking)
        .create()
        .unwrap();

    let payload = vec![0x5A; 2048];
    for sequence in 1..=256u64 {
        recorder
            .append_log_record(LogRecordInput {
                sequence,
                event_time_ns: sequence * 10,
                user_header: &[0x10, 0x11],
                payload: &payload,
            })
            .unwrap();
    }
    recorder.flush().unwrap();

    let run_replay = Arc::new(AtomicBool::new(true));
    let replay_flag = Arc::clone(&run_replay);
    let (ready_tx, ready_rx) = mpsc::channel();
    let replay_storage_path = storage_path.clone();
    let replay_metadata_path = metadata_path.clone();
    let replay_worker = thread::spawn(move || -> usize {
        let replayer = ArchiveReplayerBuilder::new(&replay_storage_path)
            .metadata_log_path(&replay_metadata_path)
            .replay_budget(ReplayBudget {
                max_records_per_call: 8,
                max_bytes_per_call: 64 * 1024,
            })
            .open()
            .unwrap();
        ready_tx.send(()).unwrap();

        let mut iterations = 0usize;
        while replay_flag.load(Ordering::Relaxed) {
            let batch = replayer
                .read_range(1, NonZeroUsize::new(512).unwrap())
                .unwrap();
            assert!(batch.len() <= 8);
            iterations += 1;
            thread::sleep(Duration::from_millis(1));
        }
        iterations
    });
    ready_rx.recv().unwrap();

    let mut last_commit = None;
    for sequence in 257..=2048u64 {
        last_commit = Some(
            recorder
                .append_log_record(LogRecordInput {
                    sequence,
                    event_time_ns: sequence * 10,
                    user_header: &[0x20, 0x21],
                    payload: &payload,
                })
                .unwrap(),
        );
    }

    run_replay.store(false, Ordering::Relaxed);
    let replay_iterations = replay_worker.join().unwrap();
    assert_that!(replay_iterations > 0, eq true);

    recorder
        .wait_for_durable_data_and_commit_log(last_commit.unwrap())
        .unwrap();
    recorder.finalize().unwrap();

    let stats = recorder.stats();
    assert_that!(stats.committed_records, eq 2048);
    assert_that!(recorder.last_durable_data_sequence(), eq Some(2048));
    assert_that!(recorder.last_durable_commit_ordinal(), eq Some(2048));

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .replay_budget(ReplayBudget {
            max_records_per_call: 4096,
            max_bytes_per_call: 4096 * 4096,
        })
        .open()
        .unwrap();
    let replayed = replayer
        .read_range(1, NonZeroUsize::new(2048).unwrap())
        .unwrap();
    assert_that!(replayed.len(), eq 2048);
    assert_that!(replayed.last().unwrap().sequence, eq 2048);
}
