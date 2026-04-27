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

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iox2_log_archive_core::log_archive::{
    ArchiveReplayerBuilder, PersistenceMode, RecorderProfile,
};
use iox2_log_archive_iceoryx2::{
    PubSubRecorderConfig, PubSubRecorderStopReason, record_publish_subscribe,
};

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn unique_service_name(prefix: &str) -> String {
    let suffix = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}/{}/{}", std::process::id(), suffix)
}

#[test]
fn pubsub_recorder_captures_live_samples() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let service = unique_service_name("LogArchiveAdapter/PubSubRecord");

    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service_name = ServiceName::new(&service).unwrap();
    let pubsub = node
        .service_builder(&service_name)
        .publish_subscribe::<u64>()
        .open_or_create()
        .unwrap();
    let publisher = pubsub.publisher_builder().create().unwrap();

    let recorder_service = service.clone();
    let recorder_storage_path = storage_path.clone();
    let recorder_metadata_path = metadata_path.clone();
    let recorder = thread::spawn(move || {
        record_publish_subscribe(PubSubRecorderConfig {
            service: recorder_service,
            node_name: "iox2-log-archive-test-recorder".to_string(),
            storage_path: recorder_storage_path,
            metadata_log_path: recorder_metadata_path,
            profile: RecorderProfile::Balanced,
            persistence_mode: PersistenceMode::Async,
            segment_bytes: 16 * 1024,
            spare_preallocated_segments: 0,
            segment_preallocate: false,
            max_disk_bytes: None,
            async_io_backend: None,
            io_uring_queue_depth: None,
            io_submit_batch_max: None,
            io_cqe_batch_max: None,
            io_uring_register_files: None,
            checksum_mode: None,
            subscriber_max_borrowed_samples: None,
            out_of_space_policy: None,
            metadata_log_roll_bytes: None,
            metadata_log_max_bytes: None,
            source_service_id: None,
            cycle_time: Duration::from_millis(5),
            max_messages: Some(3),
            timeout: Some(Duration::from_secs(10)),
            flush_interval: Some(Duration::from_millis(10)),
            ack_level: None,
            shutdown_requested: None,
        })
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    for value in 1..=256u64 {
        publisher.send_copy(value).unwrap();
        if recorder.is_finished() {
            break;
        }
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }

    let summary = recorder.join().unwrap().unwrap();
    assert_eq!(summary.messages_recorded, 3);
    assert_eq!(summary.committed_records, 3);
    assert_eq!(summary.stop_reason, PubSubRecorderStopReason::MaxMessages);
    assert_eq!(summary.io_uring_queue_depth, 256);
    assert_eq!(summary.metadata_log_roll_bytes, 1024 * 1024 * 1024);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    for sequence in 1..=3u64 {
        let frame = replayer.read_at_sequence(sequence).unwrap().unwrap();
        assert_eq!(frame.sequence, sequence);
        assert_eq!(frame.payload.len(), core::mem::size_of::<u64>());
    }
}

#[test]
fn pubsub_recorder_stops_on_cooperative_shutdown_flag() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let service = unique_service_name("LogArchiveAdapter/PubSubShutdown");
    let shutdown_requested = Arc::new(AtomicBool::new(false));

    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service_name = ServiceName::new(&service).unwrap();
    let _pubsub = node
        .service_builder(&service_name)
        .publish_subscribe::<u64>()
        .open_or_create()
        .unwrap();

    let recorder_service = service.clone();
    let recorder_shutdown = Arc::clone(&shutdown_requested);
    let recorder = thread::spawn(move || {
        record_publish_subscribe(PubSubRecorderConfig {
            service: recorder_service,
            node_name: "iox2-log-archive-test-shutdown-recorder".to_string(),
            storage_path,
            metadata_log_path: metadata_path,
            profile: RecorderProfile::Balanced,
            persistence_mode: PersistenceMode::Async,
            segment_bytes: 16 * 1024,
            spare_preallocated_segments: 0,
            segment_preallocate: false,
            max_disk_bytes: None,
            async_io_backend: None,
            io_uring_queue_depth: Some(8),
            io_submit_batch_max: Some(4),
            io_cqe_batch_max: Some(8),
            io_uring_register_files: Some(false),
            checksum_mode: None,
            subscriber_max_borrowed_samples: None,
            out_of_space_policy: None,
            metadata_log_roll_bytes: None,
            metadata_log_max_bytes: None,
            source_service_id: None,
            cycle_time: Duration::from_millis(5),
            max_messages: None,
            timeout: Some(Duration::from_secs(10)),
            flush_interval: Some(Duration::from_millis(10)),
            ack_level: None,
            shutdown_requested: Some(recorder_shutdown),
        })
    });

    thread::sleep(Duration::from_millis(25));
    shutdown_requested.store(true, Ordering::SeqCst);

    let summary = recorder.join().unwrap().unwrap();
    assert_eq!(
        summary.stop_reason,
        PubSubRecorderStopReason::ShutdownRequested
    );
    assert_eq!(summary.io_uring_queue_depth, 8);
    assert_eq!(summary.io_submit_batch_max, 4);
    assert_eq!(summary.io_cqe_batch_max, 8);
    assert!(!summary.io_uring_register_files);
}
