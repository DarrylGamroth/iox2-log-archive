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

use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::{TypeDetail, TypeName, TypeVariant};
use iox2_log_archive_core::log_archive::{
    ArchiveMetadataIndexerBuilder, ArchiveReplayerBuilder, PersistenceMode, RecorderProfile,
};
use iox2_log_archive_iceoryx2::{
    PubSubRecorderConfig, PubSubRecorderStopReason, PubSubRematerializerBuilder,
    record_publish_subscribe,
};
use iox2_log_archive_sqlite::SqliteMetadataSink;

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn unique_service_name(prefix: &str) -> String {
    let suffix = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}/{}/{}", std::process::id(), suffix)
}

fn byte_slice_service_details(user_header_size: usize) -> (TypeDetail, TypeDetail) {
    let mut payload = TypeDetail::new::<()>(TypeVariant::Dynamic);
    iceoryx2::testing::type_detail_set_size(&mut payload, 1);
    iceoryx2::testing::type_detail_set_alignment(&mut payload, 1);
    iceoryx2::testing::type_detail_set_name(
        &mut payload,
        TypeName::from_str_truncated("u8").unwrap(),
    );

    let mut user_header = TypeDetail::new::<()>(TypeVariant::FixedSize);
    iceoryx2::testing::type_detail_set_size(&mut user_header, user_header_size);
    iceoryx2::testing::type_detail_set_alignment(&mut user_header, 1);
    iceoryx2::testing::type_detail_set_name(
        &mut user_header,
        TypeName::from_str_truncated("ArchiveE2EHeader").unwrap(),
    );

    (payload, user_header)
}

fn receive_payload(
    subscriber: &Subscriber<ipc::Service, [CustomPayloadMarker], CustomHeaderMarker>,
) -> Vec<u8> {
    for _ in 0..200 {
        if let Some(sample) = unsafe { subscriber.receive_custom_payload().unwrap() } {
            let payload = unsafe {
                core::slice::from_raw_parts(
                    sample.payload().as_ptr().cast::<u8>(),
                    sample.payload().len(),
                )
            };
            return payload.to_vec();
        }
        thread::sleep(Duration::from_millis(2));
    }

    panic!("timed out waiting for rematerialized payload");
}

#[test]
fn pubsub_record_index_query_replay_rematerialize_is_end_to_end_functional() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let sqlite_path = temp.path().join("metadata.sqlite");
    let source_service = unique_service_name("LogArchiveAdapter/E2E/Source");
    let target_service = unique_service_name("LogArchiveAdapter/E2E/Target");

    let source_node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let source_pubsub = source_node
        .service_builder(&ServiceName::new(&source_service).unwrap())
        .publish_subscribe::<u64>()
        .open_or_create()
        .unwrap();
    let source_publisher = source_pubsub.publisher_builder().create().unwrap();

    let recorder_service = source_service.clone();
    let recorder_storage_path = storage_path.clone();
    let recorder_metadata_path = metadata_path.clone();
    let recorder = thread::spawn(move || {
        record_publish_subscribe(PubSubRecorderConfig {
            service: recorder_service,
            node_name: "iox2-log-archive-e2e-recorder".to_string(),
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
            source_service_id: Some(42),
            cycle_time: Duration::from_millis(5),
            max_messages: Some(3),
            timeout: Some(Duration::from_secs(10)),
            flush_interval: Some(Duration::from_millis(10)),
            ack_level: None,
            shutdown_requested: None,
        })
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while !recorder.is_finished() && Instant::now() < deadline {
        for value in [10u64, 20, 30] {
            source_publisher.send_copy(value).unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    }

    let summary = recorder.join().unwrap().unwrap();
    assert_eq!(summary.stop_reason, PubSubRecorderStopReason::MaxMessages);
    assert_eq!(summary.committed_records, 3);

    let sqlite_sink = SqliteMetadataSink::open_for_stream(&sqlite_path, "source").unwrap();
    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .sink(Box::new(sqlite_sink.clone()))
        .open()
        .unwrap();
    assert_eq!(indexer.catch_up_once().unwrap(), 3);
    assert_eq!(sqlite_sink.record_count().unwrap(), 3);

    let selectors = sqlite_sink.query_range_by_sequence(1, 3).unwrap();
    assert_eq!(selectors.len(), 3);

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    let target_node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let (payload_type, user_header_type) = byte_slice_service_details(0);
    let target_pubsub = unsafe {
        target_node
            .service_builder(&ServiceName::new(&target_service).unwrap())
            .publish_subscribe::<[CustomPayloadMarker]>()
            .user_header::<CustomHeaderMarker>()
            .__internal_set_payload_type_details(&payload_type)
            .__internal_set_user_header_type_details(&user_header_type)
            .open_or_create()
    }
    .unwrap();
    let target_subscriber = target_pubsub.subscriber_builder().create().unwrap();

    let rematerializer = PubSubRematerializerBuilder::new(target_service)
        .node_name("iox2-log-archive-e2e-rematerializer")
        .user_header_type_name("ArchiveE2EHeader")
        .create()
        .unwrap();
    let locators = selectors
        .iter()
        .map(|record| record.locator)
        .collect::<Vec<_>>();
    assert_eq!(
        rematerializer
            .rematerialize_locators(&replayer, &locators)
            .unwrap(),
        3
    );

    let payload = receive_payload(&target_subscriber);
    assert_eq!(payload.len(), core::mem::size_of::<u64>());
}
