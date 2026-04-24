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
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::{TypeDetail, TypeName, TypeVariant};
use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, ArchiveReplayerBuilder, ArchiveSourcePattern, ChecksumMode,
    PersistenceMode, PublishSubscribeRecordInput,
};
use iox2_log_archive_iceoryx2::{ArchiveRematerializeError, PubSubRematerializerBuilder};

fn unique_token() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn type_details(user_header_size: usize, user_header_name: &str) -> (TypeDetail, TypeDetail) {
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
        TypeName::from_str_truncated(user_header_name).unwrap(),
    );
    (payload, user_header)
}

fn receive_bytes(
    subscriber: &Subscriber<ipc::Service, [CustomPayloadMarker], CustomHeaderMarker>,
    user_header_size: usize,
) -> (Vec<u8>, Vec<u8>) {
    for _ in 0..200 {
        if let Some(sample) = unsafe { subscriber.receive_custom_payload().unwrap() } {
            let payload = unsafe {
                core::slice::from_raw_parts(
                    sample.payload().as_ptr().cast::<u8>(),
                    sample.payload().len(),
                )
            }
            .to_vec();
            let user_header = unsafe {
                core::slice::from_raw_parts(
                    (sample.user_header() as *const CustomHeaderMarker).cast::<u8>(),
                    user_header_size,
                )
            }
            .to_vec();
            return (user_header, payload);
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("timed out waiting for rematerialized sample");
}

#[test]
fn log_archive_rematerializer_replays_pubsub_frames_to_publish_subscribe_service() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(16 * 1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    recorder
        .append_publish_subscribe_record(PublishSubscribeRecordInput {
            event_time_ns: 11,
            source_service_id: 7,
            source_publisher_id: 9,
            source_sequence: Some(101),
            user_header: &[0x10, 0x11],
            payload: &[0xA1, 0xA2, 0xA3],
        })
        .unwrap();
    recorder
        .append_publish_subscribe_record(PublishSubscribeRecordInput {
            event_time_ns: 12,
            source_service_id: 7,
            source_publisher_id: 9,
            source_sequence: Some(102),
            user_header: &[0x20, 0x21],
            payload: &[0xB1, 0xB2],
        })
        .unwrap();
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    let token = unique_token();
    let service_name = format!("Archive/Rematerialize/PubSub/{token}");
    let subscriber_node = NodeBuilder::new()
        .name(&NodeName::new(&format!("archive-remat-subscriber-{token}")).unwrap())
        .create::<ipc::Service>()
        .unwrap();
    let (payload_type, user_header_type) = type_details(2, "ArchiveRematHeader2");
    let subscriber_service = unsafe {
        subscriber_node
            .service_builder(&ServiceName::new(&service_name).unwrap())
            .publish_subscribe::<[CustomPayloadMarker]>()
            .user_header::<CustomHeaderMarker>()
            .__internal_set_payload_type_details(&payload_type)
            .__internal_set_user_header_type_details(&user_header_type)
            .open_or_create()
    }
    .unwrap();
    let subscriber = subscriber_service.subscriber_builder().create().unwrap();

    let rematerializer = PubSubRematerializerBuilder::new(service_name.clone())
        .node_name(format!("archive-remat-publisher-{token}"))
        .user_header_type_name("ArchiveRematHeader2")
        .user_header_size(2)
        .source_pattern_filter(Some(ArchiveSourcePattern::PublishSubscribe))
        .create()
        .unwrap();

    assert!(
        rematerializer
            .rematerialize_sequence(&replayer, 1)
            .unwrap()
            .is_some()
    );
    assert!(
        rematerializer
            .rematerialize_sequence(&replayer, 2)
            .unwrap()
            .is_some()
    );

    let (header_1, payload_1) = receive_bytes(&subscriber, 2);
    assert_eq!(header_1, vec![0x10, 0x11]);
    assert_eq!(payload_1, vec![0xA1, 0xA2, 0xA3]);

    let (header_2, payload_2) = receive_bytes(&subscriber, 2);
    assert_eq!(header_2, vec![0x20, 0x21]);
    assert_eq!(payload_2, vec![0xB1, 0xB2]);
}

#[test]
fn log_archive_rematerializer_range_replays_all_pubsub_frames() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(16 * 1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    for sequence in 1..=3u64 {
        recorder
            .append_publish_subscribe_record(PublishSubscribeRecordInput {
                event_time_ns: sequence * 10,
                source_service_id: 4,
                source_publisher_id: 3,
                source_sequence: Some(sequence),
                user_header: &[0xCC, sequence as u8],
                payload: &[sequence as u8, sequence as u8 + 1],
            })
            .unwrap();
    }
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    let token = unique_token();
    let service_name = format!("Archive/Rematerialize/Range/{token}");

    let rematerializer = PubSubRematerializerBuilder::new(service_name)
        .node_name(format!("archive-remat-range-publisher-{token}"))
        .user_header_type_name("ArchiveRematRangeHeader2")
        .user_header_size(2)
        .source_pattern_filter(Some(ArchiveSourcePattern::PublishSubscribe))
        .create()
        .unwrap();

    let published = rematerializer
        .rematerialize_range(&replayer, 1, NonZeroUsize::new(3).unwrap())
        .unwrap();
    assert_eq!(published, 3);
}

#[test]
fn log_archive_rematerializer_reports_incompatible_user_header_size() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(16 * 1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()
        .unwrap();
    recorder
        .append_publish_subscribe_record(PublishSubscribeRecordInput {
            event_time_ns: 41,
            source_service_id: 11,
            source_publisher_id: 12,
            source_sequence: Some(1),
            user_header: &[0xAA, 0xBB],
            payload: &[0x01, 0x02],
        })
        .unwrap();
    recorder.finalize().unwrap();

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()
        .unwrap();

    let token = unique_token();
    let rematerializer =
        PubSubRematerializerBuilder::new(format!("Archive/Rematerialize/Incompatible/{token}"))
            .node_name(format!("archive-remat-incompatible-{token}"))
            .user_header_size(3)
            .create()
            .unwrap();

    let result = rematerializer.rematerialize_sequence(&replayer, 1);
    assert!(matches!(
        result,
        Err(ArchiveRematerializeError::IncompatibleUserHeaderSize {
            expected: 3,
            actual: 2,
            sequence: 1,
        })
    ));
}
