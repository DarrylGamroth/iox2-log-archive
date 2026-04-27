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

use iceoryx2::prelude::*;
use iox2_log_archive_core::log_archive::{PersistenceMode, RecorderProfile};
use iox2_log_archive_iceoryx2::{
    LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP, LogRecorderControlClientConfig, LogRecorderControlError,
    PubSubRecorderConfig, PubSubRecorderStopReason, record_publish_subscribe,
    request_recorder_control,
};

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn unique_service_name(prefix: &str) -> String {
    let suffix = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}/{}/{}", std::process::id(), suffix)
}

#[test]
fn pubsub_recorder_accepts_live_control_commands() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let service = unique_service_name("LogArchiveAdapter/PubSubControl");

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
            node_name: "iox2-log-archive-control-test-recorder".to_string(),
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
            max_messages: None,
            timeout: Some(Duration::from_secs(30)),
            flush_interval: Some(Duration::from_millis(25)),
            ack_level: None,
            shutdown_requested: None,
        })
    });

    let status = wait_for_control_status(&service);
    assert!(!status.is_paused);
    assert_eq!(status.committed_records, 0);

    let pause = control(&service, LOG_RECORDER_CONTROL_CMD_PAUSE).unwrap();
    assert!(pause.is_paused);
    assert!(pause.paused_since_ns.is_some());

    let paused_status = publish_until(&publisher, &service, 1, |status| {
        status.dropped_while_paused >= 3
    });
    assert!(paused_status.is_paused);
    assert_eq!(paused_status.committed_records, 0);

    let resume = control(&service, LOG_RECORDER_CONTROL_CMD_RESUME).unwrap();
    assert!(!resume.is_paused);
    assert_eq!(resume.paused_since_ns, None);

    let recorded_status = publish_until(&publisher, &service, 10_000, |status| {
        status.committed_records >= 3
    });
    assert!(recorded_status.dropped_while_paused >= 3);
    assert!(recorded_status.committed_records >= 3);

    let flushed = control(&service, LOG_RECORDER_CONTROL_CMD_FLUSH).unwrap();
    assert!(flushed.committed_records >= 3);
    assert!(flushed.payload_bytes_committed >= 3 * core::mem::size_of::<u64>() as u64);
    assert!(flushed.data_bytes_written > 0);
    assert!(flushed.metadata_bytes_written > 0);

    let stopped = control(&service, LOG_RECORDER_CONTROL_CMD_STOP).unwrap();
    assert!(stopped.committed_records >= 3);

    let summary = recorder.join().unwrap().unwrap();
    assert_eq!(summary.stop_reason, PubSubRecorderStopReason::ControlStop);
    assert!(summary.messages_recorded >= 3);
    assert!(summary.dropped_while_paused >= 3);
    assert!(!summary.paused_at_shutdown);
}

fn wait_for_control_status(service: &str) -> iox2_log_archive_iceoryx2::LogRecorderControlResult {
    wait_until(service, |_| true)
}

fn wait_until(
    service: &str,
    predicate: impl Fn(&iox2_log_archive_iceoryx2::LogRecorderControlResult) -> bool,
) -> iox2_log_archive_iceoryx2::LogRecorderControlResult {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_error = None;

    loop {
        match control(service, LOG_RECORDER_CONTROL_CMD_STATUS) {
            Ok(status) if predicate(&status) => return status,
            Ok(_) => {}
            Err(error) => last_error = Some(error),
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for control status; last_error={last_error:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn publish_until(
    publisher: &iceoryx2::port::publisher::Publisher<ipc::Service, u64, ()>,
    service: &str,
    mut value: u64,
    predicate: impl Fn(&iox2_log_archive_iceoryx2::LogRecorderControlResult) -> bool,
) -> iox2_log_archive_iceoryx2::LogRecorderControlResult {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        publisher.send_copy(value).unwrap();
        value = value.saturating_add(1);

        let status = control(service, LOG_RECORDER_CONTROL_CMD_STATUS).unwrap();
        if predicate(&status) {
            return status;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for published samples to satisfy control predicate; status={status:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn control(
    service: &str,
    command: u16,
) -> Result<iox2_log_archive_iceoryx2::LogRecorderControlResult, LogRecorderControlError> {
    request_recorder_control(
        LogRecorderControlClientConfig {
            service: service.to_string(),
            node_name: format!(
                "iox2-log-archive-control-test-client-{}",
                UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
            ),
            timeout: Duration::from_secs(2),
        },
        command,
    )
}
