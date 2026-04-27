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
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use iceoryx2::prelude::*;
use iox2_log_archive_core::log_archive::{PersistenceMode, RecorderProfile};
use iox2_log_archive_iceoryx2::{
    LOG_RECORDER_CONTROL_CMD_FLUSH, LOG_RECORDER_CONTROL_CMD_PAUSE,
    LOG_RECORDER_CONTROL_CMD_RESUME, LOG_RECORDER_CONTROL_CMD_STATUS,
    LOG_RECORDER_CONTROL_CMD_STOP, LOG_RECORDER_CONTROL_NONE,
    LOG_RECORDER_CONTROL_PROTOCOL_VERSION, LOG_RECORDER_CONTROL_STATE_RUNNING,
    LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR, LogRecorderControlClientConfig,
    LogRecorderControlError, LogRecorderControlRequest, LogRecorderControlResponse,
    PubSubRecorderConfig, PubSubRecorderStopReason, decode_optional_u64, encode_optional_u64,
    log_recorder_control_service_name, record_publish_subscribe, request_recorder_control,
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

#[test]
fn pubsub_recorder_rejects_unknown_live_control_command() {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let service = unique_service_name("LogArchiveAdapter/PubSubControlInvalid");

    let node = NodeBuilder::new().create::<ipc::Service>().unwrap();
    let service_name = ServiceName::new(&service).unwrap();
    let pubsub = node
        .service_builder(&service_name)
        .publish_subscribe::<u64>()
        .open_or_create()
        .unwrap();
    let publisher = pubsub.publisher_builder().create().unwrap();

    let recorder_service = service.clone();
    let recorder_node_name = format!(
        "iox2-log-archive-invalid-control-test-recorder-{}",
        UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let recorder = thread::spawn(move || {
        record_publish_subscribe(PubSubRecorderConfig {
            service: recorder_service,
            node_name: recorder_node_name,
            storage_path,
            metadata_log_path: metadata_path,
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

    wait_for_control_status(&service);
    publisher.send_copy(1).unwrap();
    let invalid = control(&service, 999).unwrap_err();
    assert!(matches!(invalid, LogRecorderControlError::InvalidInput(_)));
    assert!(
        invalid
            .to_string()
            .contains("daemon rejected command as invalid")
    );

    control(&service, LOG_RECORDER_CONTROL_CMD_STOP).unwrap();
    let summary = recorder.join().unwrap().unwrap();
    assert_eq!(summary.stop_reason, PubSubRecorderStopReason::ControlStop);
}

#[test]
fn control_protocol_helpers_and_config_errors_are_stable() {
    assert_eq!(
        log_recorder_control_service_name("Service/Name/"),
        "Service/Name/_log_recorder_control"
    );
    assert_eq!(encode_optional_u64(Some(7)), 7);
    assert_eq!(encode_optional_u64(None), LOG_RECORDER_CONTROL_NONE);
    assert_eq!(decode_optional_u64(9), Some(9));
    assert_eq!(decode_optional_u64(LOG_RECORDER_CONTROL_NONE), None);

    let request = LogRecorderControlRequest::new(LOG_RECORDER_CONTROL_CMD_STATUS);
    assert_eq!(request.command, LOG_RECORDER_CONTROL_CMD_STATUS);
    assert_eq!(request.reserved, 0);

    let response = LogRecorderControlResponse::ok(0, 1, 2, 3, 4, 5, 6, 7, 8);
    assert_eq!(response.committed_records, 1);
    assert_eq!(response.payload_bytes_committed, 2);
    assert_eq!(response.last_durable_data_sequence, 5);

    let error_response = LogRecorderControlResponse::error(1);
    assert_eq!(error_response.status, 1);
    assert_eq!(
        error_response.last_durable_commit_ordinal,
        LOG_RECORDER_CONTROL_NONE
    );

    for (config, expected) in [
        (
            LogRecorderControlClientConfig {
                service: "".to_string(),
                node_name: "node".to_string(),
                timeout: Duration::from_millis(1),
            },
            "service must not be empty",
        ),
        (
            LogRecorderControlClientConfig {
                service: "Service".to_string(),
                node_name: "".to_string(),
                timeout: Duration::from_millis(1),
            },
            "node_name must not be empty",
        ),
        (
            LogRecorderControlClientConfig {
                service: "Service".to_string(),
                node_name: "node".to_string(),
                timeout: Duration::ZERO,
            },
            "timeout must be greater than zero",
        ),
    ] {
        let error = request_recorder_control(config, LOG_RECORDER_CONTROL_CMD_STATUS).unwrap_err();
        assert!(matches!(error, LogRecorderControlError::InvalidInput(_)));
        assert!(error.to_string().contains(expected));
    }
}

#[test]
fn control_client_reports_malformed_daemon_responses() {
    let service = unique_service_name("LogArchiveAdapter/FakeControl");

    let mut wrong_protocol = LogRecorderControlResponse::ok(
        LOG_RECORDER_CONTROL_STATE_RUNNING,
        0,
        0,
        0,
        0,
        LOG_RECORDER_CONTROL_NONE,
        LOG_RECORDER_CONTROL_NONE,
        0,
        LOG_RECORDER_CONTROL_NONE,
    );
    wrong_protocol.protocol_version = LOG_RECORDER_CONTROL_PROTOCOL_VERSION - 1;
    let error = fake_control_response(&format!("{service}/Protocol"), wrong_protocol).unwrap_err();
    assert!(matches!(error, LogRecorderControlError::NotAvailable(_)));
    assert!(error.to_string().contains("protocol version mismatch"));

    let error = fake_control_response(
        &format!("{service}/Internal"),
        LogRecorderControlResponse::error(LOG_RECORDER_CONTROL_STATUS_INTERNAL_ERROR),
    )
    .unwrap_err();
    assert!(matches!(error, LogRecorderControlError::NotAvailable(_)));
    assert!(
        error
            .to_string()
            .contains("daemon failed to execute command")
    );

    let error = fake_control_response(
        &format!("{service}/Status"),
        LogRecorderControlResponse::error(99),
    )
    .unwrap_err();
    assert!(matches!(error, LogRecorderControlError::Iceoryx2(_)));
    assert!(error.to_string().contains("unknown status code 99"));

    let error = fake_control_response(
        &format!("{service}/State"),
        LogRecorderControlResponse::ok(
            99,
            0,
            0,
            0,
            0,
            LOG_RECORDER_CONTROL_NONE,
            LOG_RECORDER_CONTROL_NONE,
            0,
            LOG_RECORDER_CONTROL_NONE,
        ),
    )
    .unwrap_err();
    assert!(matches!(error, LogRecorderControlError::Iceoryx2(_)));
    assert!(error.to_string().contains("unknown state code 99"));
}

fn wait_for_control_status(service: &str) -> iox2_log_archive_iceoryx2::LogRecorderControlResult {
    wait_until(service, |_| true)
}

fn fake_control_response(
    service: &str,
    response: LogRecorderControlResponse,
) -> Result<iox2_log_archive_iceoryx2::LogRecorderControlResult, LogRecorderControlError> {
    let (ready_tx, ready_rx) = mpsc::channel();
    let service = service.to_string();
    let server_service = service.clone();
    let server_node_name = format!(
        "iox2-log-archive-fake-control-server-{}",
        UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let handle = thread::spawn(move || {
        let node = NodeBuilder::new()
            .name(&NodeName::new(&server_node_name).unwrap())
            .create::<ipc::Service>()
            .unwrap();
        let control_service = log_recorder_control_service_name(&server_service);
        let request_response = node
            .service_builder(&ServiceName::new(&control_service).unwrap())
            .request_response::<LogRecorderControlRequest, LogRecorderControlResponse>()
            .open_or_create()
            .unwrap();
        let server = request_response.server_builder().create().unwrap();
        ready_tx.send(()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            while let Some(active_request) = server.receive().unwrap() {
                let _request = *active_request;
                let _ = active_request.send_copy(response);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for fake control request"
            );
            thread::sleep(Duration::from_millis(2));
        }
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let result = request_recorder_control(
        LogRecorderControlClientConfig {
            service,
            node_name: format!(
                "iox2-log-archive-fake-control-client-{}",
                UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
            ),
            timeout: Duration::from_secs(2),
        },
        LOG_RECORDER_CONTROL_CMD_STATUS,
    );
    handle.join().unwrap();
    result
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
