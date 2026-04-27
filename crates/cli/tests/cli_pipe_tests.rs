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

use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::{TypeDetail, TypeName, TypeVariant};
use iox2_log_archive_core::log_archive::{
    ARCHIVE_FILE_HEADER_V1_LEN, ArchiveLocator, ArchiveMetadataSink, ArchiveRecorderBuilder,
    ArchiveSourcePattern, ChecksumMode, MetadataCommitRecord, PersistenceMode,
    PublishSubscribeRecordInput,
};
use iox2_log_archive_sqlite::{SQLITE_SCHEMA_VERSION, SqliteIndexerState, SqliteMetadataSink};

static UNIQUE_COUNTER: AtomicU64 = AtomicU64::new(1);
const TEST_FRAME_OFFSET_MAGIC: u64 = 0;

fn unique_service_name(prefix: &str) -> String {
    let suffix = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}/{}/{}", std::process::id(), suffix)
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn overwrite_bytes(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
}

fn wait_for_child(mut child: Child, context: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            panic!(
                "{context} did not exit before timeout\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
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
        TypeName::from_str_truncated("()").unwrap(),
    );

    (payload, user_header)
}

fn receive_payload(
    subscriber: &Subscriber<ipc::Service, [CustomPayloadMarker], CustomHeaderMarker>,
) -> Vec<u8> {
    for _ in 0..2500 {
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

    panic!("timed out waiting for publish-subscribe replay payload");
}

fn assert_failure_contains(output: &Output, expected: &str, context: &str) {
    assert!(
        !output.status.success(),
        "{context} unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains(expected),
        "{context} did not contain {expected:?}\ncombined output:\n{combined}"
    );
}

fn archive_args(service: &str, storage_path: &Path, metadata_path: &Path) -> Vec<String> {
    vec![
        "--service".to_string(),
        service.to_string(),
        "--storage-path".to_string(),
        storage_path
            .to_str()
            .expect("utf-8 storage path")
            .to_string(),
        "--metadata-log-path".to_string(),
        metadata_path
            .to_str()
            .expect("utf-8 metadata path")
            .to_string(),
    ]
}

fn replay_archive_args(storage_path: &Path, metadata_path: &Path) -> Vec<String> {
    vec![
        "--storage-path".to_string(),
        storage_path
            .to_str()
            .expect("utf-8 storage path")
            .to_string(),
        "--metadata-log-path".to_string(),
        metadata_path
            .to_str()
            .expect("utf-8 metadata path")
            .to_string(),
    ]
}

fn create_archive(
    storage_path: &std::path::Path,
    metadata_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = ArchiveRecorderBuilder::new(storage_path)
        .metadata_log_path(metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()?;

    for sequence in 1..=4u64 {
        let payload = vec![sequence as u8; sequence as usize + 4];
        recorder.append_publish_subscribe_record(PublishSubscribeRecordInput {
            event_time_ns: sequence * 1_000,
            source_service_id: 1,
            source_publisher_id: 1,
            source_sequence: Some(sequence),
            user_header: &[0xA1, sequence as u8],
            payload: &payload,
        })?;
    }
    recorder.finalize()?;
    Ok(())
}

fn index_archive(
    metadata_path: &Path,
    db_path: &Path,
    stream_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_iox2-log-query"))
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            stream_id,
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
        ])
        .output()?;
    assert_success(&output, "index catch-up");
    Ok(())
}

fn wait_for_control_cli_status(
    service: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let output = control_cli(service, "status")?;
        if output.status.success() {
            return Ok(serde_json::from_slice(&output.stdout)?);
        }

        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for control CLI status\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn control_cli(service: &str, command: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let node_name = format!(
        "iox2-log-archive-cli-control-test-{}",
        UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    Ok(Command::new(env!("CARGO_BIN_EXE_iox2-log-control"))
        .args([
            "--format",
            "JSON",
            command,
            "--service",
            service,
            "--node-name",
            &node_name,
            "--timeout-ms",
            "1000",
        ])
        .output()?)
}

fn first_locator_from_range(
    db_path: &Path,
    stream_id: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new(env!("CARGO_BIN_EXE_iox2-log-query"))
        .args([
            "--format",
            "JSON",
            "query",
            "locate-range",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            stream_id,
            "--from",
            "1",
            "--count",
            "1",
            "--emit",
            "selectors",
            "--expand-selectors",
        ])
        .output()?;
    assert_success(&output, "locate-range for locator");
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn query_expanded_selectors_pipe_to_replay_stdout() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query.sqlite");
    create_archive(&storage_path, &metadata_path)?;

    let replay_bin = env!("CARGO_BIN_EXE_iox2-log-replay");
    index_archive(&metadata_path, &db_path, "smoke")?;

    let selectors = Command::new(env!("CARGO_BIN_EXE_iox2-log-query"))
        .args([
            "--format",
            "JSON",
            "query",
            "locate-range",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            "smoke",
            "--from",
            "1",
            "--count",
            "4",
            "--emit",
            "selectors",
            "--expand-selectors",
        ])
        .output()?;
    assert_success(&selectors, "query locate-range");
    assert_eq!(
        String::from_utf8_lossy(&selectors.stdout).lines().count(),
        4
    );

    let mut replay = Command::new(replay_bin)
        .args([
            "--format",
            "JSON",
            "replay",
            "--storage-path",
            storage_path.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--to",
            "stdout",
            "selectors",
            "--stdin",
            "--selector-format",
            "ndjson",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    replay
        .stdin
        .as_mut()
        .expect("piped stdin")
        .write_all(&selectors.stdout)?;
    let replay = replay.wait_with_output()?;
    assert_success(&replay, "replay from piped selectors");
    assert_eq!(String::from_utf8_lossy(&replay.stdout).lines().count(), 4);
    assert!(String::from_utf8_lossy(&replay.stderr).contains("\"emitted\": 4"));

    Ok(())
}

#[test]
fn replay_all_replays_every_record() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;

    let replay = Command::new(env!("CARGO_BIN_EXE_iox2-log-replay"))
        .args([
            "--format",
            "JSON",
            "replay",
            "--storage-path",
            storage_path.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--to",
            "stdout",
            "all",
        ])
        .output()?;
    assert_success(&replay, "replay all");
    assert_eq!(String::from_utf8_lossy(&replay.stdout).lines().count(), 4);
    assert!(String::from_utf8_lossy(&replay.stderr).contains("\"selector_source\": \"all\""));
    assert!(String::from_utf8_lossy(&replay.stderr).contains("\"emitted\": 4"));

    Ok(())
}

#[test]
fn recorder_help_exposes_general_throughput_profile_only() -> Result<(), Box<dyn std::error::Error>>
{
    let help = Command::new(env!("CARGO_BIN_EXE_iox2-log-recorder"))
        .args(["publish-subscribe", "--help"])
        .output()?;
    assert_success(&help, "recorder help");

    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(stdout.contains("durable, balanced, throughput, replay"));
    assert!(stdout.contains("--subscriber-max-borrowed-samples"));
    assert!(!stdout.contains("camera-throughput"));

    Ok(())
}

#[test]
fn recorder_rejects_zero_borrowed_sample_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");

    let output = Command::new(env!("CARGO_BIN_EXE_iox2-log-recorder"))
        .args([
            "--format",
            "JSON",
            "publish-subscribe",
            "--service",
            "Test/Recorder/InvalidBorrowCapacity",
            "--storage-path",
            storage_path.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--subscriber-max-borrowed-samples",
            "0",
            "--timeout-ms",
            "1",
        ])
        .output()?;

    assert_failure_contains(
        &output,
        "--subscriber-max-borrowed-samples must be greater than 0",
        "recorder zero borrowed-sample capacity",
    );

    Ok(())
}

#[test]
fn recorder_rejects_invalid_runtime_and_archive_options() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let recorder_bin = env!("CARGO_BIN_EXE_iox2-log-recorder");
    let storage = storage_path.to_str().expect("utf-8 storage path");
    let metadata = metadata_path.to_str().expect("utf-8 metadata path");

    for (args, expected, context) in [
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--timeout-ms",
                "1",
            ],
            "--service must not be empty",
            "recorder empty service",
        ),
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "Test/Recorder/BadCycle",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--cycle-time-ms",
                "0",
                "--timeout-ms",
                "1",
            ],
            "--cycle-time-ms must be greater than 0",
            "recorder zero cycle time",
        ),
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "Test/Recorder/BadSegment",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--segment-bytes",
                "0",
                "--timeout-ms",
                "1",
            ],
            "--segment-bytes must be greater than 0",
            "recorder zero segment bytes",
        ),
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "Test/Recorder/BadQueueDepth",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--io-uring-queue-depth",
                "0",
                "--timeout-ms",
                "1",
            ],
            "--io-uring-queue-depth must be greater than 0",
            "recorder zero io_uring queue depth",
        ),
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "Test/Recorder/BadSubmitBatch",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--io-submit-batch-max",
                "0",
                "--timeout-ms",
                "1",
            ],
            "--io-submit-batch-max must be greater than 0",
            "recorder zero submit batch",
        ),
        (
            vec![
                "--format",
                "JSON",
                "publish-subscribe",
                "--service",
                "Test/Recorder/BadCqeBatch",
                "--storage-path",
                storage,
                "--metadata-log-path",
                metadata,
                "--io-cqe-batch-max",
                "0",
                "--timeout-ms",
                "1",
            ],
            "--io-cqe-batch-max must be greater than 0",
            "recorder zero cqe batch",
        ),
    ] {
        let output = Command::new(recorder_bin).args(args).output()?;
        assert_failure_contains(&output, expected, context);
    }

    Ok(())
}

#[test]
fn admin_commands_cover_archive_lifecycle_and_inspection() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;

    let admin_bin = env!("CARGO_BIN_EXE_iox2-log-admin");
    let service = "Test/Admin/Coverage";
    let archive = archive_args(service, &storage_path, &metadata_path);

    for (command, expected) in [
        ("status", "\"operation\": \"status\""),
        ("flush", "\"operation\": \"flush\""),
        ("list-segments", "\"segments\""),
    ] {
        let mut args = vec![
            "--format".to_string(),
            "JSON".to_string(),
            command.to_string(),
        ];
        args.extend(archive.clone());
        let output = Command::new(admin_bin).args(args).output()?;
        assert_success(&output, command);
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(expected),
            "{command} stdout missing {expected}"
        );
    }

    let mut inspect_log_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "inspect-commit-log".to_string(),
    ];
    inspect_log_args.extend(archive.clone());
    inspect_log_args.extend(["--from-ordinal".to_string(), "2".to_string()]);
    inspect_log_args.extend(["--limit".to_string(), "2".to_string()]);
    let output = Command::new(admin_bin).args(inspect_log_args).output()?;
    assert_success(&output, "inspect-commit-log");
    let inspect_log: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(inspect_log["entries"].as_array().unwrap().len(), 2);
    assert_eq!(inspect_log["entries"][0]["sequence"], 2);

    let mut inspect_record_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "inspect-record".to_string(),
    ];
    inspect_record_args.extend(archive.clone());
    inspect_record_args.extend(["--at-sequence".to_string(), "3".to_string()]);
    inspect_record_args.extend(["--preview-bytes".to_string(), "2".to_string()]);
    let output = Command::new(admin_bin).args(inspect_record_args).output()?;
    assert_success(&output, "inspect-record");
    let record: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(record["record"]["sequence"], 3);
    assert_eq!(record["record"]["payload"]["preview_len"], 2);
    assert_eq!(record["record"]["payload"]["truncated"], true);

    let mut detach_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "detach".to_string(),
    ];
    detach_args.extend(archive.clone());
    detach_args.extend(["--before-sequence".to_string(), "5".to_string()]);
    let output = Command::new(admin_bin).args(detach_args).output()?;
    assert_success(&output, "detach");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"affected_segments\": 1"));

    let mut list_detached_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "list-segments".to_string(),
    ];
    list_detached_args.extend(archive.clone());
    list_detached_args.push("--detached-only".to_string());
    let output = Command::new(admin_bin).args(list_detached_args).output()?;
    assert_success(&output, "list detached segments");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"tier\": \"ColdDetached\""));

    let mut attach_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "attach".to_string(),
    ];
    attach_args.extend(archive.clone());
    let output = Command::new(admin_bin).args(attach_args).output()?;
    assert_success(&output, "attach");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"affected_segments\": 1"));

    let mut trim_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "trim".to_string(),
    ];
    trim_args.extend(archive);
    trim_args.extend(["--before-sequence".to_string(), "0".to_string()]);
    let output = Command::new(admin_bin).args(trim_args).output()?;
    assert_success(&output, "trim no-op");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"operation\": \"trim\""));

    Ok(())
}

#[test]
fn admin_commands_cover_start_stop_delete_detached_and_record_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let admin_bin = env!("CARGO_BIN_EXE_iox2-log-admin");

    let empty_storage = temp.path().join("empty-archive");
    let empty_metadata = temp.path().join("empty-metadata");
    let empty_archive = archive_args("Test/Admin/StartStop", &empty_storage, &empty_metadata);

    let mut start_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "start".to_string(),
    ];
    start_args.extend(empty_archive.clone());
    start_args.extend(["--segment-bytes".to_string(), "4096".to_string()]);
    start_args.extend(["--segment-preallocate".to_string(), "false".to_string()]);
    start_args.extend(["--spare-preallocated-segments".to_string(), "0".to_string()]);
    let output = Command::new(admin_bin).args(start_args).output()?;
    assert_success(&output, "admin start");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"operation\": \"start\""));
    assert!(empty_storage.join("catalog.bin").exists());
    assert!(empty_metadata.join("commit.idxlog").exists());

    let mut stop_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "stop".to_string(),
    ];
    stop_args.extend(empty_archive);
    let output = Command::new(admin_bin).args(stop_args).output()?;
    assert_success(&output, "admin stop");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"operation\": \"stop\""));

    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;
    let archive = archive_args("Test/Admin/DeleteDetached", &storage_path, &metadata_path);

    let mut invalid_locator_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "inspect-record".to_string(),
    ];
    invalid_locator_args.extend(archive.clone());
    invalid_locator_args.extend(["--at-locator".to_string(), "1:0:0:64".to_string()]);
    let output = Command::new(admin_bin)
        .args(invalid_locator_args)
        .output()?;
    assert_failure_contains(
        &output,
        "invalid locator segment generation '0'",
        "admin invalid locator",
    );

    let mut missing_sequence_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "inspect-record".to_string(),
    ];
    missing_sequence_args.extend(archive.clone());
    missing_sequence_args.extend(["--at-sequence".to_string(), "99".to_string()]);
    let output = Command::new(admin_bin)
        .args(missing_sequence_args)
        .output()?;
    assert_failure_contains(
        &output,
        "sequence 99 is not available",
        "admin missing record",
    );

    let mut detach_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "detach".to_string(),
    ];
    detach_args.extend(archive.clone());
    detach_args.extend(["--before-sequence".to_string(), "5".to_string()]);
    assert_success(
        &Command::new(admin_bin).args(detach_args).output()?,
        "detach",
    );

    let mut delete_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "delete-detached".to_string(),
    ];
    delete_args.extend(archive.clone());
    delete_args.extend(["--before-sequence".to_string(), "5".to_string()]);
    let output = Command::new(admin_bin).args(delete_args).output()?;
    assert_success(&output, "delete detached");
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"affected_segments\": 1"));

    let storage_without_metadata = temp.path().join("archive-without-metadata");
    let metadata_missing = temp.path().join("metadata-missing");
    create_archive(&storage_without_metadata, &metadata_missing)?;
    std::fs::remove_file(metadata_missing.join("commit.idxlog"))?;
    let mut missing_commit_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "status".to_string(),
    ];
    missing_commit_args.extend(archive_args(
        "Test/Admin/MissingCommitLog",
        &storage_without_metadata,
        &metadata_missing,
    ));
    let output = Command::new(admin_bin).args(missing_commit_args).output()?;
    assert_failure_contains(
        &output,
        "commit.idxlog not found",
        "admin missing commit log",
    );

    Ok(())
}

#[test]
fn admin_and_control_report_validation_errors() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let missing_storage = temp.path().join("missing-archive");
    let missing_metadata = temp.path().join("missing-metadata");

    let mut missing_args = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "status".to_string(),
    ];
    missing_args.extend(archive_args(
        "Test/Admin/Missing",
        &missing_storage,
        &missing_metadata,
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_iox2-log-admin"))
        .args(missing_args)
        .output()?;
    assert_failure_contains(&output, "archive not found", "admin missing archive");

    let limit_output = Command::new(env!("CARGO_BIN_EXE_iox2-log-admin"))
        .args([
            "--format",
            "JSON",
            "inspect-commit-log",
            "--service",
            "",
            "--storage-path",
            missing_storage.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            missing_metadata.to_str().expect("utf-8 metadata path"),
            "--limit",
            "0",
        ])
        .output()?;
    assert_failure_contains(&limit_output, "--limit must be > 0", "admin zero limit");

    let empty_service = Command::new(env!("CARGO_BIN_EXE_iox2-log-control"))
        .args([
            "--format",
            "JSON",
            "status",
            "--service",
            "",
            "--timeout-ms",
            "1",
        ])
        .output()?;
    assert_failure_contains(
        &empty_service,
        "--service must not be empty",
        "control empty service",
    );

    let zero_timeout = Command::new(env!("CARGO_BIN_EXE_iox2-log-control"))
        .args([
            "--format",
            "JSON",
            "flush",
            "--service",
            "Test/Control/ZeroTimeout",
            "--timeout-ms",
            "0",
        ])
        .output()?;
    assert_failure_contains(
        &zero_timeout,
        "--timeout-ms must be greater than 0",
        "control zero timeout",
    );

    let unavailable = Command::new(env!("CARGO_BIN_EXE_iox2-log-control"))
        .args([
            "--format",
            "JSON",
            "status",
            "--service",
            "Test/Control/Unavailable",
            "--timeout-ms",
            "1",
        ])
        .output()?;
    assert_failure_contains(
        &unavailable,
        "recorder daemon control service",
        "control unavailable daemon",
    );

    Ok(())
}

#[test]
fn query_commands_cover_status_locators_windows_and_alignment()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query.sqlite");
    create_archive(&storage_path, &metadata_path)?;
    index_archive(&metadata_path, &db_path, "cam-a")?;

    let query_bin = env!("CARGO_BIN_EXE_iox2-log-query");

    let status = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "status",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
        ])
        .output()?;
    assert_success(&status, "query status");
    assert!(String::from_utf8_lossy(&status.stdout).contains("\"stream_count\": 1"));

    let sequence = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-sequence",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            "cam-a",
            "--at",
            "2",
        ])
        .output()?;
    assert_success(&sequence, "locate-sequence");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&sequence.stdout)?["sequence"],
        2
    );

    let locator = first_locator_from_range(&db_path, "cam-a")?;
    let locator_text = format!(
        "{}:{}:{}:{}",
        locator["segment_id"].as_u64().unwrap(),
        locator["segment_generation"].as_u64().unwrap(),
        locator["file_offset"].as_u64().unwrap(),
        locator["frame_len"].as_u64().unwrap()
    );
    let locator_output = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-locator",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            "cam-a",
            "--at",
            &locator_text,
        ])
        .output()?;
    assert_success(&locator_output, "locate-locator");
    assert!(String::from_utf8_lossy(&locator_output.stdout).contains("\"kind\":\"locator\""));

    let window = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-window",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            "cam-a",
            "--start-ns",
            "1000",
            "--end-ns",
            "4000",
            "--emit",
            "summary",
        ])
        .output()?;
    assert_success(&window, "locate-window summary");
    assert!(String::from_utf8_lossy(&window.stdout).contains("\"rows\": 4"));

    let align = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "align-window",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--streams",
            "cam-a",
            "--start-ns",
            "1000",
            "--end-ns",
            "4000",
            "--mode",
            "anchor",
            "--anchor-stream",
            "cam-a",
            "--emit",
            "summary",
        ])
        .output()?;
    assert_success(&align, "align-window summary");
    assert!(String::from_utf8_lossy(&align.stdout).contains("\"rows\": 4"));

    let empty_window = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-window",
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--stream-id",
            "cam-a",
            "--start-ns",
            "9000",
            "--end-ns",
            "10000",
            "--emit",
            "summary",
        ])
        .output()?;
    assert_success(&empty_window, "empty locate-window summary");
    assert!(String::from_utf8_lossy(&empty_window.stdout).contains("\"rows\": 0"));

    Ok(())
}

#[test]
fn query_commands_cover_reindex_latest_filtered_status_and_not_indexed()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query.sqlite");
    create_archive(&storage_path, &metadata_path)?;

    let query_bin = env!("CARGO_BIN_EXE_iox2-log-query");
    let db = db_path.to_str().expect("utf-8 db path");
    let metadata = metadata_path.to_str().expect("utf-8 metadata path");

    let initial = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            "cam-a",
            "--metadata-log-path",
            metadata,
            "--db-path",
            db,
            "--target",
            "latest",
        ])
        .output()?;
    assert_success(&initial, "query catch-up latest");
    assert!(String::from_utf8_lossy(&initial.stdout).contains("\"target\": \"latest\""));

    let reindex = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            "cam-a",
            "--metadata-log-path",
            metadata,
            "--db-path",
            db,
            "--reindex",
        ])
        .output()?;
    assert_success(&reindex, "query catch-up reindex");
    assert!(String::from_utf8_lossy(&reindex.stdout).contains("\"operation\": \"index-catch-up\""));
    assert!(
        String::from_utf8_lossy(&reindex.stdout).contains("\"last_indexed_commit_ordinal\": 4")
    );

    let status_a = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "status",
            "--db-path",
            db,
            "--stream-id",
            "cam-a",
        ])
        .output()?;
    assert_success(&status_a, "filtered status existing stream");
    assert!(String::from_utf8_lossy(&status_a.stdout).contains("\"stream_count\": 1"));
    assert!(String::from_utf8_lossy(&status_a.stdout).contains("\"stream_id\": \"cam-a\""));

    let status_missing = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "status",
            "--db-path",
            db,
            "--stream-id",
            "cam-b",
        ])
        .output()?;
    assert_success(&status_missing, "filtered status missing stream");
    assert!(String::from_utf8_lossy(&status_missing.stdout).contains("\"stream_count\": 0"));

    let unavailable = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-sequence",
            "--db-path",
            db,
            "--stream-id",
            "cam-a",
            "--at",
            "99",
        ])
        .output()?;
    assert_failure_contains(
        &unavailable,
        "sequence 99 is not available",
        "query missing indexed sequence",
    );

    let partial_db = temp.path().join("partial.sqlite");
    let partial_db_str = partial_db.to_str().expect("utf-8 partial db path");
    let mut partial_sink = SqliteMetadataSink::open_for_stream(&partial_db, "cam-partial")?;
    partial_sink.on_records(&[
        MetadataCommitRecord {
            log_id: [0x42; 16],
            commit_ordinal: 1,
            sequence: 1,
            locator: ArchiveLocator {
                segment_id: 1,
                segment_generation: 1,
                file_offset: 64,
                frame_len: 64,
            },
            frame_checksum: 0,
            event_time_ns: 1,
            commit_time_ns: 1,
            source_pattern: ArchiveSourcePattern::PublishSubscribe,
            source_service_id: 1,
            source_instance_id: 1,
            source_sequence: Some(1),
        },
        MetadataCommitRecord {
            log_id: [0x42; 16],
            commit_ordinal: 2,
            sequence: 2,
            locator: ArchiveLocator {
                segment_id: 1,
                segment_generation: 1,
                file_offset: 128,
                frame_len: 64,
            },
            frame_checksum: 0,
            event_time_ns: 2,
            commit_time_ns: 2,
            source_pattern: ArchiveSourcePattern::PublishSubscribe,
            source_service_id: 1,
            source_instance_id: 1,
            source_sequence: Some(2),
        },
    ])?;
    partial_sink.upsert_indexer_state(&SqliteIndexerState {
        stream_id: "cam-partial".to_string(),
        log_id: [0x42; 16],
        last_commit_ordinal: 4,
        last_indexed_commit_ordinal: 2,
        roll_file: "commit.idxlog".to_string(),
        byte_offset: 0,
        updated_at_ns: 1,
        schema_version: SQLITE_SCHEMA_VERSION,
    })?;

    let not_indexed = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "query",
            "locate-sequence",
            "--db-path",
            partial_db_str,
            "--stream-id",
            "cam-partial",
            "--at",
            "4",
        ])
        .output()?;
    assert_failure_contains(&not_indexed, "\"NotIndexedYet\"", "query not indexed yet");
    assert!(String::from_utf8_lossy(&not_indexed.stderr).contains("\"query_watermark\": 2"));

    let bad_max_records = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            "cam-a",
            "--metadata-log-path",
            metadata,
            "--db-path",
            db,
            "--max-records",
            "0",
        ])
        .output()?;
    assert_failure_contains(
        &bad_max_records,
        "--max-records must be > 0",
        "query zero max-records",
    );

    Ok(())
}

#[test]
fn query_commands_report_invalid_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query.sqlite");
    create_archive(&storage_path, &metadata_path)?;
    index_archive(&metadata_path, &db_path, "cam-a")?;

    let query_bin = env!("CARGO_BIN_EXE_iox2-log-query");
    let db = db_path.to_str().expect("utf-8 db path");

    for (args, expected, context) in [
        (
            vec![
                "--format",
                "JSON",
                "query",
                "locate-range",
                "--db-path",
                db,
                "--stream-id",
                "cam-a",
                "--from",
                "1",
                "--count",
                "0",
            ],
            "--count must be > 0",
            "locate-range zero count",
        ),
        (
            vec![
                "--format",
                "JSON",
                "query",
                "locate-range",
                "--db-path",
                db,
                "--stream-id",
                "cam-a",
                "--from",
                "1",
                "--count",
                "1",
                "--emit",
                "aligned",
            ],
            "--emit aligned is not supported by locate-range",
            "locate-range aligned",
        ),
        (
            vec![
                "--format",
                "JSON",
                "query",
                "locate-locator",
                "--db-path",
                db,
                "--stream-id",
                "cam-a",
                "--at",
                "bad",
            ],
            "invalid locator 'bad'",
            "locate-locator invalid locator",
        ),
        (
            vec![
                "--format",
                "JSON",
                "query",
                "locate-window",
                "--db-path",
                db,
                "--stream-id",
                "cam-a",
                "--start-ns",
                "2",
                "--end-ns",
                "1",
            ],
            "time window start must be <= end",
            "locate-window invalid range",
        ),
        (
            vec![
                "--format",
                "JSON",
                "query",
                "align-window",
                "--db-path",
                db,
                "--streams",
                "cam-a",
                "--start-ns",
                "1",
                "--end-ns",
                "2",
                "--mode",
                "grid",
            ],
            "--step-ns is required with --mode grid",
            "align-window missing step",
        ),
        (
            vec![
                "--format",
                "JSON",
                "query",
                "align-window",
                "--db-path",
                db,
                "--streams",
                "cam-a",
                "--start-ns",
                "1",
                "--end-ns",
                "2",
                "--mode",
                "anchor",
                "--anchor-stream",
                "cam-b",
            ],
            "--anchor-stream must be part of --streams",
            "align-window invalid anchor",
        ),
    ] {
        let output = Command::new(query_bin).args(args).output()?;
        assert_failure_contains(&output, expected, context);
    }

    let missing_db = temp.path().join("missing.sqlite");
    let missing = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "status",
            "--db-path",
            missing_db.to_str().expect("utf-8 db path"),
        ])
        .output()?;
    assert_failure_contains(&missing, "query database not found", "query missing db");

    Ok(())
}

#[test]
fn replay_commands_cover_error_modes_and_rate_validation() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;

    let replay_bin = env!("CARGO_BIN_EXE_iox2-log-replay");
    let mut base = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "replay".to_string(),
    ];
    base.extend(replay_archive_args(&storage_path, &metadata_path));
    base.extend(["--to".to_string(), "stdout".to_string()]);

    let mut recorded_args = base.clone();
    recorded_args.extend(["--rate".to_string(), "recorded".to_string()]);
    recorded_args.push("all".to_string());
    let output = Command::new(replay_bin).args(recorded_args).output()?;
    assert_success(&output, "replay recorded-rate all");
    assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 4);

    let mut fixed_args = base.clone();
    fixed_args.extend(["--rate".to_string(), "fixed".to_string()]);
    fixed_args.extend(["--messages-per-sec".to_string(), "1000000".to_string()]);
    fixed_args.extend(["sequence".to_string(), "--at".to_string(), "1".to_string()]);
    let output = Command::new(replay_bin).args(fixed_args).output()?;
    assert_success(&output, "replay fixed-rate sequence");
    assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 1);

    let mut missing_skip_args = base.clone();
    missing_skip_args.push("--skip-missing".to_string());
    missing_skip_args.extend(["--max-errors".to_string(), "1".to_string()]);
    missing_skip_args.extend(["sequence".to_string(), "--at".to_string(), "99".to_string()]);
    let output = Command::new(replay_bin).args(missing_skip_args).output()?;
    assert_success(&output, "replay skip missing sequence");
    assert!(String::from_utf8_lossy(&output.stderr).contains("\"skipped_missing\": 1"));

    for (args, expected, context) in [
        {
            let mut args = base.clone();
            args.extend(["locator".to_string(), "--at".to_string(), "bad".to_string()]);
            (args, "invalid locator 'bad'", "replay invalid locator")
        },
        {
            let mut args = base.clone();
            args.extend(["range".to_string(), "--from".to_string(), "1".to_string()]);
            args.extend(["--count".to_string(), "0".to_string()]);
            (args, "--count must be > 0", "replay zero range count")
        },
        {
            let mut args = base.clone();
            args.extend(["--rate".to_string(), "fixed".to_string()]);
            args.extend(["sequence".to_string(), "--at".to_string(), "1".to_string()]);
            (
                args,
                "--messages-per-sec is required when --rate=fixed",
                "replay fixed rate missing messages_per_sec",
            )
        },
        {
            let mut args = base.clone();
            args.extend(["--max-errors".to_string(), "0".to_string()]);
            args.extend(["sequence".to_string(), "--at".to_string(), "99".to_string()]);
            (args, "--max-errors must be > 0", "replay zero max errors")
        },
        {
            let mut args = base.clone();
            args.extend(["--service".to_string(), "not-valid-for-stdout".to_string()]);
            args.extend(["sequence".to_string(), "--at".to_string(), "1".to_string()]);
            (
                args,
                "--service is only valid with --to=publish-subscribe",
                "replay stdout service",
            )
        },
        {
            let mut args = base.clone();
            args.extend(["sequence".to_string(), "--at".to_string(), "99".to_string()]);
            (
                args,
                "sequence 99 is not available",
                "replay missing sequence",
            )
        },
    ] {
        let output = Command::new(replay_bin).args(&args).output()?;
        assert_failure_contains(&output, expected, context);
    }

    Ok(())
}

#[test]
fn cli_commands_report_corrupted_archive_failures() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;

    let corrupt_commit_storage = temp.path().join("corrupt-commit-archive");
    let corrupt_commit_metadata = temp.path().join("corrupt-commit-metadata");
    create_archive(&corrupt_commit_storage, &corrupt_commit_metadata)?;
    overwrite_bytes(
        &corrupt_commit_metadata.join("commit.idxlog"),
        ARCHIVE_FILE_HEADER_V1_LEN as u64,
        b"BAD!",
    );

    let replay_output = Command::new(env!("CARGO_BIN_EXE_iox2-log-replay"))
        .args([
            "--format",
            "JSON",
            "replay",
            "--storage-path",
            corrupt_commit_storage.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            corrupt_commit_metadata
                .to_str()
                .expect("utf-8 metadata path"),
            "--to",
            "stdout",
            "all",
        ])
        .output()?;
    assert_failure_contains(
        &replay_output,
        "invalid commit entry magic",
        "replay corrupt commit log",
    );

    let query_output = Command::new(env!("CARGO_BIN_EXE_iox2-log-query"))
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            "cam-corrupt",
            "--metadata-log-path",
            corrupt_commit_metadata
                .to_str()
                .expect("utf-8 metadata path"),
            "--db-path",
            temp.path()
                .join("corrupt-query.sqlite")
                .to_str()
                .expect("utf-8 db path"),
        ])
        .output()?;
    assert_failure_contains(
        &query_output,
        "invalid commit entry magic",
        "query corrupt commit log",
    );

    let corrupt_frame_storage = temp.path().join("corrupt-frame-archive");
    let corrupt_frame_metadata = temp.path().join("corrupt-frame-metadata");
    let mut recorder = ArchiveRecorderBuilder::new(&corrupt_frame_storage)
        .metadata_log_path(&corrupt_frame_metadata)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()?;
    let commit = recorder.append_publish_subscribe_record(PublishSubscribeRecordInput {
        event_time_ns: 1_000,
        source_service_id: 1,
        source_publisher_id: 1,
        source_sequence: Some(1),
        user_header: &[0x01],
        payload: &[0x02; 8],
    })?;
    recorder.finalize()?;
    let segment_path = corrupt_frame_storage.join(format!(
        "segments/segment-{}-g{}.data",
        commit.locator.segment_id, commit.locator.segment_generation
    ));
    overwrite_bytes(
        &segment_path,
        commit.locator.file_offset + TEST_FRAME_OFFSET_MAGIC,
        b"BAD!",
    );

    let replay_output = Command::new(env!("CARGO_BIN_EXE_iox2-log-replay"))
        .args([
            "--format",
            "JSON",
            "replay",
            "--storage-path",
            corrupt_frame_storage.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            corrupt_frame_metadata
                .to_str()
                .expect("utf-8 metadata path"),
            "--to",
            "stdout",
            "sequence",
            "--at",
            "1",
        ])
        .output()?;
    assert_failure_contains(
        &replay_output,
        "InvalidFrameMagic",
        "replay corrupt frame header",
    );

    Ok(())
}

#[test]
fn replay_selector_files_cover_csv_and_ndjson_validation() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;

    let replay_bin = env!("CARGO_BIN_EXE_iox2-log-replay");
    let mut base = vec![
        "--format".to_string(),
        "JSON".to_string(),
        "replay".to_string(),
    ];
    base.extend(replay_archive_args(&storage_path, &metadata_path));
    base.extend(["--to".to_string(), "stdout".to_string()]);

    let csv_path = temp.path().join("selectors.csv");
    std::fs::write(
        &csv_path,
        "kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len\n\
         sequence,1,,,,,,\n\
         range,,2,2,,,,\n",
    )?;
    let mut csv_args = base.clone();
    csv_args.extend(["selectors".to_string(), "--file".to_string()]);
    csv_args.push(csv_path.to_str().expect("utf-8 csv path").to_string());
    csv_args.extend(["--selector-format".to_string(), "csv".to_string()]);
    let output = Command::new(replay_bin).args(csv_args).output()?;
    assert_success(&output, "replay csv selector file");
    assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 3);

    let ndjson_path = temp.path().join("selectors.ndjson");
    std::fs::write(
        &ndjson_path,
        "\n{\"kind\":\"sequence\",\"sequence\":1}\n{\"kind\":\"range\",\"from\":2,\"count\":1}\n",
    )?;
    let mut ndjson_args = base.clone();
    ndjson_args.extend(["selectors".to_string(), "--file".to_string()]);
    ndjson_args.push(ndjson_path.to_str().expect("utf-8 ndjson path").to_string());
    ndjson_args.extend(["--selector-format".to_string(), "ndjson".to_string()]);
    let output = Command::new(replay_bin).args(ndjson_args).output()?;
    assert_success(&output, "replay ndjson selector file");
    assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 2);

    for (contents, format, expected, context) in [
        (
            "{\"kind\":\"sequence\"}\n",
            "ndjson",
            "missing field 'sequence'",
            "ndjson missing sequence",
        ),
        (
            "{\"kind\":\"range\",\"from\":1,\"count\":0}\n",
            "ndjson",
            "count must be > 0",
            "ndjson zero range count",
        ),
        (
            "{\"kind\":\"locator\",\"segment_id\":1,\"segment_generation\":0,\"file_offset\":0,\"frame_len\":1}\n",
            "ndjson",
            "invalid locator segment generation '0'",
            "ndjson invalid locator generation",
        ),
        (
            "{\"kind\":\"unknown\"}\n",
            "ndjson",
            "unsupported kind 'unknown'",
            "ndjson unsupported kind",
        ),
        (
            "kind,sequence\nsequence,1\n",
            "csv",
            "csv selector header is missing 'from'",
            "csv missing header",
        ),
        (
            "kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len\nsequence,abc,,,,,,\n",
            "csv",
            "invalid u64 in field 'sequence'",
            "csv invalid sequence",
        ),
        (
            "kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len\nrange,,1,0,,,,\n",
            "csv",
            "count must be > 0",
            "csv zero range count",
        ),
        (
            "kind,sequence,from,count,segment_id,segment_generation,file_offset,frame_len\nunknown,,,,,,,\n",
            "csv",
            "unsupported kind 'unknown'",
            "csv unsupported kind",
        ),
    ] {
        let selector_path = temp.path().join(format!("{context}.selectors"));
        std::fs::write(&selector_path, contents)?;
        let mut args = base.clone();
        args.extend(["selectors".to_string(), "--file".to_string()]);
        args.push(
            selector_path
                .to_str()
                .expect("utf-8 selector path")
                .to_string(),
        );
        args.extend(["--selector-format".to_string(), format.to_string()]);
        let output = Command::new(replay_bin).args(args).output()?;
        assert_failure_contains(&output, expected, context);
    }

    let missing_path = temp.path().join("does-not-exist.ndjson");
    let mut args = base.clone();
    args.extend(["selectors".to_string(), "--file".to_string()]);
    args.push(
        missing_path
            .to_str()
            .expect("utf-8 missing path")
            .to_string(),
    );
    let output = Command::new(replay_bin).args(args).output()?;
    assert_failure_contains(
        &output,
        "failed to open selector file",
        "missing selector file",
    );

    Ok(())
}

#[test]
fn query_index_run_emits_progress_until_stopped() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query-run.sqlite");
    create_archive(&storage_path, &metadata_path)?;

    let mut child = Command::new(env!("CARGO_BIN_EXE_iox2-log-query"))
        .args([
            "--format",
            "JSON",
            "index",
            "run",
            "--stream-id",
            "cam-run",
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
            "--poll-interval-ms",
            "10",
            "--batch-max-records",
            "2",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        let mut seen = String::new();
        for line in reader.lines() {
            let line = line.expect("query index-run stdout line");
            seen.push_str(&line);
            seen.push('\n');
            if line.contains("\"processed_records\"") {
                let _ = tx.send(seen);
                return;
            }
        }
        let _ = tx.send(seen);
    });

    let observed = match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(value) => value,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("timed out waiting for index-run output: {error}");
        }
    };
    let _ = child.kill();
    let _ = child.wait();

    assert!(observed.contains("\"operation\": \"index-run\""));
    assert!(observed.contains("\"processed_records\""));

    Ok(())
}

#[test]
fn control_cli_commands_succeed_against_live_recorder() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let service = unique_service_name("LogArchiveCli/Control/Source");

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let pubsub = node
        .service_builder(&ServiceName::new(&service)?)
        .publish_subscribe::<u64>()
        .open_or_create()?;
    let publisher = pubsub.publisher_builder().create()?;

    let recorder_node = format!(
        "iox2-log-archive-cli-recorder-{}",
        UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let recorder = Command::new(env!("CARGO_BIN_EXE_iox2-log-recorder"))
        .args([
            "--format",
            "JSON",
            "publish-subscribe",
            "--service",
            &service,
            "--node-name",
            &recorder_node,
            "--storage-path",
            storage_path.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--segment-bytes",
            "16384",
            "--spare-preallocated-segments",
            "0",
            "--segment-preallocate",
            "false",
            "--cycle-time-ms",
            "5",
            "--timeout-ms",
            "30000",
            "--flush-interval-ms",
            "10",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let status = wait_for_control_cli_status(&service)?;
    assert_eq!(status["operation"], "status");
    assert_eq!(status["is_paused"], false);

    let pause = control_cli(&service, "pause")?;
    assert_success(&pause, "control pause");
    let pause_json: serde_json::Value = serde_json::from_slice(&pause.stdout)?;
    assert_eq!(pause_json["operation"], "pause");
    assert_eq!(pause_json["is_paused"], true);

    for value in 1..=8u64 {
        publisher.send_copy(value)?;
    }

    let resume = control_cli(&service, "resume")?;
    assert_success(&resume, "control resume");
    let resume_json: serde_json::Value = serde_json::from_slice(&resume.stdout)?;
    assert_eq!(resume_json["operation"], "resume");
    assert_eq!(resume_json["is_paused"], false);

    for value in 10..=16u64 {
        publisher.send_copy(value)?;
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let flushed = loop {
        let flush = control_cli(&service, "flush")?;
        assert_success(&flush, "control flush");
        let flush_json: serde_json::Value = serde_json::from_slice(&flush.stdout)?;
        if flush_json["committed_records"].as_u64().unwrap_or(0) > 0 {
            break flush_json;
        }
        assert!(
            Instant::now() < deadline,
            "recorder did not commit live samples"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(flushed["operation"], "flush");

    let stop = control_cli(&service, "stop")?;
    assert_success(&stop, "control stop");
    let stop_json: serde_json::Value = serde_json::from_slice(&stop.stdout)?;
    assert_eq!(stop_json["operation"], "stop");

    let recorder = wait_for_child(recorder, "recorder CLI after control stop")?;
    assert_success(&recorder, "recorder CLI after control stop");
    assert!(String::from_utf8_lossy(&recorder.stdout).contains("\"stop_reason\": \"ControlStop\""));

    Ok(())
}

#[test]
fn replay_cli_publish_subscribe_replays_archive_to_service()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    create_archive(&storage_path, &metadata_path)?;

    let target_service = unique_service_name("LogArchiveCli/Replay/Target");
    let target_node = NodeBuilder::new().create::<ipc::Service>()?;
    let (payload_type, user_header_type) = byte_slice_service_details(2);
    let target_pubsub = unsafe {
        target_node
            .service_builder(&ServiceName::new(&target_service)?)
            .publish_subscribe::<[CustomPayloadMarker]>()
            .user_header::<CustomHeaderMarker>()
            .__internal_set_payload_type_details(&payload_type)
            .__internal_set_user_header_type_details(&user_header_type)
            .open_or_create()
    }?;
    let subscriber = target_pubsub.subscriber_builder().create()?;
    thread::sleep(Duration::from_millis(50));

    let replay = Command::new(env!("CARGO_BIN_EXE_iox2-log-replay"))
        .args([
            "--format",
            "JSON",
            "replay",
            "--storage-path",
            storage_path.to_str().expect("utf-8 storage path"),
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--to",
            "publish-subscribe",
            "--service",
            &target_service,
            "--rate",
            "fixed",
            "--messages-per-sec",
            "2",
            "--node-name",
            "iox2-log-archive-cli-replay-pubsub",
            "range",
            "--from",
            "1",
            "--count",
            "3",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let payload = receive_payload(&subscriber);
    let replay = wait_for_child(replay, "replay publish-subscribe")?;
    assert_success(&replay, "replay publish-subscribe");
    assert!(
        String::from_utf8_lossy(&replay.stdout).contains("\"destination\": \"publish-subscribe\"")
    );
    assert!(String::from_utf8_lossy(&replay.stdout).contains("\"emitted\": 3"));

    let first_byte = *payload.first().expect("non-empty replay payload");
    assert!((1..=3).contains(&first_byte));
    assert_eq!(payload.len(), first_byte as usize + 4);
    assert!(payload.iter().all(|byte| *byte == first_byte));

    Ok(())
}
