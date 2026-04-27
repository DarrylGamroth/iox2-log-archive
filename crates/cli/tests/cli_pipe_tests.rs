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

use std::io::Write;
use std::process::{Command, Stdio};

use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, ChecksumMode, PersistenceMode, PublishSubscribeRecordInput,
};

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

#[test]
fn query_expanded_selectors_pipe_to_replay_stdout() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let storage_path = temp.path().join("archive");
    let metadata_path = temp.path().join("metadata");
    let db_path = temp.path().join("query.sqlite");
    create_archive(&storage_path, &metadata_path)?;

    let query_bin = env!("CARGO_BIN_EXE_iox2-log-query");
    let replay_bin = env!("CARGO_BIN_EXE_iox2-log-replay");

    let index = Command::new(query_bin)
        .args([
            "--format",
            "JSON",
            "index",
            "catch-up",
            "--stream-id",
            "smoke",
            "--metadata-log-path",
            metadata_path.to_str().expect("utf-8 metadata path"),
            "--db-path",
            db_path.to_str().expect("utf-8 db path"),
        ])
        .output()?;
    assert!(
        index.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&index.stderr)
    );

    let selectors = Command::new(query_bin)
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
    assert!(
        selectors.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&selectors.stderr)
    );
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
    assert!(
        replay.status.success(),
        "replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
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
    assert!(
        replay.status.success(),
        "replay all failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
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
    assert!(
        help.status.success(),
        "help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );

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

    assert!(
        !output.status.success(),
        "recorder unexpectedly accepted zero borrowed-sample capacity"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--subscriber-max-borrowed-samples must be greater than 0"),
        "stderr did not contain validation error: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(())
}
