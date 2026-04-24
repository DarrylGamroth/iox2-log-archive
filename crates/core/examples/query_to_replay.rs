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

use std::error::Error;
use std::fs;
use std::path::PathBuf;

use iox2_log_archive_core::log_archive::{
    ArchiveMetadataIndexerBuilder, ArchiveRecorderBuilder, ArchiveReplayerBuilder, ChecksumMode,
    PersistenceMode, PublishSubscribeRecordInput,
};

fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::var_os("IOX2_LOG_ARCHIVE_EXAMPLE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("iox2-log-archive-query-replay"));
    let storage_path = root.join("archive");
    let metadata_path = root.join("metadata");

    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&root)?;

    let mut recorder = ArchiveRecorderBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .segment_bytes(1024)
        .segment_preallocate(false)
        .spare_preallocated_segments(0)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .create()?;

    for sequence in 1..=4u64 {
        let payload = vec![sequence as u8; (sequence as usize) + 4];
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

    let mut indexer = ArchiveMetadataIndexerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .enable_core_locator_index(true)
        .open()?;
    let indexed = indexer.catch_up_once()?;
    println!("indexed records: {indexed}");
    println!(
        "query watermark: {}",
        indexer.status().watermark.query_watermark()
    );

    let replayer = ArchiveReplayerBuilder::new(&storage_path)
        .metadata_log_path(&metadata_path)
        .open()?;
    let replayed = indexer.replay_by_sequence(&replayer, 3)?;
    println!(
        "replayed sequence={} payload_len={} locator={:?}",
        replayed.sequence,
        replayed.payload.len(),
        replayed.locator
    );

    Ok(())
}
