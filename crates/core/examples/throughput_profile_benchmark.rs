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

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use iox2_log_archive_core::log_archive::{
    ArchiveRecorderBuilder, AsyncIoBackend, ChecksumMode, PersistenceMode,
    PublishSubscribeRecordInput, RecorderProfile,
};

struct BenchmarkConfig {
    storage_path: PathBuf,
    metadata_path: PathBuf,
    records: u64,
    payload_bytes: usize,
    segment_bytes: usize,
    backend: AsyncIoBackend,
    profile: RecorderProfile,
}

fn parse_u64(value: Option<&String>, flag: &'static str) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    value
        .parse::<u64>()
        .map_err(|_| format!("invalid numeric value for {flag}: {value}"))
}

fn parse_usize(value: Option<&String>, flag: &'static str) -> Result<usize, String> {
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value for {flag}: {value}"))
}

fn parse_backend(value: Option<&String>) -> Result<AsyncIoBackend, String> {
    match value.map(|v| v.as_str()) {
        Some("auto") => Ok(AsyncIoBackend::IoUringPreferred),
        Some("blocking") => Ok(AsyncIoBackend::Blocking),
        Some("io_uring_required") => Ok(AsyncIoBackend::IoUringRequired),
        Some(other) => Err(format!(
            "invalid --backend value: {other} (expected auto|blocking|io_uring_required)"
        )),
        None => Ok(AsyncIoBackend::IoUringPreferred),
    }
}

fn parse_profile(value: Option<&String>) -> Result<RecorderProfile, String> {
    match value.map(|v| v.as_str()) {
        Some("durable") => Ok(RecorderProfile::Durable),
        Some("balanced") => Ok(RecorderProfile::Balanced),
        Some("throughput") => Ok(RecorderProfile::Throughput),
        Some("replay") => Ok(RecorderProfile::Replay),
        Some(other) => Err(format!(
            "invalid --profile value: {other} (expected durable|balanced|throughput|replay)"
        )),
        None => Ok(RecorderProfile::Throughput),
    }
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let args: Vec<String> = env::args().collect();
    let mut storage_path: Option<PathBuf> = None;
    let mut metadata_path: Option<PathBuf> = None;
    let mut records = 250_000u64;
    let mut payload_bytes = 4096usize;
    let mut segment_bytes = 64 * 1024 * 1024usize;
    let mut backend = AsyncIoBackend::IoUringPreferred;
    let mut profile = RecorderProfile::Throughput;

    let mut index = 1usize;
    while index < args.len() {
        let arg = &args[index];
        match arg.as_str() {
            "--storage-path" => {
                index += 1;
                storage_path = args.get(index).map(PathBuf::from);
            }
            "--metadata-log-path" => {
                index += 1;
                metadata_path = args.get(index).map(PathBuf::from);
            }
            "--records" => {
                index += 1;
                records = parse_u64(args.get(index), "--records")?;
            }
            "--payload-bytes" => {
                index += 1;
                payload_bytes = parse_usize(args.get(index), "--payload-bytes")?;
            }
            "--segment-bytes" => {
                index += 1;
                segment_bytes = parse_usize(args.get(index), "--segment-bytes")?;
            }
            "--backend" => {
                index += 1;
                backend = parse_backend(args.get(index))?;
            }
            "--profile" => {
                index += 1;
                profile = parse_profile(args.get(index))?;
            }
            "--help" | "-h" => {
                return Err(String::from(
                    "usage: throughput_profile_benchmark \
--storage-path <path> \
--metadata-log-path <path> \
[--records <u64>] \
[--payload-bytes <usize>] \
[--segment-bytes <usize>] \
[--backend auto|blocking|io_uring_required] \
[--profile durable|balanced|throughput|replay]",
                ));
            }
            _ => {
                return Err(format!("unknown argument: {arg}"));
            }
        }
        index += 1;
    }

    let storage_path = storage_path.ok_or_else(|| String::from("missing --storage-path"))?;
    let metadata_path = metadata_path.ok_or_else(|| String::from("missing --metadata-log-path"))?;
    if records == 0 {
        return Err(String::from("--records must be > 0"));
    }
    if payload_bytes == 0 {
        return Err(String::from("--payload-bytes must be > 0"));
    }
    if segment_bytes < 1024 {
        return Err(String::from("--segment-bytes must be >= 1024"));
    }

    Ok(BenchmarkConfig {
        storage_path,
        metadata_path,
        records,
        payload_bytes,
        segment_bytes,
        backend,
        profile,
    })
}

fn prepare_path(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path).map_err(|error| {
            format!(
                "failed to remove existing benchmark path {}: {error}",
                path.display()
            )
        })?;
    }
    fs::create_dir_all(path).map_err(|error| {
        format!(
            "failed to create benchmark path {}: {error}",
            path.display()
        )
    })
}

fn payload_seed(sequence: u64) -> u8 {
    (sequence as u8).wrapping_mul(31).wrapping_add(17)
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    prepare_path(&config.storage_path)?;
    prepare_path(&config.metadata_path)?;

    let mut payload = vec![0u8; config.payload_bytes];

    let mut recorder = ArchiveRecorderBuilder::new(&config.storage_path)
        .metadata_log_path(&config.metadata_path)
        .profile(config.profile)
        .segment_bytes(config.segment_bytes)
        .segment_preallocate(true)
        .spare_preallocated_segments(2)
        .persistence_mode(PersistenceMode::Async)
        .checksum_mode(ChecksumMode::Crc32c)
        .async_io_backend(config.backend)
        .create()
        .map_err(|error| format!("failed to create recorder: {error:?}"))?;

    let start = Instant::now();
    for sequence in 1..=config.records {
        payload.fill(payload_seed(sequence));
        let user_header = [sequence as u8, (sequence >> 8) as u8, 0xA5, 0x5A];
        recorder
            .append_publish_subscribe_record(PublishSubscribeRecordInput {
                event_time_ns: sequence * 100,
                source_service_id: 1,
                source_publisher_id: 1,
                source_sequence: Some(sequence),
                user_header: &user_header,
                payload: &payload,
            })
            .map_err(|error| format!("append failed at sequence {sequence}: {error:?}"))?;
    }
    recorder
        .finalize()
        .map_err(|error| format!("finalize failed: {error:?}"))?;
    let elapsed = start.elapsed();

    let elapsed_seconds = elapsed.as_secs_f64().max(1e-9);
    let stats = recorder.stats();
    let payload_bytes_per_second = stats.payload_bytes_committed as f64 / elapsed_seconds;
    let records_per_second = stats.committed_records as f64 / elapsed_seconds;

    println!(
        "{{\"records\":{},\"payload_bytes\":{},\"elapsed_seconds\":{:.6},\"records_per_second\":{:.3},\"payload_bytes_per_second\":{:.3},\"effective_backend\":\"{:?}\",\"configured_backend\":\"{:?}\",\"profile\":\"{:?}\",\"segment_bytes\":{},\"metadata_bytes_written\":{},\"data_bytes_written\":{},\"amplification_ratio\":{:.6}}}",
        stats.committed_records,
        stats.payload_bytes_committed,
        elapsed_seconds,
        records_per_second,
        payload_bytes_per_second,
        recorder.effective_async_io_backend(),
        recorder.configured_async_io_backend(),
        recorder.profile(),
        recorder.segment_bytes(),
        stats.metadata_bytes_written,
        stats.data_bytes_written,
        stats.amplification_ratio(),
    );

    Ok(())
}
