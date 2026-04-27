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
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iceoryx2::prelude::*;
use iox2_log_archive_core::log_archive::{
    AsyncIoBackend, ChecksumMode, PersistenceMode, RecorderProfile,
};
use iox2_log_archive_iceoryx2::{PubSubRecorderConfig, record_publish_subscribe};

const SYNTHETIC_SUBSCRIBER_MAX_BORROWED_SAMPLES: usize = 512;

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    storage_path: PathBuf,
    metadata_path: PathBuf,
    records: u64,
    payload_bytes: usize,
    segment_bytes: usize,
    backend: Option<AsyncIoBackend>,
    profile: RecorderProfile,
    publish_mode: PublishMode,
    timeout: Duration,
    checksum_mode: Option<ChecksumMode>,
    io_uring_queue_depth: Option<u32>,
    io_submit_batch_max: Option<u32>,
    io_cqe_batch_max: Option<u32>,
    subscriber_max_borrowed_samples: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PublishMode {
    Copy,
    Loan,
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    match config.payload_bytes {
        8 => run::<8>(config),
        64 => run::<64>(config),
        256 => run::<256>(config),
        1024 => run::<1024>(config),
        4096 => run::<4096>(config),
        16384 => run::<16384>(config),
        1048576 => run::<1048576>(config),
        other => Err(format!(
            "unsupported --payload-bytes {other}; expected one of 8|64|256|1024|4096|16384|1048576"
        )),
    }
}

fn run<const PAYLOAD_BYTES: usize>(config: BenchmarkConfig) -> Result<(), String> {
    prepare_path(&config.storage_path)?;
    prepare_path(&config.metadata_path)?;

    let token = unique_token();
    let service = format!(
        "LogArchiveBenchmark/SyntheticPubSub/{}/{}",
        std::process::id(),
        token
    );
    let node = NodeBuilder::new()
        .name(&NodeName::new(&format!("synthetic-pubsub-source-{token}")).map_err(to_string)?)
        .create::<ipc::Service>()
        .map_err(to_string)?;
    let pubsub = node
        .service_builder(&ServiceName::new(&service).map_err(to_string)?)
        .publish_subscribe::<[u8; PAYLOAD_BYTES]>()
        .enable_safe_overflow(false)
        .subscriber_max_buffer_size(config.subscriber_max_borrowed_samples)
        .subscriber_max_borrowed_samples(config.subscriber_max_borrowed_samples)
        .open_or_create()
        .map_err(to_string)?;
    let publisher = pubsub
        .publisher_builder()
        .unable_to_deliver_strategy(UnableToDeliverStrategy::Block)
        .create()
        .map_err(to_string)?;

    let recorder_config = PubSubRecorderConfig {
        service: service.clone(),
        node_name: format!("synthetic-pubsub-recorder-{token}"),
        storage_path: config.storage_path.clone(),
        metadata_log_path: config.metadata_path.clone(),
        profile: config.profile,
        persistence_mode: PersistenceMode::Async,
        segment_bytes: config.segment_bytes,
        spare_preallocated_segments: 2,
        segment_preallocate: true,
        max_disk_bytes: None,
        async_io_backend: config.backend,
        io_uring_queue_depth: config.io_uring_queue_depth,
        io_submit_batch_max: config.io_submit_batch_max,
        io_cqe_batch_max: config.io_cqe_batch_max,
        io_uring_register_files: None,
        checksum_mode: config.checksum_mode,
        subscriber_max_borrowed_samples: Some(config.subscriber_max_borrowed_samples),
        out_of_space_policy: None,
        metadata_log_roll_bytes: None,
        metadata_log_max_bytes: None,
        source_service_id: Some(1),
        cycle_time: Duration::from_millis(1),
        max_messages: Some(config.records),
        timeout: Some(config.timeout),
        flush_interval: Some(Duration::from_millis(100)),
        ack_level: None,
        shutdown_requested: None,
    };

    let publish_start = Instant::now();
    let recorder = thread::spawn(move || record_publish_subscribe(recorder_config));

    // Give the recorder subscriber a chance to connect before the hot publish loop.
    thread::sleep(Duration::from_millis(50));

    let mut sent = 0u64;
    let deadline = Instant::now() + config.timeout;
    while sent < config.records && !recorder.is_finished() && Instant::now() < deadline {
        match config.publish_mode {
            PublishMode::Copy => {
                let payload = [payload_seed(sent + 1); PAYLOAD_BYTES];
                publisher.send_copy(payload).map_err(to_string)?;
            }
            PublishMode::Loan => {
                let mut sample = publisher.loan_uninit().map_err(to_string)?;
                unsafe {
                    sample
                        .payload_mut()
                        .as_mut_ptr()
                        .write_bytes(payload_seed(sent + 1), 1);
                    sample.assume_init().send().map_err(to_string)?;
                }
            }
        }
        sent = sent.saturating_add(1);
        if sent % 4096 == 0 {
            thread::yield_now();
        }
    }

    let summary = recorder
        .join()
        .map_err(|_| "recorder thread panicked".to_string())?
        .map_err(|error| format!("recorder failed: {error:?}"))?;
    let publish_elapsed = publish_start.elapsed();
    let elapsed_seconds = summary.elapsed.as_secs_f64().max(1e-9);
    let wall_seconds = publish_elapsed.as_secs_f64().max(1e-9);
    let io_uring_avg_writes_per_submit = if summary.io_uring_submit_calls == 0 {
        0.0
    } else {
        summary.io_uring_submitted_writes as f64 / summary.io_uring_submit_calls as f64
    };

    println!(
        "{{\"records\":{},\"payload_bytes\":{},\"sent_messages\":{},\"elapsed_seconds\":{:.6},\"wall_seconds\":{:.6},\"records_per_second\":{:.3},\"payload_bytes_per_second\":{:.3},\"wall_records_per_second\":{:.3},\"wall_payload_bytes_per_second\":{:.3},\"effective_backend\":\"{:?}\",\"configured_backend\":\"{:?}\",\"profile\":\"{:?}\",\"publish_mode\":\"{:?}\",\"checksum_mode\":\"{:?}\",\"io_uring_queue_depth\":{},\"io_submit_batch_max\":{},\"io_cqe_batch_max\":{},\"subscriber_max_borrowed_samples\":{},\"external_payload_fast_path\":{},\"segment_bytes\":{},\"data_bytes_written\":{},\"metadata_bytes_written\":{},\"amplification_ratio\":{:.6},\"async_write_enqueued\":{},\"io_uring_submit_calls\":{},\"io_uring_submitted_writes\":{},\"io_uring_completed_writes\":{},\"io_uring_wait_calls\":{},\"io_uring_pending_high_watermark\":{},\"io_uring_avg_writes_per_submit\":{:.3},\"stop_reason\":\"{:?}\"}}",
        summary.committed_records,
        summary.payload_bytes_committed,
        sent,
        elapsed_seconds,
        wall_seconds,
        summary.committed_records as f64 / elapsed_seconds,
        summary.payload_bytes_committed as f64 / elapsed_seconds,
        summary.committed_records as f64 / wall_seconds,
        summary.payload_bytes_committed as f64 / wall_seconds,
        summary.effective_async_io_backend,
        summary.configured_async_io_backend,
        summary.profile,
        config.publish_mode,
        summary.checksum_mode,
        summary.io_uring_queue_depth,
        summary.io_submit_batch_max,
        summary.io_cqe_batch_max,
        summary.subscriber_max_borrowed_samples,
        summary.external_payload_fast_path,
        config.segment_bytes,
        summary.data_bytes_written,
        summary.metadata_bytes_written,
        summary.write_amplification_ratio,
        summary.async_write_enqueued,
        summary.io_uring_submit_calls,
        summary.io_uring_submitted_writes,
        summary.io_uring_completed_writes,
        summary.io_uring_wait_calls,
        summary.io_uring_pending_high_watermark,
        io_uring_avg_writes_per_submit,
        summary.stop_reason,
    );

    Ok(())
}

fn parse_args() -> Result<BenchmarkConfig, String> {
    let args = env::args().collect::<Vec<_>>();
    let mut storage_path = None;
    let mut metadata_path = None;
    let mut records = 100_000u64;
    let mut payload_bytes = 4096usize;
    let mut segment_bytes = 64 * 1024 * 1024usize;
    let mut backend = None;
    let mut profile = RecorderProfile::Throughput;
    let mut publish_mode = PublishMode::Copy;
    let mut timeout = Duration::from_secs(120);
    let mut checksum_mode = None;
    let mut io_uring_queue_depth = None;
    let mut io_submit_batch_max = None;
    let mut io_cqe_batch_max = None;
    let mut subscriber_max_borrowed_samples = SYNTHETIC_SUBSCRIBER_MAX_BORROWED_SAMPLES;

    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
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
            "--publish-mode" => {
                index += 1;
                publish_mode = parse_publish_mode(args.get(index))?;
            }
            "--timeout-seconds" => {
                index += 1;
                timeout = Duration::from_secs(parse_u64(args.get(index), "--timeout-seconds")?);
            }
            "--checksum-mode" => {
                index += 1;
                checksum_mode = Some(parse_checksum_mode(args.get(index))?);
            }
            "--io-uring-queue-depth" => {
                index += 1;
                io_uring_queue_depth = Some(parse_u32(args.get(index), "--io-uring-queue-depth")?);
            }
            "--io-submit-batch-max" => {
                index += 1;
                io_submit_batch_max = Some(parse_u32(args.get(index), "--io-submit-batch-max")?);
            }
            "--io-cqe-batch-max" => {
                index += 1;
                io_cqe_batch_max = Some(parse_u32(args.get(index), "--io-cqe-batch-max")?);
            }
            "--subscriber-max-borrowed-samples" => {
                index += 1;
                subscriber_max_borrowed_samples =
                    parse_usize(args.get(index), "--subscriber-max-borrowed-samples")?;
            }
            "--help" | "-h" => {
                return Err(String::from(
                    "usage: synthetic_pubsub_record_benchmark \
--storage-path <path> \
--metadata-log-path <path> \
[--records <u64>] \
[--payload-bytes 8|64|256|1024|4096|16384|1048576] \
[--segment-bytes <usize>] \
[--backend auto|blocking|io_uring_required] \
[--profile durable|balanced|throughput|replay] \
[--publish-mode copy|loan] \
[--timeout-seconds <u64>] \
[--checksum-mode crc32c|none] \
[--io-uring-queue-depth <u32>] \
[--io-submit-batch-max <u32>] \
[--io-cqe-batch-max <u32>] \
[--subscriber-max-borrowed-samples <usize>]",
                ));
            }
            other => return Err(format!("unknown argument: {other}")),
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
    if timeout.is_zero() {
        return Err(String::from("--timeout-seconds must be > 0"));
    }
    if subscriber_max_borrowed_samples == 0 {
        return Err(String::from(
            "--subscriber-max-borrowed-samples must be > 0",
        ));
    }

    Ok(BenchmarkConfig {
        storage_path,
        metadata_path,
        records,
        payload_bytes,
        segment_bytes,
        backend,
        profile,
        publish_mode,
        timeout,
        checksum_mode,
        io_uring_queue_depth,
        io_submit_batch_max,
        io_cqe_batch_max,
        subscriber_max_borrowed_samples,
    })
}

fn parse_u64(value: Option<&String>, flag: &'static str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<u64>()
        .map_err(|_| format!("invalid numeric value for {flag}"))
}

fn parse_usize(value: Option<&String>, flag: &'static str) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value for {flag}"))
}

fn parse_u32(value: Option<&String>, flag: &'static str) -> Result<u32, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<u32>()
        .map_err(|_| format!("invalid numeric value for {flag}"))
}

fn parse_backend(value: Option<&String>) -> Result<Option<AsyncIoBackend>, String> {
    match value.map(String::as_str) {
        Some("auto") => Ok(Some(AsyncIoBackend::IoUringPreferred)),
        Some("blocking") => Ok(Some(AsyncIoBackend::Blocking)),
        Some("io_uring_required") => Ok(Some(AsyncIoBackend::IoUringRequired)),
        Some(other) => Err(format!(
            "invalid --backend value: {other} (expected auto|blocking|io_uring_required)"
        )),
        None => Ok(None),
    }
}

fn parse_profile(value: Option<&String>) -> Result<RecorderProfile, String> {
    match value.map(String::as_str) {
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

fn parse_publish_mode(value: Option<&String>) -> Result<PublishMode, String> {
    match value.map(String::as_str) {
        Some("copy") => Ok(PublishMode::Copy),
        Some("loan") => Ok(PublishMode::Loan),
        Some(other) => Err(format!(
            "invalid --publish-mode value: {other} (expected copy|loan)"
        )),
        None => Ok(PublishMode::Copy),
    }
}

fn parse_checksum_mode(value: Option<&String>) -> Result<ChecksumMode, String> {
    match value.map(String::as_str) {
        Some("crc32c") => Ok(ChecksumMode::Crc32c),
        Some("none") => Ok(ChecksumMode::None),
        Some(other) => Err(format!(
            "invalid --checksum-mode value: {other} (expected crc32c|none)"
        )),
        None => Ok(ChecksumMode::Crc32c),
    }
}

fn prepare_path(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|error| format!("failed to remove {}: {error}", path.display()))?;
    }
    fs::create_dir_all(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))
}

fn payload_seed(sequence: u64) -> u8 {
    (sequence as u8).wrapping_mul(31).wrapping_add(17)
}

fn unique_token() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn to_string(error: impl core::fmt::Debug) -> String {
    format!("{error:?}")
}
