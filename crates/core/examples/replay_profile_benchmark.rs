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
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Instant;

use iox2_log_archive_core::log_archive::{ArchiveReplayerBuilder, ReplayBudget, ReplayedFrame};

#[derive(Debug, Clone)]
struct Config {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    mode: ReplayMode,
    batch_records: usize,
    batch_bytes: usize,
    max_records: Option<usize>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ReplayMode {
    NextBatch,
    Locators,
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    let replayer = ArchiveReplayerBuilder::new(&config.storage_path)
        .metadata_log_path_opt(config.metadata_log_path.as_deref())
        .replay_budget(ReplayBudget {
            max_records_per_call: config.batch_records,
            max_bytes_per_call: config.batch_bytes,
        })
        .open()
        .map_err(|error| format!("{error:?}"))?;

    let start = Instant::now();
    let mut records = 0usize;
    let mut payload_bytes = 0usize;
    let mut frame_bytes = 0usize;
    let mut checksum_accumulator = 0u32;

    match config.mode {
        ReplayMode::NextBatch => {
            let mut replayer = replayer;
            loop {
                let remaining = config
                    .max_records
                    .map(|max_records| max_records.saturating_sub(records))
                    .unwrap_or(config.batch_records);
                if remaining == 0 {
                    break;
                }
                let limit = NonZeroUsize::new(remaining.min(config.batch_records))
                    .ok_or_else(|| "batch limit must be non-zero".to_string())?;
                let batch = replayer
                    .next_batch(limit)
                    .map_err(|error| format!("{error:?}"))?;
                if batch.is_empty() {
                    break;
                }
                accumulate_batch(
                    &batch,
                    &mut records,
                    &mut payload_bytes,
                    &mut frame_bytes,
                    &mut checksum_accumulator,
                );
            }
        }
        ReplayMode::Locators => {
            let mut from_commit_ordinal = 1u64;
            loop {
                if let Some(max_records) = config.max_records {
                    if records >= max_records {
                        break;
                    }
                }
                let entries = replayer.inspect_commit_log_entries(
                    from_commit_ordinal,
                    NonZeroUsize::new(config.batch_records)
                        .ok_or_else(|| "batch limit must be non-zero".to_string())?,
                );
                if entries.is_empty() {
                    break;
                }
                let remaining = config
                    .max_records
                    .map(|max_records| max_records.saturating_sub(records))
                    .unwrap_or(entries.len());
                let locators = entries
                    .iter()
                    .take(remaining)
                    .map(|entry| entry.locator)
                    .collect::<Vec<_>>();
                if locators.is_empty() {
                    break;
                }
                let batch = replayer
                    .read_many_locators(&locators)
                    .map_err(|error| format!("{error:?}"))?;
                if batch.is_empty() {
                    break;
                }
                accumulate_batch(
                    &batch,
                    &mut records,
                    &mut payload_bytes,
                    &mut frame_bytes,
                    &mut checksum_accumulator,
                );
                from_commit_ordinal = entries
                    .last()
                    .map(|entry| entry.commit_ordinal.saturating_add(1))
                    .unwrap_or(from_commit_ordinal);
            }
        }
    }

    let elapsed_seconds = start.elapsed().as_secs_f64().max(1e-9);
    println!(
        "{{\"mode\":\"{:?}\",\"records\":{},\"payload_bytes\":{},\"frame_bytes\":{},\"elapsed_seconds\":{:.6},\"records_per_second\":{:.3},\"payload_bytes_per_second\":{:.3},\"frame_bytes_per_second\":{:.3},\"batch_records\":{},\"batch_bytes\":{},\"checksum_accumulator\":{}}}",
        config.mode,
        records,
        payload_bytes,
        frame_bytes,
        elapsed_seconds,
        records as f64 / elapsed_seconds,
        payload_bytes as f64 / elapsed_seconds,
        frame_bytes as f64 / elapsed_seconds,
        config.batch_records,
        config.batch_bytes,
        checksum_accumulator,
    );

    Ok(())
}

trait MetadataPathOpt {
    fn metadata_log_path_opt(self, value: Option<&std::path::Path>) -> Self;
}

impl MetadataPathOpt for ArchiveReplayerBuilder {
    fn metadata_log_path_opt(self, value: Option<&std::path::Path>) -> Self {
        if let Some(path) = value {
            self.metadata_log_path(path)
        } else {
            self
        }
    }
}

fn accumulate_batch(
    batch: &[ReplayedFrame],
    records: &mut usize,
    payload_bytes: &mut usize,
    frame_bytes: &mut usize,
    checksum_accumulator: &mut u32,
) {
    for frame in batch {
        *records += 1;
        *payload_bytes += frame.payload.len();
        *frame_bytes += frame.locator.frame_len as usize;
        *checksum_accumulator ^= frame.frame_checksum;
    }
}

fn parse_args() -> Result<Config, String> {
    let args = env::args().collect::<Vec<_>>();
    let mut storage_path = None;
    let mut metadata_log_path = None;
    let mut mode = ReplayMode::NextBatch;
    let mut batch_records = 256usize;
    let mut batch_bytes = 256 * 1024 * 1024usize;
    let mut max_records = None;

    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
            "--storage-path" => {
                index += 1;
                storage_path = args.get(index).map(PathBuf::from);
            }
            "--metadata-log-path" => {
                index += 1;
                metadata_log_path = args.get(index).map(PathBuf::from);
            }
            "--mode" => {
                index += 1;
                mode = parse_mode(args.get(index))?;
            }
            "--batch-records" => {
                index += 1;
                batch_records = parse_usize(args.get(index), "--batch-records")?;
            }
            "--batch-bytes" => {
                index += 1;
                batch_bytes = parse_usize(args.get(index), "--batch-bytes")?;
            }
            "--max-records" => {
                index += 1;
                max_records = Some(parse_usize(args.get(index), "--max-records")?);
            }
            "--help" | "-h" => {
                return Err(String::from(
                    "usage: replay_profile_benchmark \
--storage-path <path> \
[--metadata-log-path <path>] \
[--mode next-batch|locators] \
[--batch-records <usize>] \
[--batch-bytes <usize>] \
[--max-records <usize>]",
                ));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
    }

    let storage_path = storage_path.ok_or_else(|| String::from("missing --storage-path"))?;
    if batch_records == 0 {
        return Err(String::from("--batch-records must be > 0"));
    }
    if batch_bytes == 0 {
        return Err(String::from("--batch-bytes must be > 0"));
    }

    Ok(Config {
        storage_path,
        metadata_log_path,
        mode,
        batch_records,
        batch_bytes,
        max_records,
    })
}

fn parse_mode(value: Option<&String>) -> Result<ReplayMode, String> {
    match value.map(String::as_str) {
        Some("next-batch") => Ok(ReplayMode::NextBatch),
        Some("locators") => Ok(ReplayMode::Locators),
        Some(other) => Err(format!(
            "invalid --mode value: {other} (expected next-batch|locators)"
        )),
        None => Ok(ReplayMode::NextBatch),
    }
}

fn parse_usize(value: Option<&String>, flag: &'static str) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<usize>()
        .map_err(|_| format!("invalid numeric value for {flag}"))
}
