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

use alloc::vec;
use alloc::vec::Vec;
use core::cmp::min;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::log_archive::{ARCHIVE_FILE_HEADER_V1_LEN, ArchiveFileHeaderV1, ArchiveFileKind};

use super::common::*;

pub(super) fn encode_commit_entry(entry: CommitEntry) -> [u8; COMMIT_ENTRY_LEN] {
    let mut bytes = [0u8; COMMIT_ENTRY_LEN];
    bytes[COMMIT_OFFSET_MAGIC..COMMIT_OFFSET_MAGIC + 4].copy_from_slice(&COMMIT_ENTRY_MAGIC);
    bytes[COMMIT_OFFSET_ENTRY_LEN..COMMIT_OFFSET_ENTRY_LEN + 2]
        .copy_from_slice(&(COMMIT_ENTRY_LEN as u16).to_le_bytes());
    let mut flags = 0u16;
    if entry.source_sequence.is_some() {
        flags |= COMMIT_FLAG_SOURCE_SEQUENCE_PRESENT;
    }
    bytes[COMMIT_OFFSET_FLAGS..COMMIT_OFFSET_FLAGS + 2].copy_from_slice(&flags.to_le_bytes());
    bytes[COMMIT_OFFSET_COMMIT_ORDINAL..COMMIT_OFFSET_COMMIT_ORDINAL + 8]
        .copy_from_slice(&entry.commit_ordinal.to_le_bytes());
    bytes[COMMIT_OFFSET_SEQUENCE..COMMIT_OFFSET_SEQUENCE + 8]
        .copy_from_slice(&entry.sequence.to_le_bytes());
    bytes[COMMIT_OFFSET_SEGMENT_ID..COMMIT_OFFSET_SEGMENT_ID + 8]
        .copy_from_slice(&entry.locator.segment_id.to_le_bytes());
    bytes[COMMIT_OFFSET_SEGMENT_GENERATION..COMMIT_OFFSET_SEGMENT_GENERATION + 4]
        .copy_from_slice(&entry.locator.segment_generation.to_le_bytes());
    bytes[COMMIT_OFFSET_FILE_OFFSET..COMMIT_OFFSET_FILE_OFFSET + 8]
        .copy_from_slice(&entry.locator.file_offset.to_le_bytes());
    bytes[COMMIT_OFFSET_FRAME_LEN..COMMIT_OFFSET_FRAME_LEN + 4]
        .copy_from_slice(&entry.locator.frame_len.to_le_bytes());
    bytes[COMMIT_OFFSET_FRAME_CHECKSUM..COMMIT_OFFSET_FRAME_CHECKSUM + 4]
        .copy_from_slice(&entry.frame_checksum.to_le_bytes());
    bytes[COMMIT_OFFSET_EVENT_TIME_NS..COMMIT_OFFSET_EVENT_TIME_NS + 8]
        .copy_from_slice(&entry.event_time_ns.to_le_bytes());
    bytes[COMMIT_OFFSET_COMMIT_TIME_NS..COMMIT_OFFSET_COMMIT_TIME_NS + 8]
        .copy_from_slice(&entry.commit_time_ns.to_le_bytes());
    bytes[COMMIT_OFFSET_SOURCE_SERVICE_ID..COMMIT_OFFSET_SOURCE_SERVICE_ID + 8]
        .copy_from_slice(&entry.source_service_id.to_le_bytes());
    bytes[COMMIT_OFFSET_SOURCE_INSTANCE_ID..COMMIT_OFFSET_SOURCE_INSTANCE_ID + 8]
        .copy_from_slice(&entry.source_instance_id.to_le_bytes());
    bytes[COMMIT_OFFSET_SOURCE_SEQUENCE..COMMIT_OFFSET_SOURCE_SEQUENCE + 8]
        .copy_from_slice(&entry.source_sequence.unwrap_or(0).to_le_bytes());
    bytes[COMMIT_OFFSET_SOURCE_PATTERN] = entry.source_pattern as u8;
    bytes[COMMIT_OFFSET_SOURCE_FLAGS] = if entry.source_sequence.is_some() {
        1
    } else {
        0
    };
    bytes
}

fn decode_commit_entry(bytes: &[u8]) -> Result<CommitEntry, &'static str> {
    if bytes.len() < COMMIT_ENTRY_PREFIX_LEN {
        return Err("commit entry too short");
    }

    let entry_len = read_u16(bytes, COMMIT_OFFSET_ENTRY_LEN) as usize;
    if entry_len != bytes.len() {
        return Err("commit entry length does not match payload");
    }
    if entry_len != COMMIT_ENTRY_LEN {
        return Err("invalid commit entry length");
    }

    let locator = ArchiveLocator {
        segment_id: read_u64(bytes, COMMIT_OFFSET_SEGMENT_ID),
        segment_generation: read_u32(bytes, COMMIT_OFFSET_SEGMENT_GENERATION),
        file_offset: read_u64(bytes, COMMIT_OFFSET_FILE_OFFSET),
        frame_len: read_u32(bytes, COMMIT_OFFSET_FRAME_LEN),
    };

    let event_time_ns = read_u64(bytes, COMMIT_OFFSET_EVENT_TIME_NS);
    let commit_time_ns = read_u64(bytes, COMMIT_OFFSET_COMMIT_TIME_NS);
    let source_service_id = read_u64(bytes, COMMIT_OFFSET_SOURCE_SERVICE_ID);
    let source_instance_id = read_u64(bytes, COMMIT_OFFSET_SOURCE_INSTANCE_ID);
    let source_pattern = ArchiveSourcePattern::try_from(bytes[COMMIT_OFFSET_SOURCE_PATTERN])
        .map_err(|_| "invalid commit entry source pattern")?;
    let flags = read_u16(bytes, COMMIT_OFFSET_FLAGS);
    let source_sequence = if (flags & COMMIT_FLAG_SOURCE_SEQUENCE_PRESENT) != 0 {
        Some(read_u64(bytes, COMMIT_OFFSET_SOURCE_SEQUENCE))
    } else {
        None
    };

    Ok(CommitEntry {
        commit_ordinal: read_u64(bytes, COMMIT_OFFSET_COMMIT_ORDINAL),
        sequence: read_u64(bytes, COMMIT_OFFSET_SEQUENCE),
        locator,
        frame_checksum: read_u32(bytes, COMMIT_OFFSET_FRAME_CHECKSUM),
        event_time_ns,
        commit_time_ns,
        source_pattern,
        source_service_id,
        source_instance_id,
        source_sequence,
    })
}

pub(super) fn preallocate_metadata_log(
    file: &mut File,
    commit_log_path: &Path,
    logical_end_offset: u64,
    preallocate_entries: usize,
    max_file_len: Option<u64>,
) -> Result<u64, ArchiveRecorderError> {
    let preallocate_bytes = (preallocate_entries.saturating_mul(COMMIT_ENTRY_LEN)) as u64;
    let mut target_len = logical_end_offset.checked_add(preallocate_bytes).ok_or(
        ArchiveRecorderError::InvalidConfiguration("metadata-log preallocation length overflow"),
    )?;
    if let Some(max_file_len) = max_file_len {
        target_len = target_len.min(max_file_len);
        if target_len < logical_end_offset {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "metadata-log preallocation target below logical end",
            ));
        }
    }
    file.set_len(target_len)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "preallocate commit idxlog",
            path: commit_log_path.to_path_buf(),
            source,
        })?;
    Ok(target_len)
}

pub(super) fn read_catalog_entries(
    path: &Path,
) -> Result<Vec<SegmentSummary>, ArchiveRecorderError> {
    let mut file = File::open(path).map_err(|source| ArchiveRecorderError::Io {
        operation: "open catalog",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read catalog header",
            path: path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != ArchiveFileKind::Catalog {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "catalog.bin has invalid file kind",
        ));
    }

    let file_len = file
        .metadata()
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read catalog metadata",
            path: path.to_path_buf(),
            source,
        })?
        .len() as usize;
    let remaining = file_len.saturating_sub(ARCHIVE_FILE_HEADER_V1_LEN);
    if remaining % SEGMENT_SUMMARY_LEN != 0 {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "catalog summary area is not aligned to segment summary length",
        ));
    }

    let mut summaries = Vec::with_capacity(remaining / SEGMENT_SUMMARY_LEN);
    for _ in 0..(remaining / SEGMENT_SUMMARY_LEN) {
        let mut summary_bytes = [0u8; SEGMENT_SUMMARY_LEN];
        file.read_exact(&mut summary_bytes)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "read catalog segment summary",
                path: path.to_path_buf(),
                source,
            })?;
        summaries.push(SegmentSummary::from_bytes(&summary_bytes));
    }

    Ok(summaries)
}

pub(super) fn list_data_segments(
    segments_path: &Path,
) -> Result<Vec<(u64, u32)>, ArchiveRecorderError> {
    let mut segments = Vec::new();
    let entries = fs::read_dir(segments_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "read segments directory",
        path: segments_path.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| ArchiveRecorderError::Io {
            operation: "read directory entry",
            path: segments_path.to_path_buf(),
            source,
        })?;
        let Some(file_name) = entry.file_name().to_str().map(|value| value.to_owned()) else {
            continue;
        };
        if let Some((segment_id, generation)) = parse_segment_data_filename(&file_name) {
            segments.push((segment_id, generation));
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

pub(super) fn parse_segment_data_filename(file_name: &str) -> Option<(u64, u32)> {
    let value = file_name.strip_prefix("segment-")?.strip_suffix(".data")?;
    let (segment_id, generation) = value.split_once("-g")?;
    Some((segment_id.parse().ok()?, generation.parse().ok()?))
}

pub(super) fn determine_active_segment_for_recovery(
    data_segments: &[(u64, u32)],
    catalog_summaries: &[SegmentSummary],
    commit_entries: &[CommitEntry],
    default_generation: u32,
    segments_path: &Path,
) -> (u64, u32) {
    if let Some(last_entry) = commit_entries.last() {
        let last_segment_id = last_entry.locator.segment_id;
        let generation = last_entry.locator.segment_generation;
        let segment_is_sealed =
            segment_meta_path(segments_path, last_segment_id, generation).exists();
        return if segment_is_sealed {
            (last_segment_id + 1, generation)
        } else {
            (last_segment_id, generation)
        };
    }

    for (segment_id, generation) in data_segments {
        let meta_path = segment_meta_path(segments_path, *segment_id, *generation);
        if !meta_path.exists() {
            return (*segment_id, *generation);
        }
    }

    let next_segment_id = catalog_summaries
        .iter()
        .map(|summary| summary.segment_id)
        .max()
        .or_else(|| {
            data_segments
                .iter()
                .map(|(segment_id, _)| *segment_id)
                .max()
        })
        .unwrap_or(0)
        + 1;
    let generation = data_segments
        .iter()
        .map(|(_, generation)| *generation)
        .max()
        .unwrap_or(default_generation);
    (next_segment_id, generation)
}

pub(super) fn recover_commit_log_entries(
    file: &mut File,
    commit_log_path: &Path,
    segments_path: &Path,
) -> Result<CommitLogRecoveryResult, ArchiveRecorderError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "seek commit idxlog for recovery",
            path: commit_log_path.to_path_buf(),
            source,
        })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read commit idxlog recovery header",
            path: commit_log_path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != ArchiveFileKind::CommitIdxLog {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "commit.idxlog has invalid file kind",
        ));
    }

    let file_len = file
        .metadata()
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read commit idxlog metadata for recovery",
            path: commit_log_path.to_path_buf(),
            source,
        })?
        .len();
    let mut logical_end_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    let mut entries = Vec::new();

    while logical_end_offset + COMMIT_ENTRY_PREFIX_LEN as u64 <= file_len {
        file.seek(SeekFrom::Start(logical_end_offset))
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "seek commit idxlog entry for recovery",
                path: commit_log_path.to_path_buf(),
                source,
            })?;
        let mut prefix = [0u8; COMMIT_ENTRY_PREFIX_LEN];
        file.read_exact(&mut prefix)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "read commit idxlog entry for recovery",
                path: commit_log_path.to_path_buf(),
                source,
            })?;

        if prefix.iter().all(|byte| *byte == 0) {
            break;
        }

        let magic = [
            prefix[COMMIT_OFFSET_MAGIC],
            prefix[COMMIT_OFFSET_MAGIC + 1],
            prefix[COMMIT_OFFSET_MAGIC + 2],
            prefix[COMMIT_OFFSET_MAGIC + 3],
        ];
        if magic != COMMIT_ENTRY_MAGIC {
            break;
        }
        let entry_len = read_u16(&prefix, COMMIT_OFFSET_ENTRY_LEN) as usize;
        if entry_len != COMMIT_ENTRY_LEN {
            break;
        }
        if logical_end_offset + entry_len as u64 > file_len {
            break;
        }

        let mut bytes = vec![0u8; entry_len];
        bytes[..COMMIT_ENTRY_PREFIX_LEN].copy_from_slice(&prefix);
        if entry_len > COMMIT_ENTRY_PREFIX_LEN {
            file.read_exact(&mut bytes[COMMIT_ENTRY_PREFIX_LEN..])
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "read commit idxlog entry payload for recovery",
                    path: commit_log_path.to_path_buf(),
                    source,
                })?;
        }

        let Ok(entry) = decode_commit_entry(&bytes) else {
            break;
        };
        if !locator_points_to_valid_frame(segments_path, entry.locator)? {
            break;
        }

        entries.push(entry);
        logical_end_offset += entry_len as u64;
    }

    let truncated_bytes = file_len.saturating_sub(logical_end_offset);
    if truncated_bytes > 0 {
        file.set_len(logical_end_offset)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "truncate commit idxlog recovery tail",
                path: commit_log_path.to_path_buf(),
                source,
            })?;
    }

    Ok(CommitLogRecoveryResult {
        entries,
        logical_end_offset,
        truncated_bytes,
    })
}

pub(super) fn locator_points_to_valid_frame(
    segments_path: &Path,
    locator: ArchiveLocator,
) -> Result<bool, ArchiveRecorderError> {
    let segment_path = segment_data_path(
        segments_path,
        locator.segment_id,
        locator.segment_generation,
    );
    if !segment_path.exists() {
        return Ok(false);
    }

    let mut file = File::open(&segment_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "open segment for commit-log recovery",
        path: segment_path.clone(),
        source,
    })?;
    let file_len = file
        .metadata()
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read segment metadata for commit-log recovery",
            path: segment_path.clone(),
            source,
        })?
        .len();
    if locator.frame_len < FRAME_HEADER_LEN as u32 {
        return Ok(false);
    }
    if locator.file_offset + locator.frame_len as u64 > file_len {
        return Ok(false);
    }

    file.seek(SeekFrom::Start(locator.file_offset))
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "seek segment for commit-log recovery",
            path: segment_path.clone(),
            source,
        })?;
    let mut frame_header = [0u8; FRAME_HEADER_LEN];
    file.read_exact(&mut frame_header)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read frame header for commit-log recovery",
            path: segment_path.clone(),
            source,
        })?;

    let decoded_magic = [
        frame_header[FRAME_OFFSET_MAGIC],
        frame_header[FRAME_OFFSET_MAGIC + 1],
        frame_header[FRAME_OFFSET_MAGIC + 2],
        frame_header[FRAME_OFFSET_MAGIC + 3],
    ];
    if decoded_magic != FRAME_MAGIC {
        return Ok(false);
    }
    let header_len = read_u16(&frame_header, FRAME_OFFSET_HEADER_LEN);
    if header_len as usize != FRAME_HEADER_LEN {
        return Ok(false);
    }
    let frame_len = read_u32(&frame_header, FRAME_OFFSET_FRAME_LEN);
    if frame_len != locator.frame_len {
        return Ok(false);
    }

    let flags = read_u16(&frame_header, FRAME_OFFSET_FLAGS);
    if (flags & FRAME_FLAG_CHECKSUM_CRC32C) != 0 {
        let mut frame_bytes = vec![0u8; frame_len as usize];
        frame_bytes[..FRAME_HEADER_LEN].copy_from_slice(&frame_header);
        file.read_exact(&mut frame_bytes[FRAME_HEADER_LEN..])
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "read frame bytes for commit-log recovery",
                path: segment_path.clone(),
                source,
            })?;
        let expected = read_u32(&frame_header, FRAME_OFFSET_CHECKSUM);
        frame_bytes[FRAME_OFFSET_CHECKSUM..FRAME_OFFSET_CHECKSUM + 4].fill(0);
        if crc32c::crc32c(&frame_bytes) != expected {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(super) fn scan_active_segment_tail(
    file: &mut File,
    segment_path: &Path,
    max_segment_len: u64,
) -> Result<SegmentTailScanResult, ArchiveRecorderError> {
    let original_len = file
        .metadata()
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read active segment metadata for recovery",
            path: segment_path.to_path_buf(),
            source,
        })?
        .len();
    let scan_limit = min(original_len, max_segment_len);

    let mut valid_end = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    while valid_end < scan_limit {
        let remaining = scan_limit - valid_end;
        if remaining < FRAME_HEADER_LEN as u64 {
            file.seek(SeekFrom::Start(valid_end))
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "seek active segment trailing bytes for recovery",
                    path: segment_path.to_path_buf(),
                    source,
                })?;
            let mut tail = vec![0u8; remaining as usize];
            file.read_exact(&mut tail)
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "read active segment trailing bytes for recovery",
                    path: segment_path.to_path_buf(),
                    source,
                })?;
            break;
        }

        file.seek(SeekFrom::Start(valid_end))
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "seek active segment frame for recovery",
                path: segment_path.to_path_buf(),
                source,
            })?;
        let mut frame_header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut frame_header)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "read active segment frame header for recovery",
                path: segment_path.to_path_buf(),
                source,
            })?;

        if frame_header.iter().all(|byte| *byte == 0) {
            break;
        }

        let decoded_magic = [
            frame_header[FRAME_OFFSET_MAGIC],
            frame_header[FRAME_OFFSET_MAGIC + 1],
            frame_header[FRAME_OFFSET_MAGIC + 2],
            frame_header[FRAME_OFFSET_MAGIC + 3],
        ];
        if decoded_magic != FRAME_MAGIC {
            break;
        }
        if read_u16(&frame_header, FRAME_OFFSET_HEADER_LEN) as usize != FRAME_HEADER_LEN {
            break;
        }

        let frame_len = read_u32(&frame_header, FRAME_OFFSET_FRAME_LEN);
        if frame_len < FRAME_HEADER_LEN as u32 || frame_len as usize % 8 != 0 {
            break;
        }
        if valid_end + frame_len as u64 > scan_limit {
            break;
        }

        let flags = read_u16(&frame_header, FRAME_OFFSET_FLAGS);
        if (flags & FRAME_FLAG_CHECKSUM_CRC32C) != 0 {
            let mut frame_bytes = vec![0u8; frame_len as usize];
            frame_bytes[..FRAME_HEADER_LEN].copy_from_slice(&frame_header);
            file.read_exact(&mut frame_bytes[FRAME_HEADER_LEN..])
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "read active segment frame bytes for recovery",
                    path: segment_path.to_path_buf(),
                    source,
                })?;
            let expected = read_u32(&frame_header, FRAME_OFFSET_CHECKSUM);
            frame_bytes[FRAME_OFFSET_CHECKSUM..FRAME_OFFSET_CHECKSUM + 4].fill(0);
            if crc32c::crc32c(&frame_bytes) != expected {
                break;
            }
        }

        valid_end += frame_len as u64;
    }

    Ok(SegmentTailScanResult {
        original_len,
        valid_end,
    })
}

pub(super) fn read_commit_entries(path: &Path) -> Result<Vec<CommitEntry>, ArchiveReplayError> {
    if path.file_name().and_then(|value| value.to_str()) == Some("commit.idxlog") {
        let metadata_root = path.parent().unwrap_or(Path::new("."));
        let commit_logs =
            list_commit_log_paths(metadata_root).map_err(|source| ArchiveReplayError::Io {
                operation: "list commit idxlog files",
                path: metadata_root.to_path_buf(),
                source,
            })?;
        if commit_logs.is_empty() {
            return Err(ArchiveReplayError::MissingCommitLog(path.to_path_buf()));
        }

        let mut entries = Vec::new();
        let mut last_commit_ordinal = 0u64;
        for commit_log in commit_logs {
            let part = read_commit_entries_single(&commit_log)?;
            for entry in part {
                if last_commit_ordinal != 0 && entry.commit_ordinal <= last_commit_ordinal {
                    return Err(ArchiveReplayError::InvalidCommitEntry(
                        "commit idxlog files are not strictly ordered by commit ordinal",
                    ));
                }
                last_commit_ordinal = entry.commit_ordinal;
                entries.push(entry);
            }
        }

        return Ok(entries);
    }

    read_commit_entries_single(path)
}

fn read_commit_entries_single(path: &Path) -> Result<Vec<CommitEntry>, ArchiveReplayError> {
    let mut file = File::open(path).map_err(|source| ArchiveReplayError::Io {
        operation: "open commit idxlog",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveReplayError::Io {
            operation: "read commit idxlog header",
            path: path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != ArchiveFileKind::CommitIdxLog {
        return Err(ArchiveReplayError::InvalidCommitEntry(
            "commit.idxlog has invalid file kind",
        ));
    }

    let file_len = file
        .metadata()
        .map_err(|source| ArchiveReplayError::Io {
            operation: "read commit idxlog metadata",
            path: path.to_path_buf(),
            source,
        })?
        .len() as usize;
    let mut position = ARCHIVE_FILE_HEADER_V1_LEN;
    let mut entries = Vec::new();

    while position < file_len {
        let remaining = file_len - position;
        if remaining < COMMIT_ENTRY_PREFIX_LEN {
            let mut tail = vec![0u8; remaining];
            file.read_exact(&mut tail)
                .map_err(|source| ArchiveReplayError::Io {
                    operation: "read commit idxlog tail bytes",
                    path: path.to_path_buf(),
                    source,
                })?;
            if tail.iter().any(|byte| *byte != 0) {
                return Err(ArchiveReplayError::InvalidCommitEntry(
                    "commit.idxlog contains trailing non-zero bytes",
                ));
            }
            break;
        }

        let mut prefix = [0u8; COMMIT_ENTRY_PREFIX_LEN];
        file.read_exact(&mut prefix)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read commit idxlog entry prefix",
                path: path.to_path_buf(),
                source,
            })?;

        if prefix.iter().all(|byte| *byte == 0) {
            let remaining_tail = remaining - COMMIT_ENTRY_PREFIX_LEN;
            if remaining_tail > 0 {
                let mut tail = vec![0u8; remaining_tail];
                file.read_exact(&mut tail)
                    .map_err(|source| ArchiveReplayError::Io {
                        operation: "read commit idxlog zero tail",
                        path: path.to_path_buf(),
                        source,
                    })?;
                if tail.iter().any(|byte| *byte != 0) {
                    return Err(ArchiveReplayError::InvalidCommitEntry(
                        "commit.idxlog contains non-zero bytes after zero-tail marker",
                    ));
                }
            }
            break;
        }

        let magic = [
            prefix[COMMIT_OFFSET_MAGIC],
            prefix[COMMIT_OFFSET_MAGIC + 1],
            prefix[COMMIT_OFFSET_MAGIC + 2],
            prefix[COMMIT_OFFSET_MAGIC + 3],
        ];
        if magic != COMMIT_ENTRY_MAGIC {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "invalid commit entry magic",
            ));
        }

        let entry_len = read_u16(&prefix, COMMIT_OFFSET_ENTRY_LEN) as usize;
        if entry_len != COMMIT_ENTRY_LEN {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "invalid commit entry length",
            ));
        }
        if entry_len > remaining {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "commit.idxlog entry exceeds available bytes",
            ));
        }

        let mut bytes = vec![0u8; entry_len];
        bytes[..COMMIT_ENTRY_PREFIX_LEN].copy_from_slice(&prefix);
        if entry_len > COMMIT_ENTRY_PREFIX_LEN {
            file.read_exact(&mut bytes[COMMIT_ENTRY_PREFIX_LEN..])
                .map_err(|source| ArchiveReplayError::Io {
                    operation: "read commit idxlog entry payload",
                    path: path.to_path_buf(),
                    source,
                })?;
        }
        let entry = decode_commit_entry(&bytes).map_err(ArchiveReplayError::InvalidCommitEntry)?;
        entries.push(entry);
        position += entry_len;
    }

    Ok(entries)
}

pub(super) fn list_commit_log_paths(metadata_root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut rolled = Vec::<(u64, PathBuf)>::new();
    let mut active = None::<PathBuf>;

    let entries = fs::read_dir(metadata_root)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name == "commit.idxlog" {
            active = Some(path);
            continue;
        }
        if let Some(index) = parse_rolled_commit_log_index(name) {
            rolled.push((index, path));
        }
    }

    rolled.sort_by_key(|(index, _)| *index);
    let mut result = rolled.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
    if let Some(active) = active {
        result.push(active);
    }

    Ok(result)
}

pub(super) fn parse_rolled_commit_log_index(file_name: &str) -> Option<u64> {
    let value = file_name.strip_prefix("commit-")?.strip_suffix(".idxlog")?;
    value.parse::<u64>().ok()
}

pub(super) fn commit_log_roll_path(metadata_root: &Path, roll_index: u64) -> PathBuf {
    metadata_root.join(format!("commit-{roll_index:020}.idxlog"))
}

pub(super) fn write_archive_header(
    file: &mut File,
    path: &Path,
    file_kind: ArchiveFileKind,
    log_id: [u8; 16],
    segment_id: u64,
    segment_generation: u32,
) -> Result<(), ArchiveRecorderError> {
    let mut header = ArchiveFileHeaderV1::new(file_kind);
    header.log_id = log_id;
    header.created_at_ns = now_ns();
    header.segment_id = segment_id;
    header.segment_generation = segment_generation;
    let bytes = header.to_bytes()?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "seek archive header",
            path: path.to_path_buf(),
            source,
        })?;
    file.write_all(&bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "write archive header",
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

pub(super) fn create_new_file(path: &Path) -> Result<(File, PathBuf), ArchiveRecorderError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "create file",
            path: path.to_path_buf(),
            source,
        })?;
    Ok((file, path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_commit_entry_roundtrip_preserves_alignment_metadata_fields() {
        let entry = CommitEntry {
            commit_ordinal: 101,
            sequence: 202,
            locator: ArchiveLocator {
                segment_id: 303,
                segment_generation: 404,
                file_offset: 505,
                frame_len: 606,
            },
            frame_checksum: 0xDEADBEEF,
            event_time_ns: 707,
            commit_time_ns: 808,
            source_pattern: ArchiveSourcePattern::Pipeline,
            source_service_id: 909,
            source_instance_id: 1001,
            source_sequence: Some(1102),
        };

        let bytes = encode_commit_entry(entry);
        let decoded = decode_commit_entry(&bytes).unwrap();
        assert_eq!(decoded.commit_ordinal, entry.commit_ordinal);
        assert_eq!(decoded.sequence, entry.sequence);
        assert_eq!(decoded.locator, entry.locator);
        assert_eq!(decoded.frame_checksum, entry.frame_checksum);
        assert_eq!(decoded.event_time_ns, entry.event_time_ns);
        assert_eq!(decoded.commit_time_ns, entry.commit_time_ns);
        assert_eq!(decoded.source_pattern, entry.source_pattern);
        assert_eq!(decoded.source_service_id, entry.source_service_id);
        assert_eq!(decoded.source_instance_id, entry.source_instance_id);
        assert_eq!(decoded.source_sequence, entry.source_sequence);
    }
}
