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

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;
use core::cell::RefCell;
use core::cmp::min;
use core::num::NonZeroUsize;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use super::common::*;
use super::storage::read_commit_entries;

/// Builder for [`ArchiveReplayer`].
pub struct ArchiveReplayerBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    replay_budget: ReplayBudget,
    verify_checksums: bool,
}

impl ArchiveReplayerBuilder {
    /// Creates a replayer builder.
    pub fn new(storage_path: &Path) -> Self {
        Self {
            storage_path: storage_path.to_path_buf(),
            metadata_log_path: None,
            replay_budget: ReplayBudget::default(),
            verify_checksums: true,
        }
    }

    /// Overrides metadata-log root path.
    pub fn metadata_log_path(mut self, value: &Path) -> Self {
        self.metadata_log_path = Some(value.to_path_buf());
        self
    }

    /// Sets replay budget limits.
    pub fn replay_budget(mut self, value: ReplayBudget) -> Self {
        self.replay_budget = value;
        self
    }

    /// Enables/disables checksum verification.
    pub fn verify_checksums(mut self, value: bool) -> Self {
        self.verify_checksums = value;
        self
    }

    /// Opens archive replayer.
    pub fn open(self) -> Result<ArchiveReplayer, ArchiveReplayError> {
        self.open_snapshot()
    }

    /// Opens live archive replayer.
    ///
    /// Live replay is explicit: snapshot replay remains deterministic, while
    /// callers that need recorder-following behavior can refresh visibility as
    /// new commit-log entries are appended.
    pub fn open_live(self) -> Result<ArchiveLiveReplayer, ArchiveReplayError> {
        Ok(ArchiveLiveReplayer {
            replayer: self.open_snapshot()?,
            live_pin: None,
        })
    }

    fn open_snapshot(self) -> Result<ArchiveReplayer, ArchiveReplayError> {
        if !self.verify_checksums {
            return Err(ArchiveReplayError::InvalidConfiguration(
                "checksum verification cannot be disabled",
            ));
        }

        let metadata_root = self
            .metadata_log_path
            .clone()
            .unwrap_or_else(|| self.storage_path.clone());
        let commit_log_path = metadata_root.join("commit.idxlog");
        if !commit_log_path.exists() {
            return Err(ArchiveReplayError::MissingCommitLog(commit_log_path));
        }

        let segments_path = self.storage_path.join("segments");
        let snapshot = load_replayer_snapshot(&segments_path, &commit_log_path)?;

        Ok(ArchiveReplayer {
            segments_path,
            metadata_log_path: metadata_root,
            commit_log_path,
            commit_log_entries: snapshot.commit_log_entries,
            index_by_sequence: snapshot.index_by_sequence,
            ordered_sequences: snapshot.ordered_sequences,
            cursor: 0,
            replay_budget: self.replay_budget,
            verify_checksums: self.verify_checksums,
            segment_file_cache: RefCell::new(BTreeMap::new()),
        })
    }
}

#[derive(Debug)]
struct ReplayerSnapshot {
    commit_log_entries: Vec<CommitEntry>,
    index_by_sequence: BTreeMap<u64, CommitEntry>,
    ordered_sequences: Vec<u64>,
}

fn load_replayer_snapshot(
    segments_path: &Path,
    commit_log_path: &Path,
) -> Result<ReplayerSnapshot, ArchiveReplayError> {
    let commit_log_entries = read_commit_entries(commit_log_path)?;
    let mut index_by_sequence = BTreeMap::new();
    for entry in &commit_log_entries {
        let hot_segment_path = segment_data_path(
            segments_path,
            entry.locator.segment_id,
            entry.locator.segment_generation,
        );
        if !hot_segment_path.exists() {
            continue;
        }
        if index_by_sequence.insert(entry.sequence, *entry).is_some() {
            return Err(ArchiveReplayError::DuplicateSequence(entry.sequence));
        }
    }
    let ordered_sequences: Vec<u64> = index_by_sequence.keys().copied().collect();

    Ok(ReplayerSnapshot {
        commit_log_entries,
        index_by_sequence,
        ordered_sequences,
    })
}

/// Replayer core for archived segment data.
#[derive(Debug)]
pub struct ArchiveReplayer {
    segments_path: PathBuf,
    metadata_log_path: PathBuf,
    commit_log_path: PathBuf,
    commit_log_entries: Vec<CommitEntry>,
    index_by_sequence: BTreeMap<u64, CommitEntry>,
    ordered_sequences: Vec<u64>,
    cursor: usize,
    replay_budget: ReplayBudget,
    verify_checksums: bool,
    segment_file_cache: RefCell<BTreeMap<(u64, u32), File>>,
}

impl ArchiveReplayer {
    fn refresh_snapshot(&mut self) -> Result<usize, ArchiveReplayError> {
        let previous_sequences: BTreeSet<u64> = self.index_by_sequence.keys().copied().collect();
        let snapshot = load_replayer_snapshot(&self.segments_path, &self.commit_log_path)?;
        let newly_visible_records = snapshot
            .ordered_sequences
            .iter()
            .filter(|sequence| !previous_sequences.contains(sequence))
            .count();
        self.commit_log_entries = snapshot.commit_log_entries;
        self.index_by_sequence = snapshot.index_by_sequence;
        self.ordered_sequences = snapshot.ordered_sequences;
        self.cursor = self.cursor.min(self.ordered_sequences.len());

        Ok(newly_visible_records)
    }

    fn live_status(&self) -> LiveReplayStatus {
        let last_visible_sequence = self.ordered_sequences.last().copied();
        let last_visible_commit_ordinal = self
            .index_by_sequence
            .values()
            .map(|entry| entry.commit_ordinal)
            .max()
            .unwrap_or(0);

        LiveReplayStatus {
            visible_records: self.ordered_sequences.len(),
            last_visible_sequence,
            last_visible_commit_ordinal,
        }
    }

    /// Returns current replay budget.
    pub fn replay_budget(&self) -> ReplayBudget {
        self.replay_budget
    }

    /// Sets replay budget.
    pub fn set_replay_budget(&mut self, value: ReplayBudget) {
        self.replay_budget = value;
    }

    /// Begins a snapshot pin that protects the current replay window from retention trim.
    pub fn begin_snapshot(&self) -> Result<ReplayPin, ArchiveReplayError> {
        let Some(sequence_start) = self.ordered_sequences.first().copied() else {
            return Err(ArchiveReplayError::InvalidPinState(
                "cannot create snapshot pin for empty archive",
            ));
        };
        let sequence_end =
            self.ordered_sequences
                .last()
                .copied()
                .ok_or(ArchiveReplayError::InvalidPinState(
                    "cannot resolve snapshot end sequence",
                ))?;

        self.begin_pin(sequence_start, sequence_end)
    }

    /// Begins a replay pin for the provided inclusive sequence range.
    pub fn begin_pin(
        &self,
        sequence_start: u64,
        sequence_end: u64,
    ) -> Result<ReplayPin, ArchiveReplayError> {
        if sequence_start > sequence_end {
            return Err(ArchiveReplayError::InvalidPinState(
                "replay pin requires sequence_start <= sequence_end",
            ));
        }

        let pin_dir = pin_directory(&self.metadata_log_path);
        fs::create_dir_all(&pin_dir).map_err(|source| ArchiveReplayError::Io {
            operation: "create replay pin directory",
            path: pin_dir.clone(),
            source,
        })?;

        for attempt in 0..1024u64 {
            let pin = ReplayPin {
                pin_id: now_ns().wrapping_add(attempt),
                sequence_start,
                sequence_end,
            };
            let path = pin_file_path(&self.metadata_log_path, pin);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.flush().map_err(|source| ArchiveReplayError::Io {
                        operation: "flush replay pin file",
                        path: path.clone(),
                        source,
                    })?;
                    return Ok(pin);
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => continue,
                Err(source) => {
                    return Err(ArchiveReplayError::Io {
                        operation: "create replay pin file",
                        path,
                        source,
                    });
                }
            }
        }

        Err(ArchiveReplayError::InvalidPinState(
            "unable to allocate unique replay pin id",
        ))
    }

    /// Releases a previously created snapshot/replay pin. Idempotent.
    pub fn release_snapshot(&self, pin: ReplayPin) -> Result<(), ArchiveReplayError> {
        let path = pin_file_path(&self.metadata_log_path, pin);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ArchiveReplayError::Io {
                operation: "remove replay pin file",
                path,
                source,
            }),
        }
    }

    /// Reads a record by source sequence.
    pub fn read_at_sequence(
        &self,
        sequence: u64,
    ) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        let Some(entry) = self.index_by_sequence.get(&sequence) else {
            return Ok(None);
        };
        self.read_frame_from_entry(entry)
    }

    /// Reads a record by source sequence into a reusable replay buffer.
    pub fn read_at_sequence_into<'a>(
        &self,
        sequence: u64,
        buffer: &'a mut ReplayFrameBuffer,
    ) -> Result<Option<ReplayedFrameView<'a>>, ArchiveReplayError> {
        let Some(entry) = self.index_by_sequence.get(&sequence) else {
            return Ok(None);
        };
        let frame = self.read_frame_into(entry.locator, buffer)?;
        if self.verify_checksums
            && entry.frame_checksum != 0
            && frame.frame_checksum != entry.frame_checksum
        {
            return Err(ArchiveReplayError::ChecksumMismatch {
                expected: entry.frame_checksum,
                actual: frame.frame_checksum,
                locator: frame.locator,
            });
        }
        Ok(Some(frame))
    }

    /// Reads multiple records starting from `start_sequence`.
    pub fn read_range(
        &self,
        start_sequence: u64,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let max_records = min(max_records.get(), self.replay_budget.max_records_per_call);
        let mut records = Vec::with_capacity(max_records);
        let mut accumulated_bytes = 0usize;
        for (_sequence, entry) in self.index_by_sequence.range(start_sequence..) {
            if records.len() >= max_records {
                break;
            }
            if accumulated_bytes + entry.locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !records.is_empty()
            {
                break;
            }
            let frame = self.read_frame_from_entry(entry)?.ok_or(
                ArchiveReplayError::InvalidCommitEntry("commit entry sequence missing in segment"),
            )?;
            accumulated_bytes += frame.locator.frame_len as usize;
            records.push(frame);
        }

        Ok(records)
    }

    /// Positions cursor to the first sequence `>= sequence`.
    pub fn seek(&mut self, sequence: u64) {
        self.cursor = lower_bound(&self.ordered_sequences, sequence);
    }

    /// Reads next record from cursor and advances it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        if self.cursor >= self.ordered_sequences.len() {
            return Ok(None);
        }

        let sequence = self.ordered_sequences[self.cursor];
        self.cursor += 1;
        self.read_at_sequence(sequence)
    }

    /// Reads next batch with replay budget limits.
    pub fn next_batch(
        &mut self,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let max_records = min(max_records.get(), self.replay_budget.max_records_per_call);
        let mut records = Vec::with_capacity(max_records);
        let mut accumulated_bytes = 0usize;

        while self.cursor < self.ordered_sequences.len() && records.len() < max_records {
            let sequence = self.ordered_sequences[self.cursor];
            let entry = self.index_by_sequence.get(&sequence).ok_or(
                ArchiveReplayError::InvalidCommitEntry("cursor points to missing sequence"),
            )?;
            if accumulated_bytes + entry.locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !records.is_empty()
            {
                break;
            }
            let frame = self.read_frame_from_entry(entry)?.ok_or(
                ArchiveReplayError::InvalidCommitEntry("commit entry sequence missing in segment"),
            )?;
            accumulated_bytes += frame.locator.frame_len as usize;
            records.push(frame);
            self.cursor += 1;
        }

        Ok(records)
    }

    /// Reads one frame via physical locator.
    pub fn read_at_locator(
        &self,
        locator: ArchiveLocator,
    ) -> Result<ReplayedFrame, ArchiveReplayError> {
        validate_locator_input(locator)?;
        self.read_frame(locator)
    }

    /// Reads one frame via physical locator into a reusable replay buffer.
    pub fn read_at_locator_into<'a>(
        &self,
        locator: ArchiveLocator,
        buffer: &'a mut ReplayFrameBuffer,
    ) -> Result<ReplayedFrameView<'a>, ArchiveReplayError> {
        validate_locator_input(locator)?;
        self.read_frame_into(locator, buffer)
    }

    /// Returns commit-log entries for CLI/admin introspection.
    ///
    /// Entries are ordered by `commit_ordinal`.
    pub fn inspect_commit_log_entries(
        &self,
        from_commit_ordinal: u64,
        max_entries: NonZeroUsize,
    ) -> Vec<ArchiveCommitLogEntry> {
        let mut result = Vec::with_capacity(max_entries.get());
        for entry in &self.commit_log_entries {
            if entry.commit_ordinal < from_commit_ordinal {
                continue;
            }
            if result.len() >= max_entries.get() {
                break;
            }

            let hot_segment_path = segment_data_path(
                &self.segments_path,
                entry.locator.segment_id,
                entry.locator.segment_generation,
            );
            result.push(ArchiveCommitLogEntry {
                commit_ordinal: entry.commit_ordinal,
                sequence: entry.sequence,
                locator: entry.locator,
                frame_checksum: entry.frame_checksum,
                event_time_ns: entry.event_time_ns,
                commit_time_ns: entry.commit_time_ns,
                source_pattern: entry.source_pattern,
                source_service_id: entry.source_service_id,
                source_instance_id: entry.source_instance_id,
                source_sequence: entry.source_sequence,
                hot_attached: hot_segment_path.exists(),
            });
        }

        result
    }

    /// Reads multiple frames via locators preserving caller-provided order.
    pub fn read_many_locators(
        &self,
        locators: &[ArchiveLocator],
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let limit = min(locators.len(), self.replay_budget.max_records_per_call);
        let mut result = Vec::with_capacity(limit);
        let mut accumulated_bytes = 0usize;

        for locator in locators.iter().take(limit) {
            validate_locator_input(*locator)?;
            if accumulated_bytes + locator.frame_len as usize
                > self.replay_budget.max_bytes_per_call
                && !result.is_empty()
            {
                break;
            }
            let frame = self.read_frame(*locator)?;
            accumulated_bytes += frame.locator.frame_len as usize;
            result.push(frame);
        }

        Ok(result)
    }

    fn read_frame_from_entry(
        &self,
        entry: &CommitEntry,
    ) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        let frame = self.read_frame(entry.locator)?;
        if self.verify_checksums
            && entry.frame_checksum != 0
            && frame.frame_checksum != entry.frame_checksum
        {
            return Err(ArchiveReplayError::ChecksumMismatch {
                expected: entry.frame_checksum,
                actual: frame.frame_checksum,
                locator: frame.locator,
            });
        }
        Ok(Some(frame))
    }

    fn read_frame(&self, locator: ArchiveLocator) -> Result<ReplayedFrame, ArchiveReplayError> {
        let mut buffer = ReplayFrameBuffer::new();
        let frame = self.read_frame_into(locator, &mut buffer)?;
        Ok(frame.to_owned_frame())
    }

    fn read_frame_into<'a>(
        &self,
        locator: ArchiveLocator,
        buffer: &'a mut ReplayFrameBuffer,
    ) -> Result<ReplayedFrameView<'a>, ArchiveReplayError> {
        let segment_key = (locator.segment_id, locator.segment_generation);
        let segment_path = segment_data_path(
            &self.segments_path,
            locator.segment_id,
            locator.segment_generation,
        );
        let mut file_cache = self.segment_file_cache.borrow_mut();
        let file = match file_cache.entry(segment_key) {
            alloc::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            alloc::collections::btree_map::Entry::Vacant(entry) => {
                if !segment_path.exists() {
                    return Err(ArchiveReplayError::MissingSegment(segment_path));
                }
                let file = File::open(&segment_path).map_err(|source| ArchiveReplayError::Io {
                    operation: "open segment data",
                    path: segment_path.clone(),
                    source,
                })?;
                entry.insert(file)
            }
        };
        file.seek(SeekFrom::Start(locator.file_offset))
            .map_err(|source| ArchiveReplayError::Io {
                operation: "seek segment frame",
                path: segment_path.clone(),
                source,
            })?;

        let mut frame_header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut frame_header)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read frame header",
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
            return Err(ArchiveReplayError::InvalidFrameMagic(decoded_magic));
        }

        let header_len = read_u16(&frame_header, FRAME_OFFSET_HEADER_LEN);
        if header_len as usize != FRAME_HEADER_LEN {
            return Err(ArchiveReplayError::InvalidFrameHeaderLength(header_len));
        }
        let flags = read_u16(&frame_header, FRAME_OFFSET_FLAGS);
        let frame_len = read_u32(&frame_header, FRAME_OFFSET_FRAME_LEN);
        if frame_len != locator.frame_len {
            return Err(ArchiveReplayError::InvalidFrameLength {
                expected: locator.frame_len,
                decoded: frame_len,
            });
        }

        let variable_len = frame_len as usize - FRAME_HEADER_LEN;
        buffer.variable.resize(variable_len, 0);
        file.read_exact(&mut buffer.variable)
            .map_err(|source| ArchiveReplayError::Io {
                operation: "read frame payload",
                path: segment_path.clone(),
                source,
            })?;

        let user_header_len = read_u32(&frame_header, FRAME_OFFSET_USER_HEADER_LEN) as usize;
        let payload_len = read_u32(&frame_header, FRAME_OFFSET_PAYLOAD_LEN) as usize;
        let payload_start = user_header_len;
        let payload_end = payload_start + payload_len;
        if payload_end > variable_len {
            return Err(ArchiveReplayError::InvalidCommitEntry(
                "frame user/payload lengths exceed frame bounds",
            ));
        }

        let frame_checksum = if self.verify_checksums && (flags & FRAME_FLAG_CHECKSUM_CRC32C) != 0 {
            let expected = read_u32(&frame_header, FRAME_OFFSET_CHECKSUM);
            let mut actual = crc32c::crc32c_append(0, &frame_header[..FRAME_OFFSET_CHECKSUM]);
            actual = crc32c::crc32c_append(actual, &[0u8; 4]);
            actual = crc32c::crc32c_append(
                actual,
                &frame_header[FRAME_OFFSET_CHECKSUM + 4..FRAME_HEADER_LEN],
            );
            actual = crc32c::crc32c_append(actual, &buffer.variable);
            if expected != actual {
                return Err(ArchiveReplayError::ChecksumMismatch {
                    expected,
                    actual,
                    locator,
                });
            }
            actual
        } else {
            0
        };

        Ok(ReplayedFrameView {
            commit_ordinal: read_u64(&frame_header, FRAME_OFFSET_COMMIT_ORDINAL),
            sequence: read_u64(&frame_header, FRAME_OFFSET_SEQUENCE),
            event_time_ns: read_u64(&frame_header, FRAME_OFFSET_EVENT_TIME_NS),
            commit_time_ns: read_u64(&frame_header, FRAME_OFFSET_COMMIT_TIME_NS),
            user_header: &buffer.variable[..user_header_len],
            payload: &buffer.variable[payload_start..payload_end],
            frame_checksum,
            locator,
        })
    }
}

/// Visible live replay boundary.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct LiveReplayStatus {
    /// Number of currently visible hot-attached records.
    pub visible_records: usize,
    /// Highest visible archive sequence.
    pub last_visible_sequence: Option<u64>,
    /// Highest visible commit ordinal.
    pub last_visible_commit_ordinal: u64,
}

/// Explicit live/follow replayer.
#[derive(Debug)]
pub struct ArchiveLiveReplayer {
    replayer: ArchiveReplayer,
    live_pin: Option<ReplayPin>,
}

impl ArchiveLiveReplayer {
    /// Returns the underlying replay budget.
    pub fn replay_budget(&self) -> ReplayBudget {
        self.replayer.replay_budget()
    }

    /// Sets replay budget.
    pub fn set_replay_budget(&mut self, value: ReplayBudget) {
        self.replayer.set_replay_budget(value);
    }

    /// Returns the current visible live replay boundary.
    pub fn status(&self) -> LiveReplayStatus {
        self.replayer.live_status()
    }

    /// Refreshes commit-log visibility and returns the count of newly visible records.
    pub fn refresh(&mut self) -> Result<usize, ArchiveReplayError> {
        let new_records = self.replayer.refresh_snapshot()?;
        self.refresh_live_pin()?;
        Ok(new_records)
    }

    /// Positions live cursor to the first visible sequence `>= sequence`.
    pub fn seek(&mut self, sequence: u64) {
        self.replayer.seek(sequence);
        let _ = self.refresh_live_pin();
    }

    /// Reads a visible record by archive sequence.
    pub fn read_at_sequence(
        &self,
        sequence: u64,
    ) -> Result<Option<ReplayedFrame>, ArchiveReplayError> {
        self.replayer.read_at_sequence(sequence)
    }

    /// Reads visible records starting from `start_sequence`.
    pub fn read_range(
        &self,
        start_sequence: u64,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        self.replayer.read_range(start_sequence, max_records)
    }

    /// Reads next currently visible batch from the live cursor.
    pub fn next_batch(
        &mut self,
        max_records: NonZeroUsize,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        self.replayer.next_batch(max_records)
    }

    /// Waits for the next visible batch from the live cursor.
    ///
    /// Returns an empty batch when `timeout` elapses before new records become
    /// visible. `poll_interval` values of zero are treated as a single refresh.
    pub fn next_live_batch(
        &mut self,
        max_records: NonZeroUsize,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<Vec<ReplayedFrame>, ArchiveReplayError> {
        let started = Instant::now();
        loop {
            self.refresh()?;
            let batch = self.next_batch(max_records)?;
            if !batch.is_empty() || started.elapsed() >= timeout || poll_interval.is_zero() {
                return Ok(batch);
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            thread::sleep(poll_interval.min(remaining));
        }
    }

    fn refresh_live_pin(&mut self) -> Result<(), ArchiveReplayError> {
        let Some(sequence_start) = self
            .replayer
            .ordered_sequences
            .get(self.replayer.cursor)
            .copied()
        else {
            self.release_live_pin()?;
            return Ok(());
        };
        let Some(sequence_end) = self.replayer.ordered_sequences.last().copied() else {
            self.release_live_pin()?;
            return Ok(());
        };

        if self.live_pin.is_some_and(|pin| {
            pin.sequence_start == sequence_start && pin.sequence_end == sequence_end
        }) {
            return Ok(());
        }

        let next_pin = self.replayer.begin_pin(sequence_start, sequence_end)?;
        self.release_live_pin()?;
        self.live_pin = Some(next_pin);
        Ok(())
    }

    fn release_live_pin(&mut self) -> Result<(), ArchiveReplayError> {
        let Some(pin) = self.live_pin.take() else {
            return Ok(());
        };
        self.replayer.release_snapshot(pin)
    }
}

impl Drop for ArchiveLiveReplayer {
    fn drop(&mut self) {
        let _ = self.release_live_pin();
    }
}

fn validate_locator_input(locator: ArchiveLocator) -> Result<(), ArchiveReplayError> {
    if locator.segment_generation == 0 {
        return Err(ArchiveReplayError::InvalidConfiguration(
            "locator segment_generation must be > 0",
        ));
    }
    Ok(())
}
