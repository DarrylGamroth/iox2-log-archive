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

use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::log_archive::{ARCHIVE_FILE_HEADER_V1_LEN, ArchiveFileHeaderV1, ArchiveFileKind};

use super::backend::RecorderIoBackend;
use super::common::*;
use super::storage::*;

const DEFAULT_METADATA_LOG_ROLL_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_METADATA_LOG_MAX_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const ZERO_LOG_ID: [u8; 16] = [0u8; 16];

#[derive(Debug, Clone, Copy)]
struct RecordInput<'a> {
    sequence: u64,
    event_time_ns: u64,
    user_header: &'a [u8],
    payload: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
struct ProfileDefaults {
    segment_bytes: usize,
    spare_preallocated_segments: usize,
    persistence_mode: PersistenceMode,
    async_io_backend: AsyncIoBackend,
    io_uring_queue_depth: u32,
    io_submit_batch_max: u32,
    io_cqe_batch_max: u32,
    io_uring_register_files: bool,
    metadata_log_roll_bytes: u64,
    metadata_log_max_bytes: u64,
}

fn profile_defaults(profile: RecorderProfile) -> ProfileDefaults {
    match profile {
        RecorderProfile::Durable => ProfileDefaults {
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            persistence_mode: PersistenceMode::Sync,
            async_io_backend: AsyncIoBackend::IoUringPreferred,
            io_uring_queue_depth: 256,
            io_submit_batch_max: 64,
            io_cqe_batch_max: 128,
            io_uring_register_files: true,
            metadata_log_roll_bytes: DEFAULT_METADATA_LOG_ROLL_BYTES,
            metadata_log_max_bytes: DEFAULT_METADATA_LOG_MAX_BYTES,
        },
        RecorderProfile::Balanced => ProfileDefaults {
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            persistence_mode: PersistenceMode::Async,
            async_io_backend: AsyncIoBackend::IoUringPreferred,
            io_uring_queue_depth: 256,
            io_submit_batch_max: 64,
            io_cqe_batch_max: 128,
            io_uring_register_files: true,
            metadata_log_roll_bytes: DEFAULT_METADATA_LOG_ROLL_BYTES,
            metadata_log_max_bytes: DEFAULT_METADATA_LOG_MAX_BYTES,
        },
        RecorderProfile::Throughput => ProfileDefaults {
            segment_bytes: 1024 * 1024 * 1024,
            spare_preallocated_segments: 2,
            persistence_mode: PersistenceMode::Async,
            async_io_backend: AsyncIoBackend::IoUringRequired,
            io_uring_queue_depth: 1024,
            io_submit_batch_max: 256,
            io_cqe_batch_max: 512,
            io_uring_register_files: true,
            metadata_log_roll_bytes: 4 * 1024 * 1024 * 1024,
            metadata_log_max_bytes: DEFAULT_METADATA_LOG_MAX_BYTES,
        },
        RecorderProfile::Replay => ProfileDefaults {
            segment_bytes: 256 * 1024 * 1024,
            spare_preallocated_segments: 1,
            persistence_mode: PersistenceMode::Async,
            async_io_backend: AsyncIoBackend::IoUringPreferred,
            io_uring_queue_depth: 256,
            io_submit_batch_max: 64,
            io_cqe_batch_max: 128,
            io_uring_register_files: true,
            metadata_log_roll_bytes: DEFAULT_METADATA_LOG_ROLL_BYTES,
            metadata_log_max_bytes: DEFAULT_METADATA_LOG_MAX_BYTES,
        },
    }
}

/// Builder for [`ArchiveRecorder`].
pub struct ArchiveRecorderBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    profile: RecorderProfile,
    segment_bytes: usize,
    segment_bytes_overridden: bool,
    segment_preallocate: bool,
    spare_preallocated_segments: usize,
    spare_preallocated_segments_overridden: bool,
    metadata_log_preallocate_entries: usize,
    persistence_mode: PersistenceMode,
    persistence_mode_overridden: bool,
    async_io_backend: AsyncIoBackend,
    async_io_backend_overridden: bool,
    io_uring_queue_depth: u32,
    io_uring_queue_depth_overridden: bool,
    io_submit_batch_max: u32,
    io_submit_batch_max_overridden: bool,
    io_cqe_batch_max: u32,
    io_cqe_batch_max_overridden: bool,
    io_uring_register_files: bool,
    io_uring_register_files_overridden: bool,
    checksum_mode: ChecksumMode,
    out_of_space_policy: OutOfSpacePolicy,
    metadata_log_roll_bytes: u64,
    metadata_log_roll_bytes_overridden: bool,
    metadata_log_max_bytes: u64,
    metadata_log_max_bytes_overridden: bool,
    max_disk_bytes: Option<u64>,
    log_id: [u8; 16],
    segment_generation: u32,
}

impl ArchiveRecorderBuilder {
    /// Creates a builder with throughput-oriented defaults.
    pub fn new(storage_path: &Path) -> Self {
        let defaults = profile_defaults(RecorderProfile::Balanced);
        Self {
            storage_path: storage_path.to_path_buf(),
            metadata_log_path: None,
            profile: RecorderProfile::Balanced,
            segment_bytes: defaults.segment_bytes,
            segment_bytes_overridden: false,
            segment_preallocate: true,
            spare_preallocated_segments: defaults.spare_preallocated_segments,
            spare_preallocated_segments_overridden: false,
            metadata_log_preallocate_entries: DEFAULT_METADATA_LOG_PREALLOCATE_ENTRIES,
            persistence_mode: defaults.persistence_mode,
            persistence_mode_overridden: false,
            async_io_backend: defaults.async_io_backend,
            async_io_backend_overridden: false,
            io_uring_queue_depth: defaults.io_uring_queue_depth,
            io_uring_queue_depth_overridden: false,
            io_submit_batch_max: defaults.io_submit_batch_max,
            io_submit_batch_max_overridden: false,
            io_cqe_batch_max: defaults.io_cqe_batch_max,
            io_cqe_batch_max_overridden: false,
            io_uring_register_files: defaults.io_uring_register_files,
            io_uring_register_files_overridden: false,
            checksum_mode: ChecksumMode::Crc32c,
            out_of_space_policy: OutOfSpacePolicy::FailWriter,
            metadata_log_roll_bytes: defaults.metadata_log_roll_bytes,
            metadata_log_roll_bytes_overridden: false,
            metadata_log_max_bytes: defaults.metadata_log_max_bytes,
            metadata_log_max_bytes_overridden: false,
            max_disk_bytes: None,
            log_id: ZERO_LOG_ID,
            segment_generation: 1,
        }
    }

    fn apply_profile_defaults(&mut self, defaults: ProfileDefaults) {
        if !self.segment_bytes_overridden {
            self.segment_bytes = defaults.segment_bytes;
        }
        if !self.spare_preallocated_segments_overridden {
            self.spare_preallocated_segments = defaults.spare_preallocated_segments;
        }
        if !self.persistence_mode_overridden {
            self.persistence_mode = defaults.persistence_mode;
        }
        if !self.async_io_backend_overridden {
            self.async_io_backend = defaults.async_io_backend;
        }
        if !self.io_uring_queue_depth_overridden {
            self.io_uring_queue_depth = defaults.io_uring_queue_depth;
        }
        if !self.io_submit_batch_max_overridden {
            self.io_submit_batch_max = defaults.io_submit_batch_max;
        }
        if !self.io_cqe_batch_max_overridden {
            self.io_cqe_batch_max = defaults.io_cqe_batch_max;
        }
        if !self.io_uring_register_files_overridden {
            self.io_uring_register_files = defaults.io_uring_register_files;
        }
        if !self.metadata_log_roll_bytes_overridden {
            self.metadata_log_roll_bytes = defaults.metadata_log_roll_bytes;
        }
        if !self.metadata_log_max_bytes_overridden {
            self.metadata_log_max_bytes = defaults.metadata_log_max_bytes;
        }
    }

    /// Configures recorder runtime profile defaults.
    pub fn profile(mut self, value: RecorderProfile) -> Self {
        self.profile = value;
        self.apply_profile_defaults(profile_defaults(value));
        self
    }

    /// Overrides metadata-log root path.
    pub fn metadata_log_path(mut self, value: &Path) -> Self {
        self.metadata_log_path = Some(value.to_path_buf());
        self
    }

    /// Configures segment byte size.
    pub fn segment_bytes(mut self, value: usize) -> Self {
        self.segment_bytes_overridden = true;
        self.segment_bytes = value;
        self
    }

    /// Enables/disables segment preallocation.
    pub fn segment_preallocate(mut self, value: bool) -> Self {
        self.segment_preallocate = value;
        self
    }

    /// Configures number of spare preallocated segments.
    pub fn spare_preallocated_segments(mut self, value: usize) -> Self {
        self.spare_preallocated_segments_overridden = true;
        self.spare_preallocated_segments = value;
        self
    }

    /// Configures number of commit-log entries reserved in each metadata-log preallocation chunk.
    pub fn metadata_log_preallocate_entries(mut self, value: usize) -> Self {
        self.metadata_log_preallocate_entries = value;
        self
    }

    /// Configures durability mode.
    pub fn persistence_mode(mut self, value: PersistenceMode) -> Self {
        self.persistence_mode_overridden = true;
        self.persistence_mode = value;
        self
    }

    /// Configures async data-path backend selection.
    pub fn async_io_backend(mut self, value: AsyncIoBackend) -> Self {
        self.async_io_backend_overridden = true;
        self.async_io_backend = value;
        self
    }

    /// Configures `io_uring` queue depth (Linux, when `IoUringPreferred` is used).
    pub fn io_uring_queue_depth(mut self, value: u32) -> Self {
        self.io_uring_queue_depth_overridden = true;
        self.io_uring_queue_depth = value;
        self
    }

    /// Configures maximum io_uring submissions pushed per batch.
    pub fn io_submit_batch_max(mut self, value: u32) -> Self {
        self.io_submit_batch_max_overridden = true;
        self.io_submit_batch_max = value;
        self
    }

    /// Configures completion reaping batch upper bound.
    pub fn io_cqe_batch_max(mut self, value: u32) -> Self {
        self.io_cqe_batch_max_overridden = true;
        self.io_cqe_batch_max = value;
        self
    }

    /// Enables/disables io_uring registered-file mode.
    pub fn io_uring_register_files(mut self, value: bool) -> Self {
        self.io_uring_register_files_overridden = true;
        self.io_uring_register_files = value;
        self
    }

    /// Configures frame checksum mode.
    pub fn checksum_mode(mut self, value: ChecksumMode) -> Self {
        self.checksum_mode = value;
        self
    }

    /// Configures out-of-space policy.
    pub fn out_of_space_policy(mut self, value: OutOfSpacePolicy) -> Self {
        self.out_of_space_policy = value;
        self
    }

    /// Configures active metadata-log roll size in bytes.
    pub fn metadata_log_roll_bytes(mut self, value: u64) -> Self {
        self.metadata_log_roll_bytes_overridden = true;
        self.metadata_log_roll_bytes = value;
        self
    }

    /// Configures global metadata-log size cap in bytes.
    pub fn metadata_log_max_bytes(mut self, value: u64) -> Self {
        self.metadata_log_max_bytes_overridden = true;
        self.metadata_log_max_bytes = value;
        self
    }

    /// Configures global retained-bytes cap across hot and detached tiers.
    pub fn max_disk_bytes(mut self, value: u64) -> Self {
        self.max_disk_bytes = Some(value);
        self
    }

    /// Configures archive log id embedded into file headers.
    pub fn log_id(mut self, value: [u8; 16]) -> Self {
        self.log_id = value;
        self
    }

    /// Configures segment generation value.
    pub fn segment_generation(mut self, value: u32) -> Self {
        self.segment_generation = value;
        self
    }

    /// Creates a new recorder and fails when archive paths already exist.
    pub fn create(self) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        self.create_internal(false)
    }

    /// Opens an existing recorder archive and runs startup recovery, or creates a new archive.
    pub fn open_or_recover(self) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        self.create_internal(true)
    }

    fn create_internal(
        self,
        recover_existing: bool,
    ) -> Result<ArchiveRecorder, ArchiveRecorderError> {
        let mut config = self.build_config()?;
        if config.persistence_mode == PersistenceMode::Volatile {
            return Ok(new_volatile_recorder(config));
        }

        let archive_exists = config.storage_path.join("catalog.bin").exists()
            || config.storage_path.join("segments").exists();
        if archive_exists {
            if !recover_existing {
                return Err(ArchiveRecorderError::ArchiveAlreadyExists(
                    config.storage_path.clone(),
                ));
            }
            return recover_existing_archive(&mut config);
        }

        create_new_archive(config)
    }

    fn build_config(&self) -> Result<RecorderConfig, ArchiveRecorderError> {
        let minimal_frame_bytes = ARCHIVE_FILE_HEADER_V1_LEN + FRAME_HEADER_LEN + 8;
        if self.segment_bytes <= minimal_frame_bytes {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "segment_bytes is too small to store any frame",
            ));
        }
        if self.metadata_log_preallocate_entries == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "metadata_log_preallocate_entries must be > 0",
            ));
        }
        if let Some(max_disk_bytes) = self.max_disk_bytes {
            if max_disk_bytes == 0 {
                return Err(ArchiveRecorderError::InvalidConfiguration(
                    "max_disk_bytes must be > 0 when configured",
                ));
            }
        }
        if self.io_uring_queue_depth == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "io_uring_queue_depth must be > 0",
            ));
        }
        if self.io_submit_batch_max == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "io_submit_batch_max must be > 0",
            ));
        }
        if self.io_cqe_batch_max == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "io_cqe_batch_max must be > 0",
            ));
        }
        if self.io_submit_batch_max > self.io_uring_queue_depth {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "io_submit_batch_max must be <= io_uring_queue_depth",
            ));
        }
        if self.io_cqe_batch_max > self.io_uring_queue_depth.saturating_mul(2) {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "io_cqe_batch_max must be <= 2 * io_uring_queue_depth",
            ));
        }
        if self.metadata_log_roll_bytes
            < (ARCHIVE_FILE_HEADER_V1_LEN as u64 + COMMIT_ENTRY_LEN as u64)
        {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "metadata_log_roll_bytes is too small",
            ));
        }
        if self.metadata_log_max_bytes < self.metadata_log_roll_bytes {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "metadata_log_max_bytes must be >= metadata_log_roll_bytes",
            ));
        }
        if self.segment_generation == 0 {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "segment_generation must be > 0",
            ));
        }
        if self.checksum_mode == ChecksumMode::None {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "checksum_mode None is not allowed; framing checksum is mandatory",
            ));
        }

        Ok(RecorderConfig {
            profile: self.profile,
            storage_path: self.storage_path.clone(),
            metadata_log_path: self
                .metadata_log_path
                .clone()
                .unwrap_or_else(|| self.storage_path.clone()),
            segment_bytes: self.segment_bytes,
            segment_preallocate: self.segment_preallocate,
            spare_preallocated_segments: self.spare_preallocated_segments,
            metadata_log_preallocate_entries: self.metadata_log_preallocate_entries,
            persistence_mode: self.persistence_mode,
            async_io_backend: self.async_io_backend,
            io_uring_queue_depth: self.io_uring_queue_depth,
            io_submit_batch_max: self.io_submit_batch_max,
            io_cqe_batch_max: self.io_cqe_batch_max,
            io_uring_register_files: self.io_uring_register_files,
            checksum_mode: self.checksum_mode,
            out_of_space_policy: self.out_of_space_policy,
            max_disk_bytes: self.max_disk_bytes,
            metadata_log_roll_bytes: self.metadata_log_roll_bytes,
            metadata_log_max_bytes: self.metadata_log_max_bytes,
            log_id: self.log_id,
            segment_generation: self.segment_generation,
        })
    }
}

fn new_volatile_recorder(config: RecorderConfig) -> ArchiveRecorder {
    let (io_backend, effective_async_io_backend) = RecorderIoBackend::create(
        config.async_io_backend,
        config.io_uring_queue_depth,
        config.io_submit_batch_max,
        config.io_cqe_batch_max,
        config.io_uring_register_files,
    )
    .expect("volatile recorder backend configuration must be valid");
    ArchiveRecorder {
        config,
        io_backend,
        effective_async_io_backend,
        disk: None,
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal: 1,
        last_sequence: None,
        last_durable_data_sequence: None,
        last_durable_commit_ordinal: None,
        index_by_sequence: BTreeMap::new(),
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    }
}

fn create_new_archive(config: RecorderConfig) -> Result<ArchiveRecorder, ArchiveRecorderError> {
    let mut config = config;
    if config.log_id == ZERO_LOG_ID {
        config.log_id = generate_random_log_id()?;
    }

    fs::create_dir_all(&config.storage_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create storage directory",
        path: config.storage_path.clone(),
        source,
    })?;
    let segments_path = config.storage_path.join("segments");
    fs::create_dir_all(&segments_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create segments directory",
        path: segments_path.clone(),
        source,
    })?;
    let detached_path = detached_segments_path(&config.storage_path);
    fs::create_dir_all(&detached_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create detached segments directory",
        path: detached_path.clone(),
        source,
    })?;
    fs::create_dir_all(&config.metadata_log_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create metadata directory",
        path: config.metadata_log_path.clone(),
        source,
    })?;
    let pins_path = pin_directory(&config.metadata_log_path);
    fs::create_dir_all(&pins_path).map_err(|source| ArchiveRecorderError::Io {
        operation: "create replay pin directory",
        path: pins_path.clone(),
        source,
    })?;

    let (mut catalog_file, catalog_path) =
        create_new_file(&config.storage_path.join("catalog.bin"))?;
    write_archive_header(
        &mut catalog_file,
        &catalog_path,
        ArchiveFileKind::Catalog,
        config.log_id,
        0,
        0,
    )?;

    let commit_log_path = config.metadata_log_path.join("commit.idxlog");
    let (mut commit_log_file, commit_log_path) = create_new_file(&commit_log_path)?;
    write_archive_header(
        &mut commit_log_file,
        &commit_log_path,
        ArchiveFileKind::CommitIdxLog,
        config.log_id,
        0,
        0,
    )?;
    let commit_log_write_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    let commit_log_preallocated_len = preallocate_metadata_log(
        &mut commit_log_file,
        &commit_log_path,
        commit_log_write_offset,
        config.metadata_log_preallocate_entries,
        Some(config.metadata_log_roll_bytes),
    )?;

    let (io_backend, effective_async_io_backend) = RecorderIoBackend::create(
        config.async_io_backend,
        config.io_uring_queue_depth,
        config.io_submit_batch_max,
        config.io_cqe_batch_max,
        config.io_uring_register_files,
    )?;

    let mut recorder = ArchiveRecorder {
        config,
        io_backend,
        effective_async_io_backend,
        disk: Some(DiskRecorderState {
            segments_path,
            catalog_path,
            commit_log_path,
            catalog_file,
            commit_log_file,
            commit_log_write_offset,
            commit_log_preallocated_len,
            commit_log_roll_index: 1,
            active_segment: None,
        }),
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal: 1,
        last_sequence: None,
        last_durable_data_sequence: None,
        last_durable_commit_ordinal: None,
        index_by_sequence: BTreeMap::new(),
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    };

    recorder.open_new_active_segment(1)?;
    recorder.enforce_metadata_log_cap()?;
    Ok(recorder)
}

fn recover_existing_archive(
    config: &mut RecorderConfig,
) -> Result<ArchiveRecorder, ArchiveRecorderError> {
    let recovery_start = Instant::now();
    let segments_path = config.storage_path.join("segments");
    let detached_path = detached_segments_path(&config.storage_path);
    let catalog_path = config.storage_path.join("catalog.bin");
    let commit_log_path = config.metadata_log_path.join("commit.idxlog");

    if !catalog_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(catalog_path));
    }
    if !segments_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(segments_path));
    }
    if !detached_path.exists() {
        fs::create_dir_all(&detached_path).map_err(|source| ArchiveRecorderError::Io {
            operation: "create detached segments directory during recovery",
            path: detached_path.clone(),
            source,
        })?;
    }
    if !commit_log_path.exists() {
        return Err(ArchiveRecorderError::MissingArchiveComponent(
            commit_log_path,
        ));
    }

    let catalog_log_id = read_log_id_from_archive_header(&catalog_path, ArchiveFileKind::Catalog)?;
    let commit_log_id =
        read_log_id_from_archive_header(&commit_log_path, ArchiveFileKind::CommitIdxLog)?;
    if catalog_log_id != commit_log_id {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "archive log_id mismatch between catalog.bin and commit.idxlog",
        ));
    }
    if config.log_id == ZERO_LOG_ID {
        config.log_id = catalog_log_id;
    } else if config.log_id != catalog_log_id {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "configured log_id does not match existing archive log_id",
        ));
    }

    let pins_path = pin_directory(&config.metadata_log_path);
    if !pins_path.exists() {
        fs::create_dir_all(&pins_path).map_err(|source| ArchiveRecorderError::Io {
            operation: "create replay pin directory during recovery",
            path: pins_path.clone(),
            source,
        })?;
    }

    let catalog_summaries = read_catalog_entries(&catalog_path)?;
    let data_segments = list_data_segments(&segments_path)?;
    let commit_log_paths = list_commit_log_paths(&config.metadata_log_path).map_err(|source| {
        ArchiveRecorderError::Io {
            operation: "list commit idxlog files during recovery",
            path: config.metadata_log_path.clone(),
            source,
        }
    })?;
    let mut commit_entries = Vec::new();
    let mut commit_log_roll_index = 1u64;
    for path in &commit_log_paths {
        if let Some(name) = path.file_name().and_then(|value| value.to_str()) {
            if let Some(index) = parse_rolled_commit_log_index(name) {
                commit_log_roll_index = commit_log_roll_index.max(index.saturating_add(1));
            }
        }
        if path == &commit_log_path {
            continue;
        }
        let rolled_log_id = read_log_id_from_archive_header(path, ArchiveFileKind::CommitIdxLog)?;
        if rolled_log_id != config.log_id {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "rolled commit idxlog log_id does not match archive log_id",
            ));
        }
        let mut rolled_entries = read_commit_entries(path).map_err(|source| match source {
            ArchiveReplayError::Io {
                operation,
                path,
                source,
            } => ArchiveRecorderError::Io {
                operation,
                path,
                source,
            },
            ArchiveReplayError::FileHeader(error) => ArchiveRecorderError::FileHeader(error),
            _ => ArchiveRecorderError::RecoveryInconsistent("invalid rolled commit idxlog file"),
        })?;
        commit_entries.append(&mut rolled_entries);
    }

    let mut commit_log_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&commit_log_path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "open commit idxlog for recovery",
            path: commit_log_path.clone(),
            source,
        })?;
    let commit_recovery =
        recover_commit_log_entries(&mut commit_log_file, &commit_log_path, &segments_path)?;
    commit_entries.extend(commit_recovery.entries.iter().copied());
    let commit_log_write_offset = commit_recovery.logical_end_offset;
    let commit_log_preallocated_len = preallocate_metadata_log(
        &mut commit_log_file,
        &commit_log_path,
        commit_log_write_offset,
        config.metadata_log_preallocate_entries,
        Some(config.metadata_log_roll_bytes),
    )?;

    let mut index_by_sequence = BTreeMap::new();
    let mut next_commit_ordinal = 1u64;
    let mut last_sequence = None;
    let mut last_commit_ordinal = 0u64;
    for entry in &commit_entries {
        if last_commit_ordinal != 0 && entry.commit_ordinal <= last_commit_ordinal {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "commit idxlog entries are not strictly ordered by commit ordinal",
            ));
        }
        last_commit_ordinal = entry.commit_ordinal;
        if index_by_sequence
            .insert(entry.sequence, entry.locator)
            .is_some()
        {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "commit.idxlog contains duplicate sequence",
            ));
        }
        if let Some(previous) = last_sequence {
            if entry.sequence <= previous {
                return Err(ArchiveRecorderError::RecoveryInconsistent(
                    "commit.idxlog sequence is not strictly monotonic",
                ));
            }
        }
        last_sequence = Some(entry.sequence);
        next_commit_ordinal = entry.commit_ordinal.saturating_add(1);
    }

    let (active_segment_id, active_segment_generation) = determine_active_segment_for_recovery(
        &data_segments,
        &catalog_summaries,
        &commit_entries,
        config.segment_generation,
        &segments_path,
    );
    let recovered_last_durable_data_sequence = last_sequence;
    let recovered_last_durable_commit_ordinal = if last_commit_ordinal == 0 {
        None
    } else {
        Some(last_commit_ordinal)
    };
    config.segment_generation = active_segment_generation;

    let mut active_committed_records = 0u64;
    let mut active_sequence_start = None;
    let mut active_sequence_end = None;
    let mut committed_active_write_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    for entry in &commit_entries {
        if entry.locator.segment_id == active_segment_id
            && entry.locator.segment_generation == active_segment_generation
        {
            active_committed_records += 1;
            active_sequence_start.get_or_insert(entry.sequence);
            active_sequence_end = Some(entry.sequence);
            let end_offset = entry.locator.file_offset + entry.locator.frame_len as u64;
            if end_offset > committed_active_write_offset {
                committed_active_write_offset = end_offset;
            }
        }
    }

    let catalog_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&catalog_path)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "open catalog for recovery",
            path: catalog_path.clone(),
            source,
        })?;

    let (io_backend, effective_async_io_backend) = RecorderIoBackend::create(
        config.async_io_backend,
        config.io_uring_queue_depth,
        config.io_submit_batch_max,
        config.io_cqe_batch_max,
        config.io_uring_register_files,
    )?;

    let mut recorder = ArchiveRecorder {
        config: config.clone(),
        io_backend,
        effective_async_io_backend,
        disk: Some(DiskRecorderState {
            segments_path,
            catalog_path,
            commit_log_path,
            catalog_file,
            commit_log_file,
            commit_log_write_offset,
            commit_log_preallocated_len,
            commit_log_roll_index: 1,
            active_segment: None,
        }),
        stats: ArchiveRecorderStats::default(),
        recovery_status: ArchiveRecoveryStatus::default(),
        next_commit_ordinal,
        last_sequence,
        last_durable_data_sequence: recovered_last_durable_data_sequence,
        last_durable_commit_ordinal: recovered_last_durable_commit_ordinal,
        index_by_sequence,
        volatile_records: Vec::new(),
        finalized: false,
        degraded: false,
    };

    recorder.open_new_active_segment(active_segment_id)?;
    let segment_recovery = recorder.recover_active_segment_tail(
        committed_active_write_offset,
        active_committed_records,
        active_sequence_start,
        active_sequence_end,
    )?;

    recorder.recovery_status = ArchiveRecoveryStatus {
        recovered_existing_archive: true,
        catalog_segments_loaded: catalog_summaries.len() as u64,
        commit_entries_loaded: commit_entries.len() as u64,
        active_segment_id,
        active_segment_generation,
        active_segment_records: active_committed_records,
        segment_truncation_events: if segment_recovery.truncated_bytes > 0 {
            1
        } else {
            0
        },
        segment_truncated_bytes: segment_recovery.truncated_bytes,
        commit_log_truncated_bytes: commit_recovery.truncated_bytes,
        recovery_duration_ns: recovery_start.elapsed().as_nanos() as u64,
    };

    recorder.enforce_retention_cap()?;
    recorder.enforce_metadata_log_cap()?;

    Ok(recorder)
}
impl ArchiveRecorder {
    /// Returns current recorder stats.
    pub fn stats(&self) -> ArchiveRecorderStats {
        self.stats
    }

    /// Returns startup recovery status.
    pub fn recovery_status(&self) -> ArchiveRecoveryStatus {
        self.recovery_status
    }

    /// Returns true when recorder entered degraded state.
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Returns effective persistence mode.
    pub fn persistence_mode(&self) -> PersistenceMode {
        self.config.persistence_mode
    }

    /// Returns resolved recorder profile.
    pub fn profile(&self) -> RecorderProfile {
        self.config.profile
    }

    /// Returns default acknowledgment level for append operations.
    pub fn default_ack_level(&self) -> RecorderAckLevel {
        match self.config.persistence_mode {
            PersistenceMode::Volatile | PersistenceMode::Async => RecorderAckLevel::Accepted,
            PersistenceMode::Sync => RecorderAckLevel::DurableData,
        }
    }

    /// Returns last known durable data sequence.
    pub fn last_durable_data_sequence(&self) -> Option<u64> {
        self.last_durable_data_sequence
    }

    /// Returns last known durable commit-log ordinal.
    pub fn last_durable_commit_ordinal(&self) -> Option<u64> {
        self.last_durable_commit_ordinal
    }

    /// Returns configured async backend preference.
    pub fn configured_async_io_backend(&self) -> AsyncIoBackend {
        self.config.async_io_backend
    }

    /// Returns effective async backend selected at runtime.
    pub fn effective_async_io_backend(&self) -> EffectiveAsyncIoBackend {
        self.effective_async_io_backend
    }

    /// Returns effective segment size in bytes.
    pub fn segment_bytes(&self) -> usize {
        self.config.segment_bytes
    }

    /// Returns configured io_uring queue depth.
    pub fn io_uring_queue_depth(&self) -> u32 {
        self.config.io_uring_queue_depth
    }

    /// Returns configured io_uring submission batch size.
    pub fn io_submit_batch_max(&self) -> u32 {
        self.config.io_submit_batch_max
    }

    /// Returns configured io_uring completion batch size.
    pub fn io_cqe_batch_max(&self) -> u32 {
        self.config.io_cqe_batch_max
    }

    /// Returns configured metadata-log roll threshold in bytes.
    pub fn metadata_log_roll_bytes(&self) -> u64 {
        self.config.metadata_log_roll_bytes
    }

    /// Returns configured metadata-log global size cap in bytes.
    pub fn metadata_log_max_bytes(&self) -> u64 {
        self.config.metadata_log_max_bytes
    }

    /// Returns configured retained-bytes cap across tiers.
    pub fn max_disk_bytes(&self) -> Option<u64> {
        self.config.max_disk_bytes
    }

    /// Returns retention/tier status for admin surfaces.
    pub fn retention_status(&self) -> Result<ArchiveRetentionStatus, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(ArchiveRetentionStatus {
                max_disk_bytes: self.config.max_disk_bytes,
                ..ArchiveRetentionStatus::default()
            });
        }

        let segments = self.collect_segment_states()?;
        let mut status = ArchiveRetentionStatus {
            max_disk_bytes: self.config.max_disk_bytes,
            ..ArchiveRetentionStatus::default()
        };

        for segment in segments {
            status.retained_bytes_total += segment.data_bytes_used;
            if segment.pinned {
                status.pinned_segments += 1;
            }
            match segment.tier {
                ArchiveSegmentTier::HotAttached => {
                    status.segments_hot_attached += 1;
                    status.retained_bytes_hot_attached += segment.data_bytes_used;
                }
                ArchiveSegmentTier::ColdDetached => {
                    status.segments_cold_detached += 1;
                    status.retained_bytes_cold_detached += segment.data_bytes_used;
                }
            }
        }

        Ok(status)
    }

    /// Lists sealed segment tier state and pin overlap.
    pub fn list_segments(&self) -> Result<Vec<ArchiveSegmentState>, ArchiveRecorderError> {
        self.collect_segment_states()
    }

    /// Creates an explicit replay pin for `[sequence_start, sequence_end]`.
    pub fn begin_replay_pin(
        &self,
        sequence_start: u64,
        sequence_end: u64,
    ) -> Result<ReplayPin, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "replay pins require persisted archive mode",
            ));
        }
        if sequence_start > sequence_end {
            return Err(ArchiveRecorderError::InvalidConfiguration(
                "replay pin requires sequence_start <= sequence_end",
            ));
        }

        let metadata_root = &self.config.metadata_log_path;
        let pin_dir = pin_directory(metadata_root);
        fs::create_dir_all(&pin_dir).map_err(|source| ArchiveRecorderError::Io {
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
            let path = pin_file_path(metadata_root, pin);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.flush().map_err(|source| ArchiveRecorderError::Io {
                        operation: "flush replay pin file",
                        path: path.clone(),
                        source,
                    })?;
                    return Ok(pin);
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => continue,
                Err(source) => {
                    return Err(ArchiveRecorderError::Io {
                        operation: "create replay pin file",
                        path,
                        source,
                    });
                }
            }
        }

        Err(ArchiveRecorderError::InvalidConfiguration(
            "unable to allocate unique replay pin id",
        ))
    }

    /// Releases a previously created replay pin. Idempotent.
    pub fn release_replay_pin(&self, pin: ReplayPin) -> Result<(), ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(());
        }
        let path = pin_file_path(&self.config.metadata_log_path, pin);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ArchiveRecorderError::Io {
                operation: "remove replay pin file",
                path,
                source,
            }),
        }
    }

    /// Detaches all sealed hot-attached segments with `sequence_end < before_sequence`.
    pub fn detach_before_sequence(
        &mut self,
        before_sequence: u64,
    ) -> Result<u64, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(0);
        }

        let summaries = self.sealed_segment_summaries()?;
        let pins = self.active_replay_pins()?;
        let mut detached = 0u64;
        for summary in summaries {
            if summary.sequence_end >= before_sequence {
                continue;
            }
            if overlaps_any_pin(summary.sequence_start, summary.sequence_end, &pins) {
                continue;
            }
            if self.detach_segment(summary.segment_id, summary.segment_generation)? {
                detached += 1;
            }
        }

        Ok(detached)
    }

    /// Attaches all detached sealed segments. Idempotent.
    pub fn attach_all_detached(&mut self) -> Result<u64, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(0);
        }

        let summaries = self.sealed_segment_summaries()?;
        let mut attached = 0u64;
        for summary in summaries {
            if self.attach_segment(summary.segment_id, summary.segment_generation)? {
                attached += 1;
            }
        }
        Ok(attached)
    }

    /// Deletes detached sealed segments with `sequence_end < before_sequence`.
    ///
    /// Segments overlapping active replay pins are skipped.
    pub fn delete_detached_before_sequence(
        &mut self,
        before_sequence: u64,
    ) -> Result<u64, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(0);
        }

        let summaries = self.sealed_segment_summaries()?;
        let pins = self.active_replay_pins()?;
        let mut deleted = 0u64;
        for summary in summaries {
            if summary.sequence_end >= before_sequence {
                continue;
            }
            if overlaps_any_pin(summary.sequence_start, summary.sequence_end, &pins) {
                continue;
            }
            if self.delete_detached_segment(summary.segment_id, summary.segment_generation)? {
                self.remove_sequence_index_for_segment(&summary);
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Trims by sequence boundary with deterministic oldest-first policy.
    ///
    /// This operation first detaches matching hot segments, then deletes detached segments.
    pub fn trim_before_sequence(
        &mut self,
        before_sequence: u64,
    ) -> Result<u64, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(0);
        }

        let _ = self.detach_before_sequence(before_sequence)?;
        self.delete_detached_before_sequence(before_sequence)
    }

    fn append_record_internal(
        &mut self,
        input: RecordInput<'_>,
        source_metadata: ArchiveSourceMetadata,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        if self.finalized {
            return Err(ArchiveRecorderError::Finalized);
        }
        if self.degraded {
            return Err(ArchiveRecorderError::Degraded);
        }
        if let Some(previous) = self.last_sequence {
            if input.sequence <= previous {
                return Err(ArchiveRecorderError::SequenceNotMonotonic {
                    previous,
                    next: input.sequence,
                });
            }
        }

        let commit_ordinal = self.next_commit_ordinal;
        self.next_commit_ordinal += 1;
        self.last_sequence = Some(input.sequence);

        match self.config.persistence_mode {
            PersistenceMode::Volatile => self.append_volatile(input, commit_ordinal),
            PersistenceMode::Async | PersistenceMode::Sync => {
                self.append_disk(input, commit_ordinal, source_metadata)
            }
        }
    }

    /// Waits for a requested acknowledgment level for an already recorded commit.
    pub fn wait_for_ack(
        &mut self,
        commit: RecordedCommit,
        requested_ack: RecorderAckLevel,
        timeout: Duration,
    ) -> Result<(), ArchiveRecorderError> {
        if requested_ack == RecorderAckLevel::Accepted {
            return Ok(());
        }

        if self.ack_satisfied(commit, requested_ack) {
            return Ok(());
        }

        if timeout.is_zero() {
            return Err(self.ack_timeout_error(requested_ack, timeout));
        }

        let deadline = Instant::now() + timeout;
        loop {
            match self.config.persistence_mode {
                PersistenceMode::Volatile => {}
                PersistenceMode::Async | PersistenceMode::Sync => {
                    self.sync_segment_for_locator(commit.locator)?;
                    self.note_durable_data(commit);
                    if requested_ack == RecorderAckLevel::DurableDataAndCommitLog {
                        self.sync_commit_log_and_catalog()?;
                        self.note_durable_commit_log(commit);
                    }
                }
            }

            if self.ack_satisfied(commit, requested_ack) {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(self.ack_timeout_error(requested_ack, timeout));
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            thread::sleep(remaining.min(DEFAULT_ACK_POLL_INTERVAL));
        }
    }

    /// Waits for `DurableData` using the default timeout.
    pub fn wait_for_durable_data(
        &mut self,
        commit: RecordedCommit,
    ) -> Result<(), ArchiveRecorderError> {
        self.wait_for_ack(
            commit,
            RecorderAckLevel::DurableData,
            DEFAULT_WAIT_DURABLE_DATA_TIMEOUT,
        )
    }

    /// Waits for `DurableDataAndCommitLog` using the default timeout.
    pub fn wait_for_durable_data_and_commit_log(
        &mut self,
        commit: RecordedCommit,
    ) -> Result<(), ArchiveRecorderError> {
        self.wait_for_ack(
            commit,
            RecorderAckLevel::DurableDataAndCommitLog,
            DEFAULT_WAIT_DURABLE_DATA_AND_COMMIT_LOG_TIMEOUT,
        )
    }

    fn ack_satisfied(&self, commit: RecordedCommit, requested_ack: RecorderAckLevel) -> bool {
        match requested_ack {
            RecorderAckLevel::Accepted => true,
            RecorderAckLevel::DurableData => self
                .last_durable_data_sequence
                .is_some_and(|value| value >= commit.sequence),
            RecorderAckLevel::DurableDataAndCommitLog => {
                self.last_durable_data_sequence
                    .is_some_and(|value| value >= commit.sequence)
                    && self
                        .last_durable_commit_ordinal
                        .is_some_and(|value| value >= commit.commit_ordinal)
            }
        }
    }

    fn ack_timeout_error(
        &self,
        requested: RecorderAckLevel,
        timeout: Duration,
    ) -> ArchiveRecorderError {
        ArchiveRecorderError::AckTimeout {
            requested,
            timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            last_durable_data_sequence: self.last_durable_data_sequence,
            last_durable_commit_ordinal: self.last_durable_commit_ordinal,
        }
    }

    fn note_durable_data(&mut self, commit: RecordedCommit) {
        self.last_durable_data_sequence = Some(
            self.last_durable_data_sequence
                .unwrap_or(0)
                .max(commit.sequence),
        );
    }

    fn note_durable_commit_log(&mut self, commit: RecordedCommit) {
        self.note_durable_data(commit);
        self.last_durable_commit_ordinal = Some(
            self.last_durable_commit_ordinal
                .unwrap_or(0)
                .max(commit.commit_ordinal),
        );
    }

    /// Flushes recorder output streams.
    pub fn flush(&mut self) -> Result<(), ArchiveRecorderError> {
        if let Some(disk) = self.disk.as_mut() {
            let io_backend = &mut self.io_backend;

            if let Some(active) = disk.active_segment.as_mut() {
                let segment_path = segment_data_path(
                    &disk.segments_path,
                    active.segment_id,
                    active.segment_generation,
                );
                io_backend.flush(&mut active.file, &segment_path, "flush active segment")?;
            }

            io_backend.flush(
                &mut disk.commit_log_file,
                &disk.commit_log_path,
                "flush commit idxlog",
            )?;
            io_backend.flush(&mut disk.catalog_file, &disk.catalog_path, "flush catalog")?;
        }

        Ok(())
    }

    /// Finalizes recorder output by sealing the active segment.
    pub fn finalize(&mut self) -> Result<(), ArchiveRecorderError> {
        if self.finalized {
            return Ok(());
        }

        if self.disk.is_some() {
            self.seal_active_segment_internal(false)?;
            self.truncate_commit_log_to_logical_size()?;
            self.flush()?;
        }

        self.finalized = true;
        Ok(())
    }

    pub(super) fn next_pattern_adapter_sequence(&self) -> Result<u64, ArchiveRecorderError> {
        match self.last_sequence {
            Some(previous) => {
                previous
                    .checked_add(1)
                    .ok_or(ArchiveRecorderError::InvalidConfiguration(
                        "archive sequence space exhausted",
                    ))
            }
            None => Ok(1),
        }
    }

    pub(super) fn append_adapted_record(
        &mut self,
        source_metadata: ArchiveSourceMetadata,
        event_time_ns: u64,
        user_header: &[u8],
        payload: &[u8],
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let adapted_user_header = encode_adapter_user_header(source_metadata, user_header);
        self.append_record_internal(
            RecordInput {
                sequence: self.next_pattern_adapter_sequence()?,
                event_time_ns,
                user_header: &adapted_user_header,
                payload,
            },
            source_metadata,
        )
    }

    fn append_volatile(
        &mut self,
        input: RecordInput<'_>,
        commit_ordinal: u64,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let commit_time_ns = now_ns();
        let frame_len = align_up(
            FRAME_HEADER_LEN + input.user_header.len() + input.payload.len(),
            8,
        );
        let locator = ArchiveLocator {
            segment_id: 0,
            segment_generation: 0,
            file_offset: self.volatile_records.len() as u64,
            frame_len: frame_len as u32,
        };
        self.index_by_sequence.insert(input.sequence, locator);
        self.volatile_records.push(VolatileFrame {
            commit_ordinal,
            sequence: input.sequence,
            event_time_ns: input.event_time_ns,
            commit_time_ns,
            user_header: input.user_header.to_vec(),
            payload: input.payload.to_vec(),
            locator,
        });
        self.stats.committed_records += 1;
        self.stats.payload_bytes_committed += input.payload.len() as u64;

        Ok(RecordedCommit {
            commit_ordinal,
            sequence: input.sequence,
            locator,
        })
    }

    fn append_disk(
        &mut self,
        input: RecordInput<'_>,
        commit_ordinal: u64,
        source_metadata: ArchiveSourceMetadata,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let commit_time_ns = now_ns();
        let frame = EncodedFrame::new(
            commit_ordinal,
            input.sequence,
            input.event_time_ns,
            commit_time_ns,
            input.user_header,
            input.payload,
            self.config.checksum_mode,
        );

        let max_frame_bytes = self.config.segment_bytes - ARCHIVE_FILE_HEADER_V1_LEN;
        if frame.bytes.len() > max_frame_bytes {
            return Err(ArchiveRecorderError::FrameTooLarge {
                required: frame.bytes.len(),
                segment_bytes: self.config.segment_bytes,
            });
        }

        let disk = self.disk.as_mut().expect("disk recorder state must exist");
        let active = disk
            .active_segment
            .as_mut()
            .expect("active segment must exist");

        if (active.write_offset as usize + frame.bytes.len()) > self.config.segment_bytes {
            self.seal_active_segment_internal(true)?;
        }

        let (locator, segment_path) = {
            let disk = self.disk.as_ref().expect("disk recorder state must exist");
            let active = disk
                .active_segment
                .as_ref()
                .expect("active segment must exist");
            (
                ArchiveLocator {
                    segment_id: active.segment_id,
                    segment_generation: active.segment_generation,
                    file_offset: active.write_offset,
                    frame_len: frame.bytes.len() as u32,
                },
                segment_data_path(
                    &disk.segments_path,
                    active.segment_id,
                    active.segment_generation,
                ),
            )
        };

        let write_result = {
            let (io_backend, disk) = (
                &mut self.io_backend,
                self.disk.as_mut().expect("disk recorder state must exist"),
            );
            let active = disk
                .active_segment
                .as_mut()
                .expect("active segment must exist");
            io_backend.write_all_at(
                &mut active.file,
                &segment_path,
                locator.file_offset,
                &frame.bytes,
                "write active segment",
            )
        };
        if let Err(source) = write_result {
            return Err(self.handle_commit_write_failure(source));
        }

        {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            let active = disk
                .active_segment
                .as_mut()
                .expect("active segment must exist");
            active.write_offset += frame.bytes.len() as u64;
            active.sequence_start.get_or_insert(input.sequence);
            active.sequence_end = Some(input.sequence);
            active.records += 1;
        }

        self.roll_commit_log_if_needed()?;
        let commit_log_resized = self.ensure_commit_log_capacity()?;
        if commit_log_resized {
            self.enforce_metadata_log_cap()?;
        }
        let commit_log_path = self
            .disk
            .as_ref()
            .expect("disk recorder state must exist")
            .commit_log_path
            .clone();
        let write_offset = self
            .disk
            .as_ref()
            .expect("disk recorder state must exist")
            .commit_log_write_offset;
        let commit_entry_bytes = encode_commit_entry(CommitEntry {
            commit_ordinal,
            sequence: input.sequence,
            locator,
            frame_checksum: frame.checksum,
            event_time_ns: input.event_time_ns,
            commit_time_ns,
            source_pattern: source_metadata.source_pattern,
            source_service_id: source_metadata.source_service_id,
            source_instance_id: source_metadata.source_instance_id,
            source_sequence: source_metadata.source_sequence,
        });
        let write_result = {
            let (io_backend, disk) = (
                &mut self.io_backend,
                self.disk.as_mut().expect("disk recorder state must exist"),
            );
            io_backend.write_all_at(
                &mut disk.commit_log_file,
                &commit_log_path,
                write_offset,
                &commit_entry_bytes,
                "append commit idxlog entry",
            )
        };
        if let Err(source) = write_result {
            return Err(self.handle_commit_write_failure(source));
        }
        self.disk
            .as_mut()
            .expect("disk recorder state must exist")
            .commit_log_write_offset += COMMIT_ENTRY_LEN as u64;

        self.stats.committed_records += 1;
        self.stats.payload_bytes_committed += input.payload.len() as u64;
        self.stats.data_bytes_written += frame.bytes.len() as u64;
        self.stats.metadata_bytes_written += COMMIT_ENTRY_LEN as u64;
        self.index_by_sequence.insert(input.sequence, locator);

        let commit = RecordedCommit {
            commit_ordinal,
            sequence: input.sequence,
            locator,
        };

        if self.config.persistence_mode == PersistenceMode::Sync {
            self.sync_data_files()?;
            self.note_durable_commit_log(commit);
        }

        Ok(commit)
    }

    fn handle_commit_write_failure(
        &mut self,
        source: ArchiveRecorderError,
    ) -> ArchiveRecorderError {
        if let ArchiveRecorderError::Io {
            operation,
            path,
            source,
        } = source
        {
            if is_out_of_space(&source) {
                self.stats.out_of_space_events += 1;
                self.degraded = true;
                return match self.config.out_of_space_policy {
                    OutOfSpacePolicy::FailWriter => ArchiveRecorderError::OutOfSpace(path),
                };
            }

            self.degraded = true;
            return ArchiveRecorderError::Io {
                operation,
                path,
                source,
            };
        }

        self.degraded = true;
        source
    }

    #[cfg(test)]
    fn handle_write_failure(
        &mut self,
        path: &Path,
        source: std::io::Error,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        if is_out_of_space(&source) {
            self.stats.out_of_space_events += 1;
            self.degraded = true;
            return match self.config.out_of_space_policy {
                OutOfSpacePolicy::FailWriter => {
                    Err(ArchiveRecorderError::OutOfSpace(path.to_path_buf()))
                }
            };
        }

        self.degraded = true;
        Err(ArchiveRecorderError::Io {
            operation: "write active segment",
            path: path.to_path_buf(),
            source,
        })
    }

    fn enforce_metadata_log_cap(&mut self) -> Result<(), ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(());
        }

        let commit_logs =
            list_commit_log_paths(&self.config.metadata_log_path).map_err(|source| {
                ArchiveRecorderError::Io {
                    operation: "list commit idxlog files",
                    path: self.config.metadata_log_path.clone(),
                    source,
                }
            })?;
        let mut total_bytes = 0u64;
        for path in &commit_logs {
            let bytes = path
                .metadata()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "read commit idxlog metadata",
                    path: path.to_path_buf(),
                    source,
                })?
                .len();
            total_bytes = total_bytes.saturating_add(bytes);
        }

        if total_bytes > self.config.metadata_log_max_bytes {
            self.degraded = true;
            return Err(ArchiveRecorderError::MetadataLogCapacityExceeded {
                max_bytes: self.config.metadata_log_max_bytes,
                required_bytes: total_bytes,
            });
        }

        Ok(())
    }

    fn roll_commit_log_if_needed(&mut self) -> Result<(), ArchiveRecorderError> {
        let should_roll = {
            let disk = self.disk.as_ref().expect("disk recorder state must exist");
            disk.commit_log_write_offset + COMMIT_ENTRY_LEN as u64
                > self.config.metadata_log_roll_bytes
        };
        if !should_roll {
            return Ok(());
        }

        {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            self.io_backend.flush(
                &mut disk.commit_log_file,
                &disk.commit_log_path,
                "flush commit idxlog before roll",
            )?;
            self.io_backend.sync_data(
                &mut disk.commit_log_file,
                &disk.commit_log_path,
                "sync commit idxlog before roll",
            )?;
        }
        self.truncate_commit_log_to_logical_size()?;

        let (active_path, rolled_path, roll_index) = {
            let disk = self.disk.as_ref().expect("disk recorder state must exist");
            let mut path =
                commit_log_roll_path(&self.config.metadata_log_path, disk.commit_log_roll_index);
            let mut index = disk.commit_log_roll_index;
            while path.exists() {
                index = index.saturating_add(1);
                path = commit_log_roll_path(&self.config.metadata_log_path, index);
            }
            (disk.commit_log_path.clone(), path, index)
        };

        fs::rename(&active_path, &rolled_path).map_err(|source| ArchiveRecorderError::Io {
            operation: "roll commit idxlog file",
            path: active_path.clone(),
            source,
        })?;

        let new_active_path = self.config.metadata_log_path.join("commit.idxlog");
        let (mut commit_log_file, commit_log_path) = create_new_file(&new_active_path)?;
        write_archive_header(
            &mut commit_log_file,
            &commit_log_path,
            ArchiveFileKind::CommitIdxLog,
            self.config.log_id,
            0,
            0,
        )?;
        let commit_log_write_offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
        let commit_log_preallocated_len = preallocate_metadata_log(
            &mut commit_log_file,
            &commit_log_path,
            commit_log_write_offset,
            self.config.metadata_log_preallocate_entries,
            Some(self.config.metadata_log_roll_bytes),
        )?;

        {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            disk.commit_log_file = commit_log_file;
            disk.commit_log_path = commit_log_path;
            disk.commit_log_write_offset = commit_log_write_offset;
            disk.commit_log_preallocated_len = commit_log_preallocated_len;
            disk.commit_log_roll_index = roll_index.saturating_add(1);
        }
        self.stats.metadata_log_rolls = self.stats.metadata_log_rolls.saturating_add(1);
        self.refresh_backend_registered_files()?;
        self.enforce_metadata_log_cap()?;
        Ok(())
    }

    fn ensure_commit_log_capacity(&mut self) -> Result<bool, ArchiveRecorderError> {
        let required = {
            let disk = self.disk.as_ref().expect("disk recorder state must exist");
            let required = disk.commit_log_write_offset + COMMIT_ENTRY_LEN as u64;
            if required <= disk.commit_log_preallocated_len {
                return Ok(false);
            }
            required
        };

        let result = {
            let disk = self.disk.as_mut().expect("disk recorder state must exist");
            preallocate_metadata_log(
                &mut disk.commit_log_file,
                &disk.commit_log_path,
                required,
                self.config.metadata_log_preallocate_entries,
                Some(self.config.metadata_log_roll_bytes),
            )
        };
        let preallocated_len = match result {
            Ok(value) => value,
            Err(source) => return Err(self.handle_commit_write_failure(source)),
        };

        let disk = self.disk.as_mut().expect("disk recorder state must exist");
        if required <= disk.commit_log_preallocated_len {
            return Ok(false);
        }
        disk.commit_log_preallocated_len = preallocated_len;
        Ok(true)
    }

    fn truncate_commit_log_to_logical_size(&mut self) -> Result<(), ArchiveRecorderError> {
        let Some(disk) = self.disk.as_mut() else {
            return Ok(());
        };

        self.io_backend.set_len(
            &mut disk.commit_log_file,
            &disk.commit_log_path,
            disk.commit_log_write_offset,
            "truncate commit idxlog to logical size",
        )?;
        disk.commit_log_preallocated_len = disk.commit_log_write_offset;
        Ok(())
    }

    fn collect_segment_states(&self) -> Result<Vec<ArchiveSegmentState>, ArchiveRecorderError> {
        let summaries = self.sealed_segment_summaries()?;
        let pins = self.active_replay_pins()?;
        let segments_path = match self.disk.as_ref() {
            Some(disk) => disk.segments_path.clone(),
            None => self.config.storage_path.join("segments"),
        };
        let detached_path = detached_segments_path(&self.config.storage_path);

        let mut states = Vec::new();
        for summary in summaries {
            let hot_data = segment_data_path(
                &segments_path,
                summary.segment_id,
                summary.segment_generation,
            );
            let hot_meta = segment_meta_path(
                &segments_path,
                summary.segment_id,
                summary.segment_generation,
            );
            let detached_data = segment_data_path(
                &detached_path,
                summary.segment_id,
                summary.segment_generation,
            );
            let detached_meta = segment_meta_path(
                &detached_path,
                summary.segment_id,
                summary.segment_generation,
            );

            let tier = if hot_data.exists() || hot_meta.exists() {
                Some(ArchiveSegmentTier::HotAttached)
            } else if detached_data.exists() || detached_meta.exists() {
                Some(ArchiveSegmentTier::ColdDetached)
            } else {
                None
            };

            let Some(tier) = tier else {
                continue;
            };

            states.push(ArchiveSegmentState {
                segment_id: summary.segment_id,
                segment_generation: summary.segment_generation,
                sequence_start: summary.sequence_start,
                sequence_end: summary.sequence_end,
                records: summary.records,
                data_bytes_used: summary.data_bytes_used,
                tier,
                pinned: overlaps_any_pin(summary.sequence_start, summary.sequence_end, &pins),
            });
        }

        states.sort_by_key(|state| {
            (
                state.sequence_start,
                state.segment_id,
                state.segment_generation,
            )
        });
        Ok(states)
    }

    fn sealed_segment_summaries(&self) -> Result<Vec<SegmentSummary>, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(Vec::new());
        }

        let catalog_path = match self.disk.as_ref() {
            Some(disk) => disk.catalog_path.clone(),
            None => self.config.storage_path.join("catalog.bin"),
        };
        if !catalog_path.exists() {
            return Ok(Vec::new());
        }

        read_catalog_entries(&catalog_path)
    }

    fn active_replay_pins(&self) -> Result<Vec<ReplayPin>, ArchiveRecorderError> {
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(Vec::new());
        }

        let pin_dir = pin_directory(&self.config.metadata_log_path);
        if !pin_dir.exists() {
            return Ok(Vec::new());
        }

        let entries = fs::read_dir(&pin_dir).map_err(|source| ArchiveRecorderError::Io {
            operation: "read replay pin directory",
            path: pin_dir.clone(),
            source,
        })?;
        let mut pins = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| ArchiveRecorderError::Io {
                operation: "read replay pin entry",
                path: pin_dir.clone(),
                source,
            })?;
            let Some(name) = entry.file_name().to_str().map(|value| value.to_owned()) else {
                continue;
            };
            if let Some(pin) = parse_pin_file_name(&name) {
                pins.push(pin);
            }
        }

        Ok(pins)
    }

    fn detach_segment(
        &mut self,
        segment_id: u64,
        segment_generation: u32,
    ) -> Result<bool, ArchiveRecorderError> {
        let Some(disk) = self.disk.as_ref() else {
            return Ok(false);
        };
        if let Some(active) = disk.active_segment.as_ref() {
            if active.segment_id == segment_id && active.segment_generation == segment_generation {
                return Ok(false);
            }
        }

        let segments_path = disk.segments_path.clone();
        let detached_path = detached_segments_path(&self.config.storage_path);
        fs::create_dir_all(&detached_path).map_err(|source| ArchiveRecorderError::Io {
            operation: "create detached segments directory",
            path: detached_path.clone(),
            source,
        })?;

        let mut changed = false;
        changed |= self.move_segment_file(
            &segment_data_path(&segments_path, segment_id, segment_generation),
            &segment_data_path(&detached_path, segment_id, segment_generation),
        )?;
        changed |= self.move_segment_file(
            &segment_meta_path(&segments_path, segment_id, segment_generation),
            &segment_meta_path(&detached_path, segment_id, segment_generation),
        )?;

        Ok(changed)
    }

    fn attach_segment(
        &mut self,
        segment_id: u64,
        segment_generation: u32,
    ) -> Result<bool, ArchiveRecorderError> {
        let Some(disk) = self.disk.as_ref() else {
            return Ok(false);
        };
        let segments_path = disk.segments_path.clone();
        let detached_path = detached_segments_path(&self.config.storage_path);

        let mut changed = false;
        changed |= self.move_segment_file(
            &segment_data_path(&detached_path, segment_id, segment_generation),
            &segment_data_path(&segments_path, segment_id, segment_generation),
        )?;
        changed |= self.move_segment_file(
            &segment_meta_path(&detached_path, segment_id, segment_generation),
            &segment_meta_path(&segments_path, segment_id, segment_generation),
        )?;

        Ok(changed)
    }

    fn delete_detached_segment(
        &mut self,
        segment_id: u64,
        segment_generation: u32,
    ) -> Result<bool, ArchiveRecorderError> {
        let Some(disk) = self.disk.as_ref() else {
            return Ok(false);
        };
        let segments_path = disk.segments_path.clone();
        let detached_path = detached_segments_path(&self.config.storage_path);

        let hot_data = segment_data_path(&segments_path, segment_id, segment_generation);
        let hot_meta = segment_meta_path(&segments_path, segment_id, segment_generation);
        if hot_data.exists() || hot_meta.exists() {
            return Ok(false);
        }

        let mut deleted = false;
        deleted |= self.remove_file_if_exists(&segment_data_path(
            &detached_path,
            segment_id,
            segment_generation,
        ))?;
        deleted |= self.remove_file_if_exists(&segment_meta_path(
            &detached_path,
            segment_id,
            segment_generation,
        ))?;

        Ok(deleted)
    }

    fn move_segment_file(
        &self,
        source: &Path,
        target: &Path,
    ) -> Result<bool, ArchiveRecorderError> {
        if !source.exists() {
            return Ok(false);
        }

        if target.exists() {
            fs::remove_file(source).map_err(|source_error| ArchiveRecorderError::Io {
                operation: "remove duplicate segment file during tier move",
                path: source.to_path_buf(),
                source: source_error,
            })?;
            return Ok(true);
        }

        fs::rename(source, target).map_err(|source_error| ArchiveRecorderError::Io {
            operation: "move segment file between tiers",
            path: source.to_path_buf(),
            source: source_error,
        })?;
        Ok(true)
    }

    fn remove_file_if_exists(&self, path: &Path) -> Result<bool, ArchiveRecorderError> {
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(false),
            Err(source) => Err(ArchiveRecorderError::Io {
                operation: "remove segment file",
                path: path.to_path_buf(),
                source,
            }),
        }
    }

    fn remove_sequence_index_for_segment(&mut self, summary: &SegmentSummary) {
        let keys: Vec<u64> = self
            .index_by_sequence
            .range(summary.sequence_start..=summary.sequence_end)
            .map(|(sequence, _)| *sequence)
            .collect();
        for key in keys {
            self.index_by_sequence.remove(&key);
        }
    }

    fn enforce_retention_cap(&mut self) -> Result<(), ArchiveRecorderError> {
        let Some(max_disk_bytes) = self.config.max_disk_bytes else {
            return Ok(());
        };
        if self.config.persistence_mode == PersistenceMode::Volatile {
            return Ok(());
        }

        let mut retained = self.retention_status()?.retained_bytes_total;
        if retained <= max_disk_bytes {
            return Ok(());
        }

        let mut states = self.collect_segment_states()?;
        states.sort_by_key(|state| {
            (
                state.sequence_start,
                state.segment_id,
                state.segment_generation,
            )
        });
        for state in states {
            if retained <= max_disk_bytes {
                return Ok(());
            }
            if state.pinned {
                continue;
            }
            if state.tier == ArchiveSegmentTier::HotAttached {
                let _ = self.detach_segment(state.segment_id, state.segment_generation)?;
            }
            if self.delete_detached_segment(state.segment_id, state.segment_generation)? {
                let summary = SegmentSummary {
                    segment_id: state.segment_id,
                    segment_generation: state.segment_generation,
                    sequence_start: state.sequence_start,
                    sequence_end: state.sequence_end,
                    records: state.records,
                    created_at_ns: 0,
                    sealed_at_ns: 0,
                    data_bytes_used: state.data_bytes_used,
                    segment_checksum: 0,
                };
                self.remove_sequence_index_for_segment(&summary);
                retained = retained.saturating_sub(state.data_bytes_used);
            }
        }

        if retained > max_disk_bytes {
            return Err(ArchiveRecorderError::RetentionBlockedByPins {
                max_disk_bytes,
                retained_bytes: retained,
            });
        }

        Ok(())
    }

    fn recover_active_segment_tail(
        &mut self,
        committed_write_offset: u64,
        committed_records: u64,
        committed_sequence_start: Option<u64>,
        committed_sequence_end: Option<u64>,
    ) -> Result<SegmentRecoveryResult, ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let active = disk
            .active_segment
            .as_mut()
            .expect("active segment must exist");
        let segment_path = segment_data_path(
            &disk.segments_path,
            active.segment_id,
            active.segment_generation,
        );

        let scan_result = scan_active_segment_tail(
            &mut active.file,
            &segment_path,
            self.config.segment_bytes as u64,
        )?;

        if committed_write_offset < ARCHIVE_FILE_HEADER_V1_LEN as u64 {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "committed write offset is below frame area",
            ));
        }
        if committed_write_offset > scan_result.valid_end {
            return Err(ArchiveRecorderError::RecoveryInconsistent(
                "commit.idxlog points beyond active segment valid boundary",
            ));
        }

        let target_write_offset = committed_write_offset.min(scan_result.valid_end);
        let mut truncated_bytes = 0u64;
        if scan_result.original_len > target_write_offset {
            self.io_backend.set_len(
                &mut active.file,
                &segment_path,
                target_write_offset,
                "truncate active segment recovery tail",
            )?;
            truncated_bytes = scan_result.original_len - target_write_offset;
        }

        if self.config.segment_preallocate {
            self.io_backend.set_len(
                &mut active.file,
                &segment_path,
                self.config.segment_bytes as u64,
                "re-preallocate active segment after recovery",
            )?;
        }

        active.write_offset = target_write_offset;
        active.records = committed_records;
        active.sequence_start = committed_sequence_start;
        active.sequence_end = committed_sequence_end;

        Ok(SegmentRecoveryResult { truncated_bytes })
    }

    fn sync_segment_for_locator(
        &mut self,
        locator: ArchiveLocator,
    ) -> Result<(), ArchiveRecorderError> {
        let segments_path = self
            .disk
            .as_ref()
            .ok_or(ArchiveRecorderError::InvalidConfiguration(
                "ack waits require persisted archive mode",
            ))?
            .segments_path
            .clone();

        let segment_path = segment_data_path(
            &segments_path,
            locator.segment_id,
            locator.segment_generation,
        );

        let active_matches = self
            .disk
            .as_ref()
            .and_then(|disk| disk.active_segment.as_ref())
            .is_some_and(|active| {
                active.segment_id == locator.segment_id
                    && active.segment_generation == locator.segment_generation
            });

        if active_matches {
            let disk = self.disk.as_mut().expect("disk state must exist");
            let active = disk
                .active_segment
                .as_mut()
                .expect("active segment must exist");
            return self.io_backend.sync_data(
                &mut active.file,
                &segment_path,
                "sync durable data segment",
            );
        }

        let mut segment_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_path)
            .map_err(|source| ArchiveRecorderError::Io {
                operation: "open segment for durable data sync",
                path: segment_path.clone(),
                source,
            })?;
        self.io_backend.sync_data(
            &mut segment_file,
            &segment_path,
            "sync durable data segment",
        )
    }

    fn sync_commit_log_and_catalog(&mut self) -> Result<(), ArchiveRecorderError> {
        let disk = self
            .disk
            .as_mut()
            .ok_or(ArchiveRecorderError::InvalidConfiguration(
                "ack waits require persisted archive mode",
            ))?;
        self.io_backend.sync_data(
            &mut disk.commit_log_file,
            &disk.commit_log_path,
            "sync durable commit idxlog",
        )?;
        self.io_backend.sync_data(
            &mut disk.catalog_file,
            &disk.catalog_path,
            "sync durable catalog",
        )?;
        Ok(())
    }

    fn sync_data_files(&mut self) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let io_backend = &mut self.io_backend;
        if let Some(active) = disk.active_segment.as_mut() {
            let segment_path = segment_data_path(
                &disk.segments_path,
                active.segment_id,
                active.segment_generation,
            );
            io_backend.sync_data(&mut active.file, &segment_path, "sync active segment")?;
        }
        io_backend.sync_data(
            &mut disk.commit_log_file,
            &disk.commit_log_path,
            "sync commit idxlog",
        )?;
        io_backend.sync_data(&mut disk.catalog_file, &disk.catalog_path, "sync catalog")?;

        Ok(())
    }

    fn refresh_backend_registered_files(&mut self) -> Result<(), ArchiveRecorderError> {
        #[cfg(target_os = "linux")]
        {
            let Some(disk) = self.disk.as_ref() else {
                return Ok(());
            };
            let mut fds = Vec::with_capacity(3);
            fds.push(disk.commit_log_file.as_raw_fd());
            fds.push(disk.catalog_file.as_raw_fd());
            if let Some(active) = disk.active_segment.as_ref() {
                fds.push(active.file.as_raw_fd());
            }
            self.io_backend.refresh_registered_files(&fds)?;
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.io_backend.refresh_registered_files(&[])?;
        }
        Ok(())
    }

    fn open_new_active_segment(&mut self, segment_id: u64) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let segment_path = segment_data_path(
            &disk.segments_path,
            segment_id,
            self.config.segment_generation,
        );

        let (mut file, created_new) = if segment_path.exists() {
            (
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&segment_path)
                    .map_err(|source| ArchiveRecorderError::Io {
                        operation: "open preallocated segment",
                        path: segment_path.clone(),
                        source,
                    })?,
                false,
            )
        } else {
            let (file, _) = create_new_file(&segment_path)?;
            (file, true)
        };

        write_archive_header(
            &mut file,
            &segment_path,
            ArchiveFileKind::SegmentData,
            self.config.log_id,
            segment_id,
            self.config.segment_generation,
        )?;

        if self.config.segment_preallocate {
            self.io_backend.set_len(
                &mut file,
                &segment_path,
                self.config.segment_bytes as u64,
                "preallocate active segment",
            )?;

            if created_new {
                self.stats.preallocated_segments += 1;
            }
        }

        disk.active_segment = Some(ActiveSegment {
            segment_id,
            segment_generation: self.config.segment_generation,
            created_at_ns: now_ns(),
            write_offset: ARCHIVE_FILE_HEADER_V1_LEN as u64,
            sequence_start: None,
            sequence_end: None,
            records: 0,
            file,
        });

        self.refresh_backend_registered_files()?;
        self.create_spare_preallocated_segments(segment_id + 1)?;
        Ok(())
    }

    fn create_spare_preallocated_segments(
        &mut self,
        start_segment_id: u64,
    ) -> Result<(), ArchiveRecorderError> {
        if !self.config.segment_preallocate || self.config.spare_preallocated_segments == 0 {
            return Ok(());
        }

        let disk = self.disk.as_mut().expect("disk state must exist");
        let spare_count = self.config.spare_preallocated_segments as u64;
        for segment_id in start_segment_id..start_segment_id + spare_count {
            let path = segment_data_path(
                &disk.segments_path,
                segment_id,
                self.config.segment_generation,
            );
            if path.exists() {
                continue;
            }

            let (mut file, _) = create_new_file(&path)?;
            write_archive_header(
                &mut file,
                &path,
                ArchiveFileKind::SegmentData,
                self.config.log_id,
                segment_id,
                self.config.segment_generation,
            )?;
            self.io_backend.set_len(
                &mut file,
                &path,
                self.config.segment_bytes as u64,
                "preallocate spare segment",
            )?;
            self.stats.preallocated_segments += 1;
        }

        Ok(())
    }

    fn seal_active_segment_internal(
        &mut self,
        open_next: bool,
    ) -> Result<(), ArchiveRecorderError> {
        let disk = self.disk.as_mut().expect("disk state must exist");
        let mut active = match disk.active_segment.take() {
            Some(value) => value,
            None => return Ok(()),
        };

        if self.config.persistence_mode != PersistenceMode::Volatile {
            let segment_path = segment_data_path(
                &disk.segments_path,
                active.segment_id,
                active.segment_generation,
            );
            self.io_backend
                .flush(&mut active.file, &segment_path, "flush segment before seal")?;
            self.io_backend.sync_data(
                &mut active.file,
                &segment_path,
                "sync segment before seal",
            )?;
        }

        if active.records > 0 {
            let summary = SegmentSummary {
                segment_id: active.segment_id,
                segment_generation: active.segment_generation,
                sequence_start: active.sequence_start.unwrap_or(0),
                sequence_end: active.sequence_end.unwrap_or(0),
                records: active.records,
                created_at_ns: active.created_at_ns,
                sealed_at_ns: now_ns(),
                data_bytes_used: active.write_offset,
                segment_checksum: 0,
            };

            let segment_meta_path = segment_meta_path(
                &disk.segments_path,
                active.segment_id,
                active.segment_generation,
            );
            let (mut meta_file, _) = create_new_file(&segment_meta_path)?;
            write_archive_header(
                &mut meta_file,
                &segment_meta_path,
                ArchiveFileKind::SegmentMeta,
                self.config.log_id,
                active.segment_id,
                active.segment_generation,
            )?;

            let summary_bytes = summary.to_bytes();
            self.io_backend.write_all_at(
                &mut meta_file,
                &segment_meta_path,
                ARCHIVE_FILE_HEADER_V1_LEN as u64,
                &summary_bytes,
                "write segment summary",
            )?;
            self.io_backend.flush_pending()?;
            self.stats.metadata_bytes_written +=
                ARCHIVE_FILE_HEADER_V1_LEN as u64 + summary_bytes.len() as u64;

            let catalog_write_offset = disk
                .catalog_file
                .metadata()
                .map_err(|source| ArchiveRecorderError::Io {
                    operation: "read catalog metadata",
                    path: disk.catalog_path.clone(),
                    source,
                })?
                .len();
            self.io_backend.write_all_at(
                &mut disk.catalog_file,
                &disk.catalog_path,
                catalog_write_offset,
                &summary_bytes,
                "append catalog segment summary",
            )?;
            self.io_backend.flush_pending()?;
            self.stats.metadata_bytes_written += summary_bytes.len() as u64;
            self.stats.rolled_segments += 1;
        }

        if open_next {
            self.open_new_active_segment(active.segment_id + 1)?;
        }

        self.enforce_retention_cap()?;

        Ok(())
    }
}

fn overlaps_any_pin(sequence_start: u64, sequence_end: u64, pins: &[ReplayPin]) -> bool {
    pins.iter()
        .any(|pin| pin.sequence_start <= sequence_end && sequence_start <= pin.sequence_end)
}

fn read_log_id_from_archive_header(
    path: &Path,
    expected_kind: ArchiveFileKind,
) -> Result<[u8; 16], ArchiveRecorderError> {
    let mut file = File::open(path).map_err(|source| ArchiveRecorderError::Io {
        operation: "open archive header for recovery",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveRecorderError::Io {
            operation: "read archive header for recovery",
            path: path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != expected_kind {
        return Err(ArchiveRecorderError::RecoveryInconsistent(
            "archive file header has unexpected file kind",
        ));
    }
    Ok(header.log_id)
}

fn generate_random_log_id() -> Result<[u8; 16], ArchiveRecorderError> {
    let mut log_id = [0u8; 16];
    getrandom::getrandom(&mut log_id).map_err(|_| {
        ArchiveRecorderError::InvalidConfiguration("failed to generate random archive log_id")
    })?;
    if log_id == ZERO_LOG_ID {
        // Reserve all-zero as \"unset\" sentinel.
        log_id[0] = 1;
    }
    Ok(log_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline_recorder_config() -> RecorderConfig {
        RecorderConfig {
            profile: RecorderProfile::Balanced,
            storage_path: PathBuf::from("/tmp/unused"),
            metadata_log_path: PathBuf::from("/tmp/unused"),
            segment_bytes: 1024,
            segment_preallocate: true,
            spare_preallocated_segments: 1,
            metadata_log_preallocate_entries: DEFAULT_METADATA_LOG_PREALLOCATE_ENTRIES,
            persistence_mode: PersistenceMode::Async,
            async_io_backend: AsyncIoBackend::Blocking,
            io_uring_queue_depth: 8,
            io_submit_batch_max: 8,
            io_cqe_batch_max: 8,
            io_uring_register_files: false,
            checksum_mode: ChecksumMode::Crc32c,
            out_of_space_policy: OutOfSpacePolicy::FailWriter,
            max_disk_bytes: None,
            metadata_log_roll_bytes: 1024 * 1024,
            metadata_log_max_bytes: 16 * 1024 * 1024,
            log_id: [0u8; 16],
            segment_generation: 1,
        }
    }

    fn baseline_recorder() -> ArchiveRecorder {
        let (io_backend, effective_async_io_backend) =
            RecorderIoBackend::create(AsyncIoBackend::Blocking, 8, 8, 8, false)
                .expect("blocking backend");
        ArchiveRecorder {
            config: baseline_recorder_config(),
            io_backend,
            effective_async_io_backend,
            disk: None,
            stats: ArchiveRecorderStats::default(),
            recovery_status: ArchiveRecoveryStatus::default(),
            next_commit_ordinal: 1,
            last_sequence: None,
            last_durable_data_sequence: None,
            last_durable_commit_ordinal: None,
            index_by_sequence: BTreeMap::new(),
            volatile_records: Vec::new(),
            finalized: false,
            degraded: false,
        }
    }

    #[test]
    fn fail_writer_policy_marks_recorder_degraded_on_enospc() {
        let mut recorder = baseline_recorder();
        let path = Path::new("/tmp/segment-1-g0.data");

        let result = recorder.handle_write_failure(path, std::io::Error::from_raw_os_error(28));
        assert!(matches!(result, Err(ArchiveRecorderError::OutOfSpace(_))));
        assert!(recorder.degraded);
        assert_eq!(recorder.stats.out_of_space_events, 1);
    }

    #[test]
    fn non_enospc_write_failures_return_io_error_and_mark_degraded() {
        let mut recorder = baseline_recorder();
        let path = Path::new("/tmp/segment-1-g0.data");

        let result = recorder.handle_write_failure(path, std::io::Error::from_raw_os_error(5));
        assert!(matches!(result, Err(ArchiveRecorderError::Io { .. })));
        assert!(recorder.degraded);
        assert_eq!(recorder.stats.out_of_space_events, 0);
    }
}
