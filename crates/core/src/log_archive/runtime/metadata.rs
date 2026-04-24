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
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;

use crate::log_archive::{
    ARCHIVE_FILE_HEADER_V1_LEN, ArchiveFileHeaderError, ArchiveFileHeaderV1, ArchiveFileKind,
};

use super::common::{
    ArchiveLocator, ArchiveReplayError, ArchiveSourcePattern, ReplayedFrame, read_u16, read_u32,
    read_u64,
};
use super::replayer::ArchiveReplayer;
use super::storage::read_commit_entries;

const WATERMARK_MAGIC: [u8; 4] = *b"WMK1";
const WATERMARK_LEN: usize = 24;
const WATERMARK_OFFSET_MAGIC: usize = 0;
const WATERMARK_OFFSET_MAJOR: usize = 4;
const WATERMARK_OFFSET_MINOR: usize = 6;
const WATERMARK_OFFSET_LAST_COMMIT: usize = 8;
const WATERMARK_OFFSET_LAST_INDEXED: usize = 16;

const CORE_LOCATOR_INDEX_HEADER_MAGIC: [u8; 4] = *b"CLX1";
const CORE_LOCATOR_INDEX_HEADER_LEN: usize = 16;
const CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC: usize = 0;
const CORE_LOCATOR_INDEX_HEADER_OFFSET_MAJOR: usize = 4;
const CORE_LOCATOR_INDEX_HEADER_OFFSET_MINOR: usize = 6;
const CORE_LOCATOR_INDEX_HEADER_OFFSET_ENTRY_LEN: usize = 8;

const CORE_LOCATOR_INDEX_ENTRY_MAGIC: [u8; 4] = *b"CIX1";
const CORE_LOCATOR_INDEX_ENTRY_LEN: usize = 104;
const CORE_LOCATOR_INDEX_OFFSET_MAGIC: usize = 0;
const CORE_LOCATOR_INDEX_OFFSET_ENTRY_LEN: usize = 4;
const CORE_LOCATOR_INDEX_OFFSET_FLAGS: usize = 6;
const CORE_LOCATOR_INDEX_OFFSET_COMMIT_ORDINAL: usize = 8;
const CORE_LOCATOR_INDEX_OFFSET_SEQUENCE: usize = 16;
const CORE_LOCATOR_INDEX_OFFSET_SEGMENT_ID: usize = 24;
const CORE_LOCATOR_INDEX_OFFSET_SEGMENT_GENERATION: usize = 32;
const CORE_LOCATOR_INDEX_OFFSET_FILE_OFFSET: usize = 40;
const CORE_LOCATOR_INDEX_OFFSET_FRAME_LEN: usize = 48;
const CORE_LOCATOR_INDEX_OFFSET_FRAME_CHECKSUM: usize = 52;
const CORE_LOCATOR_INDEX_OFFSET_EVENT_TIME_NS: usize = 56;
const CORE_LOCATOR_INDEX_OFFSET_COMMIT_TIME_NS: usize = 64;
const CORE_LOCATOR_INDEX_OFFSET_SOURCE_SERVICE_ID: usize = 72;
const CORE_LOCATOR_INDEX_OFFSET_SOURCE_INSTANCE_ID: usize = 80;
const CORE_LOCATOR_INDEX_OFFSET_SOURCE_SEQUENCE: usize = 88;
const CORE_LOCATOR_INDEX_OFFSET_SOURCE_PATTERN: usize = 96;
const CORE_LOCATOR_INDEX_OFFSET_SOURCE_FLAGS: usize = 97;
const CORE_LOCATOR_INDEX_FLAG_SOURCE_SEQUENCE_PRESENT: u16 = 0x0001;

/// Metadata schema major version used by the indexer contract.
pub const METADATA_SCHEMA_VERSION_MAJOR: u16 = 1;
/// Metadata schema minor version used by the indexer contract.
pub const METADATA_SCHEMA_VERSION_MINOR: u16 = 0;

/// Query readiness mode reported by metadata indexer status.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueryReadinessMode {
    /// Query surfaces are unavailable until an external indexer catches up.
    Unavailable,
    /// Query surfaces are served by an external metadata indexer/sink.
    IndexerBacked,
    /// Query surfaces are served by the optional built-in `core-locator.idx`.
    CoreLocatorIndex,
}

/// Watermark contract for query readiness.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct MetadataWatermark {
    /// Recorder durable metadata boundary (`commit.idxlog`).
    pub last_commit_ordinal: u64,
    /// Queryable metadata boundary.
    pub last_indexed_commit_ordinal: u64,
}

impl MetadataWatermark {
    /// Returns the query watermark (`last_indexed_commit_ordinal`).
    pub fn query_watermark(&self) -> u64 {
        self.last_indexed_commit_ordinal
    }
}

/// Canonical metadata record keyed by archive locator.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MetadataCommitRecord {
    /// Stable log identity.
    pub log_id: [u8; 16],
    /// Recorder-local commit ordering key.
    pub commit_ordinal: u64,
    /// Source sequence.
    pub sequence: u64,
    /// Canonical archive locator.
    pub locator: ArchiveLocator,
    /// Frame checksum as recorded in `commit.idxlog`.
    pub frame_checksum: u32,
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// Commit timestamp in nanoseconds.
    pub commit_time_ns: u64,
    /// Source pattern (`log`, `publish_subscribe`, `pipeline`).
    pub source_pattern: ArchiveSourcePattern,
    /// Stable source service identity (`0` when unavailable).
    pub source_service_id: u64,
    /// Pattern-local source identity (`0` when unavailable).
    pub source_instance_id: u64,
    /// Optional source sequence when available.
    pub source_sequence: Option<u64>,
}

/// Indexer admin/status snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArchiveMetadataIndexerStatus {
    /// Active readiness mode.
    pub query_readiness_mode: QueryReadinessMode,
    /// Current watermark values.
    pub watermark: MetadataWatermark,
    /// Metadata-log root path.
    pub metadata_log_path: PathBuf,
    /// Commit-log path.
    pub commit_log_path: PathBuf,
    /// Optional core-locator index path.
    pub core_locator_index_path: Option<PathBuf>,
}

/// Error payload returned by metadata sinks.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArchiveMetadataSinkError {
    /// Human-readable sink failure details.
    pub details: String,
}

impl ArchiveMetadataSinkError {
    /// Creates a new sink error.
    pub fn new(details: impl Into<String>) -> Self {
        Self {
            details: details.into(),
        }
    }
}

impl core::fmt::Display for ArchiveMetadataSinkError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveMetadataSinkError({})", self.details)
    }
}

impl std::error::Error for ArchiveMetadataSinkError {}

/// Sink interface for metadata index materialization.
pub trait ArchiveMetadataSink: Send {
    /// Consumes one index batch.
    fn on_records(
        &mut self,
        records: &[MetadataCommitRecord],
    ) -> Result<(), ArchiveMetadataSinkError>;

    /// Flushes sink state.
    fn flush(&mut self) -> Result<(), ArchiveMetadataSinkError> {
        Ok(())
    }
}

/// Errors returned by [`ArchiveMetadataIndexer`] operations.
#[derive(Debug)]
pub enum ArchiveMetadataIndexerError {
    /// Invalid configuration value.
    InvalidConfiguration(&'static str),
    /// Required commit-log file is missing.
    MissingCommitLog(PathBuf),
    /// Invalid archive file header in metadata-log file.
    FileHeader(ArchiveFileHeaderError),
    /// Commit-log parsing/replay error.
    Replay(ArchiveReplayError),
    /// Corrupted watermark or core index file.
    Corrupted(&'static str),
    /// Metadata sink error.
    Sink(ArchiveMetadataSinkError),
    /// Underlying I/O failure.
    Io {
        /// Operation name.
        operation: &'static str,
        /// Path involved in operation.
        path: PathBuf,
        /// Source error.
        source: std::io::Error,
    },
}

impl core::fmt::Display for ArchiveMetadataIndexerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveMetadataIndexerError::{self:?}")
    }
}

impl std::error::Error for ArchiveMetadataIndexerError {}

impl From<ArchiveFileHeaderError> for ArchiveMetadataIndexerError {
    fn from(value: ArchiveFileHeaderError) -> Self {
        Self::FileHeader(value)
    }
}

impl From<ArchiveReplayError> for ArchiveMetadataIndexerError {
    fn from(value: ArchiveReplayError) -> Self {
        Self::Replay(value)
    }
}

impl From<ArchiveMetadataSinkError> for ArchiveMetadataIndexerError {
    fn from(value: ArchiveMetadataSinkError) -> Self {
        Self::Sink(value)
    }
}

/// Errors returned by metadata query operations.
#[derive(Debug)]
pub enum MetadataQueryError {
    /// Query target is above current query watermark.
    NotIndexedYet {
        /// Sequence requested by caller when available.
        requested_sequence: Option<u64>,
        /// Locator requested by caller when available.
        requested_locator: Option<ArchiveLocator>,
        /// Current query watermark.
        query_watermark: u64,
        /// Current recorder durable boundary.
        last_commit_ordinal: u64,
    },
    /// Sequence is not available within indexed history.
    NotAvailableSequence(u64),
    /// Locator is not available within indexed history.
    NotAvailableLocator(ArchiveLocator),
    /// Underlying indexer error.
    Indexer(ArchiveMetadataIndexerError),
}

impl core::fmt::Display for MetadataQueryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "MetadataQueryError::{self:?}")
    }
}

impl std::error::Error for MetadataQueryError {}

impl From<ArchiveMetadataIndexerError> for MetadataQueryError {
    fn from(value: ArchiveMetadataIndexerError) -> Self {
        Self::Indexer(value)
    }
}

/// Builder for [`ArchiveMetadataIndexer`].
pub struct ArchiveMetadataIndexerBuilder {
    storage_path: PathBuf,
    metadata_log_path: Option<PathBuf>,
    watermark_path: Option<PathBuf>,
    core_locator_index_path: Option<PathBuf>,
    enable_core_locator_index: bool,
    sink: Option<Box<dyn ArchiveMetadataSink>>,
    query_readiness_mode: Option<QueryReadinessMode>,
}

impl ArchiveMetadataIndexerBuilder {
    /// Creates a metadata indexer builder.
    pub fn new(storage_path: &Path) -> Self {
        Self {
            storage_path: storage_path.to_path_buf(),
            metadata_log_path: None,
            watermark_path: None,
            core_locator_index_path: None,
            enable_core_locator_index: false,
            sink: None,
            query_readiness_mode: None,
        }
    }

    /// Overrides metadata-log root path.
    pub fn metadata_log_path(mut self, value: &Path) -> Self {
        self.metadata_log_path = Some(value.to_path_buf());
        self
    }

    /// Overrides persisted watermark path.
    pub fn watermark_path(mut self, value: &Path) -> Self {
        self.watermark_path = Some(value.to_path_buf());
        self
    }

    /// Overrides optional `core-locator.idx` path.
    pub fn core_locator_index_path(mut self, value: &Path) -> Self {
        self.core_locator_index_path = Some(value.to_path_buf());
        self
    }

    /// Enables/disables built-in `core-locator.idx` updates.
    pub fn enable_core_locator_index(mut self, value: bool) -> Self {
        self.enable_core_locator_index = value;
        self
    }

    /// Configures metadata sink implementation.
    pub fn sink(mut self, value: Box<dyn ArchiveMetadataSink>) -> Self {
        self.sink = Some(value);
        self
    }

    /// Overrides query readiness mode reported by status.
    pub fn query_readiness_mode(mut self, value: QueryReadinessMode) -> Self {
        self.query_readiness_mode = Some(value);
        self
    }

    /// Opens metadata indexer state.
    pub fn open(self) -> Result<ArchiveMetadataIndexer, ArchiveMetadataIndexerError> {
        let metadata_log_path = self
            .metadata_log_path
            .clone()
            .unwrap_or_else(|| self.storage_path.clone());
        let commit_log_path = metadata_log_path.join("commit.idxlog");
        if !commit_log_path.exists() {
            return Err(ArchiveMetadataIndexerError::MissingCommitLog(
                commit_log_path,
            ));
        }

        let watermark_path = self
            .watermark_path
            .clone()
            .unwrap_or_else(|| metadata_log_path.join("indexer.watermark"));
        let core_locator_index_path = match self.enable_core_locator_index {
            true => Some(
                self.core_locator_index_path
                    .clone()
                    .unwrap_or_else(|| self.storage_path.join("core-locator.idx")),
            ),
            false => self.core_locator_index_path.clone(),
        };

        let query_readiness_mode = match self.query_readiness_mode {
            Some(mode) => mode,
            None => {
                if core_locator_index_path.is_some() {
                    QueryReadinessMode::CoreLocatorIndex
                } else if self.sink.is_some() {
                    QueryReadinessMode::IndexerBacked
                } else {
                    QueryReadinessMode::Unavailable
                }
            }
        };

        let log_id = read_commit_log_header_log_id(&commit_log_path)?;
        let mut indexer = ArchiveMetadataIndexer {
            metadata_log_path,
            commit_log_path,
            watermark_path,
            core_locator_index_path,
            query_readiness_mode,
            sink: self.sink,
            log_id,
            index_by_sequence: BTreeMap::new(),
            index_by_locator: BTreeMap::new(),
            watermark: MetadataWatermark::default(),
        };
        indexer.load_persisted_state()?;
        Ok(indexer)
    }
}

/// Metadata indexer for `commit.idxlog` ingestion and query watermark reporting.
pub struct ArchiveMetadataIndexer {
    metadata_log_path: PathBuf,
    commit_log_path: PathBuf,
    watermark_path: PathBuf,
    core_locator_index_path: Option<PathBuf>,
    query_readiness_mode: QueryReadinessMode,
    sink: Option<Box<dyn ArchiveMetadataSink>>,
    log_id: [u8; 16],
    index_by_sequence: BTreeMap<u64, MetadataCommitRecord>,
    index_by_locator: BTreeMap<ArchiveLocator, MetadataCommitRecord>,
    watermark: MetadataWatermark,
}

impl ArchiveMetadataIndexer {
    /// Returns schema version `(major, minor)`.
    pub fn schema_version(&self) -> (u16, u16) {
        (METADATA_SCHEMA_VERSION_MAJOR, METADATA_SCHEMA_VERSION_MINOR)
    }

    /// Returns current query readiness mode.
    pub fn query_readiness_mode(&self) -> QueryReadinessMode {
        self.query_readiness_mode
    }

    /// Returns current watermark values.
    pub fn watermark(&self) -> MetadataWatermark {
        self.watermark
    }

    /// Returns admin/status view.
    pub fn status(&self) -> ArchiveMetadataIndexerStatus {
        ArchiveMetadataIndexerStatus {
            query_readiness_mode: self.query_readiness_mode,
            watermark: self.watermark,
            metadata_log_path: self.metadata_log_path.clone(),
            commit_log_path: self.commit_log_path.clone(),
            core_locator_index_path: self.core_locator_index_path.clone(),
        }
    }

    /// Performs one catch-up cycle from `commit.idxlog`.
    pub fn catch_up_once(&mut self) -> Result<usize, ArchiveMetadataIndexerError> {
        self.catch_up_with_limit(None)
    }

    /// Performs one catch-up cycle while capping ingested records.
    ///
    /// This supports continuous indexing loops with bounded per-iteration work.
    pub fn catch_up_with_limit(
        &mut self,
        max_new_records: Option<usize>,
    ) -> Result<usize, ArchiveMetadataIndexerError> {
        let entries = read_commit_entries(&self.commit_log_path)?;
        let last_commit_ordinal = entries
            .last()
            .map(|entry| entry.commit_ordinal)
            .unwrap_or(0);
        let mut new_records = Vec::new();
        let mut last_indexed = self.watermark.last_indexed_commit_ordinal;
        let mut processed = 0usize;
        let limit = max_new_records.unwrap_or(usize::MAX);

        for entry in entries {
            if entry.commit_ordinal <= self.watermark.last_indexed_commit_ordinal {
                continue;
            }
            if processed >= limit {
                break;
            }
            let record = MetadataCommitRecord {
                log_id: self.log_id,
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
            };
            self.index_by_sequence.insert(record.sequence, record);
            self.index_by_locator.insert(record.locator, record);
            new_records.push(record);
            processed += 1;
            last_indexed = entry.commit_ordinal;
        }

        if !new_records.is_empty() {
            if let Some(path) = self.core_locator_index_path.as_ref() {
                append_core_locator_index(path, &new_records)?;
            }
            if let Some(sink) = self.sink.as_mut() {
                sink.on_records(&new_records)?;
                sink.flush()?;
            }
            self.watermark.last_indexed_commit_ordinal = last_indexed;
        }
        self.watermark.last_commit_ordinal = last_commit_ordinal;
        self.persist_watermark()?;
        Ok(processed)
    }

    /// Runs the indexer continuously until `stop_condition` returns true.
    pub fn run_continuous<F>(
        &mut self,
        poll_interval: Duration,
        mut stop_condition: F,
    ) -> Result<(), ArchiveMetadataIndexerError>
    where
        F: FnMut(ArchiveMetadataIndexerStatus) -> bool,
    {
        loop {
            self.catch_up_once()?;
            let status = self.status();
            if stop_condition(status) {
                return Ok(());
            }
            thread::sleep(poll_interval);
        }
    }

    /// Rebuilds index state from scratch in an idempotent way.
    pub fn reindex(&mut self) -> Result<(), ArchiveMetadataIndexerError> {
        self.index_by_sequence.clear();
        self.index_by_locator.clear();
        self.watermark = MetadataWatermark::default();

        if let Some(path) = self.core_locator_index_path.as_ref() {
            reset_core_locator_index(path)?;
        }
        self.persist_watermark()?;
        let _ = self.catch_up_once()?;
        Ok(())
    }

    /// Resolves metadata record by source sequence.
    pub fn query_by_sequence(
        &self,
        sequence: u64,
    ) -> Result<MetadataCommitRecord, MetadataQueryError> {
        if sequence > self.watermark.query_watermark() {
            return Err(MetadataQueryError::NotIndexedYet {
                requested_sequence: Some(sequence),
                requested_locator: None,
                query_watermark: self.watermark.query_watermark(),
                last_commit_ordinal: self.watermark.last_commit_ordinal,
            });
        }

        self.index_by_sequence
            .get(&sequence)
            .copied()
            .ok_or(MetadataQueryError::NotAvailableSequence(sequence))
    }

    /// Resolves metadata record by physical locator.
    pub fn query_by_locator(
        &self,
        locator: ArchiveLocator,
    ) -> Result<MetadataCommitRecord, MetadataQueryError> {
        if let Some(value) = self.index_by_locator.get(&locator) {
            return Ok(*value);
        }

        if self.watermark.last_indexed_commit_ordinal < self.watermark.last_commit_ordinal {
            return Err(MetadataQueryError::NotIndexedYet {
                requested_sequence: None,
                requested_locator: Some(locator),
                query_watermark: self.watermark.query_watermark(),
                last_commit_ordinal: self.watermark.last_commit_ordinal,
            });
        }

        Err(MetadataQueryError::NotAvailableLocator(locator))
    }

    /// Replays a frame through locator query resolution.
    pub fn replay_by_sequence(
        &self,
        replayer: &ArchiveReplayer,
        sequence: u64,
    ) -> Result<ReplayedFrame, MetadataQueryError> {
        let metadata = self.query_by_sequence(sequence)?;
        replayer
            .read_at_locator(metadata.locator)
            .map_err(|err| MetadataQueryError::Indexer(ArchiveMetadataIndexerError::Replay(err)))
    }

    fn load_persisted_state(&mut self) -> Result<(), ArchiveMetadataIndexerError> {
        self.watermark = load_watermark(&self.watermark_path)?;

        if let Some(path) = self.core_locator_index_path.as_ref() {
            let records = load_core_locator_index(path, self.log_id)?;
            let mut highest = 0u64;
            for record in records {
                self.index_by_sequence.insert(record.sequence, record);
                self.index_by_locator.insert(record.locator, record);
                highest = highest.max(record.commit_ordinal);
            }
            self.watermark.last_indexed_commit_ordinal =
                self.watermark.last_indexed_commit_ordinal.max(highest);
        }

        let entries = read_commit_entries(&self.commit_log_path)?;
        self.watermark.last_commit_ordinal = entries
            .last()
            .map(|entry| entry.commit_ordinal)
            .unwrap_or(0);
        Ok(())
    }

    fn persist_watermark(&self) -> Result<(), ArchiveMetadataIndexerError> {
        persist_watermark(&self.watermark_path, self.watermark)
    }
}

fn read_commit_log_header_log_id(path: &Path) -> Result<[u8; 16], ArchiveMetadataIndexerError> {
    let mut file = File::open(path).map_err(|source| ArchiveMetadataIndexerError::Io {
        operation: "open commit idxlog for metadata schema",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header_bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "read commit idxlog header for metadata schema",
            path: path.to_path_buf(),
            source,
        })?;
    let header = ArchiveFileHeaderV1::from_bytes(&header_bytes)?;
    if header.file_kind != ArchiveFileKind::CommitIdxLog {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "metadata schema expected commit idxlog file kind",
        ));
    }
    Ok(header.log_id)
}

fn load_watermark(path: &Path) -> Result<MetadataWatermark, ArchiveMetadataIndexerError> {
    if !path.exists() {
        return Ok(MetadataWatermark::default());
    }

    let mut file = File::open(path).map_err(|source| ArchiveMetadataIndexerError::Io {
        operation: "open watermark file",
        path: path.to_path_buf(),
        source,
    })?;
    let mut bytes = [0u8; WATERMARK_LEN];
    file.read_exact(&mut bytes)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "read watermark file",
            path: path.to_path_buf(),
            source,
        })?;

    let decoded_magic = [
        bytes[WATERMARK_OFFSET_MAGIC],
        bytes[WATERMARK_OFFSET_MAGIC + 1],
        bytes[WATERMARK_OFFSET_MAGIC + 2],
        bytes[WATERMARK_OFFSET_MAGIC + 3],
    ];
    if decoded_magic != WATERMARK_MAGIC {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "invalid watermark file magic",
        ));
    }
    let major = read_u16(&bytes, WATERMARK_OFFSET_MAJOR);
    if major != METADATA_SCHEMA_VERSION_MAJOR {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "unsupported watermark major version",
        ));
    }

    Ok(MetadataWatermark {
        last_commit_ordinal: read_u64(&bytes, WATERMARK_OFFSET_LAST_COMMIT),
        last_indexed_commit_ordinal: read_u64(&bytes, WATERMARK_OFFSET_LAST_INDEXED),
    })
}

fn persist_watermark(
    path: &Path,
    watermark: MetadataWatermark,
) -> Result<(), ArchiveMetadataIndexerError> {
    let parent = path
        .parent()
        .ok_or(ArchiveMetadataIndexerError::InvalidConfiguration(
            "watermark path must have parent directory",
        ))?;
    fs::create_dir_all(parent).map_err(|source| ArchiveMetadataIndexerError::Io {
        operation: "create watermark directory",
        path: parent.to_path_buf(),
        source,
    })?;

    let mut bytes = [0u8; WATERMARK_LEN];
    bytes[WATERMARK_OFFSET_MAGIC..WATERMARK_OFFSET_MAGIC + 4].copy_from_slice(&WATERMARK_MAGIC);
    bytes[WATERMARK_OFFSET_MAJOR..WATERMARK_OFFSET_MAJOR + 2]
        .copy_from_slice(&METADATA_SCHEMA_VERSION_MAJOR.to_le_bytes());
    bytes[WATERMARK_OFFSET_MINOR..WATERMARK_OFFSET_MINOR + 2]
        .copy_from_slice(&METADATA_SCHEMA_VERSION_MINOR.to_le_bytes());
    bytes[WATERMARK_OFFSET_LAST_COMMIT..WATERMARK_OFFSET_LAST_COMMIT + 8]
        .copy_from_slice(&watermark.last_commit_ordinal.to_le_bytes());
    bytes[WATERMARK_OFFSET_LAST_INDEXED..WATERMARK_OFFSET_LAST_INDEXED + 8]
        .copy_from_slice(&watermark.last_indexed_commit_ordinal.to_le_bytes());

    let temp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "open watermark temp file",
            path: temp_path.clone(),
            source,
        })?;
    file.write_all(&bytes)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "write watermark temp file",
            path: temp_path.clone(),
            source,
        })?;
    file.flush()
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "flush watermark temp file",
            path: temp_path.clone(),
            source,
        })?;

    fs::rename(&temp_path, path).map_err(|source| ArchiveMetadataIndexerError::Io {
        operation: "commit watermark file",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn reset_core_locator_index(path: &Path) -> Result<(), ArchiveMetadataIndexerError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "create core-locator index directory",
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "reset core-locator index",
            path: path.to_path_buf(),
            source,
        })?;
    let mut header = [0u8; CORE_LOCATOR_INDEX_HEADER_LEN];
    header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC..CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC + 4]
        .copy_from_slice(&CORE_LOCATOR_INDEX_HEADER_MAGIC);
    header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAJOR..CORE_LOCATOR_INDEX_HEADER_OFFSET_MAJOR + 2]
        .copy_from_slice(&METADATA_SCHEMA_VERSION_MAJOR.to_le_bytes());
    header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MINOR..CORE_LOCATOR_INDEX_HEADER_OFFSET_MINOR + 2]
        .copy_from_slice(&METADATA_SCHEMA_VERSION_MINOR.to_le_bytes());
    header[CORE_LOCATOR_INDEX_HEADER_OFFSET_ENTRY_LEN
        ..CORE_LOCATOR_INDEX_HEADER_OFFSET_ENTRY_LEN + 2]
        .copy_from_slice(&(CORE_LOCATOR_INDEX_ENTRY_LEN as u16).to_le_bytes());
    file.write_all(&header)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "write core-locator index header",
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

fn append_core_locator_index(
    path: &Path,
    records: &[MetadataCommitRecord],
) -> Result<(), ArchiveMetadataIndexerError> {
    if !path.exists() {
        reset_core_locator_index(path)?;
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "open core-locator index for append",
            path: path.to_path_buf(),
            source,
        })?;
    file.seek(SeekFrom::End(0))
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "seek core-locator index end",
            path: path.to_path_buf(),
            source,
        })?;

    for record in records {
        let mut bytes = [0u8; CORE_LOCATOR_INDEX_ENTRY_LEN];
        bytes[CORE_LOCATOR_INDEX_OFFSET_MAGIC..CORE_LOCATOR_INDEX_OFFSET_MAGIC + 4]
            .copy_from_slice(&CORE_LOCATOR_INDEX_ENTRY_MAGIC);
        bytes[CORE_LOCATOR_INDEX_OFFSET_ENTRY_LEN..CORE_LOCATOR_INDEX_OFFSET_ENTRY_LEN + 2]
            .copy_from_slice(&(CORE_LOCATOR_INDEX_ENTRY_LEN as u16).to_le_bytes());
        let mut flags = 0u16;
        if record.source_sequence.is_some() {
            flags |= CORE_LOCATOR_INDEX_FLAG_SOURCE_SEQUENCE_PRESENT;
        }
        bytes[CORE_LOCATOR_INDEX_OFFSET_FLAGS..CORE_LOCATOR_INDEX_OFFSET_FLAGS + 2]
            .copy_from_slice(&flags.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_COMMIT_ORDINAL
            ..CORE_LOCATOR_INDEX_OFFSET_COMMIT_ORDINAL + 8]
            .copy_from_slice(&record.commit_ordinal.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SEQUENCE..CORE_LOCATOR_INDEX_OFFSET_SEQUENCE + 8]
            .copy_from_slice(&record.sequence.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SEGMENT_ID..CORE_LOCATOR_INDEX_OFFSET_SEGMENT_ID + 8]
            .copy_from_slice(&record.locator.segment_id.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SEGMENT_GENERATION
            ..CORE_LOCATOR_INDEX_OFFSET_SEGMENT_GENERATION + 4]
            .copy_from_slice(&record.locator.segment_generation.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_FILE_OFFSET..CORE_LOCATOR_INDEX_OFFSET_FILE_OFFSET + 8]
            .copy_from_slice(&record.locator.file_offset.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_FRAME_LEN..CORE_LOCATOR_INDEX_OFFSET_FRAME_LEN + 4]
            .copy_from_slice(&record.locator.frame_len.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_FRAME_CHECKSUM
            ..CORE_LOCATOR_INDEX_OFFSET_FRAME_CHECKSUM + 4]
            .copy_from_slice(&record.frame_checksum.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_EVENT_TIME_NS..CORE_LOCATOR_INDEX_OFFSET_EVENT_TIME_NS + 8]
            .copy_from_slice(&record.event_time_ns.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_COMMIT_TIME_NS
            ..CORE_LOCATOR_INDEX_OFFSET_COMMIT_TIME_NS + 8]
            .copy_from_slice(&record.commit_time_ns.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_SERVICE_ID
            ..CORE_LOCATOR_INDEX_OFFSET_SOURCE_SERVICE_ID + 8]
            .copy_from_slice(&record.source_service_id.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_INSTANCE_ID
            ..CORE_LOCATOR_INDEX_OFFSET_SOURCE_INSTANCE_ID + 8]
            .copy_from_slice(&record.source_instance_id.to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_SEQUENCE
            ..CORE_LOCATOR_INDEX_OFFSET_SOURCE_SEQUENCE + 8]
            .copy_from_slice(&record.source_sequence.unwrap_or(0).to_le_bytes());
        bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_PATTERN] = record.source_pattern as u8;
        bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_FLAGS] = if record.source_sequence.is_some() {
            1
        } else {
            0
        };

        file.write_all(&bytes)
            .map_err(|source| ArchiveMetadataIndexerError::Io {
                operation: "append core-locator index entry",
                path: path.to_path_buf(),
                source,
            })?;
    }

    file.flush()
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "flush core-locator index",
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

fn load_core_locator_index(
    path: &Path,
    expected_log_id: [u8; 16],
) -> Result<Vec<MetadataCommitRecord>, ArchiveMetadataIndexerError> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut file = File::open(path).map_err(|source| ArchiveMetadataIndexerError::Io {
        operation: "open core-locator index",
        path: path.to_path_buf(),
        source,
    })?;
    let mut header = [0u8; CORE_LOCATOR_INDEX_HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "read core-locator index header",
            path: path.to_path_buf(),
            source,
        })?;
    let header_magic = [
        header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC],
        header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC + 1],
        header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC + 2],
        header[CORE_LOCATOR_INDEX_HEADER_OFFSET_MAGIC + 3],
    ];
    if header_magic != CORE_LOCATOR_INDEX_HEADER_MAGIC {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "invalid core-locator index header magic",
        ));
    }
    let major = read_u16(&header, CORE_LOCATOR_INDEX_HEADER_OFFSET_MAJOR);
    if major != METADATA_SCHEMA_VERSION_MAJOR {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "unsupported core-locator index major version",
        ));
    }
    let entry_len_from_header =
        read_u16(&header, CORE_LOCATOR_INDEX_HEADER_OFFSET_ENTRY_LEN) as usize;
    if entry_len_from_header != CORE_LOCATOR_INDEX_ENTRY_LEN {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "unsupported core-locator index entry length",
        ));
    }

    let file_len = file
        .metadata()
        .map_err(|source| ArchiveMetadataIndexerError::Io {
            operation: "read core-locator index metadata",
            path: path.to_path_buf(),
            source,
        })?
        .len() as usize;
    let remaining = file_len.saturating_sub(CORE_LOCATOR_INDEX_HEADER_LEN);
    if remaining % entry_len_from_header != 0 {
        return Err(ArchiveMetadataIndexerError::Corrupted(
            "core-locator index trailing bytes are not aligned to entry length",
        ));
    }

    let mut records = Vec::with_capacity(remaining / entry_len_from_header);
    for _ in 0..(remaining / entry_len_from_header) {
        let mut bytes = vec![0u8; entry_len_from_header];
        file.read_exact(&mut bytes)
            .map_err(|source| ArchiveMetadataIndexerError::Io {
                operation: "read core-locator index entry",
                path: path.to_path_buf(),
                source,
            })?;

        let entry_magic = [
            bytes[CORE_LOCATOR_INDEX_OFFSET_MAGIC],
            bytes[CORE_LOCATOR_INDEX_OFFSET_MAGIC + 1],
            bytes[CORE_LOCATOR_INDEX_OFFSET_MAGIC + 2],
            bytes[CORE_LOCATOR_INDEX_OFFSET_MAGIC + 3],
        ];
        if entry_magic != CORE_LOCATOR_INDEX_ENTRY_MAGIC {
            return Err(ArchiveMetadataIndexerError::Corrupted(
                "invalid core-locator index entry magic",
            ));
        }
        if read_u16(&bytes, CORE_LOCATOR_INDEX_OFFSET_ENTRY_LEN) as usize != entry_len_from_header {
            return Err(ArchiveMetadataIndexerError::Corrupted(
                "invalid core-locator index entry length",
            ));
        }

        let flags = read_u16(&bytes, CORE_LOCATOR_INDEX_OFFSET_FLAGS);
        let source_pattern =
            ArchiveSourcePattern::try_from(bytes[CORE_LOCATOR_INDEX_OFFSET_SOURCE_PATTERN])
                .map_err(|_| ArchiveMetadataIndexerError::Corrupted("invalid source pattern"))?;
        let source_sequence = if (flags & CORE_LOCATOR_INDEX_FLAG_SOURCE_SEQUENCE_PRESENT) != 0 {
            Some(read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_SOURCE_SEQUENCE))
        } else {
            None
        };

        records.push(MetadataCommitRecord {
            log_id: expected_log_id,
            commit_ordinal: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_COMMIT_ORDINAL),
            sequence: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_SEQUENCE),
            locator: ArchiveLocator {
                segment_id: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_SEGMENT_ID),
                segment_generation: read_u32(&bytes, CORE_LOCATOR_INDEX_OFFSET_SEGMENT_GENERATION),
                file_offset: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_FILE_OFFSET),
                frame_len: read_u32(&bytes, CORE_LOCATOR_INDEX_OFFSET_FRAME_LEN),
            },
            frame_checksum: read_u32(&bytes, CORE_LOCATOR_INDEX_OFFSET_FRAME_CHECKSUM),
            event_time_ns: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_EVENT_TIME_NS),
            commit_time_ns: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_COMMIT_TIME_NS),
            source_pattern,
            source_service_id: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_SOURCE_SERVICE_ID),
            source_instance_id: read_u64(&bytes, CORE_LOCATOR_INDEX_OFFSET_SOURCE_INSTANCE_ID),
            source_sequence,
        });
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_roundtrip_persists_and_loads_values() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("indexer.watermark");
        let watermark = MetadataWatermark {
            last_commit_ordinal: 42,
            last_indexed_commit_ordinal: 40,
        };

        persist_watermark(&path, watermark).unwrap();
        let loaded = load_watermark(&path).unwrap();
        assert_eq!(loaded, watermark);
    }
}
