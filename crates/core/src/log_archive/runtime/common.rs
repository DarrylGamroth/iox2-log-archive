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
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use super::backend::RecorderIoBackend;
use crate::log_archive::ArchiveFileHeaderError;

pub(super) const FRAME_MAGIC: [u8; 4] = *b"LAR1";
pub(super) const FRAME_HEADER_LEN: usize = 64;
pub(super) const FRAME_FLAG_CHECKSUM_CRC32C: u16 = 0x0001;
pub(super) const FRAME_OFFSET_MAGIC: usize = 0;
pub(super) const FRAME_OFFSET_HEADER_LEN: usize = 4;
pub(super) const FRAME_OFFSET_FLAGS: usize = 6;
pub(super) const FRAME_OFFSET_FRAME_LEN: usize = 8;
pub(super) const FRAME_OFFSET_COMMIT_ORDINAL: usize = 16;
pub(super) const FRAME_OFFSET_SEQUENCE: usize = 24;
pub(super) const FRAME_OFFSET_EVENT_TIME_NS: usize = 32;
pub(super) const FRAME_OFFSET_COMMIT_TIME_NS: usize = 40;
pub(super) const FRAME_OFFSET_USER_HEADER_LEN: usize = 48;
pub(super) const FRAME_OFFSET_PAYLOAD_LEN: usize = 52;
pub(super) const FRAME_OFFSET_CHECKSUM: usize = 56;

pub(super) const COMMIT_ENTRY_MAGIC: [u8; 4] = *b"CID1";
pub(super) const COMMIT_ENTRY_LEN: usize = 104;
pub(super) const COMMIT_ENTRY_PREFIX_LEN: usize = 8;
pub(super) const COMMIT_OFFSET_MAGIC: usize = 0;
pub(super) const COMMIT_OFFSET_ENTRY_LEN: usize = 4;
pub(super) const COMMIT_OFFSET_FLAGS: usize = 6;
pub(super) const COMMIT_OFFSET_COMMIT_ORDINAL: usize = 8;
pub(super) const COMMIT_OFFSET_SEQUENCE: usize = 16;
pub(super) const COMMIT_OFFSET_SEGMENT_ID: usize = 24;
pub(super) const COMMIT_OFFSET_SEGMENT_GENERATION: usize = 32;
pub(super) const COMMIT_OFFSET_FILE_OFFSET: usize = 40;
pub(super) const COMMIT_OFFSET_FRAME_LEN: usize = 48;
pub(super) const COMMIT_OFFSET_FRAME_CHECKSUM: usize = 52;
pub(super) const COMMIT_OFFSET_EVENT_TIME_NS: usize = 56;
pub(super) const COMMIT_OFFSET_COMMIT_TIME_NS: usize = 64;
pub(super) const COMMIT_OFFSET_SOURCE_SERVICE_ID: usize = 72;
pub(super) const COMMIT_OFFSET_SOURCE_INSTANCE_ID: usize = 80;
pub(super) const COMMIT_OFFSET_SOURCE_SEQUENCE: usize = 88;
pub(super) const COMMIT_OFFSET_SOURCE_PATTERN: usize = 96;
pub(super) const COMMIT_OFFSET_SOURCE_FLAGS: usize = 97;
pub(super) const COMMIT_FLAG_SOURCE_SEQUENCE_PRESENT: u16 = 0x0001;

pub(super) const ADAPTER_USER_HEADER_MAGIC: [u8; 4] = *b"AUM1";
pub(super) const ADAPTER_USER_HEADER_PREFIX_LEN: usize = 40;
pub(super) const ADAPTER_USER_HEADER_OFFSET_MAGIC: usize = 0;
pub(super) const ADAPTER_USER_HEADER_OFFSET_LEN: usize = 4;
pub(super) const ADAPTER_USER_HEADER_OFFSET_FLAGS: usize = 6;
pub(super) const ADAPTER_USER_HEADER_OFFSET_SOURCE_PATTERN: usize = 8;
pub(super) const ADAPTER_USER_HEADER_OFFSET_SOURCE_SERVICE_ID: usize = 16;
pub(super) const ADAPTER_USER_HEADER_OFFSET_SOURCE_INSTANCE_ID: usize = 24;
pub(super) const ADAPTER_USER_HEADER_OFFSET_SOURCE_SEQUENCE: usize = 32;
pub(super) const ADAPTER_USER_HEADER_FLAG_SOURCE_SEQUENCE_PRESENT: u16 = 0x0001;

pub(super) const SEGMENT_SUMMARY_LEN: usize = 88;
pub(super) const DEFAULT_METADATA_LOG_PREALLOCATE_ENTRIES: usize = 4096;

/// Durability mode of the archive recorder.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PersistenceMode {
    /// Keeps records only in-memory and does not persist segment files.
    Volatile,
    /// Persists data without forcing a per-append fsync barrier.
    Async,
    /// Persists data and enforces a per-append fsync barrier.
    Sync,
}

/// Default timeout for waiting on `DurableData` acknowledgments.
pub const DEFAULT_WAIT_DURABLE_DATA_TIMEOUT: Duration = Duration::from_secs(1);
/// Default timeout for waiting on `DurableDataAndCommitLog` acknowledgments.
pub const DEFAULT_WAIT_DURABLE_DATA_AND_COMMIT_LOG_TIMEOUT: Duration = Duration::from_secs(2);
/// Default poll interval for acknowledgment wait loops.
pub const DEFAULT_ACK_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Recorder acknowledgment level returned or requested by append APIs.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecorderAckLevel {
    /// Record is accepted by recorder API but not yet guaranteed durable.
    Accepted,
    /// Segment data bytes are durable.
    DurableData,
    /// Segment data and commit-log entry are durable.
    DurableDataAndCommitLog,
}

/// Named recorder runtime profile.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RecorderProfile {
    /// Prioritizes stronger durability defaults.
    Durable,
    /// General-purpose defaults.
    Balanced,
    /// Prioritizes ingest throughput.
    Throughput,
    /// Prioritizes replay/query freshness.
    Replay,
}

/// Async write backend selection for recorder data path.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AsyncIoBackend {
    /// Prefer Linux `io_uring` and fall back to blocking file I/O when unavailable.
    IoUringPreferred,
    /// Require Linux `io_uring`; fail recorder creation when unavailable.
    IoUringRequired,
    /// Always use blocking file I/O.
    Blocking,
}

/// Effective async write backend selected by recorder runtime.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EffectiveAsyncIoBackend {
    /// Blocking file I/O backend.
    Blocking,
    /// Linux `io_uring` backend.
    IoUring,
}

/// Checksum strategy for persisted frames.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ChecksumMode {
    /// Do not store per-frame checksum values.
    None,
    /// Store CRC32C checksums per frame.
    Crc32c,
}

/// Out-of-space handling policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OutOfSpacePolicy {
    /// Fail the append operation and mark recorder as degraded.
    FailWriter,
}

/// Tier location of a sealed archive segment.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArchiveSegmentTier {
    /// Segment is available in the hot replay/query path.
    HotAttached,
    /// Segment is detached and requires attach before default replay/query.
    ColdDetached,
}

/// Snapshot/pin handle used to protect replay windows from trim/detach.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReplayPin {
    /// Unique pin identifier.
    pub pin_id: u64,
    /// Inclusive start sequence protected by this pin.
    pub sequence_start: u64,
    /// Inclusive end sequence protected by this pin.
    pub sequence_end: u64,
}

/// Observable state of one sealed segment for admin/introspection.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ArchiveSegmentState {
    /// Segment id.
    pub segment_id: u64,
    /// Segment generation.
    pub segment_generation: u32,
    /// Inclusive first sequence in this segment.
    pub sequence_start: u64,
    /// Inclusive last sequence in this segment.
    pub sequence_end: u64,
    /// Number of records in this segment.
    pub records: u64,
    /// Used bytes in the segment file.
    pub data_bytes_used: u64,
    /// Current tier.
    pub tier: ArchiveSegmentTier,
    /// True when this segment overlaps at least one active replay pin.
    pub pinned: bool,
}

/// Retention/tier counters for admin status fields.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ArchiveRetentionStatus {
    /// Configured global disk cap (`None` means unlimited).
    pub max_disk_bytes: Option<u64>,
    /// Total retained bytes across tiers.
    pub retained_bytes_total: u64,
    /// Retained bytes in hot-attached tier.
    pub retained_bytes_hot_attached: u64,
    /// Retained bytes in cold-detached tier.
    pub retained_bytes_cold_detached: u64,
    /// Number of sealed segments in hot-attached tier.
    pub segments_hot_attached: usize,
    /// Number of sealed segments in cold-detached tier.
    pub segments_cold_detached: usize,
    /// Number of pinned sealed segments.
    pub pinned_segments: usize,
}

/// Physical frame locator in archive storage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ArchiveLocator {
    /// Segment id of the record.
    pub segment_id: u64,
    /// Segment generation of the record.
    pub segment_generation: u32,
    /// File offset of the frame start in the segment file.
    pub file_offset: u64,
    /// Encoded frame length in bytes.
    pub frame_len: u32,
}

/// Returned when a frame was appended successfully.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RecordedCommit {
    /// Monotonic commit ordinal assigned by recorder.
    pub commit_ordinal: u64,
    /// Monotonic archive-local sequence.
    pub sequence: u64,
    /// Physical locator of the persisted frame.
    pub locator: ArchiveLocator,
}

/// Commit-log entry view exposed for archive introspection surfaces.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ArchiveCommitLogEntry {
    /// Monotonic commit ordinal assigned by recorder.
    pub commit_ordinal: u64,
    /// Logical source sequence.
    pub sequence: u64,
    /// Physical frame locator.
    pub locator: ArchiveLocator,
    /// Persisted frame checksum from commit-log metadata.
    pub frame_checksum: u32,
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// Commit timestamp in nanoseconds.
    pub commit_time_ns: u64,
    /// Source pattern as recorded by pattern adapter metadata.
    pub source_pattern: ArchiveSourcePattern,
    /// Stable source service identity (`0` when unavailable).
    pub source_service_id: u64,
    /// Pattern-local source identity (`0` when unavailable).
    pub source_instance_id: u64,
    /// Optional source sequence from upstream pattern endpoint.
    pub source_sequence: Option<u64>,
    /// True when the referenced segment is currently available in hot-attached storage.
    pub hot_attached: bool,
}

/// Source pattern discriminator used by pattern adapters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum ArchiveSourcePattern {
    /// Reserved for deferred log-pattern adapter support.
    Log = 1,
    /// Record originated from publish-subscribe messaging pattern.
    PublishSubscribe = 2,
    /// Reserved for deferred pipeline-pattern adapter support.
    Pipeline = 3,
}

impl TryFrom<u8> for ArchiveSourcePattern {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Log),
            2 => Ok(Self::PublishSubscribe),
            3 => Ok(Self::Pipeline),
            _ => Err(()),
        }
    }
}

/// Canonical source metadata emitted by pattern adapters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ArchiveSourceMetadata {
    /// Source pattern of the record.
    pub source_pattern: ArchiveSourcePattern,
    /// Stable source service identity chosen by adapter caller.
    pub source_service_id: u64,
    /// Pattern-local source identity.
    /// In pubsub-v1 this is the publisher identity.
    pub source_instance_id: u64,
    /// Optional source sequence from upstream pattern endpoint.
    pub source_sequence: Option<u64>,
}

/// Input frame for publish-subscribe pattern adapter ingestion.
#[derive(Debug, Clone, Copy)]
pub struct PublishSubscribeRecordInput<'a> {
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// Stable source service identity chosen by caller.
    pub source_service_id: u64,
    /// Stable source publisher identity chosen by caller.
    pub source_publisher_id: u64,
    /// Optional source sequence from publisher-side metadata.
    pub source_sequence: Option<u64>,
    /// User header bytes.
    pub user_header: &'a [u8],
    /// Payload bytes.
    pub payload: &'a [u8],
}

/// Adapter user-header decode output.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DecodedAdapterUserHeader<'a> {
    /// Adapter source metadata prefix.
    pub source_metadata: ArchiveSourceMetadata,
    /// Original caller-provided user header bytes.
    pub user_header: &'a [u8],
}

/// Encodes adapter source metadata in a deterministic user-header prefix.
pub fn encode_adapter_user_header(
    source_metadata: ArchiveSourceMetadata,
    user_header: &[u8],
) -> Vec<u8> {
    let mut bytes = vec![0u8; ADAPTER_USER_HEADER_PREFIX_LEN + user_header.len()];
    bytes[ADAPTER_USER_HEADER_OFFSET_MAGIC..ADAPTER_USER_HEADER_OFFSET_MAGIC + 4]
        .copy_from_slice(&ADAPTER_USER_HEADER_MAGIC);
    bytes[ADAPTER_USER_HEADER_OFFSET_LEN..ADAPTER_USER_HEADER_OFFSET_LEN + 2]
        .copy_from_slice(&(ADAPTER_USER_HEADER_PREFIX_LEN as u16).to_le_bytes());
    let mut flags = 0u16;
    if source_metadata.source_sequence.is_some() {
        flags |= ADAPTER_USER_HEADER_FLAG_SOURCE_SEQUENCE_PRESENT;
    }
    bytes[ADAPTER_USER_HEADER_OFFSET_FLAGS..ADAPTER_USER_HEADER_OFFSET_FLAGS + 2]
        .copy_from_slice(&flags.to_le_bytes());
    bytes[ADAPTER_USER_HEADER_OFFSET_SOURCE_PATTERN] = source_metadata.source_pattern as u8;
    bytes[ADAPTER_USER_HEADER_OFFSET_SOURCE_SERVICE_ID
        ..ADAPTER_USER_HEADER_OFFSET_SOURCE_SERVICE_ID + 8]
        .copy_from_slice(&source_metadata.source_service_id.to_le_bytes());
    bytes[ADAPTER_USER_HEADER_OFFSET_SOURCE_INSTANCE_ID
        ..ADAPTER_USER_HEADER_OFFSET_SOURCE_INSTANCE_ID + 8]
        .copy_from_slice(&source_metadata.source_instance_id.to_le_bytes());
    bytes[ADAPTER_USER_HEADER_OFFSET_SOURCE_SEQUENCE
        ..ADAPTER_USER_HEADER_OFFSET_SOURCE_SEQUENCE + 8]
        .copy_from_slice(&source_metadata.source_sequence.unwrap_or(0).to_le_bytes());
    bytes[ADAPTER_USER_HEADER_PREFIX_LEN..].copy_from_slice(user_header);
    bytes
}

/// Decodes adapter source metadata and strips the deterministic prefix.
pub fn decode_adapter_user_header(user_header: &[u8]) -> Option<DecodedAdapterUserHeader<'_>> {
    if user_header.len() < ADAPTER_USER_HEADER_PREFIX_LEN {
        return None;
    }

    if user_header[ADAPTER_USER_HEADER_OFFSET_MAGIC..ADAPTER_USER_HEADER_OFFSET_MAGIC + 4]
        != ADAPTER_USER_HEADER_MAGIC
    {
        return None;
    }

    let header_len = read_u16(user_header, ADAPTER_USER_HEADER_OFFSET_LEN) as usize;
    if header_len != ADAPTER_USER_HEADER_PREFIX_LEN {
        return None;
    }

    let flags = read_u16(user_header, ADAPTER_USER_HEADER_OFFSET_FLAGS);
    let pattern =
        ArchiveSourcePattern::try_from(user_header[ADAPTER_USER_HEADER_OFFSET_SOURCE_PATTERN])
            .ok()?;

    let source_sequence = if (flags & ADAPTER_USER_HEADER_FLAG_SOURCE_SEQUENCE_PRESENT) != 0 {
        Some(read_u64(
            user_header,
            ADAPTER_USER_HEADER_OFFSET_SOURCE_SEQUENCE,
        ))
    } else {
        None
    };

    Some(DecodedAdapterUserHeader {
        source_metadata: ArchiveSourceMetadata {
            source_pattern: pattern,
            source_service_id: read_u64(user_header, ADAPTER_USER_HEADER_OFFSET_SOURCE_SERVICE_ID),
            source_instance_id: read_u64(
                user_header,
                ADAPTER_USER_HEADER_OFFSET_SOURCE_INSTANCE_ID,
            ),
            source_sequence,
        },
        user_header: &user_header[ADAPTER_USER_HEADER_PREFIX_LEN..],
    })
}

/// Recorder statistics and accounting values.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ArchiveRecorderStats {
    /// Number of committed records.
    pub committed_records: u64,
    /// Number of committed payload bytes.
    pub payload_bytes_committed: u64,
    /// Number of data bytes written (`segment-*.data`).
    pub data_bytes_written: u64,
    /// Number of metadata bytes written (`catalog.bin`, `commit.idxlog`, `segment-*.meta`).
    pub metadata_bytes_written: u64,
    /// Number of segment roll operations.
    pub rolled_segments: u64,
    /// Number of preallocated segment files created.
    pub preallocated_segments: u64,
    /// Number of out-of-space events observed.
    pub out_of_space_events: u64,
    /// Number of commit-log roll operations.
    pub metadata_log_rolls: u64,
}

impl ArchiveRecorderStats {
    /// Returns write amplification ratio (`written / payload`).
    pub fn amplification_ratio(&self) -> f64 {
        if self.payload_bytes_committed == 0 {
            return 0.0;
        }

        (self.data_bytes_written + self.metadata_bytes_written) as f64
            / self.payload_bytes_committed as f64
    }
}

/// Deterministic startup recovery status for recorder admin surfaces.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ArchiveRecoveryStatus {
    /// True when recorder was opened from an existing archive and recovery ran.
    pub recovered_existing_archive: bool,
    /// Number of segment summaries loaded from catalog.
    pub catalog_segments_loaded: u64,
    /// Number of valid commit-log entries loaded.
    pub commit_entries_loaded: u64,
    /// Active segment id selected after recovery.
    pub active_segment_id: u64,
    /// Active segment generation selected after recovery.
    pub active_segment_generation: u32,
    /// Number of committed records recovered in active segment.
    pub active_segment_records: u64,
    /// Number of active-segment truncation events during recovery.
    pub segment_truncation_events: u64,
    /// Number of bytes truncated from active segment tails during recovery.
    pub segment_truncated_bytes: u64,
    /// Number of bytes truncated from commit-log tail during recovery.
    pub commit_log_truncated_bytes: u64,
    /// Elapsed recovery time in nanoseconds for startup recovery.
    pub recovery_duration_ns: u64,
}

/// Replay budget limits.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ReplayBudget {
    /// Maximum records returned per range/batch call.
    pub max_records_per_call: usize,
    /// Maximum aggregate bytes returned per range/batch call.
    pub max_bytes_per_call: usize,
}

impl Default for ReplayBudget {
    fn default() -> Self {
        Self {
            max_records_per_call: 1024,
            max_bytes_per_call: 64 * 1024 * 1024,
        }
    }
}

/// Decoded frame returned by replayer APIs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplayedFrame {
    /// Commit ordinal from archive.
    pub commit_ordinal: u64,
    /// Source sequence.
    pub sequence: u64,
    /// Event timestamp in nanoseconds.
    pub event_time_ns: u64,
    /// Commit timestamp in nanoseconds.
    pub commit_time_ns: u64,
    /// User header bytes.
    pub user_header: Vec<u8>,
    /// Payload bytes.
    pub payload: Vec<u8>,
    /// Physical locator.
    pub locator: ArchiveLocator,
}

/// Errors returned by archive recorder operations.
#[derive(Debug)]
pub enum ArchiveRecorderError {
    /// Invalid configuration value.
    InvalidConfiguration(&'static str),
    /// Recorder storage root already contains archive files.
    ArchiveAlreadyExists(PathBuf),
    /// Existing archive is missing a required path.
    MissingArchiveComponent(PathBuf),
    /// Recorder has been finalized and no more appends are accepted.
    Finalized,
    /// Recorder entered degraded state after a fatal I/O failure.
    Degraded,
    /// Waiting for a stronger acknowledgment level exceeded timeout.
    AckTimeout {
        /// Requested acknowledgment level.
        requested: RecorderAckLevel,
        /// Timeout in milliseconds.
        timeout_ms: u64,
        /// Last known durable data sequence.
        last_durable_data_sequence: Option<u64>,
        /// Last known durable commit-log ordinal.
        last_durable_commit_ordinal: Option<u64>,
    },
    /// Sequence values must be strictly monotonic.
    SequenceNotMonotonic {
        /// Previous sequence value.
        previous: u64,
        /// Incoming sequence value.
        next: u64,
    },
    /// A single frame cannot fit into the configured segment size.
    FrameTooLarge {
        /// Encoded bytes required for one frame.
        required: usize,
        /// Configured segment byte size.
        segment_bytes: usize,
    },
    /// Out-of-space failure.
    OutOfSpace(PathBuf),
    /// Metadata-log size cap would be exceeded by the next append.
    MetadataLogCapacityExceeded {
        /// Configured metadata-log cap.
        max_bytes: u64,
        /// Required bytes to proceed.
        required_bytes: u64,
    },
    /// Retention cap cannot be met because all eligible segments are pinned.
    RetentionBlockedByPins {
        /// Configured maximum disk bytes.
        max_disk_bytes: u64,
        /// Current retained bytes.
        retained_bytes: u64,
    },
    /// Recovery validation failed due to inconsistent persisted state.
    RecoveryInconsistent(&'static str),
    /// File header encoding/validation failure.
    FileHeader(ArchiveFileHeaderError),
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

impl Display for ArchiveRecorderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveRecorderError::{self:?}")
    }
}

impl std::error::Error for ArchiveRecorderError {}

impl From<ArchiveFileHeaderError> for ArchiveRecorderError {
    fn from(value: ArchiveFileHeaderError) -> Self {
        Self::FileHeader(value)
    }
}

/// Errors returned by archive replayer operations.
#[derive(Debug)]
pub enum ArchiveReplayError {
    /// Invalid configuration value.
    InvalidConfiguration(&'static str),
    /// `commit.idxlog` file is missing.
    MissingCommitLog(PathBuf),
    /// Required segment file is missing.
    MissingSegment(PathBuf),
    /// Unsupported or malformed header.
    FileHeader(ArchiveFileHeaderError),
    /// Corrupted commit-log entry.
    InvalidCommitEntry(&'static str),
    /// Duplicate sequence in commit-log.
    DuplicateSequence(u64),
    /// Creating/releasing replay pins failed due to malformed pin state.
    InvalidPinState(&'static str),
    /// Corrupted frame header magic.
    InvalidFrameMagic([u8; 4]),
    /// Corrupted frame header length.
    InvalidFrameHeaderLength(u16),
    /// Corrupted frame length.
    InvalidFrameLength {
        /// Frame length from locator/commit metadata.
        expected: u32,
        /// Frame length decoded from frame header.
        decoded: u32,
    },
    /// Frame checksum mismatch.
    ChecksumMismatch {
        /// Expected checksum.
        expected: u32,
        /// Actual checksum.
        actual: u32,
        /// Locator of the corrupted frame.
        locator: ArchiveLocator,
    },
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

impl Display for ArchiveReplayError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveReplayError::{self:?}")
    }
}

impl std::error::Error for ArchiveReplayError {}

impl From<ArchiveFileHeaderError> for ArchiveReplayError {
    fn from(value: ArchiveFileHeaderError) -> Self {
        Self::FileHeader(value)
    }
}

/// Builder for [`ArchiveRecorder`].
#[derive(Debug, Clone)]
pub(super) struct RecorderConfig {
    pub(super) profile: RecorderProfile,
    pub(super) storage_path: PathBuf,
    pub(super) metadata_log_path: PathBuf,
    pub(super) segment_bytes: usize,
    pub(super) segment_preallocate: bool,
    pub(super) spare_preallocated_segments: usize,
    pub(super) metadata_log_preallocate_entries: usize,
    pub(super) persistence_mode: PersistenceMode,
    pub(super) async_io_backend: AsyncIoBackend,
    pub(super) io_uring_queue_depth: u32,
    pub(super) io_submit_batch_max: u32,
    pub(super) io_cqe_batch_max: u32,
    pub(super) io_uring_register_files: bool,
    pub(super) checksum_mode: ChecksumMode,
    pub(super) out_of_space_policy: OutOfSpacePolicy,
    pub(super) max_disk_bytes: Option<u64>,
    pub(super) metadata_log_roll_bytes: u64,
    pub(super) metadata_log_max_bytes: u64,
    pub(super) log_id: [u8; 16],
    pub(super) segment_generation: u32,
}

#[derive(Debug)]
pub(super) struct DiskRecorderState {
    pub(super) segments_path: PathBuf,
    pub(super) catalog_path: PathBuf,
    pub(super) commit_log_path: PathBuf,
    pub(super) catalog_file: File,
    pub(super) commit_log_file: File,
    pub(super) commit_log_write_offset: u64,
    pub(super) commit_log_preallocated_len: u64,
    pub(super) commit_log_roll_index: u64,
    pub(super) active_segment: Option<ActiveSegment>,
}

#[derive(Debug)]
pub(super) struct ActiveSegment {
    pub(super) segment_id: u64,
    pub(super) segment_generation: u32,
    pub(super) created_at_ns: u64,
    pub(super) write_offset: u64,
    pub(super) sequence_start: Option<u64>,
    pub(super) sequence_end: Option<u64>,
    pub(super) records: u64,
    pub(super) file: File,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct VolatileFrame {
    pub(super) commit_ordinal: u64,
    pub(super) sequence: u64,
    pub(super) event_time_ns: u64,
    pub(super) commit_time_ns: u64,
    pub(super) user_header: Vec<u8>,
    pub(super) payload: Vec<u8>,
    pub(super) locator: ArchiveLocator,
}

/// Recorder core for log archive segment files.
#[derive(Debug)]
pub struct ArchiveRecorder {
    pub(super) config: RecorderConfig,
    pub(super) io_backend: RecorderIoBackend,
    pub(super) effective_async_io_backend: EffectiveAsyncIoBackend,
    pub(super) disk: Option<DiskRecorderState>,
    pub(super) stats: ArchiveRecorderStats,
    pub(super) recovery_status: ArchiveRecoveryStatus,
    pub(super) next_commit_ordinal: u64,
    pub(super) last_sequence: Option<u64>,
    pub(super) last_durable_data_sequence: Option<u64>,
    pub(super) last_durable_commit_ordinal: Option<u64>,
    pub(super) index_by_sequence: BTreeMap<u64, ArchiveLocator>,
    pub(super) volatile_records: Vec<VolatileFrame>,
    pub(super) finalized: bool,
    pub(super) degraded: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CommitEntry {
    pub(super) commit_ordinal: u64,
    pub(super) sequence: u64,
    pub(super) locator: ArchiveLocator,
    pub(super) frame_checksum: u32,
    pub(super) event_time_ns: u64,
    pub(super) commit_time_ns: u64,
    pub(super) source_pattern: ArchiveSourcePattern,
    pub(super) source_service_id: u64,
    pub(super) source_instance_id: u64,
    pub(super) source_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct CommitLogRecoveryResult {
    pub(super) entries: Vec<CommitEntry>,
    pub(super) logical_end_offset: u64,
    pub(super) truncated_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SegmentRecoveryResult {
    pub(super) truncated_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SegmentTailScanResult {
    pub(super) original_len: u64,
    pub(super) valid_end: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SegmentSummary {
    pub(super) segment_id: u64,
    pub(super) segment_generation: u32,
    pub(super) sequence_start: u64,
    pub(super) sequence_end: u64,
    pub(super) records: u64,
    pub(super) created_at_ns: u64,
    pub(super) sealed_at_ns: u64,
    pub(super) data_bytes_used: u64,
    pub(super) segment_checksum: u32,
}

impl SegmentSummary {
    pub(super) fn to_bytes(self) -> [u8; SEGMENT_SUMMARY_LEN] {
        let mut bytes = [0u8; SEGMENT_SUMMARY_LEN];
        bytes[0..8].copy_from_slice(&self.segment_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.segment_generation.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.sequence_start.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.sequence_end.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.records.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.created_at_ns.to_le_bytes());
        bytes[44..52].copy_from_slice(&self.sealed_at_ns.to_le_bytes());
        bytes[52..60].copy_from_slice(&self.data_bytes_used.to_le_bytes());
        bytes[60..64].copy_from_slice(&self.segment_checksum.to_le_bytes());
        bytes
    }

    pub(super) fn from_bytes(bytes: &[u8; SEGMENT_SUMMARY_LEN]) -> Self {
        Self {
            segment_id: read_u64(bytes, 0),
            segment_generation: read_u32(bytes, 8),
            sequence_start: read_u64(bytes, 12),
            sequence_end: read_u64(bytes, 20),
            records: read_u64(bytes, 28),
            created_at_ns: read_u64(bytes, 36),
            sealed_at_ns: read_u64(bytes, 44),
            data_bytes_used: read_u64(bytes, 52),
            segment_checksum: read_u32(bytes, 60),
        }
    }
}

#[derive(Debug)]
pub(super) struct EncodedFrame {
    pub(super) bytes: Vec<u8>,
    pub(super) checksum: u32,
}

impl EncodedFrame {
    pub(super) fn new(
        commit_ordinal: u64,
        sequence: u64,
        event_time_ns: u64,
        commit_time_ns: u64,
        user_header: &[u8],
        payload: &[u8],
        checksum_mode: ChecksumMode,
    ) -> Self {
        let frame_len = align_up(FRAME_HEADER_LEN + user_header.len() + payload.len(), 8);
        let mut bytes = vec![0u8; frame_len];
        bytes[FRAME_OFFSET_MAGIC..FRAME_OFFSET_MAGIC + 4].copy_from_slice(&FRAME_MAGIC);
        bytes[FRAME_OFFSET_HEADER_LEN..FRAME_OFFSET_HEADER_LEN + 2]
            .copy_from_slice(&(FRAME_HEADER_LEN as u16).to_le_bytes());
        let mut flags = 0u16;
        if checksum_mode == ChecksumMode::Crc32c {
            flags |= FRAME_FLAG_CHECKSUM_CRC32C;
        }
        bytes[FRAME_OFFSET_FLAGS..FRAME_OFFSET_FLAGS + 2].copy_from_slice(&flags.to_le_bytes());
        bytes[FRAME_OFFSET_FRAME_LEN..FRAME_OFFSET_FRAME_LEN + 4]
            .copy_from_slice(&(frame_len as u32).to_le_bytes());
        bytes[FRAME_OFFSET_COMMIT_ORDINAL..FRAME_OFFSET_COMMIT_ORDINAL + 8]
            .copy_from_slice(&commit_ordinal.to_le_bytes());
        bytes[FRAME_OFFSET_SEQUENCE..FRAME_OFFSET_SEQUENCE + 8]
            .copy_from_slice(&sequence.to_le_bytes());
        bytes[FRAME_OFFSET_EVENT_TIME_NS..FRAME_OFFSET_EVENT_TIME_NS + 8]
            .copy_from_slice(&event_time_ns.to_le_bytes());
        bytes[FRAME_OFFSET_COMMIT_TIME_NS..FRAME_OFFSET_COMMIT_TIME_NS + 8]
            .copy_from_slice(&commit_time_ns.to_le_bytes());
        bytes[FRAME_OFFSET_USER_HEADER_LEN..FRAME_OFFSET_USER_HEADER_LEN + 4]
            .copy_from_slice(&(user_header.len() as u32).to_le_bytes());
        bytes[FRAME_OFFSET_PAYLOAD_LEN..FRAME_OFFSET_PAYLOAD_LEN + 4]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[FRAME_HEADER_LEN..FRAME_HEADER_LEN + user_header.len()].copy_from_slice(user_header);
        bytes[FRAME_HEADER_LEN + user_header.len()
            ..FRAME_HEADER_LEN + user_header.len() + payload.len()]
            .copy_from_slice(payload);

        let checksum = if checksum_mode == ChecksumMode::Crc32c {
            let crc = crc32c::crc32c(&bytes);
            bytes[FRAME_OFFSET_CHECKSUM..FRAME_OFFSET_CHECKSUM + 4]
                .copy_from_slice(&crc.to_le_bytes());
            crc
        } else {
            0
        };

        Self { bytes, checksum }
    }
}

pub(super) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_nanos() as u64)
        .unwrap_or(0)
}

pub(super) fn segment_data_path(base: &Path, segment_id: u64, generation: u32) -> PathBuf {
    base.join(format!("segment-{segment_id}-g{generation}.data"))
}

pub(super) fn segment_meta_path(base: &Path, segment_id: u64, generation: u32) -> PathBuf {
    base.join(format!("segment-{segment_id}-g{generation}.meta"))
}

pub(super) fn detached_segments_path(storage_root: &Path) -> PathBuf {
    storage_root.join("detached")
}

pub(super) fn pin_directory(metadata_root: &Path) -> PathBuf {
    metadata_root.join("pins")
}

pub(super) fn pin_file_name(pin: ReplayPin) -> String {
    format!(
        "pin-{}-s{}-e{}.pin",
        pin.pin_id, pin.sequence_start, pin.sequence_end
    )
}

pub(super) fn pin_file_path(metadata_root: &Path, pin: ReplayPin) -> PathBuf {
    pin_directory(metadata_root).join(pin_file_name(pin))
}

pub(super) fn parse_pin_file_name(name: &str) -> Option<ReplayPin> {
    let value = name.strip_prefix("pin-")?.strip_suffix(".pin")?;
    let (id, rest) = value.split_once("-s")?;
    let (start, end) = rest.split_once("-e")?;
    Some(ReplayPin {
        pin_id: id.parse().ok()?,
        sequence_start: start.parse().ok()?,
        sequence_end: end.parse().ok()?,
    })
}

pub(super) fn align_up(value: usize, alignment: usize) -> usize {
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + (alignment - remainder)
    }
}

pub(super) fn is_out_of_space(source: &std::io::Error) -> bool {
    source.raw_os_error() == Some(28)
}

pub(super) fn lower_bound(values: &[u64], needle: u64) -> usize {
    values.partition_point(|value| *value < needle)
}

pub(super) fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

pub(super) fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

pub(super) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

pub(super) fn frame_crc_from_payload(
    frame: &ReplayedFrame,
    verify: bool,
) -> Result<u32, ArchiveReplayError> {
    if !verify {
        return Ok(0);
    }

    let encoded = EncodedFrame::new(
        frame.commit_ordinal,
        frame.sequence,
        frame.event_time_ns,
        frame.commit_time_ns,
        &frame.user_header,
        &frame.payload,
        ChecksumMode::Crc32c,
    );
    Ok(encoded.checksum)
}
