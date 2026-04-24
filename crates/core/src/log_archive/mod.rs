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

//! Archive file format contracts for the log recorder/replayer.
//!
//! This module provides the canonical v1 archive file header and conformance
//! checks that are required before recorder/replayer implementation phases.

/// 4-byte archive file magic (`IOX2`).
pub const ARCHIVE_FILE_MAGIC: [u8; 4] = *b"IOX2";
/// Encoded byte length of [`ArchiveFileHeaderV1`].
pub const ARCHIVE_FILE_HEADER_V1_LEN: usize = 76;
/// Supported major version for v1 archive files.
pub const ARCHIVE_FILE_VERSION_MAJOR_V1: u16 = 1;
/// Optional flags mask (low 24 bits).
pub const ARCHIVE_FILE_OPTIONAL_FLAGS_MASK: u32 = 0x00FF_FFFF;
/// Must-understand flags mask (high 8 bits).
pub const ARCHIVE_FILE_MUST_UNDERSTAND_FLAGS_MASK: u32 = 0xFF00_0000;
/// Optional flag indicating padding records may be present.
pub const ARCHIVE_FILE_OPTIONAL_FLAG_HAS_PADDING_RECORDS: u32 = 0x0000_0001;
/// Optional flag indicating segment checksum metadata may be present.
pub const ARCHIVE_FILE_OPTIONAL_FLAG_HAS_SEGMENT_CHECKSUM: u32 = 0x0000_0002;

const OFFSET_MAGIC: usize = 0;
const OFFSET_FILE_KIND: usize = 4;
const OFFSET_MAJOR: usize = 6;
const OFFSET_MINOR: usize = 8;
const OFFSET_HEADER_LEN: usize = 10;
const OFFSET_FLAGS: usize = 12;
const OFFSET_CREATED_AT_NS: usize = 16;
const OFFSET_LOG_ID: usize = 24;
const OFFSET_SEGMENT_ID: usize = 40;
const OFFSET_SEGMENT_GENERATION: usize = 48;
const OFFSET_RESERVED: usize = 52;
const OFFSET_HEADER_CRC32C: usize = 72;

/// Canonical archive file kind values for v1 headers.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u16)]
pub enum ArchiveFileKind {
    /// `catalog.bin`
    Catalog = 1,
    /// `commit-*.idxlog`
    CommitIdxLog = 2,
    /// `segment-*.data`
    SegmentData = 3,
    /// `segment-*.idx`
    SegmentIndex = 4,
    /// `segment-*.meta`
    SegmentMeta = 5,
}

impl ArchiveFileKind {
    /// Returns the encoded `u16` discriminator.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Returns the supported minor format version for this file kind.
    pub const fn supported_minor(self) -> u16 {
        let _ = self;
        0
    }

    const fn is_segment_scoped(self) -> bool {
        matches!(
            self,
            Self::SegmentData | Self::SegmentIndex | Self::SegmentMeta
        )
    }
}

impl TryFrom<u16> for ArchiveFileKind {
    type Error = ArchiveFileHeaderError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Catalog),
            2 => Ok(Self::CommitIdxLog),
            3 => Ok(Self::SegmentData),
            4 => Ok(Self::SegmentIndex),
            5 => Ok(Self::SegmentMeta),
            v => Err(ArchiveFileHeaderError::UnsupportedFileKind(v)),
        }
    }
}

/// Validation and decoding errors for [`ArchiveFileHeaderV1`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArchiveFileHeaderError {
    /// Input byte slice is too short.
    BufferTooSmall {
        /// Number of bytes in the input.
        available: usize,
        /// Number of bytes required.
        required: usize,
    },
    /// Header magic is invalid.
    InvalidMagic([u8; 4]),
    /// File kind value is unknown.
    UnsupportedFileKind(u16),
    /// Header length is smaller than the mandatory v1 base header.
    HeaderLengthTooSmall(u16),
    /// Major version is unsupported.
    UnsupportedMajor(u16),
    /// Minor version is unsupported for this file kind.
    UnsupportedMinor {
        /// Decoded file kind.
        file_kind: ArchiveFileKind,
        /// Decoded minor version.
        minor: u16,
        /// Supported minor version for this decoder.
        supported_minor: u16,
    },
    /// Unknown must-understand flags are set.
    UnknownMustUnderstandFlags(u32),
    /// Header CRC32C check failed.
    InvalidHeaderCrc {
        /// Expected CRC value from the encoded header.
        expected: u32,
        /// Recomputed CRC32C from the payload.
        actual: u32,
    },
    /// Non-segment files must use `segment_id == 0` and `segment_generation == 0`.
    NonZeroSegmentIdentityForGlobalFile {
        /// File kind with invalid segment identity.
        file_kind: ArchiveFileKind,
        /// Decoded segment id.
        segment_id: u64,
        /// Decoded segment generation.
        segment_generation: u32,
    },
    /// Segment-scoped files require `segment_id > 0`.
    ZeroSegmentIdForSegmentFile {
        /// File kind with invalid segment id.
        file_kind: ArchiveFileKind,
    },
    /// Encoding only supports canonical v1 base header length.
    NonCanonicalHeaderLengthForV1(u16),
}

impl core::fmt::Display for ArchiveFileHeaderError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "ArchiveFileHeaderError::{self:?}")
    }
}

impl core::error::Error for ArchiveFileHeaderError {}

/// Canonical archive file header format (v1).
///
/// The encoded layout is little-endian and exactly
/// [`ARCHIVE_FILE_HEADER_V1_LEN`] bytes.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
pub struct ArchiveFileHeaderV1 {
    /// Must be [`ARCHIVE_FILE_MAGIC`].
    pub magic: [u8; 4],
    /// File kind discriminator.
    pub file_kind: ArchiveFileKind,
    /// Incompatible format version.
    pub major: u16,
    /// Additive compatible format version.
    pub minor: u16,
    /// Header byte length, including optional extension bytes.
    pub header_len: u16,
    /// Optional and must-understand flags.
    pub flags: u32,
    /// File creation timestamp.
    pub created_at_ns: u64,
    /// Logical log identity.
    pub log_id: [u8; 16],
    /// Segment id (`0` for non-segment files).
    pub segment_id: u64,
    /// Segment generation (`0` for non-segment files).
    pub segment_generation: u32,
    /// Reserved for future use.
    pub reserved: [u8; 20],
    /// CRC32C over the header bytes excluding this field.
    pub header_crc32c: u32,
}

impl ArchiveFileHeaderV1 {
    /// Creates a canonical v1 header with default values for `file_kind`.
    pub const fn new(file_kind: ArchiveFileKind) -> Self {
        Self {
            magic: ARCHIVE_FILE_MAGIC,
            file_kind,
            major: ARCHIVE_FILE_VERSION_MAJOR_V1,
            minor: 0,
            header_len: ARCHIVE_FILE_HEADER_V1_LEN as u16,
            flags: 0,
            created_at_ns: 0,
            log_id: [0u8; 16],
            segment_id: if file_kind.is_segment_scoped() { 1 } else { 0 },
            segment_generation: 0,
            reserved: [0u8; 20],
            header_crc32c: 0,
        }
    }

    /// Encodes the header into canonical v1 bytes and computes `header_crc32c`.
    pub fn to_bytes(&self) -> Result<[u8; ARCHIVE_FILE_HEADER_V1_LEN], ArchiveFileHeaderError> {
        self.validate_for_encoding()?;

        let mut bytes = [0u8; ARCHIVE_FILE_HEADER_V1_LEN];
        bytes[OFFSET_MAGIC..OFFSET_MAGIC + 4].copy_from_slice(&self.magic);
        bytes[OFFSET_FILE_KIND..OFFSET_FILE_KIND + 2]
            .copy_from_slice(&self.file_kind.as_u16().to_le_bytes());
        bytes[OFFSET_MAJOR..OFFSET_MAJOR + 2].copy_from_slice(&self.major.to_le_bytes());
        bytes[OFFSET_MINOR..OFFSET_MINOR + 2].copy_from_slice(&self.minor.to_le_bytes());
        bytes[OFFSET_HEADER_LEN..OFFSET_HEADER_LEN + 2]
            .copy_from_slice(&self.header_len.to_le_bytes());
        bytes[OFFSET_FLAGS..OFFSET_FLAGS + 4].copy_from_slice(&self.flags.to_le_bytes());
        bytes[OFFSET_CREATED_AT_NS..OFFSET_CREATED_AT_NS + 8]
            .copy_from_slice(&self.created_at_ns.to_le_bytes());
        bytes[OFFSET_LOG_ID..OFFSET_LOG_ID + 16].copy_from_slice(&self.log_id);
        bytes[OFFSET_SEGMENT_ID..OFFSET_SEGMENT_ID + 8]
            .copy_from_slice(&self.segment_id.to_le_bytes());
        bytes[OFFSET_SEGMENT_GENERATION..OFFSET_SEGMENT_GENERATION + 4]
            .copy_from_slice(&self.segment_generation.to_le_bytes());
        bytes[OFFSET_RESERVED..OFFSET_RESERVED + 20].copy_from_slice(&self.reserved);
        bytes[OFFSET_HEADER_CRC32C..OFFSET_HEADER_CRC32C + 4].fill(0);

        let crc = crc32c_without_header_crc_field(&bytes, self.header_len as usize);
        bytes[OFFSET_HEADER_CRC32C..OFFSET_HEADER_CRC32C + 4].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    /// Decodes and validates a v1 archive file header from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ArchiveFileHeaderError> {
        if bytes.len() < ARCHIVE_FILE_HEADER_V1_LEN {
            return Err(ArchiveFileHeaderError::BufferTooSmall {
                available: bytes.len(),
                required: ARCHIVE_FILE_HEADER_V1_LEN,
            });
        }

        let magic = [
            bytes[OFFSET_MAGIC],
            bytes[OFFSET_MAGIC + 1],
            bytes[OFFSET_MAGIC + 2],
            bytes[OFFSET_MAGIC + 3],
        ];
        if magic != ARCHIVE_FILE_MAGIC {
            return Err(ArchiveFileHeaderError::InvalidMagic(magic));
        }

        let file_kind = ArchiveFileKind::try_from(read_u16(bytes, OFFSET_FILE_KIND))?;
        let major = read_u16(bytes, OFFSET_MAJOR);
        let minor = read_u16(bytes, OFFSET_MINOR);
        let header_len = read_u16(bytes, OFFSET_HEADER_LEN);
        let flags = read_u32(bytes, OFFSET_FLAGS);
        let created_at_ns = read_u64(bytes, OFFSET_CREATED_AT_NS);
        let log_id = read_bytes_16(bytes, OFFSET_LOG_ID);
        let segment_id = read_u64(bytes, OFFSET_SEGMENT_ID);
        let segment_generation = read_u32(bytes, OFFSET_SEGMENT_GENERATION);
        let reserved = read_bytes_20(bytes, OFFSET_RESERVED);
        let header_crc32c = read_u32(bytes, OFFSET_HEADER_CRC32C);

        if header_len < ARCHIVE_FILE_HEADER_V1_LEN as u16 {
            return Err(ArchiveFileHeaderError::HeaderLengthTooSmall(header_len));
        }

        let header_len = header_len as usize;
        if bytes.len() < header_len {
            return Err(ArchiveFileHeaderError::BufferTooSmall {
                available: bytes.len(),
                required: header_len,
            });
        }

        if major != ARCHIVE_FILE_VERSION_MAJOR_V1 {
            return Err(ArchiveFileHeaderError::UnsupportedMajor(major));
        }

        let supported_minor = file_kind.supported_minor();
        if minor > supported_minor {
            return Err(ArchiveFileHeaderError::UnsupportedMinor {
                file_kind,
                minor,
                supported_minor,
            });
        }

        let must_understand_flags = flags & ARCHIVE_FILE_MUST_UNDERSTAND_FLAGS_MASK;
        if must_understand_flags != 0 {
            return Err(ArchiveFileHeaderError::UnknownMustUnderstandFlags(
                must_understand_flags,
            ));
        }

        validate_segment_identity(file_kind, segment_id, segment_generation)?;

        let actual_crc = crc32c_without_header_crc_field(&bytes[..header_len], header_len);
        if header_crc32c != actual_crc {
            return Err(ArchiveFileHeaderError::InvalidHeaderCrc {
                expected: header_crc32c,
                actual: actual_crc,
            });
        }

        Ok(Self {
            magic,
            file_kind,
            major,
            minor,
            header_len: header_len as u16,
            flags,
            created_at_ns,
            log_id,
            segment_id,
            segment_generation,
            reserved,
            header_crc32c,
        })
    }

    fn validate_for_encoding(&self) -> Result<(), ArchiveFileHeaderError> {
        if self.magic != ARCHIVE_FILE_MAGIC {
            return Err(ArchiveFileHeaderError::InvalidMagic(self.magic));
        }

        if self.major != ARCHIVE_FILE_VERSION_MAJOR_V1 {
            return Err(ArchiveFileHeaderError::UnsupportedMajor(self.major));
        }

        let supported_minor = self.file_kind.supported_minor();
        if self.minor > supported_minor {
            return Err(ArchiveFileHeaderError::UnsupportedMinor {
                file_kind: self.file_kind,
                minor: self.minor,
                supported_minor,
            });
        }

        if self.header_len != ARCHIVE_FILE_HEADER_V1_LEN as u16 {
            return Err(ArchiveFileHeaderError::NonCanonicalHeaderLengthForV1(
                self.header_len,
            ));
        }

        let must_understand_flags = self.flags & ARCHIVE_FILE_MUST_UNDERSTAND_FLAGS_MASK;
        if must_understand_flags != 0 {
            return Err(ArchiveFileHeaderError::UnknownMustUnderstandFlags(
                must_understand_flags,
            ));
        }

        validate_segment_identity(self.file_kind, self.segment_id, self.segment_generation)
    }
}

fn validate_segment_identity(
    file_kind: ArchiveFileKind,
    segment_id: u64,
    segment_generation: u32,
) -> Result<(), ArchiveFileHeaderError> {
    if file_kind.is_segment_scoped() {
        if segment_id == 0 {
            return Err(ArchiveFileHeaderError::ZeroSegmentIdForSegmentFile { file_kind });
        }
    } else if segment_id != 0 || segment_generation != 0 {
        return Err(
            ArchiveFileHeaderError::NonZeroSegmentIdentityForGlobalFile {
                file_kind,
                segment_id,
                segment_generation,
            },
        );
    }

    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
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

fn read_bytes_16(bytes: &[u8], offset: usize) -> [u8; 16] {
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes[offset..offset + 16]);
    out
}

fn read_bytes_20(bytes: &[u8], offset: usize) -> [u8; 20] {
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes[offset..offset + 20]);
    out
}

fn crc32c_without_header_crc_field(header: &[u8], header_len: usize) -> u32 {
    let crc = crc32c_append(0, &header[..OFFSET_HEADER_CRC32C]);
    crc32c_append(crc, &header[OFFSET_HEADER_CRC32C + 4..header_len])
}

#[cfg(feature = "std")]
#[inline]
fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, data)
}

#[cfg(not(feature = "std"))]
const CRC32C_TABLE: [u32; 256] = generate_crc32c_table();

#[cfg(not(feature = "std"))]
const fn generate_crc32c_table() -> [u32; 256] {
    // Castagnoli polynomial in reversed bit order.
    const POLYNOMIAL: u32 = 0x82F63B78;

    let mut table = [0u32; 256];
    let mut i = 0;

    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ POLYNOMIAL;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }

    table
}

#[cfg(not(feature = "std"))]
#[inline]
fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    let mut internal_crc = !crc;

    for byte in data {
        let table_idx = ((internal_crc ^ (*byte as u32)) & 0xFF) as usize;
        internal_crc = (internal_crc >> 8) ^ CRC32C_TABLE[table_idx];
    }

    !internal_crc
}

#[cfg(feature = "std")]
mod runtime;

#[cfg(feature = "std")]
pub use runtime::*;
