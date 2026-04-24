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

use iceoryx2_bb_testing::assert_that;
use iox2_log_archive_core::log_archive::*;

const GOLDEN_CATALOG_HEADER_V1: [u8; ARCHIVE_FILE_HEADER_V1_LEN] = [
    73, 79, 88, 50, 1, 0, 1, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 28, 140, 89, 236,
];

fn baseline_header(file_kind: ArchiveFileKind) -> ArchiveFileHeaderV1 {
    let mut header = ArchiveFileHeaderV1::new(file_kind);
    header.created_at_ns = 0x0102_0304_0506_0708;
    header.log_id = [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
        0x1F,
    ];
    header
}

#[test]
fn log_archive_file_header_v1_encode_decode_roundtrip_works_for_all_file_kinds() {
    let file_kinds = [
        ArchiveFileKind::Catalog,
        ArchiveFileKind::CommitIdxLog,
        ArchiveFileKind::SegmentData,
        ArchiveFileKind::SegmentIndex,
        ArchiveFileKind::SegmentMeta,
    ];

    for file_kind in file_kinds {
        let mut header = baseline_header(file_kind);
        if matches!(
            file_kind,
            ArchiveFileKind::SegmentData
                | ArchiveFileKind::SegmentIndex
                | ArchiveFileKind::SegmentMeta
        ) {
            header.segment_id = 0x8877_6655_4433_2211;
            header.segment_generation = 77;
        }

        let bytes = header.to_bytes().unwrap();
        let decoded = ArchiveFileHeaderV1::from_bytes(&bytes).unwrap();

        assert_that!(decoded.magic, eq ARCHIVE_FILE_MAGIC);
        assert_that!(decoded.file_kind, eq file_kind);
        assert_that!(decoded.major, eq ARCHIVE_FILE_VERSION_MAJOR_V1);
        assert_that!(decoded.minor, eq 0);
        assert_that!(decoded.header_len, eq ARCHIVE_FILE_HEADER_V1_LEN as u16);
        assert_that!(decoded.flags, eq 0);
        assert_that!(decoded.created_at_ns, eq header.created_at_ns);
        assert_that!(decoded.log_id, eq header.log_id);
        assert_that!(decoded.segment_id, eq header.segment_id);
        assert_that!(decoded.segment_generation, eq header.segment_generation);
    }
}

#[test]
fn log_archive_file_header_v1_rejects_invalid_magic() {
    let header = baseline_header(ArchiveFileKind::Catalog);
    let mut bytes = header.to_bytes().unwrap();
    bytes[0] = b'X';

    let result = ArchiveFileHeaderV1::from_bytes(&bytes);
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::InvalidMagic([
            b'X', b'O', b'X', b'2'
        ]))
    );
}

#[test]
fn log_archive_file_header_v1_rejects_unknown_file_kind() {
    let header = baseline_header(ArchiveFileKind::Catalog);
    let mut bytes = header.to_bytes().unwrap();
    bytes[4..6].copy_from_slice(&99u16.to_le_bytes());

    let result = ArchiveFileHeaderV1::from_bytes(&bytes);
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::UnsupportedFileKind(99))
    );
}

#[test]
fn log_archive_file_header_v1_rejects_header_len_smaller_than_v1_base_header() {
    let header = baseline_header(ArchiveFileKind::Catalog);
    let mut bytes = header.to_bytes().unwrap();
    bytes[10..12].copy_from_slice(&(ARCHIVE_FILE_HEADER_V1_LEN as u16 - 1).to_le_bytes());

    let result = ArchiveFileHeaderV1::from_bytes(&bytes);
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::HeaderLengthTooSmall(
            ARCHIVE_FILE_HEADER_V1_LEN as u16 - 1
        ))
    );
}

#[test]
fn log_archive_file_header_v1_rejects_unsupported_major_version() {
    let mut header = baseline_header(ArchiveFileKind::Catalog);
    header.major = 2;

    let result = header.to_bytes();
    assert_that!(result.err(), eq Some(ArchiveFileHeaderError::UnsupportedMajor(2)));
}

#[test]
fn log_archive_file_header_v1_rejects_unsupported_minor_version() {
    let mut header = baseline_header(ArchiveFileKind::Catalog);
    header.minor = 1;

    let result = header.to_bytes();
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::UnsupportedMinor {
            file_kind: ArchiveFileKind::Catalog,
            minor: 1,
            supported_minor: 0
        })
    );
}

#[test]
fn log_archive_file_header_v1_rejects_unknown_must_understand_flags() {
    let mut header = baseline_header(ArchiveFileKind::Catalog);
    header.flags = 0xAB00_0000;

    let result = header.to_bytes();
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::UnknownMustUnderstandFlags(
            0xAB00_0000
        ))
    );
}

#[test]
fn log_archive_file_header_v1_rejects_crc_mismatch() {
    let header = baseline_header(ArchiveFileKind::Catalog);
    let mut bytes = header.to_bytes().unwrap();
    bytes[20] ^= 0x55;

    let result = ArchiveFileHeaderV1::from_bytes(&bytes);
    assert_that!(result.err(), is_some);
    assert_that!(
        matches!(
            result.err().unwrap(),
            ArchiveFileHeaderError::InvalidHeaderCrc { .. }
        ),
        eq true
    );
}

#[test]
fn log_archive_file_header_v1_rejects_non_zero_segment_identity_for_catalog_file() {
    let mut header = baseline_header(ArchiveFileKind::Catalog);
    header.segment_id = 1;
    header.segment_generation = 9;

    let result = header.to_bytes();
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::NonZeroSegmentIdentityForGlobalFile {
            file_kind: ArchiveFileKind::Catalog,
            segment_id: 1,
            segment_generation: 9
        })
    );
}

#[test]
fn log_archive_file_header_v1_rejects_non_zero_segment_identity_for_commit_idxlog_file() {
    let mut header = baseline_header(ArchiveFileKind::CommitIdxLog);
    header.segment_id = 1;

    let result = header.to_bytes();
    assert_that!(
        result.err(),
        eq Some(ArchiveFileHeaderError::NonZeroSegmentIdentityForGlobalFile {
            file_kind: ArchiveFileKind::CommitIdxLog,
            segment_id: 1,
            segment_generation: 0
        })
    );
}

#[test]
fn log_archive_file_header_v1_rejects_zero_segment_id_for_segment_scoped_files() {
    let file_kinds = [
        ArchiveFileKind::SegmentData,
        ArchiveFileKind::SegmentIndex,
        ArchiveFileKind::SegmentMeta,
    ];

    for file_kind in file_kinds {
        let mut header = baseline_header(file_kind);
        header.segment_id = 0;

        let result = header.to_bytes();
        assert_that!(
            result.err(),
            eq Some(ArchiveFileHeaderError::ZeroSegmentIdForSegmentFile {
                file_kind
            })
        );
    }
}

#[test]
fn log_archive_file_header_v1_matches_golden_catalog_fixture() {
    let header = ArchiveFileHeaderV1::new(ArchiveFileKind::Catalog);
    let bytes = header.to_bytes().unwrap();
    assert_that!(bytes, eq GOLDEN_CATALOG_HEADER_V1);
}
