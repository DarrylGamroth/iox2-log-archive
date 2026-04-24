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

use std::time::{SystemTime, UNIX_EPOCH};

use iox2_log_archive_core::log_archive::{ArchiveMetadataSinkError, ArchiveSourcePattern};

pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as u64)
        .unwrap_or(0)
}

pub(crate) fn u64_to_i64(value: u64, field: &str) -> Result<i64, ArchiveMetadataSinkError> {
    if value > i64::MAX as u64 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from u64 to i64"
        )));
    }
    Ok(value as i64)
}

pub(crate) fn usize_to_i64(value: usize, field: &str) -> Result<i64, ArchiveMetadataSinkError> {
    if value > i64::MAX as usize {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from usize to i64"
        )));
    }
    Ok(value as i64)
}

pub(crate) fn u32_to_i64(value: u32) -> i64 {
    value as i64
}

pub(crate) fn option_u64_to_option_i64(
    value: Option<u64>,
    field: &str,
) -> Result<Option<i64>, ArchiveMetadataSinkError> {
    value.map(|v| u64_to_i64(v, field)).transpose()
}

pub(crate) fn source_pattern_to_i64(value: ArchiveSourcePattern) -> i64 {
    (value as u8) as i64
}

pub(crate) fn i64_to_u64(value: i64, field: &str) -> Result<u64, ArchiveMetadataSinkError> {
    if value < 0 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "negative sqlite value for {field}"
        )));
    }
    Ok(value as u64)
}

pub(crate) fn i64_to_u32(value: i64, field: &str) -> Result<u32, ArchiveMetadataSinkError> {
    let unsigned = i64_to_u64(value, field)?;
    if unsigned > u32::MAX as u64 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from i64 to u32"
        )));
    }
    Ok(unsigned as u32)
}

pub(crate) fn i64_to_source_pattern(
    value: i64,
    field: &str,
) -> Result<ArchiveSourcePattern, ArchiveMetadataSinkError> {
    let unsigned = i64_to_u64(value, field)?;
    if unsigned > u8::MAX as u64 {
        return Err(ArchiveMetadataSinkError::new(format!(
            "value overflow converting {field} from i64 to ArchiveSourcePattern"
        )));
    }
    ArchiveSourcePattern::try_from(unsigned as u8).map_err(|_| {
        ArchiveMetadataSinkError::new(format!(
            "unknown source pattern discriminator in sqlite for {field}"
        ))
    })
}
