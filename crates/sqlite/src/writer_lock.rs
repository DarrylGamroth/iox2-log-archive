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

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use iox2_log_archive_core::log_archive::ArchiveMetadataSinkError;

/// Single-writer lock for an index database.
///
/// The lock is process-scoped and held for the lifetime of this value.
#[derive(Debug)]
pub struct SqliteWriterLock {
    lock_path: PathBuf,
    _file: File,
}

impl SqliteWriterLock {
    /// Acquires an exclusive lock for `db_path`.
    ///
    /// Returns an explicit error when another index writer already owns the lock.
    pub fn acquire(db_path: &Path) -> Result<Self, ArchiveMetadataSinkError> {
        let lock_path = lock_path_for_db(db_path)?;
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                ArchiveMetadataSinkError::new(format!(
                    "create writer lock directory failed ({}): {err}",
                    parent.display()
                ))
            })?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .map_err(|err| {
                let details = if err.kind() == std::io::ErrorKind::AlreadyExists {
                    format!(
                        "index writer lock is already held for '{}'",
                        db_path.display()
                    )
                } else {
                    format!(
                        "acquire writer lock failed for '{}' (lock='{}'): {err}",
                        db_path.display(),
                        lock_path.display()
                    )
                };
                ArchiveMetadataSinkError::new(details)
            })?;

        let _ = writeln!(file, "pid={}", std::process::id());

        Ok(Self {
            lock_path,
            _file: file,
        })
    }

    /// Returns the lock-file path.
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for SqliteWriterLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock_path);
    }
}

fn lock_path_for_db(db_path: &Path) -> Result<PathBuf, ArchiveMetadataSinkError> {
    let file_name = db_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            ArchiveMetadataSinkError::new(format!(
                "db path '{}' must have a valid file name",
                db_path.display()
            ))
        })?;
    Ok(db_path.with_file_name(format!("{file_name}.writer.lock")))
}
