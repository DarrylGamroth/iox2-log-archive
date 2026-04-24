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

#![warn(missing_docs)]

//! SQLite reference sink for `iox2-log-archive-core` metadata indexing.
//!
//! This crate is intentionally separate from the core log-archive crate so
//! database dependencies remain external tooling concerns.

mod conversion;
mod query;
mod schema;
mod sink;
mod types;
mod writer_lock;

pub use types::{
    DEFAULT_STREAM_ID, SQLITE_SCHEMA_VERSION, SqliteIndexerState, SqliteMetadataSink,
    SqliteTimeField,
};
pub use writer_lock::SqliteWriterLock;
