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

//! iceoryx2 integration adapters for `iox2-log-archive`.
//!
//! The archive core remains usable without an iceoryx2 dependency. This crate is
//! the public integration point for rematerializing archived records back into
//! iceoryx2 publish-subscribe services.

mod control;
mod record;
mod rematerialize;

pub use control::*;
pub use record::*;
pub use rematerialize::*;
