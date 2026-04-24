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

//! Dynamic iceoryx2 type-detail helpers.
//!
//! Upstream iceoryx2 currently exposes arbitrary runtime `TypeDetail` layout
//! construction through the `iceoryx2::testing` helpers plus hidden builder
//! overrides. Keep that dependency isolated here so replacing it with a stable
//! public API later does not touch recorder/replay logic.

use iceoryx2::service::static_config::message_type_details::{TypeDetail, TypeName, TypeVariant};

/// Builds a runtime `TypeDetail` with caller-supplied layout metadata.
pub(crate) fn type_detail_with_layout(
    type_name: &str,
    variant: TypeVariant,
    size: usize,
    alignment: usize,
) -> Result<TypeDetail, String> {
    if alignment == 0 {
        return Err("type alignment must be greater than zero".to_string());
    }

    let type_name = TypeName::from_str_truncated(type_name)
        .map_err(|error| format!("invalid type name: {error:?}"))?;
    let mut value = TypeDetail::new::<()>(variant);
    iceoryx2::testing::type_detail_set_size(&mut value, size);
    iceoryx2::testing::type_detail_set_alignment(&mut value, alignment);
    iceoryx2::testing::type_detail_set_name(&mut value, type_name);
    Ok(value)
}
