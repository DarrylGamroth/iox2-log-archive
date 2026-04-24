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

use core::num::NonZeroUsize;
use core::ptr::copy_nonoverlapping;

use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use iceoryx2::service::builder::{CustomHeaderMarker, CustomPayloadMarker};
use iceoryx2::service::static_config::message_type_details::TypeVariant;
use iox2_log_archive_core::log_archive::{
    ArchiveLocator, ArchiveReplayer, ArchiveSourcePattern, ReplayedFrame,
    decode_adapter_user_header,
};

use crate::dynamic_type::type_detail_with_layout;

use super::{ArchiveRematerializeError, DEFAULT_PUBSUB_REMATERIALIZER_NODE_NAME};

/// Builder for [`PubSubRematerializer`].
#[derive(Debug, Clone)]
pub struct PubSubRematerializerBuilder {
    service_name: String,
    node_name: String,
    payload_type_name: String,
    user_header_type_name: String,
    user_header_size: usize,
    user_header_alignment: usize,
    initial_max_slice_len: usize,
    allocation_strategy: AllocationStrategy,
    source_pattern_filter: Option<ArchiveSourcePattern>,
}

impl PubSubRematerializerBuilder {
    /// Creates a rematerializer builder for one target publish-subscribe service.
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            node_name: DEFAULT_PUBSUB_REMATERIALIZER_NODE_NAME.to_string(),
            payload_type_name: "u8".to_string(),
            user_header_type_name: "()".to_string(),
            user_header_size: 0,
            user_header_alignment: 1,
            initial_max_slice_len: 4096,
            allocation_strategy: AllocationStrategy::PowerOfTwo,
            source_pattern_filter: Some(ArchiveSourcePattern::PublishSubscribe),
        }
    }

    /// Overrides rematerializer node name.
    pub fn node_name(mut self, value: impl Into<String>) -> Self {
        self.node_name = value.into();
        self
    }

    /// Overrides payload type name exposed in service metadata.
    pub fn payload_type_name(mut self, value: impl Into<String>) -> Self {
        self.payload_type_name = value.into();
        self
    }

    /// Overrides user-header type name exposed in service metadata.
    pub fn user_header_type_name(mut self, value: impl Into<String>) -> Self {
        self.user_header_type_name = value.into();
        self
    }

    /// Sets required user-header size for rematerialized samples.
    pub fn user_header_size(mut self, value: usize) -> Self {
        self.user_header_size = value;
        self
    }

    /// Sets user-header alignment for service metadata.
    pub fn user_header_alignment(mut self, value: usize) -> Self {
        self.user_header_alignment = value;
        self
    }

    /// Sets initial dynamic slice capacity for publisher loans.
    pub fn initial_max_slice_len(mut self, value: usize) -> Self {
        self.initial_max_slice_len = value;
        self
    }

    /// Sets allocation strategy for dynamic payload loans.
    pub fn allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.allocation_strategy = value;
        self
    }

    /// Restricts rematerialization to a specific archived source pattern.
    ///
    /// `None` disables filtering.
    pub fn source_pattern_filter(mut self, value: Option<ArchiveSourcePattern>) -> Self {
        self.source_pattern_filter = value;
        self
    }

    /// Creates a configured [`PubSubRematerializer`].
    pub fn create(self) -> Result<PubSubRematerializer, ArchiveRematerializeError> {
        if self.service_name.trim().is_empty() {
            return Err(ArchiveRematerializeError::InvalidConfiguration(
                "service_name must not be empty",
            ));
        }
        if self.node_name.trim().is_empty() {
            return Err(ArchiveRematerializeError::InvalidConfiguration(
                "node_name must not be empty",
            ));
        }
        if self.user_header_alignment == 0 {
            return Err(ArchiveRematerializeError::InvalidConfiguration(
                "user_header_alignment must be > 0",
            ));
        }
        if self.initial_max_slice_len == 0 {
            return Err(ArchiveRematerializeError::InvalidConfiguration(
                "initial_max_slice_len must be > 0",
            ));
        }

        let node_name = NodeName::new(&self.node_name)
            .map_err(|error| ArchiveRematerializeError::InvalidNodeName(format!("{error:?}")))?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()
            .map_err(|error| ArchiveRematerializeError::NodeCreation(format!("{error:?}")))?;

        let service_name = ServiceName::new(&self.service_name)
            .map_err(|error| ArchiveRematerializeError::InvalidServiceName(format!("{error:?}")))?;

        let payload_type =
            type_detail_with_layout(&self.payload_type_name, TypeVariant::Dynamic, 1, 1)
                .map_err(ArchiveRematerializeError::InvalidTypeName)?;
        let user_header_type = type_detail_with_layout(
            &self.user_header_type_name,
            TypeVariant::FixedSize,
            self.user_header_size,
            self.user_header_alignment,
        )
        .map_err(ArchiveRematerializeError::InvalidTypeName)?;

        let service = unsafe {
            node.service_builder(&service_name)
                .publish_subscribe::<[CustomPayloadMarker]>()
                .user_header::<CustomHeaderMarker>()
                .__internal_set_payload_type_details(&payload_type)
                .__internal_set_user_header_type_details(&user_header_type)
                .open_or_create()
        }
        .map_err(|error| ArchiveRematerializeError::ServiceCreation(format!("{error:?}")))?;

        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(self.initial_max_slice_len)
            .allocation_strategy(self.allocation_strategy)
            .create()
            .map_err(|error| ArchiveRematerializeError::PublisherCreation(format!("{error:?}")))?;

        Ok(PubSubRematerializer {
            publisher,
            expected_user_header_size: self.user_header_size,
            source_pattern_filter: self.source_pattern_filter,
        })
    }
}

/// Adapter-out publisher for rematerializing archived frames into publish-subscribe services.
pub struct PubSubRematerializer {
    publisher: Publisher<ipc::Service, [CustomPayloadMarker], CustomHeaderMarker>,
    expected_user_header_size: usize,
    source_pattern_filter: Option<ArchiveSourcePattern>,
}

impl core::fmt::Debug for PubSubRematerializer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PubSubRematerializer")
            .field("expected_user_header_size", &self.expected_user_header_size)
            .field("source_pattern_filter", &self.source_pattern_filter)
            .finish()
    }
}

impl PubSubRematerializer {
    /// Returns configured source-pattern filter.
    pub fn source_pattern_filter(&self) -> Option<ArchiveSourcePattern> {
        self.source_pattern_filter
    }

    /// Returns expected user-header length for rematerialized samples.
    pub fn expected_user_header_size(&self) -> usize {
        self.expected_user_header_size
    }

    /// Rematerializes one frame into publish-subscribe service.
    ///
    /// Returns `Ok(None)` when source-pattern filter excludes this frame.
    /// Returns `Ok(Some(receivers))` when frame was published successfully.
    pub fn rematerialize_frame(
        &self,
        frame: &ReplayedFrame,
    ) -> Result<Option<usize>, ArchiveRematerializeError> {
        let (source_pattern, user_header_bytes) =
            if let Some(decoded) = decode_adapter_user_header(&frame.user_header) {
                (
                    Some(decoded.source_metadata.source_pattern),
                    decoded.user_header,
                )
            } else {
                (None, frame.user_header.as_slice())
            };

        if let Some(required_pattern) = self.source_pattern_filter {
            if source_pattern != Some(required_pattern) {
                return Ok(None);
            }
        }

        if user_header_bytes.len() != self.expected_user_header_size {
            return Err(ArchiveRematerializeError::IncompatibleUserHeaderSize {
                expected: self.expected_user_header_size,
                actual: user_header_bytes.len(),
                sequence: frame.sequence,
            });
        }

        let mut sample = unsafe { self.publisher.loan_custom_payload(frame.payload.len()) }
            .map_err(|error| ArchiveRematerializeError::Loan(format!("{error:?}")))?;
        if sample.payload().len() != frame.payload.len() {
            return Err(ArchiveRematerializeError::UnexpectedLoanedPayloadSize {
                expected: frame.payload.len(),
                actual: sample.payload().len(),
                sequence: frame.sequence,
            });
        }

        if !frame.payload.is_empty() {
            unsafe {
                copy_nonoverlapping(
                    frame.payload.as_ptr(),
                    sample.payload_mut().as_mut_ptr().cast(),
                    frame.payload.len(),
                );
            }
        }

        if !user_header_bytes.is_empty() {
            unsafe {
                copy_nonoverlapping(
                    user_header_bytes.as_ptr(),
                    (sample.user_header_mut() as *mut CustomHeaderMarker).cast(),
                    user_header_bytes.len(),
                );
            }
        }

        let sample = unsafe { sample.assume_init() };
        let receivers = sample
            .send()
            .map_err(|error| ArchiveRematerializeError::Send(format!("{error:?}")))?;
        Ok(Some(receivers))
    }

    /// Reads one frame by sequence and rematerializes it.
    pub fn rematerialize_sequence(
        &self,
        replayer: &ArchiveReplayer,
        sequence: u64,
    ) -> Result<Option<usize>, ArchiveRematerializeError> {
        let Some(frame) = replayer.read_at_sequence(sequence)? else {
            return Ok(None);
        };
        self.rematerialize_frame(&frame)
    }

    /// Reads one frame by locator and rematerializes it.
    pub fn rematerialize_locator(
        &self,
        replayer: &ArchiveReplayer,
        locator: ArchiveLocator,
    ) -> Result<Option<usize>, ArchiveRematerializeError> {
        let frame = replayer.read_at_locator(locator)?;
        self.rematerialize_frame(&frame)
    }

    /// Reads a sequence range and rematerializes all matching frames.
    ///
    /// Returned count equals number of published frames (filtered-out frames are not counted).
    pub fn rematerialize_range(
        &self,
        replayer: &ArchiveReplayer,
        sequence_start: u64,
        max_records: NonZeroUsize,
    ) -> Result<usize, ArchiveRematerializeError> {
        let frames = replayer.read_range(sequence_start, max_records)?;
        let mut published = 0usize;
        for frame in &frames {
            if self.rematerialize_frame(frame)?.is_some() {
                published += 1;
            }
        }
        Ok(published)
    }

    /// Reads a locator list and rematerializes all matching frames.
    ///
    /// Returned count equals number of published frames (filtered-out frames are not counted).
    pub fn rematerialize_locators(
        &self,
        replayer: &ArchiveReplayer,
        locators: &[ArchiveLocator],
    ) -> Result<usize, ArchiveRematerializeError> {
        let frames = replayer.read_many_locators(locators)?;
        let mut published = 0usize;
        for frame in &frames {
            if self.rematerialize_frame(frame)?.is_some() {
                published += 1;
            }
        }
        Ok(published)
    }
}
