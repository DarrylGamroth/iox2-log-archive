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

use std::time::Duration;

use super::super::common::{
    ArchiveRecorder, ArchiveRecorderError, ArchiveSourceMetadata, ArchiveSourcePattern,
    PublishSubscribeRecordInput, RecordedCommit, RecorderAckLevel,
};

impl ArchiveRecorder {
    /// Appends one publish-subscribe record through the canonical adapter contract.
    ///
    /// The persisted archive sequence is recorder-local and monotonic. Optional
    /// source-sequence metadata is preserved in the adapter user-header prefix.
    pub fn append_publish_subscribe_record(
        &mut self,
        input: PublishSubscribeRecordInput<'_>,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let source_metadata = ArchiveSourceMetadata {
            source_pattern: ArchiveSourcePattern::PublishSubscribe,
            source_service_id: input.source_service_id,
            source_instance_id: input.source_publisher_id,
            source_sequence: input.source_sequence,
        };
        self.append_adapted_record(
            source_metadata,
            input.event_time_ns,
            input.user_header,
            input.payload,
        )
    }

    /// Appends one publish-subscribe record and waits for requested acknowledgment level.
    pub fn append_publish_subscribe_record_with_ack(
        &mut self,
        input: PublishSubscribeRecordInput<'_>,
        requested_ack: RecorderAckLevel,
        timeout: Duration,
    ) -> Result<RecordedCommit, ArchiveRecorderError> {
        let commit = self.append_publish_subscribe_record(input)?;
        self.wait_for_ack(commit, requested_ack, timeout)?;
        Ok(commit)
    }
}
