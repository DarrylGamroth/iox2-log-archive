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

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use iox2_log_archive_cli::Format;
use iox2_log_archive_core::log_archive::{
    ArchiveLiveReplayer, ArchiveLocator, ArchiveReplayError, ArchiveReplayerBuilder, ReplayedFrame,
    decode_adapter_user_header,
};
use iox2_log_archive_iceoryx2::{
    ArchiveRematerializeError, PubSubRematerializer, PubSubRematerializerBuilder,
};
use serde::{Deserialize, Serialize};

use crate::cli::{
    LocatorSelector, LogReplayAction, RangeSelector, ReplayDestination, ReplayOptions,
    ReplayRateMode, ReplaySelector, SelectorFormat, SelectorsSelector, SequenceSelector,
};

#[derive(Debug)]
pub(crate) enum LogReplayCommandError {
    InvalidInput(String),
    NotAvailable(String),
    Internal(anyhow::Error),
}

impl LogReplayCommandError {
    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::Internal(_) => 1,
            Self::InvalidInput(_) => 2,
            Self::NotAvailable(_) => 3,
        }
    }

    pub(crate) fn to_formatted_error(&self, format: Format) -> String {
        #[derive(Serialize)]
        struct ErrorPayload<'a> {
            error_code: &'a str,
            message: &'a str,
        }

        let payload = match self {
            LogReplayCommandError::InvalidInput(message) => ErrorPayload {
                error_code: "InvalidInput",
                message,
            },
            LogReplayCommandError::NotAvailable(message) => ErrorPayload {
                error_code: "NotAvailable",
                message,
            },
            LogReplayCommandError::Internal(error) => ErrorPayload {
                error_code: "Internal",
                message: &format!("{error:#}"),
            },
        };

        format
            .as_string(&payload)
            .unwrap_or_else(|_| format!("{:?}", payload.error_code))
    }
}

impl core::fmt::Display for LogReplayCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "{message}"),
            Self::NotAvailable(message) => write!(f, "{message}"),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for LogReplayCommandError {}

#[derive(Debug, Clone)]
struct ArchivePaths {
    storage_path: PathBuf,
    metadata_log_path: PathBuf,
}

impl ArchivePaths {
    fn from_options(options: &ReplayOptions) -> Self {
        let storage_path = options.storage_path.clone();
        let metadata_log_path = options
            .metadata_log_path
            .clone()
            .unwrap_or_else(|| storage_path.clone());

        Self {
            storage_path,
            metadata_log_path,
        }
    }

    fn ensure_archive_exists(&self) -> Result<(), LogReplayCommandError> {
        if !self.storage_path.join("catalog.bin").exists() {
            return Err(LogReplayCommandError::NotAvailable(format!(
                "archive not found at {}",
                self.storage_path.display()
            )));
        }

        if !self.metadata_log_path.join("commit.idxlog").exists() {
            return Err(LogReplayCommandError::NotAvailable(format!(
                "commit.idxlog not found at {}",
                self.metadata_log_path.display()
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Selector {
    Sequence(u64),
    Range { from: u64, count: usize },
    Locator(ArchiveLocator),
}

#[derive(Debug, Clone, Serialize)]
struct ReplaySummary {
    operation: &'static str,
    storage_path: String,
    metadata_log_path: String,
    destination: &'static str,
    service: Option<String>,
    selector_source: String,
    rate_mode: &'static str,
    live_mode: bool,
    selected: usize,
    emitted: usize,
    skipped_missing: usize,
    errors: usize,
    bytes_emitted: usize,
    elapsed_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_visible_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_visible_commit_ordinal: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ReplayCounters {
    selected: usize,
    emitted: usize,
    skipped_missing: usize,
    errors: usize,
    bytes_emitted: usize,
}

#[derive(Debug, Clone, Copy)]
struct MissingPolicy {
    skip_missing: bool,
    max_errors: usize,
}

struct LiveReplayContext<'a> {
    emitter: &'a mut ReplayEmitter,
    pacer: &'a mut ReplayPacer,
    counters: &'a mut ReplayCounters,
    missing_policy: MissingPolicy,
}

#[derive(Debug, Clone, Copy)]
struct FollowConfig {
    poll_interval: Duration,
    idle_timeout: Option<Duration>,
}

impl FollowConfig {
    fn from_options(options: &ReplayOptions) -> Result<Self, LogReplayCommandError> {
        if !options.follow {
            if options.follow_idle_timeout_ms.is_some() {
                return Err(LogReplayCommandError::InvalidInput(
                    "--follow-idle-timeout-ms is only valid with --follow".to_string(),
                ));
            }
            return Ok(Self {
                poll_interval: Duration::from_millis(options.follow_poll_ms),
                idle_timeout: None,
            });
        }

        if options.follow_poll_ms == 0 {
            return Err(LogReplayCommandError::InvalidInput(
                "--follow-poll-ms must be > 0".to_string(),
            ));
        }

        Ok(Self {
            poll_interval: Duration::from_millis(options.follow_poll_ms),
            idle_timeout: options.follow_idle_timeout_ms.map(Duration::from_millis),
        })
    }

    fn wait_timeout(self) -> Duration {
        self.idle_timeout
            .unwrap_or_else(|| Duration::from_secs(24 * 60 * 60))
    }
}

impl ReplayCounters {
    fn handle_missing(
        &mut self,
        missing: usize,
        skip_missing: bool,
        max_errors: usize,
        context: &str,
    ) -> Result<(), LogReplayCommandError> {
        if missing == 0 {
            return Ok(());
        }

        if skip_missing {
            self.skipped_missing = self.skipped_missing.saturating_add(missing);
            return Ok(());
        }

        self.errors = self.errors.saturating_add(missing);
        if self.errors >= max_errors {
            return Err(LogReplayCommandError::NotAvailable(format!(
                "{context}; reached error limit ({}/{})",
                self.errors, max_errors
            )));
        }

        Ok(())
    }
}

struct ReplayPacer {
    mode: ReplayRateMode,
    fixed_interval: Option<Duration>,
    max_recorded_gap: Duration,
    last_event_time_ns: Option<u64>,
    emitted_records: usize,
}

impl ReplayPacer {
    fn new(options: &ReplayOptions) -> Result<Self, LogReplayCommandError> {
        let fixed_interval = match options.rate {
            ReplayRateMode::Fixed => {
                let messages_per_sec = options.messages_per_sec.ok_or_else(|| {
                    LogReplayCommandError::InvalidInput(
                        "--messages-per-sec is required when --rate=fixed".to_string(),
                    )
                })?;
                if messages_per_sec == 0 {
                    return Err(LogReplayCommandError::InvalidInput(
                        "--messages-per-sec must be > 0".to_string(),
                    ));
                }

                Some(Duration::from_secs_f64(1.0 / messages_per_sec as f64))
            }
            _ => {
                if options.messages_per_sec.is_some() {
                    return Err(LogReplayCommandError::InvalidInput(
                        "--messages-per-sec is only valid when --rate=fixed".to_string(),
                    ));
                }
                None
            }
        };

        Ok(Self {
            mode: options.rate,
            fixed_interval,
            max_recorded_gap: Duration::from_millis(options.max_recorded_gap_ms),
            last_event_time_ns: None,
            emitted_records: 0,
        })
    }

    fn pace(&mut self, frame: &ReplayedFrame) {
        match self.mode {
            ReplayRateMode::Fast => {}
            ReplayRateMode::Fixed => {
                if self.emitted_records > 0 {
                    if let Some(interval) = self.fixed_interval {
                        std::thread::sleep(interval);
                    }
                }
            }
            ReplayRateMode::Recorded => {
                if let Some(previous) = self.last_event_time_ns {
                    if frame.event_time_ns > previous {
                        let delta_ns = frame.event_time_ns - previous;
                        let delay = Duration::from_nanos(delta_ns).min(self.max_recorded_gap);
                        if !delay.is_zero() {
                            std::thread::sleep(delay);
                        }
                    }
                }
                self.last_event_time_ns = Some(frame.event_time_ns);
            }
        }

        self.emitted_records = self.emitted_records.saturating_add(1);
    }
}

struct StdoutEmitter;

impl StdoutEmitter {
    fn emit(frame: &ReplayedFrame) -> Result<usize, LogReplayCommandError> {
        #[derive(Serialize)]
        struct StdoutFrame {
            commit_ordinal: u64,
            sequence: u64,
            event_time_ns: u64,
            commit_time_ns: u64,
            segment_id: u64,
            segment_generation: u32,
            file_offset: u64,
            frame_len: u32,
            user_header_hex: String,
            payload_hex: String,
        }

        let stdout_frame = StdoutFrame {
            commit_ordinal: frame.commit_ordinal,
            sequence: frame.sequence,
            event_time_ns: frame.event_time_ns,
            commit_time_ns: frame.commit_time_ns,
            segment_id: frame.locator.segment_id,
            segment_generation: frame.locator.segment_generation,
            file_offset: frame.locator.file_offset,
            frame_len: frame.locator.frame_len,
            user_header_hex: bytes_to_hex(&frame.user_header),
            payload_hex: bytes_to_hex(&frame.payload),
        };

        let line = serde_json::to_string(&stdout_frame)
            .map_err(|error| LogReplayCommandError::Internal(anyhow!(error)))?;
        println!("{line}");
        Ok(1)
    }
}

enum ReplayEmitter {
    Stdout,
    PublishSubscribe {
        service_name: String,
        node_name: String,
        rematerializer: Option<PubSubRematerializer>,
    },
}

impl ReplayEmitter {
    fn from_options(options: &ReplayOptions) -> Result<Self, LogReplayCommandError> {
        match options.to {
            ReplayDestination::Stdout => {
                if options.service.is_some() {
                    return Err(LogReplayCommandError::InvalidInput(
                        "--service is only valid with --to=publish-subscribe".to_string(),
                    ));
                }
                Ok(Self::Stdout)
            }
            ReplayDestination::PublishSubscribe => {
                let service_name = required_destination_service(options)?;
                Ok(Self::PublishSubscribe {
                    service_name,
                    node_name: options.node_name.clone(),
                    rematerializer: None,
                })
            }
        }
    }

    fn destination_label(&self) -> &'static str {
        match self {
            ReplayEmitter::Stdout => "stdout",
            ReplayEmitter::PublishSubscribe { .. } => "publish-subscribe",
        }
    }

    fn destination_service(&self) -> Option<String> {
        match self {
            ReplayEmitter::Stdout => None,
            ReplayEmitter::PublishSubscribe { service_name, .. } => Some(service_name.clone()),
        }
    }

    fn emit(&mut self, frame: &ReplayedFrame) -> Result<usize, LogReplayCommandError> {
        match self {
            ReplayEmitter::Stdout => StdoutEmitter::emit(frame),
            ReplayEmitter::PublishSubscribe {
                service_name,
                node_name,
                rematerializer,
            } => {
                if rematerializer.is_none() {
                    let user_header_len = effective_user_header(frame).len();
                    let builder = PubSubRematerializerBuilder::new(service_name.clone())
                        .node_name(node_name.clone())
                        .user_header_size(user_header_len)
                        .source_pattern_filter(None);
                    let value = builder.create().map_err(map_rematerialize_error)?;
                    *rematerializer = Some(value);
                }

                rematerializer
                    .as_ref()
                    .expect("rematerializer must be initialized")
                    .rematerialize_frame(frame)
                    .map_err(map_rematerialize_error)?
                    .ok_or_else(|| {
                        LogReplayCommandError::Internal(anyhow!(
                            "publish-subscribe rematerializer unexpectedly filtered frame"
                        ))
                    })
            }
        }
    }
}

pub(crate) fn log_replay(
    action: LogReplayAction,
    format: Format,
) -> Result<(), LogReplayCommandError> {
    match action {
        LogReplayAction::Replay(options) => replay(options, format),
    }
}

fn replay(options: ReplayOptions, format: Format) -> Result<(), LogReplayCommandError> {
    if options.node_name.trim().is_empty() {
        return Err(LogReplayCommandError::InvalidInput(
            "--node-name must not be empty".to_string(),
        ));
    }

    if let Some(max_errors) = options.max_errors {
        if max_errors == 0 {
            return Err(LogReplayCommandError::InvalidInput(
                "--max-errors must be > 0".to_string(),
            ));
        }
    }
    let follow_config = FollowConfig::from_options(&options)?;
    if options.follow
        && matches!(
            &options.selector,
            ReplaySelector::Locator(_) | ReplaySelector::Selectors(_)
        )
    {
        return Err(LogReplayCommandError::InvalidInput(
            "--follow supports all, sequence, and range selectors".to_string(),
        ));
    }

    let paths = ArchivePaths::from_options(&options);
    paths.ensure_archive_exists()?;

    let mut emitter = ReplayEmitter::from_options(&options)?;
    let mut pacer = ReplayPacer::new(&options)?;

    let max_errors =
        options
            .max_errors
            .unwrap_or(if options.skip_missing { usize::MAX } else { 1 });

    let selector_source = selector_source_label(&options.selector);
    let started = Instant::now();
    let mut counters = ReplayCounters {
        selected: 0,
        emitted: 0,
        skipped_missing: 0,
        errors: 0,
        bytes_emitted: 0,
    };
    let live_status = if options.follow {
        let mut live_replayer = ArchiveReplayerBuilder::new(&paths.storage_path)
            .metadata_log_path(&paths.metadata_log_path)
            .open_live()
            .map_err(map_replay_error)?;
        replay_live(
            &options,
            follow_config,
            &mut live_replayer,
            LiveReplayContext {
                emitter: &mut emitter,
                pacer: &mut pacer,
                counters: &mut counters,
                missing_policy: MissingPolicy {
                    skip_missing: options.skip_missing,
                    max_errors,
                },
            },
        )?;
        Some(live_replayer.status())
    } else {
        let mut replayer = ArchiveReplayerBuilder::new(&paths.storage_path)
            .metadata_log_path(&paths.metadata_log_path)
            .open()
            .map_err(map_replay_error)?;

        match &options.selector {
            ReplaySelector::All => {
                replay_all(&mut replayer, &mut emitter, &mut pacer, &mut counters)?;
            }
            ReplaySelector::Sequence(SequenceSelector { at }) => {
                replay_selector(
                    Selector::Sequence(*at),
                    &replayer,
                    &mut emitter,
                    &mut pacer,
                    &mut counters,
                    options.skip_missing,
                    max_errors,
                )?;
            }
            ReplaySelector::Range(RangeSelector { from, count }) => {
                if *count == 0 {
                    return Err(LogReplayCommandError::InvalidInput(
                        "--count must be > 0".to_string(),
                    ));
                }
                replay_selector(
                    Selector::Range {
                        from: *from,
                        count: *count,
                    },
                    &replayer,
                    &mut emitter,
                    &mut pacer,
                    &mut counters,
                    options.skip_missing,
                    max_errors,
                )?;
            }
            ReplaySelector::Locator(LocatorSelector { at }) => {
                replay_selector(
                    Selector::Locator(parse_locator(at)?),
                    &replayer,
                    &mut emitter,
                    &mut pacer,
                    &mut counters,
                    options.skip_missing,
                    max_errors,
                )?;
            }
            ReplaySelector::Selectors(selector_options) => {
                replay_selector_stream(
                    selector_options,
                    &replayer,
                    &mut emitter,
                    &mut pacer,
                    &mut counters,
                    options.skip_missing,
                    max_errors,
                )?;
            }
        }
        None
    };

    if counters.selected == 0 {
        return Err(LogReplayCommandError::InvalidInput(
            "no selectors resolved for replay".to_string(),
        ));
    }

    let summary = ReplaySummary {
        operation: "replay",
        storage_path: paths.storage_path.display().to_string(),
        metadata_log_path: paths.metadata_log_path.display().to_string(),
        destination: emitter.destination_label(),
        service: emitter.destination_service(),
        selector_source,
        rate_mode: rate_mode_label(options.rate),
        live_mode: options.follow,
        selected: counters.selected,
        emitted: counters.emitted,
        skipped_missing: counters.skipped_missing,
        errors: counters.errors,
        bytes_emitted: counters.bytes_emitted,
        elapsed_ms: started.elapsed().as_millis(),
        last_visible_sequence: live_status.and_then(|status| status.last_visible_sequence),
        last_visible_commit_ordinal: live_status.map(|status| status.last_visible_commit_ordinal),
    };

    if matches!(emitter, ReplayEmitter::Stdout) {
        print_output_to_stderr(&summary, format)
    } else {
        print_output_to_stdout(&summary, format)
    }
}

fn replay_all(
    replayer: &mut iox2_log_archive_core::log_archive::ArchiveReplayer,
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    counters: &mut ReplayCounters,
) -> Result<(), LogReplayCommandError> {
    replayer.seek(0);
    let batch_size = NonZeroUsize::new(1024).expect("constant batch size is non-zero");
    loop {
        let batch = replayer.next_batch(batch_size).map_err(map_replay_error)?;
        if batch.is_empty() {
            break;
        }
        counters.selected = counters.selected.saturating_add(batch.len());
        for frame in &batch {
            emit_one(emitter, pacer, frame, counters)?;
        }
    }

    Ok(())
}

fn replay_live(
    options: &ReplayOptions,
    follow_config: FollowConfig,
    replayer: &mut ArchiveLiveReplayer,
    mut context: LiveReplayContext<'_>,
) -> Result<(), LogReplayCommandError> {
    match &options.selector {
        ReplaySelector::All => {
            replay_live_all(replayer, follow_config, &mut context)?;
        }
        ReplaySelector::Sequence(SequenceSelector { at }) => {
            replay_live_sequence(replayer, follow_config, *at, &mut context)?;
        }
        ReplaySelector::Range(RangeSelector { from, count }) => {
            if *count == 0 {
                return Err(LogReplayCommandError::InvalidInput(
                    "--count must be > 0".to_string(),
                ));
            }
            replay_live_range(replayer, follow_config, *from, *count, &mut context)?;
        }
        ReplaySelector::Locator(_) | ReplaySelector::Selectors(_) => {
            return Err(LogReplayCommandError::InvalidInput(
                "--follow supports all, sequence, and range selectors".to_string(),
            ));
        }
    }

    Ok(())
}

fn replay_live_all(
    replayer: &mut ArchiveLiveReplayer,
    follow_config: FollowConfig,
    context: &mut LiveReplayContext<'_>,
) -> Result<(), LogReplayCommandError> {
    replayer.seek(0);
    let batch_size = NonZeroUsize::new(1024).expect("constant batch size is non-zero");
    loop {
        let batch = replayer
            .next_live_batch(
                batch_size,
                follow_config.poll_interval,
                follow_config.wait_timeout(),
            )
            .map_err(map_replay_error)?;
        if batch.is_empty() {
            break;
        }
        context.counters.selected = context.counters.selected.saturating_add(batch.len());
        for frame in &batch {
            emit_one(context.emitter, context.pacer, frame, context.counters)?;
        }
    }

    Ok(())
}

fn replay_live_sequence(
    replayer: &mut ArchiveLiveReplayer,
    follow_config: FollowConfig,
    sequence: u64,
    context: &mut LiveReplayContext<'_>,
) -> Result<(), LogReplayCommandError> {
    context.counters.selected = context.counters.selected.saturating_add(1);
    let started = Instant::now();
    loop {
        replayer.refresh().map_err(map_replay_error)?;
        if let Some(frame) = replayer
            .read_at_sequence(sequence)
            .map_err(map_replay_error)?
        {
            return emit_one(context.emitter, context.pacer, &frame, context.counters);
        }
        if follow_config
            .idle_timeout
            .is_some_and(|timeout| started.elapsed() >= timeout)
        {
            return context.counters.handle_missing(
                1,
                context.missing_policy.skip_missing,
                context.missing_policy.max_errors,
                &format!("sequence {sequence} is not available before follow idle timeout"),
            );
        }
        std::thread::sleep(follow_config.poll_interval);
    }
}

fn replay_live_range(
    replayer: &mut ArchiveLiveReplayer,
    follow_config: FollowConfig,
    from: u64,
    count: usize,
    context: &mut LiveReplayContext<'_>,
) -> Result<(), LogReplayCommandError> {
    context.counters.selected = context.counters.selected.saturating_add(count);
    replayer.seek(from);
    let batch_size = NonZeroUsize::new(1024).expect("constant batch size is non-zero");
    let mut remaining = count;
    let mut emitted_for_range = 0usize;

    while remaining > 0 {
        let request = NonZeroUsize::new(remaining.min(batch_size.get()))
            .expect("remaining is non-zero inside live range loop");
        let batch = replayer
            .next_live_batch(
                request,
                follow_config.poll_interval,
                follow_config.wait_timeout(),
            )
            .map_err(map_replay_error)?;
        if batch.is_empty() {
            break;
        }
        for frame in &batch {
            emit_one(context.emitter, context.pacer, frame, context.counters)?;
        }
        emitted_for_range = emitted_for_range.saturating_add(batch.len());
        remaining = remaining.saturating_sub(batch.len());
    }

    let missing = count.saturating_sub(emitted_for_range);
    context.counters.handle_missing(
        missing,
        context.missing_policy.skip_missing,
        context.missing_policy.max_errors,
        &format!("range from {from} count {count} resolved only {emitted_for_range} records"),
    )
}

fn replay_selector(
    selector: Selector,
    replayer: &iox2_log_archive_core::log_archive::ArchiveReplayer,
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    counters: &mut ReplayCounters,
    skip_missing: bool,
    max_errors: usize,
) -> Result<(), LogReplayCommandError> {
    match selector {
        Selector::Sequence(sequence) => {
            counters.selected = counters.selected.saturating_add(1);
            let frame = replayer
                .read_at_sequence(sequence)
                .map_err(map_replay_error)?;

            let Some(frame) = frame else {
                counters.handle_missing(
                    1,
                    skip_missing,
                    max_errors,
                    &format!("sequence {sequence} is not available"),
                )?;
                return Ok(());
            };

            emit_one(emitter, pacer, &frame, counters)
        }
        Selector::Locator(locator) => {
            counters.selected = counters.selected.saturating_add(1);
            let frame = match replayer.read_at_locator(locator) {
                Ok(value) => value,
                Err(ArchiveReplayError::MissingSegment(_)) => {
                    counters.handle_missing(
                        1,
                        skip_missing,
                        max_errors,
                        &format!(
                            "locator {}:{}:{}:{} is not available",
                            locator.segment_id,
                            locator.segment_generation,
                            locator.file_offset,
                            locator.frame_len
                        ),
                    )?;
                    return Ok(());
                }
                Err(error) => return Err(map_replay_error(error)),
            };

            emit_one(emitter, pacer, &frame, counters)
        }
        Selector::Range { from, count } => {
            counters.selected = counters.selected.saturating_add(count);
            let mut remaining = count;
            let mut next_sequence = from;
            let mut emitted_for_range = 0usize;

            while remaining > 0 {
                let batch = read_range_batch(replayer, next_sequence, remaining)?;
                if batch.is_empty() {
                    break;
                }

                for frame in &batch {
                    emit_one(emitter, pacer, frame, counters)?;
                }

                emitted_for_range = emitted_for_range.saturating_add(batch.len());
                remaining = remaining.saturating_sub(batch.len());
                let next = batch
                    .last()
                    .expect("batch is not empty")
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| {
                        LogReplayCommandError::InvalidInput(
                            "sequence overflow while replaying range".to_string(),
                        )
                    })?;
                next_sequence = next;
            }

            let missing = count.saturating_sub(emitted_for_range);
            if missing > 0 {
                counters.handle_missing(
                    missing,
                    skip_missing,
                    max_errors,
                    &format!(
                        "range from {from} count {count} resolved only {emitted_for_range} records"
                    ),
                )?;
            }

            Ok(())
        }
    }
}

fn emit_one(
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    frame: &ReplayedFrame,
    counters: &mut ReplayCounters,
) -> Result<(), LogReplayCommandError> {
    pacer.pace(frame);
    let _receivers = emitter.emit(frame)?;
    counters.emitted = counters.emitted.saturating_add(1);
    counters.bytes_emitted = counters
        .bytes_emitted
        .saturating_add(frame.payload.len())
        .saturating_add(effective_user_header(frame).len());
    Ok(())
}

fn read_range_batch(
    replayer: &iox2_log_archive_core::log_archive::ArchiveReplayer,
    from: u64,
    remaining: usize,
) -> Result<Vec<ReplayedFrame>, LogReplayCommandError> {
    let batch_size = remaining.min(1024);
    let batch_size = NonZeroUsize::new(batch_size).ok_or_else(|| {
        LogReplayCommandError::InvalidInput("range replay batch size must be > 0".to_string())
    })?;

    replayer
        .read_range(from, batch_size)
        .map_err(map_replay_error)
}

fn replay_selector_stream(
    options: &SelectorsSelector,
    replayer: &iox2_log_archive_core::log_archive::ArchiveReplayer,
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    counters: &mut ReplayCounters,
    skip_missing: bool,
    max_errors: usize,
) -> Result<(), LogReplayCommandError> {
    let reader = selector_reader(options)?;

    match options.selector_format {
        SelectorFormat::Ndjson => replay_ndjson_selectors(
            reader,
            replayer,
            emitter,
            pacer,
            counters,
            skip_missing,
            max_errors,
        ),
        SelectorFormat::Csv => replay_csv_selectors(
            reader,
            replayer,
            emitter,
            pacer,
            counters,
            skip_missing,
            max_errors,
        ),
    }
}

fn selector_reader(options: &SelectorsSelector) -> Result<Box<dyn BufRead>, LogReplayCommandError> {
    if options.stdin {
        return Ok(Box::new(BufReader::new(io::stdin())));
    }
    if let Some(path) = &options.file {
        let file = File::open(path).map_err(|source| {
            LogReplayCommandError::Internal(anyhow!(
                "failed to open selector file {}: {source}",
                path.display()
            ))
        })?;
        return Ok(Box::new(BufReader::new(file)));
    }

    Err(LogReplayCommandError::InvalidInput(
        "either --stdin or --file is required".to_string(),
    ))
}

#[derive(Debug, Deserialize)]
struct NdjsonSelectorRecord {
    kind: String,
    sequence: Option<u64>,
    from: Option<u64>,
    count: Option<usize>,
    segment_id: Option<u64>,
    segment_generation: Option<u32>,
    file_offset: Option<u64>,
    frame_len: Option<u32>,
}

fn replay_ndjson_selectors(
    mut reader: Box<dyn BufRead>,
    replayer: &iox2_log_archive_core::log_archive::ArchiveReplayer,
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    counters: &mut ReplayCounters,
    skip_missing: bool,
    max_errors: usize,
) -> Result<(), LogReplayCommandError> {
    let mut line = String::new();
    let mut line_number = 0usize;

    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| LogReplayCommandError::Internal(anyhow!(error)))?;
        if bytes == 0 {
            break;
        }

        line_number = line_number.saturating_add(1);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record: NdjsonSelectorRecord = serde_json::from_str(trimmed).map_err(|error| {
            LogReplayCommandError::InvalidInput(format!(
                "invalid ndjson selector at line {line_number}: {error}"
            ))
        })?;
        let selector = selector_from_ndjson_record(record, line_number)?;
        replay_selector(
            selector,
            replayer,
            emitter,
            pacer,
            counters,
            skip_missing,
            max_errors,
        )?;
    }

    Ok(())
}

fn selector_from_ndjson_record(
    record: NdjsonSelectorRecord,
    line_number: usize,
) -> Result<Selector, LogReplayCommandError> {
    match record.kind.as_str() {
        "sequence" => {
            let sequence = record.sequence.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'sequence' for kind=sequence"
                ))
            })?;
            Ok(Selector::Sequence(sequence))
        }
        "range" => {
            let from = record.from.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'from' for kind=range"
                ))
            })?;
            let count = record.count.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'count' for kind=range"
                ))
            })?;
            if count == 0 {
                return Err(LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: count must be > 0"
                )));
            }
            Ok(Selector::Range { from, count })
        }
        "locator" => {
            let segment_id = record.segment_id.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'segment_id' for kind=locator"
                ))
            })?;
            let segment_generation = record.segment_generation.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'segment_generation' for kind=locator"
                ))
            })?;
            let file_offset = record.file_offset.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'file_offset' for kind=locator"
                ))
            })?;
            let frame_len = record.frame_len.ok_or_else(|| {
                LogReplayCommandError::InvalidInput(format!(
                    "ndjson selector line {line_number}: missing field 'frame_len' for kind=locator"
                ))
            })?;

            Ok(Selector::Locator(validate_locator(ArchiveLocator {
                segment_id,
                segment_generation,
                file_offset,
                frame_len,
            })?))
        }
        other => Err(LogReplayCommandError::InvalidInput(format!(
            "ndjson selector line {line_number}: unsupported kind '{other}'"
        ))),
    }
}

fn replay_csv_selectors(
    mut reader: Box<dyn BufRead>,
    replayer: &iox2_log_archive_core::log_archive::ArchiveReplayer,
    emitter: &mut ReplayEmitter,
    pacer: &mut ReplayPacer,
    counters: &mut ReplayCounters,
    skip_missing: bool,
    max_errors: usize,
) -> Result<(), LogReplayCommandError> {
    let mut header_line = String::new();
    reader
        .read_line(&mut header_line)
        .map_err(|error| LogReplayCommandError::Internal(anyhow!(error)))?;

    if header_line.trim().is_empty() {
        return Err(LogReplayCommandError::InvalidInput(
            "csv selector input is empty".to_string(),
        ));
    }

    let headers: Vec<&str> = header_line
        .trim()
        .split(',')
        .map(|value| value.trim())
        .collect();
    let header_index = |name: &str| headers.iter().position(|value| *value == name);

    let required = [
        "kind",
        "sequence",
        "from",
        "count",
        "segment_id",
        "segment_generation",
        "file_offset",
        "frame_len",
    ];

    for field in required {
        if header_index(field).is_none() {
            return Err(LogReplayCommandError::InvalidInput(format!(
                "csv selector header is missing '{field}'"
            )));
        }
    }

    let mut line = String::new();
    let mut line_number = 1usize;

    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| LogReplayCommandError::Internal(anyhow!(error)))?;
        if bytes == 0 {
            break;
        }

        line_number = line_number.saturating_add(1);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let values: Vec<&str> = trimmed.split(',').map(|value| value.trim()).collect();
        if values.len() != headers.len() {
            return Err(LogReplayCommandError::InvalidInput(format!(
                "csv selector line {line_number}: expected {} columns, got {}",
                headers.len(),
                values.len()
            )));
        }

        let value = |name: &str| -> &str {
            let index = header_index(name).expect("header was validated");
            values[index]
        };

        let kind = value("kind");
        let selector = match kind {
            "sequence" => {
                let sequence = parse_required_csv_u64(value("sequence"), line_number, "sequence")?;
                Selector::Sequence(sequence)
            }
            "range" => {
                let from = parse_required_csv_u64(value("from"), line_number, "from")?;
                let count = parse_required_csv_usize(value("count"), line_number, "count")?;
                if count == 0 {
                    return Err(LogReplayCommandError::InvalidInput(format!(
                        "csv selector line {line_number}: count must be > 0"
                    )));
                }
                Selector::Range { from, count }
            }
            "locator" => {
                let segment_id =
                    parse_required_csv_u64(value("segment_id"), line_number, "segment_id")?;
                let segment_generation = parse_required_csv_u32(
                    value("segment_generation"),
                    line_number,
                    "segment_generation",
                )?;
                let file_offset =
                    parse_required_csv_u64(value("file_offset"), line_number, "file_offset")?;
                let frame_len =
                    parse_required_csv_u32(value("frame_len"), line_number, "frame_len")?;
                Selector::Locator(validate_locator(ArchiveLocator {
                    segment_id,
                    segment_generation,
                    file_offset,
                    frame_len,
                })?)
            }
            other => {
                return Err(LogReplayCommandError::InvalidInput(format!(
                    "csv selector line {line_number}: unsupported kind '{other}'"
                )));
            }
        };

        replay_selector(
            selector,
            replayer,
            emitter,
            pacer,
            counters,
            skip_missing,
            max_errors,
        )?;
    }

    Ok(())
}

fn parse_required_csv_u64(
    value: &str,
    line_number: usize,
    field: &str,
) -> Result<u64, LogReplayCommandError> {
    if value.is_empty() {
        return Err(LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: missing field '{field}'"
        )));
    }

    value.parse::<u64>().map_err(|_| {
        LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: invalid u64 in field '{field}'"
        ))
    })
}

fn parse_required_csv_u32(
    value: &str,
    line_number: usize,
    field: &str,
) -> Result<u32, LogReplayCommandError> {
    if value.is_empty() {
        return Err(LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: missing field '{field}'"
        )));
    }

    value.parse::<u32>().map_err(|_| {
        LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: invalid u32 in field '{field}'"
        ))
    })
}

fn parse_required_csv_usize(
    value: &str,
    line_number: usize,
    field: &str,
) -> Result<usize, LogReplayCommandError> {
    if value.is_empty() {
        return Err(LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: missing field '{field}'"
        )));
    }

    value.parse::<usize>().map_err(|_| {
        LogReplayCommandError::InvalidInput(format!(
            "csv selector line {line_number}: invalid usize in field '{field}'"
        ))
    })
}

fn parse_locator(value: &str) -> Result<ArchiveLocator, LogReplayCommandError> {
    let mut parts = value.split(':');
    let segment_id = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u64>()
        .map_err(|_| invalid_locator(value))?;
    let segment_generation = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u32>()
        .map_err(|_| invalid_locator(value))?;
    let file_offset = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u64>()
        .map_err(|_| invalid_locator(value))?;
    let frame_len = parts
        .next()
        .ok_or_else(|| invalid_locator(value))?
        .parse::<u32>()
        .map_err(|_| invalid_locator(value))?;
    if parts.next().is_some() {
        return Err(invalid_locator(value));
    }

    validate_locator(ArchiveLocator {
        segment_id,
        segment_generation,
        file_offset,
        frame_len,
    })
}

fn invalid_locator(value: &str) -> LogReplayCommandError {
    LogReplayCommandError::InvalidInput(format!(
        "invalid locator '{value}', expected <segment_id>:<generation>:<offset>:<frame_len>"
    ))
}

fn validate_locator(locator: ArchiveLocator) -> Result<ArchiveLocator, LogReplayCommandError> {
    if locator.segment_generation == 0 {
        return Err(LogReplayCommandError::InvalidInput(
            "invalid locator segment generation '0', expected > 0".to_string(),
        ));
    }

    if locator.frame_len == 0 {
        return Err(LogReplayCommandError::InvalidInput(
            "invalid locator frame_len '0', expected > 0".to_string(),
        ));
    }

    Ok(locator)
}

fn required_destination_service(options: &ReplayOptions) -> Result<String, LogReplayCommandError> {
    let service = options.service.clone().ok_or_else(|| {
        LogReplayCommandError::InvalidInput(
            "--service is required for --to=publish-subscribe".to_string(),
        )
    })?;

    if service.trim().is_empty() {
        return Err(LogReplayCommandError::InvalidInput(
            "--service must not be empty".to_string(),
        ));
    }

    Ok(service)
}

fn map_replay_error(error: ArchiveReplayError) -> LogReplayCommandError {
    match error {
        ArchiveReplayError::MissingCommitLog(path) | ArchiveReplayError::MissingSegment(path) => {
            LogReplayCommandError::NotAvailable(format!(
                "requested data is not available: {}",
                path.display()
            ))
        }
        ArchiveReplayError::InvalidConfiguration(message)
        | ArchiveReplayError::InvalidCommitEntry(message)
        | ArchiveReplayError::InvalidPinState(message) => {
            LogReplayCommandError::InvalidInput(message.to_string())
        }
        other => LogReplayCommandError::Internal(anyhow!("{other:?}")),
    }
}

fn map_rematerialize_error(error: ArchiveRematerializeError) -> LogReplayCommandError {
    match error {
        ArchiveRematerializeError::InvalidConfiguration(message) => {
            LogReplayCommandError::InvalidInput(message.to_string())
        }
        ArchiveRematerializeError::Replay(replay_error) => map_replay_error(replay_error),
        ArchiveRematerializeError::IncompatibleUserHeaderSize {
            expected,
            actual,
            sequence,
        } => LogReplayCommandError::InvalidInput(format!(
            "incompatible user header for sequence {sequence}: expected {expected} bytes, got {actual}"
        )),
        other => LogReplayCommandError::Internal(anyhow!("{other:?}")),
    }
}

fn selector_source_label(selector: &ReplaySelector) -> String {
    match selector {
        ReplaySelector::All => "all".to_string(),
        ReplaySelector::Sequence(_) => "sequence".to_string(),
        ReplaySelector::Range(_) => "range".to_string(),
        ReplaySelector::Locator(_) => "locator".to_string(),
        ReplaySelector::Selectors(options) => {
            let source = if options.stdin { "stdin" } else { "file" };
            let format = match options.selector_format {
                SelectorFormat::Ndjson => "ndjson",
                SelectorFormat::Csv => "csv",
            };
            format!("selectors:{source}:{format}")
        }
    }
}

fn rate_mode_label(value: ReplayRateMode) -> &'static str {
    match value {
        ReplayRateMode::Fast => "fast",
        ReplayRateMode::Recorded => "recorded",
        ReplayRateMode::Fixed => "fixed",
    }
}

fn bytes_to_hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        use core::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn effective_user_header(frame: &ReplayedFrame) -> &[u8] {
    decode_adapter_user_header(&frame.user_header)
        .map(|decoded| decoded.user_header)
        .unwrap_or(frame.user_header.as_slice())
}

fn print_output_to_stdout<T: Serialize>(
    payload: &T,
    format: Format,
) -> Result<(), LogReplayCommandError> {
    let output = format
        .as_string(payload)
        .with_context(|| "failed to serialize log-replay output")
        .map_err(LogReplayCommandError::Internal)?;
    println!("{output}");
    Ok(())
}

fn print_output_to_stderr<T: Serialize>(
    payload: &T,
    format: Format,
) -> Result<(), LogReplayCommandError> {
    let output = format
        .as_string(payload)
        .with_context(|| "failed to serialize log-replay output")
        .map_err(LogReplayCommandError::Internal)?;
    eprintln!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn frame(sequence: u64, event_time_ns: u64) -> ReplayedFrame {
        ReplayedFrame {
            commit_ordinal: sequence,
            sequence,
            event_time_ns,
            commit_time_ns: event_time_ns + 10,
            user_header: vec![0xA0, sequence as u8],
            payload: vec![sequence as u8; 4],
            frame_checksum: 0,
            locator: ArchiveLocator {
                segment_id: 1,
                segment_generation: 1,
                file_offset: sequence * 64,
                frame_len: 64,
            },
        }
    }

    fn replay_options(rate: ReplayRateMode) -> ReplayOptions {
        ReplayOptions {
            storage_path: PathBuf::from("archive"),
            metadata_log_path: Some(PathBuf::from("metadata")),
            to: ReplayDestination::Stdout,
            service: None,
            rate,
            messages_per_sec: None,
            max_recorded_gap_ms: 1,
            node_name: "test-node".to_string(),
            skip_missing: false,
            max_errors: None,
            follow: false,
            follow_poll_ms: 100,
            follow_idle_timeout_ms: None,
            selector: ReplaySelector::All,
        }
    }

    #[test]
    fn replay_errors_render_exit_codes_display_and_json_payloads() {
        let invalid = LogReplayCommandError::InvalidInput("bad selector".to_string());
        assert_eq!(invalid.exit_code(), 2);
        assert_eq!(invalid.to_string(), "bad selector");
        assert!(
            invalid
                .to_formatted_error(Format::Json)
                .contains("\"InvalidInput\"")
        );

        let unavailable = LogReplayCommandError::NotAvailable("missing".to_string());
        assert_eq!(unavailable.exit_code(), 3);
        assert_eq!(unavailable.to_string(), "missing");
        assert!(
            unavailable
                .to_formatted_error(Format::Json)
                .contains("\"NotAvailable\"")
        );

        let internal = LogReplayCommandError::Internal(anyhow!("boom"));
        assert_eq!(internal.exit_code(), 1);
        assert_eq!(internal.to_string(), "boom");
        assert!(
            internal
                .to_formatted_error(Format::Json)
                .contains("\"Internal\"")
        );
    }

    #[test]
    fn replay_pacer_and_counters_cover_rate_and_missing_edges() {
        let mut counters = ReplayCounters {
            selected: 0,
            emitted: 0,
            skipped_missing: 0,
            errors: 0,
            bytes_emitted: 0,
        };
        counters.handle_missing(0, false, 1, "none").unwrap();
        counters
            .handle_missing(2, false, 3, "partial range")
            .unwrap();
        assert_eq!(counters.errors, 2);
        assert!(
            counters
                .handle_missing(1, false, 3, "partial range")
                .unwrap_err()
                .to_string()
                .contains("reached error limit")
        );
        counters.handle_missing(4, true, 1, "skip").unwrap();
        assert_eq!(counters.skipped_missing, 4);

        let mut fast = ReplayPacer::new(&replay_options(ReplayRateMode::Fast)).unwrap();
        fast.pace(&frame(1, 100));

        let mut recorded = ReplayPacer::new(&replay_options(ReplayRateMode::Recorded)).unwrap();
        recorded.pace(&frame(1, 200));
        recorded.pace(&frame(2, 200));
        recorded.pace(&frame(3, 201));

        let mut fixed_options = replay_options(ReplayRateMode::Fixed);
        fixed_options.messages_per_sec = Some(1_000_000);
        let mut fixed = ReplayPacer::new(&fixed_options).unwrap();
        fixed.pace(&frame(1, 100));
        fixed.pace(&frame(2, 200));
    }

    #[test]
    fn follow_config_validates_live_replay_options() {
        let snapshot = replay_options(ReplayRateMode::Fast);
        assert!(
            FollowConfig::from_options(&snapshot)
                .unwrap()
                .idle_timeout
                .is_none()
        );

        let mut timeout_without_follow = replay_options(ReplayRateMode::Fast);
        timeout_without_follow.follow_idle_timeout_ms = Some(10);
        assert!(
            FollowConfig::from_options(&timeout_without_follow)
                .unwrap_err()
                .to_string()
                .contains("only valid with --follow")
        );

        let mut zero_poll = replay_options(ReplayRateMode::Fast);
        zero_poll.follow = true;
        zero_poll.follow_poll_ms = 0;
        assert!(
            FollowConfig::from_options(&zero_poll)
                .unwrap_err()
                .to_string()
                .contains("--follow-poll-ms must be > 0")
        );

        let mut live = replay_options(ReplayRateMode::Fast);
        live.follow = true;
        live.follow_poll_ms = 7;
        live.follow_idle_timeout_ms = Some(11);
        let config = FollowConfig::from_options(&live).unwrap();
        assert_eq!(config.poll_interval, Duration::from_millis(7));
        assert_eq!(config.idle_timeout, Some(Duration::from_millis(11)));
        assert_eq!(config.wait_timeout(), Duration::from_millis(11));
    }

    #[test]
    fn replay_selector_and_locator_helpers_validate_inputs() {
        let locator = parse_locator("1:2:3:4").unwrap();
        assert_eq!(locator.segment_generation, 2);
        assert_eq!(locator.frame_len, 4);
        assert!(
            parse_locator("bad")
                .unwrap_err()
                .to_string()
                .contains("invalid locator")
        );
        assert!(
            parse_locator("1:2:3:4:5")
                .unwrap_err()
                .to_string()
                .contains("invalid locator")
        );
        assert!(
            parse_locator("1:0:3:4")
                .unwrap_err()
                .to_string()
                .contains("segment generation")
        );
        assert!(
            parse_locator("1:1:3:0")
                .unwrap_err()
                .to_string()
                .contains("frame_len")
        );

        assert!(parse_required_csv_u64("", 2, "sequence").is_err());
        assert!(parse_required_csv_u64("abc", 2, "sequence").is_err());
        assert!(parse_required_csv_u32("", 2, "segment_generation").is_err());
        assert!(parse_required_csv_u32("abc", 2, "segment_generation").is_err());
        assert!(parse_required_csv_usize("", 2, "count").is_err());
        assert!(parse_required_csv_usize("abc", 2, "count").is_err());

        let sequence = selector_from_ndjson_record(
            NdjsonSelectorRecord {
                kind: "sequence".to_string(),
                sequence: Some(7),
                from: None,
                count: None,
                segment_id: None,
                segment_generation: None,
                file_offset: None,
                frame_len: None,
            },
            1,
        )
        .unwrap();
        assert!(matches!(sequence, Selector::Sequence(7)));
    }

    #[test]
    fn replay_destination_and_error_mappers_cover_non_cli_edges() {
        let mut stdout_service = replay_options(ReplayRateMode::Fast);
        stdout_service.service = Some("service".to_string());
        let error = match ReplayEmitter::from_options(&stdout_service) {
            Ok(_) => panic!("stdout emitter with service should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("--service is only valid"));

        let mut pubsub_missing = replay_options(ReplayRateMode::Fast);
        pubsub_missing.to = ReplayDestination::PublishSubscribe;
        assert!(
            required_destination_service(&pubsub_missing)
                .unwrap_err()
                .to_string()
                .contains("--service is required")
        );
        pubsub_missing.service = Some(" ".to_string());
        assert!(
            required_destination_service(&pubsub_missing)
                .unwrap_err()
                .to_string()
                .contains("--service must not be empty")
        );

        assert!(matches!(
            map_replay_error(ArchiveReplayError::MissingCommitLog(
                Path::new("commit.idxlog").into()
            )),
            LogReplayCommandError::NotAvailable(_)
        ));
        assert!(matches!(
            map_replay_error(ArchiveReplayError::InvalidConfiguration("bad config")),
            LogReplayCommandError::InvalidInput(_)
        ));
        assert!(matches!(
            map_replay_error(ArchiveReplayError::InvalidPinState("bad pin")),
            LogReplayCommandError::InvalidInput(_)
        ));
        assert!(matches!(
            map_rematerialize_error(ArchiveRematerializeError::InvalidConfiguration("bad remat")),
            LogReplayCommandError::InvalidInput(_)
        ));
        assert!(matches!(
            map_rematerialize_error(ArchiveRematerializeError::Replay(
                ArchiveReplayError::InvalidCommitEntry("bad commit")
            )),
            LogReplayCommandError::InvalidInput(_)
        ));
        assert!(
            map_rematerialize_error(ArchiveRematerializeError::IncompatibleUserHeaderSize {
                expected: 4,
                actual: 2,
                sequence: 9,
            })
            .to_string()
            .contains("incompatible user header")
        );
    }

    #[test]
    fn replay_summary_labels_and_header_accounting_are_stable() {
        assert_eq!(rate_mode_label(ReplayRateMode::Fast), "fast");
        assert_eq!(rate_mode_label(ReplayRateMode::Recorded), "recorded");
        assert_eq!(rate_mode_label(ReplayRateMode::Fixed), "fixed");
        assert_eq!(bytes_to_hex(&[0x00, 0xAB, 0xFF]), "00abff");

        assert_eq!(
            selector_source_label(&ReplaySelector::Sequence(SequenceSelector { at: 1 })),
            "sequence"
        );
        assert_eq!(
            selector_source_label(&ReplaySelector::Range(RangeSelector { from: 1, count: 2 })),
            "range"
        );
        assert_eq!(
            selector_source_label(&ReplaySelector::Locator(LocatorSelector {
                at: "1:1:0:64".to_string(),
            })),
            "locator"
        );
        assert_eq!(
            selector_source_label(&ReplaySelector::Selectors(SelectorsSelector {
                stdin: true,
                file: None,
                selector_format: SelectorFormat::Ndjson,
            })),
            "selectors:stdin:ndjson"
        );

        let frame = frame(1, 100);
        assert_eq!(effective_user_header(&frame), &[0xA0, 0x01]);
    }
}
