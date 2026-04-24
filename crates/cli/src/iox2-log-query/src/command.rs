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

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use iox2_log_archive_cli::Format;
use iox2_log_archive_core::log_archive::{
    ARCHIVE_FILE_HEADER_V1_LEN, ArchiveLocator, ArchiveMetadataIndexerBuilder,
    MetadataCommitRecord, MetadataWatermark,
};
use iox2_log_archive_sqlite::{
    SQLITE_SCHEMA_VERSION, SqliteIndexerState, SqliteMetadataSink, SqliteTimeField,
    SqliteWriterLock,
};
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::cli::{
    AlignMode, AlignWindowOptions, FillPolicy, IndexAction, IndexCatchUpOptions,
    IndexCatchUpTarget, IndexRunOptions, LocateLocatorOptions, LocateRangeOptions,
    LocateSequenceOptions, LocateWindowOptions, LogQueryAction, QueryAction, QueryEmitMode,
    StatusOptions, TimeField,
};

const DEFAULT_RANGE_QUERY_LIMIT: usize = 100_000;
const DEFAULT_ALIGN_ROW_LIMIT: usize = 1_000_000;
const SUPPORTED_SCHEMA_MIN: u32 = SQLITE_SCHEMA_VERSION;
const SUPPORTED_SCHEMA_MAX: u32 = SQLITE_SCHEMA_VERSION;

#[derive(Debug)]
pub(crate) enum LogQueryCommandError {
    InvalidInput(String),
    NotAvailable(String),
    NotIndexedYet {
        message: String,
        requested_bound: Option<String>,
        query_watermark: u64,
        last_commit_ordinal: u64,
    },
    ResourceBusy(String),
    Internal(anyhow::Error),
}

impl LogQueryCommandError {
    pub(crate) fn exit_code(&self) -> i32 {
        match self {
            Self::Internal(_) => 1,
            Self::InvalidInput(_) => 2,
            Self::NotAvailable(_) | Self::NotIndexedYet { .. } | Self::ResourceBusy(_) => 3,
        }
    }

    pub(crate) fn to_formatted_error(&self, format: Format) -> String {
        #[derive(Serialize)]
        struct ErrorPayload<'a> {
            error_code: &'a str,
            message: &'a str,
            requested_bound: Option<&'a str>,
            query_watermark: Option<u64>,
            last_commit_ordinal: Option<u64>,
        }

        let payload = match self {
            LogQueryCommandError::InvalidInput(message) => ErrorPayload {
                error_code: "InvalidInput",
                message,
                requested_bound: None,
                query_watermark: None,
                last_commit_ordinal: None,
            },
            LogQueryCommandError::NotAvailable(message) => ErrorPayload {
                error_code: "NotAvailable",
                message,
                requested_bound: None,
                query_watermark: None,
                last_commit_ordinal: None,
            },
            LogQueryCommandError::NotIndexedYet {
                message,
                requested_bound,
                query_watermark,
                last_commit_ordinal,
            } => ErrorPayload {
                error_code: "NotIndexedYet",
                message,
                requested_bound: requested_bound.as_deref(),
                query_watermark: Some(*query_watermark),
                last_commit_ordinal: Some(*last_commit_ordinal),
            },
            LogQueryCommandError::ResourceBusy(message) => ErrorPayload {
                error_code: "ResourceBusy",
                message,
                requested_bound: None,
                query_watermark: None,
                last_commit_ordinal: None,
            },
            LogQueryCommandError::Internal(error) => ErrorPayload {
                error_code: "Internal",
                message: &format!("{error:#}"),
                requested_bound: None,
                query_watermark: None,
                last_commit_ordinal: None,
            },
        };

        format
            .as_string(&payload)
            .unwrap_or_else(|_| format!("{:?}", payload.error_code))
    }
}

impl core::fmt::Display for LogQueryCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidInput(message) => write!(f, "{message}"),
            Self::NotAvailable(message) => write!(f, "{message}"),
            Self::NotIndexedYet { message, .. } => write!(f, "{message}"),
            Self::ResourceBusy(message) => write!(f, "{message}"),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}

impl std::error::Error for LogQueryCommandError {}

#[derive(Debug, Clone, Serialize)]
struct IndexResult {
    operation: &'static str,
    stream_id: String,
    metadata_log_path: String,
    db_path: String,
    target: Option<&'static str>,
    poll_interval_ms: Option<u64>,
    batch_max_records: Option<usize>,
    processed_records: u64,
    last_commit_ordinal: u64,
    last_indexed_commit_ordinal: u64,
}

#[derive(Debug, Clone, Serialize)]
struct StatusCheckpointPayload {
    roll_file: String,
    byte_offset: u64,
}

#[derive(Debug, Clone, Serialize)]
struct StatusStreamPayload {
    stream_id: String,
    log_id: String,
    last_commit_ordinal: u64,
    last_indexed_commit_ordinal: u64,
    lag_commits: u64,
    updated_at_ns: u64,
    checkpoint: StatusCheckpointPayload,
}

#[derive(Debug, Clone, Serialize)]
struct StatusAggregatePayload {
    stream_count: usize,
    aligned_horizon_commit_ordinal: u64,
}

#[derive(Debug, Clone, Serialize)]
struct StatusResult {
    operation: &'static str,
    db_path: String,
    schema_version: u32,
    streams: Vec<StatusStreamPayload>,
    aggregate: StatusAggregatePayload,
}

#[derive(Debug, Clone, Serialize)]
struct SequenceSelector {
    kind: &'static str,
    sequence: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RangeSelector {
    kind: &'static str,
    from: u64,
    count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct LocatorSelector {
    kind: &'static str,
    segment_id: u64,
    segment_generation: u32,
    file_offset: u64,
    frame_len: u32,
}

#[derive(Debug, Clone, Serialize)]
struct QuerySummary {
    operation: &'static str,
    db_path: String,
    stream_id: String,
    emit: &'static str,
    rows: usize,
}

#[derive(Debug, Clone, Serialize)]
struct AlignmentProvenance {
    stream_id: String,
    log_id: String,
    commit_ordinal: u64,
    sequence: u64,
    event_time_ns: u64,
    commit_time_ns: u64,
    segment_id: u64,
    segment_generation: u32,
    file_offset: u64,
    frame_len: u32,
    frame_checksum: u32,
}

#[derive(Debug, Clone, Serialize)]
struct AlignedStreamPayload {
    status: &'static str,
    delta_ns: Option<i64>,
    locator: Option<LocatorWithoutKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<AlignmentProvenance>,
}

#[derive(Debug, Clone, Serialize)]
struct AlignedRowPayload {
    aligned_time_ns: u64,
    time_field: &'static str,
    streams: BTreeMap<String, AlignedStreamPayload>,
}

#[derive(Debug, Clone, Serialize)]
struct AlignSelectorRow {
    aligned_time_ns: u64,
    streams: BTreeMap<String, Option<LocatorSelector>>,
}

#[derive(Debug, Clone, Serialize)]
struct AlignSummary {
    operation: &'static str,
    db_path: String,
    streams: Vec<String>,
    mode: &'static str,
    time_field: &'static str,
    rows: usize,
    aligned_horizon_commit_ordinal: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct LocatorWithoutKind {
    segment_id: u64,
    segment_generation: u32,
    file_offset: u64,
    frame_len: u32,
}

#[derive(Debug, Clone, Copy)]
struct StampedRecord {
    ts: u64,
    record: MetadataCommitRecord,
}

#[derive(Debug, Clone, Copy)]
enum MatchStatus {
    Exact,
    Nearest,
    Missing,
}

#[derive(Debug, Clone, Copy)]
struct MatchResult {
    status: MatchStatus,
    delta_ns: Option<i64>,
    record: Option<MetadataCommitRecord>,
}

pub(crate) fn log_query(
    action: LogQueryAction,
    format: Format,
) -> Result<(), LogQueryCommandError> {
    match action {
        LogQueryAction::Index { action } => match action {
            IndexAction::Run(options) => index_run(options, format),
            IndexAction::CatchUp(options) => index_catch_up(options, format),
        },
        LogQueryAction::Status(options) => status(options, format),
        LogQueryAction::Query { action } => match action {
            QueryAction::LocateSequence(options) => locate_sequence(options),
            QueryAction::LocateRange(options) => locate_range(options, format),
            QueryAction::LocateLocator(options) => locate_locator(options),
            QueryAction::LocateWindow(options) => locate_window(options, format),
            QueryAction::AlignWindow(options) => align_window(options, format),
        },
    }
}

fn index_run(options: IndexRunOptions, format: Format) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    if options.batch_max_records == 0 {
        return Err(LogQueryCommandError::InvalidInput(
            "--batch-max-records must be > 0".to_string(),
        ));
    }
    let _writer_lock = SqliteWriterLock::acquire(&options.db_path).map_err(map_sink_error)?;
    ensure_schema_compatibility(&options.db_path, options.reindex)?;

    let state_sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    if options.reindex {
        state_sink.clear_stream().map_err(map_sink_error)?;
    }

    let mut indexer = open_indexer(
        &options.metadata_log_path,
        &options.db_path,
        &options.stream_id,
        &state_sink,
    )?;
    if options.reindex {
        indexer
            .reindex()
            .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
    }

    loop {
        let processed = indexer
            .catch_up_with_limit(Some(options.batch_max_records))
            .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
        let status = indexer.status();
        persist_indexer_state(
            &state_sink,
            &options.stream_id,
            &options.metadata_log_path,
            status.watermark,
        )?;

        if processed > 0 {
            let payload = IndexResult {
                operation: "index-run",
                stream_id: options.stream_id.clone(),
                metadata_log_path: options.metadata_log_path.display().to_string(),
                db_path: options.db_path.display().to_string(),
                target: None,
                poll_interval_ms: Some(options.poll_interval_ms),
                batch_max_records: Some(options.batch_max_records),
                processed_records: processed as u64,
                last_commit_ordinal: status.watermark.last_commit_ordinal,
                last_indexed_commit_ordinal: status.watermark.last_indexed_commit_ordinal,
            };
            print_output(&payload, format)?;
        }

        thread::sleep(Duration::from_millis(options.poll_interval_ms));
    }
}

fn index_catch_up(
    options: IndexCatchUpOptions,
    format: Format,
) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    if let Some(max_records) = options.max_records {
        if max_records == 0 {
            return Err(LogQueryCommandError::InvalidInput(
                "--max-records must be > 0 when provided".to_string(),
            ));
        }
    }
    let _writer_lock = SqliteWriterLock::acquire(&options.db_path).map_err(map_sink_error)?;
    ensure_schema_compatibility(&options.db_path, options.reindex)?;

    let state_sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    if options.reindex {
        state_sink.clear_stream().map_err(map_sink_error)?;
    }

    let mut indexer = open_indexer(
        &options.metadata_log_path,
        &options.db_path,
        &options.stream_id,
        &state_sink,
    )?;
    if options.reindex {
        indexer
            .reindex()
            .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
    }

    let snapshot_current = indexer.status().watermark.last_commit_ordinal;
    let mut total_processed = 0u64;
    let final_watermark = loop {
        let processed = indexer
            .catch_up_with_limit(options.max_records)
            .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
        total_processed = total_processed.saturating_add(processed as u64);
        let final_watermark = indexer.status().watermark;
        persist_indexer_state(
            &state_sink,
            &options.stream_id,
            &options.metadata_log_path,
            final_watermark,
        )?;

        let done = match options.target {
            IndexCatchUpTarget::Current => {
                final_watermark.last_indexed_commit_ordinal >= snapshot_current || processed == 0
            }
            IndexCatchUpTarget::Latest => {
                final_watermark.last_indexed_commit_ordinal >= final_watermark.last_commit_ordinal
                    || processed == 0
            }
        };
        if done {
            break final_watermark;
        }
    };

    let payload = IndexResult {
        operation: "index-catch-up",
        stream_id: options.stream_id.clone(),
        metadata_log_path: options.metadata_log_path.display().to_string(),
        db_path: options.db_path.display().to_string(),
        target: Some(match options.target {
            IndexCatchUpTarget::Current => "current",
            IndexCatchUpTarget::Latest => "latest",
        }),
        poll_interval_ms: None,
        batch_max_records: options.max_records,
        processed_records: total_processed,
        last_commit_ordinal: final_watermark.last_commit_ordinal,
        last_indexed_commit_ordinal: final_watermark.last_indexed_commit_ordinal,
    };
    print_output(&payload, format)
}

fn status(options: StatusOptions, format: Format) -> Result<(), LogQueryCommandError> {
    ensure_schema_compatibility(&options.db_path, false)?;
    let mut states =
        SqliteMetadataSink::list_indexer_states(&options.db_path).map_err(map_sink_error)?;
    if let Some(stream_id) = options.stream_id.as_ref() {
        states.retain(|value| value.stream_id == *stream_id);
    }

    let streams: Vec<StatusStreamPayload> = states
        .iter()
        .map(|state| StatusStreamPayload {
            stream_id: state.stream_id.clone(),
            log_id: hex_log_id(state.log_id),
            last_commit_ordinal: state.last_commit_ordinal,
            last_indexed_commit_ordinal: state.last_indexed_commit_ordinal,
            lag_commits: state
                .last_commit_ordinal
                .saturating_sub(state.last_indexed_commit_ordinal),
            updated_at_ns: state.updated_at_ns,
            checkpoint: StatusCheckpointPayload {
                roll_file: state.roll_file.clone(),
                byte_offset: state.byte_offset,
            },
        })
        .collect();

    let aligned_horizon = states
        .iter()
        .map(|state| state.last_indexed_commit_ordinal)
        .min()
        .unwrap_or(0);
    let payload = StatusResult {
        operation: "status",
        db_path: options.db_path.display().to_string(),
        schema_version: SQLITE_SCHEMA_VERSION,
        streams,
        aggregate: StatusAggregatePayload {
            stream_count: states.len(),
            aligned_horizon_commit_ordinal: aligned_horizon,
        },
    };
    print_output(&payload, format)
}

fn locate_sequence(options: LocateSequenceOptions) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    ensure_schema_compatibility(&options.db_path, false)?;
    let sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    let state = load_state_or_default(&sink, &options.stream_id)?;
    let record = sink.query_by_sequence(options.at).map_err(map_sink_error)?;
    if record.is_none() {
        let indexed_upper = indexed_sequence_upper_bound(&sink)?;
        if is_sequence_not_indexed_yet(options.at, indexed_upper, &state) {
            return Err(not_indexed_error(
                format!("sequence={}", options.at),
                state.last_indexed_commit_ordinal,
                state.last_commit_ordinal,
            ));
        }
        return Err(LogQueryCommandError::NotAvailable(format!(
            "sequence {} is not available for stream '{}'",
            options.at, options.stream_id
        )));
    }

    print_ndjson(&SequenceSelector {
        kind: "sequence",
        sequence: options.at,
    })
}

fn locate_range(options: LocateRangeOptions, format: Format) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    ensure_schema_compatibility(&options.db_path, false)?;
    if options.count == 0 {
        return Err(LogQueryCommandError::InvalidInput(
            "--count must be > 0".to_string(),
        ));
    }
    if options.count > DEFAULT_RANGE_QUERY_LIMIT {
        return Err(LogQueryCommandError::InvalidInput(format!(
            "--count must be <= {}",
            DEFAULT_RANGE_QUERY_LIMIT
        )));
    }
    if options.emit == QueryEmitMode::Aligned {
        return Err(LogQueryCommandError::InvalidInput(
            "--emit aligned is not supported by locate-range".to_string(),
        ));
    }

    let sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    let state = load_state_or_default(&sink, &options.stream_id)?;
    let records = sink
        .query_range_by_sequence(options.from, options.count)
        .map_err(map_sink_error)?;
    let range_complete = range_is_complete(options.from, options.count, &records);
    if !range_complete {
        let requested_end = options
            .from
            .saturating_add(options.count as u64)
            .saturating_sub(1);
        let indexed_upper = indexed_sequence_upper_bound(&sink)?;
        if is_sequence_not_indexed_yet(requested_end, indexed_upper, &state) {
            return Err(not_indexed_error(
                format!("range={}..={}", options.from, requested_end),
                state.last_indexed_commit_ordinal,
                state.last_commit_ordinal,
            ));
        }
        return Err(LogQueryCommandError::NotAvailable(format!(
            "sequence range from {} (count={}) is not available for stream '{}'",
            options.from, options.count, options.stream_id
        )));
    }

    match options.emit {
        QueryEmitMode::Selectors => print_ndjson(&RangeSelector {
            kind: "range",
            from: options.from,
            count: options.count,
        }),
        QueryEmitMode::Summary => {
            let payload = QuerySummary {
                operation: "locate-range",
                db_path: options.db_path.display().to_string(),
                stream_id: options.stream_id,
                emit: "summary",
                rows: records.len(),
            };
            print_output(&payload, format)
        }
        QueryEmitMode::Aligned => unreachable!(),
    }
}

fn locate_locator(options: LocateLocatorOptions) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    ensure_schema_compatibility(&options.db_path, false)?;
    let locator = parse_locator(&options.at)?;
    let sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    let state = load_state_or_default(&sink, &options.stream_id)?;
    let record = sink.query_by_locator(locator).map_err(map_sink_error)?;
    let Some(_record) = record else {
        if state.last_indexed_commit_ordinal < state.last_commit_ordinal {
            return Err(not_indexed_error(
                format!(
                    "locator={}:{}:{}:{}",
                    locator.segment_id,
                    locator.segment_generation,
                    locator.file_offset,
                    locator.frame_len
                ),
                state.last_indexed_commit_ordinal,
                state.last_commit_ordinal,
            ));
        }
        return Err(LogQueryCommandError::NotAvailable(format!(
            "locator {} is not available for stream '{}'",
            options.at, options.stream_id
        )));
    };

    print_ndjson(&locator_selector(locator))
}

fn locate_window(options: LocateWindowOptions, format: Format) -> Result<(), LogQueryCommandError> {
    validate_stream_id(&options.stream_id)?;
    ensure_schema_compatibility(&options.db_path, false)?;
    if options.emit == QueryEmitMode::Aligned {
        return Err(LogQueryCommandError::InvalidInput(
            "--emit aligned is not supported by locate-window".to_string(),
        ));
    }

    let (start_ns, end_ns) = resolve_time_window(
        options.start_ns,
        options.end_ns,
        options.start_utc.as_deref(),
        options.end_utc.as_deref(),
    )?;
    let sink = SqliteMetadataSink::open_for_stream(&options.db_path, &options.stream_id)
        .map_err(map_sink_error)?;
    let state = load_state_or_default(&sink, &options.stream_id)?;
    let time_field = sqlite_time_field(options.time_field);
    let max_indexed_time = sink.max_timestamp_ns(time_field).map_err(map_sink_error)?;
    if end_ns > max_indexed_time.unwrap_or(0)
        && state.last_indexed_commit_ordinal < state.last_commit_ordinal
    {
        return Err(not_indexed_error(
            format!("window_end_ns={end_ns}"),
            state.last_indexed_commit_ordinal,
            state.last_commit_ordinal,
        ));
    }

    let records = sink
        .query_window(start_ns, end_ns, time_field, DEFAULT_RANGE_QUERY_LIMIT)
        .map_err(map_sink_error)?;

    match options.emit {
        QueryEmitMode::Selectors => {
            for record in &records {
                print_ndjson(&locator_selector(record.locator))?;
            }
            Ok(())
        }
        QueryEmitMode::Summary => {
            let payload = QuerySummary {
                operation: "locate-window",
                db_path: options.db_path.display().to_string(),
                stream_id: options.stream_id,
                emit: "summary",
                rows: records.len(),
            };
            print_output(&payload, format)
        }
        QueryEmitMode::Aligned => unreachable!(),
    }
}

fn align_window(options: AlignWindowOptions, format: Format) -> Result<(), LogQueryCommandError> {
    ensure_schema_compatibility(&options.db_path, false)?;
    let stream_ids = normalize_streams(&options.streams)?;
    let (start_ns, end_ns) = resolve_time_window(
        options.start_ns,
        options.end_ns,
        options.start_utc.as_deref(),
        options.end_utc.as_deref(),
    )?;

    let align_limit = options.limit.unwrap_or(DEFAULT_ALIGN_ROW_LIMIT);
    if align_limit == 0 || align_limit > DEFAULT_ALIGN_ROW_LIMIT {
        return Err(LogQueryCommandError::InvalidInput(format!(
            "--limit must be in [1, {}]",
            DEFAULT_ALIGN_ROW_LIMIT
        )));
    }

    if options.mode == AlignMode::Anchor {
        let anchor_stream = options.anchor_stream.as_ref().ok_or_else(|| {
            LogQueryCommandError::InvalidInput(
                "--anchor-stream is required with --mode anchor".to_string(),
            )
        })?;
        if !stream_ids.iter().any(|value| value == anchor_stream) {
            return Err(LogQueryCommandError::InvalidInput(
                "--anchor-stream must be part of --streams".to_string(),
            ));
        }
    }
    if options.mode == AlignMode::Grid {
        let step_ns = options.step_ns.ok_or_else(|| {
            LogQueryCommandError::InvalidInput("--step-ns is required with --mode grid".to_string())
        })?;
        if step_ns == 0 {
            return Err(LogQueryCommandError::InvalidInput(
                "--step-ns must be > 0".to_string(),
            ));
        }
    }

    let time_field = sqlite_time_field(options.time_field);
    let mut stream_data = BTreeMap::<String, Vec<StampedRecord>>::new();
    let mut aligned_horizon = u64::MAX;
    let mut last_commit_ordinal = 0u64;

    for stream_id in &stream_ids {
        let sink = SqliteMetadataSink::open_for_stream(&options.db_path, stream_id)
            .map_err(map_sink_error)?;
        let state = load_state_or_default(&sink, stream_id)?;
        aligned_horizon = aligned_horizon.min(state.last_indexed_commit_ordinal);
        last_commit_ordinal = last_commit_ordinal.max(state.last_commit_ordinal);
        let max_indexed_time = sink.max_timestamp_ns(time_field).map_err(map_sink_error)?;
        if end_ns > max_indexed_time.unwrap_or(0)
            && state.last_indexed_commit_ordinal < state.last_commit_ordinal
        {
            return Err(not_indexed_error(
                format!("stream='{}',window_end_ns={end_ns}", stream_id),
                state.last_indexed_commit_ordinal,
                state.last_commit_ordinal,
            ));
        }

        let records = sink
            .query_window(start_ns, end_ns, time_field, align_limit)
            .map_err(map_sink_error)?;
        let stamped = records
            .into_iter()
            .map(|record| StampedRecord {
                ts: record_timestamp(record, options.time_field),
                record,
            })
            .collect::<Vec<_>>();
        stream_data.insert(stream_id.clone(), stamped);
    }

    let timeline = build_timeline(TimelineRequest {
        stream_data: &stream_data,
        streams: &stream_ids,
        mode: options.mode,
        anchor_stream: options.anchor_stream.as_deref(),
        step_ns: options.step_ns,
        start_ns,
        end_ns,
        limit: align_limit,
    })?;

    let align_context = AlignBuildContext {
        streams: &stream_ids,
        stream_data: &stream_data,
        fill_policy: options.fill_policy,
        max_skew_ns: options.max_skew_ns,
        require_all_streams: options.require_all_streams,
        time_field: options.time_field,
    };

    let mut emitted = 0usize;
    match options.emit {
        QueryEmitMode::Selectors => {
            for aligned_time_ns in timeline {
                let row = build_align_selector_row(aligned_time_ns, &align_context);
                if let Some(row) = row {
                    print_ndjson(&row)?;
                    emitted = emitted.saturating_add(1);
                }
            }
            Ok(())
        }
        QueryEmitMode::Aligned => {
            for aligned_time_ns in timeline {
                let row =
                    build_aligned_row(aligned_time_ns, &align_context, options.include_provenance);
                if let Some(row) = row {
                    print_ndjson(&row)?;
                    emitted = emitted.saturating_add(1);
                }
            }
            Ok(())
        }
        QueryEmitMode::Summary => {
            for aligned_time_ns in timeline {
                let row = build_aligned_row(aligned_time_ns, &align_context, false);
                if row.is_some() {
                    emitted = emitted.saturating_add(1);
                }
            }
            let payload = AlignSummary {
                operation: "align-window",
                db_path: options.db_path.display().to_string(),
                streams: stream_ids,
                mode: match options.mode {
                    AlignMode::Anchor => "anchor",
                    AlignMode::Grid => "grid",
                },
                time_field: match options.time_field {
                    TimeField::Event => "event",
                    TimeField::Commit => "commit",
                },
                rows: emitted,
                aligned_horizon_commit_ordinal: aligned_horizon.min(last_commit_ordinal),
            };
            print_output(&payload, format)
        }
    }
}

fn open_indexer(
    metadata_log_path: &Path,
    db_path: &Path,
    stream_id: &str,
    sink: &SqliteMetadataSink,
) -> Result<iox2_log_archive_core::log_archive::ArchiveMetadataIndexer, LogQueryCommandError> {
    let watermark_path = stream_watermark_path(db_path, stream_id)?;
    ArchiveMetadataIndexerBuilder::new(metadata_log_path)
        .metadata_log_path(metadata_log_path)
        .watermark_path(&watermark_path)
        .enable_core_locator_index(false)
        .sink(Box::new(sink.clone()))
        .open()
        .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))
}

fn persist_indexer_state(
    sink: &SqliteMetadataSink,
    stream_id: &str,
    metadata_log_path: &Path,
    watermark: MetadataWatermark,
) -> Result<(), LogQueryCommandError> {
    let previous = sink.load_indexer_state().map_err(map_sink_error)?;
    let log_id = if let Some(record) = sink.latest_record().map_err(map_sink_error)? {
        record.log_id
    } else if let Some(state) = previous.as_ref() {
        state.log_id
    } else {
        [0u8; 16]
    };

    let checkpoint = resolve_checkpoint(metadata_log_path, watermark.last_indexed_commit_ordinal)?;
    let row = SqliteIndexerState {
        stream_id: stream_id.to_string(),
        log_id,
        last_commit_ordinal: watermark.last_commit_ordinal,
        last_indexed_commit_ordinal: watermark.last_indexed_commit_ordinal,
        roll_file: checkpoint.roll_file,
        byte_offset: checkpoint.byte_offset,
        updated_at_ns: now_ns(),
        schema_version: SQLITE_SCHEMA_VERSION,
    };
    sink.upsert_indexer_state(&row).map_err(map_sink_error)
}

fn load_state_or_default(
    sink: &SqliteMetadataSink,
    stream_id: &str,
) -> Result<SqliteIndexerState, LogQueryCommandError> {
    Ok(sink
        .load_indexer_state()
        .map_err(map_sink_error)?
        .unwrap_or(SqliteIndexerState {
            stream_id: stream_id.to_string(),
            log_id: [0u8; 16],
            last_commit_ordinal: 0,
            last_indexed_commit_ordinal: 0,
            roll_file: "commit.idxlog".to_string(),
            byte_offset: 0,
            updated_at_ns: 0,
            schema_version: SQLITE_SCHEMA_VERSION,
        }))
}

fn normalize_streams(streams: &[String]) -> Result<Vec<String>, LogQueryCommandError> {
    if streams.is_empty() {
        return Err(LogQueryCommandError::InvalidInput(
            "--streams must contain at least one stream id".to_string(),
        ));
    }

    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for stream in streams {
        validate_stream_id(stream)?;
        if seen.insert(stream.clone()) {
            result.push(stream.clone());
        }
    }
    if result.is_empty() {
        return Err(LogQueryCommandError::InvalidInput(
            "--streams must contain at least one unique stream id".to_string(),
        ));
    }
    Ok(result)
}

struct TimelineRequest<'a> {
    stream_data: &'a BTreeMap<String, Vec<StampedRecord>>,
    streams: &'a [String],
    mode: AlignMode,
    anchor_stream: Option<&'a str>,
    step_ns: Option<u64>,
    start_ns: u64,
    end_ns: u64,
    limit: usize,
}

struct AlignBuildContext<'a> {
    streams: &'a [String],
    stream_data: &'a BTreeMap<String, Vec<StampedRecord>>,
    fill_policy: FillPolicy,
    max_skew_ns: u64,
    require_all_streams: bool,
    time_field: TimeField,
}

fn build_timeline(request: TimelineRequest<'_>) -> Result<Vec<u64>, LogQueryCommandError> {
    match request.mode {
        AlignMode::Anchor => {
            let anchor = request.anchor_stream.ok_or_else(|| {
                LogQueryCommandError::InvalidInput(
                    "--anchor-stream is required with --mode anchor".to_string(),
                )
            })?;
            if !request.streams.iter().any(|value| value == anchor) {
                return Err(LogQueryCommandError::InvalidInput(
                    "--anchor-stream must be part of --streams".to_string(),
                ));
            }
            let anchor_rows = request.stream_data.get(anchor).ok_or_else(|| {
                LogQueryCommandError::InvalidInput("anchor stream has no indexed data".to_string())
            })?;
            if anchor_rows.len() > request.limit {
                return Err(LogQueryCommandError::InvalidInput(format!(
                    "aligned row cap exceeded ({} > {})",
                    anchor_rows.len(),
                    request.limit
                )));
            }
            Ok(anchor_rows.iter().map(|value| value.ts).collect())
        }
        AlignMode::Grid => {
            let step = request.step_ns.ok_or_else(|| {
                LogQueryCommandError::InvalidInput(
                    "--step-ns is required with --mode grid".to_string(),
                )
            })?;
            if step == 0 {
                return Err(LogQueryCommandError::InvalidInput(
                    "--step-ns must be > 0".to_string(),
                ));
            }

            let mut timeline = Vec::new();
            let mut cursor = request.start_ns;
            loop {
                if timeline.len() >= request.limit {
                    return Err(LogQueryCommandError::InvalidInput(format!(
                        "aligned row cap exceeded (>{})",
                        request.limit
                    )));
                }
                timeline.push(cursor);
                if cursor >= request.end_ns {
                    break;
                }
                let next = cursor.saturating_add(step);
                if next <= cursor {
                    break;
                }
                cursor = next.min(request.end_ns);
            }
            Ok(timeline)
        }
    }
}

fn build_align_selector_row(
    aligned_time_ns: u64,
    context: &AlignBuildContext<'_>,
) -> Option<AlignSelectorRow> {
    let mut row = AlignSelectorRow {
        aligned_time_ns,
        streams: BTreeMap::new(),
    };

    let mut missing = false;
    for stream in context.streams {
        let records = context
            .stream_data
            .get(stream)
            .map(|value| value.as_slice())
            .unwrap_or(&[]);
        let matched = find_match(
            records,
            aligned_time_ns,
            context.fill_policy,
            context.max_skew_ns,
        );
        if matches!(matched.status, MatchStatus::Missing) {
            missing = true;
            row.streams.insert(stream.clone(), None);
        } else {
            let locator = matched
                .record
                .map(|record| locator_selector(record.locator));
            row.streams.insert(stream.clone(), locator);
        }
    }

    if (context.require_all_streams || context.fill_policy == FillPolicy::Drop) && missing {
        return None;
    }

    Some(row)
}

fn build_aligned_row(
    aligned_time_ns: u64,
    context: &AlignBuildContext<'_>,
    include_provenance: bool,
) -> Option<AlignedRowPayload> {
    let mut row = AlignedRowPayload {
        aligned_time_ns,
        time_field: match context.time_field {
            TimeField::Event => "event",
            TimeField::Commit => "commit",
        },
        streams: BTreeMap::new(),
    };

    let mut missing = false;
    for stream in context.streams {
        let records = context
            .stream_data
            .get(stream)
            .map(|value| value.as_slice())
            .unwrap_or(&[]);
        let matched = find_match(
            records,
            aligned_time_ns,
            context.fill_policy,
            context.max_skew_ns,
        );
        let payload = match matched.status {
            MatchStatus::Exact | MatchStatus::Nearest => {
                let record = matched
                    .record
                    .expect("record must exist for non-missing match");
                AlignedStreamPayload {
                    status: match matched.status {
                        MatchStatus::Exact => "exact",
                        MatchStatus::Nearest => "nearest",
                        MatchStatus::Missing => "missing",
                    },
                    delta_ns: matched.delta_ns,
                    locator: Some(LocatorWithoutKind {
                        segment_id: record.locator.segment_id,
                        segment_generation: record.locator.segment_generation,
                        file_offset: record.locator.file_offset,
                        frame_len: record.locator.frame_len,
                    }),
                    provenance: if include_provenance {
                        Some(AlignmentProvenance {
                            stream_id: stream.clone(),
                            log_id: hex_log_id(record.log_id),
                            commit_ordinal: record.commit_ordinal,
                            sequence: record.sequence,
                            event_time_ns: record.event_time_ns,
                            commit_time_ns: record.commit_time_ns,
                            segment_id: record.locator.segment_id,
                            segment_generation: record.locator.segment_generation,
                            file_offset: record.locator.file_offset,
                            frame_len: record.locator.frame_len,
                            frame_checksum: record.frame_checksum,
                        })
                    } else {
                        None
                    },
                }
            }
            MatchStatus::Missing => {
                missing = true;
                AlignedStreamPayload {
                    status: "missing",
                    delta_ns: None,
                    locator: None,
                    provenance: None,
                }
            }
        };
        row.streams.insert(stream.clone(), payload);
    }

    if (context.require_all_streams || context.fill_policy == FillPolicy::Drop) && missing {
        return None;
    }

    Some(row)
}

fn find_match(
    records: &[StampedRecord],
    aligned_time_ns: u64,
    fill_policy: FillPolicy,
    max_skew_ns: u64,
) -> MatchResult {
    if records.is_empty() {
        return MatchResult {
            status: MatchStatus::Missing,
            delta_ns: None,
            record: None,
        };
    }

    let idx = records
        .binary_search_by_key(&aligned_time_ns, |value| value.ts)
        .ok();
    if let Some(index) = idx {
        return MatchResult {
            status: MatchStatus::Exact,
            delta_ns: Some(0),
            record: Some(records[index].record),
        };
    }

    if fill_policy != FillPolicy::Nearest {
        return MatchResult {
            status: MatchStatus::Missing,
            delta_ns: None,
            record: None,
        };
    }

    let insertion = match records.binary_search_by_key(&aligned_time_ns, |value| value.ts) {
        Ok(index) => index,
        Err(index) => index,
    };
    let prev = insertion.checked_sub(1).map(|index| records[index]);
    let next = if insertion < records.len() {
        Some(records[insertion])
    } else {
        None
    };

    let choose = match (prev, next) {
        (Some(left), Some(right)) => {
            let left_delta = aligned_time_ns.saturating_sub(left.ts);
            let right_delta = right.ts.saturating_sub(aligned_time_ns);
            if left_delta <= right_delta {
                Some((left, -(left_delta as i64), left_delta))
            } else {
                Some((right, right_delta as i64, right_delta))
            }
        }
        (Some(left), None) => {
            let delta = aligned_time_ns.saturating_sub(left.ts);
            Some((left, -(delta as i64), delta))
        }
        (None, Some(right)) => {
            let delta = right.ts.saturating_sub(aligned_time_ns);
            Some((right, delta as i64, delta))
        }
        (None, None) => None,
    };

    if let Some((candidate, signed_delta, abs_delta)) = choose {
        if abs_delta <= max_skew_ns {
            return MatchResult {
                status: MatchStatus::Nearest,
                delta_ns: Some(signed_delta),
                record: Some(candidate.record),
            };
        }
    }

    MatchResult {
        status: MatchStatus::Missing,
        delta_ns: None,
        record: None,
    }
}

fn resolve_time_window(
    start_ns: Option<u64>,
    end_ns: Option<u64>,
    start_utc: Option<&str>,
    end_utc: Option<&str>,
) -> Result<(u64, u64), LogQueryCommandError> {
    let (start, end) = match (start_ns, end_ns, start_utc, end_utc) {
        (Some(start), Some(end), None, None) => (start, end),
        (None, None, Some(start), Some(end)) => (parse_utc_ns(start)?, parse_utc_ns(end)?),
        _ => {
            return Err(LogQueryCommandError::InvalidInput(
                "provide either (--start-ns and --end-ns) or (--start-utc and --end-utc)"
                    .to_string(),
            ));
        }
    };

    if start > end {
        return Err(LogQueryCommandError::InvalidInput(
            "time window start must be <= end".to_string(),
        ));
    }
    Ok((start, end))
}

fn parse_utc_ns(value: &str) -> Result<u64, LogQueryCommandError> {
    let dt = OffsetDateTime::parse(value, &Rfc3339).map_err(|err| {
        LogQueryCommandError::InvalidInput(format!("invalid RFC3339 timestamp '{value}': {err}"))
    })?;
    let nanos = dt.unix_timestamp_nanos();
    if nanos < 0 {
        return Err(LogQueryCommandError::InvalidInput(format!(
            "timestamp '{value}' resolves to a negative epoch value"
        )));
    }
    if nanos > u64::MAX as i128 {
        return Err(LogQueryCommandError::InvalidInput(format!(
            "timestamp '{value}' exceeds u64 nanosecond range"
        )));
    }
    Ok(nanos as u64)
}

fn sqlite_time_field(value: TimeField) -> SqliteTimeField {
    match value {
        TimeField::Event => SqliteTimeField::Event,
        TimeField::Commit => SqliteTimeField::Commit,
    }
}

fn record_timestamp(value: MetadataCommitRecord, field: TimeField) -> u64 {
    match field {
        TimeField::Event => value.event_time_ns,
        TimeField::Commit => value.commit_time_ns,
    }
}

fn locator_selector(locator: ArchiveLocator) -> LocatorSelector {
    LocatorSelector {
        kind: "locator",
        segment_id: locator.segment_id,
        segment_generation: locator.segment_generation,
        file_offset: locator.file_offset,
        frame_len: locator.frame_len,
    }
}

fn parse_locator(value: &str) -> Result<ArchiveLocator, LogQueryCommandError> {
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
    if segment_generation == 0 {
        return Err(LogQueryCommandError::InvalidInput(
            "invalid locator segment generation '0', expected > 0".to_string(),
        ));
    }
    Ok(ArchiveLocator {
        segment_id,
        segment_generation,
        file_offset,
        frame_len,
    })
}

fn invalid_locator(value: &str) -> LogQueryCommandError {
    LogQueryCommandError::InvalidInput(format!(
        "invalid locator '{value}', expected <segment_id>:<generation>:<offset>:<frame_len>"
    ))
}

#[derive(Debug, Clone)]
struct CommitCheckpoint {
    roll_file: String,
    byte_offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct CommitLogScan {
    entries: u64,
    target_offset: Option<u64>,
}

const COMMIT_ENTRY_MAGIC: [u8; 4] = *b"CID1";
const COMMIT_ENTRY_PREFIX_LEN: u64 = 8;

fn indexed_sequence_upper_bound(
    sink: &SqliteMetadataSink,
) -> Result<Option<u64>, LogQueryCommandError> {
    Ok(sink
        .latest_record()
        .map_err(map_sink_error)?
        .map(|record| record.sequence))
}

fn is_sequence_not_indexed_yet(
    requested_sequence: u64,
    indexed_upper_bound: Option<u64>,
    state: &SqliteIndexerState,
) -> bool {
    if state.last_indexed_commit_ordinal >= state.last_commit_ordinal {
        return false;
    }

    match indexed_upper_bound {
        Some(upper) => requested_sequence > upper,
        None => requested_sequence > 0,
    }
}

fn range_is_complete(from: u64, count: usize, records: &[MetadataCommitRecord]) -> bool {
    if records.len() != count {
        return false;
    }

    let mut expected = from;
    for record in records {
        if record.sequence != expected {
            return false;
        }
        expected = expected.saturating_add(1);
    }
    true
}

fn resolve_checkpoint(
    metadata_log_path: &Path,
    last_indexed_commit_ordinal: u64,
) -> Result<CommitCheckpoint, LogQueryCommandError> {
    let commit_logs = list_commit_log_files(metadata_log_path)?;
    let last_path = commit_logs.last().ok_or_else(|| {
        LogQueryCommandError::Internal(anyhow!(
            "no commit log files found in '{}'",
            metadata_log_path.display()
        ))
    })?;

    if last_indexed_commit_ordinal == 0 {
        return Ok(CommitCheckpoint {
            roll_file: file_name_string(last_path)?,
            byte_offset: ARCHIVE_FILE_HEADER_V1_LEN as u64,
        });
    }

    let mut remaining = last_indexed_commit_ordinal;

    for path in &commit_logs {
        let scan = scan_commit_log(path, remaining)?;
        if let Some(offset) = scan.target_offset {
            return Ok(CommitCheckpoint {
                roll_file: file_name_string(path)?,
                byte_offset: offset,
            });
        }
        remaining = remaining.saturating_sub(scan.entries);
    }

    Err(LogQueryCommandError::Internal(anyhow!(
        "unable to resolve checkpoint for commit ordinal {} under '{}'; visible ordinal range is smaller",
        last_indexed_commit_ordinal,
        metadata_log_path.display()
    )))
}

fn list_commit_log_files(metadata_log_path: &Path) -> Result<Vec<PathBuf>, LogQueryCommandError> {
    let entries = fs::read_dir(metadata_log_path).map_err(|err| {
        LogQueryCommandError::Internal(anyhow!(
            "list metadata path '{}' failed: {err}",
            metadata_log_path.display()
        ))
    })?;

    let mut rolled = Vec::<(u64, PathBuf)>::new();
    let mut active: Option<PathBuf> = None;
    for entry in entries {
        let entry = entry.map_err(|err| {
            LogQueryCommandError::Internal(anyhow!(
                "read metadata directory entry in '{}' failed: {err}",
                metadata_log_path.display()
            ))
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name == "commit.idxlog" {
            active = Some(path);
            continue;
        }
        if let Some(index) = parse_rolled_commit_log_index(name) {
            rolled.push((index, path));
        }
    }
    rolled.sort_by_key(|(index, _)| *index);

    let mut result = rolled.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
    if let Some(active_path) = active {
        result.push(active_path);
    }
    if result.is_empty() {
        let fallback = metadata_log_path.join("commit.idxlog");
        if fallback.exists() {
            result.push(fallback);
        }
    }
    Ok(result)
}

fn parse_rolled_commit_log_index(file_name: &str) -> Option<u64> {
    let value = file_name.strip_prefix("commit-")?.strip_suffix(".idxlog")?;
    if value.is_empty() {
        return None;
    }
    value.parse().ok()
}

fn file_name_string(path: &Path) -> Result<String, LogQueryCommandError> {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(ToString::to_string)
        .ok_or_else(|| {
            LogQueryCommandError::Internal(anyhow!(
                "path '{}' has no utf8 file name",
                path.display()
            ))
        })
}

fn scan_commit_log(
    path: &Path,
    target_ordinal_in_file: u64,
) -> Result<CommitLogScan, LogQueryCommandError> {
    let mut file = File::open(path).map_err(|err| {
        LogQueryCommandError::Internal(anyhow!(
            "open commit log '{}' failed: {err}",
            path.display()
        ))
    })?;
    let file_len = file
        .metadata()
        .map_err(|err| {
            LogQueryCommandError::Internal(anyhow!(
                "read commit log metadata '{}' failed: {err}",
                path.display()
            ))
        })?
        .len();
    if file_len < ARCHIVE_FILE_HEADER_V1_LEN as u64 {
        return Err(LogQueryCommandError::Internal(anyhow!(
            "commit log '{}' is shorter than archive header",
            path.display()
        )));
    }

    let mut offset = ARCHIVE_FILE_HEADER_V1_LEN as u64;
    let mut entries = 0u64;
    while offset.saturating_add(COMMIT_ENTRY_PREFIX_LEN) <= file_len {
        file.seek(SeekFrom::Start(offset)).map_err(|err| {
            LogQueryCommandError::Internal(anyhow!(
                "seek commit log '{}' to {} failed: {err}",
                path.display(),
                offset
            ))
        })?;
        let mut prefix = [0u8; COMMIT_ENTRY_PREFIX_LEN as usize];
        file.read_exact(&mut prefix).map_err(|err| {
            LogQueryCommandError::Internal(anyhow!(
                "read commit log prefix '{}' at {} failed: {err}",
                path.display(),
                offset
            ))
        })?;

        if prefix == [0u8; COMMIT_ENTRY_PREFIX_LEN as usize] {
            let remaining = file_len.saturating_sub(offset + COMMIT_ENTRY_PREFIX_LEN);
            if remaining > 0 {
                let mut tail = vec![0u8; remaining as usize];
                file.read_exact(&mut tail).map_err(|err| {
                    LogQueryCommandError::Internal(anyhow!(
                        "read commit log zero tail '{}' failed: {err}",
                        path.display()
                    ))
                })?;
                if tail.iter().any(|byte| *byte != 0) {
                    return Err(LogQueryCommandError::Internal(anyhow!(
                        "commit log '{}' contains non-zero bytes after zero-tail marker",
                        path.display()
                    )));
                }
            }
            break;
        }

        if prefix[..4] != COMMIT_ENTRY_MAGIC {
            return Err(LogQueryCommandError::Internal(anyhow!(
                "commit log '{}' contains invalid entry magic at offset {}",
                path.display(),
                offset
            )));
        }
        let entry_len = u16::from_le_bytes([prefix[4], prefix[5]]) as u64;
        if entry_len < COMMIT_ENTRY_PREFIX_LEN {
            return Err(LogQueryCommandError::Internal(anyhow!(
                "commit log '{}' entry length {} is smaller than prefix length",
                path.display(),
                entry_len
            )));
        }
        if offset.saturating_add(entry_len) > file_len {
            return Err(LogQueryCommandError::Internal(anyhow!(
                "commit log '{}' entry exceeds file bounds at offset {}",
                path.display(),
                offset
            )));
        }

        entries = entries.saturating_add(1);
        offset = offset.saturating_add(entry_len);
        if entries == target_ordinal_in_file {
            return Ok(CommitLogScan {
                entries,
                target_offset: Some(offset),
            });
        }
    }

    Ok(CommitLogScan {
        entries,
        target_offset: None,
    })
}

fn validate_stream_id(stream_id: &str) -> Result<(), LogQueryCommandError> {
    if stream_id.trim().is_empty() {
        return Err(LogQueryCommandError::InvalidInput(
            "--stream-id must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn stream_watermark_path(db_path: &Path, stream_id: &str) -> Result<PathBuf, LogQueryCommandError> {
    let parent = db_path.parent().ok_or_else(|| {
        LogQueryCommandError::InvalidInput("--db-path must have a parent directory".to_string())
    })?;
    let stem = db_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("index");
    let sanitized = stream_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    Ok(parent.join(format!("{stem}.{sanitized}.watermark")))
}

fn not_indexed_error(
    requested_bound: String,
    query_watermark: u64,
    last_commit_ordinal: u64,
) -> LogQueryCommandError {
    LogQueryCommandError::NotIndexedYet {
        message: format!(
            "requested bound '{requested_bound}' exceeds query watermark {query_watermark} (last_commit_ordinal={last_commit_ordinal})"
        ),
        requested_bound: Some(requested_bound),
        query_watermark,
        last_commit_ordinal,
    }
}

fn map_sink_error(
    error: iox2_log_archive_core::log_archive::ArchiveMetadataSinkError,
) -> LogQueryCommandError {
    if error.details.contains("database is locked")
        || error.details.contains("database is busy")
        || error.details.contains("writer lock is already held")
    {
        return LogQueryCommandError::ResourceBusy(error.details);
    }
    LogQueryCommandError::Internal(anyhow!(error))
}

fn print_output<T: Serialize>(value: &T, format: Format) -> Result<(), LogQueryCommandError> {
    let serialized = format
        .as_string(value)
        .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
    println!("{serialized}");
    Ok(())
}

fn print_ndjson<T: Serialize>(value: &T) -> Result<(), LogQueryCommandError> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    serde_json::to_writer(&mut lock, value)
        .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
    lock.write_all(b"\n")
        .map_err(|err| LogQueryCommandError::Internal(anyhow!(err)))?;
    Ok(())
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as u64)
        .unwrap_or(0)
}

fn hex_log_id(log_id: [u8; 16]) -> String {
    let mut result = String::with_capacity(32);
    for byte in log_id {
        use core::fmt::Write;
        let _ = write!(result, "{byte:02x}");
    }
    result
}

fn ensure_schema_compatibility(
    db_path: &Path,
    allow_reindex_reset: bool,
) -> Result<(), LogQueryCommandError> {
    if !db_path.exists() {
        return Ok(());
    }

    let versions = match SqliteMetadataSink::list_schema_versions(db_path) {
        Ok(value) => value,
        Err(error) => {
            if error.details.contains("schema_migrations table is missing") {
                if allow_reindex_reset {
                    reset_index_db(db_path)?;
                    return Ok(());
                }
                return Err(LogQueryCommandError::InvalidInput(format!(
                    "index DB at '{}' has no schema migration metadata; migration is required or rerun with --reindex",
                    db_path.display()
                )));
            }
            return Err(map_sink_error(error));
        }
    };

    if versions.is_empty() {
        return Ok(());
    }

    let min_version = *versions
        .first()
        .expect("schema versions checked for emptiness");
    if min_version < SUPPORTED_SCHEMA_MIN {
        if allow_reindex_reset {
            reset_index_db(db_path)?;
            return Ok(());
        }
        return Err(LogQueryCommandError::InvalidInput(format!(
            "index DB schema version {} is below supported minimum {}; migration required or rerun with --reindex",
            min_version, SUPPORTED_SCHEMA_MIN
        )));
    }

    let max_version = *versions.last().expect("schema versions are not empty");
    if max_version > SUPPORTED_SCHEMA_MAX {
        if allow_reindex_reset {
            reset_index_db(db_path)?;
            return Ok(());
        }
        return Err(LogQueryCommandError::InvalidInput(format!(
            "index DB schema version {} is above supported maximum {}; upgrade iox2-log-query or rerun with --reindex to rebuild this DB",
            max_version, SUPPORTED_SCHEMA_MAX
        )));
    }

    Ok(())
}

fn reset_index_db(db_path: &Path) -> Result<(), LogQueryCommandError> {
    for path in [
        db_path.to_path_buf(),
        sqlite_sidecar_path(db_path, "-wal"),
        sqlite_sidecar_path(db_path, "-shm"),
    ] {
        if path.exists() {
            fs::remove_file(&path).map_err(|err| {
                LogQueryCommandError::Internal(anyhow!(
                    "failed to remove stale index DB artifact '{}' during --reindex: {err}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = db_path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}
