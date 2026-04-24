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

use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;

use iox2_log_archive_cli::Format;
use iox2_log_archive_cli::HelpOptions;
use iox2_log_archive_cli::help_template;

#[derive(Parser)]
#[command(
    name = "iox2-log-query",
    bin_name = "iox2-log-query",
    about = "Index and query log-archive metadata for replay/rematerialization",
    long_about = None,
    version = env!("CARGO_PKG_VERSION"),
    disable_help_subcommand = true,
    arg_required_else_help = false,
    help_template = help_template(HelpOptions::PrintCommandSection),
)]
pub struct Cli {
    #[clap(subcommand)]
    pub action: Option<LogQueryAction>,

    #[clap(long, short = 'f', value_enum, global = true, default_value_t = Format::Ron)]
    pub format: Format,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum IndexCatchUpTarget {
    Current,
    Latest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum TimeField {
    Event,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum QueryEmitMode {
    Selectors,
    Aligned,
    Summary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum AlignMode {
    Anchor,
    Grid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum FillPolicy {
    Drop,
    Null,
    Nearest,
}

#[derive(Clone, Debug, Args)]
pub struct IndexRunOptions {
    #[clap(long)]
    pub stream_id: String,

    #[clap(long)]
    pub metadata_log_path: std::path::PathBuf,

    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long, default_value = "100")]
    pub poll_interval_ms: u64,

    #[clap(long, default_value = "4096")]
    pub batch_max_records: usize,

    #[clap(long, default_value_t = false)]
    pub reindex: bool,
}

#[derive(Clone, Debug, Args)]
pub struct IndexCatchUpOptions {
    #[clap(long)]
    pub stream_id: String,

    #[clap(long)]
    pub metadata_log_path: std::path::PathBuf,

    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub max_records: Option<usize>,

    #[clap(long, value_enum, default_value_t = IndexCatchUpTarget::Current)]
    pub target: IndexCatchUpTarget,

    #[clap(long, default_value_t = false)]
    pub reindex: bool,
}

#[derive(Clone, Debug, Subcommand)]
pub enum IndexAction {
    #[clap(
        about = "Run indexer continuously and update query state.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Run(IndexRunOptions),

    #[clap(
        about = "Run one catch-up indexing cycle.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    CatchUp(IndexCatchUpOptions),
}

#[derive(Clone, Debug, Args)]
pub struct StatusOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub stream_id: Option<String>,
}

#[derive(Clone, Debug, Args)]
pub struct LocateSequenceOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub stream_id: String,

    #[clap(long)]
    pub at: u64,
}

#[derive(Clone, Debug, Args)]
pub struct LocateRangeOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub stream_id: String,

    #[clap(long)]
    pub from: u64,

    #[clap(long)]
    pub count: usize,

    #[clap(long, default_value_t = QueryEmitMode::Selectors, value_enum)]
    pub emit: QueryEmitMode,
}

#[derive(Clone, Debug, Args)]
pub struct LocateLocatorOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub stream_id: String,

    #[clap(
        long,
        help = "Locator in <segment_id>:<generation>:<offset>:<frame_len> format."
    )]
    pub at: String,
}

#[derive(Clone, Debug, Args)]
pub struct LocateWindowOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long)]
    pub stream_id: String,

    #[clap(long)]
    pub start_ns: Option<u64>,

    #[clap(long)]
    pub end_ns: Option<u64>,

    #[clap(long)]
    pub start_utc: Option<String>,

    #[clap(long)]
    pub end_utc: Option<String>,

    #[clap(long, value_enum, default_value_t = TimeField::Event)]
    pub time_field: TimeField,

    #[clap(long, default_value_t = QueryEmitMode::Selectors, value_enum)]
    pub emit: QueryEmitMode,
}

#[derive(Clone, Debug, Args)]
pub struct AlignWindowOptions {
    #[clap(long)]
    pub db_path: std::path::PathBuf,

    #[clap(long, value_delimiter = ',')]
    pub streams: Vec<String>,

    #[clap(long)]
    pub start_ns: Option<u64>,

    #[clap(long)]
    pub end_ns: Option<u64>,

    #[clap(long)]
    pub start_utc: Option<String>,

    #[clap(long)]
    pub end_utc: Option<String>,

    #[clap(long, value_enum, default_value_t = TimeField::Event)]
    pub time_field: TimeField,

    #[clap(long, value_enum, default_value_t = AlignMode::Anchor)]
    pub mode: AlignMode,

    #[clap(long)]
    pub anchor_stream: Option<String>,

    #[clap(long)]
    pub step_ns: Option<u64>,

    #[clap(long, default_value = "0")]
    pub max_skew_ns: u64,

    #[clap(long, value_enum, default_value_t = FillPolicy::Drop)]
    pub fill_policy: FillPolicy,

    #[clap(long, default_value_t = false)]
    pub require_all_streams: bool,

    #[clap(long, default_value_t = QueryEmitMode::Aligned, value_enum)]
    pub emit: QueryEmitMode,

    #[clap(long, default_value_t = false)]
    pub include_provenance: bool,

    #[clap(long)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Subcommand)]
pub enum QueryAction {
    #[clap(
        about = "Resolve one sequence to locator metadata.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    LocateSequence(LocateSequenceOptions),

    #[clap(
        about = "Resolve a sequence range.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    LocateRange(LocateRangeOptions),

    #[clap(
        about = "Resolve one locator back to indexed metadata.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    LocateLocator(LocateLocatorOptions),

    #[clap(
        about = "Resolve a time window to selector rows.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    LocateWindow(LocateWindowOptions),

    #[clap(
        about = "Align multiple streams on a common time basis.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    AlignWindow(AlignWindowOptions),
}

#[derive(Clone, Debug, Subcommand)]
pub enum LogQueryAction {
    #[clap(
        about = "Run metadata indexing commands.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Index {
        #[clap(subcommand)]
        action: IndexAction,
    },

    #[clap(
        about = "Show indexer/query readiness state.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Status(StatusOptions),

    #[clap(
        about = "Resolve metadata queries.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Query {
        #[clap(subcommand)]
        action: QueryAction,
    },
}
