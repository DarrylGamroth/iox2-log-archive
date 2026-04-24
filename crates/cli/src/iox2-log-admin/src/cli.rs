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

use clap::ArgAction;
use clap::ArgGroup;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;

use iox2_log_archive_cli::Format;
use iox2_log_archive_cli::HelpOptions;
use iox2_log_archive_cli::help_template;

#[derive(Parser)]
#[command(
    name = "iox2-log-admin",
    bin_name = "iox2-log-admin",
    about = "Operate and inspect userland log archive recorder state",
    long_about = None,
    version = env!("CARGO_PKG_VERSION"),
    disable_help_subcommand = true,
    arg_required_else_help = false,
    help_template = help_template(HelpOptions::PrintCommandSection),
)]
pub struct Cli {
    #[clap(subcommand)]
    pub action: Option<LogRecorderAction>,

    #[clap(long, short = 'f', value_enum, global = true, default_value_t = Format::Ron)]
    pub format: Format,
}

#[derive(Clone, Copy, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
pub enum CliRecorderProfile {
    Durable,
    #[default]
    Balanced,
    Throughput,
    Replay,
}

#[derive(Clone, Copy, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
pub enum CliPersistenceMode {
    Volatile,
    #[default]
    Async,
    Sync,
}

#[derive(Clone, Debug, Args)]
pub struct LogRecorderArchiveOptions {
    #[clap(long, help = "Logical service name represented by this archive.")]
    pub service: String,

    #[clap(
        long,
        help = "Path to archive storage root for the service (contains catalog.bin and segments/)."
    )]
    pub storage_path: std::path::PathBuf,

    #[clap(
        long,
        help = "Path to metadata root for commit.idxlog (defaults to --storage-path)."
    )]
    pub metadata_log_path: Option<std::path::PathBuf>,
}

#[derive(Parser)]
pub struct LogRecorderStartOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long, value_enum, default_value_t = CliRecorderProfile::Balanced)]
    pub profile: CliRecorderProfile,

    #[clap(long, value_enum, default_value_t = CliPersistenceMode::Async)]
    pub mode: CliPersistenceMode,

    #[clap(long, default_value = "268435456")]
    pub segment_bytes: usize,

    #[clap(long, default_value = "1")]
    pub spare_preallocated_segments: usize,

    #[clap(long, default_value_t = true, action = ArgAction::Set)]
    pub segment_preallocate: bool,

    #[clap(long)]
    pub max_disk_bytes: Option<u64>,
}

#[derive(Parser)]
pub struct LogRecorderStopOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,
}

#[derive(Parser)]
pub struct LogRecorderStatusOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,
}

#[derive(Parser)]
pub struct LogRecorderFlushOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,
}

#[derive(Parser)]
pub struct LogRecorderTrimOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long)]
    pub before_sequence: u64,
}

#[derive(Parser)]
pub struct LogRecorderDetachOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long)]
    pub before_sequence: u64,
}

#[derive(Parser)]
pub struct LogRecorderAttachOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,
}

#[derive(Parser)]
pub struct LogRecorderDeleteDetachedOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long)]
    pub before_sequence: Option<u64>,
}

#[derive(Parser)]
pub struct LogRecorderListSegmentsOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long, help = "Return only detached segments.")]
    pub detached_only: bool,
}

#[derive(Parser)]
pub struct LogRecorderInspectCommitLogOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long, default_value = "1")]
    pub from_ordinal: u64,

    #[clap(long, default_value = "128")]
    pub limit: usize,
}

#[derive(Parser)]
#[command(group(
    ArgGroup::new("record_locator")
        .required(true)
        .args(&["at_sequence", "at_locator"]),
))]
pub struct LogRecorderInspectRecordOptions {
    #[command(flatten)]
    pub archive: LogRecorderArchiveOptions,

    #[clap(long, group = "record_locator", conflicts_with = "at_locator")]
    pub at_sequence: Option<u64>,

    #[clap(long, group = "record_locator", conflicts_with = "at_sequence")]
    pub at_locator: Option<String>,

    #[clap(long, default_value = "64")]
    pub preview_bytes: usize,
}

#[derive(Subcommand)]
pub enum LogRecorderAction {
    #[clap(
        about = "Create or recover a recorder archive.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Start(LogRecorderStartOptions),
    #[clap(
        about = "Finalize current recorder archive state.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Stop(LogRecorderStopOptions),
    #[clap(
        about = "Show recorder status and retention counters.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Status(LogRecorderStatusOptions),
    #[clap(
        about = "Flush active recorder archive files.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Flush(LogRecorderFlushOptions),
    #[clap(
        about = "Trim archived segments before a sequence.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Trim(LogRecorderTrimOptions),
    #[clap(
        about = "Detach archived segments before a sequence.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Detach(LogRecorderDetachOptions),
    #[clap(
        about = "Attach all detached archived segments.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Attach(LogRecorderAttachOptions),
    #[clap(
        about = "Delete detached archived segments.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    DeleteDetached(LogRecorderDeleteDetachedOptions),
    #[clap(
        about = "List sealed archived segment states.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    ListSegments(LogRecorderListSegmentsOptions),
    #[clap(
        about = "Inspect commit.idxlog entries.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    InspectCommitLog(LogRecorderInspectCommitLogOptions),
    #[clap(
        about = "Inspect one archived record by sequence or locator.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    InspectRecord(LogRecorderInspectRecordOptions),
}
