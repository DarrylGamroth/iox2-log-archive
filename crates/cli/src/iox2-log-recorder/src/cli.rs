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
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;

use iox2_log_archive_cli::Format;
use iox2_log_archive_cli::HelpOptions;
use iox2_log_archive_cli::help_template;

#[derive(Parser)]
#[command(
    name = "iox2-log-recorder",
    bin_name = "iox2-log-recorder",
    about = "Run a long-lived recorder process that captures live iceoryx2 traffic into a log archive",
    long_about = None,
    version = env!("CARGO_PKG_VERSION"),
    disable_help_subcommand = true,
    arg_required_else_help = false,
    help_template = help_template(HelpOptions::PrintCommandSection),
)]
pub struct Cli {
    #[clap(subcommand)]
    pub action: Option<LogRecordAction>,

    #[clap(long, short = 'f', value_enum, global = true, default_value_t = Format::Ron)]
    pub format: Format,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
pub enum CliRecorderProfile {
    Durable,
    #[default]
    Balanced,
    Throughput,
    Replay,
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
#[value(rename_all = "kebab-case")]
pub enum CliPersistenceMode {
    Volatile,
    #[default]
    Async,
    Sync,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CliRecorderAckLevel {
    Accepted,
    DurableData,
    DurableDataAndCommitLog,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CliAsyncIoBackend {
    IoUringPreferred,
    IoUringRequired,
    Blocking,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CliChecksumMode {
    None,
    Crc32c,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CliOutOfSpacePolicy {
    FailWriter,
}

#[derive(Clone, Debug, Args)]
pub struct LogRecordArchiveOptions {
    #[clap(long, help = "Logical service name to record.")]
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

    #[clap(
        long,
        value_enum,
        help = "Override async data-path backend. If omitted, the selected profile decides."
    )]
    pub async_io_backend: Option<CliAsyncIoBackend>,

    #[clap(long, help = "Override Linux io_uring queue depth.")]
    pub io_uring_queue_depth: Option<u32>,

    #[clap(long, help = "Override maximum io_uring submissions per batch.")]
    pub io_submit_batch_max: Option<u32>,

    #[clap(long, help = "Override maximum io_uring completions reaped per batch.")]
    pub io_cqe_batch_max: Option<u32>,

    #[clap(
        long,
        action = ArgAction::Set,
        help = "Override io_uring registered-file mode."
    )]
    pub io_uring_register_files: Option<bool>,

    #[clap(long, value_enum, help = "Override persisted frame checksum mode.")]
    pub checksum_mode: Option<CliChecksumMode>,

    #[clap(long, value_enum, help = "Override disk-full handling policy.")]
    pub out_of_space_policy: Option<CliOutOfSpacePolicy>,

    #[clap(long, help = "Override active metadata-log roll threshold in bytes.")]
    pub metadata_log_roll_bytes: Option<u64>,

    #[clap(long, help = "Override global metadata-log size cap in bytes.")]
    pub metadata_log_max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Args)]
pub struct LogRecordRuntimeOptions {
    #[clap(
        short,
        long,
        default_value = "iox2-log-recorder",
        help = "Node name of the recorder endpoint."
    )]
    pub node_name: String,

    #[clap(
        long,
        default_value = "10",
        help = "Wait interval in milliseconds when no data is available."
    )]
    pub cycle_time_ms: u64,

    #[clap(
        long,
        help = "Stop after this many captured messages. If omitted, record indefinitely until timeout or process termination."
    )]
    pub max_messages: Option<u64>,

    #[clap(
        long,
        help = "Stop after this timeout in milliseconds. If omitted, run until max-messages or process termination."
    )]
    pub timeout_ms: Option<u64>,

    #[clap(
        long,
        default_value = "100",
        help = "Flush interval in milliseconds. Set to 0 to disable periodic flushes."
    )]
    pub flush_interval_ms: u64,

    #[clap(
        long,
        value_enum,
        help = "Optional per-record ack wait level. If omitted, uses append behavior without explicit ack wait."
    )]
    pub ack_level: Option<CliRecorderAckLevel>,
}

#[derive(Clone, Debug, Args)]
pub struct LogRecordPublishSubscribeRuntimeOptions {
    #[command(flatten)]
    pub common: LogRecordRuntimeOptions,

    #[clap(
        long,
        help = "Stable source service identity override for pattern adapters. If omitted, a deterministic hash of --service is used."
    )]
    pub source_service_id: Option<u64>,
}

#[derive(Clone, Debug, Args)]
pub struct LogRecordPublishSubscribeOptions {
    #[command(flatten)]
    pub archive: LogRecordArchiveOptions,

    #[command(flatten)]
    pub runtime: LogRecordPublishSubscribeRuntimeOptions,
}

#[derive(Subcommand)]
pub enum LogRecordAction {
    #[clap(
        about = "Record live publish-subscribe samples into a log archive.",
        alias = "pubsub",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    PublishSubscribe(LogRecordPublishSubscribeOptions),
}
