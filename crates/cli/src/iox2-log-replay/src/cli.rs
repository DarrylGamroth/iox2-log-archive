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
    name = "iox2-log-replay",
    bin_name = "iox2-log-replay",
    about = "Replay log archive records to stdout or iceoryx2 services",
    long_about = None,
    version = env!("CARGO_PKG_VERSION"),
    disable_help_subcommand = true,
    arg_required_else_help = false,
    help_template = help_template(HelpOptions::PrintCommandSection),
)]
pub struct Cli {
    #[clap(subcommand)]
    pub action: Option<LogReplayAction>,

    #[clap(long, short = 'f', value_enum, global = true, default_value_t = Format::Ron)]
    pub format: Format,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ReplayDestination {
    Stdout,
    PublishSubscribe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum ReplayRateMode {
    Fast,
    Recorded,
    Fixed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SelectorFormat {
    Ndjson,
    Csv,
}

#[derive(Clone, Debug, Args)]
#[command(group(
    ArgGroup::new("service_requirement")
        .required(false)
        .args(&["service"]),
))]
pub struct ReplayOptions {
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

    #[clap(long, value_enum, default_value_t = ReplayDestination::Stdout)]
    pub to: ReplayDestination,

    #[clap(long, help = "Destination service name for --to publish-subscribe.")]
    pub service: Option<String>,

    #[clap(long, value_enum, default_value_t = ReplayRateMode::Fast)]
    pub rate: ReplayRateMode,

    #[clap(long, help = "Required when --rate=fixed.")]
    pub messages_per_sec: Option<u64>,

    #[clap(
        long,
        default_value = "2000",
        help = "Maximum sleep applied by --rate=recorded in milliseconds."
    )]
    pub max_recorded_gap_ms: u64,

    #[clap(
        long,
        default_value = "iox2-log-replay",
        help = "Node name for service replay outputs."
    )]
    pub node_name: String,

    #[clap(long, help = "Skip missing records instead of failing.")]
    pub skip_missing: bool,

    #[clap(
        long,
        help = "Maximum tolerated replay errors before failing (default: 1; with --skip-missing default is unlimited)."
    )]
    pub max_errors: Option<usize>,

    #[clap(
        long,
        action = ArgAction::SetTrue,
        help = "Refresh commit.idxlog and follow newly committed records."
    )]
    pub follow: bool,

    #[clap(
        long,
        default_value = "100",
        help = "Polling interval for --follow in milliseconds."
    )]
    pub follow_poll_ms: u64,

    #[clap(
        long,
        help = "Stop --follow after this many milliseconds without a new visible record."
    )]
    pub follow_idle_timeout_ms: Option<u64>,

    #[clap(subcommand)]
    pub selector: ReplaySelector,
}

#[derive(Clone, Debug, Args)]
pub struct SequenceSelector {
    #[clap(long)]
    pub at: u64,
}

#[derive(Clone, Debug, Args)]
pub struct RangeSelector {
    #[clap(long)]
    pub from: u64,

    #[clap(long)]
    pub count: usize,
}

#[derive(Clone, Debug, Args)]
pub struct LocatorSelector {
    #[clap(
        long,
        help = "Locator in <segment_id>:<generation>:<offset>:<frame_len> format."
    )]
    pub at: String,
}

#[derive(Clone, Debug, Args)]
#[command(group(
    ArgGroup::new("selector_source")
        .required(true)
        .args(&["stdin", "file"]),
))]
pub struct SelectorsSelector {
    #[clap(long, action = ArgAction::SetTrue, group = "selector_source")]
    pub stdin: bool,

    #[clap(long, group = "selector_source")]
    pub file: Option<std::path::PathBuf>,

    #[clap(long, value_enum, default_value_t = SelectorFormat::Ndjson)]
    pub selector_format: SelectorFormat,
}

#[derive(Clone, Debug, Subcommand)]
pub enum ReplaySelector {
    #[clap(
        about = "Replay every available archive record in sequence order.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    All,

    #[clap(
        about = "Replay one record by archive sequence.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Sequence(SequenceSelector),

    #[clap(
        about = "Replay a sequence range.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Range(RangeSelector),

    #[clap(
        about = "Replay one record by physical locator.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Locator(LocatorSelector),

    #[clap(
        about = "Replay selectors from stdin or file (ndjson/csv).",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Selectors(SelectorsSelector),
}

#[derive(Subcommand)]
pub enum LogReplayAction {
    #[clap(
        about = "Replay archived records.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Replay(ReplayOptions),
}
