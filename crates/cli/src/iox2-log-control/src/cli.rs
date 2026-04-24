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

use iox2_log_archive_cli::Format;
use iox2_log_archive_cli::HelpOptions;
use iox2_log_archive_cli::help_template;

#[derive(Parser)]
#[command(
    name = "iox2-log-control",
    bin_name = "iox2-log-control",
    about = "Control a running iox2-log-recorder daemon via request-response",
    long_about = None,
    version = env!("CARGO_PKG_VERSION"),
    disable_help_subcommand = true,
    arg_required_else_help = false,
    help_template = help_template(HelpOptions::PrintCommandSection),
)]
pub struct Cli {
    #[clap(subcommand)]
    pub action: Option<LogControlAction>,

    #[clap(long, short = 'f', value_enum, global = true, default_value_t = Format::Ron)]
    pub format: Format,
}

#[derive(Clone, Debug, Args)]
pub struct LogControlOptions {
    #[clap(long, help = "Logical service name recorded by the daemon.")]
    pub service: String,

    #[clap(
        short,
        long,
        default_value = "iox2-log-control",
        help = "Node name of the control client endpoint."
    )]
    pub node_name: String,

    #[clap(
        long,
        default_value = "2000",
        help = "Timeout in milliseconds while waiting for a daemon response."
    )]
    pub timeout_ms: u64,
}

#[derive(Subcommand)]
pub enum LogControlAction {
    #[clap(
        about = "Query recorder counters from a running daemon.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Status(LogControlOptions),

    #[clap(
        about = "Force a durable flush on a running daemon.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Flush(LogControlOptions),

    #[clap(
        about = "Request graceful stop of a running daemon.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Stop(LogControlOptions),

    #[clap(
        about = "Pause recording and drop incoming live samples.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Pause(LogControlOptions),

    #[clap(
        about = "Resume recording after a pause.",
        help_template = help_template(HelpOptions::DontPrintCommandSection)
    )]
    Resume(LogControlOptions),
}
