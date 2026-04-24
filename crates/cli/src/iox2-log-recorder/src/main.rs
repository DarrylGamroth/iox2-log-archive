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

#[cfg(not(debug_assertions))]
use human_panic::setup_panic;
#[cfg(debug_assertions)]
extern crate better_panic;

mod cli;
mod command;

use anyhow::Result;
use clap::CommandFactory;
use clap::Parser;
use cli::Cli;
use iceoryx2_log::error;
use iceoryx2_log::{LogLevel, set_log_level_from_env_or};

fn main() -> Result<()> {
    #[cfg(not(debug_assertions))]
    {
        setup_panic!();
    }
    #[cfg(debug_assertions)]
    {
        better_panic::Settings::debug()
            .most_recent_first(false)
            .lineno_suffix(true)
            .verbosity(better_panic::Verbosity::Full)
            .install();
    }

    set_log_level_from_env_or(LogLevel::Warn);

    let cli = Cli::parse();
    if let Some(action) = cli.action {
        if let Err(e) = command::log_record(action, cli.format) {
            eprintln!("{}", e.to_formatted_error(cli.format));
            std::process::exit(e.exit_code());
        }
    } else if let Err(e) = Cli::command().print_help() {
        error!("Failed to print help: {}", e);
    }

    Ok(())
}
