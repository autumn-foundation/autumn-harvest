//! `cargo xtask perf usage-report-activity-lookback`: the port of `autumn-harvest/scripts/usage_report_activity_lookback_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf usage-report-activity-lookback`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf usage-report-activity-lookback`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf usage-report-activity-lookback` is not ported yet: {:?}",
        args.rest
    )
}
