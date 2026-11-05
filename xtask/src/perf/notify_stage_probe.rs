//! `cargo xtask perf notify-stage-probe`: the port of `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf notify-stage-probe`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf notify-stage-probe`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf notify-stage-probe` is not ported yet: {:?}",
        args.rest
    )
}
