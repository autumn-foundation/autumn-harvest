//! `cargo xtask perf broken-session-scan`: the port of `autumn-harvest/scripts/broken_session_scan_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf broken-session-scan`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf broken-session-scan`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf broken-session-scan` is not ported yet: {:?}",
        args.rest
    )
}
