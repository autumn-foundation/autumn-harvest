//! `cargo xtask perf quota-reconcile-candidate-scan`: the port of `autumn-harvest/scripts/quota_reconcile_candidate_scan_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf quota-reconcile-candidate-scan`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf quota-reconcile-candidate-scan`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf quota-reconcile-candidate-scan` is not ported yet: {:?}",
        args.rest
    )
}
