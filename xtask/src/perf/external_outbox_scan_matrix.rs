//! `cargo xtask perf external-outbox-scan-matrix`: the port of `autumn-harvest/scripts/external_outbox_scan_matrix.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf external-outbox-scan-matrix`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf external-outbox-scan-matrix`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf external-outbox-scan-matrix` is not ported yet: {:?}",
        args.rest
    )
}
