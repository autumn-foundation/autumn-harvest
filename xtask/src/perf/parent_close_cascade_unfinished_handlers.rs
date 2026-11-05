//! `cargo xtask perf parent-close-cascade-unfinished-handlers`: the port of `autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask perf parent-close-cascade-unfinished-handlers`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask perf parent-close-cascade-unfinished-handlers`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask perf parent-close-cascade-unfinished-handlers` is not ported yet: {:?}",
        args.rest
    )
}
