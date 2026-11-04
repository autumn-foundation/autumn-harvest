//! `cargo xtask dev formal-traces`: the port of `scripts/formal_traces.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev formal-traces`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev formal-traces`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev formal-traces` is not ported yet: {:?}",
        args.rest
    )
}
