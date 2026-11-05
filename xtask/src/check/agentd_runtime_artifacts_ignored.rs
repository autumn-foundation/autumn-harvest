//! `cargo xtask check agentd-runtime-artifacts-ignored`: the port of `scripts/check-agentd-runtime-artifacts-ignored.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check agentd-runtime-artifacts-ignored`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check agentd-runtime-artifacts-ignored`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check agentd-runtime-artifacts-ignored` is not ported yet: {:?}",
        args.rest
    )
}
