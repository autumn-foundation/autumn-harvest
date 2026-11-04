//! `cargo xtask check agent-adapter-deps`: the port of `scripts/check-agent-adapter-deps.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check agent-adapter-deps`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check agent-adapter-deps`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check agent-adapter-deps` is not ported yet: {:?}",
        args.rest
    )
}
