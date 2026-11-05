//! `cargo xtask check signals-clean-approval-example-determinism`: the port of `scripts/check-signals-clean-approval-example-determinism.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check signals-clean-approval-example-determinism`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check signals-clean-approval-example-determinism`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check signals-clean-approval-example-determinism` is not ported yet: {:?}",
        args.rest
    )
}
