//! `cargo xtask check webhooks-feature-example`: the port of `scripts/check-webhooks-feature-example.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check webhooks-feature-example`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check webhooks-feature-example`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check webhooks-feature-example` is not ported yet: {:?}",
        args.rest
    )
}
