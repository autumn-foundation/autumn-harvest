//! `cargo xtask check child-idempotency-examples`: the port of `scripts/check-child-idempotency-examples.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check child-idempotency-examples`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check child-idempotency-examples`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check child-idempotency-examples` is not ported yet: {:?}",
        args.rest
    )
}
