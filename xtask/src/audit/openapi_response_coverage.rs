//! `cargo xtask audit openapi-response-coverage`: the port of `docs/audits/openapi-response-coverage.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask audit openapi-response-coverage`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask audit openapi-response-coverage`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask audit openapi-response-coverage` is not ported yet: {:?}",
        args.rest
    )
}
