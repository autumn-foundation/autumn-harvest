//! `cargo xtask check preflight-declared-deps-example`: the port of `scripts/check-preflight-declared-deps-example.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check preflight-declared-deps-example`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check preflight-declared-deps-example`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check preflight-declared-deps-example` is not ported yet: {:?}",
        args.rest
    )
}
