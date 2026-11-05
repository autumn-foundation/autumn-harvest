//! `cargo xtask check search-attr-query-syntax`: the port of `scripts/check-search-attr-query-syntax.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check search-attr-query-syntax`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check search-attr-query-syntax`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check search-attr-query-syntax` is not ported yet: {:?}",
        args.rest
    )
}
