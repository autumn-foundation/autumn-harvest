//! `cargo xtask check changelog-connector-migration-version`: the port of `scripts/check-changelog-connector-migration-version.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check changelog-connector-migration-version`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check changelog-connector-migration-version`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check changelog-connector-migration-version` is not ported yet: {:?}",
        args.rest
    )
}
