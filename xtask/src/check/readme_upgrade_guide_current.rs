//! `cargo xtask check readme-upgrade-guide-current`: the port of `scripts/check-readme-upgrade-guide-current.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check readme-upgrade-guide-current`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check readme-upgrade-guide-current`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check readme-upgrade-guide-current` is not ported yet: {:?}",
        args.rest
    )
}
