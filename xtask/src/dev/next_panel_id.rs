//! `cargo xtask dev next-panel-id`: the port of `docs/dashboards/next-panel-id.py`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask dev next-panel-id`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask dev next-panel-id`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask dev next-panel-id` is not ported yet: {:?}",
        args.rest
    )
}
