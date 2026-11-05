//! `cargo xtask ci pull-test-images`: the port of `.github/ci/pull-test-images.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask ci pull-test-images`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask ci pull-test-images`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask ci pull-test-images` is not ported yet: {:?}",
        args.rest
    )
}
