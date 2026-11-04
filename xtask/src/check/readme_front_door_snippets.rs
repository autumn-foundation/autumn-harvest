//! `cargo xtask check readme-front-door-snippets`: the port of `scripts/check-readme-front-door-snippets.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check readme-front-door-snippets`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check readme-front-door-snippets`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check readme-front-door-snippets` is not ported yet: {:?}",
        args.rest
    )
}
