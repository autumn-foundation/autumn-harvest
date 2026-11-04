//! `cargo xtask check durable-timers-examples`: the port of `scripts/check-durable-timers-examples.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check durable-timers-examples`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check durable-timers-examples`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check durable-timers-examples` is not ported yet: {:?}",
        args.rest
    )
}
