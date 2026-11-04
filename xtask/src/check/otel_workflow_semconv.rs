//! `cargo xtask check otel-workflow-semconv`: the port of `scripts/check-otel-workflow-semconv.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check otel-workflow-semconv`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check otel-workflow-semconv`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check otel-workflow-semconv` is not ported yet: {:?}",
        args.rest
    )
}
