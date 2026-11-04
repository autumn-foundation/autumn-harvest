//! `cargo xtask check verus-proofs`: the port of `scripts/check-verus-proofs.sh`.

use std::process::ExitCode;

use smop::prelude::*;

/// Arguments of `cargo xtask check verus-proofs`.
#[derive(Debug, Args)]
pub struct Args {
    /// Arguments that the port has not defined yet.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub rest: Vec<String>,
}

/// Runs `cargo xtask check verus-proofs`.
pub fn run(args: Args) -> Result<ExitCode> {
    bail!(
        "`cargo xtask check verus-proofs` is not ported yet: {:?}",
        args.rest
    )
}
