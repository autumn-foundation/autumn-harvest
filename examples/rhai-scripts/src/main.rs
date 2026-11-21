//! `harvest-rhai`: run Rhai scripts that survive a crash.

use std::process::ExitCode;

use rhai_scripts::cli::{self, Cli};
use smop::prelude::Parser;

#[tokio::main]
async fn main() -> ExitCode {
    cli::main(Cli::parse()).await
}
