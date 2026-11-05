//! `cargo xtask check formal-traces`: check engine traces against the TLA+
//! specs with TLC (issue #2003).
//!
//! This is the port of `scripts/check-formal-traces.sh`.
//!
//! Usage: `cargo xtask check formal-traces <dir>...`
//!
//! Each `<dir>` holds NDJSON traces. The header line of a trace names its
//! spec and the result that each guard setting must give. The chaos suite
//! writes traces to `HARVEST_TLA_TRACE_DIR`. `formal/tla/trace/fixtures`
//! holds fixtures.
//!
//! `TLA2TOOLS_JAR` names a local `tla2tools.jar`. Without it, the command
//! downloads the pinned release and checks its SHA-256. The pin is the one
//! of `cargo xtask check formal-models`, so both gates run the same TLC.
//! The trace check itself is `cargo xtask dev formal-traces`, and this
//! command calls it in process.

use std::path::Path;
use std::process::ExitCode;

use smop::prelude::*;

use super::formal_models::{finish, tla2tools_jar};
use crate::util::repo_path;

/// Arguments of `cargo xtask check formal-traces`.
#[derive(Debug, Args)]
pub struct Args {
    /// The directories of NDJSON traces.
    pub dirs: Vec<String>,
}

/// Runs `cargo xtask check formal-traces`.
pub fn run(args: Args) -> Result<ExitCode> {
    if args.dirs.is_empty() {
        eprintln!("usage: cargo xtask check formal-traces <trace dir>...");
        return Ok(ExitCode::from(2));
    }
    finish(check_traces(&args.dirs))
}

/// Fetches TLC, then checks every trace in `dirs`.
fn check_traces(dirs: &[String]) -> Result<ExitCode> {
    let work = fs::temp_dir()?;
    let jar = tla2tools_jar(work.path())?;
    crate::dev::formal_traces::check(
        &jar,
        &repo_path("formal/tla"),
        &Path::new(work.path()).join("run"),
        dirs,
    )
}
