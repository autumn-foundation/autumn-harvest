//! Developer and release tooling (formerly loose scripts).

use std::process::ExitCode;

use smop::prelude::*;

pub mod build_typescript_client;
pub mod clean_room_getting_started;
pub mod flake_rerun;
pub mod formal_traces;
pub mod fuzz_smoke;
pub mod next_panel_id;
pub mod regenerate_openapi;
pub mod run_mixed_version_smoke;
pub mod run_standalone_chapter;

/// Each subcommand of `cargo xtask dev`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `docs/dashboards/next-panel-id.py`.
    NextPanelId(next_panel_id::Args),
    /// Port of `scripts/build-typescript-client.sh`.
    BuildTypescriptClient(build_typescript_client::Args),
    /// Port of `scripts/regenerate-openapi.sh`.
    RegenerateOpenapi(regenerate_openapi::Args),
    /// Port of `scripts/flake-rerun.sh`.
    FlakeRerun(flake_rerun::Args),
    /// Port of `scripts/run-standalone-chapter.sh`.
    RunStandaloneChapter(run_standalone_chapter::Args),
    /// Port of `scripts/run-mixed-version-smoke.sh`.
    RunMixedVersionSmoke(run_mixed_version_smoke::Args),
    /// Port of `scripts/clean-room-getting-started.sh`.
    CleanRoomGettingStarted(clean_room_getting_started::Args),
    /// Port of `scripts/formal_traces.py`.
    FormalTraces(formal_traces::Args),
    /// Port of `fuzz/smoke.sh`.
    FuzzSmoke(fuzz_smoke::Args),
}

/// Runs one subcommand of `cargo xtask dev`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::NextPanelId(args) => next_panel_id::run(args),
        Command::BuildTypescriptClient(args) => build_typescript_client::run(args),
        Command::RegenerateOpenapi(args) => regenerate_openapi::run(args),
        Command::FlakeRerun(args) => flake_rerun::run(args),
        Command::RunStandaloneChapter(args) => run_standalone_chapter::run(args),
        Command::RunMixedVersionSmoke(args) => run_mixed_version_smoke::run(args),
        Command::CleanRoomGettingStarted(args) => clean_room_getting_started::run(args),
        Command::FormalTraces(args) => formal_traces::run(args),
        Command::FuzzSmoke(args) => fuzz_smoke::run(args),
    }
}
