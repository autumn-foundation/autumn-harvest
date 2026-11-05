//! Sandbox guest image tooling (formerly `scripts/sandbox/*.sh`).

use std::process::ExitCode;

use smop::prelude::*;

pub mod build_guest_image;
pub mod image_path;
pub mod publish_image;

/// Each subcommand of `cargo xtask sandbox`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Port of `scripts/sandbox/build-guest-image.sh`.
    BuildGuestImage(build_guest_image::Args),
    /// Port of `scripts/sandbox/image-path.sh`.
    ImagePath(image_path::Args),
    /// Port of `scripts/sandbox/publish-image.sh`.
    PublishImage(publish_image::Args),
}

/// Runs one subcommand of `cargo xtask sandbox`.
pub fn run(command: Command) -> Result<ExitCode> {
    match command {
        Command::BuildGuestImage(args) => build_guest_image::run(args),
        Command::ImagePath(args) => image_path::run(args),
        Command::PublishImage(args) => publish_image::run(args),
    }
}
