//! `harvest-guest-agent`: process 1 of a sandbox microVM.
//!
//! Without arguments it runs as init: it mounts the basic file systems,
//! serves one run on vsock, and reboots. With `--listen-unix <path>` it
//! serves on a Unix socket until it is killed. That mode is for tests on a
//! host without KVM, and it is not a sandbox.

fn main() -> std::process::ExitCode {
    autumn_harvest_sandbox_guest_main()
}

#[cfg(target_os = "linux")]
fn autumn_harvest_sandbox_guest_main() -> std::process::ExitCode {
    todo!("main")
}

#[cfg(not(target_os = "linux"))]
fn autumn_harvest_sandbox_guest_main() -> std::process::ExitCode {
    eprintln!("harvest-guest-agent runs on Linux only");
    std::process::ExitCode::FAILURE
}
