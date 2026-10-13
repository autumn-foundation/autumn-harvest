//! `harvest-guest-agent`: process 1 of a sandbox microVM.
//!
//! Without arguments it runs as init: it mounts the basic file systems,
//! serves one run on vsock, and reboots. With `--listen-unix <path>` it
//! serves on a Unix socket until it is killed. That mode is for tests on a
//! host without KVM, and it is not a sandbox.

fn main() -> std::process::ExitCode {
    agent_main()
}

#[cfg(target_os = "linux")]
fn agent_main() -> std::process::ExitCode {
    use std::process::ExitCode;
    use std::sync::Arc;

    use autumn_harvest::sandbox::exec::{ExecOptions, ProcessExecutor};
    use autumn_harvest_sandbox_guest::{linux, parse_cmdline, serve};

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("harvest-guest-agent: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--listen-unix") {
        let Some(path) = args.get(2) else {
            eprintln!("usage: harvest-guest-agent [--listen-unix <path>]");
            return ExitCode::FAILURE;
        };
        let result = runtime.block_on(async {
            let listener = tokio::net::UnixListener::bind(path)?;
            serve(
                || async { listener.accept().await.map(|(stream, _)| stream) },
                &ProcessExecutor::default(),
                &|_: &[u8; 32]| Ok(()),
                false,
            )
            .await
        });
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("harvest-guest-agent: {e}");
                ExitCode::FAILURE
            }
        };
    }

    let pid1 = std::process::id() == 1;
    let result: std::io::Result<()> = runtime.block_on(async {
        linux::mount_basics()?;
        let cmdline = std::fs::read_to_string("/proc/cmdline")?;
        let config = parse_cmdline(&cmdline);
        linux::mount_work(config.work_mib)?;
        let executor = ProcessExecutor::new(ExecOptions {
            scratch_root: config.work_dir.clone(),
            run_as: Some(config.run_as),
        });
        let listener = Arc::new(linux::vsock_listener(config.port)?);
        serve(
            || linux::accept_vsock(&listener),
            &executor,
            &linux::reseed_kernel,
            true,
        )
        .await
    });
    if let Err(e) = &result {
        eprintln!("harvest-guest-agent: {e}");
    }
    if pid1 {
        linux::reboot();
    }
    if result.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(not(target_os = "linux"))]
fn agent_main() -> std::process::ExitCode {
    eprintln!("harvest-guest-agent runs on Linux only");
    std::process::ExitCode::FAILURE
}
