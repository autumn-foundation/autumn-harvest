use std::io::Write;

use clap::Parser;

use autumn_harvest_cli::{Cli, run_cli};

/// Stack for the thread that does all the work.
///
/// Windows gives a process's main thread **1 MiB** by default. A debug build of
/// a CLI with this many subcommands overflows that inside clap's own
/// argument-tree construction — before any subcommand is dispatched, so even
/// `harvest --help` aborts with `STATUS_STACK_OVERFLOW` (`0xC00000FD`). Linux's
/// 8 MiB default hides it entirely.
///
/// A thread's stack is sized at spawn and is not subject to the main thread's
/// limit, so doing the work here makes the binary behave the same everywhere.
/// This is the same approach rustc itself takes for the same reason.
///
/// Reproduce the Windows condition on Linux with:
/// `bash -c 'ulimit -s 1024; harvest --help'`
const WORKER_STACK_BYTES: usize = 16 * 1024 * 1024;

fn main() {
    let worker = std::thread::Builder::new()
        .name("harvest-main".to_string())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(run)
        .expect("failed to spawn the harvest worker thread");

    // A panic inside `run` has already printed its own message; exit non-zero
    // rather than the silent success a discarded `JoinHandle` would produce.
    let code = worker.join().unwrap_or(101);
    std::process::exit(code);
}

fn run() -> i32 {
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to start the async runtime: {error}");
            return 1;
        }
    };

    let code = runtime.block_on(async {
        if let Err(error) = run_cli(cli).await {
            let exit_code = error.exit_code();
            eprintln!("error: {error}");
            return exit_code;
        }
        0
    });

    // `std::process::exit` does not unwind, so flush explicitly rather than
    // relying on it to drain a block-buffered stdout (which is what a caller
    // capturing our output gets).
    let _ = std::io::stdout().flush();
    code
}
