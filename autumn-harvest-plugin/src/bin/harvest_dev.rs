//! `harvest-dev` — the zero-setup local dev runtime (issue #525).
//!
//! ```text
//! cargo dev
//! ```
//!
//! Starts an ephemeral `PostgreSQL`, applies the engine's migrations, runs a
//! worker, and serves the management API and the Vantage dashboard. Prints the
//! dashboard URL and one command that starts a durable workflow. Reclaims
//! everything it created on exit.
//!
//! Development and evaluation only. It refuses to point at anything it cannot
//! show to be a local development database, and it says so on every start.
//!
//! # Logging
//!
//! This binary deliberately installs **no** tracing subscriber. `autumn-web`
//! installs its own during boot, and a `tracing` global can only be set once —
//! setting one here made that fail fatally, so the app never started and the
//! runtime timed out waiting for a server that had already given up. Verbosity
//! is `RUST_LOG`, exactly as it is for any Autumn app.

use std::path::PathBuf;

use autumn_harvest_plugin::dev::{DevRuntime, DevRuntimeConfig};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let config = match parse_args(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            print!("{USAGE}");
            return std::process::ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("harvest-dev: {message}\n\n{USAGE}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let runtime = match DevRuntime::start(config).await {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("harvest-dev: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    print!("{}", runtime.banner());

    DevRuntime::wait_for_shutdown_signal().await;
    eprintln!("\nharvest-dev: stopping and reclaiming ephemeral storage…");
    if let Err(error) = runtime.shutdown().await {
        eprintln!("harvest-dev: teardown was incomplete: {error}");
        return std::process::ExitCode::FAILURE;
    }
    std::process::ExitCode::SUCCESS
}

const USAGE: &str = "\
harvest-dev — a throwaway local Harvest runtime (development only)

USAGE:
    harvest-dev [OPTIONS]

OPTIONS:
    --port <PORT>          HTTP port (default 3000; 0 picks a free one)
    --database-url <DSN>   Use this local database instead of provisioning one
    --session-root <DIR>   Where the ephemeral cluster is created
    --allow-suspicious-database-name
                           Accept a loopback database whose name looks
                           production-shaped. There is deliberately no
                           equivalent for a remote host.
    -h, --help             Show this help

ENVIRONMENT:
    HARVEST_DEV_DATABASE_URL   Same as --database-url
    HARVEST_DEV_PORT           Same as --port
    HARVEST_DEV_PG_BIN         Directory holding initdb/pg_ctl/postgres
    HARVEST_DEV_CACHE_DIR      Where a downloaded PostgreSQL is cached
";

/// Parse the argument list. `Ok(None)` means `--help` was asked for.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Option<DevRuntimeConfig>, String> {
    let mut config = DevRuntimeConfig::default().with_env_overrides();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--allow-suspicious-database-name" => config.allow_suspicious_database_name = true,
            "--port" => {
                let value = args.next().ok_or("--port needs a value")?;
                config.http_port = value
                    .parse()
                    .map_err(|_| format!("`{value}` is not a port number"))?;
            }
            "--database-url" => {
                config.database_url = Some(args.next().ok_or("--database-url needs a value")?);
            }
            "--session-root" => {
                config.session_root = Some(PathBuf::from(
                    args.next().ok_or("--session-root needs a value")?,
                ));
            }
            other => return Err(format!("unrecognised argument `{other}`")),
        }
    }
    Ok(Some(config))
}
