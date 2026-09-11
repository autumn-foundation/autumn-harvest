//! `agentd` — a durable agent harness for Claude, running as a local daemon on
//! embedded `SQLite`.
//!
//! The daemon runs an agent loop as an `autumn-harvest` workflow. Every model
//! call and every tool call is a durable activity. A session therefore
//! survives a restart: recorded turns replay from history instead of being
//! paid for a second time. The whole engine is embedded, so there is no
//! database server and no Docker.
//!
//! ```text
//! agentd serve &                         # the daemon: the only writer
//! agentd submit "summarise the README"   # returns an execution id
//! agentd status <id>                     # the session, including why it parked
//! agentd approve <id> <token>            # release a gated write
//! agentd history <id>                    # the recorded event log
//! ```
//!
//! See `README.md` for the full walkthrough, including the restart proof.

// This backend runs the registered closures, not the async fn bodies. The
// `#[activity]` macro expands each await-free placeholder body into code that
// consumes the `_`-prefixed parameters. Both lints are artifacts of that
// pattern.
#![allow(clippy::unused_async, clippy::used_underscore_binding)]

mod claude;
mod daemon;
mod guard;
mod inspect;
mod protocol;
mod session;
mod tools;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::protocol::{Request, Response, SessionView};

/// The durable Claude agent daemon.
#[derive(Parser)]
#[command(
    name = "agentd",
    version,
    about = "A durable Claude agent daemon on SQLite"
)]
struct Cli {
    /// The workflow database file.
    #[arg(long, global = true, env = "AGENTD_DB", default_value = "agentd.db")]
    db: PathBuf,
    /// The control socket the daemon listens on.
    #[arg(
        long,
        global = true,
        env = "AGENTD_SOCKET",
        default_value = "agentd.sock"
    )]
    socket: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon. This process is the single writer.
    Serve {
        /// The directory the tools may read and write.
        #[arg(long, env = "AGENTD_WORKSPACE", default_value = "agent-workspace")]
        workspace: PathBuf,
        /// The model every session calls.
        #[arg(long, env = "AGENTD_MODEL", default_value = claude::DEFAULT_MODEL)]
        model: String,
        /// The output cap of one turn.
        #[arg(long, env = "AGENTD_MAX_TOKENS", default_value_t = claude::DEFAULT_MAX_TOKENS)]
        max_tokens: u32,
        /// How often the daemon drives its sessions, in milliseconds.
        ///
        /// A zero period has no meaning and panics the timer, so one is the
        /// floor.
        #[arg(
            long,
            env = "AGENTD_TICK_MS",
            default_value_t = 500,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        tick_ms: u64,
        /// The API key. An absent key selects the offline stub model.
        #[arg(long, env = "ANTHROPIC_API_KEY", hide_env_values = true)]
        api_key: Option<String>,
    },
    /// Start one session.
    Submit {
        /// The task, in your own words.
        goal: String,
        /// The hard bound on model calls.
        #[arg(long, default_value_t = 8)]
        max_turns: u32,
        /// How long a gated tool call waits for approval.
        #[arg(long, default_value_t = 900)]
        approval_timeout_secs: u64,
    },
    /// Report one session.
    Status {
        execution_id: String,
        /// Print the pending call's arguments in full.
        #[arg(long)]
        full: bool,
    },
    /// Report every session.
    List,
    /// Print the recorded event log of one session.
    History { execution_id: String },
    /// Release one gated tool call.
    ///
    /// `token` is the approval token `status` printed. It names one wait of one
    /// run, which is what keeps the decision tied to the call you read.
    Approve {
        execution_id: String,
        token: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Refuse one gated tool call.
    Deny {
        execution_id: String,
        token: String,
        #[arg(long)]
        note: Option<String>,
    },
}

/// `#[tokio::main]` builds a multi-thread runtime. The model activity needs
/// one: its body is synchronous and bridges to async through
/// `tokio::task::block_in_place`, which a current-thread runtime rejects.
#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("agentd: {message}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatch one command.
async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Serve {
            workspace,
            model,
            max_tokens,
            tick_ms,
            api_key,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
                )
                .init();
            daemon::serve(daemon::Options {
                db: cli.db,
                socket: cli.socket,
                workspace,
                model,
                max_tokens,
                tick: Duration::from_millis(tick_ms),
                api_key,
            })
            .await
        }
        Command::Submit {
            goal,
            max_turns,
            approval_timeout_secs,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Submit {
                    goal,
                    max_turns,
                    approval_timeout_secs,
                },
            )
            .await?,
        ),
        Command::Status { execution_id, full } => {
            report(protocol::call(&cli.socket, &Request::Status { execution_id, full }).await?)
        }
        Command::List => report(protocol::call(&cli.socket, &Request::List).await?),
        Command::History { execution_id } => {
            report(protocol::call(&cli.socket, &Request::History { execution_id }).await?)
        }
        Command::Approve {
            execution_id,
            token,
            note,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Approve {
                    execution_id,
                    token,
                    approved: true,
                    note,
                },
            )
            .await?,
        ),
        Command::Deny {
            execution_id,
            token,
            note,
        } => report(
            protocol::call(
                &cli.socket,
                &Request::Approve {
                    execution_id,
                    token,
                    approved: false,
                    note,
                },
            )
            .await?,
        ),
    }
}

/// Print one line, and end quietly when the reader has gone.
///
/// `println!` panics once stdout is closed, so `agentd history … | head` would
/// end in a backtrace instead of a clean exit.
fn line(text: &str) {
    use std::io::Write;
    drop(writeln!(std::io::stdout(), "{text}"));
}

/// Print one answer from the daemon.
fn report(response: Response) -> Result<(), String> {
    match response {
        Response::Submitted { execution_id } => {
            line(&execution_id);
            line(&format!("Watch it with: agentd status {execution_id}"));
        }
        Response::Session { session } => print_session(&session),
        Response::Sessions { sessions } => {
            if sessions.is_empty() {
                line("no sessions yet");
            }
            for session in &sessions {
                print_session(session);
                line("");
            }
        }
        Response::History { events } => {
            for event in &events {
                line(event);
            }
        }
        Response::Ack { detail } => line(&detail),
        Response::Error { message } => return Err(message),
    }
    Ok(())
}

/// Print one session in a stable, greppable shape.
fn print_session(view: &SessionView) {
    line(&format!("{}  {}", view.execution_id, view.state));
    line(&format!("  goal:    {}", view.goal));
    if let Some(blocked) = &view.blocked_on {
        line(&format!("  blocked: {blocked}"));
    }
    if let Some(pending) = &view.pending {
        line(&format!("  pending: {} ({})", pending.tool, pending.id));
        line(&format!("           {}", pending.input));
        line(&format!(
            "  decide:  agentd approve {} {}   (or `deny`)",
            view.execution_id, pending.token
        ));
    }
    if let Some(answer) = &view.answer {
        line(&format!("  answer:  {answer}"));
    }
    if let Some(error) = &view.error {
        line(&format!("  error:   {error}"));
    }
}
