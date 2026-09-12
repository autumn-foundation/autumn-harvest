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
mod shutdown;
mod tools;

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::protocol::{Request, Response, SessionView};

/// The socket a command uses when the operator names none.
///
/// The printed `approve` line carries `--socket` only when the operator chose
/// another one, so the common case stays short.
const DEFAULT_SOCKET: &str = "agentd.sock";

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
        default_value = DEFAULT_SOCKET
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
        ///
        /// The Messages API requires at least one token, and a zero cap would
        /// be refused there. That refusal is not retryable, so every session
        /// submitted to such a daemon would fail. The floor is here instead.
        #[arg(
            long,
            env = "AGENTD_MAX_TOKENS",
            default_value_t = claude::DEFAULT_MAX_TOKENS,
            value_parser = clap::value_parser!(u32).range(1..)
        )]
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

/// The API key, from the environment only.
///
/// There is deliberately no `--api-key` flag. A process's arguments are
/// readable by every user of the host, through `ps` or `/proc/<pid>/cmdline`.
/// This daemon runs for as long as its sessions do. A key on the command line
/// would therefore be readable by the users the owner-only socket exists to
/// keep out.
///
/// An absent key selects the offline stub model.
fn api_key() -> Option<String> {
    usable_key(&std::env::var("ANTHROPIC_API_KEY").unwrap_or_default())
}

/// Read one environment value as a key, or as no key at all.
///
/// A key of whitespace is not a key. It would otherwise count as present, and
/// the daemon would run live against it. Every turn would then fail at the
/// API, where an absent key runs the offline stub instead.
///
/// The value is trimmed, because an operator commonly reads a key out of a
/// file and keeps the newline. A header carries that byte to the API, which
/// rejects it, and the error names neither the newline nor the file.
fn usable_key(raw: &str) -> Option<String> {
    let key = raw.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// Dispatch one command.
async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Serve {
            workspace,
            model,
            max_tokens,
            tick_ms,
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
                api_key: api_key(),
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
            &cli.socket,
        ),
        Command::Status { execution_id, full } => report(
            protocol::call(&cli.socket, &Request::Status { execution_id, full }).await?,
            &cli.socket,
        ),
        Command::List => report(
            protocol::call(&cli.socket, &Request::List).await?,
            &cli.socket,
        ),
        Command::History { execution_id } => report(
            protocol::call(&cli.socket, &Request::History { execution_id }).await?,
            &cli.socket,
        ),
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
            &cli.socket,
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
            &cli.socket,
        ),
    }
}

/// Print one line, and end quietly when the reader has gone.
///
/// `println!` panics once stdout is closed, so `agentd history … | head` would
/// end in a backtrace instead of a clean exit.
///
/// Every line this command prints goes through here, so [`visible`] is applied
/// once, at the sink, rather than at each place that formats model text.
fn line(text: &str) {
    use std::io::Write;
    drop(writeln!(std::io::stdout(), "{}", visible(text)));
}

/// The `--socket` the printed command needs, or nothing.
///
/// `status` reaches the daemon the operator named, and the command it prints
/// must reach the same one. Without the flag the copied line goes to the
/// default socket, which is another daemon or nothing at all. The documented
/// setup gives each daemon its own socket.
///
/// The path is quoted when a shell would read it as more than one word. It
/// comes from the operator rather than from the model. A directory with a
/// space in its name is ordinary, and the line is made to be copied.
fn socket_flag(socket: &Path) -> String {
    if socket == Path::new(DEFAULT_SOCKET) {
        return String::new();
    }
    format!(" --socket {}", quoted(&socket.to_string_lossy()))
}

/// One shell word, quoted only when it needs to be.
fn quoted(word: &str) -> String {
    let plain = !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'));
    if plain {
        return word.to_string();
    }
    // Single quotes take everything literally. A single quote itself cannot
    // appear inside them, so it is closed, escaped, and reopened.
    format!("'{}'", word.replace('\'', "'\\''"))
}

/// Show a character a terminal would obey, instead of obeying it.
///
/// The model writes into this output: the answer, a tool name, a tool
/// argument. A file in the workspace can tell the model what to write, so all
/// of it is untrusted. An escape sequence would rewrite the display an
/// operator reads a pending call from, and `OSC 52` would write their
/// clipboard. The operator approves a call from what this prints.
///
/// A newline and a tab are kept. An answer uses them for layout, and neither
/// one moves the cursor back over text that is already written. A carriage
/// return is NOT kept: it returns to the start of the line, and what follows
/// overwrites what the operator already read.
///
/// The bidirectional controls are escaped as well. They obey nothing, but
/// they reorder what is displayed, so a path can be shown as a different path.
/// The whole `Bidi_Control` set is covered, and not only the overrides: a
/// single mark beside right-to-left text reorders it too.
///
/// The other format characters are left alone, because a joiner is part of
/// ordinary text.
///
/// The characters are shown, not removed. An operator can then see what
/// arrived, rather than a tidied version of it.
fn visible(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.chars().any(is_obeyed) {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut shown = String::with_capacity(text.len());
    for character in text.chars() {
        if is_obeyed(character) {
            let _ = write!(shown, "\\u{{{:04x}}}", character as u32);
        } else {
            shown.push(character);
        }
    }
    std::borrow::Cow::Owned(shown)
}

/// Would a terminal act on this character rather than print it?
fn is_obeyed(character: char) -> bool {
    // The Unicode `Bidi_Control` property, in full: the marks, the embeddings
    // and overrides, and the isolates.
    let bidi = matches!(character,
        '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}');
    bidi || (character.is_control() && character != '\n' && character != '\t')
}

/// Print one answer from the daemon.
fn report(response: Response, socket: &Path) -> Result<(), String> {
    match response {
        Response::Submitted { execution_id } => {
            line(&execution_id);
            line(&format!("Watch it with: agentd status {execution_id}"));
        }
        Response::Session { session } => print_session(&session, socket),
        Response::Sessions { sessions } => {
            if sessions.is_empty() {
                line("no sessions yet");
            }
            for session in &sessions {
                print_session(session, socket);
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
fn print_session(view: &SessionView, socket: &Path) {
    for text in session_lines(view, socket) {
        line(&text);
    }
}

/// Render one session as the lines an operator reads.
///
/// The lines are built here, apart from the printing, so a test can read what
/// the operator would see. This view carries the model's own words, and the
/// operator approves a pending call from it, so every line leaves through
/// [`visible`].
fn session_lines(view: &SessionView, socket: &Path) -> Vec<String> {
    let mut lines = vec![
        format!("{}  {}", view.execution_id, view.state),
        format!("  goal:    {}", view.goal),
    ];
    if let Some(blocked) = &view.blocked_on {
        lines.push(format!("  blocked: {blocked}"));
    }
    if let Some(pending) = &view.pending {
        lines.push(format!("  pending: {} ({})", pending.tool, pending.id));
        lines.push(format!("           {}", pending.input));
        lines.push(format!(
            "  decide:  agentd approve{} {} {}   (or `deny`)",
            socket_flag(socket),
            view.execution_id,
            pending.token
        ));
    }
    if let Some(answer) = &view.answer {
        lines.push(format!("  answer:  {answer}"));
    }
    if let Some(error) = &view.error {
        lines.push(format!("  error:   {error}"));
    }
    lines
        .into_iter()
        .map(|text| visible(&text).into_owned())
        .collect()
}
