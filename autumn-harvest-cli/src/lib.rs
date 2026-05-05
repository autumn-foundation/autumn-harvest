//! Command-line client for the autumn-harvest management API.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde_json::{Map, Value, json};
use thiserror::Error;

const DEFAULT_BASE_URL: &str = "http://localhost:3000/api/harvest";
const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/');

/// Top-level CLI arguments for the `harvest` binary.
#[derive(Debug, Parser)]
#[command(
    name = "harvest",
    version,
    about = "Manage autumn-harvest workflows and DAGs"
)]
pub struct Cli {
    /// Base URL where the Harvest management API is mounted.
    #[arg(
        long,
        global = true,
        env = "HARVEST_URL",
        default_value = DEFAULT_BASE_URL
    )]
    base_url: String,

    /// Bearer token to send with every request.
    #[arg(long, global = true, env = "HARVEST_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Operator identity recorded in the audit trail for mutating commands.
    /// Only sent on POST/PATCH/DELETE requests as `x-harvest-actor`.
    /// If omitted, the server defaults to `"anonymous"` (acceptable only for dev).
    #[arg(long, global = true, env = "HARVEST_ACTOR")]
    actor: Option<String>,

    /// Correlation request-id forwarded as `x-request-id` on mutating commands.
    #[arg(long, global = true, env = "HARVEST_REQUEST_ID")]
    request_id: Option<String>,

    /// Output format for successful API responses.
    #[arg(long, global = true, value_enum, default_value = "pretty-json")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

/// Successful response output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    /// Pretty-printed JSON.
    PrettyJson,
    /// Compact JSON for scripts.
    Json,
}

/// Signal reapply policy for `workflow reset`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ResetSignalReapply {
    /// Discard undelivered signals on the source execution.
    Drop,
    /// Re-enqueue undelivered source signals onto the fork.
    Buffer,
}

impl ResetSignalReapply {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Buffer => "buffer",
        }
    }
}

/// HTTP method used by a management API request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiMethod {
    /// HTTP GET.
    Get,
    /// HTTP PATCH.
    Patch,
    /// HTTP POST.
    Post,
    /// HTTP DELETE.
    Delete,
}

/// Thin request description built from CLI arguments.
#[derive(Debug, Eq, PartialEq)]
pub struct ApiRequest {
    /// HTTP method.
    pub method: ApiMethod,
    /// Path relative to the configured Harvest API mount.
    pub path: String,
    /// Optional JSON request body.
    pub body: Option<Value>,
}

/// CLI failure modes.
#[derive(Debug, Error)]
pub enum CliError {
    /// A supplied JSON string could not be parsed.
    #[error("invalid JSON for {label}: {source}")]
    InvalidJson {
        /// User-facing source label.
        label: &'static str,
        /// JSON parser error.
        source: serde_json::Error,
    },

    /// A supplied JSON file could not be read.
    #[error("failed to read {label} from {path}: {source}")]
    ReadJson {
        /// User-facing source label.
        label: &'static str,
        /// Path displayed to the user.
        path: String,
        /// I/O failure.
        source: std::io::Error,
    },

    /// Both inline and file JSON sources were supplied for one field.
    #[error("{label} accepts either inline JSON or a file, not both")]
    ConflictingJsonSources {
        /// User-facing source label.
        label: &'static str,
    },

    /// HTTP transport failed.
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),

    /// The Harvest API returned a non-success status.
    #[error("harvest API returned {status}: {body}")]
    Http {
        /// HTTP status code.
        status: reqwest::StatusCode,
        /// Response body text.
        body: String,
    },

    /// API response JSON could not be parsed.
    #[error("failed to parse response JSON: {0}")]
    ParseResponse(serde_json::Error),

    /// JSON output could not be serialized.
    #[error("failed to serialize response JSON: {0}")]
    SerializeResponse(serde_json::Error),

    /// A `--search-attr` flag was missing the `=` separator.
    #[error("invalid --search-attr '{value}': expected 'key=value'")]
    InvalidSearchAttr {
        /// Original CLI argument value.
        value: String,
    },
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check management API health.
    Health,
    /// Manage workflow executions.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Manage DAG schedules and runs.
    Dag {
        #[command(subcommand)]
        command: DagCommand,
    },
    /// Manage workflow and DAG schedules (issue #91).
    #[command(alias = "schedules")]
    Schedule {
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    /// Manage dead-lettered tasks.
    #[command(alias = "dead-letter", alias = "dead-letters")]
    Dlq {
        #[command(subcommand)]
        command: DeadLetterCommand,
    },
    /// Retention janitor operations.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    /// Inspect cluster-wide per-activity concurrency caps.
    Concurrency {
        #[command(subcommand)]
        command: ConcurrencyCommand,
    },
    /// Manage batch operations.
    Batch {
        #[command(subcommand)]
        command: BatchCommand,
    },
    /// Browse the management API audit trail.
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
    /// Open the TUI dashboard to monitor workflows.
    Tui,
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
    /// List audit records, newest first.
    List {
        /// Filter by operator identity.
        #[arg(long)]
        actor: Option<String>,
        /// Filter by operation name (e.g. `workflow.start`, `dlq.replay`).
        #[arg(long)]
        operation: Option<String>,
        /// Filter by target type (e.g. `workflow`, `schedule`, `dead_letter`).
        #[arg(long)]
        target_type: Option<String>,
        /// Filter by target ID (execution ID, schedule name, DLQ entry ID, …).
        #[arg(long)]
        target_id: Option<String>,
        /// Filter by outcome: `succeeded` or `failed`.
        #[arg(long)]
        status: Option<String>,
        /// Lower bound (inclusive), RFC 3339 (e.g. `2026-01-01T00:00:00Z`).
        #[arg(long)]
        since: Option<String>,
        /// Upper bound (exclusive), RFC 3339.
        #[arg(long)]
        before: Option<String>,
        /// Maximum number of records to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// List workflow executions.
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
        /// Filter by workflow execution state. Repeat the flag or pass a
        /// comma-separated list to match any of several states.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Filter by registered workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by a `search_attrs` key/value pair (`key=value`). Repeat to
        /// AND multiple predicates together.
        #[arg(long = "search-attr", value_name = "KEY=VALUE")]
        search_attr: Vec<String>,
    },
    /// Get one workflow execution and event history.
    Get {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Show what a workflow is currently waiting on.
    Stack {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// List child workflow executions for a parent execution.
    Children {
        /// Parent workflow execution ID.
        execution_id: String,
        /// Filter by child workflow status. Repeat the flag or pass a
        /// comma-separated list to match any of several statuses.
        #[arg(long, value_delimiter = ',')]
        status: Vec<String>,
        /// Filter by registered child workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Opaque pagination cursor returned by the previous response.
        #[arg(long)]
        cursor: Option<String>,
        /// Recursive descent depth; 0 returns direct children only.
        #[arg(long, value_parser = clap::value_parser!(u8).range(0..=5))]
        depth: Option<u8>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Start a workflow execution.
    Start {
        /// Registered workflow name.
        workflow_name: String,
        /// Stable workflow ID for idempotent starts.
        #[arg(long)]
        workflow_id: Option<String>,
        /// Queue to place the initial workflow task on.
        #[arg(long)]
        queue: Option<String>,
        /// Inline JSON workflow input.
        #[arg(long, conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON workflow input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// Inline JSON memo.
        #[arg(long, conflicts_with = "memo_file")]
        memo_json: Option<String>,
        /// File containing JSON memo. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "memo_json")]
        memo_file: Option<PathBuf>,
        /// Inline JSON search attributes.
        #[arg(long, conflicts_with = "search_attrs_file")]
        search_attrs_json: Option<String>,
        /// File containing JSON search attributes. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "search_attrs_json")]
        search_attrs_file: Option<PathBuf>,
        /// Execution timeout in seconds.
        #[arg(long)]
        execution_timeout_secs: Option<i64>,
        /// How to handle a duplicate `(workflow_name, workflow_id)` start.
        /// One of: `allow_duplicate` (default), `reject_duplicate`,
        /// `allow_duplicate_failed_only`, `terminate_if_running`.
        #[arg(long, value_name = "POLICY")]
        reuse_policy: Option<String>,
    },
    /// Cancel a workflow execution.
    Cancel {
        /// Workflow execution ID.
        execution_id: String,
        /// Cancellation reason.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Fork a workflow execution at an event boundary.
    Reset {
        /// Workflow execution ID.
        execution_id: String,
        /// Last event ID to carry into the fork.
        #[arg(long = "to-event")]
        reset_to_event_id: i64,
        /// Recovery reason recorded in reset marker events.
        #[arg(long)]
        reason: String,
        /// Operator identity recorded in reset marker events.
        #[arg(long, default_value = "cli")]
        operator_id: String,
        /// How to handle undelivered source signals.
        #[arg(long, value_enum, default_value = "drop")]
        signal_reapply: ResetSignalReapply,
        /// Validate and print the reset plan without committing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Send a signal to a workflow execution.
    Signal {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered signal name.
        signal_name: String,
        /// Inline JSON signal payload.
        #[arg(long, conflicts_with = "payload_file")]
        payload_json: Option<String>,
        /// File containing JSON signal payload. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "payload_json")]
        payload_file: Option<PathBuf>,
    },
    /// Query workflow state.
    Query {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered query name.
        query_name: String,
    },
    /// Send a synchronous update request to a running workflow.
    Update {
        /// Workflow execution ID.
        execution_id: String,
        /// Registered update handler name.
        update_name: String,
        /// Inline JSON input for the update handler.
        #[arg(long, conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// How long to wait for the result.
        /// `admitted` — return immediately after durable admission (202).
        /// `completed` — block until the handler returns (default).
        #[arg(long, value_name = "MODE", default_value = "completed")]
        wait: String,
        /// Timeout in seconds when `--wait completed` (default: 30).
        #[arg(long, value_name = "SECS")]
        timeout_secs: Option<u64>,
    },
    /// Look up the durable result of a previously admitted update.
    UpdateResult {
        /// Workflow execution ID.
        execution_id: String,
        /// Update ID returned by a prior `harvest workflow update` call.
        update_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum DagCommand {
    /// List DAG schedules.
    List,
    /// List runs for a DAG.
    Runs {
        /// Registered DAG name.
        dag_name: String,
    },
    /// Trigger a DAG run.
    Trigger {
        /// Registered DAG name.
        dag_name: String,
        /// Inline JSON DAG run config.
        #[arg(long, conflicts_with = "conf_file")]
        conf_json: Option<String>,
        /// File containing JSON DAG run config. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "conf_json")]
        conf_file: Option<PathBuf>,
    },
    /// Pause a DAG schedule.
    Pause {
        /// Registered DAG name.
        dag_name: String,
    },
    /// Unpause a DAG schedule.
    Unpause {
        /// Registered DAG name.
        dag_name: String,
    },
}

#[derive(Debug, Subcommand)]
enum ScheduleCommand {
    /// List all schedules (DAG and workflow), tagged with kind.
    List,
    /// Create or update a workflow schedule.
    CreateWorkflow {
        /// Registered workflow name to schedule.
        #[arg(long)]
        name: String,
        /// Cron expression (e.g. `"0 3 * * *"`) or `"interval:<secs>"`.
        #[arg(long)]
        cron: String,
        /// Inline JSON input passed to each scheduled run.
        #[arg(long, value_name = "JSON", conflicts_with = "input_file")]
        input_json: Option<String>,
        /// File containing JSON input. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "input_json")]
        input_file: Option<PathBuf>,
        /// Maximum concurrent in-flight runs (default: 1).
        #[arg(long, default_value_t = 1)]
        max_active_runs: u32,
        /// Backfill missed runs when the scheduler was down.
        #[arg(long)]
        catchup: bool,
        /// Create the schedule in a paused state.
        #[arg(long)]
        paused: bool,
    },
    /// Pause a schedule (works for both DAG and workflow schedules).
    Pause {
        /// Schedule row ID (UUID).
        id: String,
    },
    /// Resume a paused schedule.
    Resume {
        /// Schedule row ID (UUID).
        id: String,
    },
    /// Delete a schedule.
    Delete {
        /// Schedule row ID (UUID).
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum RetentionCommand {
    /// Show retention config and last tick results.
    Status,
    /// Trigger a retention tick immediately.
    RunNow,
}

#[derive(Debug, Subcommand)]
enum ConcurrencyCommand {
    /// Show per-key concurrency stats: cap, in-flight, and pending counts.
    Status,
}

#[derive(Debug, Subcommand)]
enum BatchCommand {
    /// List batch operations.
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
    },
    /// Get details of a batch operation.
    Get {
        /// Batch operation ID.
        batch_job_id: String,
    },
    /// Submit a new batch operation.
    Submit {
        /// Action to perform: Cancel, Terminate, or Signal.
        #[arg(value_parser = clap::builder::PossibleValuesParser::new(["Cancel", "Terminate", "Signal"]))]
        action: String,
        /// Inline JSON filter definition.
        #[arg(long, conflicts_with = "filter_file")]
        filter_json: Option<String>,
        /// File containing JSON filter definition. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "filter_json")]
        filter_file: Option<PathBuf>,
        /// Name of the signal (required if action is Signal).
        #[arg(long, required_if_eq("action", "Signal"))]
        signal_name: Option<String>,
        /// Inline JSON signal payload.
        #[arg(long, conflicts_with = "signal_payload_file")]
        signal_payload_json: Option<String>,
        /// File containing JSON signal payload. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "signal_payload_json")]
        signal_payload_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum DeadLetterCommand {
    /// List dead-lettered tasks.
    List {
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=200))]
        limit: Option<i64>,
    },
    /// Replay a single dead-lettered task by ID.
    Replay {
        /// Dead-letter row ID.
        dead_letter_id: String,
    },
    /// Bulk-replay dead-lettered tasks matching a filter.
    ///
    /// At least one filter criterion must be provided. Use --dry-run to preview
    /// matching rows without performing any writes.
    BulkReplay {
        /// Exact match on activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Exact match on workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any writes.
        #[arg(long)]
        dry_run: bool,
    },
    /// Bulk-discard dead-lettered tasks matching a filter (delete without replay).
    ///
    /// At least one filter criterion must be provided. Use --dry-run to preview
    /// matching rows without performing any deletes.
    BulkDiscard {
        /// Exact match on activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Exact match on workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any deletes.
        #[arg(long)]
        dry_run: bool,
    },
}

impl Cli {
    /// Build the management API request represented by these CLI arguments.
    ///
    /// # Errors
    ///
    /// Returns an error when inline JSON cannot be parsed or JSON file/stdin
    /// input cannot be read.
    pub fn api_request(&self) -> Result<ApiRequest, CliError> {
        match &self.command {
            Commands::Health => Ok(ApiRequest::get("/health")),
            Commands::Workflow { command } => workflow_request(command),
            Commands::Dag { command } => dag_request(command),
            Commands::Schedule { command } => schedule_request(command),
            Commands::Dlq { command } => Ok(dead_letter_request(command)),
            Commands::Retention { command } => Ok(retention_request(command)),
            Commands::Concurrency { command } => Ok(concurrency_request(command)),
            Commands::Batch { command } => batch_request(command),
            Commands::Audit { command } => Ok(audit_request(command)),
            Commands::Tui => unreachable!("Tui command handles its own requests"),
        }
    }
}

impl ApiRequest {
    fn get(path: impl Into<String>) -> Self {
        Self {
            method: ApiMethod::Get,
            path: path.into(),
            body: None,
        }
    }

    fn patch(path: impl Into<String>, body: Value) -> Self {
        Self {
            method: ApiMethod::Patch,
            path: path.into(),
            body: Some(body),
        }
    }

    fn post(path: impl Into<String>, body: Option<Value>) -> Self {
        Self {
            method: ApiMethod::Post,
            path: path.into(),
            body,
        }
    }
}

pub mod tui;

/// Run the CLI, print successful response data to stdout, and return errors.
///
/// # Errors
///
/// Returns an error if request construction, HTTP transport, response parsing,
/// or response formatting fails.
pub async fn run_cli(cli: Cli) -> Result<(), CliError> {
    if matches!(cli.command, Commands::Tui) {
        return tui::run_tui(&cli).await;
    }

    let response = execute(&cli).await?;
    let rendered = render_response(&cli, &response)?;
    println!("{rendered}");
    Ok(())
}

/// Execute the API request represented by the CLI arguments.
///
/// # Errors
///
/// Returns an error if request construction fails, the HTTP request fails, the
/// API returns a non-success status, or the response body is not valid JSON.
pub async fn execute(cli: &Cli) -> Result<Value, CliError> {
    let request = cli.api_request()?;
    let client = reqwest::Client::new();
    let url = format!("{}{}", cli.base_url.trim_end_matches('/'), request.path);
    let builder = match request.method {
        ApiMethod::Get => client.get(url),
        ApiMethod::Patch => client.patch(url),
        ApiMethod::Post => client.post(url),
        ApiMethod::Delete => client.delete(url),
    };
    let builder = if let Some(token) = &cli.token {
        builder.bearer_auth(token)
    } else {
        builder
    };
    // Mutating requests identify the CLI as the call source and carry the
    // operator identity and correlation id when supplied.
    let builder = if request.method == ApiMethod::Get {
        builder
    } else {
        let mut b = builder.header("x-harvest-source", "cli");
        if let Some(actor) = &cli.actor {
            b = b.header("x-harvest-actor", actor);
        }
        if let Some(rid) = &cli.request_id {
            b = b.header("x-request-id", rid);
        }
        b
    };
    let builder = if let Some(body) = &request.body {
        builder.json(body)
    } else {
        builder
    };

    let response = builder.send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(CliError::Http { status, body });
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }

    serde_json::from_str(&body).map_err(CliError::ParseResponse)
}

/// Render a successful response.
///
/// # Errors
///
/// Returns an error if the JSON value cannot be serialized.
pub fn format_output(value: &Value, output: OutputFormat) -> Result<String, CliError> {
    match output {
        OutputFormat::PrettyJson => {
            serde_json::to_string_pretty(value).map_err(CliError::SerializeResponse)
        }
        OutputFormat::Json => serde_json::to_string(value).map_err(CliError::SerializeResponse),
    }
}

fn render_response(cli: &Cli, value: &Value) -> Result<String, CliError> {
    if workflow_children_wants_table(cli) {
        return Ok(format_workflow_children_table(value));
    }
    if audit_list_wants_table(cli) {
        return Ok(format_audit_table(value));
    }

    let output = if workflow_children_wants_raw_json(cli) {
        OutputFormat::Json
    } else {
        cli.output
    };
    format_output(value, output)
}

fn audit_list_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Audit {
            command: AuditCommand::List { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn workflow_children_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Children { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn workflow_children_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Children { json: true, .. }
        }
    )
}

fn format_workflow_children_table(value: &Value) -> String {
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return "No child workflows found.".to_string();
    };
    if items.is_empty() {
        return "No child workflows found.".to_string();
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "DEPTH".to_string(),
        "EXEC ID".to_string(),
        "WORKFLOW".to_string(),
        "STATUS".to_string(),
        "STARTED".to_string(),
        "COMPLETED".to_string(),
        "SHARD".to_string(),
        "ERROR".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_number(item.get("depth")),
            cell_str(item.get("exec_id")),
            cell_str(item.get("workflow_name")),
            cell_str(item.get("status")),
            cell_str(item.get("started_at")),
            cell_optional_str(item.get("completed_at")),
            cell_number(item.get("shard_id")),
            cell_optional_str(item.get("error_summary")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let mut rendered = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");

    if let Some(cursor) = value.get("next_cursor").and_then(Value::as_str) {
        rendered.push_str("\nnext_cursor: ");
        rendered.push_str(cursor);
    }
    rendered
}

fn format_audit_table(value: &Value) -> String {
    let Some(items) = value.as_array() else {
        return "No audit records found.".to_string();
    };
    if items.is_empty() {
        return "No audit records found.".to_string();
    }

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "OCCURRED_AT".to_string(),
        "ACTOR".to_string(),
        "OPERATION".to_string(),
        "TARGET".to_string(),
        "STATUS".to_string(),
        "SRC".to_string(),
        "ERROR".to_string(),
    ]);
    for item in items {
        let target = match (
            item.get("target_type").and_then(Value::as_str),
            item.get("target_id").and_then(Value::as_str),
        ) {
            (Some(tt), Some(tid)) => format!("{tt}:{tid}"),
            (Some(tt), None) => tt.to_string(),
            _ => String::new(),
        };
        rows.push(vec![
            cell_str(item.get("occurred_at")),
            cell_str(item.get("actor")),
            cell_str(item.get("operation")),
            target,
            cell_str(item.get("status")),
            cell_str(item.get("source")),
            cell_optional_str(item.get("error_summary")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(col, cell)| format!("{cell:<width$}", width = widths[col]))
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn cell_str(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or("").to_string()
}

fn cell_optional_str(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or("-").to_string()
}

fn cell_number(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_i64)
        .map_or_else(String::new, |number| number.to_string())
}

#[allow(clippy::too_many_lines)]
fn workflow_request(command: &WorkflowCommand) -> Result<ApiRequest, CliError> {
    match command {
        WorkflowCommand::List {
            limit,
            state,
            workflow_name,
            search_attr,
        } => Ok(ApiRequest::get(build_workflow_list_path(
            *limit,
            state,
            workflow_name.as_deref(),
            search_attr,
        )?)),
        WorkflowCommand::Get { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Stack { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/stack",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Children {
            execution_id,
            status,
            workflow_name,
            limit,
            cursor,
            depth,
            json: _,
        } => Ok(ApiRequest::get(build_workflow_children_path(
            execution_id,
            status,
            workflow_name.as_deref(),
            *limit,
            cursor.as_deref(),
            *depth,
        ))),
        WorkflowCommand::Start {
            workflow_name,
            workflow_id,
            queue,
            input_json,
            input_file,
            memo_json,
            memo_file,
            search_attrs_json,
            search_attrs_file,
            execution_timeout_secs,
            reuse_policy,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "workflow_id", workflow_id.as_deref());
            insert_string(&mut body, "queue", queue.as_deref());
            insert_json(
                &mut body,
                "input",
                parse_json_source(
                    input_json.as_deref(),
                    input_file.as_deref(),
                    "workflow input",
                )?,
            );
            insert_json(
                &mut body,
                "memo",
                parse_json_source(memo_json.as_deref(), memo_file.as_deref(), "memo")?,
            );
            insert_json(
                &mut body,
                "search_attrs",
                parse_json_source(
                    search_attrs_json.as_deref(),
                    search_attrs_file.as_deref(),
                    "search attributes",
                )?,
            );
            if let Some(timeout) = execution_timeout_secs {
                body.insert("execution_timeout_secs".to_string(), json!(timeout));
            }
            insert_string(&mut body, "reuse_policy", reuse_policy.as_deref());

            Ok(ApiRequest::post(
                format!("/workflows/{}/start", path_segment(workflow_name)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Cancel {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/cancel", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Reset {
            execution_id,
            reset_to_event_id,
            reason,
            operator_id,
            signal_reapply,
            dry_run,
        } => {
            let mut body = Map::new();
            body.insert("reset_to_event_id".to_string(), json!(reset_to_event_id));
            body.insert("reason".to_string(), Value::String(reason.clone()));
            body.insert(
                "operator_id".to_string(),
                Value::String(operator_id.clone()),
            );
            body.insert(
                "signal_reapply".to_string(),
                Value::String(signal_reapply.as_wire().to_string()),
            );
            let suffix = if *dry_run { "?dry_run=true" } else { "" };
            Ok(ApiRequest::post(
                format!("/workflows/{}/reset{suffix}", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Signal {
            execution_id,
            signal_name,
            payload_json,
            payload_file,
        } => {
            let payload = parse_json_source(
                payload_json.as_deref(),
                payload_file.as_deref(),
                "signal payload",
            )?
            .unwrap_or_else(|| json!({}));
            Ok(ApiRequest::post(
                format!(
                    "/workflows/{}/signal/{}",
                    path_segment(execution_id),
                    path_segment(signal_name)
                ),
                Some(payload),
            ))
        }
        WorkflowCommand::Query {
            execution_id,
            query_name,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/query/{}",
            path_segment(execution_id),
            path_segment(query_name)
        ))),
        WorkflowCommand::Update {
            execution_id,
            update_name,
            input_json,
            input_file,
            wait,
            timeout_secs,
        } => {
            let input =
                parse_json_source(input_json.as_deref(), input_file.as_deref(), "update input")?
                    .unwrap_or(serde_json::Value::Null);
            let path = timeout_secs.map_or_else(
                || {
                    format!(
                        "/workflows/{}/update/{}?wait={}",
                        path_segment(execution_id),
                        path_segment(update_name),
                        wait,
                    )
                },
                |secs| {
                    format!(
                        "/workflows/{}/update/{}?wait={}&timeout_secs={secs}",
                        path_segment(execution_id),
                        path_segment(update_name),
                        wait,
                    )
                },
            );
            Ok(ApiRequest::post(path, Some(json!({ "input": input }))))
        }
        WorkflowCommand::UpdateResult {
            execution_id,
            update_id,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/update/{}/result",
            path_segment(execution_id),
            path_segment(update_id),
        ))),
    }
}

fn dag_request(command: &DagCommand) -> Result<ApiRequest, CliError> {
    match command {
        DagCommand::List => Ok(ApiRequest::get("/dags")),
        DagCommand::Runs { dag_name } => Ok(ApiRequest::get(format!(
            "/dags/{}/runs",
            path_segment(dag_name)
        ))),
        DagCommand::Trigger {
            dag_name,
            conf_json,
            conf_file,
        } => {
            let mut body = Map::new();
            insert_json(
                &mut body,
                "conf",
                parse_json_source(conf_json.as_deref(), conf_file.as_deref(), "DAG run config")?,
            );
            Ok(ApiRequest::post(
                format!("/dags/{}/trigger", path_segment(dag_name)),
                Some(Value::Object(body)),
            ))
        }
        DagCommand::Pause { dag_name } => Ok(ApiRequest::patch(
            format!("/dags/{}", path_segment(dag_name)),
            json!({ "paused": true }),
        )),
        DagCommand::Unpause { dag_name } => Ok(ApiRequest::patch(
            format!("/dags/{}", path_segment(dag_name)),
            json!({ "paused": false }),
        )),
    }
}

fn schedule_request(command: &ScheduleCommand) -> Result<ApiRequest, CliError> {
    match command {
        ScheduleCommand::List => Ok(ApiRequest::get("/admin/schedules")),
        ScheduleCommand::CreateWorkflow {
            name,
            cron,
            input_json,
            input_file,
            max_active_runs,
            catchup,
            paused,
        } => {
            let mut body = Map::new();
            body.insert("workflow_name".to_string(), Value::String(name.clone()));
            body.insert("schedule_expr".to_string(), Value::String(cron.clone()));
            body.insert("max_active_runs".to_string(), json!(max_active_runs));
            body.insert("catchup".to_string(), json!(catchup));
            body.insert("paused".to_string(), json!(paused));
            if let Some(input) =
                parse_json_source(input_json.as_deref(), input_file.as_deref(), "input")?
            {
                body.insert("input".to_string(), input);
            }
            Ok(ApiRequest::post(
                "/admin/schedules/workflow",
                Some(Value::Object(body)),
            ))
        }
        ScheduleCommand::Pause { id } => Ok(ApiRequest::post(
            format!("/admin/schedules/{}/pause", path_segment(id)),
            None,
        )),
        ScheduleCommand::Resume { id } => Ok(ApiRequest::post(
            format!("/admin/schedules/{}/resume", path_segment(id)),
            None,
        )),
        ScheduleCommand::Delete { id } => {
            // DELETE — use a dedicated ApiMethod variant or reuse Post with a
            // special path. Since ApiMethod only has Get/Patch/Post and adding
            // Delete would require more changes, we'll represent it as a Post
            // to a /delete path.
            // Actually, let's add Delete to ApiMethod.
            Ok(ApiRequest {
                method: ApiMethod::Delete,
                path: format!("/admin/schedules/{}", path_segment(id)),
                body: None,
            })
        }
    }
}

fn retention_request(command: &RetentionCommand) -> ApiRequest {
    match command {
        RetentionCommand::Status => ApiRequest::get("/admin/retention"),
        RetentionCommand::RunNow => ApiRequest::post("/admin/retention/run-now", None),
    }
}

fn concurrency_request(command: &ConcurrencyCommand) -> ApiRequest {
    match command {
        ConcurrencyCommand::Status => ApiRequest::get("/admin/concurrency"),
    }
}

fn audit_request(command: &AuditCommand) -> ApiRequest {
    match command {
        AuditCommand::List {
            actor,
            operation,
            target_type,
            target_id,
            status,
            since,
            before,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(v) = actor {
                params.push(("actor", v.clone()));
            }
            if let Some(v) = operation {
                params.push(("operation", v.clone()));
            }
            if let Some(v) = target_type {
                params.push(("target_type", v.clone()));
            }
            if let Some(v) = target_id {
                params.push(("target_id", v.clone()));
            }
            if let Some(v) = status {
                params.push(("status", v.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = before {
                params.push(("before", v.clone()));
            }
            if let Some(v) = limit {
                params.push(("limit", v.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/admin/audit");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/admin/audit?{qs}"))
        }
    }
}

fn batch_request(command: &BatchCommand) -> Result<ApiRequest, CliError> {
    match command {
        BatchCommand::List { limit } => Ok(ApiRequest::get(path_with_limit(
            "/batch-operations",
            limit.map(|value| ("limit", value)),
        ))),
        BatchCommand::Get { batch_job_id } => Ok(ApiRequest::get(format!(
            "/batch-operations/{}",
            path_segment(batch_job_id)
        ))),
        BatchCommand::Submit {
            action,
            filter_json,
            filter_file,
            signal_name,
            signal_payload_json,
            signal_payload_file,
        } => {
            let filter = parse_json_source(
                filter_json.as_deref(),
                filter_file.as_deref(),
                "filter JSON",
            )?
            .unwrap_or_else(|| json!({}));
            let mut body = Map::new();
            body.insert("action".to_string(), json!(action));
            body.insert("filter".to_string(), filter);
            if let Some(sn) = signal_name {
                body.insert("signal_name".to_string(), json!(sn));
            }
            if let Some(payload) = parse_json_source(
                signal_payload_json.as_deref(),
                signal_payload_file.as_deref(),
                "signal payload JSON",
            )? {
                body.insert("signal_payload".to_string(), payload);
            }
            Ok(ApiRequest::post(
                "/batch-operations",
                Some(Value::Object(body)),
            ))
        }
    }
}

fn dead_letter_request(command: &DeadLetterCommand) -> ApiRequest {
    match command {
        DeadLetterCommand::List { limit } => ApiRequest::get(path_with_limit(
            "/dead-letters",
            limit.map(|value| ("limit", value)),
        )),
        DeadLetterCommand::Replay { dead_letter_id } => ApiRequest::post(
            format!("/dead-letters/{}/replay", path_segment(dead_letter_id)),
            None,
        ),
        DeadLetterCommand::BulkReplay {
            activity_name,
            workflow_name,
            failed_after,
            failed_before,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/replay",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                failed_after.as_deref(),
                failed_before.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
        DeadLetterCommand::BulkDiscard {
            activity_name,
            workflow_name,
            failed_after,
            failed_before,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/discard",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                failed_after.as_deref(),
                failed_before.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
    }
}

fn build_bulk_dlq_body(
    activity_name: Option<&str>,
    workflow_name: Option<&str>,
    failed_after: Option<&str>,
    failed_before: Option<&str>,
    limit: Option<u32>,
    dry_run: bool,
) -> Value {
    let mut body = Map::new();
    insert_string(&mut body, "activity_name", activity_name);
    insert_string(&mut body, "workflow_name", workflow_name);
    insert_string(&mut body, "failed_after", failed_after);
    insert_string(&mut body, "failed_before", failed_before);
    if let Some(l) = limit {
        body.insert("limit".to_string(), json!(l));
    }
    if dry_run {
        body.insert("dry_run".to_string(), json!(true));
    }
    Value::Object(body)
}

fn parse_json_source(
    inline: Option<&str>,
    file: Option<&Path>,
    label: &'static str,
) -> Result<Option<Value>, CliError> {
    match (inline, file) {
        (Some(_), Some(_)) => Err(CliError::ConflictingJsonSources { label }),
        (Some(raw), None) => serde_json::from_str(raw)
            .map(Some)
            .map_err(|source| CliError::InvalidJson { label, source }),
        (None, Some(path)) => {
            let raw = read_json_file(path, label)?;
            serde_json::from_str(&raw)
                .map(Some)
                .map_err(|source| CliError::InvalidJson { label, source })
        }
        (None, None) => Ok(None),
    }
}

fn read_json_file(path: &Path, label: &'static str) -> Result<String, CliError> {
    if path == Path::new("-") {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .map_err(|source| CliError::ReadJson {
                label,
                path: "-".to_string(),
                source,
            })?;
        return Ok(input);
    }

    fs::read_to_string(path).map_err(|source| CliError::ReadJson {
        label,
        path: path.display().to_string(),
        source,
    })
}

fn insert_string(body: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        body.insert(key.to_string(), Value::String(value.to_string()));
    }
}

fn insert_json(body: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(key.to_string(), value);
    }
}

fn path_segment(raw: &str) -> String {
    utf8_percent_encode(raw, PATH_SEGMENT_ENCODE_SET).to_string()
}

fn path_with_limit(base: &str, limit: Option<(&str, i64)>) -> String {
    let Some((key, value)) = limit else {
        return base.to_string();
    };

    let mut query = BTreeMap::new();
    query.insert(key, value.to_string());
    let query = query
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{query}")
}

fn build_workflow_list_path(
    limit: Option<i64>,
    states: &[String],
    workflow_name: Option<&str>,
    search_attrs: &[String],
) -> Result<String, CliError> {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if !states.is_empty() {
        // Use comma-separated values to match the management API's documented
        // canonical form. The server also accepts repeated `state=` params.
        params.push(("state", states.join(",")));
    }
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    for raw in search_attrs {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| CliError::InvalidSearchAttr { value: raw.clone() })?;
        params.push(("search_attr", format!("{key}:{value}")));
    }

    if params.is_empty() {
        return Ok("/workflows".to_string());
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("/workflows?{encoded}"))
}

fn build_workflow_children_path(
    execution_id: &str,
    statuses: &[String],
    workflow_name: Option<&str>,
    limit: Option<i64>,
    cursor: Option<&str>,
    depth: Option<u8>,
) -> String {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    for status in statuses {
        params.push(("status", status.clone()));
    }
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if let Some(value) = cursor {
        params.push(("cursor", value.to_string()));
    }
    if let Some(value) = depth {
        params.push(("depth", value.to_string()));
    }

    let base = format!("/workflows/{}/children", path_segment(execution_id));
    if params.is_empty() {
        return base;
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{base}?{encoded}")
}

fn query_encode(input: &str) -> String {
    // RFC 3986 query-component encoding. We intentionally leave `:` unencoded
    // so the management API sees `search_attr=key:value` as a stable shape.
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':' | b',') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod reuse_policy_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn start_request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn start_omitting_reuse_policy_sends_no_field() {
        let req = start_request(&["workflow", "start", "my_wf"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert!(
            body.get("reuse_policy").is_none(),
            "omitting --reuse-policy must not send the field"
        );
    }

    #[test]
    fn start_allow_duplicate_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "allow_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "allow_duplicate");
    }

    #[test]
    fn start_reject_duplicate_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "reject_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "reject_duplicate");
    }

    #[test]
    fn start_allow_duplicate_failed_only_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "allow_duplicate_failed_only",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "allow_duplicate_failed_only");
    }

    #[test]
    fn start_terminate_if_running_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--reuse-policy",
            "terminate_if_running",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["reuse_policy"], "terminate_if_running");
    }

    #[test]
    fn start_preserves_other_fields_alongside_reuse_policy() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--workflow-id",
            "wf-123",
            "--reuse-policy",
            "reject_duplicate",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["workflow_id"], "wf-123");
        assert_eq!(body["reuse_policy"], "reject_duplicate");
    }

    #[test]
    fn children_default_output_renders_human_table() {
        let cli = parse(&[
            "workflow",
            "children",
            "00000000-0000-0000-0000-000000000001",
        ]);
        let payload = json!({
            "items": [{
                "exec_id": "00000000-0000-0000-0000-000000000002",
                "workflow_name": "billing_child",
                "status": "Failed",
                "started_at": "2026-05-04T12:00:00Z",
                "completed_at": null,
                "error_summary": "charge card failed",
                "shard_id": 1,
                "depth": 0
            }],
            "next_cursor": null
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("EXEC ID"));
        assert!(rendered.contains("billing_child"));
        assert!(rendered.contains("charge card failed"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn children_json_flag_renders_raw_payload() {
        let cli = parse(&[
            "workflow",
            "children",
            "00000000-0000-0000-0000-000000000001",
            "--json",
        ]);
        let payload = json!({
            "items": [],
            "next_cursor": null
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(rendered, r#"{"items":[],"next_cursor":null}"#);
    }
}
