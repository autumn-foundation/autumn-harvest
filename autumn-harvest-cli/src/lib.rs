//! Command-line client for the autumn-harvest management API.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use autumn_harvest::{DetCheckReport, DetSeverity, check_paths};
use clap::{Parser, Subcommand, ValueEnum};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde_json::{Map, Value, json};
use thiserror::Error;

const DEFAULT_BASE_URL: &str = "http://localhost:3000/api/harvest";
/// Characters percent-encoded when a caller-supplied value becomes one URL path
/// segment.
///
/// `/` and `\` are both here because the URL parser reqwest uses treats **both**
/// as path separators for special (http/https) URLs, so either one would split a
/// single value into extra segments and silently retarget the request at a
/// different route — and `\` additionally re-enables `..` traversal inside what
/// should be one opaque segment (`payments\..\admin` resolved to
/// `/admin/queues/admin/pause`). `%` is here so a caller-supplied `%2e` cannot
/// become a dot-segment; the LITERAL `.`/`..` forms cannot be encoded away at
/// all and are rejected instead — see `is_url_dot_segment`.
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
    .add(b'/')
    .add(b'\\');

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

/// Output format for the `det-check` subcommand (issue #778).
///
/// This is a **local** flag (`--format`); it is deliberately distinct from the
/// global `--output` used for API responses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum DetCheckFormat {
    /// Human-readable one-line-per-finding text (default).
    #[default]
    Text,
    /// Machine-readable `DetCheckReport` JSON for CI consumption.
    Json,
}

/// Project template flavour for `harvest new` (issue #692).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ScaffoldTemplate {
    /// One `#[workflow]` calling one `#[activity]`, `HarvestPlugin` wiring, a
    /// `compose.yaml` Postgres, and a README with the exact run steps.
    #[default]
    Minimal,
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

/// Execution-state scope for version-gate usage reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum VersionUsageStateGroup {
    /// Include only executions that may still replay old branches.
    Active,
    /// Include only terminal executions.
    Terminal,
    /// Include active and terminal executions.
    All,
}

impl VersionUsageStateGroup {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::All => "all",
        }
    }
}

/// Payload policy for history exports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HistoryExportPayloadPolicy {
    /// Redact payload-bearing fields and emit deterministic summaries.
    Redacted,
    /// Emit raw payloads for private replay fixtures. Sensitive.
    Full,
}

impl HistoryExportPayloadPolicy {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Redacted => "redacted",
            Self::Full => "full",
        }
    }
}

/// Execution-state scope for batch history exports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HistoryExportStateGroup {
    /// Include only executions that can still run or replay.
    Active,
    /// Include only terminal executions.
    Terminal,
    /// Include active and terminal executions.
    All,
}

impl HistoryExportStateGroup {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::All => "all",
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

    /// Output could not be written to the requested file.
    #[error("failed to write output to {path}: {source}")]
    WriteOutput {
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

    /// A required input source (file or inline JSON) was not provided.
    #[error("{label}")]
    MissingInput {
        /// User-facing message.
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

    /// Preflight completed but reported a non-passing deploy-gate status.
    #[error("preflight overall_status={status}")]
    PreflightGate {
        /// Reported preflight status.
        status: String,
    },

    /// Shard health completed but the deploy gate found a non-ready target.
    #[error("shard health readiness gate failed")]
    ShardHealthGate,

    /// Replay canary completed but reported a non-passing verdict.
    #[error("replay canary gate failed: verdict={verdict}")]
    CanaryGate {
        /// Reported canary verdict.
        verdict: String,
    },

    /// A queue pause/resume was only partially applied across the fleet.
    #[error("queue mutation was only partially applied: {detail}")]
    QueuePartialMutation {
        /// Per-shard failure summary reported by the API.
        detail: String,
    },

    /// A queue name would be normalized away as a URL dot-segment.
    #[error(
        "invalid queue name '{value}': '.' and '..' are removed as dot-segments \
         when the request URL is parsed, which would silently retarget the \
         request at a different route"
    )]
    QueueNameDotSegment {
        /// Original CLI argument value.
        value: String,
    },

    /// Version-gate guard found active usage or incomplete shard inspection.
    #[error("version usage guard failed")]
    VersionUsageGate,

    /// Retirement check found active old-version executions or an unavailable shard.
    #[error("version-gate retirement check failed")]
    RetirementCheckGate,

    /// Workflow-type reachability found an orphaned type, incomplete report, or transport error.
    ///
    /// `context` carries either the original transport error (connection failure,
    /// auth error, server 5xx) so operators can distinguish infra misconfiguration
    /// from an unsafe-handler-removal verdict.
    #[error("workflow-type reachability gate failed: {context}")]
    WorkflowReachabilityGate {
        /// Human-readable cause: transport error string or verdict summary.
        context: String,
    },

    /// `--wait` timed out before the worker reached `Stopped`.
    #[error("timed out waiting for worker '{worker_id}' to stop (last status: {last_status})")]
    DrainWaitTimeout {
        /// Worker ID that did not reach `Stopped`.
        worker_id: String,
        /// Last observed lifecycle status.
        last_status: String,
    },

    /// The SSE event stream closed abnormally (e.g. slow consumer).
    #[error("SSE stream closed by server: {message}")]
    SseStreamError {
        /// Server-supplied error detail.
        message: String,
    },

    /// A CLI argument value was invalid (e.g. unrecognised scope format).
    #[error("{0}")]
    InvalidInput(String),

    /// `det-check` found determinism violations that fail the gate (issue #778).
    ///
    /// The findings themselves are already printed to stdout; this only carries
    /// the counts so `main` can exit with the right code. Exit code is `1`.
    #[error("det-check: {errors} hard-blocker finding(s), {warnings} warning(s)")]
    DetCheckFindings {
        /// Number of `Error`-severity findings.
        errors: usize,
        /// Number of `Warning`-severity findings.
        warnings: usize,
    },
}

impl CliError {
    /// Process exit code associated with this error.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::PreflightGate { status } if status == "warn" => 2,
            // Issue #520: the reachability gate uses exit code 2 specifically so
            // CI can distinguish "an orphaned/partial deploy hazard" from a
            // generic transport/usage failure (exit 1).
            Self::WorkflowReachabilityGate { .. } => 2,
            _ => 1,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check management API health.
    Health,
    /// Run read-only deployment readiness checks.
    Preflight,
    /// Inspect shard rollout readiness.
    Shard {
        #[command(subcommand)]
        command: ShardCommand,
    },
    /// Manage workflow executions.
    #[command(alias = "workflows")]
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Export workflow histories for replay fixtures and diagnostics.
    History {
        #[command(subcommand)]
        command: HistoryCommand,
    },
    /// Place or release a per-execution legal hold (issue #747).
    #[command(alias = "legal-holds")]
    LegalHold {
        #[command(subcommand)]
        command: LegalHoldCommand,
    },
    /// Inspect and resolve external activity handoffs.
    #[command(
        alias = "handoffs",
        alias = "external-handoff",
        alias = "external-handoffs"
    )]
    Handoff {
        #[command(subcommand)]
        command: HandoffCommand,
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
    /// Inspect and redrive durable completion-callback deliveries (issue #605).
    #[command(alias = "completion-deliveries", alias = "callbacks")]
    CompletionDelivery {
        #[command(subcommand)]
        command: CompletionDeliveryCommand,
    },
    /// Retention janitor operations.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    /// Hold or release dispatch on a named task queue (issue #619).
    #[command(alias = "queues")]
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Inspect cluster-wide per-activity concurrency caps.
    Concurrency {
        #[command(subcommand)]
        command: ConcurrencyCommand,
    },
    /// Manage per-activity rate limits.
    #[command(alias = "rate-limits")]
    RateLimit {
        #[command(subcommand)]
        command: RateLimitCommand,
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
    /// Manage admission gates for incident-response halts (issue #377).
    #[command(alias = "gates")]
    Gate {
        #[command(subcommand)]
        command: GateCommand,
    },
    /// Manage scoped API tokens for the management API (issue #942).
    #[command(alias = "tokens")]
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    /// Report per-tenant/per-workflow usage for chargeback and capacity planning (issue #596).
    ///
    /// The historical companion to `harvest concurrency status`. Renders a
    /// table by default; pass --json for piping.
    Usage {
        /// Inclusive lower bound of the aggregation window: RFC 3339 or a
        /// relative duration like 24h.
        #[arg(long)]
        from: String,
        /// Inclusive upper bound of the aggregation window: RFC 3339 or a
        /// relative duration like 24h.
        #[arg(long)]
        to: String,
        /// Grouping dimension: `workflow_name` (default) or
        /// `search_attr:<key>` (e.g. `search_attr:tenant_id`).
        #[arg(long = "group-by")]
        group_by: Option<String>,
        /// Emit raw JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Report recorded workflow version-gate usage.
    VersionUsage {
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by version-gate change id.
        #[arg(long)]
        change_id: Option<String>,
        /// Filter by recorded version.
        #[arg(long = "version", value_parser = clap::value_parser!(u32))]
        recorded_version: Option<u32>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<VersionUsageStateGroup>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Exit non-zero if any active execution still matches the filtered version gate.
        #[arg(long, requires = "change_id", requires = "recorded_version")]
        guard: bool,
    },
    /// Check whether a version-gate change id is safe to retire below a version threshold.
    ///
    /// Exits non-zero when `--check` is passed and any non-terminal execution still carries
    /// a recorded version below `--min-safe-version`, or when any shard is unavailable.
    #[command(alias = "version-gate-check")]
    VersionGateRetirement {
        /// Version-gate change id to inspect (required).
        #[arg(long)]
        change_id: String,
        /// Versions strictly below this value are considered old branches to retire.
        #[arg(long, value_parser = clap::value_parser!(u32))]
        min_safe_version: u32,
        /// Narrow results to this workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<VersionUsageStateGroup>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Exit non-zero while any non-terminal execution still uses an old version,
        /// or while any shard is unavailable.
        #[arg(long)]
        check: bool,
    },
    /// Inspect workflow-type handler reachability for safe handler removal (issue #520).
    #[command(name = "workflow-types", alias = "workflow-type")]
    WorkflowTypes {
        #[command(subcommand)]
        command: WorkflowTypesCommand,
    },
    /// Open the TUI dashboard to monitor workflows.
    Tui,
    /// Inspect and drain worker fleet (issue #170).
    #[command(alias = "workers")]
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    /// Stream live workflow execution events.
    #[command(alias = "event")]
    Events {
        #[command(subcommand)]
        command: EventsCommand,
    },
    /// Start N workflow executions in one batched request (issue #357).
    ///
    /// Reads newline-delimited JSON (NDJSON) items from a file or inline JSON
    /// array and submits them as a single `POST /workflows/batch_start` call.
    ///
    /// Each NDJSON line must be a JSON object with at least a `workflow_name`
    /// key.  Optional keys: `workflow_id`, `input`, `search_attributes`,
    /// `idempotency_key`.
    ///
    /// Exits non-zero when `--atomic` is set and any item is rejected.
    #[command(name = "start-batch")]
    StartBatch {
        /// NDJSON file of workflow start items. Use `-` to read from stdin.
        ///
        /// Conflicts with `--items-json`.
        #[arg(long, value_name = "PATH", conflicts_with = "items_json")]
        file: Option<PathBuf>,
        /// Inline JSON array of workflow start items.
        ///
        /// Conflicts with `--file`.
        #[arg(long, conflicts_with = "file")]
        items_json: Option<String>,
        /// Require all-or-nothing semantics: if any item fails validation the
        /// entire batch is rejected with no executions inserted.
        #[arg(long, default_value_t = false)]
        atomic: bool,
    },
    /// Run a deploy-time replay canary over live executions.
    Canary {
        /// Maximum number of running workflow executions to sample.
        #[arg(long, default_value = "500")]
        sample_size: usize,
        /// Filter samples to a specific workflow type.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter samples to a specific task queue.
        #[arg(long)]
        queue: Option<String>,
        /// Output raw JSON instead of the summary table.
        #[arg(long)]
        json: bool,
    },
    /// Manage build-routing policies and percentage ramps for safe rolling
    /// deploys (issue #171, issue #604).
    #[command(alias = "build-routing")]
    Build {
        #[command(subcommand)]
        command: BuildRoutingCommand,
    },
    /// Statically check source for non-determinism reachable from `#[workflow]`
    /// bodies, including one first-party helper hop (issue #778).
    ///
    /// Read-only source analysis: no database, no network. Exits `0` when there
    /// are no hard-blocker findings and `1` when any `Error`-severity finding is
    /// present. Warnings (DET005/DET009 and command-free DET010) never fail the
    /// build unless `--deny-warnings` is passed.
    #[command(name = "det-check")]
    DetCheck {
        /// Source paths (files or directories) to scan. Defaults to the current
        /// directory. Directories are scanned recursively; `target` and hidden
        /// directories are skipped.
        #[arg(value_name = "PATHS", default_value = ".")]
        paths: Vec<PathBuf>,
        /// Output format: human-readable `text` (default) or machine-readable
        /// `json` (a full `DetCheckReport` with findings and suppressions).
        #[arg(long, value_enum, default_value_t)]
        format: DetCheckFormat,
        /// Also fail (exit `1`) when any warning-severity finding is present.
        #[arg(long, default_value_t = false)]
        deny_warnings: bool,
        /// List every active `harvest-suppress` suppression with its reason and
        /// location, then exit `0` (audit mode).
        #[arg(long, default_value_t = false)]
        list_suppressions: bool,
    },

    /// Scaffold a new, runnable autumn-harvest project (issue #692).
    ///
    /// Emits a complete crate — a `Cargo.toml` with crates.io deps and the `db`
    /// feature, a `#[workflow]`/`#[activity]` pair with `HarvestPlugin` wiring, a
    /// `compose.yaml` Postgres, an `autumn.toml`, and a README whose three-command
    /// path reaches one terminal execution. Pure local file generation: no
    /// database, no network. Everything is named after `<name>` — no manual
    /// find-and-replace of example identifiers.
    New {
        /// Project name. Becomes the crate name, the workflow/activity function
        /// stems, and the activity queue. Must be a valid Cargo package name
        /// (letters/digits/`-`/`_`, starting with a letter).
        #[arg(value_name = "NAME")]
        name: String,
        /// Target directory (defaults to `./<name>`).
        #[arg(long)]
        path: Option<PathBuf>,
        /// Overwrite files in a non-empty target directory. Opt-in; never
        /// removes files it did not write (no `rm -rf`).
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Template to emit. Currently only `minimal` ships.
        #[arg(long, value_enum, default_value_t)]
        template: ScaffoldTemplate,
    },
}

/// Build-routing subcommands (issue #604).
#[derive(Debug, Subcommand)]
enum BuildRoutingCommand {
    /// Manage a queue's percentage build ramp.
    Ramp {
        #[command(subcommand)]
        command: RampCommand,
    },
}

/// Percentage build ramp subcommands (issue #604).
#[derive(Debug, Subcommand)]
enum RampCommand {
    /// Set (or update) a queue's percentage build ramp. Requires a base
    /// build policy to already exist for the queue.
    Set {
        /// Task queue to ramp.
        #[arg(long)]
        queue: String,
        /// Ramp target build ID.
        #[arg(long)]
        target_build_id: String,
        /// Percentage of new starts routed to the target build, 0..=100.
        #[arg(long, value_parser = clap::value_parser!(i32).range(0..=100))]
        percent: i32,
    },
    /// Show current build policies and ramp state for every queue.
    #[command(alias = "list", alias = "ls")]
    Show,
    /// Clear a queue's percentage build ramp, immediately stopping new
    /// starts from reaching the target build.
    Clear {
        /// Task queue whose ramp should be cleared.
        #[arg(long)]
        queue: String,
    },
}

/// Workflow-type reachability subcommands (issue #520).
#[derive(Debug, Subcommand)]
enum WorkflowTypesCommand {
    /// Report per-workflow-type handler reachability.
    ///
    /// Exits `2` when any workflow type is `orphaned` (a non-terminal execution
    /// still depends on a handler this deployment no longer registers) or when
    /// the report is incomplete (a shard was unreachable), so it can gate a
    /// deploy in CI.
    Reachability {
        /// Narrow the report to a single workflow type.
        #[arg(long = "type")]
        workflow_type: Option<String>,
        /// Output raw JSON instead of the summary table.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum HistoryCommand {
    /// Export one workflow execution history.
    Export {
        /// Workflow execution ID.
        execution_id: String,
        /// Payload policy. `full` emits sensitive replay fixtures; `redacted` is safer for sharing.
        #[arg(long, value_enum, default_value = "redacted")]
        payload_policy: HistoryExportPayloadPolicy,
        /// Maximum serialized export size in bytes.
        #[arg(long)]
        max_bytes: Option<usize>,
        /// Write the JSON export to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        output_file: Option<PathBuf>,
    },
    /// Export a bounded batch of workflow histories.
    ExportBatch {
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by execution state group.
        #[arg(long, value_enum)]
        state_group: Option<HistoryExportStateGroup>,
        /// Lower bound on latest history event time, RFC 3339.
        #[arg(long)]
        updated_after: Option<String>,
        /// Upper bound on latest history event time, RFC 3339.
        #[arg(long)]
        updated_before: Option<String>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Maximum histories to export.
        #[arg(long)]
        limit: Option<usize>,
        /// Payload policy. `full` emits sensitive replay fixtures; `redacted` is safer for sharing.
        #[arg(long, value_enum, default_value = "redacted")]
        payload_policy: HistoryExportPayloadPolicy,
        /// Maximum serialized size per exported history in bytes.
        #[arg(long)]
        max_bytes: Option<usize>,
        /// Write the JSON export to a file instead of stdout.
        #[arg(long, value_name = "PATH")]
        output_file: Option<PathBuf>,
    },
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

/// Admission gate subcommands (issue #377).
#[derive(Debug, Subcommand)]
enum GateCommand {
    /// Create an admission gate to halt new workflow starts.
    #[command(alias = "add")]
    Create {
        /// Scope: `fleet`, `workflow_name=<name>`, `queue=<name>`, `shard_id=<N>`, or `owner=<id>`.
        #[arg(long)]
        scope: String,
        /// Required human-readable reason included in blocked-caller errors and the audit log.
        #[arg(long)]
        reason: String,
        /// Optional extended message shown in the Vantage UI.
        #[arg(long)]
        message: Option<String>,
        /// ISO 8601 expiry timestamp after which the gate self-clears (e.g. 2026-06-06T12:00:00Z).
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List all active (non-lifted) admission gates.
    #[command(alias = "ls")]
    List,
    /// Lift (remove) an admission gate by ID.
    #[command(alias = "delete", alias = "rm", alias = "remove")]
    Lift {
        /// Gate ID (UUID) to lift.
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Mint a scoped API token. The secret is returned exactly once.
    #[command(alias = "add")]
    Create {
        /// Human-readable label for the caller (CI job, dashboard, on-call, SDK).
        name: String,
        /// Verb-level scope: `read` (read-only routes) or `mutate` (everything).
        /// Defaults to `read` (least privilege).
        #[arg(long, default_value = "read")]
        scope: String,
        /// Optional RFC 3339 expiry after which the token is rejected 401.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// List all tokens as metadata (never the secret/hash).
    #[command(alias = "ls")]
    List,
    /// Revoke a token by ID (effective on the next request).
    #[command(alias = "delete", alias = "rm", alias = "remove")]
    Revoke {
        /// Token ID (UUID) to revoke.
        id: String,
    },
    /// Rotate a token: mint a replacement via the create route. Revoking the
    /// old token is a documented second step (`harvest token revoke <old-id>`).
    Rotate {
        /// The existing token ID being rotated out (used to name the replacement).
        old_id: String,
        /// Scope for the replacement token. Defaults to `read`.
        #[arg(long, default_value = "read")]
        scope: String,
        /// Optional RFC 3339 expiry for the replacement token.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// Seed the FIRST token offline (issue #942). Prints a fresh secret ONCE
    /// and the exact `INSERT INTO harvest_api_tokens ...` SQL for you to run
    /// against your database — no API call and no DB connection is made.
    ///
    /// Standalone (tokens-only) deployments use this to mint their first
    /// `mutate` token: with tokens as the only auth there is no admin caller
    /// yet to mint one via `POST /admin/tokens`. Run the printed SQL once (you
    /// already have DB access — the trust anchor), then mint every further
    /// token through the API. The printed SQL contains ONLY the hash; the
    /// secret is shown separately for you to store.
    Bootstrap {
        /// Human-readable label for the seed token.
        #[arg(long, default_value = "bootstrap")]
        name: String,
        /// Verb-level scope: `mutate` (can mint further tokens via the API) or
        /// `read`. Defaults to `mutate` so the seed token can bootstrap the rest.
        #[arg(long, default_value = "mutate", value_parser = ["read", "mutate"])]
        scope: String,
        /// Optional RFC 3339 expiry after which the token is rejected 401.
        #[arg(long)]
        expires_at: Option<String>,
        /// Audit provenance recorded as `created_by`.
        #[arg(long, default_value = "bootstrap")]
        created_by: String,
    },
}

#[derive(Debug, Subcommand)]
enum ShardCommand {
    /// Show per-shard readiness and rollout blockers.
    Health {
        /// Evaluate this readable shard as a promotion candidate.
        #[arg(long)]
        candidate_shard: Option<i32>,
        /// Deprecated compatibility flag; shard health gates writable shards by default.
        #[arg(long)]
        fail_on_unready: bool,
    },
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    /// List workflow executions.
    #[command(alias = "ls")]
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
        /// Filter by a typed comparison/set predicate over a search attribute,
        /// `key:op:value` where op is one of eq, ne, gt, gte, lt, lte, in,
        /// exists (e.g. `amount:gt:10000`, `phase:in:blocked,awaiting_approval`,
        /// `phase:exists`). Repeat to AND multiple predicates together. Forwarded
        /// verbatim to the `search_attr_filter` API param (issue #506).
        #[arg(long = "search-attr-filter", value_name = "KEY:OP:VALUE")]
        search_attr_filter: Vec<String>,
        /// Filter by owner (exact match).
        #[arg(long)]
        owner: Option<String>,
        /// Only return executions that have made no event progress for at
        /// least this many minutes. Excludes workflows correctly sleeping on
        /// a future-dated durable timer unless --include-sleeping is also set.
        #[arg(long)]
        no_progress_minutes: Option<i64>,
        /// Include executions sleeping on a future-dated durable timer in the
        /// stalled-workflow results. Only meaningful with --no-progress-minutes.
        #[arg(long)]
        include_sleeping: bool,
        /// Filter by workflow-start provenance (issue #740): one of api,
        /// schedule, backfill, `signal_with_start`, `update_with_start`,
        /// `completion_trigger`, webhook, child, batch, `continue_as_new`,
        /// reset, outbox, or unknown (matches pre-upgrade/NULL rows). The
        /// server rejects any other value with a 400.
        #[arg(long = "start-source")]
        start_source: Option<String>,
    },
    /// List tiered/summary-retention execution summaries (issue #752).
    ///
    /// Summaries are compact rows the retention janitor demotes a hard-deleted
    /// terminal execution into instead of losing it entirely. Admin-guarded.
    Summaries {
        /// Filter by registered workflow name (exact match).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by workflow ID (exact match).
        #[arg(long)]
        workflow_id: Option<String>,
        /// Filter by terminal state. Repeat the flag or pass a comma-separated
        /// list to match any of several states.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Only summaries whose `completed_at` is on or after this RFC 3339
        /// timestamp (e.g. 2026-01-01T00:00:00Z).
        #[arg(long)]
        completed_after: Option<String>,
        /// Only summaries whose `completed_at` is on or before this RFC 3339
        /// timestamp.
        #[arg(long)]
        completed_before: Option<String>,
        /// Filter by a `search_attrs` key/value pair (`key=value`). Repeat to
        /// AND multiple containment predicates together.
        #[arg(long = "search-attr", value_name = "KEY=VALUE")]
        search_attr: Vec<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Opaque keyset pagination cursor returned by the previous response.
        #[arg(long)]
        cursor: Option<String>,
        /// Sort direction: `desc` (default, newest-first) or `asc`.
        #[arg(long, value_parser = ["asc", "desc"])]
        order: Option<String>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
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
    /// Reconstruct a workflow execution's timeline (per-step durations, wait vs
    /// exec split, slowest step) from recorded history.
    Timeline {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Show the open awaitables an execution is parked on (pending activities,
    /// unfired timers, awaited-but-unsent signals, pending children,
    /// `await_condition` parks, pending updates), replay-derived.
    Awaitables {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Reconstruct the ordered continue-as-new run chain a workflow execution
    /// belongs to, resolvable from any member (origin, middle, or tail).
    RunChain {
        /// Workflow execution ID of any member of the chain.
        execution_id: String,
        /// Emit raw JSON instead of the default table.
        #[arg(long)]
        json: bool,
    },
    /// Replay a single execution's recorded history against the currently
    /// registered workflow handler and report a structured determinism verdict
    /// (clean, diverged, failed, not-registered, or not-replayable).
    ReplayDiagnosis {
        /// Workflow execution ID to diagnose.
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
        /// How to handle a collision with a currently-active (RUNNING/PAUSED)
        /// prior (issue #685). Orthogonal to `--reuse-policy` (which governs
        /// terminal priors). One of: `unspecified` (default), `fail`,
        /// `use_existing`, `terminate_existing`.
        #[arg(long, value_name = "POLICY")]
        conflict_policy: Option<String>,
        /// Target ISO 8601 / RFC 3339 timestamp to start the workflow.
        #[arg(long)]
        start_at: Option<String>,
        /// Delay duration before starting the workflow (e.g. "10s", "5m").
        #[arg(long)]
        delay: Option<String>,
    },
    /// Cancel a workflow execution.
    Cancel {
        /// Workflow execution ID.
        execution_id: String,
        /// Cancellation reason.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Pause a running workflow execution (operator intervention).
    ///
    /// While paused the executor dispatches no new commands for the execution;
    /// in-flight activities run to completion. PAUSED is a non-terminal active
    /// state — resume with `harvest workflow resume`. Pausing an
    /// already-paused execution is a no-op.
    Pause {
        /// Workflow execution ID.
        execution_id: String,
        /// Human-readable pause reason (max 500 chars), recorded in audit log.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Resume a paused workflow execution, waking the parked task.
    Resume {
        /// Workflow execution ID.
        execution_id: String,
    },
    /// Erase PII payload fields from a completed workflow execution (GDPR Art. 17).
    ///
    /// Replaces all payload-bearing fields (`input`, `output`, `payload`, `details`,
    /// `value`, `last_completion_result`) with a tombstone marker. The execution
    /// itself and its audit trail are preserved. Only terminal executions
    /// (COMPLETED, FAILED, CANCELLED, `TIMED_OUT`, `CONTINUED_AS_NEW`, TERMINATED)
    /// can be erased. Cascades to terminal child executions on the same shard.
    /// This operation is irreversible.
    ErasePayloads {
        /// Workflow execution ID.
        execution_id: String,
        /// Erasure reason (e.g. "GDPR Art. 17 request ID: DSR-12345"), recorded in audit log.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Force a backing-off activity to retry immediately, skipping its backoff.
    RetryActivity {
        /// Workflow execution ID.
        workflow_id: String,
        /// Activity execution ID (the id surfaced by `workflow stack`).
        activity_exec_id: String,
    },
    /// Force-fail a hung in-flight (RUNNING) activity, skipping all remaining
    /// retries.
    ///
    /// The owning workflow observes the distinct `OperatorForceFailed` error
    /// type and advances to its own failure/compensation path — it is NOT
    /// terminated. Re-issuing the command on an already-forced activity is an
    /// idempotent no-op success.
    FailActivity {
        /// Workflow execution ID.
        workflow_id: String,
        /// Activity execution ID (the id surfaced by `workflow stack`).
        activity_exec_id: String,
        /// Human-readable reason recorded in the forced failure (e.g. an
        /// incident id).
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
        /// Exactly-once delivery key (issue #753). Repeated deliveries with
        /// the same key for the same execution land exactly one
        /// `SignalReceived` event; the response reports
        /// `signal_delivered=false` for the deduped retries. Omit to keep the
        /// legacy at-least-once behavior (every call delivers a distinct
        /// signal event). Typically a stable upstream event id (e.g. a Stripe
        /// event id or SQS message id). An empty key is rejected — the server
        /// treats an empty `?idempotency_key=` as omitted, which would
        /// silently degrade an intended exactly-once delivery to
        /// at-least-once.
        #[arg(long, value_name = "KEY", value_parser = parse_idempotency_key)]
        idempotency_key: Option<String>,
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
    /// List declarative query and update handlers registered for a workflow type.
    Handlers {
        /// Registered workflow name.
        workflow_name: String,
    },
    /// Reset a cohort of workflow executions to a shared semantic point (issue #538).
    ///
    /// Selects candidates via the filter grammar, resolves the logical anchor
    /// per execution, and returns a per-execution outcome list. Use
    /// `--preview` to resolve without forking.
    BatchReset {
        /// Inline JSON filter (e.g. `'{"states":["FAILED"],"workflow_name":"my_flow"}'`).
        #[arg(long, conflicts_with = "filter_file")]
        filter_json: Option<String>,
        /// File containing the JSON filter. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "filter_json")]
        filter_file: Option<PathBuf>,
        /// Reset to a specific event ID (preserved for each execution individually).
        #[arg(long, conflicts_with_all = ["first_activity", "last_workflow_task"])]
        event_id: Option<i64>,
        /// Reset each execution just before the first scheduling of this activity name.
        #[arg(long, value_name = "ACTIVITY_NAME", conflicts_with_all = ["event_id", "last_workflow_task"])]
        first_activity: Option<String>,
        /// Reset each execution to the most-recent clean workflow-task boundary.
        #[arg(long, conflicts_with_all = ["event_id", "first_activity"])]
        last_workflow_task: bool,
        /// Recovery reason recorded in reset marker events.
        #[arg(long)]
        reason: String,
        /// Operator identity recorded in reset marker events.
        #[arg(long, default_value = "cli")]
        operator_id: String,
        /// How to handle undelivered source signals.
        #[arg(long, value_enum, default_value = "drop")]
        signal_reapply: ResetSignalReapply,
        /// Resolve the semantic point per execution but do not fork any execution.
        #[arg(long)]
        preview: bool,
    },
}

#[derive(Debug, Subcommand)]
enum HandoffCommand {
    /// List external activity handoffs.
    List {
        /// Filter by handoff state. Repeat the flag or pass a comma-separated list.
        #[arg(long, value_delimiter = ',')]
        state: Vec<String>,
        /// Filter by registered workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by workflow execution ID.
        #[arg(long)]
        execution_id: Option<String>,
        /// Filter by registered activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Filter by external activity token.
        #[arg(long)]
        token: Option<String>,
        /// Restrict inspection to one shard.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Return handoffs due before this RFC3339 timestamp.
        #[arg(long)]
        due_before: Option<String>,
        /// Return handoffs last updated before this RFC3339 timestamp.
        #[arg(long)]
        updated_before: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Inspect one external activity handoff by token.
    #[command(alias = "get")]
    Inspect {
        /// External activity token.
        token: String,
        /// Print the raw JSON API payload instead of a human table.
        #[arg(long)]
        json: bool,
    },
    /// Complete an external activity handoff by token.
    Complete {
        /// External activity token.
        token: String,
        /// JSON value to send as the activity output.
        #[arg(long, conflicts_with = "output_file")]
        output_json: Option<String>,
        /// File containing JSON output. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "output_json")]
        output_file: Option<PathBuf>,
        /// Full JSON request body. Use this to send `{ "output": ... }` directly.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
    },
    /// Fail an external activity handoff by token.
    Fail {
        /// External activity token.
        token: String,
        /// String error to record on the external activity.
        #[arg(long)]
        error: Option<String>,
        /// JSON error details, compacted into the recorded error string.
        #[arg(long, conflicts_with = "error_file")]
        error_json: Option<String>,
        /// File containing JSON error details. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "error_json")]
        error_file: Option<PathBuf>,
        /// Full JSON request body. Use this to send `{ "error": "...", "retryable": false }`.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
        /// Mark the external failure as retryable for workflow replay.
        #[arg(long)]
        retryable: bool,
    },
    /// Heartbeat an external activity handoff and optionally extend its deadline.
    #[command(alias = "extend")]
    Heartbeat {
        /// External activity token.
        token: String,
        /// Seconds to extend the deadline from now.
        #[arg(long)]
        extend_by_secs: Option<u64>,
        /// Full JSON request body. Use this to send `{ "extend_by_secs": 3600 }`.
        #[arg(long, conflicts_with = "request_file")]
        request_json: Option<String>,
        /// File containing the full JSON request body. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "request_json")]
        request_file: Option<PathBuf>,
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
    /// Retry a failed DAG run from one or more failed nodes (issue #366).
    ///
    /// Re-executes the named node(s) and every node declared downstream of
    /// them, carrying over the recorded results of all upstream nodes. Use
    /// `--dry-run` first to preview the resolved reset point and the exact
    /// re-execute / carry-over sets without committing.
    Retry {
        /// Registered (unified) DAG name.
        dag_name: String,
        /// The failed DAG run's execution id.
        run_exec_id: String,
        /// Node (activity) name to retry from. Repeatable.
        #[arg(long = "from-node", value_name = "NODE", required = true)]
        from_node: Vec<String>,
        /// Operator-supplied recovery reason (recorded in the audit trail).
        #[arg(long)]
        reason: String,
        /// Operator identity for audit. Defaults to the global `--actor`.
        #[arg(long)]
        operator_id: Option<String>,
        /// Preview the plan without committing any write.
        #[arg(long)]
        dry_run: bool,
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
    /// Edit an existing schedule in place — partial update, `schedule_id` preserved (issue #771).
    Update {
        /// Schedule row ID (UUID).
        id: String,
        /// New cron expression (e.g. `"0 3 * * *"`).
        #[arg(long, conflicts_with_all = ["interval_secs", "manual"])]
        cron: Option<String>,
        /// New interval in seconds.
        #[arg(long, conflicts_with_all = ["cron", "manual"])]
        interval_secs: Option<u64>,
        /// Switch the schedule to manual-only firing.
        #[arg(long, conflicts_with_all = ["cron", "interval_secs"])]
        manual: bool,
        /// New IANA timezone for the cron expression (e.g. `"America/New_York"`).
        #[arg(long)]
        tz: Option<String>,
        /// New inline JSON input passed to each scheduled run. Any non-null
        /// JSON value; a literal `null` leaves the stored input unchanged
        /// (null is the one JSON value that cannot be set as the input).
        #[arg(long, value_name = "JSON")]
        input_json: Option<String>,
        /// New task queue name for scheduled runs.
        #[arg(long)]
        queue: Option<String>,
        /// New overlap policy: skip, `buffer_one`, `buffer_all`, `cancel_other`, `terminate_other`.
        #[arg(long)]
        overlap_policy: Option<String>,
        /// New maximum buffered slots under `buffer_all`.
        #[arg(long)]
        buffer_all_max: Option<u32>,
        /// New catchup policy: `skip_all`, `most_recent`, window, unbounded.
        #[arg(long)]
        catchup_policy: Option<String>,
        /// Window length in seconds for `catchup_policy` = window.
        #[arg(long)]
        catchup_window_secs: Option<i64>,
        /// New jitter window in seconds (0 disables jitter).
        #[arg(long)]
        jitter_secs: Option<u64>,
        /// New maximum concurrent in-flight runs.
        #[arg(long)]
        max_active_runs: Option<u32>,
        /// Attach a named calendar.
        #[arg(long, conflicts_with = "clear_calendar")]
        calendar: Option<String>,
        /// Detach the calendar (sends an explicit JSON null).
        #[arg(long)]
        clear_calendar: bool,
        /// New absolute UTC cutoff, RFC 3339 (e.g. 2030-01-01T00:00:00Z).
        #[arg(long, conflicts_with = "clear_end_at")]
        end_at: Option<String>,
        /// Remove the `end_at` cutoff (sends an explicit JSON null).
        #[arg(long)]
        clear_end_at: bool,
        /// New total run budget.
        #[arg(long, conflicts_with = "clear_max_runs")]
        max_runs: Option<u32>,
        /// Remove the run budget (sends an explicit JSON null).
        #[arg(long)]
        clear_max_runs: bool,
    },
    /// Backfill missed scheduled runs over an explicit time window.
    Backfill {
        /// Schedule row ID (UUID).
        id: String,
        /// Start of the backfill window, RFC 3339 (e.g. 2026-04-01T00:00:00Z). Required.
        #[arg(long, required = true)]
        from: String,
        /// End of the backfill window, RFC 3339 (e.g. 2026-04-08T00:00:00Z). Required.
        #[arg(long, required = true)]
        to: String,
        /// Preview planned timestamps without dispatching any runs.
        #[arg(long)]
        dry_run: bool,
        /// Maximum number of timestamps to plan (default: server-side limit of 1000).
        #[arg(long)]
        max_count: Option<u64>,
        /// Backfill even if the schedule is currently paused.
        #[arg(long)]
        include_paused: bool,
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
    /// Trigger an immediate one-off run of a schedule.
    TriggerNow {
        /// Schedule row ID (UUID).
        id: String,
        /// Optional free-text reason recorded in the audit trail.
        #[arg(long)]
        reason: Option<String>,
        /// Force-trigger even if the schedule is currently paused.
        #[arg(long)]
        force: bool,
    },
    /// List the runs a schedule launched, newest-first, with terminal outcomes.
    Runs {
        /// Schedule row ID (UUID).
        id: String,
        /// Filter by execution state (repeatable), e.g. --state FAILED --state `TIMED_OUT`.
        #[arg(long = "state")]
        state: Vec<String>,
        /// Filter by dispatch origin (repeatable): scheduled, backfill, `manual_trigger`.
        #[arg(long = "origin")]
        origin: Vec<String>,
        /// Only runs started at/after this RFC 3339 time or relative duration (e.g. 24h).
        #[arg(long)]
        since: Option<String>,
        /// Only runs started before this RFC 3339 time or relative duration.
        #[arg(long)]
        until: Option<String>,
        /// Maximum runs to return (default 20, clamped 1-200).
        #[arg(long)]
        limit: Option<u32>,
        /// Opaque keyset cursor from a prior response's `next_cursor`.
        #[arg(long)]
        cursor: Option<String>,
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

/// Task-queue pause/resume (issue #619): hold dispatch on a whole queue while a
/// downstream dependency is down, then thaw it.
///
/// A pause never fails, retries, or dead-letters work — held tasks stay
/// `PENDING` and become claimable again the instant the queue is resumed, with
/// the time they spent held credited back so the thaw does not retroactively
/// schedule-to-start-time-out the backlog.
#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// Hold dispatch on a task queue.
    Pause {
        /// Task queue name.
        queue_name: String,
        /// Why the queue is being held (recorded on the pause row, surfaced by
        /// `harvest queue list-paused` and the Vantage Workers page).
        #[arg(long)]
        reason: String,
        /// Restrict the hold to one shard. Omit for a fleet-wide pause (the
        /// default).
        #[arg(long)]
        shard_id: Option<i32>,
    },
    /// Release a held task queue; held tasks become immediately claimable.
    Resume {
        /// Task queue name.
        queue_name: String,
        /// Restrict the release to one shard. Omit for fleet-wide (the default).
        #[arg(long)]
        shard_id: Option<i32>,
    },
    /// List every currently-paused queue with its reason and held-task count.
    #[command(alias = "list", alias = "status")]
    ListPaused,
}

/// Per-execution legal hold (issue #747): exempt an execution's history from
/// retention deletion and PII erasure until released or auto-expired.
#[derive(Debug, Subcommand)]
enum LegalHoldCommand {
    /// Place (or refresh) a legal hold on an execution.
    Set {
        /// Workflow execution ID.
        execution_id: String,
        /// Justification for the hold (recorded in the audit trail and
        /// `legal_hold_reason`).
        #[arg(long)]
        reason: String,
        /// Optional RFC3339 auto-expiry (e.g. `2027-01-01T00:00:00Z`). Omit for
        /// an indefinite hold.
        #[arg(long)]
        until: Option<String>,
    },
    /// Release a legal hold on an execution.
    Release {
        /// Workflow execution ID.
        execution_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum RateLimitCommand {
    /// Show all active per-activity rate limit token buckets and refill rates.
    Status,
    /// Insert or dynamically override a rate limit configuration.
    Set {
        /// Opaque rate limit identifier key.
        key: String,
        /// Rate at which tokens are added to the bucket per second.
        #[arg(long)]
        refill_rate: f64,
        /// Maximum capacity of the token bucket.
        #[arg(long)]
        burst: f64,
    },
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
        /// Name of the signal (required for action Signal unless --dry-run).
        ///
        /// Enforced manually in `batch_request` (not via `required_if_eq`) so a
        /// `Signal --dry-run` preview — which reports blast radius, not signal
        /// validity — can omit it (issue #769).
        #[arg(long)]
        signal_name: Option<String>,
        /// Inline JSON signal payload.
        #[arg(long, conflicts_with = "signal_payload_file")]
        signal_payload_json: Option<String>,
        /// File containing JSON signal payload. Use `-` for stdin.
        #[arg(long, value_name = "PATH", conflicts_with = "signal_payload_json")]
        signal_payload_file: Option<PathBuf>,
        /// Preview the blast radius (count + sample) without submitting a job.
        #[arg(long)]
        dry_run: bool,
        /// With --dry-run, print the raw JSON preview instead of a table.
        #[arg(long, requires = "dry_run")]
        json: bool,
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
        /// Exact match on task queue name (e.g. to reproduce a queue-scoped facet).
        #[arg(long)]
        queue_name: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Filter by derived error class (exact, `PascalCase`; e.g. `CircuitOpen`,
        /// `PoisonPill`, `HandlerPanic`). Matching is exact-equality, not case-folded.
        #[arg(long)]
        error_class: Option<String>,
        /// Filter by derived DLQ reason class (exact, `snake_case`; e.g.
        /// `poison_pill`, `workflow_task_timeout`, `retry_exhaustion`). Exact-equality.
        #[arg(long)]
        dlq_reason: Option<String>,
        /// Filter by derived failure signature (exact match on the normalized
        /// first line of the error).
        #[arg(long)]
        failure_signature: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any writes.
        #[arg(long)]
        dry_run: bool,
    },
    /// Aggregate dead-lettered tasks by dimension for fast root-cause triage.
    ///
    /// Groups the DLQ by one or more dimensions and reports per-group counts
    /// with representative sample IDs, merged across shards. Renders a table by
    /// default; pass --json for piping.
    #[command(alias = "summary")]
    Aggregate {
        /// Grouping dimensions (comma-separated or repeated). Supported:
        /// `workflow_name`, `activity_name`, `queue_name`, `task_type`,
        /// `time_bucket`, `failure_signature`, `dlq_reason`, `error_class`.
        /// Order builds a hierarchical key.
        #[arg(long = "group-by", value_delimiter = ',', required = true)]
        group_by: Vec<String>,
        /// Granularity for the `time_bucket` dimension: hour (default) or day.
        #[arg(long)]
        time_bucket: Option<String>,
        /// Filter by workflow name (applied before grouping).
        #[arg(long)]
        workflow_name: Option<String>,
        /// Filter by activity name.
        #[arg(long)]
        activity_name: Option<String>,
        /// Filter by queue name.
        #[arg(long)]
        queue_name: Option<String>,
        /// Inclusive lower bound on `failed_at`: RFC 3339 or relative (e.g. `24h`).
        #[arg(long)]
        since: Option<String>,
        /// Exclusive upper bound on `failed_at`: RFC 3339 or relative.
        #[arg(long)]
        until: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Cap on returned groups [1–500] (default 50). Long tail rolls into `_other`.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=500))]
        limit_groups: Option<u32>,
        /// Representative sample IDs per group [0–10] (default 3).
        #[arg(long, value_parser = clap::value_parser!(u32).range(0..=10))]
        samples_per_group: Option<u32>,
        /// Print the raw JSON API payload instead of a table.
        #[arg(long)]
        json: bool,
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
        /// Exact match on task queue name (e.g. to reproduce a queue-scoped facet).
        #[arg(long)]
        queue_name: Option<String>,
        /// Only include entries with at least this many attempts.
        #[arg(long)]
        min_attempts: Option<i32>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        failed_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        failed_before: Option<String>,
        /// Filter by derived error class (exact, `PascalCase`; e.g. `CircuitOpen`,
        /// `PoisonPill`, `HandlerPanic`). Matching is exact-equality, not case-folded.
        #[arg(long)]
        error_class: Option<String>,
        /// Filter by derived DLQ reason class (exact, `snake_case`; e.g.
        /// `poison_pill`, `workflow_task_timeout`, `retry_exhaustion`). Exact-equality.
        #[arg(long)]
        dlq_reason: Option<String>,
        /// Filter by derived failure signature (exact match on the normalized
        /// first line of the error).
        #[arg(long)]
        failure_signature: Option<String>,
        /// Maximum rows to act on per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: Option<u32>,
        /// Preview matching rows without performing any deletes.
        #[arg(long)]
        dry_run: bool,
    },
    /// Redrive (re-enqueue) dead-lettered tasks matching a filter after a fix.
    ///
    /// Re-enqueues matching entries with a fresh retry budget, reactivating any
    /// owning execution that was sealed FAILED so it resumes from existing
    /// history. Idempotent: redriving an already-redriven entry is a no-op
    /// reported as `skipped`. At least one filter criterion must be provided;
    /// use --dry-run to preview without writing.
    Redrive {
        /// Exact match on the original task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Exact match on the owning execution's workflow name.
        #[arg(long)]
        workflow_name: Option<String>,
        /// Inclusive lower bound on `failed_at` (RFC 3339, e.g. `2026-04-27T12:30:00Z`).
        #[arg(long)]
        dead_lettered_after: Option<String>,
        /// Exclusive upper bound on `failed_at` (RFC 3339).
        #[arg(long)]
        dead_lettered_before: Option<String>,
        /// Case-insensitive substring match on the dead-letter error text.
        #[arg(long)]
        error_contains: Option<String>,
        /// Explicit dead-letter IDs to redrive (comma-separated or repeated).
        #[arg(long = "dead-letter-id", value_delimiter = ',')]
        dead_letter_ids: Vec<String>,
        /// Maximum rows to redrive per call (default 100, max 1000).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
        max: Option<u32>,
        /// Optional operator reason recorded on the redrive event.
        #[arg(long)]
        reason: Option<String>,
        /// Preview matching rows without re-enqueuing.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Subcommands for `harvest completion-delivery` (issue #605).
#[derive(Debug, Subcommand)]
enum CompletionDeliveryCommand {
    /// List completion-callback deliveries registered for a workflow execution.
    ///
    /// Includes PENDING, INFLIGHT, DELIVERED, and FAILED rows, ordered by
    /// `callback_index`. Filtering by `--state` is applied client-side.
    List {
        /// Workflow execution ID.
        execution_id: String,
        /// Filter to a single delivery state: pending | inflight | delivered | failed.
        #[arg(long)]
        state: Option<String>,
    },
    /// Manually redrive a FAILED completion-callback delivery after fixing the receiver.
    ///
    /// Idempotent-shaped: redriving a delivery that is not currently FAILED
    /// returns `ok=false` with `outcome="not_failed"` instead of erroring.
    Redrive {
        /// Workflow execution ID that owns the delivery.
        execution_id: String,
        /// Completion-delivery row ID, as returned by `list`.
        delivery_id: String,
    },
}

/// Subcommands for `harvest worker` (issue #170).
#[derive(Debug, Subcommand)]
enum WorkerCommand {
    /// Request a graceful drain for a specific worker.
    ///
    /// Sets the worker status to `Draining` so it stops accepting new tasks.
    /// The worker itself will complete in-flight tasks and then transition to
    /// `Stopped`. Use `--wait` to block until the worker reaches a terminal
    /// state before the deadline.
    Drain {
        /// Worker ID to drain.
        worker_id: String,
        /// Drain-by deadline (RFC 3339, e.g. `2026-05-09T12:00:00Z`).
        /// When omitted the server uses its configured shutdown timeout.
        #[arg(long)]
        deadline: Option<String>,
        /// Block until the worker reaches `Stopped` or the deadline elapses.
        /// Polls `GET /workers/{id}` every 2 s; exits 1 on timeout.
        #[arg(long)]
        wait: bool,
        /// Maximum seconds to wait when `--wait` is set (default: 120).
        #[arg(long, default_value = "120")]
        wait_timeout_secs: u64,
    },
    /// Preview which workers would be targeted by a drain, without draining them.
    #[command(name = "drain-preview")]
    DrainPreview {
        /// Filter by task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Filter by lifecycle status (`Active`, `Draining`, `Stopped`).
        #[arg(long)]
        status: Option<String>,
        /// Maximum number of workers to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
    /// List registered workers.
    List {
        /// Filter by task queue name.
        #[arg(long)]
        queue: Option<String>,
        /// Filter by shard id.
        #[arg(long)]
        shard_id: Option<i32>,
        /// Filter by lifecycle status (`Active`, `Draining`, `Stopped`).
        #[arg(long)]
        status: Option<String>,
        /// Filter by health (`healthy` or `stale`).
        #[arg(long)]
        health: Option<String>,
        /// Maximum number of workers to return [1–500].
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..=500))]
        limit: Option<i64>,
    },
    /// Show details for a single worker.
    Get {
        /// Worker ID.
        worker_id: String,
    },
    /// Show aggregated fleet health statistics.
    Health,
}

/// Subcommands for `harvest events`.
#[derive(Debug, Subcommand)]
enum EventsCommand {
    /// Open the SSE stream for a workflow execution and print events to stdout.
    ///
    /// Each SSE event block is printed as `<event-type>: <json-data>`.
    /// The stream terminates when the execution reaches a terminal state
    /// (`event: stream-end`) or when the connection is closed.
    Tail {
        /// Workflow execution ID to watch.
        execution_id: String,
        /// Resume from this event row ID (Last-Event-ID header).
        /// Events with id > this value are replayed before entering live-tail mode.
        #[arg(long)]
        last_event_id: Option<i64>,
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
            Commands::Preflight => Ok(ApiRequest::get("/admin/preflight")),
            Commands::Shard { command } => Ok(shard_request(command)),
            Commands::Workflow { command } => workflow_request(command),
            Commands::History { command } => Ok(history_request(command)),
            Commands::LegalHold { command } => Ok(legal_hold_request(command)),
            Commands::Handoff { command } => handoff_request(command),
            Commands::Dag { command } => dag_request(command, self.actor.as_deref()),
            Commands::Schedule { command } => schedule_request(command),
            Commands::Dlq { command } => Ok(dead_letter_request(command)),
            Commands::CompletionDelivery { command } => Ok(completion_delivery_request(command)),
            Commands::Retention { command } => Ok(retention_request(command)),
            Commands::Queue { command } => queue_request(command),
            Commands::Concurrency { command } => Ok(concurrency_request(command)),
            Commands::RateLimit { command } => Ok(rate_limit_request(command)),
            Commands::Batch { command } => batch_request(command),
            Commands::Audit { command } => Ok(audit_request(command)),
            Commands::Gate { command } => gate_request(command),
            Commands::Token { command } => Ok(token_request(command)),
            Commands::Worker { command } => Ok(worker_request(command)),
            Commands::Usage {
                from,
                to,
                group_by,
                json: _,
            } => Ok(usage_request(from, to, group_by.as_deref())),
            Commands::VersionUsage {
                workflow_name,
                change_id,
                recorded_version,
                state_group,
                shard_id,
                guard,
            } => Ok(version_usage_request(
                workflow_name.as_deref(),
                change_id.as_deref(),
                *recorded_version,
                *state_group,
                *shard_id,
                *guard,
            )),
            Commands::VersionGateRetirement {
                change_id,
                min_safe_version,
                workflow_name,
                state_group,
                shard_id,
                check: _,
            } => Ok(retirement_check_request(
                change_id,
                *min_safe_version,
                workflow_name.as_deref(),
                *state_group,
                *shard_id,
            )),
            Commands::WorkflowTypes { command } => Ok(workflow_reachability_request(command)),
            Commands::Tui => unreachable!("Tui command handles its own requests"),
            Commands::Events { .. } => unreachable!("Events command handles its own requests"),
            Commands::StartBatch {
                file,
                items_json,
                atomic,
            } => start_batch_request(file.as_deref(), items_json.as_deref(), *atomic),
            Commands::Canary {
                sample_size,
                workflow_name,
                queue,
                json: _,
            } => Ok(canary_request(
                *sample_size,
                workflow_name.as_deref(),
                queue.as_deref(),
            )),
            Commands::Build { command } => Ok(build_routing_request(command)),
            Commands::DetCheck { .. } => {
                unreachable!("DetCheck handles its own execution locally")
            }
            Commands::New { .. } => {
                unreachable!("New handles its own execution locally")
            }
        }
    }
}

fn build_routing_request(command: &BuildRoutingCommand) -> ApiRequest {
    match command {
        BuildRoutingCommand::Ramp { command } => match command {
            RampCommand::Set {
                queue,
                target_build_id,
                percent,
            } => ApiRequest::post(
                "/admin/build-routing/ramp",
                Some(json!({
                    "queue_name": queue,
                    "target_build_id": target_build_id,
                    "ramp_percent": percent,
                })),
            ),
            RampCommand::Show => ApiRequest::get("/admin/build-routing"),
            RampCommand::Clear { queue } => ApiRequest {
                method: ApiMethod::Delete,
                path: format!("/admin/build-routing/ramp/{}", path_segment(queue)),
                body: None,
            },
        },
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
// A dispatcher: a sequence of `if let ... return` guards for locally-handled
// commands (det-check, tui, events, worker-drain-wait, token bootstrap) followed
// by the shared execute/render path. Splitting it would only scatter the guards.
#[allow(clippy::too_many_lines)]
pub async fn run_cli(cli: Cli) -> Result<(), CliError> {
    // det-check is read-only local source analysis: no HTTP, handled entirely
    // in-process before the API execute path (mirrors the Tui early-return).
    if let Commands::DetCheck {
        paths,
        format,
        deny_warnings,
        list_suppressions,
    } = &cli.command
    {
        return run_det_check(paths, *format, *deny_warnings, *list_suppressions);
    }

    // `new` is pure local file generation: no HTTP, no DB (mirrors DetCheck).
    if let Commands::New {
        name,
        path,
        force,
        template,
    } = &cli.command
    {
        return run_new(name, path.as_deref(), *force, *template);
    }

    // Token bootstrap is an OFFLINE seed: print the secret + INSERT SQL, open no
    // DB connection, issue no HTTP request (mirrors the DetCheck early-return).
    if let Commands::Token {
        command:
            TokenCommand::Bootstrap {
                name,
                scope,
                expires_at,
                created_by,
            },
    } = &cli.command
    {
        return run_token_bootstrap(name, scope, expires_at.as_deref(), created_by);
    }

    if matches!(cli.command, Commands::Tui) {
        return tui::run_tui(&cli).await;
    }

    // SSE streaming: bypasses JSON execute path.
    if let Commands::Events {
        command:
            EventsCommand::Tail {
                execution_id,
                last_event_id,
            },
    } = &cli.command
    {
        return run_events_tail(&cli, execution_id, *last_event_id).await;
    }

    // --wait mode: issue drain then poll until Stopped or timeout.
    if let Commands::Worker {
        command:
            WorkerCommand::Drain {
                worker_id,
                wait: true,
                wait_timeout_secs,
                ..
            },
    } = &cli.command
    {
        return run_worker_drain_wait(&cli, worker_id, *wait_timeout_secs).await;
    }

    let response = match execute(&cli).await {
        Ok(v) => v,
        Err(err) => {
            // Fail closed: a transport/API error on a reachability gate command must
            // exit 2 (deploy hazard) rather than exit 1 (generic error). Exit 1 is
            // labelled "transport/usage error" in the runbook and operators may
            // retry or ignore it; exit 2 unambiguously signals an unsafe answer.
            if workflow_reachability_should_gate(&cli) {
                return Err(CliError::WorkflowReachabilityGate {
                    context: err.to_string(),
                });
            }
            return Err(err);
        }
    };
    let rendered = render_response(&cli, &response)?;
    // Issue #756: a degraded cross-shard read carries its partial-availability
    // warning on STDERR, keeping STDOUT a clean/parseable body (`-o json | jq`)
    // on both the happy and degraded paths. The operator still sees the warning.
    if let Some(notice) = fanout_partial_notice(&response) {
        eprintln!("{notice}");
    }
    if let Some(path) = history_output_file(&cli) {
        fs::write(path, &rendered).map_err(|source| CliError::WriteOutput {
            path: path.display().to_string(),
            source,
        })?;
    } else {
        println!("{rendered}");
    }
    if matches!(cli.command, Commands::Preflight) {
        let exit_code = preflight_exit_code(&response);
        if exit_code != 0 {
            let status = response
                .get("overall_status")
                .and_then(Value::as_str)
                .unwrap_or("fail")
                .to_string();
            return Err(CliError::PreflightGate { status });
        }
    }
    if shard_health_should_gate(&cli) && shard_health_exit_code(&response) != 0 {
        return Err(CliError::ShardHealthGate);
    }
    if version_usage_should_guard(&cli) && version_usage_guard_exit_code(&response) != 0 {
        return Err(CliError::VersionUsageGate);
    }
    if retirement_check_should_check(&cli) && retirement_check_exit_code(&response) != 0 {
        return Err(CliError::RetirementCheckGate);
    }
    if workflow_reachability_should_gate(&cli) && workflow_reachability_exit_code(&response) != 0 {
        return Err(CliError::WorkflowReachabilityGate {
            context: "orphaned verdict, in_use with type filter, or incomplete shard report"
                .to_string(),
        });
    }
    if queue_mutation_should_gate(&cli) && queue_mutation_exit_code(&response) != 0 {
        let detail = response
            .get("partial_failures")
            .and_then(Value::as_str)
            .unwrap_or("see response body")
            .to_string();
        return Err(CliError::QueuePartialMutation { detail });
    }
    if canary_should_gate(&cli) && canary_exit_code(&response) != 0 {
        let verdict = response
            .get("verdict")
            .and_then(Value::as_str)
            .unwrap_or("fail")
            .to_string();
        return Err(CliError::CanaryGate { verdict });
    }
    Ok(())
}

fn history_output_file(cli: &Cli) -> Option<&Path> {
    match &cli.command {
        Commands::History {
            command:
                HistoryCommand::Export { output_file, .. }
                | HistoryCommand::ExportBatch { output_file, .. },
        } => output_file.as_deref(),
        _ => None,
    }
}

// ── det-check (issue #778) ──────────────────────────────────────────────────

/// Builds one determinism report across every requested source path.
///
/// A single shared first-party helper index (issue #778) is used, so a
/// cross-file transitive violation is caught even when the two files are passed
/// as separate arguments (the changed-files CI pattern). Directories are walked
/// recursively; files are scanned directly; symlinks are not followed;
/// overlapping arguments are de-duplicated; a non-UTF-8 file mid-walk is
/// skipped. Pure — no printing.
///
/// # Errors
/// Returns [`CliError::InvalidInput`] if a top-level path is missing or a source
/// path cannot be read.
pub fn det_check_report_for_paths(paths: &[PathBuf]) -> Result<DetCheckReport, CliError> {
    let refs: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
    check_paths(&refs).map_err(|source| {
        CliError::InvalidInput(format!("det-check: failed to read source: {source}"))
    })
}

/// Formats one finding as `file:line:col DETxxx  (safe alternative: …)`, with a
/// trailing `[in helper `H` reached from workflow `W`]` for a transitive finding.
fn format_det_finding_line(finding: &autumn_harvest::DetFinding) -> String {
    let loc = finding.location.as_ref();
    let file = loc.map_or("<unknown>", |l| l.file.as_str());
    let line = loc.map_or(0, |l| l.line);
    let col = loc.map_or(1, |l| l.col);
    let mut out = format!(
        "{file}:{line}:{col} {}  (safe alternative: {})",
        finding.rule_id, finding.alternative
    );
    if let Some(helper) = &finding.via_helper {
        let wf = finding.workflow_name.as_deref().unwrap_or("<unknown>");
        let _ = write!(out, "  [in helper `{helper}` reached from workflow `{wf}`]");
    }
    out
}

/// Renders the findings section of a report as text, one line per finding,
/// sorted by `(file, line, col, rule_id)`.
#[must_use]
pub fn format_det_findings_text(report: &DetCheckReport) -> String {
    if report.findings.is_empty() {
        return "det-check: no findings".to_string();
    }
    let mut findings: Vec<&autumn_harvest::DetFinding> = report.findings.iter().collect();
    findings.sort_by(|a, b| det_finding_sort_key(a).cmp(&det_finding_sort_key(b)));
    let mut lines: Vec<String> = findings
        .iter()
        .map(|f| format_det_finding_line(f))
        .collect();
    let (errors, warnings) = det_check_counts(report);
    lines.push(format!(
        "det-check: {errors} hard-blocker finding(s), {warnings} warning(s)"
    ));
    lines.join("\n")
}

fn det_finding_sort_key(f: &autumn_harvest::DetFinding) -> (String, u32, u32, &'static str) {
    let loc = f.location.as_ref();
    (
        loc.map_or(String::new(), |l| l.file.clone()),
        loc.map_or(0, |l| l.line),
        loc.map_or(0, |l| l.col),
        f.rule_id,
    )
}

/// Renders the always-echoed suppression audit footer for text mode.
#[must_use]
pub fn format_det_suppressions(report: &DetCheckReport) -> String {
    if report.suppressions.is_empty() {
        return "suppressed: none".to_string();
    }
    det_sorted_suppressions(report)
        .iter()
        .map(|s| {
            format!(
                "suppressed: {}:{} {} \"{}\"",
                s.location.file, s.location.line, s.rule_id, s.reason
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Renders the `--list-suppressions` audit listing (AC6): `file:line RULEID "reason"`.
#[must_use]
pub fn format_det_suppressions_list(report: &DetCheckReport) -> String {
    if report.suppressions.is_empty() {
        return "no active suppressions".to_string();
    }
    det_sorted_suppressions(report)
        .iter()
        .map(|s| {
            format!(
                "{}:{} {} \"{}\"",
                s.location.file, s.location.line, s.rule_id, s.reason
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn det_sorted_suppressions(report: &DetCheckReport) -> Vec<&autumn_harvest::DetSuppression> {
    let mut sups: Vec<&autumn_harvest::DetSuppression> = report.suppressions.iter().collect();
    sups.sort_by(|a, b| {
        (
            a.location.file.as_str(),
            a.location.line,
            a.rule_id.as_str(),
        )
            .cmp(&(
                b.location.file.as_str(),
                b.location.line,
                b.rule_id.as_str(),
            ))
    });
    sups
}

/// Serializes the report as pretty JSON (AC2).
///
/// # Errors
/// Returns [`CliError::SerializeResponse`] if serialization fails.
pub fn det_check_json(report: &DetCheckReport) -> Result<String, CliError> {
    serde_json::to_string_pretty(report).map_err(CliError::SerializeResponse)
}

/// Serializes the report's active suppressions as pretty JSON for
/// `--list-suppressions --format json` (the audit inventory as machine-readable
/// output rather than the text listing).
///
/// # Errors
/// Returns [`CliError::SerializeResponse`] if serialization fails.
pub fn det_suppressions_json(report: &DetCheckReport) -> Result<String, CliError> {
    serde_json::to_string_pretty(&json!({ "suppressions": report.suppressions }))
        .map_err(CliError::SerializeResponse)
}

/// `(errors, warnings)` counts across a report's findings.
fn det_check_counts(report: &DetCheckReport) -> (usize, usize) {
    let errors = report
        .findings
        .iter()
        .filter(|f| matches!(f.severity, DetSeverity::Error))
        .count();
    let warnings = report.findings.len() - errors;
    (errors, warnings)
}

/// Decides whether `det-check` should gate (exit non-zero).
///
/// Gates whenever a hard-blocker finding is present, or `deny_warnings` is set
/// and any warning finding is present. Returns the error to surface, or `None`
/// to pass.
#[must_use]
pub fn det_check_gate(report: &DetCheckReport, deny_warnings: bool) -> Option<CliError> {
    let (errors, warnings) = det_check_counts(report);
    if report.has_hard_blockers() || (deny_warnings && warnings > 0) {
        Some(CliError::DetCheckFindings { errors, warnings })
    } else {
        None
    }
}

/// Runs `det-check`: merges reports for `paths`, prints findings (text or JSON)
/// or the suppression listing, and gates the exit code.
///
/// # Errors
/// Returns [`CliError::DetCheckFindings`] when the gate trips (findings are
/// already on stdout), or a read/serialize error.
pub fn run_det_check(
    paths: &[PathBuf],
    format: DetCheckFormat,
    deny_warnings: bool,
    list_suppressions: bool,
) -> Result<(), CliError> {
    let report = det_check_report_for_paths(paths)?;

    if list_suppressions {
        match format {
            DetCheckFormat::Text => println!("{}", format_det_suppressions_list(&report)),
            DetCheckFormat::Json => println!("{}", det_suppressions_json(&report)?),
        }
        return Ok(());
    }

    match format {
        DetCheckFormat::Text => {
            println!("{}", format_det_findings_text(&report));
            println!("{}", format_det_suppressions(&report));
        }
        DetCheckFormat::Json => {
            println!("{}", det_check_json(&report)?);
        }
    }

    if let Some(err) = det_check_gate(&report, deny_warnings) {
        return Err(err);
    }
    Ok(())
}

// ── harvest new: project scaffolding (issue #692) ───────────────────────────

/// Every identifier the scaffold derives from the project `<name>`.
///
/// All fields are a pure function of `<name>`, so a generated project never
/// contains leftover example identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaffoldNames {
    /// The Cargo package name (may contain `-`), verbatim `<name>`.
    pub crate_name: String,
    /// A valid Rust identifier derived from `<name>` (`-` → `_`).
    pub ident: String,
    /// The workflow function name, `{ident}_workflow`.
    pub workflow_fn: String,
    /// The activity function name, `{ident}_activity`.
    pub activity_fn: String,
    /// The activity queue name, `{ident}`.
    pub queue: String,
}

/// Rust keywords (strict + reserved) that must not be used as an identifier.
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
    "use", "where", "while", "async", "await", "abstract", "become", "box", "do", "final", "gen",
    "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "union",
];

/// Cargo/Rust special names that produce confusing or conflicting crates.
const RESERVED_PROJECT_NAMES: &[&str] = &[
    "test",
    "deps",
    "build",
    "core",
    "std",
    "alloc",
    "proc-macro",
    "proc_macro",
    "main",
    "lib",
];

/// The maximum accepted project-name length.
const MAX_PROJECT_NAME_LEN: usize = 64;

/// The embedded `minimal` template: (relative output path, template body).
const MINIMAL_TEMPLATE: &[(&str, &str)] = &[
    (
        "Cargo.toml",
        include_str!("../templates/minimal/Cargo.toml.tmpl"),
    ),
    (
        "src/main.rs",
        include_str!("../templates/minimal/main.rs.tmpl"),
    ),
    (
        "README.md",
        include_str!("../templates/minimal/README.md.tmpl"),
    ),
    (
        "compose.yaml",
        include_str!("../templates/minimal/compose.yaml.tmpl"),
    ),
    (
        "autumn.toml",
        include_str!("../templates/minimal/autumn.toml.tmpl"),
    ),
    (
        ".gitignore",
        include_str!("../templates/minimal/gitignore.tmpl"),
    ),
];

/// Derives a clean, warning-free `snake_case` Rust identifier from a project name.
///
/// Lowercases, maps `-` → `_`, collapses runs of `_`, and trims leading/trailing
/// `_`, so any spec-valid `<name>` (`^[A-Za-z][A-Za-z0-9_-]*$`) yields a valid
/// `snake_case` ident with no `non_snake_case` warnings and no double underscore:
/// `my-app` → `my_app`, `MyApp` → `myapp`, `trail-` → `trail`, `my--app` →
/// `my_app`. A spec-valid name always starts with an ASCII letter, so the result
/// is non-empty and never begins with a digit or `_`.
#[must_use]
pub fn derive_crate_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_underscore = false;
    for c in name.chars() {
        if c == '-' || c == '_' {
            if !prev_underscore {
                out.push('_');
            }
            prev_underscore = true;
        } else {
            out.push(c.to_ascii_lowercase());
            prev_underscore = false;
        }
    }
    out.trim_matches('_').to_string()
}

/// Validates a project name for use as a Cargo package name (from which
/// [`derive_crate_ident`] later derives the scaffold's Rust identifiers).
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] when the name is empty, too long, not a
/// valid Cargo package name (`^[A-Za-z][A-Za-z0-9_-]*$`), a Rust keyword, or a
/// reserved project name.
pub fn validate_project_name(name: &str) -> Result<(), CliError> {
    if name.is_empty() {
        return Err(CliError::InvalidInput(
            "invalid project name: name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_PROJECT_NAME_LEN {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': must be at most {MAX_PROJECT_NAME_LEN} characters"
        )));
    }
    if !name.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': must start with an ASCII letter"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': only ASCII letters, digits, '-' and '_' are allowed"
        )));
    }
    // Keyword/reserved collision is checked against the raw `-` → `_` form (not
    // the case-folded render ident) so name *acceptance* matches cargo-new: a
    // name is rejected only when it is itself a keyword/reserved word, never
    // merely because it case-folds to one (e.g. `MyApp` stays accepted).
    let raw_ident = name.replace('-', "_");
    if RUST_KEYWORDS.contains(&raw_ident.as_str()) || RUST_KEYWORDS.contains(&name) {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': resolves to the Rust keyword '{raw_ident}'"
        )));
    }
    if RESERVED_PROJECT_NAMES.contains(&name)
        || RESERVED_PROJECT_NAMES.contains(&raw_ident.as_str())
    {
        return Err(CliError::InvalidInput(format!(
            "invalid project name '{name}': '{name}' is a reserved name"
        )));
    }
    Ok(())
}

/// Validates `name` and derives every scaffold identifier from it.
///
/// # Errors
///
/// Propagates [`validate_project_name`]'s error for an invalid name.
pub fn derive_names(name: &str) -> Result<ScaffoldNames, CliError> {
    validate_project_name(name)?;
    let ident = derive_crate_ident(name);
    Ok(ScaffoldNames {
        crate_name: name.to_string(),
        workflow_fn: format!("{ident}_workflow"),
        activity_fn: format!("{ident}_activity"),
        queue: ident.clone(),
        ident,
    })
}

/// Replaces every `{{key}}` placeholder in `template` with its value, applying
/// substitutions in the given order.
#[must_use]
pub fn apply_substitutions(template: &str, subs: &[(&str, &str)]) -> String {
    let mut out = template.to_string();
    for (key, value) in subs {
        out = out.replace(key, value);
    }
    out
}

/// Renders the `minimal` template for `names`, returning
/// `(relative_output_path, rendered_content)` for every file to emit.
#[must_use]
pub fn render_minimal(names: &ScaffoldNames) -> Vec<(&'static str, String)> {
    let subs = [
        ("{{crate_name}}", names.crate_name.as_str()),
        ("{{ident}}", names.ident.as_str()),
        ("{{workflow_fn}}", names.workflow_fn.as_str()),
        ("{{activity_fn}}", names.activity_fn.as_str()),
        ("{{queue}}", names.queue.as_str()),
    ];
    MINIMAL_TEMPLATE
        .iter()
        .map(|(path, body)| (*path, apply_substitutions(body, &subs)))
        .collect()
}

/// Returns `true` when `dir` exists and contains at least one entry.
fn dir_is_non_empty(dir: &Path) -> bool {
    fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

/// Scaffolds a new project named `name` into `path` (default `./<name>`).
///
/// Validates the name and renders the template entirely before writing any
/// file, so an invalid name or a non-empty target (without `force`) leaves the
/// filesystem untouched. Never removes files it did not write.
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] for an invalid name or a non-empty target
/// directory without `force`, or [`CliError::WriteOutput`] on an I/O failure.
pub fn run_new(
    name: &str,
    path: Option<&Path>,
    force: bool,
    template: ScaffoldTemplate,
) -> Result<(), CliError> {
    let names = derive_names(name)?;
    let default_path = PathBuf::from(&names.crate_name);
    let target = path.unwrap_or(&default_path);

    // A plain file (or other non-directory) at the target can never be
    // scaffolded into; reject it up front with a clear message rather than
    // letting `create_dir_all` surface an opaque OS error. `--force` cannot
    // help — it only overwrites the scaffold's own files inside a directory.
    if target.exists() && !target.is_dir() {
        return Err(CliError::InvalidInput(format!(
            "target '{}' exists and is not a directory",
            target.display()
        )));
    }

    if !force && dir_is_non_empty(target) {
        return Err(CliError::InvalidInput(format!(
            "target directory '{}' is not empty; pass --force to overwrite",
            target.display()
        )));
    }

    let files = match template {
        ScaffoldTemplate::Minimal => render_minimal(&names),
    };

    // Render is complete and the target passed the safety check: now write.
    fs::create_dir_all(target).map_err(|source| CliError::WriteOutput {
        path: target.display().to_string(),
        source,
    })?;
    for (rel, content) in &files {
        let out = target.join(rel);
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent).map_err(|source| CliError::WriteOutput {
                path: parent.display().to_string(),
                source,
            })?;
        }
        fs::write(&out, content).map_err(|source| CliError::WriteOutput {
            path: out.display().to_string(),
            source,
        })?;
    }

    print_new_next_steps(&names, target);
    Ok(())
}

/// Prints the post-scaffold "next steps" (the three-command run path).
fn print_new_next_steps(names: &ScaffoldNames, target: &Path) {
    println!(
        "Created project '{}' in {}",
        names.crate_name,
        target.display()
    );
    println!();
    println!("Next steps:");
    println!("  cd {}", target.display());
    println!("  docker compose up -d");
    println!("  AUTUMN_PROFILE=dev cargo run");
    println!();
    println!(
        "  # then trigger a run:\n  curl -X POST http://localhost:3000/api/harvest/workflows/{}/start \\",
        names.workflow_fn
    );
    println!(
        "    -H 'Content-Type: application/json' -d '{{\"workflow_id\":\"demo-1\",\"input\":\"World\"}}'"
    );
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

/// Open the SSE stream for `execution_id` and print events to stdout.
///
/// Each complete SSE event block is printed as `<event-type>: <data>`.
/// The function returns when the server sends `event: stream-end` or the
/// connection closes. SSE comment lines (keepalives) are silently discarded.
async fn run_events_tail(
    cli: &Cli,
    execution_id: &str,
    last_event_id: Option<i64>,
) -> Result<(), CliError> {
    let path = format!("/executions/{}", path_segment(execution_id));
    let url = format!(
        "{}{}/events/stream",
        cli.base_url.trim_end_matches('/'),
        path
    );

    let client = reqwest::Client::new();
    let mut builder = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache");

    if let Some(token) = &cli.token {
        builder = builder.bearer_auth(token);
    }
    if let Some(id) = last_event_id {
        builder = builder.header("Last-Event-ID", id.to_string());
    }

    let response = builder.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await?;
        return Err(CliError::Http { status, body });
    }

    let mut response = response;
    let mut buf: Vec<u8> = Vec::new();
    // SSE fields for the current event block.
    let mut ev_id = String::new();
    let mut ev_type = String::new();
    let mut ev_data = String::new();

    loop {
        let chunk = response.chunk().await?;
        let Some(bytes) = chunk else {
            // Server closed the connection.
            break;
        };
        buf.extend_from_slice(&bytes);

        // Process complete lines from buf.
        while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes = &buf[..nl];
            // Strip trailing CR for CRLF line endings.
            let line_bytes = line_bytes.strip_suffix(b"\r").unwrap_or(line_bytes);
            let line = String::from_utf8_lossy(line_bytes).into_owned();
            buf.drain(..=nl);

            if line.is_empty() {
                // Empty line = dispatch event block.
                if !ev_data.is_empty() || !ev_type.is_empty() {
                    let display_type = if ev_type.is_empty() {
                        "message"
                    } else {
                        &ev_type
                    };
                    println!("{display_type}: {ev_data}");
                    if ev_type == "stream-end" {
                        return Ok(());
                    }
                    if ev_type == "stream-error" {
                        return Err(CliError::SseStreamError {
                            message: ev_data.clone(),
                        });
                    }
                }
                ev_id.clear();
                ev_type.clear();
                ev_data.clear();
            } else if line.starts_with(':') {
                // SSE comment (keepalive ping) — discard silently.
            } else {
                let (key, value) = line.find(':').map_or((line.as_str(), ""), |colon_idx| {
                    let (k, mut v) = line.split_at(colon_idx);
                    v = &v[1..];
                    if v.starts_with(' ') {
                        v = &v[1..];
                    }
                    (k, v)
                });
                match key {
                    "id" => {
                        ev_id = value.to_string();
                        let _ = &ev_id; // suppress unused warning; stored for protocol correctness
                    }
                    "event" => {
                        ev_type = value.to_string();
                    }
                    "data" => {
                        if !ev_data.is_empty() {
                            ev_data.push('\n');
                        }
                        ev_data.push_str(value);
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

/// Issue drain then poll `GET /workers/{id}` until status reaches `Stopped`
/// or `wait_timeout_secs` elapses. Prints each poll result as it arrives.
async fn run_worker_drain_wait(
    cli: &Cli,
    worker_id: &str,
    wait_timeout_secs: u64,
) -> Result<(), CliError> {
    // Kick off the drain.
    let response = execute(cli).await?;
    let rendered = render_response(cli, &response)?;
    println!("{rendered}");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_timeout_secs);
    let poll_interval = std::time::Duration::from_secs(2);

    loop {
        tokio::time::sleep(poll_interval).await;

        let poll_cli = Cli {
            base_url: cli.base_url.clone(),
            token: cli.token.clone(),
            actor: cli.actor.clone(),
            request_id: cli.request_id.clone(),
            output: cli.output,
            command: Commands::Worker {
                command: WorkerCommand::Get {
                    worker_id: worker_id.to_string(),
                },
            },
        };
        let worker_value = execute(&poll_cli).await?;
        let status = worker_value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_string();

        let rendered = render_response(cli, &worker_value)?;
        println!("{rendered}");

        if status == "Stopped" {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            return Err(CliError::DrainWaitTimeout {
                worker_id: worker_id.to_string(),
                last_status: status,
            });
        }
    }
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
    let state_filtered = completion_delivery_list_state_filter(cli)
        .map(|state| filter_completion_deliveries_by_state(value, state));
    let value = state_filtered.as_ref().unwrap_or(value);

    if preflight_wants_table(cli) {
        return Ok(format_preflight_table(value));
    }
    if shard_health_wants_table(cli) {
        return Ok(format_shard_health_table(value));
    }
    if canary_wants_table(cli) {
        return Ok(format_canary_table(value));
    }
    if workflow_children_wants_table(cli) {
        return Ok(format_workflow_children_table(value));
    }
    if workflow_summaries_wants_table(cli) {
        return Ok(format_workflow_summaries_table(value));
    }
    if run_chain_wants_table(cli) {
        return Ok(format_run_chain_table(value));
    }
    if batch_preview_wants_table(cli) {
        return Ok(format_batch_preview_table(value));
    }
    if handoff_wants_table(cli) {
        return Ok(format_handoff_table(value));
    }
    if dlq_aggregate_wants_table(cli) {
        return Ok(format_dlq_aggregate_table(value));
    }
    if audit_list_wants_table(cli) {
        return Ok(format_audit_table(value));
    }
    if version_usage_wants_table(cli) {
        return Ok(format_version_usage_table(value));
    }
    if retirement_check_wants_table(cli) {
        return Ok(format_retirement_check_table(value));
    }
    if workflow_reachability_wants_table(cli) {
        return Ok(format_workflow_reachability_table(value));
    }
    if backfill_wants_table(cli) {
        return Ok(format_backfill_table(value));
    }
    if rate_limit_wants_table(cli) {
        return Ok(format_rate_limit_table(value));
    }
    if usage_wants_table(cli) {
        return Ok(format_usage_table(value));
    }

    let output = if workflow_children_wants_raw_json(cli)
        || workflow_summaries_wants_raw_json(cli)
        || run_chain_wants_raw_json(cli)
        || batch_preview_wants_raw_json(cli)
        || handoff_wants_raw_json(cli)
        || dlq_aggregate_wants_raw_json(cli)
        || canary_wants_raw_json(cli)
        || workflow_reachability_wants_raw_json(cli)
        || usage_wants_raw_json(cli)
    {
        OutputFormat::Json
    } else {
        cli.output
    };
    // Issue #756: `render_response` returns ONLY the body. When a list read
    // degraded (a shard was unreachable) the caller emits the partial-
    // availability notice separately on STDERR via `fanout_partial_notice`, so
    // STDOUT stays a clean/parseable body on both paths — `workflow list -o
    // json | jq` is not corrupted by a prepended warning line. (The special
    // table formatters above, usage/dlq_aggregate, render their own
    // unavailable-shard block inline.)
    format_output(value, output)
}

/// Build a human-readable "shard(s) unavailable" notice line from a degraded
/// cross-shard fan-out envelope (issue #756), or `None` when the body is not a
/// degraded envelope (a bare array on the happy path, or an object with an
/// empty/absent `unavailable_shards`).
fn fanout_partial_notice(value: &Value) -> Option<String> {
    let obj = value.as_object()?;
    let unavailable = obj.get("unavailable_shards")?.as_array()?;
    if unavailable.is_empty() {
        return None;
    }
    let status = obj
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("partial");
    let detail: Vec<String> = unavailable
        .iter()
        .map(|shard| {
            let id = cell_number(shard.get("shard_id"));
            let reason = cell_str(shard.get("reason"));
            if reason.is_empty() {
                id
            } else {
                format!("{id}: {reason}")
            }
        })
        .collect();
    Some(format!(
        "WARNING: cross-shard read is {status}; {} shard(s) unavailable: {}",
        unavailable.len(),
        detail.join(", ")
    ))
}

fn usage_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Usage { json: false, .. })
        && cli.output == OutputFormat::PrettyJson
}

const fn usage_wants_raw_json(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Usage { json: true, .. })
}

/// Render the `GET /admin/usage` response as a human-readable table
/// (issue #596). One row per group, plus a header line naming the window,
/// grouping dimension, and status.
fn format_usage_table(value: &Value) -> String {
    let status = cell_str(value.get("status"));
    let from = cell_str(value.get("from"));
    let to = cell_str(value.get("to"));
    let group_by = cell_str(value.get("group_by"));
    let mut summary = format!("status: {status}  window: {from} .. {to}  group_by: {group_by}");

    if let Some(unavailable) = value.get("unavailable_shards").and_then(Value::as_array)
        && !unavailable.is_empty()
    {
        let shard_ids: Vec<String> = unavailable
            .iter()
            .map(|s| cell_number(s.get("shard_id")))
            .collect();
        let _ = write!(summary, "  (unavailable shards: {})", shard_ids.join(","));
    }

    let Some(groups) = value.get("groups").and_then(Value::as_array) else {
        return format!("{summary}\nNo usage groups found.");
    };
    if groups.is_empty() {
        return format!("{summary}\nNo usage groups found.");
    }

    let header: Vec<String> = [
        "GROUP",
        "STARTS",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMED_OUT",
        "ACT_EXEC",
        "ACT_FAILED",
        "COMPUTE_S",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let mut rows = vec![header];
    for group in groups {
        rows.push(vec![
            cell_str(group.get("group")),
            cell_number(group.get("workflow_starts")),
            cell_number(group.get("completed")),
            cell_number(group.get("failed")),
            cell_number(group.get("cancelled")),
            cell_number(group.get("timed_out")),
            cell_number(group.get("activity_executions")),
            cell_number(group.get("activity_executions_failed")),
            format_f64(group.get("activity_compute_seconds")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    format!("{summary}\n\n{table}")
}

fn dlq_aggregate_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Dlq {
            command: DeadLetterCommand::Aggregate { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn dlq_aggregate_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Dlq {
            command: DeadLetterCommand::Aggregate { json: true, .. }
        }
    )
}

/// Render the DLQ aggregation response as a human-readable table.
///
/// One row per group: the hierarchical key columns, the count, the time window,
/// and a comma-joined preview of sample dead-letter IDs.
fn format_dlq_aggregate_table(value: &Value) -> String {
    let total = value.get("total").and_then(Value::as_i64).unwrap_or(0);
    let filtered = value
        .get("filtered_total")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let truncated = value
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let Some(groups) = value.get("groups").and_then(Value::as_array) else {
        return format!("total: {total}  filtered: {filtered}\nNo DLQ groups found.");
    };
    if groups.is_empty() {
        return format!("total: {total}  filtered: {filtered}\nNo DLQ groups found.");
    }

    // Collect the union of key field names (in first-seen order), skipping the
    // `_other` rollup marker so it does not create a phantom column.
    let mut key_cols: Vec<String> = Vec::new();
    for group in groups {
        if let Some(obj) = group.get("key").and_then(Value::as_object) {
            for name in obj.keys() {
                if name != "_other" && !key_cols.iter().any(|c| c == name) {
                    key_cols.push(name.clone());
                }
            }
        }
    }

    let mut header: Vec<String> = key_cols.iter().map(|c| c.to_uppercase()).collect();
    header.push("COUNT".to_string());
    header.push("FIRST_SEEN".to_string());
    header.push("LAST_SEEN".to_string());
    header.push("SAMPLES".to_string());

    let mut rows = vec![header];
    for group in groups {
        let key = group.get("key");
        let is_other = key
            .and_then(|k| k.get("_other"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut row: Vec<String> = Vec::new();
        for (idx, col) in key_cols.iter().enumerate() {
            if is_other {
                // The `_other` rollup has no per-dimension key; label the first
                // column and leave the rest blank.
                row.push(if idx == 0 {
                    "(other)".to_string()
                } else {
                    String::new()
                });
            } else {
                row.push(cell_str(key.and_then(|k| k.get(col))));
            }
        }
        row.push(cell_number(group.get("count")));
        row.push(cell_str(group.get("first_seen")));
        row.push(cell_str(group.get("last_seen")));
        let samples = group
            .get("sample_dead_letter_ids")
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        row.push(samples);
        rows.push(row);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    let mut summary = format!("total: {total}  filtered: {filtered}");
    if truncated {
        summary.push_str("  (long tail rolled into _other)");
    }
    format!("{summary}\n\n{table}")
}

fn backfill_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Schedule {
            command: ScheduleCommand::Backfill { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn rate_limit_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::RateLimit {
            command: RateLimitCommand::Status
        }
    ) && cli.output == OutputFormat::PrettyJson
}

fn format_rate_limit_table(value: &Value) -> String {
    let Some(items) = value.as_array().filter(|v| !v.is_empty()) else {
        return "No rate limit buckets found.".to_string();
    };

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "KEY".to_string(),
        "REFILL_RATE".to_string(),
        "BURST_CAPACITY".to_string(),
        "CURRENT_TOKENS".to_string(),
        "LAST_REFILLED_AT".to_string(),
    ]);

    for item in items {
        rows.push(vec![
            cell_str(item.get("key")),
            format_f64(item.get("refill_rate")),
            format_f64(item.get("burst")),
            format_f64(item.get("tokens")),
            cell_str(item.get("last_refilled_at")),
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

fn format_f64(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_f64)
        .map_or_else(String::new, |number| format!("{number:.2}"))
}

fn format_backfill_table(value: &Value) -> String {
    let status = value.get("status").and_then(Value::as_str).unwrap_or("-");
    let name = value.get("name").and_then(Value::as_str).unwrap_or("-");
    let kind = value.get("kind").and_then(Value::as_str).unwrap_or("-");
    let from = value.get("from").and_then(Value::as_str).unwrap_or("-");
    let to = value.get("to").and_then(Value::as_str).unwrap_or("-");
    let total = value.get("total").and_then(Value::as_u64).unwrap_or(0);
    let dispatched = value.get("dispatched").and_then(Value::as_u64).unwrap_or(0);
    let skipped = value.get("skipped").and_then(Value::as_u64).unwrap_or(0);
    let failed = value.get("failed").and_then(Value::as_u64).unwrap_or(0);

    let mut out = String::new();
    let _ = writeln!(out, "status: {status}  kind: {kind}  name: {name}");
    let _ = writeln!(out, "window: {from} \u{2192} {to}");
    let _ = writeln!(
        out,
        "total: {total}  dispatched: {dispatched}  skipped: {skipped}  failed: {failed}"
    );

    // Skipped reasons
    if let Some(reasons) = value
        .get("skipped_reasons")
        .and_then(Value::as_object)
        .filter(|r| !r.is_empty())
    {
        let parts: Vec<String> = reasons
            .iter()
            .map(|(k, v)| format!("{k}={}", v.as_u64().unwrap_or(0)))
            .collect();
        let _ = writeln!(out, "skipped reasons: {}", parts.join(", "));
    }

    // Planned timestamps
    if let Some(timestamps) = value.get("planned_timestamps").and_then(Value::as_array) {
        if timestamps.is_empty() {
            out.push_str("\nNo timestamps planned.\n");
        } else {
            let _ = writeln!(out, "\nPlanned timestamps ({}):", timestamps.len());
            for ts in timestamps {
                let ts_str = ts.as_str().unwrap_or("-");
                let _ = writeln!(out, "  {ts_str}");
            }
        }
    }

    // Partial shard failures
    if let Some(failures) = value
        .get("partial_shard_failures")
        .and_then(Value::as_array)
        .filter(|f| !f.is_empty())
    {
        out.push_str("\nShard failures:\n");
        for f in failures {
            let shard_id = f.get("shard_id").and_then(Value::as_i64).unwrap_or(-1);
            let reason = f.get("reason").and_then(Value::as_str).unwrap_or("-");
            let _ = writeln!(out, "  shard {shard_id}: {reason}");
        }
    }

    // Paused schedule warning (DAG backfill with include_paused=true)
    if let Some(warning) = value.get("paused_schedule_warning").and_then(Value::as_str) {
        let _ = writeln!(out, "\nWARNING: {warning}");
    }

    out
}

fn preflight_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Preflight) && cli.output == OutputFormat::PrettyJson
}

fn canary_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { json: false, .. })
        && cli.output == OutputFormat::PrettyJson
}

const fn canary_should_gate(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { .. })
}

const fn canary_wants_raw_json(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::Canary { json: true, .. })
}

fn canary_exit_code(value: &Value) -> i32 {
    match value.get("verdict").and_then(Value::as_str) {
        Some("pass") => 0,
        _ => 1,
    }
}

#[allow(clippy::too_many_lines)]
fn format_canary_table(value: &Value) -> String {
    let verdict = value
        .get("verdict")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_uppercase();
    let sampled = value.get("sampled").and_then(Value::as_u64).unwrap_or(0);
    let succeeded = value
        .get("replay_succeeded")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let failed = value
        .get("replay_failed")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let truncated = value
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut out = String::new();
    let _ = writeln!(
        out,
        "Canary Verdict: {verdict}\nSampled: {sampled} (succeeded: {succeeded}, failed: {failed}, truncated: {truncated})"
    );

    // Summary by type
    if let Some(summary_map) = value
        .get("summary_by_type")
        .and_then(Value::as_object)
        .filter(|m| !m.is_empty())
    {
        let mut rows = Vec::with_capacity(summary_map.len() + 1);
        rows.push(vec![
            "WORKFLOW TYPE".to_string(),
            "SAMPLED".to_string(),
            "SUCCEEDED".to_string(),
            "FAILED".to_string(),
        ]);

        // Sort keys for deterministic output
        let mut keys: Vec<&String> = summary_map.keys().collect();
        keys.sort();

        for name in keys {
            let summary = &summary_map[name];
            let s_sampled = summary.get("sampled").and_then(Value::as_u64).unwrap_or(0);
            let s_succeeded = summary
                .get("replay_succeeded")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let s_failed = summary
                .get("replay_failed")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            rows.push(vec![
                name.clone(),
                s_sampled.to_string(),
                s_succeeded.to_string(),
                s_failed.to_string(),
            ]);
        }

        let widths = (0..rows[0].len())
            .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
            .collect::<Vec<_>>();
        let table = rows
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

        let _ = writeln!(out, "\nSummary by Workflow Type:\n{table}");
    }

    // Failure details
    if let Some(details) = value
        .get("details")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
    {
        let mut rows = Vec::with_capacity(details.len() + 1);
        rows.push(vec![
            "EXECUTION ID".to_string(),
            "WORKFLOW TYPE".to_string(),
            "KIND".to_string(),
            "EVENT IDX".to_string(),
            "ERROR".to_string(),
        ]);

        for failure in details {
            let execution_id = failure
                .get("execution_id")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let w_name = failure
                .get("workflow_name")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let kind = failure.get("kind").and_then(Value::as_str).unwrap_or("-");
            let event_idx = failure
                .get("event_index")
                .and_then(Value::as_u64)
                .map_or_else(|| "-".to_string(), |idx| idx.to_string());
            let error = failure.get("error").and_then(Value::as_str).unwrap_or("-");

            rows.push(vec![
                execution_id.to_string(),
                w_name.to_string(),
                kind.to_string(),
                event_idx,
                error.to_string(),
            ]);
        }

        let widths = (0..rows[0].len())
            .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
            .collect::<Vec<_>>();
        let table = rows
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

        let _ = writeln!(out, "\nReplay Failures:\n{table}");

        // Additional diagnostic details (expected vs actual) if present
        for failure in details {
            let expected = failure.get("expected").and_then(Value::as_str);
            let actual = failure.get("actual").and_then(Value::as_str);
            if expected.is_some() || actual.is_some() {
                let exec_id = failure
                    .get("execution_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                let _ = writeln!(out, "\nDiagnostic details for execution {exec_id}:");
                if let Some(exp) = expected {
                    let _ = writeln!(out, "  Expected: {exp}");
                }
                if let Some(act) = actual {
                    let _ = writeln!(out, "  Actual:   {act}");
                }
            }
        }
    }

    out
}

fn shard_health_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Shard {
            command: ShardCommand::Health { .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn shard_health_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Shard {
            command: ShardCommand::Health { .. }
        }
    )
}

fn preflight_exit_code(value: &Value) -> i32 {
    match value.get("overall_status").and_then(Value::as_str) {
        Some("pass") => 0,
        Some("warn") => 2,
        _ => 1,
    }
}

fn shard_health_exit_code(value: &Value) -> i32 {
    let Some(shards) = value.get("shards").and_then(Value::as_array) else {
        return 1;
    };
    let has_non_ready_gate_target = shards.iter().any(|shard| {
        let readiness = shard.get("readiness").and_then(Value::as_str);
        if readiness == Some("ready") {
            return false;
        }
        let candidate = shard
            .get("candidate")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let writable = shard
            .get("roles")
            .and_then(Value::as_array)
            .is_some_and(|roles| roles.iter().any(|role| role.as_str() == Some("writable")));
        candidate || writable
    });
    i32::from(has_non_ready_gate_target)
}

const fn version_usage_should_guard(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionUsage { guard: true, .. })
}

fn version_usage_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionUsage { .. }) && cli.output == OutputFormat::PrettyJson
}

fn version_usage_guard_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    if matches!(status, "partial" | "unavailable") {
        return 1;
    }
    let active = value
        .get("items")
        .and_then(Value::as_array)
        .map_or(0, |items| {
            items
                .iter()
                .filter_map(|item| item.get("active_executions").and_then(Value::as_i64))
                .sum::<i64>()
        });
    i32::from(active > 0)
}

fn format_preflight_table(value: &Value) -> String {
    let overall = value
        .get("overall_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(checks) = value.get("checks").and_then(Value::as_array) else {
        return format!(
            "overall_status: {overall}\nobserved_at: {observed_at}\nNo checks returned."
        );
    };

    let mut rows = Vec::with_capacity(checks.len() + 1);
    rows.push(vec![
        "STATUS".to_string(),
        "CHECK".to_string(),
        "SCOPE".to_string(),
        "SUMMARY".to_string(),
    ]);
    for check in checks {
        let shards = check
            .get("affected_shards")
            .and_then(Value::as_array)
            .filter(|values| !values.is_empty())
            .map_or_else(
                || "-".to_string(),
                |values| {
                    let ids = values
                        .iter()
                        .filter_map(Value::as_i64)
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("shards={ids}")
                },
            );
        rows.push(vec![
            cell_str(check.get("status")),
            cell_str(check.get("name")),
            shards,
            cell_str(check.get("summary")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    format!("overall_status: {overall}\nobserved_at: {observed_at}\n\n{table}")
}

fn format_shard_health_table(value: &Value) -> String {
    let overall = value
        .get("overall_readiness")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(shards) = value.get("shards").and_then(Value::as_array) else {
        return format!(
            "overall_readiness: {overall}\nobserved_at: {observed_at}\nNo shard rows returned."
        );
    };

    let mut rows = Vec::with_capacity(shards.len() + 1);
    rows.push(vec![
        "SHARD".to_string(),
        "ROLES".to_string(),
        "READY".to_string(),
        "REACH".to_string(),
        "SCHEMA".to_string(),
        "WORKERS".to_string(),
        "SCHED".to_string(),
        "QUEUE".to_string(),
        "DLQ".to_string(),
        "BLOCKERS".to_string(),
    ]);
    for shard in shards {
        rows.push(vec![
            cell_number(shard.get("shard_id")),
            roles_cell(shard),
            cell_str(shard.get("readiness")),
            bool_cell(shard.get("reachable")),
            bool_cell(shard.get("schema").and_then(|schema| schema.get("ready"))),
            worker_coverage_cell(shard),
            scheduler_cell(shard),
            cell_number(
                shard
                    .get("queue_depth")
                    .and_then(|summary| summary.get("total_pending")),
            ),
            cell_optional_number(shard.get("dlq").and_then(|summary| summary.get("count"))),
            blockers_cell(shard),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    format!("overall_readiness: {overall}\nobserved_at: {observed_at}\n\n{table}")
}

fn format_version_usage_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo version usage rows returned."
        );
    };
    if items.is_empty() {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo version usage rows found."
        );
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "WORKFLOW".to_string(),
        "CHANGE ID".to_string(),
        "VERSION".to_string(),
        "ACTIVE".to_string(),
        "TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "NEWEST_AGE_S".to_string(),
        "SHARDS".to_string(),
        "UNAVAILABLE".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("workflow_name")),
            cell_str(item.get("change_id")),
            cell_number(item.get("recorded_version")),
            cell_number(item.get("active_executions")),
            cell_number(item.get("terminal_executions")),
            cell_number(item.get("oldest_matching_execution_age_secs")),
            cell_number(item.get("newest_matching_execution_age_secs")),
            shard_array_cell(item, "matched_shards"),
            shard_array_cell(item, "unavailable_shards"),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    format!("status: {status}\nobserved_at: {observed_at}\n\n{table}")
}

// ─── Workflow-type reachability helpers (issue #520) ──────────────────────────

fn workflow_reachability_request(command: &WorkflowTypesCommand) -> ApiRequest {
    let WorkflowTypesCommand::Reachability { workflow_type, .. } = command;
    workflow_type.as_ref().map_or_else(
        || ApiRequest::get("/admin/workflow-types/reachability"),
        |value| {
            ApiRequest::get(format!(
                "/admin/workflow-types/reachability?workflow_type={}",
                query_encode(value)
            ))
        },
    )
}

const fn workflow_reachability_should_gate(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::WorkflowTypes { .. })
}

/// `batch submit --dry-run` renders the preview as a table by default (#769).
///
/// Pass `--json` for the raw preview body. A real submit (no `--dry-run`) falls
/// through to the default JSON renderer, so its `{batch_job_id}` output is
/// unchanged.
fn batch_preview_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Batch {
            command: BatchCommand::Submit {
                dry_run: true,
                json: false,
                ..
            }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn batch_preview_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Batch {
            command: BatchCommand::Submit {
                dry_run: true,
                json: true,
                ..
            }
        }
    )
}

/// Render a #769 dry-run batch preview as a human-readable table.
fn format_batch_preview_table(value: &Value) -> String {
    use std::fmt::Write as _;

    let action = cell_str(value.get("action"));
    let matched = cell_number(value.get("matched_count"));
    let truncated = value
        .get("sample_truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let mut out = String::new();
    out.push_str("DRY RUN — no changes made\n");
    let _ = writeln!(out, "action:        {action}");
    let _ = writeln!(out, "matched_count: {matched}");

    if let Some(per_shard) = value.get("per_shard").and_then(Value::as_array)
        && !per_shard.is_empty()
    {
        out.push_str("per_shard:\n");
        for s in per_shard {
            let _ = writeln!(
                out,
                "  shard {:<4} {}",
                cell_number(s.get("shard_id")),
                cell_number(s.get("matched_count"))
            );
        }
    }

    let sample = value
        .get("sample")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let _ = writeln!(out, "sample ({} shown):", sample.len());
    let _ = writeln!(
        out,
        "  {:<38} {:<24} STATE",
        "EXECUTION_ID", "WORKFLOW_NAME"
    );
    for row in &sample {
        let _ = writeln!(
            out,
            "  {:<38} {:<24} {}",
            cell_str(row.get("execution_id")),
            cell_str(row.get("workflow_name")),
            cell_str(row.get("state")),
        );
    }
    if truncated {
        let _ = writeln!(
            out,
            "(sample truncated: {} of {matched} shown)",
            sample.len()
        );
    }
    out
}

fn workflow_reachability_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::WorkflowTypes {
            command: WorkflowTypesCommand::Reachability { json: false, .. }
        }
    ) && cli.output == OutputFormat::PrettyJson
}

const fn workflow_reachability_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::WorkflowTypes {
            command: WorkflowTypesCommand::Reachability { json: true, .. }
        }
    )
}

/// Exit `2` when the report is unsafe to deploy against:
///
/// - `partial`/`unavailable` cross-shard status: an incomplete answer must never
///   be mistaken for "safe to remove".
/// - Any `orphaned` verdict: a handler was already removed but runs are still live.
/// - When a `--type` filter is active: also block on `in_use`. Without a filter
///   the command is a fleet-wide monitor; `in_use` is the normal state for any
///   type with running workflows and should not block. With a filter the operator
///   is asking "can I delete this specific handler?"; `in_use` means "no" — live
///   runs would become orphaned the moment the handler is removed.
///
/// Exit `0` otherwise.
fn workflow_reachability_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    if matches!(status, "partial" | "unavailable") {
        return 2;
    }
    // `filter` is the echo of the `--type` query param: present → single-type check.
    let type_filter_active = value.get("filter").and_then(Value::as_str).is_some();
    let blocking_verdict = if type_filter_active {
        // Pre-removal check: any non-safe verdict blocks.
        |v: &str| matches!(v, "orphaned" | "in_use")
    } else {
        // Fleet monitor: only already-broken (orphaned) types block.
        |v: &str| v == "orphaned"
    };
    let any_blocking = value
        .get("items")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("verdict")
                    .and_then(Value::as_str)
                    .is_some_and(blocking_verdict)
            })
        });
    if any_blocking { 2 } else { 0 }
}

fn format_workflow_reachability_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return format!(
            "status: {status}\nobserved_at: {observed_at}\nNo workflow types returned."
        );
    };

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "WORKFLOW_TYPE".to_string(),
        "REGISTERED".to_string(),
        "NON_TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "VERDICT".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("workflow_type")),
            bool_cell(item.get("registered")),
            cell_number(item.get("non_terminal_count")),
            cell_number(item.get("oldest_non_terminal_age_secs")),
            cell_str(item.get("verdict")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    let unavailable = value
        .get("shards")
        .and_then(Value::as_array)
        .map(|shards| {
            shards
                .iter()
                .filter(|shard| shard.get("status").and_then(Value::as_str) == Some("unavailable"))
                .filter_map(|shard| shard.get("shard_id").and_then(Value::as_i64))
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let footer = if unavailable.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nWARNING: unavailable shards [{}] — verdicts are provisional, not safe-to-remove.",
            unavailable.join(", ")
        )
    };

    format!("status: {status}\nobserved_at: {observed_at}\n\n{table}{footer}")
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

fn workflow_summaries_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Summaries { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn workflow_summaries_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::Summaries { json: true, .. }
        }
    )
}

fn run_chain_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::RunChain { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn run_chain_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Workflow {
            command: WorkflowCommand::RunChain { json: true, .. }
        }
    )
}

fn handoff_wants_table(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Handoff {
            command:
                HandoffCommand::List { json: false, .. }
                    | HandoffCommand::Inspect { json: false, .. }
        } if cli.output == OutputFormat::PrettyJson
    )
}

const fn handoff_wants_raw_json(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Handoff {
            command: HandoffCommand::List { json: true, .. }
                | HandoffCommand::Inspect { json: true, .. }
        }
    )
}

fn format_handoff_table(value: &Value) -> String {
    let (status, items, coverage) =
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            (
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                items.clone(),
                value.get("shard_coverage"),
            )
        } else if let Some(item) = value.get("item") {
            (
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown"),
                vec![item.clone()],
                value.get("shard_coverage"),
            )
        } else {
            return "No external handoffs found.".to_string();
        };

    if items.is_empty() {
        return format!("status: {status}\nNo external handoffs found.");
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "STATE".to_string(),
        "DEADLINE".to_string(),
        "UPDATED".to_string(),
        "WORKFLOW".to_string(),
        "EXEC ID".to_string(),
        "ACTIVITY".to_string(),
        "SHARD".to_string(),
        "TOKEN".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("state")),
            cell_str(item.get("deadline_at")),
            cell_str(item.get("updated_at")),
            cell_str(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("workflow_name")),
            ),
            cell_str(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("execution_id")),
            ),
            cell_str(
                item.get("activity")
                    .and_then(|activity| activity.get("activity_name")),
            ),
            cell_number(
                item.get("workflow")
                    .and_then(|workflow| workflow.get("shard_id")),
            ),
            cell_str(item.get("token")),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    let coverage = coverage.map_or_else(String::new, handoff_coverage_summary);
    if coverage.is_empty() {
        format!("status: {status}\n\n{table}")
    } else {
        format!("status: {status}\n{coverage}\n\n{table}")
    }
}

fn handoff_coverage_summary(value: &Value) -> String {
    let unavailable = value
        .get("unavailable_shards")
        .and_then(Value::as_array)
        .map_or_else(String::new, |shards| {
            if shards.is_empty() {
                return String::new();
            }
            let cells = shards
                .iter()
                .map(|shard| {
                    let id = cell_number(shard.get("shard_id"));
                    let reason = cell_str(shard.get("reason"));
                    format!("{id}:{reason}")
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("unavailable_shards: {cells}")
        });
    let inspected = shard_array_cell(value, "inspected_shards");
    if unavailable.is_empty() {
        format!("inspected_shards: {inspected}")
    } else {
        format!("inspected_shards: {inspected}\n{unavailable}")
    }
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

fn format_workflow_summaries_table(value: &Value) -> String {
    let Some(items) = value.get("summaries").and_then(Value::as_array) else {
        return "No execution summaries found.".to_string();
    };
    if items.is_empty() {
        return "No execution summaries found.".to_string();
    }

    let mut rows = Vec::with_capacity(items.len() + 1);
    rows.push(vec![
        "EXEC ID".to_string(),
        "WORKFLOW".to_string(),
        "WORKFLOW ID".to_string(),
        "STATE".to_string(),
        "COMPLETED".to_string(),
        "DURATION_MS".to_string(),
        "SHARD".to_string(),
    ]);
    for item in items {
        rows.push(vec![
            cell_str(item.get("execution_id")),
            cell_str(item.get("workflow_name")),
            cell_str(item.get("workflow_id")),
            cell_str(item.get("state")),
            cell_str(item.get("completed_at")),
            cell_optional_number(item.get("duration_ms")),
            cell_number(item.get("shard_id")),
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

fn format_run_chain_table(value: &Value) -> String {
    let Some(runs) = value.get("runs").and_then(Value::as_array) else {
        return "No run chain found.".to_string();
    };
    if runs.is_empty() {
        return "No run chain found.".to_string();
    }

    let mut rows = Vec::with_capacity(runs.len() + 1);
    rows.push(vec![
        "SEQ".to_string(),
        "EXEC ID".to_string(),
        "RUN ID".to_string(),
        "STATE".to_string(),
        "OUTCOME".to_string(),
        "STARTED".to_string(),
        "COMPLETED".to_string(),
        "CONTINUED TO".to_string(),
    ]);
    for run in runs {
        rows.push(vec![
            cell_number(run.get("sequence")),
            cell_str(run.get("exec_id")),
            cell_str(run.get("run_id")),
            cell_str(run.get("state")),
            cell_str(run.get("outcome")),
            cell_str(run.get("started_at")),
            cell_optional_str(run.get("completed_at")),
            cell_optional_str(run.get("continued_to_exec_id")),
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

    if let Some(workflow_id) = value.get("workflow_id").and_then(Value::as_str) {
        rendered = format!("workflow_id: {workflow_id}\n{rendered}");
    }
    if value
        .get("head_unknown")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        rendered.push_str(
            "\nnote: head_unknown — the chain participates in continue-as-new but its \
             true origin could not be proven (legacy rows lacking back-links); the first \
             run shown is a best-effort head.",
        );
    }
    rendered
}

fn format_audit_table(value: &Value) -> String {
    let Some(items) = value.as_array().filter(|v| !v.is_empty()) else {
        return "No audit records found.".to_string();
    };

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

fn cell_optional_number(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_i64)
        .map_or_else(|| "-".to_string(), |number| number.to_string())
}

fn shard_array_cell(item: &Value, field: &str) -> String {
    let Some(values) = item
        .get("shard_coverage")
        .and_then(|coverage| coverage.get(field))
        .and_then(Value::as_array)
    else {
        return "-".to_string();
    };
    if values.is_empty() {
        return "-".to_string();
    }
    values
        .iter()
        .filter_map(Value::as_i64)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn bool_cell(value: Option<&Value>) -> String {
    match value.and_then(Value::as_bool) {
        Some(true) => "yes".to_string(),
        Some(false) => "no".to_string(),
        None => "-".to_string(),
    }
}

fn roles_cell(shard: &Value) -> String {
    let mut roles = shard
        .get("roles")
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |roles| {
            roles
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });
    if shard
        .get("candidate")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        roles.push("candidate".to_string());
    }
    if roles.is_empty() {
        "-".to_string()
    } else {
        roles.join(",")
    }
}

fn worker_coverage_cell(shard: &Value) -> String {
    let worker_counts = shard_worker_counts_cell(shard);
    let Some(coverage) = shard.get("worker_coverage").and_then(Value::as_array) else {
        return worker_counts.unwrap_or_else(|| "-".to_string());
    };
    if coverage.is_empty() {
        return worker_counts.unwrap_or_else(|| "-".to_string());
    }
    let queue_coverage = coverage
        .iter()
        .map(|queue| {
            let name = queue.get("queue").and_then(Value::as_str).unwrap_or("?");
            let healthy = queue
                .get("healthy_active")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let ready = queue.get("ready").and_then(Value::as_bool).unwrap_or(false);
            let mark = if ready { "ok" } else { "miss" };
            format!("{name}:{healthy}/{mark}")
        })
        .collect::<Vec<_>>()
        .join(",");
    if let Some(counts) = worker_counts {
        format!("{counts} {queue_coverage}")
    } else {
        queue_coverage
    }
}

fn shard_worker_counts_cell(shard: &Value) -> Option<String> {
    let active = shard.get("active_worker_count").and_then(Value::as_i64)?;
    let stale = shard.get("stale_worker_count").and_then(Value::as_i64)?;
    Some(format!("active={active} stale={stale}"))
}

fn scheduler_cell(shard: &Value) -> String {
    let Some(scheduler) = shard.get("scheduler") else {
        return "-".to_string();
    };
    if !scheduler
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "off".to_string();
    }
    if scheduler
        .get("ready")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "ok".to_string()
    } else {
        "stale".to_string()
    }
}

fn blockers_cell(shard: &Value) -> String {
    let Some(reasons) = shard.get("blocking_reasons").and_then(Value::as_array) else {
        return "-".to_string();
    };
    if reasons.is_empty() {
        return "-".to_string();
    }
    reasons
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("; ")
}

fn shard_request(command: &ShardCommand) -> ApiRequest {
    match command {
        ShardCommand::Health {
            candidate_shard, ..
        } => candidate_shard.as_ref().map_or_else(
            || ApiRequest::get("/admin/shards/health"),
            |shard| ApiRequest::get(format!("/admin/shards/health?candidate_shard={shard}")),
        ),
    }
}

fn canary_request(
    sample_size: usize,
    workflow_name: Option<&str>,
    queue: Option<&str>,
) -> ApiRequest {
    ApiRequest::post(
        "/admin/workflows/replay-canary",
        Some(json!({
            "sample_size": sample_size,
            "workflow_name": workflow_name,
            "queue_name": queue,
        })),
    )
}

fn usage_request(from: &str, to: &str, group_by: Option<&str>) -> ApiRequest {
    let mut params: Vec<(&'static str, String)> =
        vec![("from", from.to_string()), ("to", to.to_string())];
    if let Some(value) = group_by {
        params.push(("group_by", value.to_string()));
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/usage?{encoded}"))
}

fn version_usage_request(
    workflow_name: Option<&str>,
    change_id: Option<&str>,
    recorded_version: Option<u32>,
    state_group: Option<VersionUsageStateGroup>,
    shard_id: Option<i32>,
    guard: bool,
) -> ApiRequest {
    let state_group = if guard {
        VersionUsageStateGroup::Active
    } else {
        state_group.unwrap_or(VersionUsageStateGroup::All)
    };
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(value) = workflow_name {
        params.push(("workflow_name", value.to_string()));
    }
    if let Some(value) = change_id {
        params.push(("change_id", value.to_string()));
    }
    if let Some(value) = recorded_version {
        params.push(("recorded_version", value.to_string()));
    }
    if state_group != VersionUsageStateGroup::All {
        params.push(("state_group", state_group.as_wire().to_string()));
    }
    if let Some(value) = shard_id {
        params.push(("shard_id", value.to_string()));
    }

    if params.is_empty() {
        return ApiRequest::get("/admin/version-gates/usage");
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/version-gates/usage?{encoded}"))
}

#[allow(clippy::too_many_lines)]
fn workflow_request(command: &WorkflowCommand) -> Result<ApiRequest, CliError> {
    match command {
        WorkflowCommand::List {
            limit,
            state,
            workflow_name,
            search_attr,
            search_attr_filter,
            owner,
            no_progress_minutes,
            include_sleeping,
            start_source,
        } => Ok(ApiRequest::get(build_workflow_list_path(
            *limit,
            state,
            workflow_name.as_deref(),
            search_attr,
            search_attr_filter,
            owner.as_deref(),
            *no_progress_minutes,
            *include_sleeping,
            start_source.as_deref(),
        )?)),
        WorkflowCommand::Summaries {
            workflow_name,
            workflow_id,
            state,
            completed_after,
            completed_before,
            search_attr,
            limit,
            cursor,
            order,
            json: _,
        } => Ok(ApiRequest::get(build_summary_list_path(
            workflow_name.as_deref(),
            workflow_id.as_deref(),
            state,
            completed_after.as_deref(),
            completed_before.as_deref(),
            search_attr,
            *limit,
            cursor.as_deref(),
            order.as_deref(),
        )?)),
        WorkflowCommand::Get { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Stack { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/stack",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Timeline { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/timeline",
            path_segment(execution_id)
        ))),
        WorkflowCommand::Awaitables { execution_id } => Ok(ApiRequest::get(format!(
            "/workflows/{}/awaitables",
            path_segment(execution_id)
        ))),
        WorkflowCommand::RunChain {
            execution_id,
            json: _,
        } => Ok(ApiRequest::get(format!(
            "/workflows/{}/run-chain",
            path_segment(execution_id)
        ))),
        WorkflowCommand::ReplayDiagnosis { execution_id } => Ok(ApiRequest::post(
            format!("/workflows/{}/replay-diagnosis", path_segment(execution_id)),
            None,
        )),
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
            conflict_policy,
            start_at,
            delay,
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
            insert_string(&mut body, "conflict_policy", conflict_policy.as_deref());
            insert_string(&mut body, "start_at", start_at.as_deref());
            insert_string(&mut body, "delay", delay.as_deref());

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
        WorkflowCommand::Pause {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/pause", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::Resume { execution_id } => Ok(ApiRequest::post(
            format!("/workflows/{}/resume", path_segment(execution_id)),
            None,
        )),
        WorkflowCommand::ErasePayloads {
            execution_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!("/workflows/{}/erase-payloads", path_segment(execution_id)),
                Some(Value::Object(body)),
            ))
        }
        WorkflowCommand::RetryActivity {
            workflow_id,
            activity_exec_id,
        } => Ok(ApiRequest::post(
            format!(
                "/workflows/{}/activities/{}/retry-now",
                path_segment(workflow_id),
                path_segment(activity_exec_id)
            ),
            None,
        )),
        WorkflowCommand::FailActivity {
            workflow_id,
            activity_exec_id,
            reason,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", reason.as_deref());
            Ok(ApiRequest::post(
                format!(
                    "/workflows/{}/activities/{}/fail-now",
                    path_segment(workflow_id),
                    path_segment(activity_exec_id)
                ),
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
            idempotency_key,
        } => {
            let payload = parse_json_source(
                payload_json.as_deref(),
                payload_file.as_deref(),
                "signal payload",
            )?
            .unwrap_or_else(|| json!({}));
            // The exactly-once delivery key rides the ?idempotency_key= query
            // param (issue #521's out-of-band surface) — the request body must
            // stay the raw signal payload, so the key is never smuggled into it.
            let suffix = idempotency_key
                .as_deref()
                .map(|key| format!("?idempotency_key={}", query_encode(key)))
                .unwrap_or_default();
            Ok(ApiRequest::post(
                format!(
                    "/workflows/{}/signal/{}{suffix}",
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
        WorkflowCommand::Handlers { workflow_name } => Ok(ApiRequest::get(format!(
            "/workflows/types/{}/handlers",
            path_segment(workflow_name),
        ))),
        WorkflowCommand::BatchReset {
            filter_json,
            filter_file,
            event_id,
            first_activity,
            last_workflow_task,
            reason,
            operator_id,
            signal_reapply,
            preview,
        } => {
            let filter = parse_json_source(
                filter_json.as_deref(),
                filter_file.as_deref(),
                "batch reset filter",
            )?
            .ok_or_else(|| {
                CliError::InvalidInput(
                    "one of --filter-json or --filter-file is required".to_string(),
                )
            })?;
            let reset_point = if let Some(id) = event_id {
                json!({"type": "event_id", "event_id": id})
            } else if let Some(name) = first_activity {
                json!({"type": "first_activity_run", "activity_name": name})
            } else if *last_workflow_task {
                json!({"type": "last_workflow_task"})
            } else {
                return Err(CliError::InvalidInput(
                    "one of --event-id, --first-activity, or --last-workflow-task is required"
                        .to_string(),
                ));
            };
            let mut body = serde_json::Map::new();
            body.insert("filter".to_string(), filter);
            body.insert("reset_point".to_string(), reset_point);
            body.insert("reason".to_string(), Value::String(reason.clone()));
            body.insert(
                "operator_id".to_string(),
                Value::String(operator_id.clone()),
            );
            body.insert(
                "signal_reapply".to_string(),
                Value::String(signal_reapply.as_wire().to_string()),
            );
            body.insert("preview".to_string(), Value::Bool(*preview));
            Ok(ApiRequest::post(
                "/workflows/batch_reset".to_string(),
                Some(Value::Object(body)),
            ))
        }
    }
}

fn history_request(command: &HistoryCommand) -> ApiRequest {
    match command {
        HistoryCommand::Export {
            execution_id,
            payload_policy,
            max_bytes,
            output_file: _,
        } => {
            let mut params = vec![("payload_policy", payload_policy.as_wire().to_string())];
            if let Some(value) = max_bytes {
                params.push(("max_bytes", value.to_string()));
            }
            ApiRequest::get(format!(
                "/workflows/{}/history/export?{}",
                path_segment(execution_id),
                encode_query_params(&params)
            ))
        }
        HistoryCommand::ExportBatch {
            workflow_name,
            state_group,
            updated_after,
            updated_before,
            shard_id,
            limit,
            payload_policy,
            max_bytes,
            output_file: _,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(value) = workflow_name {
                params.push(("workflow_name", value.clone()));
            }
            if let Some(value) = state_group {
                params.push(("state_group", value.as_wire().to_string()));
            }
            if let Some(value) = updated_after {
                params.push(("updated_after", value.clone()));
            }
            if let Some(value) = updated_before {
                params.push(("updated_before", value.clone()));
            }
            if let Some(value) = shard_id {
                params.push(("shard_id", value.to_string()));
            }
            if let Some(value) = limit {
                params.push(("limit", value.to_string()));
            }
            params.push(("payload_policy", payload_policy.as_wire().to_string()));
            if let Some(value) = max_bytes {
                params.push(("max_bytes", value.to_string()));
            }

            ApiRequest::get(format!(
                "/admin/history/exports?{}",
                encode_query_params(&params)
            ))
        }
    }
}

fn handoff_request(command: &HandoffCommand) -> Result<ApiRequest, CliError> {
    match command {
        HandoffCommand::List {
            state,
            workflow_name,
            execution_id,
            activity_name,
            token,
            shard_id,
            due_before,
            updated_before,
            limit,
            json: _,
        } => Ok(ApiRequest::get(build_handoff_list_path(
            state,
            workflow_name.as_deref(),
            execution_id.as_deref(),
            activity_name.as_deref(),
            token.as_deref(),
            *shard_id,
            due_before.as_deref(),
            updated_before.as_deref(),
            *limit,
        ))),
        HandoffCommand::Inspect { token, json: _ } => Ok(ApiRequest::get(format!(
            "/admin/external-handoffs/{}",
            path_segment(token)
        ))),
        HandoffCommand::Complete {
            token,
            output_json,
            output_file,
            request_json,
            request_file,
        } => complete_handoff_request(
            token,
            output_json.as_deref(),
            output_file.as_deref(),
            request_json.as_deref(),
            request_file.as_deref(),
        ),
        HandoffCommand::Fail {
            token,
            error,
            error_json,
            error_file,
            request_json,
            request_file,
            retryable,
        } => fail_handoff_request(
            token,
            error.as_deref(),
            error_json.as_deref(),
            error_file.as_deref(),
            request_json.as_deref(),
            request_file.as_deref(),
            *retryable,
        ),
        HandoffCommand::Heartbeat {
            token,
            extend_by_secs,
            request_json,
            request_file,
        } => heartbeat_handoff_request(
            token,
            *extend_by_secs,
            request_json.as_deref(),
            request_file.as_deref(),
        ),
    }
}

fn complete_handoff_request(
    token: &str,
    output_json: Option<&str>,
    output_file: Option<&Path>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
) -> Result<ApiRequest, CliError> {
    let request_body = parse_json_source(request_json, request_file, "handoff complete request")?;
    let body = if let Some(request_body) = request_body {
        request_body
    } else {
        let output = parse_json_source(output_json, output_file, "handoff completion output")?
            .unwrap_or(Value::Null);
        json!({ "output": output })
    };
    Ok(ApiRequest::post(
        format!("/activities/external/{}/complete", path_segment(token)),
        Some(body),
    ))
}

fn fail_handoff_request(
    token: &str,
    error: Option<&str>,
    error_json: Option<&str>,
    error_file: Option<&Path>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
    retryable: bool,
) -> Result<ApiRequest, CliError> {
    let request = parse_json_source(request_json, request_file, "handoff fail request")?;
    let body = if let Some(request) = request {
        request
    } else {
        let error_value = parse_json_source(error_json, error_file, "handoff error")?;
        let error = error_value.map_or_else(
            || error.unwrap_or("external handoff failed").to_string(),
            stringify_handoff_error,
        );
        json!({ "error": error, "retryable": retryable })
    };
    Ok(ApiRequest::post(
        format!("/activities/external/{}/fail", path_segment(token)),
        Some(body),
    ))
}

fn stringify_handoff_error(value: Value) -> String {
    match value {
        Value::String(raw) => raw,
        other => serde_json::to_string(&other).unwrap_or_else(|_| other.to_string()),
    }
}

fn heartbeat_handoff_request(
    token: &str,
    extend_by_secs: Option<u64>,
    request_json: Option<&str>,
    request_file: Option<&Path>,
) -> Result<ApiRequest, CliError> {
    let body = parse_json_source(request_json, request_file, "handoff heartbeat request")?
        .unwrap_or_else(|| {
            let mut body = Map::new();
            if let Some(secs) = extend_by_secs {
                body.insert("extend_by_secs".to_string(), json!(secs));
            }
            Value::Object(body)
        });
    Ok(ApiRequest::post(
        format!("/activities/external/{}/heartbeat", path_segment(token)),
        Some(body),
    ))
}

fn dag_request(command: &DagCommand, actor: Option<&str>) -> Result<ApiRequest, CliError> {
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
        DagCommand::Retry {
            dag_name,
            run_exec_id,
            from_node,
            reason,
            operator_id,
            dry_run,
        } => {
            let operator = operator_id
                .as_deref()
                .or(actor)
                .unwrap_or("cli")
                .to_string();
            Ok(ApiRequest::post(
                format!(
                    "/dags/{}/runs/{}/retry",
                    path_segment(dag_name),
                    path_segment(run_exec_id)
                ),
                Some(json!({
                    "from_nodes": from_node,
                    "reason": reason,
                    "operator_id": operator,
                    "dry_run": dry_run,
                })),
            ))
        }
    }
}

#[allow(clippy::too_many_lines)]
fn schedule_request(command: &ScheduleCommand) -> Result<ApiRequest, CliError> {
    match command {
        ScheduleCommand::List => Ok(ApiRequest::get("/admin/schedules")),
        ScheduleCommand::Backfill {
            id,
            from,
            to,
            dry_run,
            max_count,
            include_paused,
        } => {
            let mut body = Map::new();
            body.insert("from".to_string(), Value::String(from.clone()));
            body.insert("to".to_string(), Value::String(to.clone()));
            body.insert("dry_run".to_string(), json!(dry_run));
            body.insert("include_paused".to_string(), json!(include_paused));
            if let Some(count) = max_count {
                body.insert("max_count".to_string(), json!(count));
            }
            Ok(ApiRequest::post(
                format!("/admin/schedules/{}/backfill", path_segment(id)),
                Some(Value::Object(body)),
            ))
        }
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
        ScheduleCommand::Update {
            id,
            cron,
            interval_secs,
            manual,
            tz,
            input_json,
            queue,
            overlap_policy,
            buffer_all_max,
            catchup_policy,
            catchup_window_secs,
            jitter_secs,
            max_active_runs,
            calendar,
            clear_calendar,
            end_at,
            clear_end_at,
            max_runs,
            clear_max_runs,
        } => {
            let mut body = Map::new();
            // Exactly one of --cron / --interval-secs / --manual (clap enforces
            // the mutual exclusion); all optional — omitting keeps the cadence.
            if let Some(expr) = cron {
                body.insert("schedule_expr".to_string(), Value::String(expr.clone()));
            } else if let Some(secs) = interval_secs {
                body.insert(
                    "schedule_expr".to_string(),
                    Value::String(format!("interval:{secs}")),
                );
            } else if *manual {
                body.insert("schedule_expr".to_string(), Value::String("manual".into()));
            }
            if let Some(tz) = tz {
                body.insert("timezone".to_string(), Value::String(tz.clone()));
            }
            if let Some(input) = parse_json_source(input_json.as_deref(), None, "input")? {
                body.insert("input".to_string(), input);
            }
            if let Some(queue) = queue {
                body.insert("queue_name".to_string(), Value::String(queue.clone()));
            }
            if let Some(policy) = overlap_policy {
                body.insert("overlap_policy".to_string(), Value::String(policy.clone()));
            }
            if let Some(max) = buffer_all_max {
                body.insert("buffer_all_max".to_string(), json!(max));
            }
            if let Some(policy) = catchup_policy {
                body.insert("catchup_policy".to_string(), Value::String(policy.clone()));
            }
            if let Some(secs) = catchup_window_secs {
                body.insert("catchup_window_secs".to_string(), json!(secs));
            }
            if let Some(secs) = jitter_secs {
                body.insert("jitter_secs".to_string(), json!(secs));
            }
            if let Some(max) = max_active_runs {
                body.insert("max_active_runs".to_string(), json!(max));
            }
            // Tri-state nullable fields: --clear-* sends an explicit JSON null.
            if let Some(name) = calendar {
                body.insert("calendar".to_string(), Value::String(name.clone()));
            } else if *clear_calendar {
                body.insert("calendar".to_string(), Value::Null);
            }
            if let Some(ts) = end_at {
                body.insert("end_at".to_string(), Value::String(ts.clone()));
            } else if *clear_end_at {
                body.insert("end_at".to_string(), Value::Null);
            }
            if let Some(max) = max_runs {
                body.insert("max_runs".to_string(), json!(max));
            } else if *clear_max_runs {
                body.insert("max_runs".to_string(), Value::Null);
            }
            Ok(ApiRequest {
                method: ApiMethod::Patch,
                path: format!("/admin/schedules/{}", path_segment(id)),
                body: Some(Value::Object(body)),
            })
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
        ScheduleCommand::TriggerNow { id, reason, force } => {
            let mut body = serde_json::Map::new();
            if let Some(r) = reason {
                body.insert("reason".to_string(), Value::String(r.clone()));
            }
            let mut path = format!("/admin/schedules/{}/trigger", path_segment(id));
            if *force {
                path.push_str("?force=true");
            }
            Ok(ApiRequest::post(path, Some(Value::Object(body))))
        }
        ScheduleCommand::Runs {
            id,
            state,
            origin,
            since,
            until,
            limit,
            cursor,
        } => {
            let mut params: Vec<(&str, String)> = Vec::new();
            for s in state {
                params.push(("state", s.clone()));
            }
            for o in origin {
                params.push(("origin", o.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = until {
                params.push(("until", v.clone()));
            }
            if let Some(v) = limit {
                params.push(("limit", v.to_string()));
            }
            if let Some(v) = cursor {
                params.push(("cursor", v.clone()));
            }
            let mut path = format!("/admin/schedules/{}/runs", path_segment(id));
            if !params.is_empty() {
                path.push('?');
                path.push_str(&encode_query_params(&params));
            }
            Ok(ApiRequest::get(path))
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

fn legal_hold_request(command: &LegalHoldCommand) -> ApiRequest {
    match command {
        LegalHoldCommand::Set {
            execution_id,
            reason,
            until,
        } => {
            let mut body = Map::new();
            insert_string(&mut body, "reason", Some(reason.as_str()));
            insert_string(&mut body, "hold_until", until.as_deref());
            ApiRequest::post(
                format!("/workflows/{}/legal-hold", path_segment(execution_id)),
                Some(Value::Object(body)),
            )
        }
        LegalHoldCommand::Release { execution_id } => ApiRequest::post(
            format!(
                "/workflows/{}/legal-hold/release",
                path_segment(execution_id)
            ),
            None,
        ),
    }
}

/// True when the command is a queue pause/resume, whose response carries a
/// partial-application contract worth gating the exit code on.
///
/// `list-paused` is a read with no such contract, so it is deliberately excluded.
const fn queue_mutation_should_gate(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::Queue {
            command: QueueCommand::Pause { .. } | QueueCommand::Resume { .. }
        }
    )
}

/// Exit code for a queue pause/resume response.
///
/// A `207` partial fleet-wide hold is NOT in effect on the shards it missed --
/// those keep dispatching into exactly the outage the operator is riding out --
/// so it must never look like success to a script or a runbook step. `execute`
/// only rejects non-2xx statuses, and `207` IS 2xx, hence this body-level gate
/// (the same shape as `preflight_exit_code` and friends).
///
/// Fails closed: a body carrying neither signal is not a queue-mutation response
/// we can vouch for, so it is reported as a failure rather than as a hold that
/// may not hold.
fn queue_mutation_exit_code(value: &Value) -> i32 {
    let ok = value.get("ok").and_then(Value::as_bool);
    let complete = value.get("status").and_then(Value::as_str) == Some("complete");
    i32::from(!(ok == Some(true) && complete))
}

/// True when `raw` names a WHATWG URL dot-segment.
///
/// The URL parser reqwest uses strips single-dot (`.`, `%2e`) and double-dot
/// (`..`, `.%2e`, `%2e.`, `%2e%2e`) segments, all ASCII-case-insensitively,
/// when the request URL is parsed -- which happens *after* `ApiRequest.path` is
/// assembled, so `path_segment` cannot encode its way out of the LITERAL forms:
/// `.` on `/admin/queues/{q}/pause` silently resolves to `/admin/queues/pause`
/// and `..` to `/admin/pause`, retargeting the request at a different route.
/// They are therefore rejected up front.
///
/// The percent-encoded forms are, today, already neutralized a layer down --
/// `PATH_SEGMENT_ENCODE_SET` encodes `%`, so a queue literally named `%2e`
/// reaches the URL as `%252e` and survives intact. They are matched here anyway
/// so this guard stays correct on its own terms rather than silently depending
/// on that encode set keeping `%`.
fn is_url_dot_segment(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "." | "%2e" | ".." | ".%2e" | "%2e." | "%2e%2e"
    )
}

/// Reject a queue name that cannot survive URL path parsing intact.
fn checked_queue_segment(queue_name: &str) -> Result<String, CliError> {
    if is_url_dot_segment(queue_name) {
        return Err(CliError::QueueNameDotSegment {
            value: queue_name.to_string(),
        });
    }
    Ok(path_segment(queue_name))
}

/// Map `harvest queue …` onto the three management routes (issue #619).
///
/// `--shard-id` is omitted from the body entirely when unset, so the default is
/// a fleet-wide hold rather than a shard-scoped one.
fn queue_request(command: &QueueCommand) -> Result<ApiRequest, CliError> {
    match command {
        QueueCommand::Pause {
            queue_name,
            reason,
            shard_id,
        } => {
            let segment = checked_queue_segment(queue_name)?;
            let mut body = Map::new();
            body.insert("reason".to_string(), Value::String(reason.clone()));
            if let Some(shard) = shard_id {
                body.insert("shard_id".to_string(), Value::from(*shard));
            }
            Ok(ApiRequest::post(
                format!("/admin/queues/{segment}/pause"),
                Some(Value::Object(body)),
            ))
        }
        QueueCommand::Resume {
            queue_name,
            shard_id,
        } => {
            let segment = checked_queue_segment(queue_name)?;
            let mut body = Map::new();
            if let Some(shard) = shard_id {
                body.insert("shard_id".to_string(), Value::from(*shard));
            }
            Ok(ApiRequest::post(
                format!("/admin/queues/{segment}/resume"),
                Some(Value::Object(body)),
            ))
        }
        QueueCommand::ListPaused => Ok(ApiRequest::get("/admin/queues/paused")),
    }
}

fn rate_limit_request(command: &RateLimitCommand) -> ApiRequest {
    match command {
        RateLimitCommand::Status => ApiRequest::get("/admin/rate-limits"),
        RateLimitCommand::Set {
            key,
            refill_rate,
            burst,
        } => ApiRequest::post(
            format!("/admin/rate-limits/{}", path_segment(key)),
            Some(json!({
                "refill_rate": refill_rate,
                "burst": burst,
            })),
        ),
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
            dry_run,
            json: _,
        } => {
            // A non-dry-run Signal submit requires signal_name (enforced here
            // rather than via clap's `required_if_eq`, so a `Signal --dry-run`
            // preview — blast radius only, not signal validity — may omit it).
            if action == "Signal" && signal_name.is_none() && !*dry_run {
                return Err(CliError::InvalidInput(
                    "--signal-name is required for action Signal (unless --dry-run)".to_string(),
                ));
            }
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
            // Only add the key when set, so a real-submit body stays
            // byte-identical to a pre-#769 client (issue #769).
            if *dry_run {
                body.insert("dry_run".to_string(), json!(true));
            }
            Ok(ApiRequest::post(
                "/batch-operations",
                Some(Value::Object(body)),
            ))
        }
    }
}

/// Build the `POST /workflows/batch_start` request (issue #357).
///
/// Reads NDJSON items from `file` or parses `items_json` as a JSON array,
/// then wraps them in `{ "items": [...], "atomic": <bool> }`.
fn start_batch_request(
    file: Option<&Path>,
    items_json: Option<&str>,
    atomic: bool,
) -> Result<ApiRequest, CliError> {
    let items: Value = match (file, items_json) {
        (Some(path), None) => {
            // NDJSON: one JSON object per non-empty line.
            let raw = read_json_file(path, "NDJSON items")?;
            let mut arr = Vec::new();
            for line in raw.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let item: Value =
                    serde_json::from_str(trimmed).map_err(|source| CliError::InvalidJson {
                        label: "NDJSON items",
                        source,
                    })?;
                arr.push(item);
            }
            Value::Array(arr)
        }
        (None, Some(inline)) => {
            serde_json::from_str(inline).map_err(|source| CliError::InvalidJson {
                label: "items JSON",
                source,
            })?
        }
        (None, None) => {
            return Err(CliError::MissingInput {
                label: "start-batch requires --file or --items-json",
            });
        }
        (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents both being set"),
    };

    let body = json!({
        "items": items,
        "atomic": atomic,
    });
    Ok(ApiRequest::post("/workflows/batch_start", Some(body)))
}

#[allow(clippy::too_many_lines)]
/// Builds the request for `harvest completion-delivery` subcommands (issue
/// #605). `List`'s `--state` filter is applied client-side in
/// `render_response` (the server has no query-param filter for this
/// endpoint), so it never reaches the request path/body here.
fn completion_delivery_request(command: &CompletionDeliveryCommand) -> ApiRequest {
    match command {
        CompletionDeliveryCommand::List {
            execution_id,
            state: _,
        } => ApiRequest::get(format!(
            "/workflows/{}/completion-deliveries",
            path_segment(execution_id)
        )),
        CompletionDeliveryCommand::Redrive {
            execution_id,
            delivery_id,
        } => ApiRequest::post(
            format!(
                "/workflows/{}/completion-deliveries/{}/redrive",
                path_segment(execution_id),
                path_segment(delivery_id)
            ),
            None,
        ),
    }
}

/// The `--state` filter for `harvest completion-delivery list`, if the
/// current command is that one and the flag was supplied.
const fn completion_delivery_list_state_filter(cli: &Cli) -> Option<&str> {
    match &cli.command {
        Commands::CompletionDelivery {
            command:
                CompletionDeliveryCommand::List {
                    state: Some(state), ..
                },
        } => Some(state.as_str()),
        _ => None,
    }
}

/// Filter a `GET .../completion-deliveries` JSON array response down to rows
/// whose `state` field matches `state` case-insensitively. Passes non-array
/// values through unchanged (defensive; the endpoint always returns an
/// array on success).
fn filter_completion_deliveries_by_state(value: &Value, state: &str) -> Value {
    let Some(rows) = value.as_array() else {
        return value.clone();
    };
    Value::Array(
        rows.iter()
            .filter(|row| {
                row.get("state")
                    .and_then(Value::as_str)
                    .is_some_and(|row_state| row_state.eq_ignore_ascii_case(state))
            })
            .cloned()
            .collect(),
    )
}

// Pre-existing clippy::too_many_lines debt (121 lines before issue #605
// touched this file at all; unrelated to completion-callback deliveries).
// Allowed here rather than left broken, matching the precedent already
// established for `DeferredTriggerStart::spawn` in the core crate.
#[allow(clippy::too_many_lines)]
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
            queue_name,
            min_attempts,
            failed_after,
            failed_before,
            error_class,
            dlq_reason,
            failure_signature,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/replay",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                queue_name.as_deref(),
                *min_attempts,
                failed_after.as_deref(),
                failed_before.as_deref(),
                error_class.as_deref(),
                dlq_reason.as_deref(),
                failure_signature.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
        DeadLetterCommand::BulkDiscard {
            activity_name,
            workflow_name,
            queue_name,
            min_attempts,
            failed_after,
            failed_before,
            error_class,
            dlq_reason,
            failure_signature,
            limit,
            dry_run,
        } => ApiRequest::post(
            "/dead-letters/discard",
            Some(build_bulk_dlq_body(
                activity_name.as_deref(),
                workflow_name.as_deref(),
                queue_name.as_deref(),
                *min_attempts,
                failed_after.as_deref(),
                failed_before.as_deref(),
                error_class.as_deref(),
                dlq_reason.as_deref(),
                failure_signature.as_deref(),
                *limit,
                *dry_run,
            )),
        ),
        DeadLetterCommand::Aggregate {
            group_by,
            time_bucket,
            workflow_name,
            activity_name,
            queue_name,
            since,
            until,
            min_attempts,
            limit_groups,
            samples_per_group,
            json: _,
        } => {
            let mut params: Vec<(&str, String)> = Vec::new();
            for dim in group_by {
                params.push(("group_by", dim.clone()));
            }
            if let Some(tb) = time_bucket {
                params.push(("time_bucket", tb.clone()));
            }
            if let Some(v) = workflow_name {
                params.push(("workflow_name", v.clone()));
            }
            if let Some(v) = activity_name {
                params.push(("activity_name", v.clone()));
            }
            if let Some(v) = queue_name {
                params.push(("queue_name", v.clone()));
            }
            if let Some(v) = since {
                params.push(("since", v.clone()));
            }
            if let Some(v) = until {
                params.push(("until", v.clone()));
            }
            if let Some(v) = min_attempts {
                params.push(("min_attempts", v.to_string()));
            }
            if let Some(v) = limit_groups {
                params.push(("limit_groups", v.to_string()));
            }
            if let Some(v) = samples_per_group {
                params.push(("samples_per_group", v.to_string()));
            }
            ApiRequest::get(format!(
                "/dead-letters/aggregate?{}",
                encode_query_params(&params)
            ))
        }
        DeadLetterCommand::Redrive {
            queue,
            workflow_name,
            dead_lettered_after,
            dead_lettered_before,
            error_contains,
            dead_letter_ids,
            max,
            reason,
            dry_run,
        } => ApiRequest::post(
            "/dlq/redrive",
            Some(build_redrive_dlq_body(
                queue.as_deref(),
                workflow_name.as_deref(),
                dead_lettered_after.as_deref(),
                dead_lettered_before.as_deref(),
                error_contains.as_deref(),
                dead_letter_ids,
                *max,
                reason.as_deref(),
                *dry_run,
            )),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_redrive_dlq_body(
    queue: Option<&str>,
    workflow_name: Option<&str>,
    dead_lettered_after: Option<&str>,
    dead_lettered_before: Option<&str>,
    error_contains: Option<&str>,
    dead_letter_ids: &[String],
    max: Option<u32>,
    reason: Option<&str>,
    dry_run: bool,
) -> Value {
    let mut body = Map::new();
    insert_string(&mut body, "queue", queue);
    insert_string(&mut body, "workflow_name", workflow_name);
    insert_string(&mut body, "dead_lettered_after", dead_lettered_after);
    insert_string(&mut body, "dead_lettered_before", dead_lettered_before);
    insert_string(&mut body, "error_contains", error_contains);
    insert_string(&mut body, "reason", reason);
    if !dead_letter_ids.is_empty() {
        body.insert("dead_letter_ids".to_string(), json!(dead_letter_ids));
    }
    if let Some(m) = max {
        body.insert("max".to_string(), json!(m));
    }
    if dry_run {
        body.insert("dry_run".to_string(), json!(true));
    }
    Value::Object(body)
}

#[allow(clippy::too_many_arguments)]
fn build_bulk_dlq_body(
    activity_name: Option<&str>,
    workflow_name: Option<&str>,
    queue_name: Option<&str>,
    min_attempts: Option<i32>,
    failed_after: Option<&str>,
    failed_before: Option<&str>,
    error_class: Option<&str>,
    dlq_reason: Option<&str>,
    failure_signature: Option<&str>,
    limit: Option<u32>,
    dry_run: bool,
) -> Value {
    let mut body = Map::new();
    insert_string(&mut body, "activity_name", activity_name);
    insert_string(&mut body, "workflow_name", workflow_name);
    insert_string(&mut body, "queue_name", queue_name);
    if let Some(m) = min_attempts {
        body.insert("min_attempts".to_string(), json!(m));
    }
    insert_string(&mut body, "failed_after", failed_after);
    insert_string(&mut body, "failed_before", failed_before);
    insert_string(&mut body, "error_class", error_class);
    insert_string(&mut body, "dlq_reason", dlq_reason);
    insert_string(&mut body, "failure_signature", failure_signature);
    if let Some(l) = limit {
        body.insert("limit".to_string(), json!(l));
    }
    if dry_run {
        body.insert("dry_run".to_string(), json!(true));
    }
    Value::Object(body)
}

fn gate_request(command: &GateCommand) -> Result<ApiRequest, CliError> {
    match command {
        GateCommand::List => Ok(ApiRequest::get("/admin/gates")),
        GateCommand::Lift { id } => Ok(ApiRequest {
            method: ApiMethod::Delete,
            path: format!("/admin/gates/{}", path_segment(id)),
            body: None,
        }),
        GateCommand::Create {
            scope,
            reason,
            message,
            expires_at,
        } => {
            // Parse scope string: "fleet", "workflow_name=X", "queue=X", "shard_id=N", "owner=X"
            let (scope_kind, scope_value) = if scope == "fleet" {
                ("fleet".to_string(), None::<String>)
            } else if let Some(v) = scope.strip_prefix("workflow_name=") {
                ("workflow_name".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("queue=") {
                ("queue".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("shard_id=") {
                ("shard_id".to_string(), Some(v.to_string()))
            } else if let Some(v) = scope.strip_prefix("owner=") {
                ("owner".to_string(), Some(v.to_string()))
            } else {
                return Err(CliError::InvalidInput(format!(
                    "unknown scope '{scope}'; expected: fleet, workflow_name=<name>, queue=<name>, shard_id=<N>, or owner=<id>"
                )));
            };
            let mut body = serde_json::json!({
                "scope_kind": scope_kind,
                "reason": reason,
            });
            if let Some(v) = scope_value {
                body["scope_value"] = serde_json::json!(v);
            }
            if let Some(msg) = message {
                body["message"] = serde_json::json!(msg);
            }
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            Ok(ApiRequest::post("/admin/gates", Some(body)))
        }
    }
}

fn token_request(command: &TokenCommand) -> ApiRequest {
    match command {
        TokenCommand::List => ApiRequest::get("/admin/tokens"),
        TokenCommand::Revoke { id } => ApiRequest {
            method: ApiMethod::Delete,
            path: format!("/admin/tokens/{}", path_segment(id)),
            body: None,
        },
        TokenCommand::Create {
            name,
            scope,
            expires_at,
        } => {
            let mut body = serde_json::json!({
                "name": name,
                "scope": scope,
            });
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            ApiRequest::post("/admin/tokens", Some(body))
        }
        // Rotation has no dedicated server route: the CLI mints a replacement via
        // the create route. The old token is revoked as a documented second step.
        TokenCommand::Rotate {
            old_id,
            scope,
            expires_at,
        } => {
            let mut body = serde_json::json!({
                "name": format!("rotation-of-{old_id}"),
                "scope": scope,
            });
            if let Some(exp) = expires_at {
                body["expires_at"] = serde_json::json!(exp);
            }
            ApiRequest::post("/admin/tokens", Some(body))
        }
        // Bootstrap is an OFFLINE seed: it issues no HTTP request and is handled
        // entirely in-process in `run_cli` (mirrors DetCheck/Tui/Events).
        TokenCommand::Bootstrap { .. } => {
            unreachable!("token bootstrap is handled locally in run_cli")
        }
    }
}

/// The offline seed-token output produced by `harvest token bootstrap`.
///
/// Carries the one-time plaintext `secret`, its stored `hash`, and the exact
/// `INSERT INTO harvest_api_tokens ...` statement to run out-of-band. The SQL
/// contains ONLY the hash — never the secret (issue #942).
pub struct BootstrapToken {
    /// The plaintext `hvst_...` secret — shown once, never stored.
    pub secret: String,
    /// `hex(SHA256(secret))` — the value embedded in the INSERT and stored.
    pub hash: String,
    /// The token scope (`read` | `mutate`).
    pub scope: String,
    /// The token label.
    pub name: String,
    /// A ready-to-run `INSERT INTO harvest_api_tokens (...) VALUES (...);`.
    pub insert_sql: String,
}

/// Wrap `s` as a Postgres single-quoted string literal, escaping embedded
/// single quotes (`'` → `''`). With `standard_conforming_strings` (the default),
/// this fully neutralizes injection through a crafted `--name`/`--created-by`.
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Build the offline bootstrap seed: a fresh secret, its stored hash, and the
/// exact INSERT SQL. Opens **no** database connection.
///
/// The secret and hash are produced by the SHARED core helpers
/// ([`autumn_harvest::api_token::mint_secret`] / [`hash_secret`]) — the same
/// functions the server mint route uses — so a seeded token authenticates
/// byte-for-byte identically to a route-minted one (no drift).
///
/// # Errors
///
/// Returns [`CliError::InvalidInput`] if `expires_at` is not a valid RFC 3339
/// timestamp (so a broken INSERT is never emitted).
pub fn build_bootstrap_token(
    name: &str,
    scope: &str,
    expires_at: Option<&str>,
    created_by: &str,
) -> Result<BootstrapToken, CliError> {
    // Single source of truth: the mint route hashes with these exact helpers.
    let secret = autumn_harvest::api_token::mint_secret();
    let hash = autumn_harvest::api_token::hash_secret(&secret);

    // Validate the expiry up front so we never print an INSERT that Postgres
    // rejects at run time.
    if let Some(e) = expires_at {
        autumn_harvest::chrono::DateTime::parse_from_rfc3339(e).map_err(|source| {
            CliError::InvalidInput(format!(
                "token bootstrap: --expires-at '{e}' is not a valid RFC 3339 timestamp: {source}"
            ))
        })?;
    }

    // `id`/`created_at` use the column defaults (gen_random_uuid()/NOW()); every
    // user-supplied string is single-quote escaped so a crafted flag value
    // cannot inject SQL. The statement embeds only the hash, never the secret.
    let mut columns = String::from("id, name, token_hash, scope, created_at, created_by");
    let mut values = format!(
        "gen_random_uuid(), {}, {}, {}, NOW(), {}",
        sql_quote(name),
        sql_quote(&hash),
        sql_quote(scope),
        sql_quote(created_by),
    );
    if let Some(e) = expires_at {
        use std::fmt::Write as _;
        columns.push_str(", expires_at");
        let _ = write!(values, ", {}::timestamptz", sql_quote(e));
    }
    let insert_sql = format!("INSERT INTO harvest_api_tokens ({columns})\nVALUES ({values});");

    Ok(BootstrapToken {
        secret,
        hash,
        scope: scope.to_string(),
        name: name.to_string(),
        insert_sql,
    })
}

/// Execute `harvest token bootstrap`: print the one-time secret and the INSERT
/// SQL. Opens no DB connection; the operator runs the SQL out-of-band.
///
/// # Errors
///
/// Propagates [`build_bootstrap_token`]'s validation error.
fn run_token_bootstrap(
    name: &str,
    scope: &str,
    expires_at: Option<&str>,
    created_by: &str,
) -> Result<(), CliError> {
    let token = build_bootstrap_token(name, scope, expires_at, created_by)?;

    println!("Harvest API token — offline bootstrap seed");
    println!();
    println!("  Token secret (shown once — store it now, it cannot be recovered):");
    println!();
    println!("    {}", token.secret);
    println!();
    println!("  1. Save the secret above in your secret store. Only its SHA-256 hash is");
    println!("     written to the database, so the secret cannot be recovered later.");
    println!("  2. Run this SQL against your Harvest database to create the token row");
    println!("     (it embeds only the hash — never the secret):");
    println!();
    for line in token.insert_sql.lines() {
        println!("    {line}");
    }
    println!();
    println!("  3. Send the secret above as a bearer credential:");
    println!("       Authorization: Bearer <secret>");
    println!(
        "     A `{}`-scoped token can mint every further token via POST /admin/tokens.",
        token.scope
    );
    Ok(())
}

fn worker_request(command: &WorkerCommand) -> ApiRequest {
    match command {
        WorkerCommand::Drain {
            worker_id,
            deadline,
            wait: _,
            wait_timeout_secs: _,
        } => {
            let mut body = Map::new();
            if let Some(d) = deadline {
                body.insert("deadline_at".to_string(), json!(d));
            }
            ApiRequest::post(
                format!("/workers/{}/drain", path_segment(worker_id)),
                Some(Value::Object(body)),
            )
        }
        WorkerCommand::DrainPreview {
            queue,
            shard_id,
            status,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(q) = queue {
                params.push(("queue", q.clone()));
            }
            if let Some(s) = shard_id {
                params.push(("shard_id", s.to_string()));
            }
            if let Some(s) = status {
                params.push(("status", s.clone()));
            }
            if let Some(l) = limit {
                params.push(("limit", l.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/workers/drain-preview");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/workers/drain-preview?{qs}"))
        }
        WorkerCommand::List {
            queue,
            shard_id,
            status,
            health,
            limit,
        } => {
            let mut params: Vec<(&'static str, String)> = Vec::new();
            if let Some(q) = queue {
                params.push(("queue", q.clone()));
            }
            if let Some(s) = shard_id {
                params.push(("shard_id", s.to_string()));
            }
            if let Some(s) = status {
                params.push(("status", s.clone()));
            }
            if let Some(h) = health {
                params.push(("health", h.clone()));
            }
            if let Some(l) = limit {
                params.push(("limit", l.to_string()));
            }
            if params.is_empty() {
                return ApiRequest::get("/workers");
            }
            let qs = params
                .iter()
                .map(|(k, v)| format!("{k}={}", query_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            ApiRequest::get(format!("/workers?{qs}"))
        }
        WorkerCommand::Get { worker_id } => {
            ApiRequest::get(format!("/workers/{}", path_segment(worker_id)))
        }
        WorkerCommand::Health => ApiRequest::get("/workers/health"),
    }
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

#[allow(clippy::too_many_arguments)]
fn build_handoff_list_path(
    states: &[String],
    workflow_name: Option<&str>,
    execution_id: Option<&str>,
    activity_name: Option<&str>,
    token: Option<&str>,
    shard_id: Option<i32>,
    due_before: Option<&str>,
    updated_before: Option<&str>,
    limit: Option<i64>,
) -> String {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if !states.is_empty() {
        params.push(("state", states.join(",")));
    }
    if let Some(value) = workflow_name {
        params.push(("workflow_name", value.to_string()));
    }
    if let Some(value) = execution_id {
        params.push(("execution_id", value.to_string()));
    }
    if let Some(value) = activity_name {
        params.push(("activity_name", value.to_string()));
    }
    if let Some(value) = token {
        params.push(("token", value.to_string()));
    }
    if let Some(value) = shard_id {
        params.push(("shard_id", value.to_string()));
    }
    if let Some(value) = due_before {
        params.push(("due_before", value.to_string()));
    }
    if let Some(value) = updated_before {
        params.push(("updated_before", value.to_string()));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }

    if params.is_empty() {
        return "/admin/external-handoffs".to_string();
    }
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/admin/external-handoffs?{query}")
}

#[allow(clippy::too_many_arguments)]
fn build_workflow_list_path(
    limit: Option<i64>,
    states: &[String],
    workflow_name: Option<&str>,
    search_attrs: &[String],
    search_attr_filters: &[String],
    owner: Option<&str>,
    no_progress_minutes: Option<i64>,
    include_sleeping: bool,
    start_source: Option<&str>,
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
    // Issue #506: typed comparison/set predicates forwarded verbatim. The server
    // owns validation (op grammar, numeric coercion, top-level-key rule), so the
    // CLI is a thin passthrough and returns the API's `400` message on error.
    for raw in search_attr_filters {
        params.push(("search_attr_filter", raw.clone()));
    }
    if let Some(o) = owner {
        params.push(("owner", o.to_string()));
    }
    if let Some(minutes) = no_progress_minutes {
        params.push(("no_progress_minutes", minutes.to_string()));
    }
    if include_sleeping {
        params.push(("include_sleeping", "true".to_string()));
    }
    // Issue #740: bounded provenance filter. The server owns validation (a value
    // outside the known `StartSource` set / "unknown" returns a 400), so the CLI
    // is a thin passthrough — matching how `--state` forwards verbatim.
    if let Some(source) = start_source {
        params.push(("start_source", source.to_string()));
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

#[allow(clippy::too_many_arguments)]
fn build_summary_list_path(
    workflow_name: Option<&str>,
    workflow_id: Option<&str>,
    states: &[String],
    completed_after: Option<&str>,
    completed_before: Option<&str>,
    search_attrs: &[String],
    limit: Option<i64>,
    cursor: Option<&str>,
    order: Option<&str>,
) -> Result<String, CliError> {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(wid) = workflow_id {
        params.push(("workflow_id", wid.to_string()));
    }
    if !states.is_empty() {
        params.push(("state", states.join(",")));
    }
    if let Some(after) = completed_after {
        params.push(("completed_after", after.to_string()));
    }
    if let Some(before) = completed_before {
        params.push(("completed_before", before.to_string()));
    }
    for raw in search_attrs {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| CliError::InvalidSearchAttr { value: raw.clone() })?;
        params.push(("search_attr", format!("{key}:{value}")));
    }
    if let Some(value) = limit {
        params.push(("limit", value.to_string()));
    }
    if let Some(value) = cursor {
        params.push(("cursor", value.to_string()));
    }
    if let Some(value) = order {
        params.push(("order", value.to_string()));
    }

    if params.is_empty() {
        return Ok("/workflows/summaries".to_string());
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("/workflows/summaries?{encoded}"))
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

fn encode_query_params(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
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

/// Clap value-parser for `--idempotency-key` (issue #753).
///
/// Mirrors the server's `Idempotency-Key` header semantics: a present but
/// empty (or whitespace-only) key is rejected up front rather than silently
/// degraded — the management API treats an empty `?idempotency_key=` query
/// param as omitted, which would turn an intended exactly-once delivery into
/// at-least-once without the caller noticing.
fn parse_idempotency_key(value: &str) -> Result<String, String> {
    if value.trim().is_empty() {
        return Err(
            "--idempotency-key must not be empty; omit the flag entirely for legacy \
             at-least-once delivery"
                .to_string(),
        );
    }
    Ok(value.to_string())
}

// ─── Version-gate retirement check helpers ────────────────────────────────────

const fn retirement_check_should_check(cli: &Cli) -> bool {
    matches!(
        &cli.command,
        Commands::VersionGateRetirement { check: true, .. }
    )
}

fn retirement_check_wants_table(cli: &Cli) -> bool {
    matches!(&cli.command, Commands::VersionGateRetirement { .. })
        && cli.output == OutputFormat::PrettyJson
}

fn retirement_check_exit_code(value: &Value) -> i32 {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    // Non-zero on any non-safe outcome
    i32::from(!matches!(status, "safe"))
}

fn retirement_check_request(
    change_id: &str,
    min_safe_version: u32,
    workflow_name: Option<&str>,
    state_group: Option<VersionUsageStateGroup>,
    shard_id: Option<i32>,
) -> ApiRequest {
    let mut params: Vec<(&'static str, String)> = Vec::new();
    params.push(("change_id", change_id.to_string()));
    params.push(("min_safe_version", min_safe_version.to_string()));
    if let Some(name) = workflow_name {
        params.push(("workflow_name", name.to_string()));
    }
    if let Some(sg) = state_group {
        params.push(("state_group", sg.as_wire().to_string()));
    }
    if let Some(sid) = shard_id {
        params.push(("shard_id", sid.to_string()));
    }
    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={}", query_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    ApiRequest::get(format!("/admin/version-gates/retirement-check?{encoded}"))
}

fn format_retirement_check_table(value: &Value) -> String {
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let safe = value
        .get("safe_to_retire")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let observed_at = value
        .get("observed_at")
        .and_then(Value::as_str)
        .unwrap_or("-");
    let change_id = value
        .get("filters")
        .and_then(|f| f.get("change_id"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let min_safe = value
        .get("filters")
        .and_then(|f| f.get("min_safe_version"))
        .and_then(Value::as_i64)
        .map_or_else(|| "-".to_string(), |v| v.to_string());
    let safe_str = if safe { "yes" } else { "no" };
    let header = format!(
        "status: {status}  safe_to_retire: {safe_str}  change_id: {change_id}  min_safe_version: {min_safe}\nobserved_at: {observed_at}"
    );

    let Some(blockers) = value.get("blockers").and_then(Value::as_array) else {
        return format!("{header}\nNo blockers returned.");
    };
    if blockers.is_empty() {
        return format!("{header}\nNo old-version executions found.");
    }

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(blockers.len() + 1);
    rows.push(vec![
        "WORKFLOW".to_string(),
        "VERSION".to_string(),
        "ACTIVE".to_string(),
        "TERMINAL".to_string(),
        "OLDEST_AGE_S".to_string(),
        "NEWEST_AGE_S".to_string(),
        "SHARDS".to_string(),
        "UNAVAILABLE".to_string(),
    ]);
    for blocker in blockers {
        rows.push(vec![
            cell_str(blocker.get("workflow_name")),
            cell_number(blocker.get("recorded_version")),
            cell_number(blocker.get("active_executions")),
            cell_number(blocker.get("terminal_executions")),
            cell_number(blocker.get("oldest_blocker_age_secs")),
            cell_number(blocker.get("newest_blocker_age_secs")),
            retirement_shard_array_cell(blocker, "matched_shards"),
            retirement_shard_array_cell(blocker, "unavailable_shards"),
        ]);
    }

    let widths = (0..rows[0].len())
        .map(|col| rows.iter().map(|row| row[col].len()).max().unwrap_or(0))
        .collect::<Vec<_>>();
    let table = rows
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

    format!("{header}\n\n{table}")
}

fn retirement_shard_array_cell(item: &Value, field: &str) -> String {
    let Some(values) = item
        .get("shard_coverage")
        .and_then(|coverage| coverage.get(field))
        .and_then(Value::as_array)
    else {
        return "-".to_string();
    };
    if values.is_empty() {
        return "-".to_string();
    }
    values
        .iter()
        .filter_map(Value::as_i64)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
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

    // ── issue #756: partial cross-shard read notice ──────────────────────

    #[test]
    fn fanout_partial_notice_none_for_bare_array() {
        // The happy path is a bare array; no notice.
        assert!(fanout_partial_notice(&json!([{"id": "a"}])).is_none());
    }

    #[test]
    fn fanout_partial_notice_none_when_unavailable_empty() {
        assert!(
            fanout_partial_notice(&json!({
                "workers": [],
                "status": "complete",
                "unavailable_shards": []
            }))
            .is_none()
        );
    }

    #[test]
    fn fanout_partial_notice_names_shard_and_reason() {
        let notice = fanout_partial_notice(&json!({
            "workflows": [],
            "status": "partial",
            "unavailable_shards": [
                {"shard_id": 1, "reason": "connection refused"}
            ]
        }))
        .expect("degraded envelope must produce a notice");
        assert!(notice.contains("partial"));
        assert!(notice.contains("1 shard(s) unavailable"));
        assert!(notice.contains("1: connection refused"));
    }

    #[test]
    fn workflow_list_degraded_body_is_clean_and_notice_is_separate() {
        // Issue #756: on the degraded path the notice goes to STDERR (via
        // `fanout_partial_notice`, `eprintln!`'d by the caller), NOT prepended
        // to the STDOUT body — so `workflow list -o json | jq` stays parseable.
        let cli = parse(&["workflow", "list"]);
        let payload = json!({
            "workflows": [{"id": "00000000-0000-0000-0000-000000000001"}],
            "status": "partial",
            "unavailable_shards": [{"shard_id": 2, "reason": "pool missing"}]
        });
        // The STDOUT body carries no warning and remains parseable JSON.
        let rendered = render_response(&cli, &payload).expect("render should succeed");
        assert!(
            !rendered.contains("WARNING"),
            "the STDOUT body must stay clean, got: {rendered}"
        );
        let parsed: Value =
            serde_json::from_str(&rendered).expect("the STDOUT body must remain parseable JSON");
        assert!(
            parsed.get("workflows").is_some(),
            "the data still renders in the body"
        );
        // The notice is available separately for the caller to emit on STDERR.
        let notice =
            fanout_partial_notice(&payload).expect("degraded payload yields a stderr notice");
        assert!(notice.starts_with("WARNING: cross-shard read is partial"));
        assert!(notice.contains("2: pool missing"));
    }

    #[test]
    fn workflow_list_happy_path_bare_array_has_no_notice() {
        let cli = parse(&["workflow", "list"]);
        let payload = json!([{"id": "00000000-0000-0000-0000-000000000001"}]);
        let rendered = render_response(&cli, &payload).expect("render should succeed");
        assert!(!rendered.contains("WARNING"));
        // No stderr notice on the happy path either.
        assert!(fanout_partial_notice(&payload).is_none());
    }

    #[test]
    fn handoff_list_default_output_renders_human_table() {
        let cli = parse(&["handoff", "list"]);
        let payload = json!({
            "status": "ok",
            "shard_coverage": {
                "inspected_shards": [0],
                "matched_shards": [0],
                "unavailable_shards": []
            },
            "items": [{
                "token": "11111111-1111-4111-8111-111111111111",
                "workflow": {
                    "execution_id": "00000000-0000-0000-0000-000000000001",
                    "workflow_id": "invoice-42",
                    "workflow_name": "billing_checkout",
                    "shard_id": 0
                },
                "activity": {
                    "activity_id": "22222222-2222-4222-8222-222222222222",
                    "activity_name": "manager_approval"
                },
                "state": "PENDING",
                "created_at": "2026-05-08T11:00:00Z",
                "updated_at": "2026-05-08T11:05:00Z",
                "deadline_at": "2026-05-08T12:00:00Z"
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("status: ok"));
        assert!(rendered.contains("STATE"));
        assert!(rendered.contains("manager_approval"));
        assert!(rendered.contains("11111111-1111-4111-8111-111111111111"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn handoff_json_flag_renders_raw_payload() {
        let cli = parse(&["handoff", "list", "--json"]);
        let payload = json!({
            "status": "ok",
            "items": [],
            "shard_coverage": {
                "inspected_shards": [0],
                "matched_shards": [],
                "unavailable_shards": []
            }
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"items":[],"shard_coverage":{"inspected_shards":[0],"matched_shards":[],"unavailable_shards":[]},"status":"ok"}"#
        );
    }

    #[test]
    fn preflight_default_output_renders_compact_table() {
        let cli = parse(&["preflight"]);
        let payload = json!({
            "overall_status": "warn",
            "observed_at": "2026-05-06T12:00:00Z",
            "version": {
                "package": "autumn-harvest-plugin",
                "version": "0.3.0",
                "core_version": "0.3.0"
            },
            "checks": [{
                "name": "worker_coverage",
                "status": "warn",
                "summary": "queue coverage exists but one worker is stale",
                "remediation": "Restart or replace stale workers before promotion.",
                "affected_shards": [0],
                "details": {}
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("STATUS"));
        assert!(rendered.contains("worker_coverage"));
        assert!(rendered.contains("warn"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn preflight_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "preflight"]);
        let payload = json!({
            "overall_status": "pass",
            "observed_at": "2026-05-06T12:00:00Z",
            "version": {
                "package": "autumn-harvest-plugin",
                "version": "0.3.0",
                "core_version": "0.3.0"
            },
            "checks": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"checks":[],"observed_at":"2026-05-06T12:00:00Z","overall_status":"pass","version":{"core_version":"0.3.0","package":"autumn-harvest-plugin","version":"0.3.0"}}"#
        );
    }

    #[test]
    fn preflight_exit_codes_match_deploy_gate_status() {
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "pass" })), 0);
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "warn" })), 2);
        assert_eq!(preflight_exit_code(&json!({ "overall_status": "fail" })), 1);
    }

    #[test]
    fn shard_health_default_output_renders_compact_table() {
        let cli = parse(&["shard", "health"]);
        let payload = json!({
            "overall_readiness": "degraded",
            "observed_at": "2026-05-06T12:00:00Z",
            "shards": [{
                "shard_id": 1,
                "roles": ["readable"],
                "candidate": true,
                "readiness": "degraded",
                "reachable": true,
                "active_worker_count": 0,
                "stale_worker_count": 0,
                "schema": { "ready": true },
                "worker_coverage": [{
                    "queue": "default",
                    "healthy_active": 0,
                    "stale": 0,
                    "draining": 0,
                    "ready": false
                }],
                "scheduler": {
                    "enabled": false,
                    "ready": true,
                    "last_tick_at": null
                },
                "queue_depth": {
                    "total_pending": 0,
                    "by_queue": {}
                },
                "dlq": { "count": 0 },
                "blocking_reasons": [
                    "no healthy active worker covers required queue 'default'"
                ],
                "error_summary": null
            }]
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("SHARD"));
        assert!(rendered.contains("readable"));
        assert!(rendered.contains("degraded"));
        assert!(rendered.contains("active=0"));
        assert!(rendered.contains("stale=0"));
        assert!(rendered.contains("default"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn shard_health_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "shard", "health"]);
        let payload = json!({
            "overall_readiness": "ready",
            "observed_at": "2026-05-06T12:00:00Z",
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"observed_at":"2026-05-06T12:00:00Z","overall_readiness":"ready","shards":[]}"#
        );
    }

    #[test]
    fn shard_health_gate_fails_on_non_ready_writable_or_candidate_shards_only() {
        let payload = json!({
            "shards": [
                {
                    "shard_id": 0,
                    "roles": ["readable", "writable", "default"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 1,
                    "roles": ["readable"],
                    "candidate": false,
                    "readiness": "degraded"
                }
            ]
        });
        assert_eq!(shard_health_exit_code(&payload), 0);

        let writable_degraded = json!({
            "shards": [{
                "shard_id": 0,
                "roles": ["readable", "writable", "default"],
                "candidate": false,
                "readiness": "degraded"
            }]
        });
        assert_eq!(shard_health_exit_code(&writable_degraded), 1);

        let candidate_degraded = json!({
            "shards": [{
                "shard_id": 2,
                "roles": ["readable"],
                "candidate": true,
                "readiness": "degraded"
            }]
        });
        assert_eq!(shard_health_exit_code(&candidate_degraded), 1);
    }

    #[test]
    fn shard_health_gate_is_enabled_by_default() {
        let cli = parse(&["shard", "health"]);

        assert!(
            shard_health_should_gate(&cli),
            "shard health is a rollout gate by default"
        );
    }

    #[test]
    fn shard_health_gate_treats_degraded_and_unavailable_as_failures() {
        let degraded = json!({
            "shards": [{
                "shard_id": 0,
                "roles": ["readable", "writable", "default"],
                "candidate": false,
                "readiness": "degraded"
            }]
        });
        let unavailable = json!({
            "shards": [{
                "shard_id": 1,
                "roles": ["readable", "writable"],
                "candidate": false,
                "readiness": "unavailable"
            }]
        });

        assert_eq!(shard_health_exit_code(&degraded), 1);
        assert_eq!(shard_health_exit_code(&unavailable), 1);
    }

    #[test]
    fn shard_health_gate_fails_three_shard_rollout_with_uncovered_writable_shard() {
        let payload = json!({
            "shards": [
                {
                    "shard_id": 0,
                    "roles": ["readable", "writable", "default"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 1,
                    "roles": ["readable", "writable"],
                    "candidate": false,
                    "readiness": "ready"
                },
                {
                    "shard_id": 2,
                    "roles": ["readable", "writable"],
                    "candidate": false,
                    "readiness": "degraded",
                    "reason_codes": ["worker_queue_uncovered"]
                }
            ]
        });

        assert_eq!(shard_health_exit_code(&payload), 1);
    }

    #[test]
    fn version_usage_default_output_renders_compact_table() {
        let cli = parse(&["version-usage"]);
        let payload = json!({
            "status": "complete",
            "observed_at": "2026-05-07T12:00:00Z",
            "items": [{
                "workflow_name": "billing_checkout",
                "change_id": "billing_checkout_v2_tax",
                "recorded_version": 1,
                "active_executions": 1,
                "terminal_executions": 2,
                "oldest_matching_execution_age_secs": 3600,
                "newest_matching_execution_age_secs": 60,
                "shard_coverage": {
                    "inspected_shards": [0, 1],
                    "matched_shards": [0],
                    "unavailable_shards": []
                }
            }],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("table output should render");

        assert!(rendered.contains("WORKFLOW"));
        assert!(rendered.contains("billing_checkout"));
        assert!(rendered.contains("billing_checkout_v2_tax"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn version_usage_json_output_preserves_raw_payload_shape() {
        let cli = parse(&["--output", "json", "version-usage"]);
        let payload = json!({
            "status": "no_matches",
            "observed_at": "2026-05-07T12:00:00Z",
            "items": [],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert_eq!(
            rendered,
            r#"{"items":[],"observed_at":"2026-05-07T12:00:00Z","shards":[],"status":"no_matches"}"#
        );
    }

    #[test]
    fn version_usage_guard_fails_on_active_usage_or_incomplete_shards() {
        let active = json!({
            "status": "complete",
            "items": [{ "active_executions": 1 }]
        });
        assert_eq!(version_usage_guard_exit_code(&active), 1);

        let drained = json!({
            "status": "complete",
            "items": [{ "active_executions": 0, "terminal_executions": 4 }]
        });
        assert_eq!(version_usage_guard_exit_code(&drained), 0);

        let partial = json!({
            "status": "partial",
            "items": []
        });
        assert_eq!(version_usage_guard_exit_code(&partial), 1);
    }

    // ─── Workflow-type reachability CLI tests (issue #520) ───────────────────

    #[test]
    fn workflow_reachability_builds_unfiltered_request() {
        let cli = parse(&["workflow-types", "reachability"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/workflow-types/reachability");
        assert!(req.body.is_none());
    }

    #[test]
    fn workflow_reachability_threads_type_filter() {
        let cli = parse(&["workflow-types", "reachability", "--type", "onboarding"]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(
            req.path,
            "/admin/workflow-types/reachability?workflow_type=onboarding"
        );
    }

    #[test]
    fn workflow_reachability_exit_code_zero_when_all_safe_or_in_use() {
        let value = json!({
            "status": "complete",
            "items": [
                { "verdict": "safe_to_remove" },
                { "verdict": "in_use" }
            ]
        });
        assert_eq!(workflow_reachability_exit_code(&value), 0);
    }

    #[test]
    fn workflow_reachability_exit_code_two_on_orphaned() {
        let value = json!({
            "status": "complete",
            "items": [
                { "verdict": "safe_to_remove" },
                { "verdict": "orphaned" }
            ]
        });
        assert_eq!(workflow_reachability_exit_code(&value), 2);
    }

    #[test]
    fn workflow_reachability_exit_code_two_on_partial_report() {
        // A partial answer must never be mistaken for safe-to-remove: fail closed.
        let value = json!({ "status": "partial", "items": [] });
        assert_eq!(workflow_reachability_exit_code(&value), 2);
        let unavailable = json!({ "status": "unavailable", "items": [] });
        assert_eq!(workflow_reachability_exit_code(&unavailable), 2);
    }

    #[test]
    fn path_segment_encodes_both_path_separators() {
        // The URL parser treats `\` as a path separator for http/https, so an
        // unencoded backslash splits one value into extra segments and the
        // request silently lands on a different route. Route-wide: every caller
        // of `path_segment` (queue names, workflow ids, keys, ...) depends on
        // this, not just queue pause/resume.
        assert_eq!(path_segment(r"payments\eu"), "payments%5Ceu");
        assert_eq!(path_segment(r"a\b\c"), "a%5Cb%5Cc");
        assert_eq!(path_segment("payments/eu"), "payments%2Feu");
        // Worse than a missed route: a backslash re-enables `..` traversal
        // inside what should be one opaque segment, past the whole-segment
        // dot-segment guard.
        assert_eq!(path_segment(r"payments\..\admin"), "payments%5C..%5Cadmin");
        // An ordinary name is untouched.
        assert_eq!(path_segment("orders-eu"), "orders-eu");
    }

    /// Exhaustive guard for the whole path-separator / normalization class.
    ///
    /// Every printable ASCII byte, embedded in a value that becomes one path
    /// segment, must survive `path_segment` + URL parsing as **exactly one**
    /// segment that decodes back to the original. This is the test that would
    /// have caught the missing `\` (the URL parser treats it as a path
    /// separator for http/https), and it fails if any separator-class character
    /// is ever dropped from `PATH_SEGMENT_ENCODE_SET`.
    ///
    /// Scope: *embedded* bytes. A whole-segment `.`/`..` cannot be encoded away
    /// at all and is rejected instead (`is_url_dot_segment`); control
    /// characters are covered by `CONTROLS`.
    #[test]
    fn every_printable_ascii_byte_survives_as_exactly_one_path_segment() {
        let mut offenders = Vec::new();
        for byte in 0x20u8..=0x7e {
            let raw = format!("q{}z", byte as char);
            let encoded = path_segment(&raw);
            let url = format!("http://host/admin/queues/{encoded}/pause");
            let parsed = reqwest::Url::parse(&url)
                .unwrap_or_else(|e| panic!("byte {byte:#04x} produced an unparseable URL: {e}"));
            let segments: Vec<&str> = parsed.path().trim_start_matches('/').split('/').collect();
            let intact = segments.len() == 4 && percent_decode(segments[2]) == raw;
            if !intact {
                offenders.push(format!(
                    "byte {byte:#04x} ({:?}) encoded to {encoded:?} but parsed as {:?}",
                    byte as char,
                    parsed.path()
                ));
            }
        }
        assert!(
            offenders.is_empty(),
            "these bytes did not survive as one intact path segment:\n{}",
            offenders.join("\n")
        );
    }

    fn percent_decode(value: &str) -> String {
        percent_encoding::percent_decode_str(value)
            .decode_utf8_lossy()
            .to_string()
    }

    #[test]
    fn queue_mutation_exit_code_fails_on_a_partial_hold() {
        // Issue #619: a 207 partial fleet-wide hold is NOT in effect on the
        // shards it missed -- those keep dispatching into the outage. It must
        // never look like success to a script or a runbook step.
        assert_eq!(
            queue_mutation_exit_code(&json!({ "ok": false, "status": "partial" })),
            1
        );
        assert_eq!(
            queue_mutation_exit_code(&json!({ "ok": true, "status": "complete" })),
            0
        );
    }

    #[test]
    fn queue_mutation_exit_code_gates_on_either_signal_independently() {
        // `ok` and `status` are belt-and-braces: either one reporting a
        // non-complete application is enough to fail the command.
        assert_eq!(queue_mutation_exit_code(&json!({ "ok": false })), 1);
        assert_eq!(queue_mutation_exit_code(&json!({ "status": "partial" })), 1);
        // A body missing both signals is not a queue-mutation response we can
        // vouch for -- fail closed rather than reporting a hold that may not hold.
        assert_eq!(queue_mutation_exit_code(&json!({})), 1);
    }

    #[test]
    fn queue_mutation_gate_applies_only_to_the_mutating_subcommands() {
        assert!(queue_mutation_should_gate(&parse(&[
            "queue", "pause", "q", "--reason", "x"
        ])));
        assert!(queue_mutation_should_gate(&parse(&[
            "queue", "resume", "q"
        ])));
        assert!(
            !queue_mutation_should_gate(&parse(&["queue", "list-paused"])),
            "the read route has no partial-application contract to gate on"
        );
        assert!(!queue_mutation_should_gate(&parse(&["health"])));
    }

    #[test]
    fn queue_partial_mutation_error_uses_exit_code_one() {
        assert_eq!(
            CliError::QueuePartialMutation {
                detail: "shard 1 unreachable".to_string()
            }
            .exit_code(),
            1
        );
    }

    #[test]
    fn workflow_reachability_gate_error_uses_exit_code_two() {
        assert_eq!(
            CliError::WorkflowReachabilityGate {
                context: String::new()
            }
            .exit_code(),
            2
        );
    }

    #[test]
    fn workflow_reachability_table_renders_rows_and_unavailable_warning() {
        let cli = parse(&["workflow-types", "reachability"]);
        let value = json!({
            "status": "partial",
            "observed_at": "2026-05-31T00:00:00Z",
            "filter": null,
            "items": [
                {
                    "workflow_type": "legacy_flow",
                    "registered": false,
                    "non_terminal_count": 2,
                    "oldest_non_terminal_age_secs": 3600,
                    "verdict": "orphaned",
                    "shard_breakdown": []
                }
            ],
            "shards": [
                { "shard_id": 0, "status": "inspected", "error": null },
                { "shard_id": 1, "status": "unavailable", "error": "connection refused" }
            ]
        });
        let rendered = render_response(&cli, &value).expect("table should render");
        assert!(rendered.contains("legacy_flow"));
        assert!(rendered.contains("orphaned"));
        assert!(rendered.contains("WARNING: unavailable shards [1]"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn workflow_reachability_json_flag_emits_raw_payload() {
        let cli = parse(&["workflow-types", "reachability", "--json"]);
        let value = json!({
            "status": "complete",
            "observed_at": "2026-05-31T00:00:00Z",
            "filter": null,
            "items": [],
            "shards": []
        });
        let rendered = render_response(&cli, &value).expect("json should render");
        assert!(rendered.trim_start().starts_with('{'));
    }

    // ─── VersionGateRetirement CLI tests ─────────────────────────────────────

    #[test]
    fn version_gate_retirement_builds_correct_api_request() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let req = cli.api_request().expect("request should build");
        assert_eq!(req.method, ApiMethod::Get);
        assert!(
            req.path
                .starts_with("/admin/version-gates/retirement-check"),
            "path should target retirement-check endpoint; got {}",
            req.path
        );
        assert!(req.path.contains("change_id=tax_v2"));
        assert!(req.path.contains("min_safe_version=2"));
    }

    #[test]
    fn version_gate_retirement_includes_optional_workflow_name() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "3",
            "--workflow-name",
            "billing_checkout",
        ]);
        let req = cli.api_request().expect("request should build");
        assert!(req.path.contains("workflow_name=billing_checkout"));
    }

    #[test]
    fn retirement_check_exit_code_zero_on_safe() {
        let safe = json!({ "status": "safe", "safe_to_retire": true, "blockers": [] });
        assert_eq!(retirement_check_exit_code(&safe), 0);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_blocked() {
        let blocked = json!({
            "status": "blocked",
            "safe_to_retire": false,
            "blockers": [{ "active_executions": 3 }]
        });
        assert_eq!(retirement_check_exit_code(&blocked), 1);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_partial() {
        let partial = json!({ "status": "partial", "safe_to_retire": false });
        assert_eq!(retirement_check_exit_code(&partial), 1);
    }

    #[test]
    fn retirement_check_exit_code_nonzero_on_unavailable() {
        let unavailable = json!({ "status": "unavailable", "safe_to_retire": false });
        assert_eq!(retirement_check_exit_code(&unavailable), 1);
    }

    #[test]
    fn version_gate_retirement_check_flag_not_set_by_default() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        assert!(!retirement_check_should_check(&cli));
    }

    #[test]
    fn version_gate_retirement_check_flag_set_when_passed() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
            "--check",
        ]);
        assert!(retirement_check_should_check(&cli));
    }

    #[test]
    fn version_gate_retirement_default_output_renders_table() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "blocked",
            "safe_to_retire": false,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {
                "change_id": "tax_v2",
                "min_safe_version": 2,
                "workflow_name": null,
                "state_group": "all",
                "shard_id": null
            },
            "blockers": [{
                "workflow_name": "billing_checkout",
                "change_id": "tax_v2",
                "recorded_version": 1,
                "active_executions": 2,
                "terminal_executions": 5,
                "oldest_blocker_age_secs": 3600,
                "newest_blocker_age_secs": 60,
                "sample_active_execution_ids": [],
                "shard_coverage": {
                    "inspected_shards": [0],
                    "matched_shards": [0],
                    "unavailable_shards": []
                }
            }],
            "shards": [{ "shard_id": 0, "status": "inspected", "matched_groups": 1, "error": null }]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");

        assert!(rendered.contains("WORKFLOW"));
        assert!(rendered.contains("billing_checkout"));
        assert!(rendered.contains("blocked"));
        assert!(rendered.contains("tax_v2"));
        assert!(!rendered.trim_start().starts_with('{'));
    }

    #[test]
    fn version_gate_retirement_json_output_preserves_raw_payload() {
        let cli = parse(&[
            "--output",
            "json",
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "safe",
            "safe_to_retire": true,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {},
            "blockers": [],
            "shards": []
        });

        let rendered = render_response(&cli, &payload).expect("json output should render");

        assert!(rendered.trim_start().starts_with('{'));
        assert!(rendered.contains("\"safe_to_retire\":true"));
    }

    // -- Worker subcommand (issue #170) --

    #[test]
    fn worker_drain_builds_post_request() {
        let req = parse(&["worker", "drain", "w-abc"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workers/w-abc/drain");
        assert!(req.body.is_some());
    }

    #[test]
    fn worker_drain_without_deadline_sends_empty_body() {
        let req = parse(&["worker", "drain", "w-abc"]).api_request().unwrap();
        let body = req.body.as_ref().unwrap();
        assert!(body.get("deadline_at").is_none());
    }

    #[test]
    fn worker_drain_with_deadline_includes_deadline_in_body() {
        let req = parse(&[
            "worker",
            "drain",
            "w-abc",
            "--deadline",
            "2026-05-09T12:00:00Z",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert_eq!(
            body["deadline_at"].as_str().unwrap(),
            "2026-05-09T12:00:00Z"
        );
    }

    #[test]
    fn worker_drain_preview_builds_get_request() {
        let req = parse(&["worker", "drain-preview"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/drain-preview");
        assert!(req.body.is_none());
    }

    #[test]
    fn worker_drain_preview_with_queue_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--queue", "email-workers"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert!(
            req.path.contains("queue=email-workers"),
            "path: {}",
            req.path
        );
    }

    #[test]
    fn worker_drain_preview_with_shard_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--shard-id", "2"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("shard_id=2"), "path: {}", req.path);
    }

    #[test]
    fn worker_drain_preview_with_status_filter_sends_param() {
        let req = parse(&["worker", "drain-preview", "--status", "Active"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("status=Active"), "path: {}", req.path);
    }

    #[test]
    fn worker_list_builds_get_request() {
        let req = parse(&["worker", "list"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers");
    }

    #[test]
    fn worker_list_with_status_filter_sends_param() {
        let req = parse(&["worker", "list", "--status", "Draining"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("status=Draining"), "path: {}", req.path);
    }

    #[test]
    fn worker_list_with_queue_filter_sends_param() {
        let req = parse(&["worker", "list", "--queue", "default"])
            .api_request()
            .unwrap();
        assert!(req.path.contains("queue=default"), "path: {}", req.path);
    }

    #[test]
    fn worker_get_builds_get_request() {
        let req = parse(&["worker", "get", "w-xyz"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/w-xyz");
    }

    #[test]
    fn worker_health_builds_get_request() {
        let req = parse(&["worker", "health"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workers/health");
    }

    // -- Drain wait mode (AC #6) --

    #[test]
    fn worker_drain_with_wait_flag_still_builds_drain_post_request() {
        let req = parse(&["worker", "drain", "w-abc", "--wait"])
            .api_request()
            .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workers/w-abc/drain");
    }

    #[test]
    fn worker_drain_wait_and_deadline_are_independent_flags() {
        let req = parse(&[
            "worker",
            "drain",
            "w-abc",
            "--wait",
            "--deadline",
            "2026-05-09T12:00:00Z",
        ])
        .api_request()
        .unwrap();
        let body = req.body.as_ref().unwrap();
        assert_eq!(
            body["deadline_at"].as_str().unwrap(),
            "2026-05-09T12:00:00Z"
        );
    }

    #[test]
    fn worker_drain_wait_timeout_secs_default_is_120() {
        let cli = parse(&["worker", "drain", "w-abc", "--wait"]);
        if let Commands::Worker {
            command:
                WorkerCommand::Drain {
                    wait,
                    wait_timeout_secs,
                    ..
                },
        } = &cli.command
        {
            assert!(*wait);
            assert_eq!(*wait_timeout_secs, 120);
        } else {
            panic!("expected Worker::Drain command");
        }
    }

    #[test]
    fn worker_drain_without_wait_flag_wait_is_false() {
        let cli = parse(&["worker", "drain", "w-abc"]);
        if let Commands::Worker {
            command: WorkerCommand::Drain { wait, .. },
        } = &cli.command
        {
            assert!(!*wait);
        } else {
            panic!("expected Worker::Drain command");
        }
    }

    #[test]
    fn version_gate_retirement_empty_blockers_shows_no_old_version_message() {
        let cli = parse(&[
            "version-gate-retirement",
            "--change-id",
            "tax_v2",
            "--min-safe-version",
            "2",
        ]);
        let payload = json!({
            "status": "safe",
            "safe_to_retire": true,
            "observed_at": "2026-05-07T12:00:00Z",
            "filters": {
                "change_id": "tax_v2",
                "min_safe_version": 2,
                "workflow_name": null,
                "state_group": "all",
                "shard_id": null
            },
            "blockers": [],
            "shards": [{ "shard_id": 0, "status": "inspected", "matched_groups": 0, "error": null }]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");

        assert!(rendered.contains("No old-version executions found"));
    }

    #[test]
    fn rate_limit_status_builds_get_request() {
        let req = parse(&["rate-limit", "status"]).api_request().unwrap();
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/admin/rate-limits");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn rate_limit_set_builds_post_request() {
        let req = parse(&[
            "rate-limit",
            "set",
            "my-key",
            "--refill-rate",
            "10.5",
            "--burst",
            "20",
        ])
        .api_request()
        .unwrap();
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/admin/rate-limits/my-key");
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["refill_rate"].as_f64().unwrap(), 10.5);
        assert_eq!(body["burst"].as_f64().unwrap(), 20.0);
    }

    #[test]
    fn rate_limit_table_renders_headers_and_rows() {
        let cli = parse(&["rate-limit", "status"]);
        let payload = json!([
            {
                "key": "test-key-1",
                "refill_rate": 5.0,
                "burst": 10.0,
                "tokens": 8.5,
                "last_refilled_at": "2026-05-22T22:00:00Z"
            }
        ]);
        let rendered = render_response(&cli, &payload).unwrap();
        assert!(rendered.contains("KEY"));
        assert!(rendered.contains("REFILL_RATE"));
        assert!(rendered.contains("BURST_CAPACITY"));
        assert!(rendered.contains("CURRENT_TOKENS"));
        assert!(rendered.contains("LAST_REFILLED_AT"));
        assert!(rendered.contains("test-key-1"));
        assert!(rendered.contains("5.00"));
        assert!(rendered.contains("10.00"));
        assert!(rendered.contains("8.50"));
        assert!(rendered.contains("2026-05-22T22:00:00Z"));
    }
}

#[cfg(test)]
mod conflict_policy_tests {
    //! CLI mapping tests for `--conflict-policy` on `workflow start` (issue #685).
    //! Mirror `mod reuse_policy_tests`: omit → no field; each of the 4 values
    //! sends the correct `snake_case` string; preserves other fields alongside.
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
    fn start_omitting_conflict_policy_sends_no_field() {
        let req = start_request(&["workflow", "start", "my_wf"]);
        let body = req.body.as_ref().expect("start should have a body");
        assert!(
            body.get("conflict_policy").is_none(),
            "omitting --conflict-policy must not send the field"
        );
    }

    #[test]
    fn start_unspecified_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "unspecified",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "unspecified");
    }

    #[test]
    fn start_fail_sends_correct_value() {
        let req = start_request(&["workflow", "start", "my_wf", "--conflict-policy", "fail"]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "fail");
    }

    #[test]
    fn start_use_existing_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "use_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "use_existing");
    }

    #[test]
    fn start_terminate_existing_sends_correct_value() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--conflict-policy",
            "terminate_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["conflict_policy"], "terminate_existing");
    }

    #[test]
    fn start_preserves_other_fields_alongside_conflict_policy() {
        let req = start_request(&[
            "workflow",
            "start",
            "my_wf",
            "--workflow-id",
            "wf-123",
            "--reuse-policy",
            "terminate_if_running",
            "--conflict-policy",
            "use_existing",
        ]);
        let body = req.body.as_ref().unwrap();
        assert_eq!(body["workflow_id"], "wf-123");
        assert_eq!(body["reuse_policy"], "terminate_if_running");
        assert_eq!(body["conflict_policy"], "use_existing");
    }
}

#[cfg(test)]
mod dlq_aggregate_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn aggregate_maps_to_get_with_repeated_group_by() {
        let req = request(&[
            "dlq",
            "aggregate",
            "--group-by",
            "workflow_name,failure_signature",
            "--since",
            "24h",
            "--samples-per-group",
            "3",
        ]);
        assert_eq!(req.method, ApiMethod::Get);
        assert!(req.path.starts_with("/dead-letters/aggregate?"));
        assert!(
            req.path.contains("group_by=workflow_name"),
            "path: {}",
            req.path
        );
        assert!(
            req.path.contains("group_by=failure_signature"),
            "path: {}",
            req.path
        );
        assert!(req.path.contains("since=24h"), "path: {}", req.path);
        assert!(
            req.path.contains("samples_per_group=3"),
            "path: {}",
            req.path
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn aggregate_passes_all_filters() {
        let req = request(&[
            "dlq",
            "aggregate",
            "--group-by",
            "queue_name",
            "--time-bucket",
            "day",
            "--workflow-name",
            "onboarding",
            "--activity-name",
            "charge_card",
            "--queue-name",
            "billing",
            "--until",
            "2026-05-18T04:00:00Z",
            "--min-attempts",
            "3",
            "--limit-groups",
            "100",
        ]);
        assert!(req.path.contains("time_bucket=day"));
        assert!(req.path.contains("workflow_name=onboarding"));
        assert!(req.path.contains("activity_name=charge_card"));
        assert!(req.path.contains("queue_name=billing"));
        assert!(req.path.contains("min_attempts=3"));
        assert!(req.path.contains("limit_groups=100"));
    }

    #[test]
    fn aggregate_requires_group_by() {
        let parsed = Cli::try_parse_from(["harvest", "dlq", "aggregate"]);
        assert!(parsed.is_err(), "--group-by is required");
    }

    #[test]
    fn aggregate_rejects_out_of_range_limit_groups() {
        let parsed = Cli::try_parse_from([
            "harvest",
            "dlq",
            "aggregate",
            "--group-by",
            "queue_name",
            "--limit-groups",
            "9999",
        ]);
        assert!(
            parsed.is_err(),
            "limit_groups > 500 must be rejected by clap"
        );
    }

    #[test]
    fn aggregate_table_renders_groups_and_other_rollup() {
        let cli = parse(&["dlq", "aggregate", "--group-by", "workflow_name"]);
        let payload = json!({
            "total": 100,
            "filtered_total": 100,
            "truncated": true,
            "groups": [
                {
                    "key": {"workflow_name": "onboarding"},
                    "count": 60,
                    "first_seen": "2026-05-18T03:00:00Z",
                    "last_seen": "2026-05-18T04:00:00Z",
                    "sample_dead_letter_ids": ["id-a", "id-b"]
                },
                {
                    "key": {"_other": true},
                    "count": 40,
                    "sample_dead_letter_ids": []
                }
            ]
        });

        let rendered = render_response(&cli, &payload).expect("table should render");
        assert!(rendered.contains("WORKFLOW_NAME"), "{rendered}");
        assert!(rendered.contains("COUNT"), "{rendered}");
        assert!(rendered.contains("onboarding"), "{rendered}");
        assert!(rendered.contains("(other)"), "{rendered}");
        assert!(rendered.contains("id-a,id-b"), "{rendered}");
        assert!(
            rendered.contains("long tail rolled into _other"),
            "{rendered}"
        );
    }

    #[test]
    fn aggregate_json_flag_emits_raw_payload() {
        let cli = parse(&["dlq", "aggregate", "--group-by", "workflow_name", "--json"]);
        let payload = json!({"total": 1, "filtered_total": 1, "truncated": false, "groups": []});
        let rendered = render_response(&cli, &payload).expect("json should render");
        // Compact JSON (no pretty indentation) for piping.
        assert!(rendered.starts_with('{'));
        assert!(rendered.contains("\"total\":1"));
    }
}

#[cfg(test)]
mod erase_payloads_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn erase_payloads_builds_post_request() {
        let req = request(&["workflow", "erase-payloads", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/erase-payloads");
    }

    #[test]
    fn erase_payloads_with_reason_includes_reason_in_body() {
        let req = request(&[
            "workflow",
            "erase-payloads",
            "abc-123",
            "--reason",
            "GDPR Art. 17 request DSR-99",
        ]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/erase-payloads");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "GDPR Art. 17 request DSR-99");
    }

    #[test]
    fn erase_payloads_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "erase-payloads", "abc-123"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }
}

#[cfg(test)]
mod legal_hold_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn legal_hold_set_builds_post_request_with_reason() {
        let req = request(&["legal-hold", "set", "abc-123", "--reason", "case 42"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/legal-hold");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "case 42");
        assert!(
            body.get("hold_until").is_none() || body["hold_until"].is_null(),
            "omitting --until must not send hold_until"
        );
    }

    #[test]
    fn legal_hold_set_includes_until_when_provided() {
        let req = request(&[
            "legal-hold",
            "set",
            "abc-123",
            "--reason",
            "case 42",
            "--until",
            "2027-01-01T00:00:00Z",
        ]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["hold_until"], "2027-01-01T00:00:00Z");
    }

    #[test]
    fn legal_hold_release_builds_post_request() {
        let req = request(&["legal-hold", "release", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/legal-hold/release");
    }

    #[test]
    fn legal_hold_set_requires_reason() {
        // clap must reject `set` without --reason.
        let res = Cli::try_parse_from(["harvest", "legal-hold", "set", "abc-123"]);
        assert!(res.is_err(), "--reason is required for legal-hold set");
    }
}

#[cfg(test)]
mod pause_resume_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn pause_builds_post_request() {
        let req = request(&["workflow", "pause", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/pause");
    }

    #[test]
    fn pause_with_reason_includes_reason_in_body() {
        let req = request(&[
            "workflow",
            "pause",
            "abc-123",
            "--reason",
            "investigating incident INC-42",
        ]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/pause");
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "investigating incident INC-42");
    }

    #[test]
    fn pause_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "pause", "abc-123"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }

    #[test]
    fn resume_builds_post_request_with_no_body() {
        let req = request(&["workflow", "resume", "abc-123"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/abc-123/resume");
        assert!(req.body.is_none(), "resume must send no request body");
    }
}

#[cfg(test)]
mod retry_activity_cli_tests {
    use super::*;

    fn request(args: &[&str]) -> ApiRequest {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn retry_activity_builds_post_request_with_correct_path() {
        let req = request(&["workflow", "retry-activity", "exec-123", "act-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/exec-123/activities/act-456/retry-now");
    }

    #[test]
    fn retry_activity_sends_no_body() {
        let req = request(&["workflow", "retry-activity", "exec-123", "act-456"]);
        assert!(
            req.body.is_none(),
            "retry-activity must send no request body"
        );
    }
}

#[cfg(test)]
mod fail_activity_cli_tests {
    use super::*;

    fn request(args: &[&str]) -> ApiRequest {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn fail_activity_builds_post_request_with_correct_path() {
        let req = request(&["workflow", "fail-activity", "exec-123", "act-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(req.path, "/workflows/exec-123/activities/act-456/fail-now");
    }

    #[test]
    fn fail_activity_with_reason_sends_reason_body() {
        let req = request(&[
            "workflow",
            "fail-activity",
            "exec-123",
            "act-456",
            "--reason",
            "hung on dead downstream, INC-42",
        ]);
        let body = req.body.as_ref().expect("should have a body");
        assert_eq!(body["reason"], "hung on dead downstream, INC-42");
    }

    #[test]
    fn fail_activity_without_reason_sends_no_reason_field() {
        let req = request(&["workflow", "fail-activity", "exec-123", "act-456"]);
        let body = req.body.as_ref().expect("should have a body");
        assert!(
            body.get("reason").is_none() || body["reason"].is_null(),
            "omitting --reason must not send the field"
        );
    }
}

#[cfg(test)]
mod completion_delivery_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn list_builds_get_request_with_correct_path() {
        let req = request(&["completion-delivery", "list", "exec-123"]);
        assert_eq!(req.method, ApiMethod::Get);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
        assert!(req.body.is_none());
    }

    #[test]
    fn list_alias_completion_deliveries_parses() {
        let req = request(&["completion-deliveries", "list", "exec-123"]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn list_alias_callbacks_parses() {
        let req = request(&["callbacks", "list", "exec-123"]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn list_with_state_does_not_send_state_as_a_query_param() {
        // --state is applied client-side in render_response, not sent to the
        // server (the endpoint has no query-param filter).
        let req = request(&[
            "completion-delivery",
            "list",
            "exec-123",
            "--state",
            "failed",
        ]);
        assert_eq!(req.path, "/workflows/exec-123/completion-deliveries");
    }

    #[test]
    fn redrive_builds_post_request_with_correct_path_and_no_body() {
        let req = request(&["completion-delivery", "redrive", "exec-123", "delivery-456"]);
        assert_eq!(req.method, ApiMethod::Post);
        assert_eq!(
            req.path,
            "/workflows/exec-123/completion-deliveries/delivery-456/redrive"
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn execution_id_and_delivery_id_are_path_segment_encoded() {
        let req = request(&["completion-delivery", "redrive", "exec/123", "del/456"]);
        assert!(!req.path.contains("exec/123"));
        assert!(!req.path.contains("del/456"));
        assert!(req.path.contains("exec%2F123"));
        assert!(req.path.contains("del%2F456"));
    }

    #[test]
    fn filter_completion_deliveries_by_state_is_case_insensitive_and_keeps_matches() {
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "FAILED" },
            { "delivery_id": "c", "state": "DELIVERED" },
        ]);
        let filtered = filter_completion_deliveries_by_state(&value, "failed");
        assert_eq!(filtered, json!([{ "delivery_id": "b", "state": "FAILED" }]));
    }

    #[test]
    fn filter_completion_deliveries_by_state_passes_non_array_through_unchanged() {
        let value = json!({ "ok": true });
        let filtered = filter_completion_deliveries_by_state(&value, "failed");
        assert_eq!(filtered, value);
    }

    #[test]
    fn render_response_applies_state_filter_only_for_completion_delivery_list() {
        let cli = parse(&[
            "completion-delivery",
            "list",
            "exec-123",
            "--state",
            "delivered",
        ]);
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "DELIVERED" },
        ]);
        let rendered = render_response(&cli, &value).expect("should render");
        let parsed: Value = serde_json::from_str(&rendered).expect("valid json");
        assert_eq!(
            parsed,
            json!([{ "delivery_id": "b", "state": "DELIVERED" }])
        );
    }

    #[test]
    fn render_response_without_state_flag_returns_full_list() {
        let cli = parse(&["completion-delivery", "list", "exec-123"]);
        let value = json!([
            { "delivery_id": "a", "state": "PENDING" },
            { "delivery_id": "b", "state": "DELIVERED" },
        ]);
        let rendered = render_response(&cli, &value).expect("should render");
        let parsed: Value = serde_json::from_str(&rendered).expect("valid json");
        assert_eq!(parsed, value);
    }

    #[test]
    fn redrive_command_never_applies_the_list_state_filter() {
        // Sanity check that completion_delivery_list_state_filter is scoped
        // to List and does not accidentally intercept Redrive's response.
        let cli = parse(&["completion-delivery", "redrive", "exec-123", "delivery-456"]);
        assert!(completion_delivery_list_state_filter(&cli).is_none());
    }
}

#[cfg(test)]
mod usage_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn request(args: &[&str]) -> ApiRequest {
        parse(args)
            .api_request()
            .expect("request mapping should succeed")
    }

    #[test]
    fn usage_maps_to_get_admin_usage_with_from_and_to() {
        let req = request(&[
            "usage",
            "--from",
            "2026-01-01T00:00:00Z",
            "--to",
            "2026-02-01T00:00:00Z",
        ]);
        assert_eq!(req.method, ApiMethod::Get);
        assert!(req.path.starts_with("/admin/usage?"), "path: {}", req.path);
        assert!(
            req.path.contains("from=2026-01-01T00%3A00%3A00Z")
                || req.path.contains("from=2026-01-01T00:00:00Z"),
            "path: {}",
            req.path
        );
        assert!(req.body.is_none());
    }

    #[test]
    fn usage_threads_group_by_into_query() {
        let req = request(&[
            "usage",
            "--from",
            "24h",
            "--to",
            "1h",
            "--group-by",
            "search_attr:tenant_id",
        ]);
        assert!(
            req.path.contains("group_by=search_attr%3Atenant_id")
                || req.path.contains("group_by=search_attr:tenant_id"),
            "path: {}",
            req.path
        );
        assert!(req.path.contains("from=24h"), "path: {}", req.path);
        assert!(req.path.contains("to=1h"), "path: {}", req.path);
    }

    #[test]
    fn usage_omits_group_by_when_not_supplied() {
        let req = request(&["usage", "--from", "24h", "--to", "1h"]);
        assert!(!req.path.contains("group_by"), "path: {}", req.path);
    }

    #[test]
    fn usage_requires_from_and_to() {
        assert!(Cli::try_parse_from(["harvest", "usage"]).is_err());
        assert!(Cli::try_parse_from(["harvest", "usage", "--from", "24h"]).is_err());
        assert!(Cli::try_parse_from(["harvest", "usage", "--to", "1h"]).is_err());
    }

    #[test]
    fn usage_wants_table_is_default_and_json_flag_switches_to_raw_json() {
        let table_cli = parse(&["usage", "--from", "24h", "--to", "1h"]);
        assert!(usage_wants_table(&table_cli));
        assert!(!usage_wants_raw_json(&table_cli));

        let json_cli = parse(&["usage", "--from", "24h", "--to", "1h", "--json"]);
        assert!(!usage_wants_table(&json_cli));
        assert!(usage_wants_raw_json(&json_cli));
    }

    #[test]
    fn format_usage_table_renders_header_and_rows() {
        let value = serde_json::json!({
            "status": "complete",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [
                {
                    "group": "onboarding",
                    "workflow_starts": 10,
                    "completed": 8,
                    "failed": 1,
                    "cancelled": 0,
                    "timed_out": 1,
                    "activity_executions": 25,
                    "activity_executions_failed": 2,
                    "activity_compute_seconds": 123.456
                }
            ],
            "unavailable_shards": []
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("status: complete"));
        assert!(rendered.contains("2026-01-01T00:00:00Z"));
        assert!(rendered.contains("group_by: workflow_name"));
        assert!(rendered.contains("onboarding"));
        assert!(rendered.contains("123.46"));
    }

    #[test]
    fn format_usage_table_notes_unavailable_shards() {
        let value = serde_json::json!({
            "status": "partial",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [],
            "unavailable_shards": [{"shard_id": 1, "reason": "connection refused"}]
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("unavailable shards: 1"), "{rendered}");
    }

    #[test]
    fn format_usage_table_handles_empty_groups() {
        let value = serde_json::json!({
            "status": "complete",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-02-01T00:00:00Z",
            "group_by": "workflow_name",
            "groups": [],
            "unavailable_shards": []
        });
        let rendered = format_usage_table(&value);
        assert!(rendered.contains("No usage groups found."));
    }
}

#[cfg(test)]
mod det_check_cli_tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    fn report(src: &str) -> DetCheckReport {
        autumn_harvest::check_source(src, "test.rs")
    }

    // NOTE: these fixtures embed `#[workflow]` source and MUST be single-line
    // string literals (with `\n` escapes), not multi-line `"\`-continuation
    // strings — a multi-line literal containing `#[workflow]` at a line start is
    // misread as a real workflow by the line-based det_check scanner (the
    // documented multi-line-string lexer caveat), producing a self-scan false
    // positive on this very file. Single-line literals are stripped correctly.
    const WF_TIME: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::time::SystemTime::now();\n    Ok(())\n}\n";

    const WF_WARN: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    let _ = std::process::id();\n    Ok(())\n}\n";

    const WF_SUPPRESSED: &str = "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    // harvest-suppress: DET001 \"recorded in signal payload\"\n    let _ = std::time::SystemTime::now();\n    Ok(())\n}\n";

    #[test]
    fn det_check_parses_paths_and_flags() {
        let cli = parse(&["det-check", "--format", "json", "some/path"]);
        match cli.command {
            Commands::DetCheck {
                paths,
                format,
                deny_warnings,
                list_suppressions,
            } => {
                assert_eq!(paths, vec![PathBuf::from("some/path")]);
                assert_eq!(format, DetCheckFormat::Json);
                assert!(!deny_warnings);
                assert!(!list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_defaults_to_current_dir_and_text_format() {
        let cli = parse(&["det-check"]);
        match cli.command {
            Commands::DetCheck {
                paths,
                format,
                deny_warnings,
                list_suppressions,
            } => {
                assert_eq!(paths, vec![PathBuf::from(".")]);
                assert_eq!(format, DetCheckFormat::Text);
                assert!(!deny_warnings);
                assert!(!list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_accepts_deny_warnings_and_list_suppressions() {
        let cli = parse(&[
            "det-check",
            "--deny-warnings",
            "--list-suppressions",
            "a",
            "b",
        ]);
        match cli.command {
            Commands::DetCheck {
                paths,
                deny_warnings,
                list_suppressions,
                ..
            } => {
                assert_eq!(paths, vec![PathBuf::from("a"), PathBuf::from("b")]);
                assert!(deny_warnings);
                assert!(list_suppressions);
            }
            other => panic!("expected DetCheck, got {other:?}"),
        }
    }

    #[test]
    fn det_check_format_default_is_text() {
        assert_eq!(DetCheckFormat::default(), DetCheckFormat::Text);
    }

    #[test]
    fn text_finding_line_has_location_rule_and_alternative() {
        let r = report(WF_TIME);
        let text = format_det_findings_text(&r);
        assert!(text.contains("DET001"), "{text}");
        assert!(text.contains("test.rs:"), "{text}");
        assert!(text.contains("safe alternative:"), "{text}");
        // A direct finding has no helper attribution.
        assert!(!text.contains("in helper"), "{text}");
    }

    #[test]
    fn transitive_text_line_names_helper_and_entry() {
        let src = "\
#[workflow]
async fn entry_wf(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = bad_helper();
    Ok(())
}

fn bad_helper() -> i64 {
    chrono::Utc::now().timestamp()
}
";
        let text = format_det_findings_text(&report(src));
        assert!(
            text.contains("in helper `bad_helper` reached from workflow `entry_wf`"),
            "{text}"
        );
    }

    #[test]
    fn text_findings_empty_report_is_no_findings() {
        let empty = DetCheckReport::default();
        assert_eq!(format_det_findings_text(&empty), "det-check: no findings");
    }

    #[test]
    fn suppression_formatter_renders_reason_and_location() {
        let r = report(WF_SUPPRESSED);
        let footer = format_det_suppressions(&r);
        assert!(footer.contains("suppressed:"), "{footer}");
        assert!(footer.contains("DET001"), "{footer}");
        assert!(footer.contains("recorded in signal payload"), "{footer}");

        let list = format_det_suppressions_list(&r);
        assert!(list.contains("DET001"), "{list}");
        assert!(list.contains("\"recorded in signal payload\""), "{list}");
    }

    #[test]
    fn suppression_formatters_handle_none() {
        let empty = DetCheckReport::default();
        assert_eq!(format_det_suppressions(&empty), "suppressed: none");
        assert_eq!(
            format_det_suppressions_list(&empty),
            "no active suppressions"
        );
    }

    #[test]
    fn gate_trips_on_hard_blocker() {
        let r = report(WF_TIME);
        let gated = det_check_gate(&r, false);
        assert!(matches!(
            gated,
            Some(CliError::DetCheckFindings { errors: 1, .. })
        ));
    }

    #[test]
    fn gate_passes_on_warning_only_unless_deny_warnings() {
        let r = report(WF_WARN);
        assert!(det_check_gate(&r, false).is_none());
        let gated = det_check_gate(&r, true);
        assert!(matches!(
            gated,
            Some(CliError::DetCheckFindings {
                errors: 0,
                warnings: 1
            })
        ));
    }

    #[test]
    fn gate_passes_on_clean_report() {
        let clean = report(
            "#[workflow]\nasync fn wf(ctx: &WorkflowContext) -> Result<(), String> {\n    ctx.timer(\"t\", 1).await?;\n    Ok(())\n}\n",
        );
        assert!(det_check_gate(&clean, false).is_none());
        assert!(det_check_gate(&clean, true).is_none());
    }

    #[test]
    fn det_check_findings_error_exits_with_code_one() {
        let err = CliError::DetCheckFindings {
            errors: 3,
            warnings: 1,
        };
        assert_eq!(err.exit_code(), 1);
    }

    #[test]
    fn det_check_json_serializes_report() {
        let json = det_check_json(&report(WF_TIME)).expect("json");
        assert!(json.contains("\"rule_id\""));
        assert!(json.contains("DET001"));
        assert!(json.contains("\"severity\": \"error\""));
    }

    // FIX #5: `--list-suppressions --format json` must emit JSON (not silently
    // fall back to the text listing). The output must parse as JSON containing
    // the suppression.
    #[test]
    fn det_suppressions_json_is_valid_json_containing_the_suppression() {
        let r = report(WF_SUPPRESSED);
        let json = det_suppressions_json(&r).expect("suppressions json");
        let value: Value = serde_json::from_str(&json).expect("output must be valid JSON");
        let sups = value["suppressions"]
            .as_array()
            .expect("suppressions array");
        assert!(
            sups.iter().any(|s| s["rule_id"] == "DET001"),
            "the suppression must be present in the JSON, got: {json}"
        );
        assert!(
            sups.iter()
                .any(|s| s["reason"] == "recorded in signal payload"),
            "the reason must be present in the JSON, got: {json}"
        );
    }

    #[test]
    fn run_det_check_list_suppressions_json_exits_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("sup.rs"), WF_SUPPRESSED).unwrap();
        let result = run_det_check(
            &[dir.path().to_path_buf()],
            DetCheckFormat::Json,
            false,
            true,
        );
        assert!(
            result.is_ok(),
            "--list-suppressions --format json must exit Ok, got: {result:?}"
        );
    }
}

#[cfg(test)]
mod token_bootstrap_tests {
    use super::*;

    /// The builder produces a valid `hvst_` secret, an INSERT statement whose
    /// `name`/`scope`/`created_by` match the inputs, embeds ONLY the hash, and
    /// never leaks the secret into the SQL (issue #942, Codex P1 output-shape check).
    #[test]
    fn bootstrap_builder_emits_secret_and_hash_only_sql() {
        let token = build_bootstrap_token("ci-seed", "mutate", None, "op").expect("builds");

        assert!(
            token.secret.starts_with("hvst_"),
            "secret must carry the hvst_ prefix: {}",
            token.secret
        );
        // No drift: the stored hash IS the shared core helper's output.
        assert_eq!(
            token.hash,
            autumn_harvest::api_token::hash_secret(&token.secret),
            "hash must be hash_secret(secret) — the shared mint helper"
        );

        let sql = &token.insert_sql;
        assert!(
            sql.contains("INSERT INTO harvest_api_tokens"),
            "must be an INSERT: {sql}"
        );
        assert!(sql.contains("'mutate'"), "scope must appear in SQL: {sql}");
        assert!(sql.contains("'ci-seed'"), "name must appear in SQL: {sql}");
        assert!(sql.contains("'op'"), "created_by must appear in SQL: {sql}");
        assert!(sql.contains(&token.hash), "hash must be embedded: {sql}");
        assert!(
            sql.contains("gen_random_uuid()"),
            "id default expected: {sql}"
        );
        assert!(sql.contains("NOW()"), "created_at default expected: {sql}");
        // The secret must NEVER be smuggled into the SQL — only the hash is.
        assert!(
            !sql.contains(&token.secret),
            "the plaintext secret must not appear in the INSERT SQL: {sql}"
        );
    }

    /// `--scope` defaults to `mutate` (a seed must be able to mint others) and
    /// `--created-by`/`--name` default to `bootstrap`.
    #[test]
    fn bootstrap_defaults_scope_to_mutate() {
        let cli = Cli::try_parse_from(["harvest", "token", "bootstrap"])
            .expect("token bootstrap should parse with no flags");
        match cli.command {
            Commands::Token {
                command:
                    TokenCommand::Bootstrap {
                        name,
                        scope,
                        expires_at,
                        created_by,
                    },
            } => {
                assert_eq!(scope, "mutate", "default scope must be mutate");
                assert_eq!(name, "bootstrap");
                assert_eq!(created_by, "bootstrap");
                assert_eq!(expires_at, None);
            }
            other => panic!("expected token bootstrap, got {other:?}"),
        }
    }

    /// The `value_parser` rejects a scope outside {read, mutate}.
    #[test]
    fn bootstrap_rejects_invalid_scope() {
        let result = Cli::try_parse_from(["harvest", "token", "bootstrap", "--scope", "admin"]);
        assert!(result.is_err(), "an invalid --scope must fail to parse");
    }

    /// A `read`-scoped bootstrap flows the flag values through to the SQL.
    #[test]
    fn bootstrap_read_scope_flows_through_to_sql() {
        let cli = Cli::try_parse_from([
            "harvest",
            "token",
            "bootstrap",
            "--scope",
            "read",
            "--name",
            "dashboard",
            "--created-by",
            "release-eng",
        ])
        .expect("parses");
        let Commands::Token {
            command:
                TokenCommand::Bootstrap {
                    name,
                    scope,
                    expires_at,
                    created_by,
                },
        } = cli.command
        else {
            panic!("expected token bootstrap");
        };
        let token = build_bootstrap_token(&name, &scope, expires_at.as_deref(), &created_by)
            .expect("builds");
        assert!(token.insert_sql.contains("'read'"));
        assert!(token.insert_sql.contains("'dashboard'"));
        assert!(token.insert_sql.contains("'release-eng'"));
        assert!(!token.insert_sql.contains(&token.secret));
    }

    /// A valid `--expires-at` is embedded as a `timestamptz` literal.
    #[test]
    fn bootstrap_with_expiry_includes_timestamptz() {
        let token =
            build_bootstrap_token("n", "read", Some("2027-01-01T00:00:00Z"), "op").expect("builds");
        assert!(token.insert_sql.contains("expires_at"), "column present");
        assert!(
            token
                .insert_sql
                .contains("'2027-01-01T00:00:00Z'::timestamptz"),
            "expiry literal present: {}",
            token.insert_sql
        );
    }

    /// A malformed `--expires-at` is rejected before any SQL is emitted.
    #[test]
    fn bootstrap_rejects_bad_expiry() {
        let err = build_bootstrap_token("n", "read", Some("not-a-date"), "op");
        assert!(err.is_err(), "a non-RFC-3339 expiry must be rejected");
    }

    /// A crafted name with a single quote is escaped (Postgres `'` -> `''`),
    /// neutralizing SQL injection through the flag value.
    #[test]
    fn bootstrap_escapes_single_quotes_in_name() {
        let token = build_bootstrap_token("O'Brien", "read", None, "op").expect("builds");
        assert!(
            token.insert_sql.contains("'O''Brien'"),
            "single quote must be doubled: {}",
            token.insert_sql
        );
        assert!(!token.insert_sql.contains(&token.secret));
    }

    #[test]
    fn batch_preview_table_renders_count_sample_and_truncation() {
        let payload = serde_json::json!({
            "dry_run": true,
            "action": "Cancel",
            "matched_count": 150,
            "per_shard": [{ "shard_id": 0, "matched_count": 150 }],
            "sample": [
                { "execution_id": "e1", "workflow_name": "onboarding", "state": "RUNNING" },
                { "execution_id": "e2", "workflow_name": "onboarding", "state": "RUNNING" }
            ],
            "sample_cap": 100,
            "sample_truncated": true
        });
        let out = format_batch_preview_table(&payload);
        assert!(out.contains("DRY RUN"), "table: {out}");
        assert!(out.contains("matched_count: 150"), "table: {out}");
        assert!(out.contains("onboarding"), "sample rendered: {out}");
        assert!(
            out.contains("e1") && out.contains("e2"),
            "ids rendered: {out}"
        );
        // N2: the per_shard breakdown block renders.
        assert!(out.contains("shard"), "per_shard block rendered: {out}");
        assert!(
            out.contains("truncated: 2 of 150 shown"),
            "truncation note: {out}"
        );
    }

    /// M5: `batch submit --dry-run --json` renders the RAW compact preview body
    /// (no table header / `DRY RUN` banner), so `--json` output is pipeable.
    #[test]
    fn batch_preview_dry_run_json_renders_compact_body() {
        let cli = <Cli as clap::Parser>::try_parse_from([
            "harvest",
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
            "--dry-run",
            "--json",
        ])
        .expect("CLI should parse");
        let value = serde_json::json!({
            "dry_run": true,
            "action": "Cancel",
            "matched_count": 42,
            "per_shard": [{ "shard_id": 0, "matched_count": 42 }],
            "sample": [],
            "sample_cap": 100,
            "sample_truncated": false,
            "status": "complete"
        });
        let out = render_response(&cli, &value).expect("render");
        assert!(out.contains("\"matched_count\""), "raw JSON body: {out}");
        assert!(!out.contains("DRY RUN"), "no table banner in --json: {out}");
        // Compact (not pretty): no multi-space indentation.
        assert!(!out.contains("\n  "), "compact, not pretty-printed: {out}");
    }

    /// N1: `--json` without `--dry-run` is rejected at clap parse time
    /// (`requires = "dry_run"`).
    #[test]
    fn batch_submit_json_without_dry_run_rejected() {
        let result = <Cli as clap::Parser>::try_parse_from([
            "harvest",
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
            "--json",
        ]);
        assert!(
            result.is_err(),
            "--json without --dry-run must be a clap parse error"
        );
    }

    #[test]
    fn batch_preview_wants_table_only_with_dry_run() {
        fn parse_cli(args: &[&str]) -> Cli {
            <Cli as clap::Parser>::try_parse_from(
                std::iter::once("harvest").chain(args.iter().copied()),
            )
            .expect("CLI should parse")
        }
        let base = &[
            "batch",
            "submit",
            "Cancel",
            "--filter-json",
            r#"{"workflow_name":"x"}"#,
        ];

        let with_dry = parse_cli(&[base.as_slice(), &["--dry-run"]].concat());
        assert!(batch_preview_wants_table(&with_dry));
        assert!(!batch_preview_wants_raw_json(&with_dry));

        let dry_json = parse_cli(&[base.as_slice(), &["--dry-run", "--json"]].concat());
        assert!(!batch_preview_wants_table(&dry_json));
        assert!(batch_preview_wants_raw_json(&dry_json));

        let real = parse_cli(base);
        assert!(
            !batch_preview_wants_table(&real),
            "real submit must not table-render"
        );
    }
}

#[cfg(test)]
mod scaffold_new_tests {
    //! Unit tests for the `harvest new` scaffold (issue #692): clap parsing of
    //! the `New` variant and the name-derivation / keyword-rejection helpers.
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("harvest").chain(args.iter().copied()))
            .expect("CLI should parse successfully")
    }

    #[test]
    fn new_parses_name_and_flags() {
        let cli = parse(&[
            "new",
            "orders",
            "--force",
            "--template",
            "minimal",
            "--path",
            "/tmp/x",
        ]);
        match cli.command {
            Commands::New {
                name,
                path,
                force,
                template,
            } => {
                assert_eq!(name, "orders");
                assert_eq!(path, Some(PathBuf::from("/tmp/x")));
                assert!(force);
                assert_eq!(template, ScaffoldTemplate::Minimal);
            }
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn new_defaults_no_path_no_force_minimal_template() {
        let cli = parse(&["new", "orders"]);
        match cli.command {
            Commands::New {
                name,
                path,
                force,
                template,
            } => {
                assert_eq!(name, "orders");
                assert_eq!(path, None);
                assert!(!force);
                assert_eq!(template, ScaffoldTemplate::Minimal);
            }
            other => panic!("expected New, got {other:?}"),
        }
    }

    #[test]
    fn derive_crate_ident_hyphens_to_underscores() {
        assert_eq!(derive_crate_ident("my-app"), "my_app");
        assert_eq!(derive_crate_ident("plain"), "plain");
    }

    #[test]
    fn keyword_names_are_rejected() {
        for kw in ["fn", "match", "async", "type", "move", "struct"] {
            assert!(
                validate_project_name(kw).is_err(),
                "keyword {kw:?} must be rejected"
            );
        }
    }

    #[test]
    fn valid_names_pass_validation() {
        for ok in ["orders", "my-app", "app2", "a"] {
            assert!(
                validate_project_name(ok).is_ok(),
                "{ok:?} should be a valid name"
            );
        }
    }

    #[test]
    fn uppercase_names_stay_accepted_cargo_new_parity() {
        // Acceptance must match `cargo new`: an uppercase name is legal (only a
        // cosmetic cargo warning), even when it case-folds to a keyword/reserved
        // word. Only names that ARE a keyword/reserved word are rejected.
        for ok in ["MyApp", "Fn", "Core", "Orders"] {
            assert!(
                validate_project_name(ok).is_ok(),
                "{ok:?} should be accepted (cargo-new parity)"
            );
        }
        for bad in ["fn", "core", "async"] {
            assert!(
                validate_project_name(bad).is_err(),
                "{bad:?} should be rejected (keyword/reserved)"
            );
        }
    }

    #[test]
    fn derive_crate_ident_is_clean_snake_case() {
        assert_eq!(derive_crate_ident("MyApp"), "myapp");
        assert_eq!(derive_crate_ident("trail-"), "trail");
        assert_eq!(derive_crate_ident("my--app"), "my_app");
        assert_eq!(derive_crate_ident("A_B-c"), "a_b_c");
    }
}
