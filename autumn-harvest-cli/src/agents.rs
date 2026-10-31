//! `harvest agents`: list, describe, pause, resume and erase durable agents
//! (issue #2267).
//!
//! A durable agent is a keyed-entity run of the workflow `agent`. Its id,
//! `<kind>/<name>`, is the workflow id. Each command uses the generic routes
//! of the management API:
//!
//! | Command | Route |
//! |---|---|
//! | `list` | `GET /workflows` with a `filter` on the agent search attributes |
//! | `describe` | `GET /workflows/by-id/agent/{id}/query/harvest.entity.state` |
//! | `pause`, `resume` | `POST /workflows/by-id/agent/{id}/signal/harvest.entity.op` |
//! | `erase` | the signal route, then `POST /workflows/{id}/erase-payloads` for each run |
//!
//! `erase` restarts an agent that ended with `POST /workflows/agent/signal-with-start`.
//! The new run clears the memory and the workspace of the agent.

use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use std::fmt::Write as _;

use clap::Subcommand;
use serde_json::{Map, Value, json};

use crate::{ApiRequest, Cli, CliError, execute_request, path_segment, query_encode};

/// The workflow name of a durable agent. It is
/// `autumn_harvest_agent::AGENT_WORKFLOW_NAME`.
pub const AGENT_WORKFLOW: &str = "agent";

/// The page size of `list` when the caller gives none.
const LIST_PAGE_SIZE: i64 = 50;

/// The page size of the run list that `erase` reads.
const ERASE_PAGE_SIZE: i64 = 200;

/// The time between two reads of a run that `erase` waits for.
const ERASE_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// The counts of an erase outcome that the report sums.
const ERASE_COUNTS: [&str; 9] = [
    "events_scrubbed",
    "fields_tombstoned",
    "signals_scrubbed",
    "logs_deleted",
    "stream_chunks_deleted",
    "ledger_rows_deleted",
    "entity_states_scrubbed",
    "completion_deliveries_scrubbed",
    "dead_letters_scrubbed",
];

/// The `harvest agents` subcommands.
#[derive(Debug, Subcommand)]
pub enum AgentCommand {
    /// List running agents. The filters join with AND.
    List {
        /// Only agents of this kind.
        #[arg(long)]
        kind: Option<String>,
        /// Only agents with this status: idle, running, `waiting_approval`,
        /// paused or parked.
        #[arg(long)]
        status: Option<String>,
        /// Only agents that wake before this time, in epoch milliseconds.
        #[arg(long)]
        wakes_before: Option<i64>,
        /// Only agents of this mesh.
        #[arg(long)]
        mesh: Option<String>,
        /// Only agents of this tenant.
        #[arg(long)]
        tenant: Option<String>,
        /// The page size. The server caps it.
        #[arg(long)]
        page_size: Option<i64>,
        /// The `next_cursor` of the page before.
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show the committed state of an agent.
    Describe {
        /// The agent id, `<kind>/<name>`.
        agent_id: String,
    },
    /// Hold the work of an agent. Control ops still apply.
    Pause {
        /// The agent id, `<kind>/<name>`.
        agent_id: String,
        /// Why, for the audit trail of the agent.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Run the held work of an agent in order, then take new work.
    Resume {
        /// The agent id, `<kind>/<name>`.
        agent_id: String,
    },
    /// Erase an agent: its memory, workspace, history, stream chunks and
    /// cost ledger rows.
    ///
    /// The agent first gets a pause, a memory reset, a workspace reset and a
    /// delete. An agent that ended gets a new run for them. The command
    /// waits for the run to end. It then erases each run of each life of
    /// the agent, and prints one report. `stores_reset` is `false` when a
    /// reset failed, and `stores_error` names the error.
    Erase {
        /// The agent id, `<kind>/<name>`.
        agent_id: String,
        /// Why, for the audit log.
        #[arg(long)]
        reason: Option<String>,
        /// The longest wait for a running agent to end, in seconds.
        #[arg(long, default_value_t = 60)]
        wait_timeout_secs: u64,
    },
}

/// The request of a one-request agent command. `erase` has none.
pub fn agent_request(command: &AgentCommand) -> Result<ApiRequest, CliError> {
    match command {
        AgentCommand::List {
            kind,
            status,
            wakes_before,
            mesh,
            tenant,
            page_size,
            cursor,
        } => Ok(ApiRequest::get(list_path(
            &ListFilter {
                kind: kind.as_deref(),
                status: status.as_deref(),
                wakes_before: *wakes_before,
                mesh: mesh.as_deref(),
                tenant: tenant.as_deref(),
            },
            page_size.unwrap_or(LIST_PAGE_SIZE),
            cursor.as_deref(),
        ))),
        AgentCommand::Describe { agent_id } => Ok(ApiRequest::get(format!(
            "{}/query/{ENTITY_STATE_QUERY}",
            by_id(agent_id)
        ))),
        AgentCommand::Pause { agent_id, reason } => {
            let mut control = Map::new();
            control.insert("action".to_owned(), json!("pause"));
            if let Some(reason) = reason {
                control.insert("reason".to_owned(), json!(reason));
            }
            Ok(signal(agent_id, op_payload(control)))
        }
        AgentCommand::Resume { agent_id } => Ok(signal(agent_id, control_payload("resume"))),
        AgentCommand::Erase { .. } => Err(CliError::AgentEraseIsMultiStep),
    }
}

/// The filters of `agents list`.
struct ListFilter<'a> {
    kind: Option<&'a str>,
    status: Option<&'a str>,
    wakes_before: Option<i64>,
    mesh: Option<&'a str>,
    tenant: Option<&'a str>,
}

/// The `GET /workflows` path of `agents list`.
fn list_path(filter: &ListFilter<'_>, page_size: i64, cursor: Option<&str>) -> String {
    let mut expr = format!("workflow_name = {}", literal(AGENT_WORKFLOW));
    for (key, value) in [
        ("agent_kind", filter.kind),
        ("agent_status", filter.status),
        ("agent_mesh", filter.mesh),
        ("agent_tenant", filter.tenant),
    ] {
        if let Some(value) = value {
            let _ = write!(expr, " AND attrs.{key} = {}", literal(value));
        }
    }
    if let Some(before) = filter.wakes_before {
        let _ = write!(expr, " AND attrs.agent_next_wake_at < {before}");
    }
    let mut path = format!(
        "/workflows?state=RUNNING&filter={}&page_size={page_size}",
        query_encode(&expr)
    );
    if let Some(cursor) = cursor {
        let _ = write!(path, "&cursor={}", query_encode(cursor));
    }
    path
}

/// A quoted string of the `filter` grammar. A backslash escapes the next
/// character.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The by-id route of the agent `agent_id`.
fn by_id(agent_id: &str) -> String {
    format!(
        "/workflows/by-id/{AGENT_WORKFLOW}/{}",
        path_segment(agent_id)
    )
}

/// An entity op signal to the agent `agent_id`.
fn signal(agent_id: &str, payload: Value) -> ApiRequest {
    ApiRequest::post(
        format!("{}/signal/{ENTITY_OP_SIGNAL}", by_id(agent_id)),
        Some(payload),
    )
}

/// The entity message of one control op: `{"kind":"op","op":<control>}`.
fn op_payload(control: Map<String, Value>) -> Value {
    let mut op = control;
    op.insert("type".to_owned(), json!("control"));
    json!({"kind": "op", "op": op})
}

/// The entity message of the control op `action`.
fn control_payload(action: &str) -> Value {
    let mut control = Map::new();
    control.insert("action".to_owned(), json!(action));
    op_payload(control)
}

/// Run `agents erase`.
///
/// # Errors
///
/// Returns an error when a request fails, or when the agent does not end
/// within `wait_timeout_secs`.
pub async fn run_agent_erase(
    cli: &Cli,
    agent_id: &str,
    reason: Option<&str>,
    wait_timeout_secs: u64,
) -> Result<(), CliError> {
    let report = erase_agent(cli, agent_id, reason, wait_timeout_secs).await?;
    println!("{}", crate::format_output(&report, cli.output)?);
    Ok(())
}

/// Erase every run of the agent `agent_id` and return one report.
///
/// # Errors
///
/// As [`run_agent_erase`].
pub async fn erase_agent(
    cli: &Cli,
    agent_id: &str,
    reason: Option<&str>,
    wait_timeout_secs: u64,
) -> Result<Value, CliError> {
    let current = execute_request(cli, &ApiRequest::get(by_id(agent_id))).await?;
    // The pause comes first. A paused agent holds new work and skips its
    // ticks, so nothing writes the stores after the resets.
    let failures_before = if is_terminal(&current) {
        // The memory store scope and the workspace snapshots live outside
        // history, so they outlive the run. A new run clears them.
        let queue = execution(&current)
            .get("queue_name")
            .and_then(Value::as_str);
        execute_request(cli, &restart(agent_id, queue, control_payload("pause"))).await?;
        0
    } else {
        execute_request(cli, &signal(agent_id, control_payload("pause"))).await?;
        let state = execute_request(cli, &state_query(agent_id)).await?;
        failures(state.get("result").unwrap_or(&state))
    };
    // The resets run before the delete, in inbox order.
    for action in ["reset_memory", "reset_workspace"] {
        execute_request(cli, &signal(agent_id, control_payload(action))).await?;
    }
    execute_request(cli, &signal(agent_id, json!({"kind": "delete"}))).await?;
    let ended = wait_for_end(cli, agent_id, wait_timeout_secs).await?;
    let stores = stores_outcome(failures_before, execution(&ended).get("output"));
    let runs = list_runs(cli, agent_id).await?;
    let mut outcomes = Vec::new();
    for run in &runs {
        let Some(id) = run.get("id").and_then(Value::as_str) else {
            continue;
        };
        let mut body = Map::new();
        if let Some(reason) = reason {
            body.insert("reason".to_owned(), json!(reason));
        }
        let path = format!("/workflows/{}/erase-payloads", path_segment(id));
        outcomes
            .push(execute_request(cli, &ApiRequest::post(path, Some(Value::Object(body)))).await?);
    }
    Ok(erase_report(agent_id, &stores, &outcomes))
}

/// Whether the resets of `erase` worked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoresOutcome {
    /// `true` when both resets ran without a new failure.
    pub reset: bool,
    /// The error of a failed reset.
    pub error: Option<String>,
}

/// Compare the failure count of the agent before the resets with its final
/// state, the output of the deleted run.
///
/// A reset that fails counts a failure and keeps its error. A missing final
/// state proves nothing, so it is not a reset either.
pub fn stores_outcome(failures_before: u64, output: Option<&Value>) -> StoresOutcome {
    let Some(state) = output.filter(|state| state.is_object()) else {
        return StoresOutcome {
            reset: false,
            error: Some("the deleted run has no final state".to_owned()),
        };
    };
    if failures(state) > failures_before {
        let error = state
            .get("last_error")
            .and_then(Value::as_str)
            .unwrap_or("a reset failed");
        return StoresOutcome {
            reset: false,
            error: Some(error.to_owned()),
        };
    }
    StoresOutcome {
        reset: true,
        error: None,
    }
}

/// The failure count of an agent state.
fn failures(state: &Value) -> u64 {
    state.get("failures").and_then(Value::as_u64).unwrap_or(0)
}

/// The state query of the agent `agent_id`.
fn state_query(agent_id: &str) -> ApiRequest {
    ApiRequest::get(format!("{}/query/{ENTITY_STATE_QUERY}", by_id(agent_id)))
}

/// Start a new run of the agent `agent_id` with the entity message
/// `payload` on `queue`. The engine picks its default queue for `None`.
fn restart(agent_id: &str, queue: Option<&str>, payload: Value) -> ApiRequest {
    let mut body = Map::new();
    body.insert("workflow_id".to_owned(), json!(agent_id));
    // A keyed entity takes no start input.
    body.insert("start_input".to_owned(), json!({}));
    body.insert("signal_name".to_owned(), json!(ENTITY_OP_SIGNAL));
    body.insert("signal_payload".to_owned(), payload);
    body.insert("id_reuse_policy".to_owned(), json!("allow_duplicate"));
    if let Some(queue) = queue {
        body.insert("queue".to_owned(), json!(queue));
    }
    ApiRequest::post(
        format!("/workflows/{AGENT_WORKFLOW}/signal-with-start"),
        Some(Value::Object(body)),
    )
}

/// Wait until the current run of the agent ends. Returns that run.
async fn wait_for_end(
    cli: &Cli,
    agent_id: &str,
    wait_timeout_secs: u64,
) -> Result<Value, CliError> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_timeout_secs);
    loop {
        let run = execute_request(cli, &ApiRequest::get(by_id(agent_id))).await?;
        if is_terminal(&run) {
            return Ok(run);
        }
        if std::time::Instant::now() >= deadline {
            return Err(CliError::AgentEraseTimeout {
                agent_id: agent_id.to_owned(),
                seconds: wait_timeout_secs,
            });
        }
        tokio::time::sleep(ERASE_POLL).await;
    }
}

/// Every run of every life of the agent, one page at a time.
async fn list_runs(cli: &Cli, agent_id: &str) -> Result<Vec<Value>, CliError> {
    let expr = format!(
        "workflow_name = {} AND workflow_id = {}",
        literal(AGENT_WORKFLOW),
        literal(agent_id)
    );
    let mut runs = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut path = format!(
            "/workflows?filter={}&page_size={ERASE_PAGE_SIZE}",
            query_encode(&expr)
        );
        if let Some(cursor) = &cursor {
            let _ = write!(path, "&cursor={}", query_encode(cursor));
        }
        let page = execute_request(cli, &ApiRequest::get(path)).await?;
        if let Some(found) = page.get("workflows").and_then(Value::as_array) {
            runs.extend(found.iter().cloned());
        }
        match page.get("next_cursor").and_then(Value::as_str) {
            Some(next) if !next.is_empty() => cursor = Some(next.to_owned()),
            _ => return Ok(runs),
        }
    }
}

/// Whether a run description names a terminal state.
/// The run fields of a `GET /workflows/by-id` answer, which nests them
/// under `execution`. A list row holds them at the top.
fn execution(run: &Value) -> &Value {
    run.get("execution").unwrap_or(run)
}

fn is_terminal(run: &Value) -> bool {
    matches!(
        execution(run).get("state").and_then(Value::as_str),
        Some(
            "COMPLETED" | "FAILED" | "CANCELLED" | "TERMINATED" | "TIMED_OUT" | "CONTINUED_AS_NEW"
        )
    )
}

/// One report over the erase outcomes of every run of an agent.
///
/// Each count sums the outcome of each run and of each child it erased.
pub fn erase_report(agent_id: &str, stores: &StoresOutcome, outcomes: &[Value]) -> Value {
    let mut totals = Map::new();
    for key in ERASE_COUNTS {
        totals.insert(key.to_owned(), json!(0_u64));
    }
    let mut runs = 0_u64;
    let mut skipped = Vec::new();
    let mut failures = Vec::new();
    let mut stack: Vec<&Value> = outcomes.iter().collect();
    while let Some(outcome) = stack.pop() {
        runs += 1;
        for key in ERASE_COUNTS {
            let add = outcome.get(key).and_then(Value::as_u64).unwrap_or(0);
            let sum = totals.get(key).and_then(Value::as_u64).unwrap_or(0);
            totals.insert(key.to_owned(), json!(sum.saturating_add(add)));
        }
        for (key, into) in [
            ("skipped_children", &mut skipped),
            ("failures", &mut failures),
        ] {
            if let Some(items) = outcome.get(key).and_then(Value::as_array) {
                into.extend(items.iter().cloned());
            }
        }
        if let Some(children) = outcome.get("children").and_then(Value::as_array) {
            stack.extend(children);
        }
    }
    let mut report = Map::new();
    report.insert("agent_id".to_owned(), json!(agent_id));
    report.insert("stores_reset".to_owned(), json!(stores.reset));
    if let Some(error) = &stores.error {
        report.insert("stores_error".to_owned(), json!(error));
    }
    report.insert("runs_erased".to_owned(), json!(runs));
    report.extend(totals);
    report.insert("skipped_children".to_owned(), Value::Array(skipped));
    report.insert("failures".to_owned(), Value::Array(failures));
    Value::Object(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_list_joins_each_filter_with_and() {
        let path = list_path(
            &ListFilter {
                kind: Some("support"),
                status: Some("idle"),
                wakes_before: Some(1_730_000_000_000),
                mesh: None,
                tenant: Some("o'neil"),
            },
            50,
            Some("c1"),
        );
        let decoded = percent_decode(&path);
        assert!(
            decoded.starts_with("/workflows?state=RUNNING&filter="),
            "{decoded}"
        );
        assert!(
            decoded.contains(
                "workflow_name = 'agent' AND attrs.agent_kind = 'support' AND \
                 attrs.agent_status = 'idle' AND attrs.agent_tenant = 'o\\'neil' AND \
                 attrs.agent_next_wake_at < 1730000000000"
            ),
            "{decoded}"
        );
        assert!(decoded.ends_with("&page_size=50&cursor=c1"), "{decoded}");
    }

    #[test]
    fn a_literal_escapes_quotes_and_backslashes() {
        assert_eq!(literal(r"a'b\c"), r"'a\'b\\c'");
    }

    #[test]
    fn describe_pause_and_resume_address_the_agent_by_id() {
        let describe = agent_request(&AgentCommand::Describe {
            agent_id: "support/ada".to_owned(),
        })
        .unwrap();
        assert_eq!(
            describe.path,
            "/workflows/by-id/agent/support%2Fada/query/harvest.entity.state"
        );
        let pause = agent_request(&AgentCommand::Pause {
            agent_id: "support/ada".to_owned(),
            reason: Some("audit".to_owned()),
        })
        .unwrap();
        assert_eq!(
            pause.path,
            "/workflows/by-id/agent/support%2Fada/signal/harvest.entity.op"
        );
        assert_eq!(
            pause.body,
            Some(
                json!({"kind": "op", "op": {"type": "control", "action": "pause", "reason": "audit"}})
            )
        );
        let resume = agent_request(&AgentCommand::Resume {
            agent_id: "support/ada".to_owned(),
        })
        .unwrap();
        assert_eq!(
            resume.body,
            Some(json!({"kind": "op", "op": {"type": "control", "action": "resume"}}))
        );
    }

    #[test]
    fn the_report_sums_each_run_and_each_child() {
        let child = json!({"events_scrubbed": 1, "ledger_rows_deleted": 2});
        let outcomes = [
            json!({"events_scrubbed": 3, "stream_chunks_deleted": 4, "children": [child]}),
            json!({
                "events_scrubbed": 5,
                "skipped_children": [{"execution_id": "c"}],
                "failures": [{"execution_id": "f"}],
            }),
        ];
        let stores = StoresOutcome {
            reset: true,
            error: None,
        };
        let report = erase_report("support/ada", &stores, &outcomes);
        assert_eq!(report["runs_erased"], 3);
        assert_eq!(report["events_scrubbed"], 9);
        assert_eq!(report["stream_chunks_deleted"], 4);
        assert_eq!(report["ledger_rows_deleted"], 2);
        assert_eq!(report["stores_reset"], true);
        assert!(report.get("stores_error").is_none());
        assert_eq!(report["skipped_children"].as_array().unwrap().len(), 1);
        assert_eq!(report["failures"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn a_reset_counts_only_when_the_final_state_shows_no_new_failure() {
        let clean = json!({"failures": 2});
        assert_eq!(
            stores_outcome(2, Some(&clean)),
            StoresOutcome {
                reset: true,
                error: None
            }
        );
        let failed = json!({"failures": 3, "last_error": "the store is down"});
        let outcome = stores_outcome(2, Some(&failed));
        assert!(!outcome.reset);
        assert_eq!(outcome.error.as_deref(), Some("the store is down"));
        // A run with no final state proves nothing.
        assert!(!stores_outcome(0, None).reset);
        assert!(!stores_outcome(0, Some(&json!(null))).reset);
    }

    #[test]
    fn a_by_id_answer_nests_the_run_under_execution() {
        let nested = json!({"execution": {"state": "COMPLETED"}, "history": []});
        assert!(is_terminal(&nested));
        assert!(!is_terminal(&json!({"execution": {"state": "RUNNING"}})));
        assert!(is_terminal(&json!({"state": "FAILED"})));
    }

    #[test]
    fn a_restart_signals_with_start_on_the_queue_of_the_last_run() {
        let request = restart("support/ada", Some("agents"), control_payload("pause"));
        assert_eq!(request.path, "/workflows/agent/signal-with-start");
        let body = request.body.unwrap();
        assert_eq!(body["workflow_id"], "support/ada");
        assert_eq!(body["signal_name"], ENTITY_OP_SIGNAL);
        assert_eq!(body["queue"], "agents");
        assert_eq!(body["start_input"], json!({}));
        assert_eq!(body["signal_payload"]["op"]["action"], "pause");
    }

    fn percent_decode(raw: &str) -> String {
        percent_encoding::percent_decode_str(raw)
            .decode_utf8()
            .unwrap()
            .into_owned()
    }
}
