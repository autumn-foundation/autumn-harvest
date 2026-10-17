//! The result of one backend run, and the rules that compare two of them.
//!
//! The comparison is strict by default. Each relaxation below is a named
//! rule with its reason. A new relaxation needs a new rule here and a line in
//! `README.md`.

use autumn_harvest::event::WorkflowEvent;
use serde_json::Value;

/// The terminal result of a run, as both backends store it.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Completed(Value),
    Failed(String),
    /// A terminal state with no output and no error. The payload is the
    /// stored state name, such as `CANCELLED`.
    Ended(String),
    /// The run did not end. The payload says why.
    NotEnded(String),
}

/// One execution that a run wrote: the root, a successor or a child.
#[derive(Debug, Clone)]
pub struct Execution {
    /// The role of the execution, such as `root`, `successor 1` or `child 1`.
    pub role: String,
    /// The workflow type of the execution.
    pub workflow: String,
    pub events: Vec<WorkflowEvent>,
}

/// What one backend did with a scenario.
#[derive(Debug, Clone)]
pub struct Run {
    /// The outcome of the last run in the continue-as-new chain.
    pub outcome: Outcome,
    /// The root, each successor, then each child in order of first mention.
    pub executions: Vec<Execution>,
    /// The result of each `Update` step, in script order.
    pub update_results: Vec<Value>,
}

/// The `type` tag of `event`, as both backends store it.
pub fn event_type(event: &WorkflowEvent) -> String {
    serde_json::to_value(event)
        .ok()
        .and_then(|v| v.get("type").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default()
}

/// The children that `events` start, in order of first mention.
pub fn children(events: &[WorkflowEvent]) -> Vec<(autumn_harvest::types::ExecutionId, String)> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ChildWorkflowStarted {
                child_id,
                workflow_name,
                ..
            }
            | WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } => Some((*child_id, workflow_name.clone())),
            _ => None,
        })
        .collect()
}

/// Rule N1: drop the events that only Postgres writes.
///
/// The Postgres worker records `ActivityStarted` when it claims an activity.
/// The `SQLite` drive runs the body inline and records no claim. The core
/// matcher skips the event, so it never changes a replay.
const BACKEND_ONLY_EVENTS: &[&str] = &["ActivityStarted"];

/// Rule N2: mask the values that each backend mints for itself.
///
/// An execution id, an activity id or an instant differs between two runs of
/// one script. The comparison keeps the key and masks the value. Every key
/// that ends in `_at` is an instant too.
const VOLATILE_KEYS: &[&str] = &["activity_id", "target", "timestamp", "update_id"];

/// Rule N3: mask the value of a side effect that the runtime mints.
///
/// `new_uuid`, `random_*` and `system_now` seed from the execution id or the
/// clock. The `kind` and the `name` stay. A `Custom` value stays.
const MINTED_SIDE_EFFECTS: &[&str] = &["Uuid", "Random", "Now"];

/// Rule N4: inside one cycle, the backends may order command events of
/// different kinds differently.
///
/// Postgres writes a cycle's external signal, cancel and await requests in a
/// phase of their own, before the cycle's other command events. `SQLite`
/// keeps command order. The core matcher reads each kind with its own
/// cursor, so the order across kinds does not change a replay. Events of one
/// kind keep their order.
const COMMAND_EVENTS: &[&str] = &[
    "ActivityScheduled",
    "ChildWorkflowSpawnedDetached",
    "ChildWorkflowStarted",
    "ExternalAwaitRequested",
    "ExternalCancelRequested",
    "ExternalSignalRequested",
    "LocalActivityScheduled",
    "MarkerRecorded",
    "SideEffectRecorded",
    "TimerCancelled",
    "TimerStarted",
];

/// `event` as JSON, with rules N2 and N3 applied.
pub fn normalize(event: &WorkflowEvent) -> Value {
    let mut value = serde_json::to_value(event).unwrap_or(Value::Null);
    let minted = value["type"] == "SideEffectRecorded"
        && value["data"]["kind"]
            .as_str()
            .is_some_and(|k| MINTED_SIDE_EFFECTS.contains(&k));
    if minted {
        value["data"]["value"] = Value::String("<minted>".to_owned());
    }
    mask(&mut value);
    value
}

/// Mask volatile keys and every UUID inside a string, in place.
pub fn mask(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map.iter_mut() {
                let volatile = VOLATILE_KEYS.contains(&key.as_str()) || key.ends_with("_at");
                if volatile && !inner.is_null() {
                    *inner = Value::String("<volatile>".to_owned());
                } else {
                    mask(inner);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask),
        Value::String(text) => *text = mask_uuids(text),
        _ => {}
    }
}

/// `text` with every hyphenated UUID replaced by `<uuid>`.
fn mask_uuids(text: &str) -> String {
    const LEN: usize = 36;
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        let candidate = text.get(i..i + LEN);
        if let Some(c) = candidate.filter(|c| uuid::Uuid::try_parse(c).is_ok() && c.contains('-')) {
            out.push_str("<uuid>");
            i += c.len();
        } else {
            let ch = text[i..].chars().next().unwrap_or_default();
            out.push(ch);
            i += ch.len_utf8().max(1);
        }
    }
    out
}

/// The comparable form of a history, with every rule applied.
pub fn comparable(events: &[WorkflowEvent]) -> Vec<Value> {
    let mut out: Vec<Value> = events
        .iter()
        .filter(|e| !BACKEND_ONLY_EVENTS.contains(&event_type(e).as_str()))
        .map(normalize)
        .collect();
    // Rule N4: a stable sort by type inside each run of command events.
    let is_command = |v: &Value| v["type"].as_str().is_some_and(|t| COMMAND_EVENTS.contains(&t));
    let mut start = 0;
    while start < out.len() {
        let len = out[start..].iter().take_while(|v| is_command(v)).count();
        if len > 1 {
            out[start..start + len].sort_by(|a, b| a["type"].as_str().cmp(&b["type"].as_str()));
        }
        start += len.max(1);
    }
    out
}

/// `outcome` with every UUID inside it masked.
fn comparable_outcome(outcome: &Outcome) -> Outcome {
    match outcome {
        Outcome::Completed(v) => {
            let mut v = v.clone();
            mask(&mut v);
            Outcome::Completed(v)
        }
        Outcome::Failed(e) => Outcome::Failed(mask_uuids(e)),
        other => other.clone(),
    }
}

/// Every difference between the Postgres run and the `SQLite` run.
///
/// An empty list means the two runs conform.
pub fn differences(pg: &Run, sqlite: &Run) -> Vec<String> {
    let mut out = Vec::new();
    if comparable_outcome(&pg.outcome) != comparable_outcome(&sqlite.outcome) {
        out.push(format!(
            "outcome: postgres {:?}, sqlite {:?}",
            pg.outcome, sqlite.outcome
        ));
    }
    if pg.update_results != sqlite.update_results {
        out.push(format!(
            "update results: postgres {:?}, sqlite {:?}",
            pg.update_results, sqlite.update_results
        ));
    }
    let roles = |run: &Run| -> Vec<String> {
        run.executions
            .iter()
            .map(|e| format!("{} ({})", e.role, e.workflow))
            .collect()
    };
    if roles(pg) != roles(sqlite) {
        out.push(format!(
            "executions: postgres {:?}, sqlite {:?}",
            roles(pg),
            roles(sqlite)
        ));
    }
    for (p, s) in pg.executions.iter().zip(&sqlite.executions) {
        let (pe, se) = (comparable(&p.events), comparable(&s.events));
        if pe != se {
            out.push(format!(
                "{} history differs:\n{}",
                p.role,
                history_diff(&pe, &se)
            ));
        }
    }
    out
}

/// A side-by-side listing of two comparable histories.
fn history_diff(pg: &[Value], sqlite: &[Value]) -> String {
    let mut lines = Vec::new();
    for i in 0..pg.len().max(sqlite.len()) {
        let p = pg.get(i).map_or_else(|| "-".to_owned(), Value::to_string);
        let s = sqlite.get(i).map_or_else(|| "-".to_owned(), Value::to_string);
        let mark = if p == s { "  " } else { "!=" };
        lines.push(format!(
            "  {mark} [{i}]\n      postgres: {p}\n      sqlite:   {s}"
        ));
    }
    lines.join("\n")
}
