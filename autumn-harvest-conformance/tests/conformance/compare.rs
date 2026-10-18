//! The result of one backend run, and the rules that compare two of them.
//!
//! The comparison is strict by default. Each relaxation below is a named
//! rule with its reason. A new relaxation needs a new rule here and a line in
//! `README.md`.

use std::collections::HashMap;
use std::fmt::Write as _;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use serde_json::Value;

/// The terminal result of a run, as both backends store it.
#[derive(Debug, Clone, PartialEq, Eq)]
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
pub fn children(events: &[WorkflowEvent]) -> Vec<(ExecutionId, String)> {
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

/// Rule N2: mask the instants in the event schema.
///
/// The rule masks `timestamp` and each key that ends in `_at`, at the top of
/// an event's data. A key inside a user payload stays.
const INSTANT_KEYS: &[&str] = &["timestamp"];

/// Rule N3: mask the value of a side effect that the runtime mints.
///
/// `new_uuid`, `random_*` and `system_now` seed from the execution id or the
/// clock. The `kind` and the `name` stay. A `Custom` value stays.
const MINTED_SIDE_EFFECTS: &[&str] = &["Uuid", "Random", "Now"];

/// Rule N4: inside one run of command events, Postgres writes the external
/// requests first.
///
/// Postgres writes a cycle's external signal, cancel and await requests in a
/// phase of their own, before the cycle's other command events. `SQLite`
/// keeps command order. The core matcher reads each kind with its own
/// cursor, so this move does not change a replay. The rule moves these three
/// types to the front of each run. Every other event keeps its place.
const EXTERNAL_REQUESTS: &[&str] = &[
    "ExternalAwaitRequested",
    "ExternalCancelRequested",
    "ExternalSignalRequested",
];

/// Rule N5: inside one run of activity results, the order follows the
/// activity, not the clock.
///
/// Postgres runs parallel activities on several worker slots, so their
/// results land in any order. `SQLite` runs them in scheduling order. The
/// matcher finds each result by its activity id, so the order does not change
/// a replay. The rule sorts each run of `ActivityCompleted` events by
/// activity alias.
const PARALLEL_RESULTS: &str = "ActivityCompleted";

/// Rule N6: Postgres writes a cycle's race-loser cancellations after the
/// cycle's other command events.
///
/// `SQLite` keeps command order. The matcher reads each kind with its own
/// cursor, so the move does not change a replay. The rule moves each
/// cancellation to the end of its run. The core names a race loser with this
/// error text, on both backends.
const RACE_LOSER_ERROR: &str = "lost race to a sibling branch";

/// The event types that a decision cycle writes from its own commands.
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

/// Rule N2 also gives each UUID an alias by first appearance in one run.
///
/// Each backend mints its own ids. An alias keeps the relations: a signal to
/// the second child still names the second child, as `<id2>`.
#[derive(Default)]
pub struct Aliases {
    seen: HashMap<String, usize>,
}

impl Aliases {
    /// `text` with each hyphenated UUID replaced by its alias.
    fn rewrite(&mut self, text: &str) -> String {
        const LEN: usize = 36;
        let mut out = String::with_capacity(text.len());
        let mut i = 0;
        while i < text.len() {
            let candidate = text
                .get(i..i + LEN)
                .filter(|c| uuid::Uuid::try_parse(c).is_ok());
            if let Some(id) = candidate {
                let next = self.seen.len() + 1;
                let n = *self.seen.entry(id.to_ascii_lowercase()).or_insert(next);
                let _ = write!(out, "<id{n}>");
                i += LEN;
            } else {
                let ch = text[i..].chars().next().unwrap_or_default();
                out.push(ch);
                i += ch.len_utf8();
            }
        }
        out
    }

    /// Alias every UUID inside `value`, in place.
    fn alias(&mut self, value: &mut Value) {
        match value {
            Value::Object(map) => map.values_mut().for_each(|v| self.alias(v)),
            Value::Array(items) => items.iter_mut().for_each(|v| self.alias(v)),
            Value::String(text) => *text = self.rewrite(text),
            _ => {}
        }
    }

    /// `outcome` with every UUID inside it aliased.
    pub fn outcome(&mut self, outcome: &Outcome) -> Outcome {
        match outcome {
            Outcome::Completed(v) => {
                let mut v = v.clone();
                self.alias(&mut v);
                Outcome::Completed(v)
            }
            Outcome::Failed(e) => Outcome::Failed(self.rewrite(e)),
            other => other.clone(),
        }
    }
}

fn is_instant_key(key: &str) -> bool {
    INSTANT_KEYS.contains(&key) || key.ends_with("_at")
}

/// Mask the instant keys at the top of an event's data. Every instant in
/// the event schema is there. A key inside a user payload stays.
fn mask_instants(data: &mut Value) {
    let Value::Object(map) = data else { return };
    for (key, inner) in map.iter_mut() {
        if is_instant_key(key) && !inner.is_null() {
            *inner = Value::String("<instant>".to_owned());
        }
    }
}

/// `event` as JSON, with rule N3 and the instant mask of rule N2 applied.
fn normalize(event: &WorkflowEvent) -> Value {
    let mut value = serde_json::to_value(event).unwrap_or(Value::Null);
    let minted = value["type"] == "SideEffectRecorded"
        && value["data"]["kind"]
            .as_str()
            .is_some_and(|k| MINTED_SIDE_EFFECTS.contains(&k));
    if minted {
        value["data"]["value"] = Value::String("<minted>".to_owned());
    }
    if let Some(data) = value.get_mut("data") {
        mask_instants(data);
    }
    value
}

fn type_of(value: &Value) -> &str {
    value["type"].as_str().unwrap_or_default()
}

/// Whether `value` is the cancellation of a race loser (rule N6).
fn is_race_loser(value: &Value) -> bool {
    matches!(type_of(value), "ActivityFailed" | "ChildWorkflowFailed")
        && value["data"]["error"].as_str() == Some(RACE_LOSER_ERROR)
}

/// Sort each maximal run of events that `member` selects with `key`. The sort
/// is stable, so events with one key keep their order.
fn sort_runs<K: Ord>(
    events: &mut [Value],
    member: impl Fn(&Value) -> bool,
    key: impl Fn(&Value) -> K,
) {
    let mut start = 0;
    while start < events.len() {
        let len = events[start..].iter().take_while(|v| member(v)).count();
        if len > 1 {
            events[start..start + len].sort_by_key(|v| key(v));
        }
        start += len.max(1);
    }
}

/// Apply rules N4 and N6.
fn reorder_cycle_events(events: &mut [Value]) {
    let in_cycle = |v: &Value| COMMAND_EVENTS.contains(&type_of(v)) || is_race_loser(v);
    // N4 and N6: requests to the front, loser cancellations to the end.
    sort_runs(events, in_cycle, |v| {
        if EXTERNAL_REQUESTS.contains(&type_of(v)) {
            0
        } else if is_race_loser(v) {
            2
        } else {
            1
        }
    });
}

/// Apply rule N5. It sorts by alias, so it runs after the aliases.
fn reorder_parallel_results(events: &mut [Value]) {
    sort_runs(
        events,
        |v| type_of(v) == PARALLEL_RESULTS,
        |v| {
            v["data"]["activity_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        },
    );
}

/// The comparable form of a history, with every rule applied.
///
/// The aliases come after the reorder, so both backends alias in one order.
pub fn comparable(events: &[WorkflowEvent], aliases: &mut Aliases) -> Vec<Value> {
    let mut out: Vec<Value> = events
        .iter()
        .filter(|e| !BACKEND_ONLY_EVENTS.contains(&event_type(e).as_str()))
        .map(normalize)
        .collect();
    reorder_cycle_events(&mut out);
    for v in &mut out {
        aliases.alias(v);
    }
    reorder_parallel_results(&mut out);
    out
}

/// Every difference between the Postgres run and the `SQLite` run.
///
/// An empty list means the two runs conform.
pub fn differences(pg: &Run, sqlite: &Run) -> Vec<String> {
    let mut out = Vec::new();
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
    // Each backend aliases its own ids, in role order, then the outcome.
    let (mut pa, mut sa) = (Aliases::default(), Aliases::default());
    for (p, s) in pg.executions.iter().zip(&sqlite.executions) {
        let (pe, se) = (
            comparable(&p.events, &mut pa),
            comparable(&s.events, &mut sa),
        );
        if pe != se {
            out.push(format!(
                "{} history differs:\n{}",
                p.role,
                history_diff(&pe, &se)
            ));
        }
    }
    if pa.outcome(&pg.outcome) != sa.outcome(&sqlite.outcome) {
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
    out
}

/// A side-by-side listing of two comparable histories.
fn history_diff(pg: &[Value], sqlite: &[Value]) -> String {
    let mut lines = Vec::new();
    for i in 0..pg.len().max(sqlite.len()) {
        let p = pg.get(i).map_or_else(|| "-".to_owned(), Value::to_string);
        let s = sqlite
            .get(i)
            .map_or_else(|| "-".to_owned(), Value::to_string);
        let mark = if p == s { "  " } else { "!=" };
        lines.push(format!(
            "  {mark} [{i}]\n      postgres: {p}\n      sqlite:   {s}"
        ));
    }
    lines.join("\n")
}
