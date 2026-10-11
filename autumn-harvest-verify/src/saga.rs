//! Saga compensation coverage over a structure manifest (issue #2010).
//!
//! The check reads the flow graphs of a manifest and no MIR. It asks one
//! question of each workflow: can a completed forward step of a `Saga`
//! reach an error exit with no unwind on the way?
//!
//! For each body with a `saga-new` node, a forward fixpoint carries one flag
//! per node. The flag is set when a forward step may have completed and not
//! been unwound:
//!
//! - The `ok` edges of a tracked `saga-step` set the flag. Its `err` edges
//!   clear it, because a failed step unwinds every earlier step.
//! - Every edge of an untracked `saga-step` sets the flag.
//! - Every edge of a `saga-compensate` clears it.
//!
//! An exit with the outcome `err` or `unknown` and a set flag is a gap.
//!
//! Each of these facts makes the verdict `unknown`:
//!
//! - A saga escapes the body that owns it.
//! - The result of a step is untracked.
//! - One body builds two sagas.
//! - The workflow has a boundary that runs code. A body outside the
//!   analysis can hold a saga and a gap, even beside a visible saga. An
//!   external constant runs no code, so it does not count.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::structure::{
    BodyNode, EdgeLabel, ExitOutcome, FLOW_FORMAT, FlowEvent, FlowGraph, STRUCTURE_FORMAT,
    StructureManifest, WorkflowStructure,
};

/// The coverage verdict of one workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SagaVerdict {
    /// No body uses a saga, and no boundary can hide one.
    NoSaga,
    /// Each error exit after a completed forward step unwinds first.
    Covered,
    /// An error exit can follow a completed forward step with no unwind.
    Gap,
    /// The graph cannot show the answer. The reasons are in
    /// [`SagaReport::unknown`].
    Unknown,
}

impl SagaVerdict {
    /// The verdict as the text report prints it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::NoSaga => "no-saga",
            Self::Covered => "covered",
            Self::Gap => "gap",
            Self::Unknown => "unknown",
        }
    }
}

/// The coverage result of one workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaReport {
    /// `crate::module::fn`.
    pub workflow: String,
    /// The registered workflow name.
    pub name: String,
    pub verdict: SagaVerdict,
    /// Each exit that a completed forward step reaches with no unwind. With
    /// an `unknown` verdict, these gaps are possible, not proven.
    pub gaps: Vec<Gap>,
    /// Each reason for `unknown`, as `kind: detail`.
    pub unknown: Vec<String>,
    /// Facts that do not change the verdict, as `kind: detail`.
    pub notes: Vec<String>,
}

/// An exit that a completed forward step can reach with no unwind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gap {
    /// The body that holds the exit.
    pub body: String,
    /// The MIR block of the exit.
    pub at: String,
    pub outcome: ExitOutcome,
}

/// Why a manifest cannot be checked.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CheckError(String);

/// Check each workflow of `manifest`.
///
/// # Errors
/// When the manifest carries no flow graphs, such as a manifest from a
/// build before issue #2010. Without them every workflow would read as
/// `no-saga`.
pub fn check(manifest: &StructureManifest) -> Result<Vec<SagaReport>, CheckError> {
    check_header(Some(&manifest.format), manifest.flow.as_deref())?;
    for workflow in &manifest.workflows {
        // A workflow with no body for its root checked no code.
        let roots = workflow
            .bodies
            .iter()
            .filter(|b| b.id == workflow.root)
            .count();
        if roots != 1 {
            return Err(CheckError(format!(
                "{}: {roots} bodies match the root `{}`, not 1",
                workflow.workflow, workflow.root
            )));
        }
        // A reference names a body by id, so two bodies of one id make it
        // ambiguous. The check would also read both.
        let mut ids: BTreeSet<&str> = BTreeSet::new();
        if let Some(twice) = workflow.bodies.iter().find(|b| !ids.insert(b.id.as_str())) {
            return Err(CheckError(format!(
                "{}: two bodies have the id `{}`",
                workflow.workflow, twice.id
            )));
        }
        // The manifest names a flow format, so each body must carry a graph.
        // A body without one checked no code.
        for body in &workflow.bodies {
            let Some(graph) = &body.flow else {
                return Err(CheckError(format!(
                    "{}: the body has no flow graph",
                    body.id
                )));
            };
            validate(graph, body.steps.len())
                .map_err(|e| CheckError(format!("{}: {e}", body.id)))?;
        }
        if let Some(problem) = dangling_reference(workflow).or_else(|| unreached_body(workflow)) {
            return Err(CheckError(format!("{}: {problem}", workflow.workflow)));
        }
    }
    Ok(manifest.workflows.iter().map(check_workflow).collect())
}

/// The first reference that points at nothing. It is a body that a call, a
/// saga step or a handler names, or a handler index past the list. The call
/// list of each body counts too. That code is never checked, so it could
/// hide a saga or a gap.
fn dangling_reference(workflow: &WorkflowStructure) -> Option<String> {
    let ids: BTreeSet<&str> = workflow.bodies.iter().map(|b| b.id.as_str()).collect();
    let mut named: Vec<&String> = Vec::new();
    let mut registered: BTreeSet<usize> = BTreeSet::new();
    let graphs = workflow.bodies.iter().filter_map(|b| b.flow.as_ref());
    for node in graphs.flat_map(|graph| &graph.nodes) {
        match &node.event {
            FlowEvent::Call { callees } => named.extend(callees),
            FlowEvent::SagaStep {
                forward,
                compensate,
                ..
            } => named.extend(forward.iter().chain(compensate)),
            FlowEvent::Handler { handler } if *handler >= workflow.handlers.len() => {
                let count = workflow.handlers.len();
                return Some(format!(
                    "a node names handler {handler}, but the workflow has {count} handlers"
                ));
            }
            FlowEvent::Handler { handler } => {
                registered.insert(*handler);
            }
            _ => {}
        }
    }
    // A handler entry runs as an entry. Only a registration node makes it
    // one, or a helper listed there would read as covered.
    if let Some(orphan) = (0..workflow.handlers.len()).find(|i| !registered.contains(i)) {
        return Some(format!(
            "the workflow lists handler {orphan}, but no node registers it"
        ));
    }
    let handlers = workflow
        .handlers
        .iter()
        .flat_map(|h| h.bodies.iter().chain(&h.returns));
    let calls = workflow.bodies.iter().flat_map(|b| &b.calls);
    named
        .into_iter()
        .chain(handlers)
        .chain(calls.map(|call| &call.callee))
        .map(String::as_str)
        .find(|id| !ids.contains(id))
        .map(|missing| format!("a node or handler names `{missing}`, which has no body"))
}

/// The first body that no reference reaches from the root. The check reads
/// every body, so a stray gap would mark a workflow that has no saga.
fn unreached_body(workflow: &WorkflowStructure) -> Option<String> {
    let bodies: BTreeMap<&str, &BodyNode> =
        workflow.bodies.iter().map(|b| (b.id.as_str(), b)).collect();
    let mut reached: BTreeSet<&str> = BTreeSet::from([workflow.root.as_str()]);
    let mut queue: Vec<&str> = vec![workflow.root.as_str()];
    while let Some(id) = queue.pop() {
        let Some(body) = bodies.get(id) else {
            continue;
        };
        let mut next: Vec<&String> = body.calls.iter().map(|c| &c.callee).collect();
        for node in body.flow.iter().flat_map(|graph| &graph.nodes) {
            match &node.event {
                FlowEvent::Call { callees } => next.extend(callees),
                FlowEvent::SagaStep {
                    forward,
                    compensate,
                    ..
                } => next.extend(forward.iter().chain(compensate)),
                FlowEvent::Handler { handler } => {
                    let site = workflow.handlers.get(*handler);
                    next.extend(
                        site.into_iter()
                            .flat_map(|h| h.bodies.iter().chain(&h.returns)),
                    );
                }
                _ => {}
            }
        }
        for callee in next {
            if reached.insert(callee.as_str()) {
                queue.push(callee.as_str());
            }
        }
    }
    bodies
        .keys()
        .find(|id| !reached.contains(*id))
        .map(|stray| format!("no reference reaches the body `{stray}` from the root"))
}

/// The bodies that start an execution: the workflow root, each handler
/// body, and each coroutine a handler body returns. Their error exit ends
/// that execution, so a pending step there is a gap.
fn entry_bodies(workflow: &WorkflowStructure) -> BTreeSet<&str> {
    let mut entries: BTreeSet<&str> = BTreeSet::from([workflow.root.as_str()]);
    for handler in &workflow.handlers {
        entries.extend(handler.bodies.iter().map(String::as_str));
        entries.extend(handler.returns.iter().map(String::as_str));
    }
    entries
}

/// The bodies that an ordinary call runs as a helper.
///
/// Two edges start no helper run. A handler body builds the coroutine it
/// returns, and the engine runs that coroutine as an entry. A registration
/// passes a handler body as an argument, which shows only in `calls`.
fn helper_calls(workflow: &WorkflowStructure) -> BTreeSet<&str> {
    let mut helpers = BTreeSet::new();
    for body in &workflow.bodies {
        let returned: BTreeSet<&str> = workflow
            .handlers
            .iter()
            .filter(|h| h.bodies.contains(&body.id))
            .flat_map(|h| &h.returns)
            .map(String::as_str)
            .collect();
        let mut registered: BTreeSet<&str> = BTreeSet::new();
        let mut callees: Vec<&String> = Vec::new();
        for node in body.flow.iter().flat_map(|graph| &graph.nodes) {
            match &node.event {
                FlowEvent::Call { callees: named } => callees.extend(named),
                FlowEvent::SagaStep {
                    forward,
                    compensate,
                    ..
                } => callees.extend(forward.iter().chain(compensate)),
                FlowEvent::Handler { handler } => {
                    let site = workflow.handlers.get(*handler);
                    registered.extend(site.into_iter().flat_map(|h| &h.bodies).map(String::as_str));
                }
                _ => {}
            }
        }
        let calls = body.calls.iter().filter(|c| !c.resume);
        let calls = calls
            .map(|c| c.callee.as_str())
            .filter(|c| !registered.contains(c));
        let nodes = callees.into_iter().map(String::as_str);
        helpers.extend(nodes.chain(calls).filter(|c| !returned.contains(c)));
    }
    helpers
}

/// Refuse a graph the fixpoint would read wrong. A dropped edge or a
/// missing entry would hide an exit, and so give a false `covered`.
fn validate(graph: &FlowGraph, steps: usize) -> Result<(), String> {
    let entries = count(graph, |e| matches!(e, FlowEvent::Entry));
    if entries != 1 {
        return Err(format!("the flow graph has {entries} entry nodes, not 1"));
    }
    let size = graph.nodes.len();
    for edge in &graph.edges {
        for end in [edge.from, edge.to] {
            if end >= size {
                return Err(format!(
                    "an edge names node {end}, but the graph has {size} nodes"
                ));
            }
        }
    }
    // A step node indexes the body `steps`. A missing step reads as a body
    // with no command, so a `noop-compensation` note would be wrong.
    for node in &graph.nodes {
        if let FlowEvent::Step { step } = node.event
            && step >= steps
        {
            return Err(format!(
                "a node names step {step}, but the body has {steps} steps"
            ));
        }
    }
    // An exit returns, so nothing runs after it.
    if let Some(edge) = graph.edges.iter().find(|e| {
        graph
            .nodes
            .get(e.from)
            .is_some_and(|n| matches!(n.event, FlowEvent::Exit { .. }))
    }) {
        return Err(format!("the exit at node {} has an out-edge", edge.from));
    }
    // The fixpoint starts at the entry. A node it cannot reach is never
    // checked, so a saga or a gap there would be silent.
    let entry = graph
        .nodes
        .iter()
        .position(|n| matches!(n.event, FlowEvent::Entry))
        .unwrap_or_default();
    let mut seen = vec![false; size];
    let mut work = vec![entry];
    while let Some(id) = work.pop() {
        if seen.get(id).copied().unwrap_or(true) {
            continue;
        }
        if let Some(slot) = seen.get_mut(id) {
            *slot = true;
        }
        work.extend(graph.edges.iter().filter(|e| e.from == id).map(|e| e.to));
    }
    if let Some(id) = seen.iter().position(|reached| !reached) {
        return Err(format!("node {id} cannot be reached from the entry"));
    }
    // A tracked step has an `ok` and an `err` edge, and only its edges carry
    // a label. The emitter marks a step untracked when an arm reaches no
    // node, so a missing arm means the graph was changed after emission.
    for (id, node) in graph.nodes.iter().enumerate() {
        let tracked = matches!(node.event, FlowEvent::SagaStep { tracked: true, .. });
        let labels: Vec<Option<EdgeLabel>> = graph
            .edges
            .iter()
            .filter(|e| e.from == id)
            .map(|e| e.label)
            .collect();
        if !tracked && labels.iter().any(Option::is_some) {
            return Err(format!(
                "node {id} has a labeled edge, but it is not a tracked step"
            ));
        }
        for (arm, label) in [("`ok`", EdgeLabel::Ok), ("`err`", EdgeLabel::Err)] {
            if tracked && !labels.contains(&Some(label)) {
                return Err(format!("the tracked step at node {id} has no {arm} edge"));
            }
        }
    }
    Ok(())
}

/// Parse a manifest from JSON text.
///
/// The format fields are read first. So a manifest from another version gets
/// a clear error, not a message about a field it does not know.
///
/// # Errors
/// When the text is not JSON, names another format, or does not parse.
pub fn parse_manifest(text: &str) -> Result<StructureManifest, CheckError> {
    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| CheckError(format!("cannot parse the manifest as JSON: {e}")))?;
    check_header(
        value.get("format").and_then(serde_json::Value::as_str),
        value.get("flow").and_then(serde_json::Value::as_str),
    )?;
    serde_json::from_value(value).map_err(|e| CheckError(format!("cannot parse the manifest: {e}")))
}

fn check_header(format: Option<&str>, flow: Option<&str>) -> Result<(), CheckError> {
    if format != Some(STRUCTURE_FORMAT) {
        return Err(CheckError(format!(
            "the manifest format is {format:?}, not `{STRUCTURE_FORMAT}`"
        )));
    }
    if flow != Some(FLOW_FORMAT) {
        return Err(CheckError(format!(
            "the manifest has no `{FLOW_FORMAT}` flow graphs (flow: {flow:?}); \
             emit it again with this version of harvest-verify"
        )));
    }
    Ok(())
}

/// Check one workflow.
#[must_use]
pub fn check_workflow(workflow: &WorkflowStructure) -> SagaReport {
    let mut gaps = Vec::new();
    let mut unknown: BTreeSet<String> = BTreeSet::new();
    let mut notes: BTreeSet<String> = BTreeSet::new();
    let mut uses_saga = false;
    let entries = entry_bodies(workflow);
    let helpers = helper_calls(workflow);

    for body in &workflow.bodies {
        let Some(graph) = &body.flow else {
            unknown.insert(format!("no-flow-graph: {}", body.id));
            continue;
        };
        let owners = count(graph, |e| matches!(e, FlowEvent::SagaNew));
        // An escape is a saga use too: a saga value reached this body.
        let operations = count(graph, |e| {
            matches!(
                e,
                FlowEvent::SagaStep { .. }
                    | FlowEvent::SagaCompensate { .. }
                    | FlowEvent::SagaEscape { .. }
            )
        });
        if owners == 0 && operations == 0 {
            continue;
        }
        uses_saga = true;
        if owners == 0 {
            unknown.insert(format!(
                "saga-escapes: {} uses a saga it does not own",
                body.id
            ));
            continue;
        }
        if owners > 1 {
            unknown.insert(format!("multiple-sagas: {} builds {owners} sagas", body.id));
        }
        for node in &graph.nodes {
            match &node.event {
                FlowEvent::SagaEscape { to } => {
                    unknown.insert(format!("saga-escapes: {} at {} to {to}", body.id, node.at));
                }
                FlowEvent::SagaStep { tracked: false, .. } => {
                    unknown.insert(format!("saga-result-untracked: {} at {}", body.id, node.at));
                }
                FlowEvent::SagaStep { compensate, .. } => {
                    for start in compensate {
                        if !emits_a_step(workflow, start) {
                            notes.insert(format!("noop-compensation: {start}"));
                        }
                    }
                }
                _ => {}
            }
        }
        // A body can run as an entry and as a helper. Each way is checked.
        let id = body.id.as_str();
        let entry = entries.contains(id);
        let ways = [entry, !entry || helpers.contains(id)];
        for root in [true, false]
            .into_iter()
            .zip(ways)
            .filter(|w| w.1)
            .map(|w| w.0)
        {
            let (found, reasons) = body_gaps(body, graph, root);
            gaps.extend(found);
            unknown.extend(reasons);
        }
    }

    // A body outside the analysis can hold a saga and a gap, whether or not
    // a saga is in sight. An external constant runs no code, so it cannot.
    for boundary in &workflow.boundaries {
        if !boundary.starts_with("external-const:") {
            unknown.insert(format!("boundary: {boundary}"));
        }
    }
    let verdict = if !unknown.is_empty() {
        SagaVerdict::Unknown
    } else if !uses_saga {
        SagaVerdict::NoSaga
    } else if gaps.is_empty() {
        SagaVerdict::Covered
    } else {
        SagaVerdict::Gap
    };
    SagaReport {
        workflow: workflow.workflow.clone(),
        name: workflow.name.clone(),
        verdict,
        gaps,
        unknown: unknown.into_iter().collect(),
        notes: notes.into_iter().collect(),
    }
}

/// One line per workflow, its details indented, and a count line.
#[must_use]
pub fn render_text(reports: &[SagaReport]) -> String {
    let mut out = String::new();
    let mut counts = [0_usize; 4];
    for report in reports {
        let slot = match report.verdict {
            SagaVerdict::Covered => 0,
            SagaVerdict::Gap => 1,
            SagaVerdict::Unknown => 2,
            SagaVerdict::NoSaga => 3,
        };
        if let Some(count) = counts.get_mut(slot) {
            *count = count.saturating_add(1);
        }
        let _ = writeln!(out, "{}  {}", report.verdict.name(), report.workflow);
        // Under `unknown`, a gap is possible, not proven.
        let label = if report.verdict == SagaVerdict::Unknown {
            "possible gap"
        } else {
            "gap"
        };
        for gap in &report.gaps {
            let _ = writeln!(
                out,
                "  {label}: {} at {} returns {}",
                gap.body,
                gap.at,
                gap.outcome.name()
            );
        }
        for reason in &report.unknown {
            let _ = writeln!(out, "  unknown: {reason}");
        }
        for note in &report.notes {
            let _ = writeln!(out, "  note: {note}");
        }
    }
    let [covered, gap, unknown, none] = counts;
    let _ = write!(
        out,
        "\nchecked {}: covered {covered}, gap {gap}, unknown {unknown}, no-saga {none}",
        reports.len()
    );
    out
}

fn count(graph: &FlowGraph, pick: impl Fn(&FlowEvent) -> bool) -> usize {
    graph.nodes.iter().filter(|n| pick(&n.event)).count()
}

/// The exits of `body` that a set flag reaches, and the reasons for
/// `unknown` that the fixpoint finds.
///
/// Two states hide a pending step from the check:
///
/// - `saga-recreated`: a new saga starts while a step of the old one is
///   pending. The old saga can no longer unwind it.
/// - `saga-dropped-pending`: a body other than the workflow root exits while
///   a step is pending, even with `Ok`. Its caller cannot unwind that step.
fn body_gaps(body: &BodyNode, graph: &FlowGraph, root: bool) -> (Vec<Gap>, BTreeSet<String>) {
    let size = graph.nodes.len();
    // `reached[n]` holds each flag value seen on entry to node `n`.
    let mut reached: Vec<[bool; 2]> = vec![[false; 2]; size];
    let mut queue: Vec<(usize, bool)> = Vec::new();
    for (id, node) in graph.nodes.iter().enumerate() {
        if matches!(node.event, FlowEvent::Entry) {
            queue.push((id, false));
        }
    }
    while let Some((id, flag)) = queue.pop() {
        let Some(seen) = reached.get_mut(id) else {
            continue;
        };
        let slot = usize::from(flag);
        if seen.get(slot).copied().unwrap_or(true) {
            continue;
        }
        if let Some(bit) = seen.get_mut(slot) {
            *bit = true;
        }
        let event = graph.nodes.get(id).map(|n| &n.event);
        for edge in graph.edges.iter().filter(|e| e.from == id) {
            let out = match (event, edge.label) {
                (Some(FlowEvent::SagaStep { tracked: true, .. }), Some(EdgeLabel::Err))
                | (Some(FlowEvent::SagaCompensate { tracked: true }), _) => false,
                (Some(FlowEvent::SagaStep { .. }), _) => true,
                _ => flag,
            };
            queue.push((edge.to, out));
        }
    }
    let mut gaps = Vec::new();
    let mut reasons = BTreeSet::new();
    for (node, seen) in graph.nodes.iter().zip(&reached) {
        if !seen.get(1).copied().unwrap_or(false) {
            continue;
        }
        match node.event {
            FlowEvent::SagaNew => {
                reasons.insert(format!("saga-recreated: {} at {}", body.id, node.at));
            }
            FlowEvent::Exit { outcome } if !root => {
                reasons.insert(format!(
                    "saga-dropped-pending: {} at {} returns {}",
                    body.id,
                    node.at,
                    outcome.name()
                ));
            }
            FlowEvent::Exit { outcome } if outcome != ExitOutcome::Ok => gaps.push(Gap {
                body: body.id.clone(),
                at: node.at.clone(),
                outcome,
            }),
            _ => {}
        }
    }
    (gaps, reasons)
}

/// `start` or a body it calls emits a step.
fn emits_a_step(workflow: &WorkflowStructure, start: &str) -> bool {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut queue: Vec<&str> = vec![start];
    while let Some(id) = queue.pop() {
        if !seen.insert(id) {
            continue;
        }
        let Some(body) = workflow.bodies.iter().find(|b| b.id == id) else {
            // A body outside the graph may emit a step. Count it as one, so
            // no false note is raised.
            return true;
        };
        if !body.steps.is_empty() {
            return true;
        }
        queue.extend(
            body.calls
                .iter()
                .filter(|c| !c.resume)
                .map(|c| c.callee.as_str()),
        );
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structure::{CallSite, FlowEdge, FlowNode, HandlerSite};

    fn node(event: FlowEvent) -> FlowNode {
        FlowNode {
            at: String::new(),
            event,
        }
    }

    fn edge(from: usize, to: usize, label: Option<EdgeLabel>) -> FlowEdge {
        FlowEdge { from, to, label }
    }

    fn workflow(graph: FlowGraph) -> WorkflowStructure {
        WorkflowStructure {
            workflow: "w::wf".to_string(),
            name: "wf".to_string(),
            root: "w::wf::{closure#0}".to_string(),
            bodies: vec![BodyNode {
                id: "w::wf::{closure#0}".to_string(),
                flow: Some(graph),
                ..BodyNode::default()
            }],
            ..WorkflowStructure::default()
        }
    }

    fn step() -> FlowEvent {
        FlowEvent::SagaStep {
            forward: Vec::new(),
            compensate: Vec::new(),
            tracked: true,
        }
    }

    fn manifest(graph: FlowGraph) -> StructureManifest {
        StructureManifest {
            format: STRUCTURE_FORMAT.to_string(),
            flow: Some(FLOW_FORMAT.to_string()),
            workflows: vec![workflow(graph)],
            ..StructureManifest::default()
        }
    }

    #[test]
    fn a_malformed_graph_is_refused() {
        let out_of_range = FlowGraph {
            nodes: vec![node(FlowEvent::Entry), node(FlowEvent::SagaNew)],
            edges: vec![edge(0, 1, None), edge(1, 7, None)],
        };
        let err = check(&manifest(out_of_range)).expect_err("edge to node 7");
        assert!(err.to_string().contains("node 7"), "{err}");

        let no_ok_arm = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Err)),
            ],
        };
        let err = check(&manifest(no_ok_arm)).expect_err("a tracked step needs both arms");
        assert!(err.to_string().contains("`ok`"), "{err}");

        let stray_label = FlowGraph {
            nodes: vec![node(FlowEvent::Entry), node(FlowEvent::SagaNew)],
            edges: vec![edge(0, 1, Some(EdgeLabel::Ok))],
        };
        let err = check(&manifest(stray_label)).expect_err("a label off a tracked step");
        assert!(err.to_string().contains("label"), "{err}");

        let mut rootless = manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        });
        if let Some(w) = rootless.workflows.first_mut() {
            w.bodies.clear();
        }
        let err = check(&rootless).expect_err("no body matches the root");
        assert!(err.to_string().contains("root"), "{err}");

        let no_entry = FlowGraph {
            nodes: vec![node(FlowEvent::SagaNew)],
            edges: Vec::new(),
        };
        let err = check(&manifest(no_entry)).expect_err("no entry node");
        assert!(err.to_string().contains("entry"), "{err}");

        // A disconnected saga and gap would read as `no-saga`.
        let disconnected = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
                node(FlowEvent::SagaNew),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![edge(0, 1, None), edge(2, 3, None)],
        };
        let err = check(&manifest(disconnected)).expect_err("unreachable nodes");
        assert!(err.to_string().contains("reach"), "{err}");

        // A step node names an entry of the body `steps` that is not there.
        let err = check(&manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry), node(FlowEvent::Step { step: 0 })],
            edges: vec![edge(0, 1, None)],
        }))
        .expect_err("a step index past the list");
        assert!(err.to_string().contains("step 0"), "{err}");

        // Nodes after a return never run, so a gap there would be hidden.
        let past_exit = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
                node(FlowEvent::SagaNew),
            ],
            edges: vec![edge(0, 1, None), edge(1, 2, None)],
        };
        let err = check(&manifest(past_exit)).expect_err("an edge out of an exit");
        assert!(err.to_string().contains("exit"), "{err}");
    }

    #[test]
    fn a_reference_to_a_missing_body_is_refused() {
        // The missing body could hold the only saga, or a gap.
        let call = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Call {
                    callees: vec!["w::helper".to_string()],
                }),
            ],
            edges: vec![edge(0, 1, None)],
        };
        let err = check(&manifest(call)).expect_err("a call to a missing body");
        assert!(err.to_string().contains("w::helper"), "{err}");

        let mut handler = manifest(FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Handler { handler: 0 }),
            ],
            edges: vec![edge(0, 1, None)],
        });
        if let Some(w) = handler.workflows.first_mut() {
            w.handlers.push(HandlerSite {
                kind: "signal".to_string(),
                method: "register_signal_handler".to_string(),
                name: None,
                bodies: vec!["w::on_signal".to_string()],
                returns: Vec::new(),
            });
        }
        let err = check(&handler).expect_err("a handler with a missing body");
        assert!(err.to_string().contains("w::on_signal"), "{err}");

        // A body-level call names a body the workflow does not hold.
        let mut body_call = manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        });
        if let Some(body) = body_call
            .workflows
            .first_mut()
            .and_then(|w| w.bodies.first_mut())
        {
            body.calls.push(CallSite {
                callee: "w::hidden".to_string(),
                in_loop: false,
                resume: false,
            });
        }
        let err = check(&body_call).expect_err("a body call to a missing body");
        assert!(err.to_string().contains("w::hidden"), "{err}");

        // A body with no graph checked no code.
        let mut graphless = manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        });
        if let Some(body) = graphless
            .workflows
            .first_mut()
            .and_then(|w| w.bodies.first_mut())
        {
            body.flow = None;
        }
        let err = check(&graphless).expect_err("a body with no flow graph");
        assert!(err.to_string().contains("no flow graph"), "{err}");

        // A handler entry that no registration node names is not a handler.
        // Its bodies would read as entries, so a helper would look covered.
        let mut orphan = manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        });
        if let Some(w) = orphan.workflows.first_mut() {
            w.handlers.push(HandlerSite {
                kind: "update".to_string(),
                method: "register_update_handler".to_string(),
                name: None,
                bodies: vec![w.root.clone()],
                returns: Vec::new(),
            });
        }
        let err = check(&orphan).expect_err("a handler entry with no node");
        assert!(err.to_string().contains("handler 0"), "{err}");

        // A handler node names an entry of `handlers` that is not there.
        let dangling = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Handler { handler: 0 }),
            ],
            edges: vec![edge(0, 1, None)],
        };
        let err = check(&manifest(dangling)).expect_err("a handler index past the list");
        assert!(err.to_string().contains("handler 0"), "{err}");
    }

    #[test]
    fn a_stray_or_doubled_body_is_refused() {
        // No reference reaches the body `w::stray` from the root. Its gap
        // would mark a workflow that has no saga.
        let mut stray = manifest(FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        });
        if let Some(w) = stray.workflows.first_mut() {
            w.bodies.push(BodyNode {
                id: "w::stray".to_string(),
                flow: Some(FlowGraph {
                    nodes: vec![node(FlowEvent::Entry)],
                    edges: Vec::new(),
                }),
                ..BodyNode::default()
            });
        }
        let err = check(&stray).expect_err("a body the root never reaches");
        assert!(err.to_string().contains("w::stray"), "{err}");

        // Two bodies share the id `w::helper`. A reference to it is ambiguous,
        // and the check would read both.
        let mut twice = manifest(FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Call {
                    callees: vec!["w::helper".to_string()],
                }),
            ],
            edges: vec![edge(0, 1, None)],
        });
        if let Some(w) = twice.workflows.first_mut() {
            for _ in 0..2 {
                w.bodies.push(BodyNode {
                    id: "w::helper".to_string(),
                    flow: Some(FlowGraph {
                        nodes: vec![node(FlowEvent::Entry)],
                        edges: Vec::new(),
                    }),
                    ..BodyNode::default()
                });
            }
        }
        let err = check(&twice).expect_err("two bodies with one id");
        assert!(err.to_string().contains("w::helper"), "{err}");
    }

    #[test]
    fn a_gap_reached_only_through_a_back_edge_is_found() {
        // entry -> new -> call -> step; step -ok-> call (the loop);
        // call -> err exit. The first pass reaches the exit with the flag
        // clear. Only the back edge carries the set flag to it.
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(FlowEvent::Call {
                    callees: Vec::new(),
                }),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, None),
                edge(2, 4, None),
                edge(3, 2, Some(EdgeLabel::Ok)),
                edge(3, 4, Some(EdgeLabel::Err)),
            ],
        };
        let report = check_workflow(&workflow(graph));
        assert_eq!(report.verdict, SagaVerdict::Gap, "{report:#?}");
        assert_eq!(report.gaps.len(), 1);
    }

    #[test]
    fn an_unwind_that_is_not_awaited_clears_nothing() {
        let graph = |tracked: bool| FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::SagaCompensate { tracked }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
                edge(3, 4, None),
            ],
        };
        assert_eq!(
            check_workflow(&workflow(graph(true))).verdict,
            SagaVerdict::Covered
        );
        assert_eq!(
            check_workflow(&workflow(graph(false))).verdict,
            SagaVerdict::Gap
        );
    }

    #[test]
    fn an_ok_exit_of_the_root_with_a_pending_step_is_not_a_gap() {
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
            ],
        };
        assert_eq!(
            check_workflow(&workflow(graph)).verdict,
            SagaVerdict::Covered
        );
    }

    #[test]
    fn an_err_exit_of_a_handler_with_a_pending_step_is_a_gap() {
        // The root registers an update handler. Its closure returns a
        // coroutine that owns a saga, completes a step, then fails. The
        // manifest names that coroutine in the handler `returns`.
        let root = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Handler { handler: 0 }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
            ],
            edges: vec![edge(0, 1, None), edge(1, 2, None)],
        };
        let closure = FlowGraph {
            nodes: vec![node(FlowEvent::Entry)],
            edges: Vec::new(),
        };
        let coroutine = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
                edge(2, 4, Some(EdgeLabel::Err)),
            ],
        };
        let mut w = workflow(root);
        let handler = "w::wf::{closure#0}::{closure#0}";
        let future = "w::wf::{closure#0}::{closure#0}::{closure#0}";
        w.handlers.push(HandlerSite {
            kind: "update".to_string(),
            method: "register_update_handler".to_string(),
            name: None,
            bodies: vec![handler.to_string()],
            returns: vec![future.to_string()],
        });
        w.bodies.push(BodyNode {
            id: handler.to_string(),
            calls: vec![CallSite {
                callee: future.to_string(),
                in_loop: false,
                resume: false,
            }],
            flow: Some(closure),
            ..BodyNode::default()
        });
        w.bodies.push(BodyNode {
            id: future.to_string(),
            flow: Some(coroutine),
            ..BodyNode::default()
        });
        let r = check_workflow(&w);
        assert_eq!(r.verdict, SagaVerdict::Gap, "{r:#?}");
        assert!(r.gaps.iter().any(|g| g.body == future), "{r:#?}");
    }

    #[test]
    fn a_handler_body_that_a_helper_call_also_runs_is_checked_both_ways() {
        // The root registers `h` and also calls it. `h` returns `Ok` with a
        // step pending. As a handler that is fine. As a helper, the caller
        // cannot unwind that step.
        let h = "w::wf::{closure#0}::{closure#0}";
        let root = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::Handler { handler: 0 }),
                node(FlowEvent::Call {
                    callees: vec![h.to_string()],
                }),
            ],
            edges: vec![edge(0, 1, None), edge(1, 2, None)],
        };
        let body = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(step()),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Err,
                }),
            ],
            edges: vec![
                edge(0, 1, None),
                edge(1, 2, None),
                edge(2, 3, Some(EdgeLabel::Ok)),
                edge(2, 4, Some(EdgeLabel::Err)),
            ],
        };
        let mut w = workflow(root);
        w.handlers.push(HandlerSite {
            kind: "signal".to_string(),
            method: "register_signal_handler".to_string(),
            name: None,
            bodies: vec![h.to_string()],
            returns: Vec::new(),
        });
        // The registration passes `h` as an argument. That edge alone is not
        // a helper call.
        let registration = |id: &str| CallSite {
            callee: id.to_string(),
            in_loop: true,
            resume: false,
        };
        if let Some(root) = w.bodies.first_mut() {
            root.calls.push(registration(h));
        }
        w.bodies.push(BodyNode {
            id: h.to_string(),
            flow: Some(body),
            ..BodyNode::default()
        });
        let r = check_workflow(&w);
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
        assert!(
            r.unknown
                .iter()
                .any(|u| u.starts_with("saga-dropped-pending")),
            "{r:#?}"
        );

        // With the registration alone, `h` runs only as a handler.
        if let Some(graph) = w.bodies.first_mut().and_then(|b| b.flow.as_mut()) {
            graph.nodes.truncate(2);
            graph.edges.truncate(1);
        }
        let r = check_workflow(&w);
        assert_eq!(r.verdict, SagaVerdict::Covered, "{r:#?}");
    }

    #[test]
    fn a_body_with_only_a_saga_escape_is_unknown() {
        // A saga value arrives and leaves. No saga node owns or steps it.
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaEscape {
                    to: "dep::finish".to_string(),
                }),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
            ],
            edges: vec![edge(0, 1, None), edge(1, 2, None)],
        };
        let r = check_workflow(&workflow(graph));
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
        assert!(
            r.unknown.iter().any(|u| u.starts_with("saga-escapes: ")),
            "{r:#?}"
        );
    }

    #[test]
    fn a_boundary_that_runs_code_is_unknown_beside_a_visible_saga() {
        let graph = FlowGraph {
            nodes: vec![
                node(FlowEvent::Entry),
                node(FlowEvent::SagaNew),
                node(FlowEvent::Exit {
                    outcome: ExitOutcome::Ok,
                }),
            ],
            edges: vec![edge(0, 1, None), edge(1, 2, None)],
        };
        // A body outside the analysis can hold a second saga with a gap.
        let mut hidden = workflow(graph.clone());
        hidden.boundaries = vec!["external-crate-body: dep::book".to_string()];
        let r = check_workflow(&hidden);
        assert_eq!(r.verdict, SagaVerdict::Unknown, "{r:#?}");
        assert!(
            r.unknown.iter().any(|u| u.starts_with("boundary: ")),
            "{r:#?}"
        );

        // An external constant runs no code, so it cannot hide a saga.
        let mut constant = workflow(graph);
        constant.boundaries = vec!["external-const: dep::LIMIT".to_string()];
        assert_eq!(check_workflow(&constant).verdict, SagaVerdict::Covered);
    }
}
