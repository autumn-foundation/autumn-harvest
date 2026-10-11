//! The structure manifest: the command-emitting call graph of each workflow
//! (issue #1995).
//!
//! The analyzer already resolves this graph. A [`Recorder`] keeps what it
//! visits: each body, each call edge and each sink call site. A
//! [`StructureBuilder`] turns that record into a [`WorkflowStructure`].
//! `--emit-structure` writes one [`StructureManifest`] for the run.
//!
//! The upgrade check in `autumn_harvest::upgrade_check` reads two manifests,
//! one per build. It diffs them body by body. So a body id and a digest must
//! stay the same when the body does not change:
//!
//! - An id is the qualified body path with each source span removed.
//! - A digest hashes the raw MIR text of the body and of each item nested
//!   under it. It also hashes each `const` item and each `allocN` footer that
//!   the body reads. It drops spans and `allocN` numbers.
//!
//! A false change only costs a `review` verdict. A missed change can cost a
//! wrong `migrate`. So the digest keeps every other byte of the body.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::entry::Entry;
use crate::flow::BlockFacts;
use crate::mir::MirDoc;
use crate::mir::ast::{Body, Operand, Place, Statement, Terminator};
use crate::resolve::Program;
use crate::util::strip_generics_everywhere;

/// The manifest format this module writes.
pub const STRUCTURE_FORMAT: &str = "harvest-structure/1";

/// Marker prefixes the `#[activity]` and `#[workflow]` macros emit.
const INFO_MARKERS: [&str; 2] = ["__autumn_activity_info_", "__autumn_workflow_info_"];

/// The longest chain of locals that step-key resolution follows.
const MAX_KEY_DEPTH: u8 = 8;

/// The flow graph format this module writes (issue #2010).
pub const FLOW_FORMAT: &str = "harvest-flow/1";

/// The structure of every analyzed workflow in one build.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructureManifest {
    /// Always [`STRUCTURE_FORMAT`].
    pub format: String,
    pub model_version: String,
    pub rustc_version: String,
    /// [`FLOW_FORMAT`] when each body carries a flow graph. A manifest from
    /// an older build has none, so a check over flow graphs refuses it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<String>,
    pub workflows: Vec<WorkflowStructure>,
}

/// The call graph of one workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStructure {
    /// `crate::module::fn`, the same path as the verdict.
    pub workflow: String,
    /// The registered workflow name. The `#[workflow]` macro uses the fn name.
    pub name: String,
    /// The id of the body the analysis starts from.
    pub root: String,
    /// Each `unknown` boundary of the workflow, as `kind: detail`.
    #[serde(default)]
    pub boundaries: Vec<String>,
    /// Each body reachable from the root, sorted by id.
    pub bodies: Vec<BodyNode>,
    /// Each signal, update or query handler the workflow registers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub handlers: Vec<HandlerSite>,
}

/// One handler registration (issue #2010).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerSite {
    /// `signal`, `update` or `query`.
    pub kind: String,
    /// The model row, such as `register_signal_handler`.
    pub method: String,
    /// The handler name, when the MIR shows it.
    pub name: Option<String>,
    /// The closure bodies the registration passes, such as a validator and a
    /// handler.
    pub bodies: Vec<String>,
}

/// The condensed control-flow graph of one body (issue #2010).
///
/// A node is an event. An edge joins two events when a path joins them with
/// no other event on it. A node id is its index in `nodes`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowGraph {
    pub nodes: Vec<FlowNode>,
    pub edges: Vec<FlowEdge>,
}

/// One event in a body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowNode {
    /// The MIR block of the event. It is for diagnostics only, because block
    /// labels change from build to build.
    pub at: String,
    #[serde(flatten)]
    pub event: FlowEvent,
}

/// What happens at a flow node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum FlowEvent {
    /// The first block. For a coroutine, the target of state `0`.
    Entry,
    /// A step site. The value is its index in [`BodyNode::steps`].
    Step { step: usize },
    /// A call that starts other bodies of the graph.
    Call { callees: Vec<String> },
    /// A handler registration. The value is its index in
    /// [`WorkflowStructure::handlers`].
    Handler { handler: usize },
    /// `Saga::new`, or another call that returns a saga value.
    SagaNew,
    /// `Saga::step`, with its forward and compensation closure bodies.
    /// `tracked` is true when its `ok` and `err` edges are labeled.
    SagaStep {
        forward: Vec<String>,
        compensate: Vec<String>,
        tracked: bool,
    },
    /// `Saga::compensate_all`. `tracked` is true when the body awaits it.
    SagaCompensate { tracked: bool },
    /// A value of type `Saga` reaches a call or a value that is not a
    /// `Saga` method. The value names the call or the statement.
    SagaEscape { to: String },
    /// A write of the value the body returns.
    Exit { outcome: ExitOutcome },
}

/// What an exit returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExitOutcome {
    /// A literal `Ok(..)`.
    Ok,
    /// A literal `Err(..)`, or the error arm of `?`.
    Err,
    /// Any other value, such as a call result. It can be an error.
    Unknown,
}

impl ExitOutcome {
    /// The outcome as the manifest spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Err => "err",
            Self::Unknown => "unknown",
        }
    }
}

/// One edge of a flow graph.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FlowEdge {
    pub from: usize,
    pub to: usize,
    /// Set only on the out-edges of a tracked `Saga::step`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<EdgeLabel>,
}

/// The arm of a `Saga::step` result that an edge follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EdgeLabel {
    /// The step completed, so its compensation is pending.
    Ok,
    /// The step failed, so the saga unwound every earlier step.
    Err,
}

/// One body in a workflow graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BodyNode {
    pub id: String,
    /// Hex SHA-256 of the normalized body text and of each item it holds or
    /// reads. See the module docs.
    pub digest: String,
    /// One entry per call site and callee, sorted.
    #[serde(default)]
    pub calls: Vec<CallSite>,
    /// One entry per sink call site, sorted.
    #[serde(default)]
    pub steps: Vec<StepSite>,
    /// The flow graph of the body (issue #2010). The digest does not read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<FlowGraph>,
}

/// One call from a body to another body in the graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallSite {
    pub callee: String,
    /// The call can run more than once per call of the caller. It sits in a
    /// cycle of the caller's control flow, or it runs a closure that the
    /// caller passes to another function.
    pub in_loop: bool,
    /// The call does not start the body. It handles or drops a future that
    /// another call site built, such as `poll` or drop glue.
    #[serde(default, skip_serializing_if = "is_false")]
    pub resume: bool,
}

/// One command a body emits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSite {
    /// The model's sink row, such as `execute_activity`.
    pub sink: String,
    /// The history record, such as `activity`, or `other`.
    pub kind: String,
    /// The step name, when the MIR shows it.
    pub key: Option<String>,
    pub in_loop: bool,
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde passes a reference"
)]
const fn is_false(value: &bool) -> bool {
    !*value
}

/// What the analyzer visited while it walked one workflow.
#[derive(Debug, Clone, Default)]
pub struct Recorder {
    /// Each body id the analyzer entered.
    pub bodies: BTreeSet<String>,
    /// Each call edge it followed.
    pub edges: BTreeSet<Edge>,
    /// Each step site it classified.
    pub sinks: BTreeSet<SinkSite>,
    /// Each closure or fn item passed as an argument, with its index
    /// (issue #2010).
    pub arguments: BTreeSet<ArgumentEdge>,
    /// Each handler registration it met (issue #2010).
    pub handlers: BTreeSet<HandlerCall>,
}

/// A body passed as argument `index` of the call in `block`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArgumentEdge {
    pub caller: String,
    pub block: String,
    /// The call operand index, `self` included.
    pub index: usize,
    pub body: String,
}

/// A handler registration call.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HandlerCall {
    pub caller: String,
    pub block: String,
    /// The model row path, such as `register_signal_handler`.
    pub method: String,
}

/// A call edge between two body ids.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Edge {
    pub caller: String,
    pub block: String,
    pub callee: String,
    /// The call does not start the callee. See [`CallSite::resume`].
    pub resume: bool,
    /// The callee can run many times per call, such as a closure argument.
    pub many: bool,
}

/// A step site: a sink call, or a ctx call that can suspend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SinkSite {
    pub body: String,
    pub block: String,
    /// The model row path.
    pub sink: String,
    /// The model row `step`.
    pub step: Option<String>,
    /// The call operand index of the step key, `self` included.
    pub key_arg: Option<usize>,
}

impl Recorder {
    /// Record a call edge from body `from` to body `to`. `printed` is the
    /// callee path as MIR prints it.
    pub fn edge(&mut self, from: &str, block: &str, to: &str, printed: &str) {
        let resume = is_resume(printed);
        // A callee that names a closure type, such as `Map::<_, {closure@..}>::next`,
        // runs that closure as often as it likes.
        let many = !resume && printed.contains("closure@");
        self.insert_edge(from, block, to, resume, many);
    }

    /// Record a drop-glue edge. A drop never starts the body.
    pub fn drop_edge(&mut self, from: &str, block: &str, to: &str) {
        self.insert_edge(from, block, to, true, false);
    }

    /// Record a closure that `from` passes to another call. The callee of
    /// that call can run the closure any number of times.
    pub fn closure_edge(&mut self, from: &str, block: &str, to: &str) {
        self.insert_edge(from, block, to, false, true);
    }

    /// Record that `body` is argument `index` of the call in `block`.
    pub fn argument(&mut self, from: &str, block: &str, index: usize, body: &str) {
        self.arguments.insert(ArgumentEdge {
            caller: from.to_string(),
            block: block.to_string(),
            index,
            body: body.to_string(),
        });
    }

    /// Record the `async` block a closure returns. The closure builds it, and
    /// the caller of the closure runs it to completion once per call.
    pub fn future_edge(&mut self, from: &str, block: &str, to: &str) {
        self.insert_edge(from, block, to, false, false);
    }

    /// Record a handler registration in `block`.
    pub fn handler(&mut self, body: &str, block: &str, method: &str) {
        self.handlers.insert(HandlerCall {
            caller: body.to_string(),
            block: block.to_string(),
            method: method.to_string(),
        });
    }

    fn insert_edge(&mut self, from: &str, block: &str, to: &str, resume: bool, many: bool) {
        self.edges.insert(Edge {
            caller: from.to_string(),
            block: block.to_string(),
            callee: to.to_string(),
            resume,
            many,
        });
    }
}

/// A call that handles an existing future, not one that starts a body.
///
/// The resolver maps a callee that names an `{async ...}` type to that
/// coroutine. Only `into_future`, `poll` and the `Pin` constructors resume
/// it. Any other callee is a real start, even with a future in its generic
/// arguments.
fn is_resume(printed: &str) -> bool {
    if !printed.contains("{async") {
        return false;
    }
    let bare = strip_generics_everywhere(printed);
    bare.ends_with("::poll") || bare.ends_with("::into_future") || bare.starts_with("Pin::")
}

/// Wrap the workflow structures of one run in a manifest.
#[must_use]
pub fn manifest(
    model_version: &str,
    rustc_version: &str,
    mut workflows: Vec<WorkflowStructure>,
) -> StructureManifest {
    workflows.sort_by(|a, b| (&a.name, &a.workflow).cmp(&(&b.name, &b.workflow)));
    StructureManifest {
        format: STRUCTURE_FORMAT.to_string(),
        model_version: model_version.to_string(),
        rustc_version: rustc_version.to_string(),
        flow: Some(FLOW_FORMAT.to_string()),
        workflows,
    }
}

/// Builds workflow structures over one program. It caches each digest and
/// each loop set, because many workflows share bodies.
pub struct StructureBuilder<'p> {
    program: &'p Program,
    digests: HashMap<String, String>,
    cyclic: HashMap<String, BTreeSet<String>>,
    /// Per doc path: lookup tables over its bodies.
    doc_index: HashMap<String, DocIndex>,
    /// Per body id: the `const` items it reads that no analyzed crate holds.
    external_consts: HashMap<String, BTreeSet<String>>,
    /// The names `X` that have an activity or workflow marker body.
    markers: Option<BTreeSet<String>>,
}

impl<'p> StructureBuilder<'p> {
    #[must_use]
    pub fn new(program: &'p Program) -> Self {
        Self {
            program,
            digests: HashMap::new(),
            cyclic: HashMap::new(),
            doc_index: HashMap::new(),
            external_consts: HashMap::new(),
            markers: None,
        }
    }

    /// Build the structure of one workflow from what the analyzer recorded.
    ///
    /// `root` is the body id the analysis started from. `boundaries` are the
    /// workflow's boundaries, already rendered as `kind: detail`.
    pub fn build(
        &mut self,
        entry: &Entry,
        root: &str,
        recorder: &Recorder,
        boundaries: Vec<String>,
    ) -> WorkflowStructure {
        let program = self.program;
        // A `const` body is folded into the digest of each body that reads
        // it, so it is not a node of its own.
        let ids: Vec<&String> = recorder
            .bodies
            .iter()
            .filter(|id| program.body(id).is_none_or(|b| !b.is_const))
            .collect();
        for id in &ids {
            self.digest(id);
        }
        let (display, ambiguous) = self.display_ids(&ids);
        let mut boundaries = boundaries;
        boundaries.extend(ambiguous);
        let external: BTreeSet<&String> = ids
            .iter()
            .filter_map(|id| self.external_consts.get(id.as_str()))
            .flatten()
            .collect();
        boundaries.extend(
            external
                .into_iter()
                .map(|name| format!("external-const: {name}")),
        );
        let show = |id: &str| {
            display
                .get(id)
                .cloned()
                .unwrap_or_else(|| normalize(&program.qualified_name(id), None))
        };

        let mut edges: BTreeMap<&str, Vec<&Edge>> = BTreeMap::new();
        for edge in &recorder.edges {
            if display.contains_key(&edge.callee) {
                edges.entry(edge.caller.as_str()).or_default().push(edge);
            }
        }
        let mut sinks: BTreeMap<&str, Vec<&SinkSite>> = BTreeMap::new();
        for site in &recorder.sinks {
            sinks.entry(site.body.as_str()).or_default().push(site);
        }

        let (handlers, handler_at) = self.handlers(recorder, &display, &show);
        let arguments = group_arguments(recorder, &display);

        let mut bodies = Vec::with_capacity(ids.len());
        for id in ids {
            let cyclic = self.cyclic_blocks(id);
            let body = program.body(id);
            let mut calls: Vec<CallSite> = edges
                .get(id.as_str())
                .into_iter()
                .flatten()
                .map(|e| CallSite {
                    callee: show(&e.callee),
                    in_loop: e.many || cyclic.contains(&e.block),
                    resume: e.resume,
                })
                .collect();
            calls.sort_by(|a, b| {
                (&a.callee, a.resume, a.in_loop).cmp(&(&b.callee, b.resume, b.in_loop))
            });
            let sited = self.step_sites(
                sinks.get(id.as_str()).map_or(&[][..], Vec::as_slice),
                body,
                &cyclic,
            );
            let blocks: Vec<&str> = sited.iter().map(|(_, block)| *block).collect();
            let in_engine = |id: &str| program.qualified_name(id).starts_with("autumn_harvest::");
            let mut facts = block_facts(
                &blocks,
                edges.get(id.as_str()).map_or(&[][..], Vec::as_slice),
                arguments.get(id.as_str()).map_or(&[][..], Vec::as_slice),
                &show,
                &in_engine,
            );
            facts.handlers = handler_blocks(&handler_at, id);
            let flow = flow_graph(body, &facts);
            bodies.push(BodyNode {
                id: show(id),
                digest: self.digests.get(id.as_str()).cloned().unwrap_or_default(),
                calls,
                steps: sited.into_iter().map(|(step, _)| step).collect(),
                flow: Some(flow),
            });
        }
        bodies.sort_by(|a, b| a.id.cmp(&b.id));

        let name = entry
            .workflow
            .rsplit("::")
            .next()
            .unwrap_or(&entry.workflow)
            .to_string();
        WorkflowStructure {
            workflow: entry.workflow.clone(),
            name,
            root: show(root),
            boundaries,
            bodies,
            handlers,
        }
    }

    /// The step sites of one body, sorted, each with its block.
    fn step_sites<'s>(
        &mut self,
        sites: &[&'s SinkSite],
        body: Option<&Body>,
        cyclic: &BTreeSet<String>,
    ) -> Vec<(StepSite, &'s str)> {
        let mut out: Vec<(StepSite, &str)> = Vec::new();
        for site in sites {
            let key = match (&site.step, site.key_arg, body) {
                (Some(_), Some(arg), Some(body)) => self.step_key(body, &site.block, arg),
                _ => None,
            };
            let step = StepSite {
                sink: site.sink.clone(),
                kind: site.step.clone().unwrap_or_else(|| "other".to_string()),
                key,
                in_loop: cyclic.contains(&site.block),
            };
            out.push((step, site.block.as_str()));
        }
        out.sort_by(|(a, x), (b, y)| {
            (&a.kind, &a.key, &a.sink, a.in_loop, x).cmp(&(&b.kind, &b.key, &b.sink, b.in_loop, y))
        });
        out
    }

    /// The handler list of one workflow, and the index of each registration
    /// by `(body, block)`.
    fn handlers(
        &mut self,
        recorder: &Recorder,
        display: &BTreeMap<String, String>,
        show: &dyn Fn(&str) -> String,
    ) -> (Vec<HandlerSite>, BTreeMap<(String, String), usize>) {
        let program = self.program;
        let mut list = Vec::new();
        let mut at = BTreeMap::new();
        for call in &recorder.handlers {
            if !display.contains_key(&call.caller) {
                continue;
            }
            // Operand 0 is the context, and operand 1 is the handler name.
            let name = program.body(&call.caller).and_then(|body| {
                let block = body.blocks.iter().find(|b| b.label == call.block)?;
                let Terminator::Call { args, .. } = &block.terminator else {
                    return None;
                };
                let markers = self.markers();
                key_of_operand(markers, body, args.get(1)?, 0)
            });
            let mut bodies: Vec<String> = recorder
                .arguments
                .iter()
                .filter(|a| a.caller == call.caller && a.block == call.block)
                .filter(|a| display.contains_key(&a.body))
                .map(|a| show(&a.body))
                .collect();
            bodies.sort_unstable();
            bodies.dedup();
            let kind = ["signal", "update", "query"]
                .into_iter()
                .find(|k| call.method.contains(k))
                .unwrap_or("other");
            at.insert((call.caller.clone(), call.block.clone()), list.len());
            list.push(HandlerSite {
                kind: kind.to_string(),
                method: call.method.clone(),
                name,
                bodies,
            });
        }
        (list, at)
    }

    /// Display ids, unique within the workflow, and a boundary per collision.
    ///
    /// Two bodies can normalize to one id, such as two impls of one method in
    /// one file. They get `#2`, `#3` in digest order, which a line shift
    /// cannot change. But the suffix follows the content, not the body. Two
    /// bodies that swap code between builds keep the same pairs. So each
    /// collision is also a boundary, and the upgrade check asks for review.
    fn display_ids(&self, ids: &[&String]) -> (BTreeMap<String, String>, Vec<String>) {
        let mut groups: BTreeMap<String, Vec<(&str, &str)>> = BTreeMap::new();
        for id in ids {
            let base = normalize(&self.program.qualified_name(id), None);
            let digest = self.digests.get(id.as_str()).map_or("", String::as_str);
            groups.entry(base).or_default().push((digest, id.as_str()));
        }
        let mut out = BTreeMap::new();
        let mut ambiguous = Vec::new();
        for (base, mut members) in groups {
            if members.len() > 1 {
                ambiguous.push(format!("ambiguous-body-id: {base}"));
            }
            members.sort_unstable();
            for (n, (_, id)) in members.into_iter().enumerate() {
                let shown = if n == 0 {
                    base.clone()
                } else {
                    format!("{base}#{}", n.saturating_add(1))
                };
                out.insert(id.to_string(), shown);
            }
        }
        (out, ambiguous)
    }

    // ── digest ──────────────────────────────────────────────────────────────

    /// Hex SHA-256 of the body `id` and of everything it holds or reads:
    /// nested items, `const` items and `allocN` footers.
    ///
    /// A digest of the parsed form would miss text the parser drops, such as
    /// the case values of a `switchInt`. So the raw text is hashed. A `const`
    /// item of another analyzed crate is hashed from that crate's doc.
    fn digest(&mut self, id: &str) -> String {
        if let Some(known) = self.digests.get(id) {
            return known.clone();
        }
        let program = self.program;
        let value = match (program.body(id), program.doc_of(id)) {
            (Some(body), Some(doc)) => {
                let (text, foreign) = self.closure_text(doc, vec![body.path.clone()]);
                let mut normalized = normalize(&text, Some(&doc.alloc_statics));
                let external = self.fold_foreign_consts(doc, foreign, &mut normalized);
                if !external.is_empty() {
                    self.external_consts.insert(id.to_string(), external);
                }
                hex(&Sha256::digest(normalized.as_bytes()))
            }
            _ => hex(&Sha256::digest(format!("<missing body {id}>").as_bytes())),
        };
        self.digests.insert(id.to_string(), value.clone());
        value
    }

    /// The text of the items at `start` and of the items nested under them.
    /// Then the text of the `const` items they read, and the footers of the
    /// allocs they name. Also the `const` names that `doc` does not hold.
    fn closure_text(&mut self, doc: &MirDoc, start: Vec<String>) -> (String, Vec<String>) {
        let index = self
            .doc_index
            .entry(doc.path.clone())
            .or_insert_with(|| DocIndex::new(doc));
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut queue: Vec<String> = start;
        let mut text = String::new();
        let mut foreign: Vec<String> = Vec::new();
        while let Some(path) = queue.pop() {
            if !seen.insert(path.clone()) {
                continue;
            }
            for &at in index.by_path.get(&path).into_iter().flatten() {
                if let Some(item) = doc.bodies.get(at) {
                    let _ = write!(text, "\n#item {path}\n{}", item.text);
                    let (found, missing) = index.const_refs(&item.text);
                    queue.extend(found);
                    foreign.extend(missing);
                }
            }
            if let Some(line) = doc.inline_consts.get(&path) {
                let _ = write!(text, "\n#item {path}\n{line}");
            }
            queue.extend(index.children(&path));
        }
        append_allocs(doc, &mut text);
        (text, foreign)
    }

    /// Append the text of each `const` item in `names` that another analyzed
    /// crate holds. Return the names that no analyzed crate holds.
    ///
    /// MIR prints a `const` read from another crate with its crate root, as
    /// `const dep::LIMIT`. Its value is not in the reader's MIR. So the
    /// digest reads the item from the doc of that crate. A std or trusted
    /// crate does not change between two builds on one toolchain, so it is
    /// skipped. Any other crate is outside the analysis, so its name returns
    /// and becomes a boundary.
    fn fold_foreign_consts(
        &mut self,
        from: &MirDoc,
        names: Vec<String>,
        out: &mut String,
    ) -> BTreeSet<String> {
        let program = self.program;
        let mut external = BTreeSet::new();
        let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
        let mut queue: Vec<(&MirDoc, String)> = names.into_iter().map(|n| (from, n)).collect();
        while let Some((reader, name)) = queue.pop() {
            if !seen.insert((reader.crate_name.clone(), name.clone())) {
                continue;
            }
            // An associated constant names two crates, one for its type and
            // one for its trait, and the impl lives in either. So each
            // analyzed crate it names is searched, and each match counts.
            let roots = const_roots(&name);
            let mut found = false;
            let mut outside = false;
            for root in &roots {
                if *root == reader.crate_name {
                    continue;
                }
                let Some(doc) = program.docs.iter().find(|d| d.crate_name == *root) else {
                    outside |= !program.is_trusted_root(root);
                    continue;
                };
                let local = name
                    .strip_prefix(root)
                    .and_then(|rest| rest.strip_prefix("::"))
                    .unwrap_or(&name);
                let index = self
                    .doc_index
                    .entry(doc.path.clone())
                    .or_insert_with(|| DocIndex::new(doc));
                let paths = index.const_paths(local);
                if paths.is_empty() {
                    continue;
                }
                found = true;
                let (text, more) = self.closure_text(doc, paths);
                let _ = write!(out, "\n#crate {}\n", doc.crate_name);
                out.push_str(&normalize(&text, Some(&doc.alloc_statics)));
                queue.extend(more.into_iter().map(|n| (doc, n)));
            }
            // A crate outside the analysis can hold the item. So can an
            // analyzed crate that held no match, if none did.
            let analyzed = roots.iter().any(|r| {
                *r != reader.crate_name && program.docs.iter().any(|d| d.crate_name == *r)
            });
            if outside || (analyzed && !found) {
                external.insert(name);
            }
        }
        external
    }

    // ── loops ───────────────────────────────────────────────────────────────

    fn cyclic_blocks(&mut self, id: &str) -> BTreeSet<String> {
        if let Some(known) = self.cyclic.get(id) {
            return known.clone();
        }
        let value = self.program.body(id).map(cyclic_blocks).unwrap_or_default();
        self.cyclic.insert(id.to_string(), value.clone());
        value
    }

    // ── step keys ───────────────────────────────────────────────────────────

    /// The step key of the call in `block`, when the MIR shows it.
    ///
    /// The key is a string constant that reaches the argument through plain
    /// copies and references. A typed activity or child workflow passes
    /// `&X_info()`. The key is then `X`, when the macro marker for `X` exists.
    fn step_key(&mut self, body: &Body, block: &str, key_arg: usize) -> Option<String> {
        let block = body.blocks.iter().find(|b| b.label == block)?;
        let Terminator::Call { args, .. } = &block.terminator else {
            return None;
        };
        let operand = args.get(key_arg)?;
        let markers = self.markers();
        key_of_operand(markers, body, operand, 0)
    }

    fn markers(&mut self) -> &BTreeSet<String> {
        let program = self.program;
        self.markers.get_or_insert_with(|| {
            program
                .body_paths()
                .into_iter()
                .filter_map(|path| {
                    let tail = path.rsplit("::").next().unwrap_or(path);
                    INFO_MARKERS.iter().find_map(|m| tail.strip_prefix(m))
                })
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect()
        })
    }
}

/// Each recorded argument whose body is in the graph, by caller.
fn group_arguments<'r>(
    recorder: &'r Recorder,
    display: &BTreeMap<String, String>,
) -> BTreeMap<&'r str, Vec<&'r ArgumentEdge>> {
    let mut out: BTreeMap<&str, Vec<&ArgumentEdge>> = BTreeMap::new();
    for argument in &recorder.arguments {
        if display.contains_key(&argument.body) {
            out.entry(argument.caller.as_str())
                .or_default()
                .push(argument);
        }
    }
    out
}

/// Block → handler index, for the registrations in body `id`.
fn handler_blocks(at: &BTreeMap<(String, String), usize>, id: &str) -> BTreeMap<String, usize> {
    at.iter()
        .filter(|((caller, _), _)| caller == id)
        .map(|((_, block), index)| (block.clone(), *index))
        .collect()
}

/// The flow graph of `body`. A body with no MIR gets only its entry.
fn flow_graph(body: Option<&Body>, facts: &BlockFacts) -> FlowGraph {
    body.map_or_else(
        || FlowGraph {
            nodes: vec![FlowNode {
                at: String::new(),
                event: FlowEvent::Entry,
            }],
            edges: Vec::new(),
        },
        |body| crate::flow::build(body, facts),
    )
}

/// What the flow graph needs to know of each block of one body.
///
/// `steps` holds the block of each step site, in the sorted step order.
fn block_facts(
    steps: &[&str],
    edges: &[&Edge],
    arguments: &[&ArgumentEdge],
    show: &dyn Fn(&str) -> String,
    in_engine: &dyn Fn(&str) -> bool,
) -> BlockFacts {
    let mut facts = BlockFacts::default();
    for (index, block) in steps.iter().enumerate() {
        facts.steps.insert((*block).to_string(), index);
    }
    // A closure argument edge (`many`) starts a closure, not the callee. A
    // body of the engine crate is the engine method itself.
    for edge in edges.iter().filter(|e| !e.many && !in_engine(&e.callee)) {
        facts.bodied.insert(edge.block.clone());
    }
    for edge in edges.iter().filter(|e| !e.resume) {
        facts
            .calls
            .entry(edge.block.clone())
            .or_default()
            .push(show(&edge.callee));
    }
    for callees in facts.calls.values_mut() {
        callees.sort_unstable();
        callees.dedup();
    }
    for argument in arguments {
        let bodies = facts
            .arguments
            .entry((argument.block.clone(), argument.index))
            .or_default();
        bodies.push(show(&argument.body));
        bodies.sort_unstable();
        bodies.dedup();
    }
    facts
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Lookup tables over the bodies of one doc.
struct DocIndex {
    /// Raw body paths, sorted, for prefix lookups.
    sorted: Vec<String>,
    /// Raw body path → each body index with that path.
    by_path: HashMap<String, Vec<usize>>,
    /// Last path segment of a `const` body → its raw paths.
    consts: HashMap<String, Vec<String>>,
}

impl DocIndex {
    fn new(doc: &MirDoc) -> Self {
        let mut by_path: HashMap<String, Vec<usize>> = HashMap::new();
        let mut consts: HashMap<String, Vec<String>> = HashMap::new();
        for (at, body) in doc.bodies.iter().enumerate() {
            by_path.entry(body.path.clone()).or_default().push(at);
            if body.is_const {
                let tail = body.path.rsplit("::").next().unwrap_or(&body.path);
                consts
                    .entry(tail.to_string())
                    .or_default()
                    .push(body.path.clone());
            }
        }
        for path in doc.inline_consts.keys() {
            let tail = path.rsplit("::").next().unwrap_or(path);
            consts
                .entry(tail.to_string())
                .or_default()
                .push(path.clone());
        }
        let mut sorted: Vec<String> = by_path.keys().cloned().collect();
        sorted.sort_unstable();
        Self {
            sorted,
            by_path,
            consts,
        }
    }

    /// The raw paths of the items nested under `path`.
    fn children(&self, path: &str) -> Vec<String> {
        let prefix = format!("{path}::");
        let start = self
            .sorted
            .partition_point(|p| p.as_str() < prefix.as_str());
        self.sorted
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take_while(|p| p.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// The raw paths of the `const` items that `text` reads, and the names
    /// it reads that this doc does not hold.
    ///
    /// MIR prints a read of a `const` item by name, as `const NAME`. A
    /// promoted constant is nested under its body, so the walk finds it as a
    /// child.
    fn const_refs(&self, text: &str) -> (Vec<String>, Vec<String>) {
        let mut found = Vec::new();
        let mut missing = Vec::new();
        let mut rest = text;
        while let Some(at) = rest.find("const ") {
            let tail = rest.get(at.saturating_add(6)..).unwrap_or_default();
            let name = const_name(tail);
            let paths = self.const_paths(name);
            // A read such as `limits::ATTEMPTS` can match a local `ATTEMPTS`
            // only by its last segment. The local item may be unrelated, so
            // the name also goes to the crates its path names.
            let suffix = format!("::{name}");
            let exact = !paths.is_empty()
                && (name.starts_with('<')
                    || paths.iter().any(|p| p == name || p.ends_with(&suffix)));
            if !exact {
                missing.push(name.to_string());
            }
            found.extend(paths);
            rest = tail;
        }
        (found, missing)
    }

    /// The raw paths of the `const` items in this doc that `name` can mean.
    fn const_paths(&self, name: &str) -> Vec<String> {
        let last = name.rsplit("::").next().unwrap_or(name);
        let Some(paths) = self.consts.get(last) else {
            return Vec::new();
        };
        // An associated constant, `<X as T>::NAME`, names its impl by type.
        // The impl body has another path. So every `const` item with that
        // last segment counts: a false match only costs a review.
        let qualified = name.starts_with('<');
        let suffix = format!("::{name}");
        paths
            .iter()
            .filter(|p| {
                qualified
                    || p.as_str() == name
                    || p.ends_with(&suffix)
                    || name.ends_with(&format!("::{p}"))
            })
            .cloned()
            .collect()
    }
}

/// The crate roots that a `const` name can come from.
///
/// `dep::LIMIT` has the root `dep`. `<dep::Plan as dep::Limits>::MAX` has
/// the roots of its self type and its trait. A name with no `::`, such as a
/// literal or a local item, has none.
fn const_roots(name: &str) -> Vec<&str> {
    name.strip_prefix('<').map_or_else(
        || first_segment(name).into_iter().collect(),
        |inner| {
            let inner = inner.split_once(">::").map_or(inner, |(head, _)| head);
            let (ty, tr) = inner.split_once(" as ").unwrap_or((inner, ""));
            [first_segment(ty), first_segment(tr)]
                .into_iter()
                .flatten()
                .collect()
        },
    )
}

/// The first segment of `path`, past a leading `&`, `mut` or `dyn`.
fn first_segment(path: &str) -> Option<&str> {
    let path = path
        .trim()
        .trim_start_matches('&')
        .trim_start_matches("mut ")
        .trim_start_matches("dyn ");
    path.split_once("::").map(|(root, _)| root)
}

/// The path after `const `, such as `m::LIMIT` or `<X as T>::LIMIT`.
///
/// A leading `<...>` group is kept whole. The path ends at the first
/// character that cannot be part of it.
fn const_name(tail: &str) -> &str {
    let mut end = 0;
    if tail.starts_with('<') {
        let mut depth = 0_usize;
        for (i, c) in tail.char_indices() {
            match c {
                '<' => depth = depth.saturating_add(1),
                '>' => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0 {
                end = i.saturating_add(c.len_utf8());
                break;
            }
        }
        if end == 0 {
            return "";
        }
    }
    let rest = tail.get(end..).unwrap_or_default();
    let more = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(rest.len());
    tail.get(..end.saturating_add(more))
        .unwrap_or_default()
        .trim_end_matches(':')
}

/// Append the footer of each `allocN` that `text` names, and of each alloc
/// those footers name in turn.
fn append_allocs(doc: &MirDoc, text: &mut String) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    // A stack, so the walk takes the names in order of first use.
    let mut queue = alloc_names(text);
    queue.reverse();
    let mut footers = String::new();
    while let Some(name) = queue.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(footer) = doc.alloc_text.get(&name) {
            let _ = write!(footers, "\n#alloc\n{footer}");
            queue.extend(alloc_names(footer).into_iter().rev());
        }
    }
    text.push_str(&footers);
}

/// Each `allocN` named in `text`, in order of first use.
fn alloc_names(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("alloc") {
        let before = text.len().saturating_sub(rest.len()).saturating_add(at);
        let boundary = before == 0
            || text
                .get(..before)
                .and_then(|s| s.chars().next_back())
                .is_none_or(|p| !(p.is_alphanumeric() || p == '_'));
        let tail = rest.get(at.saturating_add(5)..).unwrap_or_default();
        let digits = tail
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(tail.len());
        if boundary && digits > 0 {
            let name = format!("alloc{}", tail.get(..digits).unwrap_or_default());
            if !out.contains(&name) {
                out.push(name);
            }
        }
        rest = tail;
    }
    out
}

/// Remove source spans and `allocN` numbers from `text`.
///
/// A span is `FILE.rs:L:C: L:C` or `FILE.rs:L:C`. The file name stays, and
/// the numbers go. An `allocN` becomes the static it names, or `alloc`.
///
/// Text inside a string literal is user data, so it stays as it is. MIR
/// escapes a newline in a string literal, so a literal never spans lines.
#[must_use]
pub fn normalize(text: &str, allocs: Option<&BTreeMap<String, String>>) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut in_string = false;
    let mut i = 0;
    while let Some(&c) = chars.get(i) {
        if c == '\n' {
            in_string = false;
        } else if c == '"' && !is_escaped(&chars, i) && !is_char_literal(&chars, i) {
            in_string = !in_string;
        }
        if in_string || c == '"' {
            out.push(c);
            i = i.saturating_add(1);
            continue;
        }
        if c == '.' && starts_with_at(&chars, i, ".rs:") {
            out.push_str(".rs");
            i = skip_span_numbers(&chars, i.saturating_add(3));
            continue;
        }
        if c == 'a'
            && starts_with_at(&chars, i, "alloc")
            && !chars
                .get(i.wrapping_sub(1))
                .is_some_and(|p| p.is_alphanumeric() || *p == '_')
        {
            let start = i.saturating_add(5);
            let mut end = start;
            while chars.get(end).is_some_and(char::is_ascii_digit) {
                end = end.saturating_add(1);
            }
            if end > start {
                let number: String = chars.get(i..end).unwrap_or_default().iter().collect();
                match allocs.and_then(|map| map.get(&number)) {
                    Some(name) => {
                        let _ = write!(out, "alloc(static {name})");
                    }
                    None => out.push_str("alloc"),
                }
                i = end;
                continue;
            }
        }
        out.push(c);
        i = i.saturating_add(1);
    }
    out
}

/// An odd number of backslashes comes before `at`.
fn is_escaped(chars: &[char], at: usize) -> bool {
    let mut count = 0_usize;
    let mut i = at;
    while i > 0 && chars.get(i - 1) == Some(&'\\') {
        count = count.saturating_add(1);
        i -= 1;
    }
    count % 2 == 1
}

/// The quote at `at` is the char literal `'"'`.
fn is_char_literal(chars: &[char], at: usize) -> bool {
    at > 0 && chars.get(at - 1) == Some(&'\'') && chars.get(at.saturating_add(1)) == Some(&'\'')
}

fn starts_with_at(chars: &[char], at: usize, needle: &str) -> bool {
    needle
        .chars()
        .enumerate()
        .all(|(k, n)| chars.get(at.saturating_add(k)) == Some(&n))
}

/// Skip `:L:C` and an optional `: L:C` after a file name.
fn skip_span_numbers(chars: &[char], at: usize) -> usize {
    let mut i = at;
    loop {
        let colon = chars.get(i) == Some(&':');
        let space = chars.get(i.saturating_add(1)) == Some(&' ');
        let digit_at = if space {
            i.saturating_add(2)
        } else {
            i.saturating_add(1)
        };
        if !colon || !chars.get(digit_at).is_some_and(char::is_ascii_digit) {
            return i;
        }
        i = digit_at;
        while chars.get(i).is_some_and(char::is_ascii_digit) {
            i = i.saturating_add(1);
        }
    }
}

// ── loops ───────────────────────────────────────────────────────────────────

/// Labels of the blocks that sit in a cycle of the body's control-flow graph.
///
/// It is Tarjan's strongly connected components, without recursion. A block
/// is cyclic when its component has two or more blocks, or a self edge.
/// Unwind and cleanup edges do not count. Optimized coroutine MIR returns at
/// each suspend point, so an `.await` alone makes no cycle.
fn cyclic_blocks(body: &Body) -> BTreeSet<String> {
    let labels: Vec<&str> = body
        .blocks
        .iter()
        .filter(|b| !b.cleanup)
        .map(|b| b.label.as_str())
        .collect();
    let index: HashMap<&str, usize> = labels.iter().enumerate().map(|(i, l)| (*l, i)).collect();
    let succ: Vec<Vec<usize>> = body
        .blocks
        .iter()
        .filter(|b| !b.cleanup)
        .map(|b| {
            b.terminator
                .successors()
                .into_iter()
                .filter_map(|l| index.get(l).copied())
                .collect()
        })
        .collect();
    let n = labels.len();
    let mut order: Vec<Option<usize>> = vec![None; n];
    let mut low: Vec<usize> = vec![0; n];
    let mut on_stack = vec![false; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut counter = 0_usize;
    let mut out = BTreeSet::new();
    for start in 0..n {
        if order.get(start).copied().flatten().is_some() {
            continue;
        }
        // Each frame is a node and the index of its next successor.
        let mut frames: Vec<(usize, usize)> = vec![(start, 0)];
        while let Some(&mut (v, ref mut next)) = frames.last_mut() {
            if order.get(v).copied().flatten().is_none() {
                if let Some(slot) = order.get_mut(v) {
                    *slot = Some(counter);
                }
                if let Some(slot) = low.get_mut(v) {
                    *slot = counter;
                }
                counter = counter.saturating_add(1);
                stack.push(v);
                if let Some(slot) = on_stack.get_mut(v) {
                    *slot = true;
                }
            }
            let edges = succ.get(v).map_or(&[][..], Vec::as_slice);
            if let Some(&w) = edges.get(*next) {
                *next = next.saturating_add(1);
                match order.get(w).copied().flatten() {
                    None => frames.push((w, 0)),
                    Some(w_order) if on_stack.get(w).copied().unwrap_or(false) => {
                        let lv = low.get(v).copied().unwrap_or(0).min(w_order);
                        if let Some(slot) = low.get_mut(v) {
                            *slot = lv;
                        }
                    }
                    Some(_) => {}
                }
                continue;
            }
            frames.pop();
            if let Some(&(parent, _)) = frames.last() {
                let lp = low
                    .get(parent)
                    .copied()
                    .unwrap_or(0)
                    .min(low.get(v).copied().unwrap_or(0));
                if let Some(slot) = low.get_mut(parent) {
                    *slot = lp;
                }
            }
            if low.get(v) == order.get(v).copied().flatten().as_ref() {
                let mut component = Vec::new();
                while let Some(w) = stack.pop() {
                    if let Some(slot) = on_stack.get_mut(w) {
                        *slot = false;
                    }
                    component.push(w);
                    if w == v {
                        break;
                    }
                }
                let self_edge = succ.get(v).is_some_and(|e| e.contains(&v));
                if component.len() > 1 || self_edge {
                    for w in component {
                        if let Some(label) = labels.get(w) {
                            out.insert((*label).to_string());
                        }
                    }
                }
            }
        }
    }
    out
}

// ── step keys ───────────────────────────────────────────────────────────────

fn key_of_operand(
    markers: &BTreeSet<String>,
    body: &Body,
    operand: &Operand,
    depth: u8,
) -> Option<String> {
    if depth > MAX_KEY_DEPTH {
        return None;
    }
    match operand {
        Operand::Const { text, .. } => str_literal(text),
        Operand::Copy(place) | Operand::Move(place) => {
            key_of_place(markers, body, place, depth.saturating_add(1))
        }
    }
}

/// How a place gets its value.
enum Definition<'b> {
    Assign(&'b crate::mir::ast::Rvalue),
    Call(Option<&'b str>),
}

fn key_of_place(
    markers: &BTreeSet<String>,
    body: &Body,
    place: &Place,
    depth: u8,
) -> Option<String> {
    if depth > MAX_KEY_DEPTH {
        return None;
    }
    let mut definitions = Vec::new();
    for block in &body.blocks {
        for statement in &block.statements {
            let Statement::Assign { dest, rvalue } = statement else {
                continue;
            };
            if dest == place {
                definitions.push(Definition::Assign(rvalue));
            }
            // A `&mut` borrow of the place can write another key into it.
            if rvalue
                .ref_of
                .as_ref()
                .is_some_and(|(referent, mutable)| *mutable && overlaps(referent, place))
            {
                return None;
            }
        }
        if let Terminator::Call { dest, callee, .. } = &block.terminator
            && dest == place
        {
            definitions.push(Definition::Call(callee.as_deref()));
        }
    }
    // Two writes to one place could carry two keys, so only one counts.
    let [definition] = definitions.as_slice() else {
        return None;
    };
    match definition {
        Definition::Assign(rvalue) => {
            if let Some(key) = str_literal(&rvalue.text) {
                return Some(key);
            }
            if let Some((referent, _)) = &rvalue.ref_of {
                return key_of_place(markers, body, referent, depth.saturating_add(1));
            }
            let text = rvalue.text.trim_start();
            let plain = text.starts_with("copy ") || text.starts_with("move ");
            match rvalue.reads.as_slice() {
                [only] if plain => key_of_operand(markers, body, only, depth.saturating_add(1)),
                _ => None,
            }
        }
        Definition::Call(callee) => info_key(markers, (*callee)?),
    }
}

/// One place contains the other: same local, and one projection list is a
/// prefix of the other.
fn overlaps(a: &Place, b: &Place) -> bool {
    a.local == b.local
        && a.projections
            .iter()
            .zip(&b.projections)
            .all(|(x, y)| x == y)
}

/// `const "name"` → `name`.
fn str_literal(text: &str) -> Option<String> {
    let inner = text.trim().strip_prefix("const \"")?.strip_suffix('"')?;
    if inner.contains('\\') {
        return None;
    }
    Some(inner.to_string())
}

/// `X_info()` or a macro marker call → `X`, when the marker for `X` exists.
fn info_key(markers: &BTreeSet<String>, callee: &str) -> Option<String> {
    let bare = strip_generics_everywhere(callee);
    let last = bare.rsplit("::").next().unwrap_or(&bare);
    let name = INFO_MARKERS
        .iter()
        .find_map(|marker| last.strip_prefix(marker))
        .or_else(|| last.strip_suffix("_info"))?;
    markers.contains(name).then(|| name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_drops_spans_and_alloc_numbers() {
        let text =
            "<impl at src/a.rs:19:1: 19:21>::f {closure@src/a.rs:3:9: 3:12} alloc12 myalloc3";
        let mut allocs = BTreeMap::new();
        allocs.insert("alloc12".to_string(), "COUNTER".to_string());
        assert_eq!(
            normalize(text, Some(&allocs)),
            "<impl at src/a.rs>::f {closure@src/a.rs} alloc(static COUNTER) myalloc3"
        );
        assert_eq!(normalize("x.rs:7:2", None), "x.rs");
        assert_eq!(normalize("alloc7 alloc", None), "alloc alloc");
    }

    #[test]
    fn a_string_constant_is_a_key() {
        assert_eq!(str_literal("const \"ship\""), Some("ship".to_string()));
        assert_eq!(str_literal("const 5_u64"), None);
        assert_eq!(str_literal("const \"a\\\"b\""), None);
    }

    #[test]
    fn a_call_on_an_existing_future_is_a_resume() {
        assert!(is_resume(
            "<{async fn body of m::f()} as std::future::Future>::poll"
        ));
        assert!(is_resume(
            "<{async fn body of m::f()} as IntoFuture>::into_future"
        ));
        assert!(is_resume(
            "Pin::<&mut {async fn body of m::f()}>::new_unchecked"
        ));
        assert!(!is_resume("m::f"));
        // A helper that takes a future is a real start of that helper.
        assert!(!is_resume("guarded::<{async fn body of charge()}>"));
    }

    #[test]
    fn a_string_literal_keeps_span_like_text() {
        assert_eq!(
            normalize("_1 = const \"msg.rs:7:2 alloc3\"; // at a.rs:1:2", None),
            "_1 = const \"msg.rs:7:2 alloc3\"; // at a.rs"
        );
        // An escaped quote does not end the literal, and a char quote does
        // not start one.
        assert_eq!(
            normalize("const \"a\\\"b.rs:1:2\" const '\"' x.rs:3:4", None),
            "const \"a\\\"b.rs:1:2\" const '\"' x.rs"
        );
    }

    #[test]
    fn a_const_name_keeps_a_qualified_self_type() {
        assert_eq!(const_name("<X as T>::LIMIT) -> x"), "<X as T>::LIMIT");
        assert_eq!(const_name("m::LIMIT, y"), "m::LIMIT");
        assert_eq!(const_name("<X as T<u8>>::N;"), "<X as T<u8>>::N");
    }

    #[test]
    fn alloc_names_are_found_in_order_of_first_use() {
        assert_eq!(
            alloc_names("const {alloc3: &u8} x alloc12 alloc3 myalloc4"),
            ["alloc3", "alloc12"]
        );
    }
}
