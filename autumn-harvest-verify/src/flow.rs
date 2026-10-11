//! The flow graph of one body (issue #2010).
//!
//! A flow graph keeps only the events of a body: steps, calls into the graph,
//! handler registrations, saga operations and exits. An edge joins two
//! events when a control-flow path joins them with no other event on it.
//!
//! The graph is built from optimized MIR. Three facts about that MIR shape it:
//!
//! - A coroutine starts with a `switchInt` on its state. State `0` is the
//!   first entry. Each other state resumes inside a poll loop that state `0`
//!   already reaches. So the entry is the target of state `0`.
//! - A suspend point returns `Poll::Pending`. That `return` is not an exit.
//! - `?` lowers to `Try::branch`, a `switchInt` on its result, and
//!   `from_residual` on the `Break` arm.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::mir::ast::{BasicBlock, Body, Operand, Place, Statement, Terminator};
use crate::structure::{EdgeLabel, ExitOutcome, FlowEdge, FlowEvent, FlowGraph, FlowNode};
use crate::util::{peel_refs, strip_generics_everywhere};

/// The longest chain of blocks the `.await?` walk after `Saga::step` follows.
const MAX_AWAIT_WALK: usize = 64;

/// What the structure builder already knows about each block of a body.
#[derive(Debug, Default)]
pub struct BlockFacts {
    /// Block → the index of its step site in the sorted step list.
    pub steps: BTreeMap<String, usize>,
    /// Block → the bodies of the graph its call starts.
    pub calls: BTreeMap<String, Vec<String>>,
    /// Block → the index of its handler in the workflow handler list.
    pub handlers: BTreeMap<String, usize>,
    /// `(block, argument index)` → the bodies passed there.
    pub arguments: BTreeMap<(String, usize), Vec<String>>,
    /// Blocks whose call resolves to a body here outside the engine crate.
    /// Such a call is never an engine `Saga` method.
    pub bodied: BTreeSet<String>,
}

/// Build the flow graph of `body`.
pub fn build(body: &Body, facts: &BlockFacts) -> FlowGraph {
    let blocks: HashMap<&str, &BasicBlock> =
        body.blocks.iter().map(|b| (b.label.as_str(), b)).collect();
    let exits = exit_outcomes(body);

    // Each block holds a chain of nodes, in order.
    let mut nodes: Vec<FlowNode> = vec![FlowNode {
        at: entry_block(body).unwrap_or_default(),
        event: FlowEvent::Entry,
    }];
    let mut chains: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    let mut arms: BTreeMap<usize, (String, String)> = BTreeMap::new();
    for block in body.blocks.iter().filter(|b| !b.cleanup) {
        let mut chain = Vec::new();
        for event in escapes(body, block, facts) {
            chain.push(push(&mut nodes, &block.label, event));
        }
        if let Some(outcome) = exits.get(block.label.as_str()) {
            let event = FlowEvent::Exit { outcome: *outcome };
            chain.push(push(&mut nodes, &block.label, event));
        } else if let Some(mut event) = block_event(body, block, facts) {
            let pair = match &mut event {
                FlowEvent::SagaStep { tracked, .. } => {
                    let pair = match await_walk(&blocks, block, true) {
                        Some(Awaited::Arms(ok, err)) => Some((ok, err)),
                        _ => None,
                    };
                    *tracked = pair.is_some();
                    pair
                }
                FlowEvent::SagaCompensate { tracked } => {
                    *tracked = await_walk(&blocks, block, false).is_some();
                    None
                }
                _ => None,
            };
            let id = push(&mut nodes, &block.label, event);
            if let Some(pair) = pair {
                arms.insert(id, pair);
            }
            chain.push(id);
        }
        if !chain.is_empty() {
            chains.insert(block.label.as_str(), chain);
        }
    }

    // An arm that reaches no node, such as an arm that never returns, gives
    // no labeled edge. The step is then untracked, so a checker can require
    // both arms of every tracked step.
    arms.retain(|id, (ok, err)| {
        let both = !reach_nodes(&blocks, &chains, ok).is_empty()
            && !reach_nodes(&blocks, &chains, err).is_empty();
        if !both
            && let Some(FlowEvent::SagaStep { tracked, .. }) =
                nodes.get_mut(*id).map(|n| &mut n.event)
        {
            *tracked = false;
        }
        both
    });

    let mut edges: BTreeSet<FlowEdge> = BTreeSet::new();
    let reach =
        |start: &str, from: usize, label: Option<EdgeLabel>, edges: &mut BTreeSet<FlowEdge>| {
            for to in reach_nodes(&blocks, &chains, start) {
                edges.insert(FlowEdge { from, to, label });
            }
        };
    if let Some(start) = entry_block(body) {
        reach(&start, 0, None, &mut edges);
    }
    for (label, chain) in &chains {
        for pair in chain.windows(2) {
            if let [from, to] = pair {
                edges.insert(FlowEdge {
                    from: *from,
                    to: *to,
                    label: None,
                });
            }
        }
        let Some(&last) = chain.last() else {
            continue;
        };
        if matches!(
            nodes.get(last).map(|n| &n.event),
            Some(FlowEvent::Exit { .. })
        ) {
            continue;
        }
        if let Some((ok, err)) = arms.get(&last) {
            reach(ok, last, Some(EdgeLabel::Ok), &mut edges);
            reach(err, last, Some(EdgeLabel::Err), &mut edges);
            continue;
        }
        let Some(block) = blocks.get(label) else {
            continue;
        };
        for next in block.terminator.successors() {
            reach(next, last, None, &mut edges);
        }
    }
    FlowGraph {
        nodes,
        edges: edges.into_iter().collect(),
    }
}

fn push(nodes: &mut Vec<FlowNode>, at: &str, event: FlowEvent) -> usize {
    nodes.push(FlowNode {
        at: at.to_string(),
        event,
    });
    nodes.len().saturating_sub(1)
}

/// The first node of each chain that `start` reaches with no other node on
/// the path. `start` itself counts.
fn reach_nodes(
    blocks: &HashMap<&str, &BasicBlock>,
    chains: &BTreeMap<&str, Vec<usize>>,
    start: &str,
) -> BTreeSet<usize> {
    let mut out = BTreeSet::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut queue: Vec<&str> = vec![start];
    while let Some(label) = queue.pop() {
        if !seen.insert(label) {
            continue;
        }
        if let Some(first) = chains.get(label).and_then(|c| c.first()) {
            out.insert(*first);
            continue;
        }
        let Some(block) = blocks.get(label) else {
            continue;
        };
        if block.cleanup {
            continue;
        }
        queue.extend(block.terminator.successors());
    }
    out
}

/// The block where the body starts. For a coroutine, the target of state `0`.
fn entry_block(body: &Body) -> Option<String> {
    let first = body.blocks.first()?;
    if is_coroutine(body)
        && let Terminator::SwitchInt {
            targets, values, ..
        } = &first.terminator
        && let Some(at) = values.iter().position(|v| v == "0")
    {
        return targets.get(at).cloned();
    }
    Some(first.label.clone())
}

/// The body is the resume function of a coroutine.
fn is_coroutine(body: &Body) -> bool {
    body.params
        .first()
        .is_some_and(|(_, ty)| ty.contains("{async") || ty.contains("{coroutine"))
}

// ── events ──────────────────────────────────────────────────────────────────

/// The event of the terminator of `block`, if it has one.
fn block_event(body: &Body, block: &BasicBlock, facts: &BlockFacts) -> Option<FlowEvent> {
    let label = &block.label;
    if let Terminator::Call {
        callee: Some(callee),
        dest,
        dest_ty,
        ..
    } = &block.terminator
    {
        // MIR trims the callee to `Saga::<'_>::step`, but it declares each
        // local with its full path. So the engine type is read from the
        // saga operand, or from the value a call returns.
        let self_saga = args_saga(body, &block.terminator);
        let ty = dest_ty
            .as_deref()
            .or_else(|| body.locals.get(&dest.local).map(String::as_str))
            .unwrap_or_default();
        let returns_saga = !ty.trim_start().starts_with('&') && is_saga_type(ty);
        match engine_method(callee, label, facts) {
            Some("compensate_all") if self_saga => {
                return Some(FlowEvent::SagaCompensate { tracked: false });
            }
            Some("step") if self_saga => {
                let bodies = |index: usize| {
                    facts
                        .arguments
                        .get(&(label.clone(), index))
                        .cloned()
                        .unwrap_or_default()
                };
                // Operand 0 is the saga, 1 the forward step, 2 its compensation.
                return Some(FlowEvent::SagaStep {
                    forward: bodies(1),
                    compensate: bodies(2),
                    tracked: false,
                });
            }
            // `Saga::new`, or another call that returns a saga value, such
            // as a helper that builds one, brings a new saga into this body.
            _ if returns_saga => return Some(FlowEvent::SagaNew),
            _ => {}
        }
    }
    if let Some(&handler) = facts.handlers.get(label) {
        return Some(FlowEvent::Handler { handler });
    }
    if let Some(&step) = facts.steps.get(label) {
        return Some(FlowEvent::Step { step });
    }
    facts
        .calls
        .get(label)
        .filter(|callees| !callees.is_empty())
        .map(|callees| FlowEvent::Call {
            callees: callees.clone(),
        })
}

/// The public methods of the engine's `Saga`.
const SAGA_METHODS: [&str; 5] = [
    "new",
    "step",
    "compensate_all",
    "context",
    "pending_compensation_count",
];

/// The method name when `callee` names one of [`SAGA_METHODS`] on a type
/// named `Saga`. The caller still checks the operand or result type, because
/// a workflow crate can have its own `Saga`.
///
/// MIR trims the engine path to `Saga`, or prints it from `autumn_harvest`.
/// Any other module path, such as `other::Saga`, names a local type.
fn saga_method(callee: &str) -> Option<&'static str> {
    let bare = strip_generics_everywhere(callee);
    let (owner, method) = bare.rsplit_once("::")?;
    let module = owner.strip_suffix("Saga")?;
    if !(module.is_empty() || module.starts_with("autumn_harvest::")) {
        return None;
    }
    SAGA_METHODS.into_iter().find(|m| *m == method)
}

/// [`saga_method`] for the call in block `label`, when it is an engine call.
///
/// A crate-root type named `Saga` prints as bare `Saga`, as the trimmed
/// engine type does. Its call resolves to a body outside the engine crate,
/// so it is excluded.
fn engine_method(callee: &str, label: &str, facts: &BlockFacts) -> Option<&'static str> {
    saga_method(callee).filter(|_| !facts.bodied.contains(label))
}

/// The engine's `Saga` type, behind any references.
///
/// The path must start at the `autumn_harvest` crate, as the model's trust
/// row does. A workflow crate's own type named `Saga` is not the engine's.
fn is_saga_type(ty: &str) -> bool {
    let ty = peel_refs(ty).trim().trim_start_matches("mut ").trim();
    let head = ty.split('<').next().unwrap_or(ty);
    head.starts_with("autumn_harvest::") && head.ends_with("::Saga")
}

/// Operand 0 of the call is the engine's `Saga`, by reference or by value.
fn args_saga(body: &Body, terminator: &Terminator) -> bool {
    let Terminator::Call { args, .. } = terminator else {
        return false;
    };
    args.first()
        .and_then(operand_place)
        .is_some_and(|place| saga_local(body, place))
}

fn saga_local(body: &Body, place: &Place) -> bool {
    body.locals
        .get(&place.local)
        .is_some_and(|ty| is_saga_type(ty))
}

const fn operand_place(operand: &Operand) -> Option<&Place> {
    match operand {
        Operand::Copy(place) | Operand::Move(place) => Some(place),
        Operand::Const { .. } => None,
    }
}

/// Each place in `block` where a `Saga` value leaves the saga API.
///
/// A copy, move or reborrow into another `Saga` local keeps it in view. A
/// call to a `Saga` method or a drop keeps it too. Any other use, such as a
/// call argument or a closure capture, is an escape.
fn escapes(body: &Body, block: &BasicBlock, facts: &BlockFacts) -> Vec<FlowEvent> {
    let mut out = Vec::new();
    for statement in &block.statements {
        let Statement::Assign { rvalue, .. } = statement else {
            continue;
        };
        let reads_saga = rvalue
            .reads
            .iter()
            .filter_map(operand_place)
            .chain(rvalue.ref_of.as_ref().map(|(place, _)| place))
            .any(|place| saga_local(body, place))
            || annotates_a_saga(&rvalue.text);
        // A move, copy or borrow hands the saga to another binding of this
        // body. Any other read, such as a closure or `async` block capture,
        // takes it out of view.
        let text = rvalue.text.trim_start();
        let keeps = rvalue.ref_of.is_some()
            || ((text.starts_with("move ") || text.starts_with("copy "))
                && rvalue.reads.len() == 1);
        if reads_saga && !keeps {
            out.push(FlowEvent::SagaEscape {
                to: rvalue.text.trim().to_string(),
            });
        }
    }
    if let Terminator::Call {
        callee, args, dest, ..
    } = &block.terminator
    {
        let passes_saga = args
            .iter()
            .filter_map(operand_place)
            .any(|place| saga_local(body, place));
        let callee = callee.as_deref().unwrap_or("<indirect call>");
        let bare = strip_generics_everywhere(callee);
        // A saga stays in view only through an engine method that takes it
        // as operand 0, such as `step`. Any other callee can keep it.
        let saga_call = engine_method(callee, &block.label, facts).is_some()
            && args_saga(body, &block.terminator);
        let kept = saga_call
            || bare.ends_with("drop_in_place")
            || (saga_local(body, dest) && is_reborrow(&bare));
        if passes_saga && !kept {
            out.push(FlowEvent::SagaEscape { to: bare });
        }
    }
    out
}

/// `text` reads a place whose printed type is `Saga`, such as
/// `move (((*_9) as variant#3).1: Saga<'_>)`. A place in a coroutine state
/// has no local of its own, so only this annotation shows its type.
fn annotates_a_saga(text: &str) -> bool {
    text.match_indices(": ").any(|(at, _)| {
        let ty = text.get(at.saturating_add(2)..).unwrap_or_default();
        let end = ty.find([')', ',', '}']).unwrap_or(ty.len());
        is_saga_type(ty.get(..end).unwrap_or_default())
    })
}

/// A std call that hands back the same place, such as `deref_mut`.
fn is_reborrow(bare: &str) -> bool {
    [
        "deref",
        "deref_mut",
        "borrow",
        "borrow_mut",
        "as_mut",
        "as_ref",
    ]
    .iter()
    .any(|m| bare.ends_with(&format!("::{m}")))
}

// ── exits ───────────────────────────────────────────────────────────────────

/// Block → the outcome of the last write of the returned value in it.
///
/// A coroutine returns `Poll::Ready(x)`, so the value is each such `x`.
/// Another body returns `_0`. Only a body that returns a `Result` has exits.
fn exit_outcomes(body: &Body) -> BTreeMap<&str, ExitOutcome> {
    let mut out = BTreeMap::new();
    if !body.return_ty.contains("Result<") {
        return out;
    }
    let coroutine = body.return_ty.trim_start().starts_with("Poll<")
        || body.return_ty.trim_start().starts_with("std::task::Poll<");
    let mut returned: BTreeSet<Place> = BTreeSet::new();
    if coroutine {
        for block in &body.blocks {
            for statement in &block.statements {
                if let Statement::Assign { dest, rvalue } = statement
                    && dest.local.0 == 0
                    && dest.projections.is_empty()
                    && rvalue.text.contains("::Ready(")
                    && let Some(place) = rvalue.reads.first().and_then(operand_place)
                {
                    returned.insert(place.clone());
                }
            }
        }
    } else {
        returned.insert(Place {
            local: crate::mir::ast::Local(0),
            projections: Vec::new(),
        });
    }
    for block in body.blocks.iter().filter(|b| !b.cleanup) {
        let mut last = None;
        for statement in &block.statements {
            if let Statement::Assign { dest, rvalue } = statement
                && returned.contains(dest)
            {
                last = Some(literal_outcome(&rvalue.text));
            }
        }
        if let Terminator::Call { dest, callee, .. } = &block.terminator
            && returned.contains(dest)
        {
            let residual = callee
                .as_deref()
                .is_some_and(|c| c.contains("from_residual"));
            last = Some(if residual {
                ExitOutcome::Err
            } else {
                ExitOutcome::Unknown
            });
        }
        if let Some(outcome) = last {
            out.insert(block.label.as_str(), outcome);
        }
    }
    out
}

/// `Result::<T, E>::Ok(..)` is `ok`, `Result::<T, E>::Err(..)` is `err`.
fn literal_outcome(text: &str) -> ExitOutcome {
    // Generics can hold a `(`, as `Result::<(), E>::Ok(..)` does. So they go
    // before the cut at the argument list.
    let bare = strip_generics_everywhere(text.trim());
    let bare = bare.split('(').next().unwrap_or(&bare).trim();
    if bare == "Ok" || bare.ends_with("Result::Ok") {
        ExitOutcome::Ok
    } else if bare == "Err" || bare.ends_with("Result::Err") {
        ExitOutcome::Err
    } else {
        ExitOutcome::Unknown
    }
}

// ── `.await` after a saga call ─────────────────────────────────────────────

/// What the walk after a saga call found.
enum Awaited {
    /// The future reached its `Ready` arm.
    Ready,
    /// The `Continue` and `Break` targets of the `?` that reads the result.
    Arms(String, String),
}

/// The value flow of one `.await` and its `?`, as the walk sees it.
#[derive(Default)]
struct AwaitState {
    /// Places that hold the saga future, or a pin or borrow of it.
    future: BTreeSet<Place>,
    /// The result of `poll` on that future.
    polled: Option<Place>,
    /// Locals that hold the `Ready` payload of that result.
    ready: BTreeSet<Place>,
    /// The result of `Try::branch` on that payload.
    branched: Option<Place>,
    /// The locals that hold the discriminant of `polled` and of `branched`.
    poll_discriminant: Option<Place>,
    branch_discriminant: Option<Place>,
}

impl AwaitState {
    fn holds_future(&self, operand: Option<&Operand>) -> bool {
        operand
            .and_then(operand_place)
            .is_some_and(|place| self.future.contains(place))
    }

    /// Follow the statements of one block.
    fn read(&mut self, block: &BasicBlock) {
        for statement in &block.statements {
            let Statement::Assign { dest, rvalue } = statement else {
                continue;
            };
            if let Some(of) = &rvalue.discriminant_of {
                if self.polled.as_ref() == Some(of) {
                    self.poll_discriminant = Some(dest.clone());
                }
                if self.branched.as_ref() == Some(of) {
                    self.branch_discriminant = Some(dest.clone());
                }
                continue;
            }
            if let Some((referent, _)) = &rvalue.ref_of {
                if self.future.contains(referent) {
                    self.future.insert(dest.clone());
                }
                continue;
            }
            let text = rvalue.text.trim_start();
            if !(text.starts_with("move ") || text.starts_with("copy ")) {
                continue;
            }
            let Some(read) = rvalue.reads.first().and_then(operand_place) else {
                continue;
            };
            let payload = self
                .polled
                .as_ref()
                .is_some_and(|p| p.local == read.local && !read.projections.is_empty());
            if self.future.contains(read) {
                self.future.insert(dest.clone());
            } else if payload || self.ready.contains(read) {
                self.ready.insert(dest.clone());
            }
        }
    }
}

/// Follow the `.await` of the saga call in `block`, and its `?` when
/// `want_arms` is set.
///
/// The walk follows the value, not only the control flow. The call result
/// must reach `poll` through `into_future`, the `Pin` constructors, moves
/// and borrows. The `switchInt` after `poll` must read the discriminant of
/// that `poll` result, and case `0` is `Ready`. `Try::branch` must read the
/// `Ready` payload. Its `switchInt` gives case `0` for `Continue` and case
/// `1` for `Break`. Any other shape returns `None`. So a call such as
/// `map_err`, or a `?` on another value, cannot pass for the step result.
fn await_walk(
    blocks: &HashMap<&str, &BasicBlock>,
    block: &BasicBlock,
    want_arms: bool,
) -> Option<Awaited> {
    let Terminator::Call {
        dest,
        target: Some(first),
        ..
    } = &block.terminator
    else {
        return None;
    };
    let mut state = AwaitState::default();
    state.future.insert(dest.clone());
    let mut at: &str = first;
    for _ in 0..MAX_AWAIT_WALK {
        let block = blocks.get(at)?;
        state.read(block);
        match &block.terminator {
            Terminator::Goto { target } | Terminator::Drop { target, .. } => at = target,
            Terminator::Call {
                callee: Some(callee),
                args,
                dest,
                target: Some(target),
                ..
            } => {
                let bare = strip_generics_everywhere(callee);
                let last = bare.rsplit("::").next().unwrap_or(&bare);
                let pin = bare.starts_with("Pin::") || bare.contains("pin::Pin::");
                let first_arg = args.first();
                match last {
                    "into_future" if state.holds_future(first_arg) => {
                        state.future.insert(dest.clone());
                    }
                    "new_unchecked" | "new" if pin && state.holds_future(first_arg) => {
                        state.future.insert(dest.clone());
                    }
                    "poll" if state.holds_future(first_arg) => {
                        state.polled = Some(dest.clone());
                    }
                    "branch"
                        if first_arg
                            .and_then(operand_place)
                            .is_some_and(|p| state.ready.contains(p)) =>
                    {
                        state.branched = Some(dest.clone());
                    }
                    _ => return None,
                }
                at = target;
            }
            Terminator::SwitchInt {
                operand,
                targets,
                values,
            } => {
                let case = |value: &str| {
                    values
                        .iter()
                        .position(|v| v == value)
                        .and_then(|i| targets.get(i))
                        .cloned()
                };
                let on = operand_place(operand);
                if on.is_some() && on == state.branch_discriminant.as_ref() {
                    return Some(Awaited::Arms(case("0")?, case("1")?));
                }
                if on.is_none() || on != state.poll_discriminant.as_ref() {
                    return None;
                }
                if !want_arms {
                    return Some(Awaited::Ready);
                }
                let ready = case("0")?;
                at = blocks.get(ready.as_str()).map(|b| b.label.as_str())?;
            }
            _ => return None,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saga_method_is_found_by_its_last_two_segments() {
        assert_eq!(saga_method("Saga::<'_>::new"), Some("new"));
        assert_eq!(
            saga_method("autumn_harvest::saga::Saga::<'_>::step::<u64, F, G>"),
            Some("step")
        );
        assert_eq!(
            saga_method("Saga::<'_>::compensate_all"),
            Some("compensate_all")
        );
        assert_eq!(
            saga_method("Saga::<'_>::pending_compensation_count"),
            Some("pending_compensation_count")
        );
        assert_eq!(saga_method("own::Saga::consume"), None);
        assert_eq!(saga_method("other::Saga::compensate_all"), None);
        assert_eq!(saga_method("MySaga::new"), None);
        assert_eq!(saga_method("Sagas::new"), None);
        assert_eq!(saga_method("new"), None);
    }

    #[test]
    fn a_saga_type_is_found_behind_references() {
        assert!(is_saga_type("&mut autumn_harvest::Saga<'_>"));
        assert!(is_saga_type("autumn_harvest::Saga<'_>"));
        assert!(is_saga_type("autumn_harvest::saga::Saga<'_>"));
        assert!(!is_saga_type("&mut Saga<'_>"));
        assert!(!is_saga_type("own::Saga"));
        assert!(!is_saga_type(
            "{async fn body of autumn_harvest::Saga<'_>::step<u64>()}"
        ));
        assert!(!is_saga_type("Pin<&mut Saga<'_>>"));
        assert!(!is_saga_type("MySaga<'_>"));
    }

    #[test]
    fn a_saga_annotation_is_found_in_a_place_type() {
        assert!(annotates_a_saga(
            "{coroutine} { s: move (((*_4) as variant#5).1: autumn_harvest::Saga<'_>) }"
        ));
        assert!(!annotates_a_saga(
            "move (((*_4) as variant#3).2: {async fn body of Saga<'_>::step<u64>()})"
        ));
    }

    #[test]
    fn a_literal_result_has_an_outcome() {
        assert_eq!(
            literal_outcome("Result::<u64, String>::Ok(copy _3)"),
            ExitOutcome::Ok
        );
        assert_eq!(
            literal_outcome("std::result::Result::<u64, String>::Err(move _4)"),
            ExitOutcome::Err
        );
        assert_eq!(
            literal_outcome("Result::<(), E>::Ok(const ())"),
            ExitOutcome::Ok
        );
        assert_eq!(literal_outcome("move _5"), ExitOutcome::Unknown);
        assert_eq!(
            literal_outcome("Option::<u8>::Some(const 1_u8)"),
            ExitOutcome::Unknown
        );
    }
}
