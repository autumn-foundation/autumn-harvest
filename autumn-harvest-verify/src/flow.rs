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
        for event in escapes(body, block) {
            chain.push(push(&mut nodes, &block.label, event));
        }
        if let Some(outcome) = exits.get(block.label.as_str()) {
            let event = FlowEvent::Exit { outcome: *outcome };
            chain.push(push(&mut nodes, &block.label, event));
        } else if let Some(mut event) = block_event(block, facts) {
            let pair = match &mut event {
                FlowEvent::SagaStep { tracked, .. } => {
                    let pair = await_try_arms(&blocks, block);
                    *tracked = pair.is_some();
                    pair
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
fn block_event(block: &BasicBlock, facts: &BlockFacts) -> Option<FlowEvent> {
    let label = &block.label;
    if let Terminator::Call {
        callee: Some(callee),
        ..
    } = &block.terminator
    {
        match saga_method(callee) {
            Some("new") => return Some(FlowEvent::SagaNew),
            Some("compensate_all") => return Some(FlowEvent::SagaCompensate),
            Some("step") => {
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

/// The method name when `callee` is a method of `Saga`.
fn saga_method(callee: &str) -> Option<&'static str> {
    let bare = strip_generics_everywhere(callee);
    let mut segments = bare.rsplit("::");
    let method = segments.next()?;
    let owner = segments.next()?;
    if owner != "Saga" {
        return None;
    }
    ["new", "step", "compensate_all"]
        .into_iter()
        .find(|m| *m == method)
        .or(Some("other"))
}

/// A type whose head is `Saga`, behind any references.
fn is_saga_type(ty: &str) -> bool {
    let ty = peel_refs(ty).trim().trim_start_matches("mut ").trim();
    let head = ty.split('<').next().unwrap_or(ty);
    !head.starts_with('{') && (head == "Saga" || head.ends_with("::Saga"))
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
fn escapes(body: &Body, block: &BasicBlock) -> Vec<FlowEvent> {
    let mut out = Vec::new();
    for statement in &block.statements {
        let Statement::Assign { dest, rvalue } = statement else {
            continue;
        };
        let reads_saga = rvalue
            .reads
            .iter()
            .filter_map(operand_place)
            .chain(rvalue.ref_of.as_ref().map(|(place, _)| place))
            .any(|place| saga_local(body, place));
        if reads_saga && !saga_local(body, dest) {
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
        let kept = saga_method(callee).is_some()
            || bare.ends_with("drop_in_place")
            || (saga_local(body, dest) && is_reborrow(&bare));
        if passes_saga && !kept {
            out.push(FlowEvent::SagaEscape { to: bare });
        }
    }
    out
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
    let text = text.trim();
    let head = text.split('(').next().unwrap_or(text);
    let bare = strip_generics_everywhere(head);
    if bare == "Ok" || bare.ends_with("Result::Ok") {
        ExitOutcome::Ok
    } else if bare == "Err" || bare.ends_with("Result::Err") {
        ExitOutcome::Err
    } else {
        ExitOutcome::Unknown
    }
}

// ── `.await?` after `Saga::step` ────────────────────────────────────────────

/// The `Continue` and `Break` targets of the `?` that reads the result of the
/// `Saga::step` call in `block`.
///
/// The walk accepts only await plumbing: `into_future`, the `Pin`
/// constructors, `poll`, gotos and drops. After `poll`, it takes the
/// `Ready` arm, case `0`. After `Try::branch`, case `0` is `Continue` and
/// case `1` is `Break`. Any other shape returns `None`, so a call such as
/// `and_then` cannot turn a later failure into a step failure.
fn await_try_arms(
    blocks: &HashMap<&str, &BasicBlock>,
    block: &BasicBlock,
) -> Option<(String, String)> {
    let Terminator::Call {
        callee: Some(callee),
        target: Some(first),
        ..
    } = &block.terminator
    else {
        return None;
    };
    if saga_method(callee) != Some("step") {
        return None;
    }
    let mut at: &str = first;
    let mut polled = false;
    let mut branched = false;
    for _ in 0..MAX_AWAIT_WALK {
        let block = blocks.get(at)?;
        match &block.terminator {
            Terminator::Goto { target } | Terminator::Drop { target, .. } => at = target,
            Terminator::Call {
                callee: Some(callee),
                target: Some(target),
                ..
            } => {
                let bare = strip_generics_everywhere(callee);
                let last = bare.rsplit("::").next().unwrap_or(&bare);
                let pin = bare.starts_with("Pin::") || bare.contains("pin::Pin::");
                match last {
                    "poll" => polled = true,
                    "branch" if polled => branched = true,
                    "into_future" => {}
                    "new_unchecked" | "new" if pin => {}
                    _ => return None,
                }
                at = target;
            }
            Terminator::SwitchInt {
                targets, values, ..
            } => {
                let case = |value: &str| {
                    values
                        .iter()
                        .position(|v| v == value)
                        .and_then(|i| targets.get(i))
                        .cloned()
                };
                if branched {
                    return Some((case("0")?, case("1")?));
                }
                if !polled {
                    return None;
                }
                at = blocks.get(case("0")?.as_str()).map(|b| b.label.as_str())?;
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
            Some("other")
        );
        assert_eq!(saga_method("Sagas::new"), None);
        assert_eq!(saga_method("new"), None);
    }

    #[test]
    fn a_saga_type_is_found_behind_references() {
        assert!(is_saga_type("&mut Saga<'_>"));
        assert!(is_saga_type("autumn_harvest::Saga<'_>"));
        assert!(!is_saga_type(
            "{async fn body of autumn_harvest::Saga<'_>::step<u64>()}"
        ));
        assert!(!is_saga_type("Pin<&mut Saga<'_>>"));
        assert!(!is_saga_type("MySaga<'_>"));
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
        assert_eq!(literal_outcome("move _5"), ExitOutcome::Unknown);
        assert_eq!(
            literal_outcome("Option::<u8>::Some(const 1_u8)"),
            ExitOutcome::Unknown
        );
    }
}
