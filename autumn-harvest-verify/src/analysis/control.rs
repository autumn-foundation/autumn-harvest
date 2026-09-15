//! Control dependence: which command emissions a tainted branch decides (D5).
//!
//! A sink that is *reached only when a non-deterministic condition holds* is a
//! determinism bug even when every argument it receives is a constant — the
//! recorded history has a different length, or a different order, on a replay.
//! The corpus pins three cases on this: `wf_atomic_shard_pick` (which of three
//! activities), `wf_order_dependence_which_first` (the same two activities in
//! the other order) and `wf_conditional_command_emission` (one activity, or
//! none).
//!
//! The rule is the textbook one and *not* "every sink after the branch":
//! block `B` is control-dependent on branch `S` when some successor of `S`
//! post-dominates `B`, and `B` does **not** post-dominate `S`. That second
//! clause is what keeps the unconditional `primary` activity in
//! `wf_conditional_command_emission` out of the finding, and what stops every
//! `?` from making the rest of a workflow control-dependent.
//!
//! Two MIR-specific rules keep the false-positive rate survivable, both
//! enforced by the *taint* of the switch operand rather than here:
//! `switchInt(discriminant(P))` reads `P` exactly (never through an extension),
//! so an `async` coroutine's own resume-state switch and rustc's drop flags are
//! clean by construction. Unwind edges are excluded from the CFG entirely:
//! every call can unwind, and a panic path is not a decision the workflow made.

use crate::mir::ast::Body;

/// The CFG of one body, with post-dominance and dominance precomputed.
#[derive(Debug)]
pub struct ControlGraph {
    labels: Vec<String>,
    successors: Vec<Vec<usize>>,
    /// `post_dominates[a][b]` — block `a` post-dominates block `b`.
    post_dominates: Vec<Vec<bool>>,
    /// `dominates[a][b]` — block `a` dominates block `b`: every path from the
    /// entry (`bb0`) to `b` passes through `a`.
    dominates: Vec<Vec<bool>>,
    reachable: Vec<bool>,
}

impl ControlGraph {
    /// Build the graph for `body` (unwind edges and cleanup blocks excluded).
    #[must_use]
    pub fn new(body: &Body) -> Self {
        let labels: Vec<String> = body.blocks.iter().map(|b| b.label.clone()).collect();
        let count = labels.len();
        let index = |label: &str| labels.iter().position(|l| l == label);
        let mut successors: Vec<Vec<usize>> = Vec::with_capacity(count);
        for block in &body.blocks {
            let mut targets: Vec<usize> = block
                .terminator
                .successors()
                .into_iter()
                .filter_map(index)
                .filter(|at| body.blocks.get(*at).is_some_and(|b| !b.cleanup))
                .collect();
            targets.sort_unstable();
            targets.dedup();
            successors.push(targets);
        }

        let mut reachable = vec![false; count];
        if count > 0 {
            let mut stack = vec![0usize];
            reachable[0] = true;
            while let Some(at) = stack.pop() {
                for &next in successors.get(at).into_iter().flatten() {
                    if let Some(seen) = reachable.get_mut(next)
                        && !*seen
                    {
                        *seen = true;
                        stack.push(next);
                    }
                }
            }
        }
        for (at, block) in body.blocks.iter().enumerate() {
            if block.cleanup
                && let Some(slot) = reachable.get_mut(at)
            {
                *slot = false;
            }
        }

        let post_dominates = post_dominance(&successors, &reachable);
        let dominates = dominance(&successors, &reachable);
        Self {
            labels,
            successors,
            post_dominates,
            dominates,
            reachable,
        }
    }

    /// Index of `label` in this graph.
    #[must_use]
    pub fn index_of(&self, label: &str) -> Option<usize> {
        self.labels.iter().position(|l| l == label)
    }

    /// True when the block is reachable from the entry and is not a cleanup block.
    #[must_use]
    pub fn is_live(&self, at: usize) -> bool {
        self.reachable.get(at).copied().unwrap_or(false)
    }

    /// True when `block` is control-dependent on the branch at `branch`.
    #[must_use]
    pub fn is_control_dependent(&self, block: usize, branch: usize) -> bool {
        let Some(targets) = self.successors.get(branch) else {
            return false;
        };
        if targets.len() < 2 || !self.is_live(block) || !self.is_live(branch) {
            return false;
        }
        if self.post_dominates(block, branch) {
            return false;
        }
        targets
            .iter()
            .any(|&target| self.post_dominates(block, target))
    }

    fn post_dominates(&self, a: usize, b: usize) -> bool {
        self.post_dominates
            .get(a)
            .and_then(|row| row.get(b))
            .copied()
            .unwrap_or(false)
    }

    /// True when `a` dominates `b`: every path from the entry to `b` passes
    /// through `a` (every block dominates itself). A caller uses this to
    /// decide whether a sanitizer kill applies to a read elsewhere in the
    /// body. A read `a` does not dominate could have executed on a path
    /// that never reached the sanitizer at all.
    #[must_use]
    pub fn dominates(&self, a: usize, b: usize) -> bool {
        self.dominates
            .get(a)
            .and_then(|row| row.get(b))
            .copied()
            .unwrap_or(false)
    }
}

/// Iterative post-dominance over the reverse CFG with one virtual exit.
///
/// A block that cannot reach the exit at all (an unconditional loop, or a chain
/// that only ends in `unreachable`) post-dominates nothing but itself — the
/// conservative direction, since "does not post-dominate" is what *adds* a
/// finding and a missed one is a false negative, not a false positive.
fn post_dominance(successors: &[Vec<usize>], reachable: &[bool]) -> Vec<Vec<bool>> {
    let count = successors.len();
    let mut can_exit = vec![false; count];
    for (at, targets) in successors.iter().enumerate() {
        if targets.is_empty()
            && let Some(slot) = can_exit.get_mut(at)
        {
            *slot = true;
        }
    }
    // Propagate "can reach the exit" backwards.
    for _ in 0..count.saturating_add(1) {
        let mut changed = false;
        for (at, targets) in successors.iter().enumerate() {
            if can_exit.get(at).copied().unwrap_or(false) {
                continue;
            }
            if targets
                .iter()
                .any(|t| can_exit.get(*t).copied().unwrap_or(false))
                && let Some(slot) = can_exit.get_mut(at)
            {
                *slot = true;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // `pdom[b]` as a bit row: which blocks post-dominate `b`.
    let mut pdom: Vec<Vec<bool>> = (0..count)
        .map(|at| {
            if successors.get(at).is_some_and(Vec::is_empty) {
                let mut row = vec![false; count];
                if let Some(slot) = row.get_mut(at) {
                    *slot = true;
                }
                row
            } else {
                vec![true; count]
            }
        })
        .collect();

    for _ in 0..count.saturating_add(2) {
        let mut changed = false;
        for at in 0..count {
            let targets = successors.get(at).map_or(&[][..], Vec::as_slice);
            if targets.is_empty() {
                continue;
            }
            let mut row = vec![true; count];
            for &target in targets {
                let Some(other) = pdom.get(target) else {
                    continue;
                };
                for slot in 0..count {
                    if !other.get(slot).copied().unwrap_or(false)
                        && let Some(cell) = row.get_mut(slot)
                    {
                        *cell = false;
                    }
                }
            }
            if let Some(cell) = row.get_mut(at) {
                *cell = true;
            }
            if pdom.get(at) != Some(&row) {
                if let Some(slot) = pdom.get_mut(at) {
                    *slot = row;
                }
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Transpose into `post_dominates[a][b]`, dropping blocks that never exit
    // and blocks that are not live at all.
    let mut out = vec![vec![false; count]; count];
    for b in 0..count {
        for a in 0..count {
            let holds = pdom
                .get(b)
                .and_then(|row| row.get(a))
                .copied()
                .unwrap_or(false)
                && (a == b || can_exit.get(a).copied().unwrap_or(false))
                && reachable.get(a).copied().unwrap_or(false);
            if let Some(cell) = out.get_mut(a).and_then(|row| row.get_mut(b)) {
                *cell = holds;
            }
        }
    }
    out
}

/// Iterative dominance over the forward CFG, entry at block 0.
///
/// The textbook data-flow equation: `dom[entry] = {entry}`, and
/// `dom[b] = {b} ∪ (∩ dom[p] for every live predecessor p of b)` elsewhere,
/// iterated to a fixpoint. Unlike [`post_dominance`] there is exactly one
/// real root, never a virtual one added for several exits. So a block that
/// is not reachable from the entry dominates nothing but itself — moot in
/// practice, since every caller here first checks [`ControlGraph::is_live`].
fn dominance(successors: &[Vec<usize>], reachable: &[bool]) -> Vec<Vec<bool>> {
    let count = successors.len();
    let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (at, targets) in successors.iter().enumerate() {
        for &target in targets {
            if let Some(preds) = predecessors.get_mut(target) {
                preds.push(at);
            }
        }
    }

    // `dom[b]` as a bit row: which blocks dominate `b`. The entry starts (and
    // stays) dominated only by itself; every other block starts as if
    // dominated by everything, narrowed down by the loop below.
    let mut dom: Vec<Vec<bool>> = (0..count)
        .map(|at| {
            let mut row = vec![true; count];
            if at == 0 {
                row.fill(false);
                if let Some(slot) = row.first_mut() {
                    *slot = true;
                }
            }
            row
        })
        .collect();

    for _ in 0..count.saturating_add(2) {
        let mut changed = false;
        for b in 1..count {
            if !reachable.get(b).copied().unwrap_or(false) {
                continue;
            }
            let live_preds: Vec<usize> = predecessors
                .get(b)
                .into_iter()
                .flatten()
                .copied()
                .filter(|p| reachable.get(*p).copied().unwrap_or(false))
                .collect();
            if live_preds.is_empty() {
                continue;
            }
            let mut row = vec![true; count];
            for &pred in &live_preds {
                let Some(other) = dom.get(pred) else {
                    continue;
                };
                for slot in 0..count {
                    if !other.get(slot).copied().unwrap_or(false)
                        && let Some(cell) = row.get_mut(slot)
                    {
                        *cell = false;
                    }
                }
            }
            if let Some(cell) = row.get_mut(b) {
                *cell = true;
            }
            if dom.get(b) != Some(&row) {
                if let Some(slot) = dom.get_mut(b) {
                    *slot = row;
                }
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Transpose into `dominates[a][b]`, dropping blocks that are not live.
    let mut out = vec![vec![false; count]; count];
    for b in 0..count {
        for a in 0..count {
            let holds = dom.get(b).and_then(|row| row.get(a)).copied().unwrap_or(false)
                && reachable.get(a).copied().unwrap_or(false)
                && reachable.get(b).copied().unwrap_or(false);
            if let Some(cell) = out.get_mut(a).and_then(|row| row.get_mut(b)) {
                *cell = holds;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mir;

    fn body(blocks: &str) -> Body {
        let text = format!("fn f() -> u8 {{\n    let mut _0: u8;\n\n{blocks}}}\n");
        let doc = mir::parse("test", "t.mir", &text);
        doc.bodies.first().cloned().expect("one body")
    }

    /// `if c { A } B` — A is control-dependent on the branch, B is not.
    #[test]
    fn only_the_conditional_arm_is_control_dependent() {
        let body = body(
            "    bb0: {\n        switchInt(copy _1) -> [0: bb2, otherwise: bb1];\n    }\n\
             \n    bb1: {\n        goto -> bb2;\n    }\n\
             \n    bb2: {\n        return;\n    }\n",
        );
        let graph = ControlGraph::new(&body);
        let (branch, arm, join) = (
            graph.index_of("bb0").expect("bb0"),
            graph.index_of("bb1").expect("bb1"),
            graph.index_of("bb2").expect("bb2"),
        );
        assert!(graph.is_control_dependent(arm, branch));
        assert!(
            !graph.is_control_dependent(join, branch),
            "the join post-dominates the branch: an unconditional sink after an \
             `if` is not control-dependent on it"
        );
    }

    #[test]
    fn both_arms_of_a_two_sided_branch_are_control_dependent() {
        let body = body(
            "    bb0: {\n        switchInt(copy _1) -> [0: bb2, otherwise: bb1];\n    }\n\
             \n    bb1: {\n        goto -> bb3;\n    }\n\
             \n    bb2: {\n        goto -> bb3;\n    }\n\
             \n    bb3: {\n        return;\n    }\n",
        );
        let graph = ControlGraph::new(&body);
        let branch = graph.index_of("bb0").expect("bb0");
        for arm in ["bb1", "bb2"] {
            let at = graph.index_of(arm).expect(arm);
            assert!(graph.is_control_dependent(at, branch), "{arm}");
        }
        let join = graph.index_of("bb3").expect("bb3");
        assert!(!graph.is_control_dependent(join, branch));
    }

    /// The regression this whole design exists to prevent, on a real dump.
    ///
    /// Every `async` body opens with `_n = discriminant((*_m)); switchInt(_n)`,
    /// and the workflow's own locals live in the *fields* of `(*_m)` across the
    /// suspend points. If a `discriminant` read saw through to those fields,
    /// one tainted local would make the resume switch tainted, every sink in
    /// the body control-dependent on it, and every async workflow in the repo a
    /// finding. `tests/fixtures/async_multi_await.mir` is a three-suspend-point
    /// coroutine that holds a struct across all of them.
    #[test]
    fn the_coroutine_resume_switch_does_not_see_its_own_suspended_locals() {
        use super::super::taint::{Fact, TaintSet, TaintState};
        use crate::mir::ast::Projection;
        use crate::verdict::{Site, TaintKind};

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/async_multi_await.mir");
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            !text.is_empty(),
            "fixture {} must be readable",
            path.display()
        );
        let doc = mir::parse("fixture", "async_multi_await.mir", &text);
        let body = doc
            .bodies
            .iter()
            .find(|b| b.path == "pipeline::{closure#0}")
            .expect("the coroutine body");

        // The resume switch: `_n = discriminant((*_m))` followed by `switchInt(_n)`.
        let entry = body.blocks.first().expect("bb0");
        let scrutinee = entry
            .statements
            .iter()
            .find_map(|statement| match statement {
                crate::mir::ast::Statement::Assign { rvalue, .. } => rvalue.discriminant_of.clone(),
                crate::mir::ast::Statement::Other(_) => None,
            })
            .expect("bb0 reads the coroutine's discriminant");
        assert!(
            matches!(
                entry.terminator,
                crate::mir::ast::Terminator::SwitchInt { .. }
            ),
            "bb0 must end in the resume switch"
        );

        // A suspended local: a field of the very place the discriminant reads.
        let mut suspended = scrutinee.clone();
        suspended.projections.push(Projection::Field(1));
        let mut state = TaintState::new();
        state.add(
            &suspended,
            &TaintSet::of(Fact {
                kind: TaintKind::Value,
                source: Site {
                    function: "helper".to_string(),
                    block: "bb0".to_string(),
                    what: "SystemTime::now".to_string(),
                    hint: None,
                },
                hops: Vec::new(),
            }),
        );
        assert!(
            state.read(&scrutinee, true).is_empty(),
            "the resume switch must not see the tainted suspended local at {suspended:?}"
        );
        assert!(
            !state.read(&scrutinee, false).is_empty(),
            "an ordinary read of the same place still sees it — the exemption is \
             specific to `discriminant`, not a hole in the read rule"
        );
    }

    /// `if c { A } B` — the entry dominates everything; neither arm of the
    /// branch dominates the join, and the join does not dominate itself's own
    /// predecessor arm.
    #[test]
    fn dominance_is_the_mirror_of_post_dominance() {
        let body = body(
            "    bb0: {\n        switchInt(copy _1) -> [0: bb2, otherwise: bb1];\n    }\n\
             \n    bb1: {\n        goto -> bb2;\n    }\n\
             \n    bb2: {\n        return;\n    }\n",
        );
        let graph = ControlGraph::new(&body);
        let (entry, arm, join) = (
            graph.index_of("bb0").expect("bb0"),
            graph.index_of("bb1").expect("bb1"),
            graph.index_of("bb2").expect("bb2"),
        );
        assert!(graph.dominates(entry, arm), "the entry dominates every block");
        assert!(graph.dominates(entry, join));
        assert!(graph.dominates(entry, entry), "every block dominates itself");
        assert!(
            !graph.dominates(arm, join),
            "the join is also reached directly from bb0, so bb1 does not dominate it"
        );
        assert!(
            !graph.dominates(join, arm),
            "dominance runs from the entry forward, never backward"
        );
    }

    /// A straight-line chain: each block dominates every block after it.
    #[test]
    fn a_straight_line_chain_is_totally_ordered_by_dominance() {
        let body = body(
            "    bb0: {\n        goto -> bb1;\n    }\n\
             \n    bb1: {\n        goto -> bb2;\n    }\n\
             \n    bb2: {\n        return;\n    }\n",
        );
        let graph = ControlGraph::new(&body);
        let (bb0, bb1, bb2) = (
            graph.index_of("bb0").expect("bb0"),
            graph.index_of("bb1").expect("bb1"),
            graph.index_of("bb2").expect("bb2"),
        );
        assert!(graph.dominates(bb0, bb1) && graph.dominates(bb0, bb2));
        assert!(graph.dominates(bb1, bb2));
        assert!(!graph.dominates(bb1, bb0) && !graph.dominates(bb2, bb0));
    }

    #[test]
    fn a_cleanup_block_is_not_part_of_the_graph() {
        let body = body(
            "    bb0: {\n        _0 = f() -> [return: bb1, unwind: bb2];\n    }\n\
             \n    bb1: {\n        return;\n    }\n\
             \n    bb2 (cleanup): {\n        resume;\n    }\n",
        );
        let graph = ControlGraph::new(&body);
        let cleanup = graph.index_of("bb2").expect("bb2");
        assert!(!graph.is_live(cleanup));
    }
}
