//! The interprocedural engine: one body's taint, its callees' taint, and the
//! sites where the two meet a sink (D4/D6).
//!
//! # Shape of the analysis
//!
//! Per body, a **flow-insensitive fixpoint** over place-keyed facts
//! ([`taint`]). Flow-insensitive because MIR at `opt-level=0` is already
//! SSA-ish for the values that matter and because the question being asked —
//! "can this source reach this sink at all" — is a reachability question;
//! flow-sensitivity would buy precision only for code that overwrites a tainted
//! local with a clean value, which the corpus deliberately does not reward.
//!
//! Across bodies, **context-sensitive expansion with memoisation** rather than
//! symbolic summaries: a callee is analyzed with its arguments' actual facts
//! seeded on its parameters, and the result is memoised on
//! `(body, substitution, argument signature)`. That keeps the *whole* hop chain
//! — `wf_three_hop_chain` → `a` → `b` → `fine_stamp` → `SystemTime::now` —
//! attached to the fact itself, which is what the corpus's `trace_contains`
//! oracle checks, and it costs nothing on bodies that are called once.
//!
//! Recursion is the one case expansion cannot handle: re-entering a body
//! already on the stack returns the conservative answer (every argument's taint
//! flows to the return value and to every out-parameter) **and** records a
//! [`BoundaryKind::Recursion`], so a recursive helper makes a workflow
//! `unknown` rather than silently `proven`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::mir::ast::{BasicBlock, Body, Local, Operand, Place, Projection, Statement, Terminator};
use crate::model::callee::CalleePath;
use crate::model::{CallClass, Model, SinkRule};
use crate::resolve::{Program, Resolution, Substitution};
use crate::verdict::{Boundary, BoundaryKind, Finding, FindingKind, Hop, Site, TaintKind};

use super::control::ControlGraph;
use super::taint::{Fact, TaintSet, TaintState};

/// Fixpoint rounds before the analysis gives up refining a body.
const MAX_ROUNDS: u32 = 24;
/// Taint of reading one operand. `discriminant` restricts the read to the
/// place and its ancestors (see [`super::taint`]).
fn read_operand(operand: &Operand, state: &TaintState, discriminant: bool) -> TaintSet {
    match operand {
        Operand::Copy(place) | Operand::Move(place) => state.read(place, discriminant),
        Operand::Const { .. } => TaintSet::new(),
    }
}

/// Parse a printed callee path (a thin wrapper for readability at call sites).
fn parsed_of(printed: &str) -> CalleePath {
    CalleePath::parse(printed)
}

/// Body analyses per workflow entry.
const BUDGET: u32 = 6000;
/// Calls that return a new name for an existing place rather than a new value.
///
/// The guard family matters as much as the `deref` family: `RefCell::borrow`
/// and `RefCell::borrow_mut` hand out two different names for the *same*
/// interior, and a body that mutates through one and reads through the other
/// (`*c.borrow_mut() += 1; *c.borrow()`) is only connected if both collapse
/// onto the cell.
const REBORROWS: [&str; 15] = [
    "deref",
    "deref_mut",
    "as_mut",
    "as_ref",
    "as_slice",
    "as_mut_slice",
    "index_mut",
    "borrow",
    "borrow_mut",
    "try_borrow",
    "try_borrow_mut",
    "lock",
    "try_lock",
    "read",
    "write",
];

/// What one body does, as seen by its caller.
#[derive(Debug, Clone, Default)]
pub struct BodyOutcome {
    /// Taint of the return value.
    pub ret: TaintSet,
    /// Taint written back through parameter `i` (an `&mut` out-parameter).
    pub out: BTreeMap<usize, TaintSet>,
    /// The body (or something it calls) emits a command.
    pub has_sink: bool,
}

/// One command emission inside the body being analyzed.
#[derive(Debug, Clone)]
struct SinkRecord {
    block: String,
    site: Site,
}

/// One `switchInt` whose operand is tainted.
#[derive(Debug, Clone)]
struct BranchRecord {
    block: String,
    facts: TaintSet,
    what: String,
}

/// The analyzer for one workflow entry.
pub struct Analyzer<'a> {
    program: &'a Program,
    model: &'a Model,
    memo: HashMap<String, BodyOutcome>,
    stack: Vec<String>,
    budget: u32,
    /// Findings collected across every body reached from this entry.
    pub findings: Vec<Finding>,
    /// Boundaries collected across every body reached from this entry.
    pub boundaries: Vec<Boundary>,
}

impl<'a> Analyzer<'a> {
    #[must_use]
    pub fn new(program: &'a Program, model: &'a Model) -> Self {
        Self {
            program,
            model,
            memo: HashMap::new(),
            stack: Vec::new(),
            budget: BUDGET,
            findings: Vec::new(),
            boundaries: Vec::new(),
        }
    }

    /// Analyze `path` with `args` seeded on its parameters.
    pub fn analyze_body(
        &mut self,
        path: &str,
        subst: &Substitution,
        args: &[TaintSet],
        hops: &[Hop],
    ) -> BodyOutcome {
        let key = memo_key(path, subst, args);
        if let Some(cached) = self.memo.get(&key) {
            return cached.clone();
        }
        if self.stack.iter().any(|frame| frame == path) {
            self.push_boundary(BoundaryKind::Recursion, path, path, "bb0");
            return conservative(args);
        }
        if self.budget == 0 {
            self.push_boundary(
                BoundaryKind::Recursion,
                &format!("analysis budget exhausted at {path}"),
                path,
                "bb0",
            );
            return conservative(args);
        }
        self.budget = self.budget.saturating_sub(1);
        self.stack.push(path.to_string());
        let outcome = self.run_body(path, subst, args, hops);
        self.stack.pop();
        self.memo.insert(key, outcome.clone());
        outcome
    }

    fn run_body(
        &mut self,
        path: &str,
        subst: &Substitution,
        args: &[TaintSet],
        hops: &[Hop],
    ) -> BodyOutcome {
        let Some(body) = self.program.body(path) else {
            self.push_boundary(BoundaryKind::MissingBody, path, path, "bb0");
            return conservative(args);
        };
        // Two nested loops. The inner one is the flow-insensitive fixpoint over
        // facts; the outer one exists because a sanitizer discovered *late* in a
        // round (`keys().collect()` in `bb3`, `sort()` in `bb5`) cannot retract
        // the taint the earlier blocks already pushed downstream. Re-running the
        // whole body with the kills already in hand is the cheapest correct
        // answer, and the kill set only grows, so it terminates.
        let mut state = TaintState::new();
        let mut kills: Vec<(Place, TaintKind)> = Vec::new();
        for _attempt in 0..3 {
            state = TaintState::new();
            state.seed_kills(&kills);
            for (index, (local, _)) in body.params.iter().enumerate() {
                if let Some(set) = args.get(index) {
                    state.add(
                        &Place {
                            local: *local,
                            projections: Vec::new(),
                        },
                        set,
                    );
                }
            }
            for _round in 0..MAX_ROUNDS {
                let mut changed = false;
                for block in &body.blocks {
                    changed |= self.run_block(path, body, block, subst, hops, &mut state, None);
                }
                changed |= state.apply_kills();
                if !changed {
                    break;
                }
            }
            state.apply_kills();
            if state.kills().len() == kills.len() {
                break;
            }
            kills = state.kills().to_vec();
        }

        // Reporting pass: sinks, boundaries and forbidden effects are emitted
        // once, against the converged state.
        let mut sinks: Vec<SinkRecord> = Vec::new();
        let mut branches: Vec<BranchRecord> = Vec::new();
        let mut report = Report {
            sinks: &mut sinks,
            branches: &mut branches,
        };
        for block in &body.blocks {
            self.run_block(
                path,
                body,
                block,
                subst,
                hops,
                &mut state,
                Some(&mut report),
            );
        }
        self.control_dependent_sinks(path, body, &sinks, &branches);

        let mut outcome = BodyOutcome {
            ret: state.read(
                &Place {
                    local: Local(0),
                    projections: Vec::new(),
                },
                false,
            ),
            out: BTreeMap::new(),
            has_sink: !sinks.is_empty(),
        };
        for (index, (local, declared)) in body.params.iter().enumerate() {
            if !declared.trim_start().starts_with("&mut") && !declared.contains("*mut") {
                continue;
            }
            let written = state.read_root(*local);
            if !written.is_empty() {
                outcome.out.insert(index, written);
            }
        }
        outcome
    }

    /// Transfer functions for one block. `report` is `Some` on the final pass only.
    #[allow(clippy::too_many_arguments)]
    fn run_block(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let mut changed = false;
        for statement in &block.statements {
            let Statement::Assign { dest, rvalue } = statement else {
                continue;
            };
            if let Some((place, is_mut)) = &rvalue.ref_of {
                let _ = is_mut;
                if dest.projections.is_empty() {
                    state.alias(dest.local, place);
                }
            }
            let mut set = TaintSet::new();
            for operand in &rvalue.reads {
                let discriminant = rvalue
                    .discriminant_of
                    .as_ref()
                    .is_some_and(|p| Some(p) == operand_place(operand));
                set.absorb(&read_operand(operand, state, discriminant));
            }
            if let Some(alloc) = &rvalue.static_alloc {
                // `const {allocN: *mut T}` is a raw pointer to a static, not a
                // reference to one: its provenance is outside the memory model,
                // and the `unsafe-raw-pointer` boundary below is the honest
                // answer rather than an ambient-source verdict (U04).
                if !rvalue.text.contains(": *mut ") && !rvalue.text.contains(": *const ") {
                    set.absorb(&self.static_taint_of_alloc(path, alloc, block, hops));
                }
            }
            set.absorb(&self.const_taint(path, body, &rvalue.text, block, subst, hops));
            if report.is_some() {
                self.raw_pointer_check(path, body, &rvalue.reads, block);
            }
            changed |= state.add(dest, &set);
        }

        match &block.terminator {
            Terminator::Call {
                dest,
                callee,
                indirect,
                args,
                ..
            } => {
                changed |= self.transfer_call(
                    path,
                    body,
                    block,
                    dest,
                    callee.as_deref(),
                    indirect.as_ref(),
                    args,
                    subst,
                    hops,
                    state,
                    report.as_deref_mut(),
                );
            }
            Terminator::SwitchInt { operand, targets } => {
                if targets.len() >= 2
                    && let Some(report) = report
                {
                    let facts = read_operand(operand, state, false);
                    if !facts.is_empty() {
                        report.branches.push(BranchRecord {
                            block: block.label.clone(),
                            facts,
                            what: operand_text(operand),
                        });
                    }
                }
            }
            Terminator::Assert { .. }
            | Terminator::Goto { .. }
            | Terminator::Drop { .. }
            | Terminator::Return
            | Terminator::Unreachable
            | Terminator::Other { .. } => {}
            Terminator::InlineAsm { .. } => {
                if report.is_some() {
                    self.push_boundary(BoundaryKind::InlineAsm, "asm!", path, &block.label);
                }
            }
        }
        changed
    }

    // ── calls ───────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn transfer_call(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        dest: &Place,
        callee: Option<&str>,
        indirect: Option<&Operand>,
        args: &[Operand],
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let emit = report.is_some();
        let arg_taints: Vec<TaintSet> = args
            .iter()
            .map(|operand| read_operand(operand, state, false))
            .collect();
        let union = union_of(&arg_taints);

        let Some(callee) = callee else {
            if emit {
                let detail = indirect.map_or_else(|| block.label.clone(), operand_text);
                self.push_boundary(BoundaryKind::IndirectCall, &detail, path, &block.label);
            }
            return state.add(dest, &union);
        };

        let printed = subst.apply(callee);
        let parsed = CalleePath::parse(&printed);
        // A reborrow hands back the *same* place under a new name. Without this
        // edge `xs.sort()` lowers to `sort(deref_mut(&mut xs))` and the sanitizer
        // would clear the taint of a temporary instead of the vector's.
        if dest.projections.is_empty()
            && REBORROWS.contains(&parsed.last_segment())
            && let Some(place) = args.first().and_then(operand_place)
        {
            state.alias(dest.local, place);
        }
        let declared = Self::dest_type(body, dest, subst, &parsed);
        let classes = self.model.classify(&parsed, declared.as_deref());

        // Every class the model can attach to a call, in decision order.
        if let Some(rule) = classes.iter().find_map(|c| match c {
            CallClass::Forbidden(rule) => Some(*rule),
            _ => None,
        }) {
            if emit {
                let site = Self::site(path, &block.label, &printed);
                self.findings.push(Finding {
                    kind: FindingKind::ForbiddenEffect,
                    taint: TaintKind::Value,
                    source: site.clone(),
                    sink: site.clone(),
                    trace: with_hops(hops, &site, &format!("calls {printed}")),
                    message: format!(
                        "forbidden effect {printed} reachable from {path} ({})",
                        first_sentence(&rule.reason)
                    ),
                });
                if let Some(report) = report.as_deref_mut() {
                    report.sinks.push(SinkRecord {
                        block: block.label.clone(),
                        site,
                    });
                }
            }
            return false;
        }

        if let Some(rule) = classes.iter().find_map(|c| match c {
            CallClass::Sink(rule) => Some(*rule),
            _ => None,
        }) {
            let offset = usize::from(parsed.receiver.is_some());
            if emit {
                let site = Self::site(path, &block.label, &printed);
                self.record_sink(rule, offset, args, &arg_taints, &site);
                if let Some(report) = report {
                    report.sinks.push(SinkRecord {
                        block: block.label.clone(),
                        site,
                    });
                }
            }
            let opaque: BTreeSet<usize> = rule
                .opaque_closure_args
                .iter()
                .map(|index| index.saturating_add(offset))
                .collect();
            self.descend_closures(
                path,
                body,
                block,
                args,
                &arg_taints,
                subst,
                hops,
                state,
                dest,
                &opaque,
            );
            // A command's result is recorded in history and replayed verbatim.
            return false;
        }

        if classes
            .iter()
            .any(|c| matches!(c, CallClass::HandlerRegistration(_)))
        {
            self.descend_closures(
                path,
                body,
                block,
                args,
                &arg_taints,
                subst,
                hops,
                state,
                dest,
                &BTreeSet::new(),
            );
            return false;
        }

        if classes
            .iter()
            .any(|c| matches!(c, CallClass::Sanctioned(_) | CallClass::NonSink(_)))
        {
            return false;
        }

        if let Some(rule) = classes.iter().find_map(|c| match c {
            CallClass::Source(rule) => Some(*rule),
            _ => None,
        }) {
            let ambient = rule
                .receiver
                .as_deref()
                .is_some_and(|receiver| self.model.is_ambient_type(receiver));
            let receiver_tainted = arg_taints.first().is_some_and(|set| !set.is_empty());
            let mut set = union.clone();
            if !ambient || receiver_tainted {
                let site = Self::site(path, &block.label, &printed);
                let mut hop_chain = hops.to_vec();
                hop_chain.push(Hop {
                    function: path.to_string(),
                    step: format!("calls {printed} ({})", first_sentence(&rule.reason)),
                });
                set.insert(Fact {
                    kind: rule.kind,
                    source: site,
                    hops: hop_chain,
                });
            }
            self.descend_closures(
                path,
                body,
                block,
                args,
                &arg_taints,
                subst,
                hops,
                state,
                dest,
                &BTreeSet::new(),
            );
            let mut changed = state.add(dest, &set);
            changed |= Self::write_back_refs(body, args, &set, state, None);
            return changed;
        }

        if let Some(rule) = classes.iter().find_map(|c| match c {
            CallClass::Sanitizer(rule) => Some(*rule),
            _ => None,
        }) {
            // A comparator that reads ambient state does not *clear* the order —
            // it decides it. `sort_by(|a, b| ..COUNTER..)` is an Order source.
            let closure_taint = self.closure_argument_taint(
                path,
                body,
                block,
                args,
                &arg_taints,
                subst,
                hops,
                state,
            );
            if closure_taint.is_empty() {
                if let Some(receiver) = args.first().and_then(operand_place) {
                    state.kill(receiver, rule.clears);
                }
                state.kill(dest, rule.clears);
                let mut cleared = BTreeSet::new();
                cleared.insert(rule.clears);
                return state.add(dest, &union.without(&cleared));
            }
            let ordered = closure_taint.as_kind(rule.clears);
            let mut changed = state.add(dest, &union);
            changed |= state.add(dest, &ordered);
            changed |= Self::write_back_refs(body, args, &ordered, state, None);
            return changed;
        }

        if let Some(rule) = classes.iter().find_map(|c| match c {
            CallClass::Reduction(rule) => Some(*rule),
            _ => None,
        }) {
            let mut cleared = BTreeSet::new();
            cleared.insert(rule.clears);
            return state.add(dest, &union.without(&cleared));
        }

        if let Some(name) = classes.iter().find_map(|c| match c {
            CallClass::UnmodeledCtxMethod(name) => Some(name.clone()),
            _ => None,
        }) {
            if emit {
                self.push_boundary(BoundaryKind::UnmodeledCtxMethod, &name, path, &block.label);
            }
            return state.add(dest, &union);
        }

        // Nothing in the model decides this call: follow it.
        self.follow_call(
            path,
            body,
            block,
            dest,
            &printed,
            callee,
            args,
            &arg_taints,
            subst,
            hops,
            state,
            report,
        )
    }

    /// `[dyn Tr devirtualized to Concrete, built in f]` for an RTA-resolved call.
    fn devirtualized(&self, parsed: &CalleePath) -> String {
        if !parsed.is_dyn {
            return String::new();
        }
        let Some(trait_name) = parsed.trait_.as_deref().or(parsed.receiver.as_deref()) else {
            return String::new();
        };
        let candidates = self.program.dyn_candidates(trait_name);
        match candidates.as_slice() {
            [(concrete, built_in)] => format!(
                " [dyn {trait_name} devirtualized to {concrete}, built in {}]",
                self.program.qualified_name(built_in)
            ),
            _ => String::new(),
        }
    }

    /// Resolve and descend into an unmodelled call.
    #[allow(clippy::too_many_arguments)]
    fn follow_call(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        dest: &Place,
        printed: &str,
        callee: &str,
        args: &[Operand],
        arg_taints: &[TaintSet],
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
        report: Option<&mut Report<'_>>,
    ) -> bool {
        let emit = report.is_some();
        let union = union_of(arg_taints);
        match self.program.resolve_call(path, printed) {
            Resolution::Body(target) => {
                let inner_subst = self.program.call_substitution_in(path, callee, subst);
                let qualified = self.program.qualified_name(&target);
                let resolved = if qualified == printed {
                    String::new()
                } else {
                    format!(" -> {qualified}")
                };
                let hop = Hop {
                    function: path.to_string(),
                    step: format!(
                        "calls {printed}{resolved}{}{}",
                        self.devirtualized(&parsed_of(printed)),
                        subst_note(&inner_subst)
                    ),
                };
                let mut inner_hops = hops.to_vec();
                inner_hops.push(hop.clone());
                let seeded: Vec<TaintSet> =
                    arg_taints.iter().map(|set| set.with_hop(&hop)).collect();
                let outcome = self.analyze_body(&target, &inner_subst, &seeded, &inner_hops);
                let mut changed = state.add(dest, &outcome.ret);
                changed |=
                    Self::write_back_refs(body, args, &TaintSet::new(), state, Some(&outcome));
                self.descend_closures(
                    path,
                    body,
                    block,
                    args,
                    arg_taints,
                    subst,
                    hops,
                    state,
                    dest,
                    &BTreeSet::new(),
                );
                if outcome.has_sink
                    && let Some(report) = report
                {
                    // A helper that emits commands makes *this call site* a sink
                    // for control dependence: `if tainted { dispatch(ctx) }` is
                    // the same finding as an inline `ctx.execute_activity_raw`.
                    report.sinks.push(SinkRecord {
                        block: block.label.clone(),
                        site: Self::site(
                            path,
                            &block.label,
                            &format!("commands emitted by {qualified}"),
                        ),
                    });
                }
                changed
            }
            Resolution::External(_) => {
                let hop = Hop {
                    function: path.to_string(),
                    step: format!("calls {printed}"),
                };
                self.descend_closures(
                    path,
                    body,
                    block,
                    args,
                    arg_taints,
                    subst,
                    hops,
                    state,
                    dest,
                    &BTreeSet::new(),
                );
                let mut changed = state.add(dest, &union.with_hop(&hop));
                changed |= Self::write_back_refs(body, args, &union, state, None);
                changed
            }
            Resolution::Boundary(kind, detail) => {
                if emit {
                    self.push_boundary(kind, &detail, path, &block.label);
                }
                state.add(dest, &union)
            }
        }
    }

    /// Analyze every closure passed as an argument, assuming it is invoked.
    #[allow(clippy::too_many_arguments)]
    fn descend_closures(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        args: &[Operand],
        arg_taints: &[TaintSet],
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
        dest: &Place,
        opaque: &BTreeSet<usize>,
    ) {
        let taint = self.closure_argument_taint_inner(
            path, body, block, args, arg_taints, subst, hops, state, opaque,
        );
        if !taint.is_empty() {
            state.add(dest, &taint);
        }
    }

    /// The taint a call's closure arguments contribute to its result.
    #[allow(clippy::too_many_arguments)]
    fn closure_argument_taint(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        args: &[Operand],
        arg_taints: &[TaintSet],
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
    ) -> TaintSet {
        self.closure_argument_taint_inner(
            path,
            body,
            block,
            args,
            arg_taints,
            subst,
            hops,
            state,
            &BTreeSet::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn closure_argument_taint_inner(
        &mut self,
        path: &str,
        body: &Body,
        block: &BasicBlock,
        args: &[Operand],
        arg_taints: &[TaintSet],
        subst: &Substitution,
        hops: &[Hop],
        state: &mut TaintState,
        opaque: &BTreeSet<usize>,
    ) -> TaintSet {
        let mut out = TaintSet::new();
        for (index, operand) in args.iter().enumerate() {
            if opaque.contains(&index) {
                continue;
            }
            let Some(span) = Self::closure_span_of(body, operand, subst) else {
                continue;
            };
            let Some(target) = self.program.closure_body(&span).map(str::to_string) else {
                continue;
            };
            let hop = Hop {
                function: path.to_string(),
                step: format!("invokes closure {span}"),
            };
            let mut inner_hops = hops.to_vec();
            inner_hops.push(hop.clone());
            // `_1` is the closure environment; the remaining parameters are
            // whatever the callee hands it, which is at most everything else
            // this call was given.
            let mut others = TaintSet::new();
            for (other, set) in arg_taints.iter().enumerate() {
                if other != index {
                    others.absorb(set);
                }
            }
            let mut seeded = vec![
                arg_taints
                    .get(index)
                    .cloned()
                    .unwrap_or_default()
                    .with_hop(&hop),
            ];
            let Some(closure_body) = self.program.body(&target) else {
                continue;
            };
            for _ in 1..closure_body.params.len() {
                seeded.push(others.with_hop(&hop));
            }
            let outcome = self.analyze_body(&target, &Substitution::new(), &seeded, &inner_hops);
            out.absorb(&outcome.ret);
        }
        let _ = (block, state);
        out
    }

    /// The closure span an argument names, from its declared type or its constant.
    fn closure_span_of(body: &Body, operand: &Operand, subst: &Substitution) -> Option<String> {
        match operand {
            // `Operand::Const::closure` carries the span *inside* the brace form
            // (`s.rs:16:21: 16:24`), while a body is keyed on the whole
            // `{closure@..}` type as its first parameter prints it.
            Operand::Const { closure, text, .. } => closure
                .as_ref()
                .map(|span| format!("{{closure@{span}}}"))
                .or_else(|| brace_form(&subst.apply(text))),
            Operand::Copy(place) | Operand::Move(place) => {
                let declared = body.locals.get(&place.local)?;
                brace_form(&subst.apply(declared))
            }
        }
    }

    /// Write an out-parameter's taint back through an `&mut` argument.
    fn write_back_refs(
        body: &Body,
        args: &[Operand],
        set: &TaintSet,
        state: &mut TaintState,
        outcome: Option<&BodyOutcome>,
    ) -> bool {
        let mut changed = false;
        for (index, operand) in args.iter().enumerate() {
            let Some(place) = operand_place(operand) else {
                continue;
            };
            let is_mut_ref = body
                .locals
                .get(&place.local)
                .is_some_and(|ty| ty.trim_start().starts_with("&mut") || ty.contains("*mut"));
            if !is_mut_ref {
                continue;
            }
            let target = Place {
                local: place.local,
                projections: {
                    let mut projections = place.projections.clone();
                    projections.push(Projection::Deref);
                    projections
                },
            };
            match outcome {
                Some(outcome) => {
                    if let Some(written) = outcome.out.get(&index) {
                        changed |= state.add(&target, written);
                    }
                }
                None => {
                    if !set.is_empty() {
                        changed |= state.add(&target, set);
                    }
                }
            }
        }
        changed
    }

    // ── sinks, branches, boundaries ─────────────────────────────────────────

    fn record_sink(
        &mut self,
        rule: &SinkRule,
        offset: usize,
        args: &[Operand],
        arg_taints: &[TaintSet],
        site: &Site,
    ) {
        let checked: Vec<usize> = if rule.args.is_empty() {
            (offset..args.len()).collect()
        } else {
            rule.args
                .iter()
                .map(|index| index.saturating_add(offset))
                .collect()
        };
        for index in checked {
            if rule
                .opaque_closure_args
                .iter()
                .any(|opaque| opaque.saturating_add(offset) == index)
            {
                continue;
            }
            let Some(set) = arg_taints.get(index) else {
                continue;
            };
            for fact in set.facts() {
                let mut trace = fact.hops.clone();
                trace.push(Hop {
                    function: site.function.clone(),
                    step: format!("emits {}", site.what),
                });
                self.findings.push(Finding {
                    kind: FindingKind::TaintedSinkArgument,
                    taint: fact.kind,
                    source: fact.source.clone(),
                    sink: site.clone(),
                    trace,
                    message: format!(
                        "{:?} taint from {} in {} reaches {} in {}",
                        fact.kind, fact.source.what, fact.source.function, site.what, site.function
                    ),
                });
            }
        }
    }

    fn control_dependent_sinks(
        &mut self,
        path: &str,
        body: &Body,
        sinks: &[SinkRecord],
        branches: &[BranchRecord],
    ) {
        if sinks.is_empty() || branches.is_empty() {
            return;
        }
        let graph = ControlGraph::new(body);
        for branch in branches {
            let Some(branch_at) = graph.index_of(&branch.block) else {
                continue;
            };
            for sink in sinks {
                let Some(sink_at) = graph.index_of(&sink.block) else {
                    continue;
                };
                if !graph.is_control_dependent(sink_at, branch_at) {
                    continue;
                }
                for fact in branch.facts.facts() {
                    let mut trace = fact.hops.clone();
                    trace.push(Hop {
                        function: path.to_string(),
                        step: format!("branches on tainted {} at {}", branch.what, branch.block),
                    });
                    trace.push(Hop {
                        function: sink.site.function.clone(),
                        step: format!("emits {}", sink.site.what),
                    });
                    self.findings.push(Finding {
                        kind: FindingKind::ControlDependentSink,
                        taint: TaintKind::Control,
                        source: fact.source.clone(),
                        sink: sink.site.clone(),
                        trace,
                        message: format!(
                            "Control taint from {} in {} reaches {} in {}",
                            fact.source.what,
                            fact.source.function,
                            sink.site.what,
                            sink.site.function
                        ),
                    });
                }
            }
        }
    }

    fn push_boundary(&mut self, kind: BoundaryKind, detail: &str, function: &str, block: &str) {
        let boundary = Boundary {
            kind,
            detail: detail.to_string(),
            site: Self::site(function, block, detail),
        };
        if !self.boundaries.contains(&boundary) {
            self.boundaries.push(boundary);
        }
    }

    fn site(function: &str, block: &str, what: &str) -> Site {
        Site {
            function: function.to_string(),
            block: block.to_string(),
            what: what.to_string(),
            hint: source_hint(function),
        }
    }

    // ── reads ───────────────────────────────────────────────────────────────

    /// Taint of a `const {allocN: &T}` read: ambient statics only.
    fn static_taint_of_alloc(
        &self,
        path: &str,
        alloc: &str,
        block: &BasicBlock,
        hops: &[Hop],
    ) -> TaintSet {
        let doc = self.program.doc_of(path);
        let Some(item) = self.program.static_of_alloc(doc, alloc) else {
            return TaintSet::new();
        };
        let ambient = item.is_mut || self.model.is_ambient_type(&item.ty);
        if !ambient {
            return TaintSet::new();
        }
        let name = item
            .path
            .rsplit("::")
            .next()
            .unwrap_or(&item.path)
            .to_string();
        Self::ambient_fact(path, &name, &item.ty, &block.label, hops)
    }

    /// Taint of a `const NAME` / `const f::promoted[0]` / `&/*tls*/ NAME` rvalue.
    fn const_taint(
        &mut self,
        path: &str,
        body: &Body,
        text: &str,
        block: &BasicBlock,
        subst: &Substitution,
        hops: &[Hop],
    ) -> TaintSet {
        let _ = body;
        let candidate = if let Some(rest) = text.trim().strip_prefix("const ") {
            rest.trim()
        } else if let Some(at) = text.find("/*tls*/") {
            text.get(at.saturating_add("/*tls*/".len())..)
                .unwrap_or("")
                .trim()
        } else {
            return TaintSet::new();
        };
        if candidate.is_empty()
            || candidate.starts_with(['{', '"', '\'', '(', '['])
            || candidate.starts_with(|c: char| c.is_ascii_digit())
            || candidate.starts_with("b\"")
            || candidate.starts_with("ZeroSized")
        {
            return TaintSet::new();
        }
        let bare = crate::resolve::strip_generics_everywhere(candidate);
        if bare.contains("promoted[") {
            if self.program.body(&bare).is_some() {
                let hop = Hop {
                    function: path.to_string(),
                    step: format!("reads {bare}"),
                };
                let mut inner = hops.to_vec();
                inner.push(hop);
                let outcome = self.analyze_body(&bare, subst, &[], &inner);
                return outcome.ret;
            }
            return TaintSet::new();
        }
        let Some(item) = self.program.static_named(&bare) else {
            return TaintSet::new();
        };
        if !(item.is_mut || self.model.is_ambient_type(&item.ty)) {
            return TaintSet::new();
        }
        let name = bare.rsplit("::").next().unwrap_or(&bare).to_string();
        let ty = item.ty.clone();
        Self::ambient_fact(path, &name, &ty, &block.label, hops)
    }

    fn ambient_fact(path: &str, name: &str, ty: &str, block: &str, hops: &[Hop]) -> TaintSet {
        let site = Self::site(path, block, &format!("static {name}"));
        let mut chain = hops.to_vec();
        chain.push(Hop {
            function: path.to_string(),
            step: format!("reads ambient static {name}: {ty}"),
        });
        TaintSet::of(Fact {
            kind: TaintKind::Value,
            source: site,
            hops: chain,
        })
    }

    /// An arbitrary raw-pointer dereference is outside the memory model (D7).
    fn raw_pointer_check(
        &mut self,
        path: &str,
        body: &Body,
        reads: &[Operand],
        block: &BasicBlock,
    ) {
        for operand in reads {
            let Some(place) = operand_place(operand) else {
                continue;
            };
            if !place
                .projections
                .iter()
                .any(|p| matches!(p, Projection::Deref))
            {
                continue;
            }
            let Some(ty) = body.locals.get(&place.local) else {
                continue;
            };
            let ty = ty.trim_start();
            // A `*const dyn Trait` is the inside of a `Box`, not a user's raw
            // pointer; flagging it would make every boxed trait object unsafe.
            if (ty.starts_with("*const") || ty.starts_with("*mut")) && !ty.contains("dyn ") {
                self.push_boundary(
                    BoundaryKind::UnsafeRawPointer,
                    &format!("(*_{}): {ty}", place.local.0),
                    path,
                    &block.label,
                );
            }
        }
    }

    /// The declared type of a call's destination, for `dest_type` rule matching.
    fn dest_type(
        body: &Body,
        dest: &Place,
        subst: &Substitution,
        parsed: &CalleePath,
    ) -> Option<String> {
        if dest.projections.is_empty()
            && let Some(ty) = body.locals.get(&dest.local)
        {
            return Some(subst.apply(ty));
        }
        // A `collect` into a coroutine field prints no destination type; its
        // turbofish carries the collection it is building.
        if parsed.last_segment() == "collect" {
            return parsed.generic_args.first().map(|arg| subst.apply(arg));
        }
        None
    }
}

/// The sink and branch records the reporting pass fills in.
struct Report<'r> {
    sinks: &'r mut Vec<SinkRecord>,
    branches: &'r mut Vec<BranchRecord>,
}

fn memo_key(path: &str, subst: &Substitution, args: &[TaintSet]) -> String {
    let mut key = String::with_capacity(path.len().saturating_add(32));
    key.push_str(path);
    key.push('|');
    key.push_str(&subst.key());
    key.push('|');
    for set in args {
        key.push_str(&set.signature());
        key.push(',');
    }
    key
}

fn conservative(args: &[TaintSet]) -> BodyOutcome {
    let ret = union_of(args);
    let mut out = BTreeMap::new();
    for (index, set) in args.iter().enumerate() {
        if !set.is_empty() {
            out.insert(index, set.clone());
        }
    }
    BodyOutcome {
        ret,
        out,
        has_sink: false,
    }
}

fn union_of(sets: &[TaintSet]) -> TaintSet {
    let mut out = TaintSet::new();
    for set in sets {
        out.absorb(set);
    }
    out
}

fn with_hops(hops: &[Hop], site: &Site, step: &str) -> Vec<Hop> {
    let mut out = hops.to_vec();
    out.push(Hop {
        function: site.function.clone(),
        step: step.to_string(),
    });
    out
}

fn subst_note(subst: &Substitution) -> String {
    if subst.is_empty() {
        return String::new();
    }
    let bindings: Vec<String> = subst
        .0
        .iter()
        .map(|(param, ty)| format!("{param} := {ty}"))
        .collect();
    format!(" [{}]", bindings.join(", "))
}

/// The first sentence of a model row's `reason` — short enough for a hop, long
/// enough to name the canonical path the row stands for (`std::process::id`).
fn first_sentence(reason: &str) -> &str {
    let reason = reason.trim();
    reason.find(". ").map_or_else(
        || reason.trim_end_matches('.'),
        |at| reason.get(..at).unwrap_or(reason).trim(),
    )
}

/// The `{closure@..}` / `{async block@..}` brace form inside a type, if any.
fn brace_form(ty: &str) -> Option<String> {
    let at = ty.find('{')?;
    let rest = ty.get(at..)?;
    let end = rest.find('}')?;
    let form = rest.get(..end.saturating_add(1))?;
    form.contains('@').then(|| form.to_string())
}

const fn operand_place(operand: &Operand) -> Option<&Place> {
    match operand {
        Operand::Copy(place) | Operand::Move(place) => Some(place),
        Operand::Const { .. } => None,
    }
}

fn operand_text(operand: &Operand) -> String {
    match operand {
        Operand::Copy(place) => format!("copy _{}", place.local.0),
        Operand::Move(place) => format!("move _{}", place.local.0),
        Operand::Const { text, .. } => text.clone(),
    }
}

/// `file:line:col` recovered from an `<impl at ..>` or `{closure@..}` body path.
fn source_hint(path: &str) -> Option<String> {
    let at = path.find("at ").or_else(|| path.find('@'))?;
    let rest = path.get(at..)?;
    let rest = rest
        .strip_prefix("at ")
        .unwrap_or(rest)
        .trim_start_matches('@');
    let end = rest.find(['>', ':']).map(|_| rest.len())?;
    let text = rest.get(..end)?;
    let head = text.split(": ").next()?;
    (!head.is_empty()).then(|| head.to_string())
}
