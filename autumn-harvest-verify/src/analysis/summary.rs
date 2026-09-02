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
use std::rc::Rc;

use crate::mir::ast::{BasicBlock, Body, Local, Operand, Place, Projection, Statement, Terminator};
use crate::model::callee::CalleePath;
use crate::model::{CallClass, ForbiddenRule, Model, SanitizerRule, SinkRule, SourceRule};
use crate::resolve::{Ambiguity, Program, Resolution, Substitution};
use crate::util::{last_segment, peel_refs, strip_generics_everywhere};
use crate::verdict::{Boundary, BoundaryKind, Finding, FindingKind, Hop, Site, TaintKind};

use super::control::ControlGraph;
use super::taint::{Fact, TaintSet, TaintState};

/// Fixpoint rounds before the analysis gives up refining a body.
const MAX_ROUNDS: u32 = 24;
/// Implicit-flow injections per fixpoint attempt.
///
/// Each injection can create new tainted branches (a control-derived value read
/// by a later `switchInt`), so the two steps alternate until neither adds a
/// fact. Facts are deduplicated on `(kind, source)` over a finite set of
/// sources, so the alternation terminates well inside this cap.
const MAX_IMPLICIT_PASSES: u32 = 8;
/// Taint of reading one operand. `discriminant` restricts the read to the
/// place and its ancestors (see [`super::taint`]).
fn read_operand(operand: &Operand, state: &TaintState, discriminant: bool) -> TaintSet {
    match operand {
        Operand::Copy(place) | Operand::Move(place) => state.read(place, discriminant),
        Operand::Const { .. } => TaintSet::new(),
    }
}

/// Body analyses per workflow entry.
const BUDGET: u32 = 6000;
/// Native recursion depth of [`Analyzer::analyze_body`] before it gives up.
///
/// The analyzer descends into callees on the *native* stack, so a long enough
/// call chain overflows it — a `SIGABRT` that reads as an infrastructure
/// failure rather than as "the tool gave up". One level costs roughly 8 KB in a
/// debug build (`analyze_body` → `run_body` → `run_block` → `transfer_call` →
/// `follow_call`), so the cliff is near 900 levels on the CLI's 8 MB main stack
/// but only ~230 on the 2 MB stack a spawned thread — a test harness thread
/// included — gets by default. The cap is chosen for the smaller of the two and
/// is still an order of magnitude above the deepest chain any real workflow
/// has: the whole corpus stays under ten.
const MAX_DEPTH: usize = 96;
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

/// Where the analyzer currently is: the body being walked, the block inside it,
/// and the context the body was entered with.
///
/// These five values are threaded verbatim through every transfer function and
/// never change inside one, so they travel as one `Copy` value rather than as
/// five parameters repeated down the call chain.
#[derive(Clone, Copy)]
struct Frame<'f> {
    /// MIR path of the body being analyzed.
    path: &'f str,
    body: &'f Body,
    block: &'f BasicBlock,
    /// The generic substitution this body is being analyzed under.
    subst: &'f Substitution,
    /// The hop chain from the workflow entry to this body.
    hops: &'f [Hop],
}

impl<'f> Frame<'f> {
    /// The same frame, moved to another block of the same body.
    const fn at(self, block: &'f BasicBlock) -> Self {
        Self { block, ..self }
    }
}

/// The parts of a `Call` terminator the transfer functions read.
#[derive(Clone, Copy)]
struct CallOperands<'c> {
    dest: &'c Place,
    /// The destination's inline type annotation, for a projected destination.
    dest_ty: Option<&'c str>,
    /// `None` for an indirect call (`_8 = copy _5(..)`), which has no path.
    callee: Option<&'c str>,
    /// The callee operand of an indirect call, for the boundary's detail text.
    indirect: Option<&'c Operand>,
    args: &'c [Operand],
}

/// One higher-order argument being invoked: which argument it is, the operand
/// that carries it, and the body it names.
#[derive(Clone, Copy)]
struct InvokedArgument<'i> {
    index: usize,
    operand: &'i Operand,
    /// The closure span or fn-item path, for the hop text.
    span: &'i str,
    /// The body to analyze.
    target: &'i str,
    /// Parameter 0 is a closure environment rather than a real parameter.
    has_env: bool,
    /// `[ambiguous closure (N candidates, unioned)]`, or empty.
    note: &'i str,
}

/// Everything about one call site that is fixed once `(body, substitution)` is:
/// the substituted callee text, its decomposition, and the model rows it matches.
///
/// The fixpoint visits every block up to `3 * MAX_ROUNDS + 1` times, and each
/// visit used to re-run `Substitution::apply`, `CalleePath::parse` and
/// `Model::classify` over the same text — all three of which scan the path
/// character by character. Caching them turns the analyzer's one accidental
/// quadratic (rounds x call sites) back into a linear pass.
struct CallClasses<'m> {
    printed: String,
    parsed: CalleePath,
    classes: Vec<CallClass<'m>>,
}

impl<'m> CallClasses<'m> {
    /// The `[[forbidden]]` row, if the model attached one.
    fn forbidden(&self) -> Option<&'m ForbiddenRule> {
        self.classes.iter().find_map(|c| match c {
            CallClass::Forbidden(rule) => Some(*rule),
            _ => None,
        })
    }

    /// The `[[sink]]` row, if the model attached one.
    fn sink(&self) -> Option<&'m SinkRule> {
        self.classes.iter().find_map(|c| match c {
            CallClass::Sink(rule) => Some(*rule),
            _ => None,
        })
    }

    /// The `[[source]]` row, if the model attached one.
    fn source(&self) -> Option<&'m SourceRule> {
        self.classes.iter().find_map(|c| match c {
            CallClass::Source(rule) => Some(*rule),
            _ => None,
        })
    }

    /// The `[[sanitizer]]` row, if the model attached one.
    fn sanitizer(&self) -> Option<&'m SanitizerRule> {
        self.classes.iter().find_map(|c| match c {
            CallClass::Sanitizer(rule) => Some(*rule),
            _ => None,
        })
    }

    /// The `[[reduction]]` row, if the model attached one.
    fn reduction(&self) -> Option<&'m SanitizerRule> {
        self.classes.iter().find_map(|c| match c {
            CallClass::Reduction(rule) => Some(*rule),
            _ => None,
        })
    }

    /// The name of an unmodelled `WorkflowContext` method — an honest boundary.
    fn unmodeled_ctx_method(&self) -> Option<&str> {
        self.classes.iter().find_map(|c| match c {
            CallClass::UnmodeledCtxMethod(name) => Some(name.as_str()),
            _ => None,
        })
    }

    /// The call registers a handler closure, which is analyzed entry-adjacent.
    fn registers_handler(&self) -> bool {
        self.classes
            .iter()
            .any(|c| matches!(c, CallClass::HandlerRegistration(_)))
    }

    /// The call is a ctx primitive whose return value is recorded in history
    /// (`[[sanctioned]]`) or is pure observability (`[[non_sink]]`) — either
    /// way it starts nothing and propagates nothing.
    fn is_clean_ctx_call(&self) -> bool {
        self.classes
            .iter()
            .any(|c| matches!(c, CallClass::Sanctioned(_) | CallClass::NonSink(_)))
    }
}

/// Where a call site goes, and under which substitution.
///
/// Split from [`CallClasses`] because resolution is only asked for on the calls
/// the model does *not* decide, and [`Program::call_substitution_in`] scans
/// every block of the caller to find the call site — an O(blocks) answer that
/// must be computed once per site, never once per round.
struct CallTarget {
    resolution: Resolution,
    subst: Substitution,
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
    /// Per-call-site classification, keyed on `body|block|substitution`.
    classes: HashMap<String, Rc<CallClasses<'a>>>,
    /// Per-call-site resolution, keyed the same way.
    targets: HashMap<String, Rc<CallTarget>>,
    stack: Vec<String>,
    budget: u32,
    /// Findings collected across every body reached from this entry.
    pub findings: Vec<Finding>,
    /// Boundaries collected across every body reached from this entry.
    pub boundaries: Vec<Boundary>,
    /// Report warnings: name collisions the analysis had to resolve
    /// conservatively. Deduplicated, because the fixpoint revisits a block.
    pub warnings: BTreeSet<String>,
}

impl<'a> Analyzer<'a> {
    #[must_use]
    pub fn new(program: &'a Program, model: &'a Model) -> Self {
        Self {
            program,
            model,
            memo: HashMap::new(),
            classes: HashMap::new(),
            targets: HashMap::new(),
            stack: Vec::new(),
            budget: BUDGET,
            findings: Vec::new(),
            boundaries: Vec::new(),
            warnings: BTreeSet::new(),
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
        if self.stack.len() >= MAX_DEPTH {
            self.push_boundary(
                BoundaryKind::Recursion,
                &format!("call chain deeper than {MAX_DEPTH} bodies at {path}"),
                path,
                "bb0",
            );
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
        // A body with no blocks (a truncated dump) has nothing to walk and no
        // block to anchor a frame on; it is clean, not conservative, because
        // nothing in it could have connected a source to a sink.
        let Some(first) = body.blocks.first() else {
            // A body whose block structure did not parse is not "clean": it is a
            // body the analysis never saw. Anything else here would turn a
            // truncated dump into a silent `proven`.
            self.push_boundary(
                BoundaryKind::MirParse,
                &format!("{path}: the body has no parsed blocks"),
                path,
                "bb0",
            );
            return conservative(args);
        };
        let frame = Frame {
            path,
            body,
            block: first,
            subst,
            hops,
        };
        let mut state = TaintState::new();
        let mut kills: Vec<(Place, TaintKind)> = Vec::new();
        // The CFG never changes while the facts do, so post-dominance is
        // computed at most once per body and shared by implicit flow and by the
        // control-dependent-sink pass.
        let mut graph: Option<ControlGraph> = None;
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
            for _pass in 0..MAX_IMPLICIT_PASSES {
                for _round in 0..MAX_ROUNDS {
                    let mut changed = false;
                    for block in &body.blocks {
                        changed |= self.run_block(frame.at(block), &mut state, None);
                    }
                    changed |= state.apply_kills();
                    if !changed {
                        break;
                    }
                }
                state.apply_kills();
                if !implicit_flow(frame, body, &mut graph, &mut state) {
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
            self.run_block(frame.at(block), &mut state, Some(&mut report));
        }
        self.control_dependent_sinks(path, body, &mut graph, &sinks, &branches);

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
    fn run_block(
        &mut self,
        frame: Frame<'_>,
        state: &mut TaintState,
        report: Option<&mut Report<'_>>,
    ) -> bool {
        let mut changed = false;
        for statement in &frame.block.statements {
            let Statement::Assign { dest, rvalue } = statement else {
                if report.is_some()
                    && let Statement::Other(text) = statement
                    && !is_benign_statement(text)
                {
                    self.push_boundary(
                        BoundaryKind::MirParse,
                        text.trim(),
                        frame.path,
                        &frame.block.label,
                    );
                }
                continue;
            };
            // `&x` and `&mut x` alias the referent identically for taint: a
            // shared borrow of a tainted place reads it, a unique one also
            // writes it back, and `TaintState` models both as one canonical
            // place — so the mutability flag is deliberately not consulted.
            if let Some((place, _)) = &rvalue.ref_of
                && dest.projections.is_empty()
            {
                state.alias(dest.local, place);
            } else if dest.projections.is_empty()
                && let Some(place) = copied_reference(frame.body, dest, rvalue)
            {
                // Copying a reference makes a second name for the same referent.
                // A closure's captured-by-reference field is reached exactly
                // this way — `_3 = copy ((*_1).0: &mut u64); (*_3) = ...` — and
                // without the alias the write lands on a local the caller has no
                // name for.
                state.alias(dest.local, &place);
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
                    let alloc = alloc.clone();
                    let text = rvalue.text.clone();
                    set.absorb(&self.static_taint_of_alloc(frame, &alloc, &text));
                }
            }
            set.absorb(&self.const_taint(frame, &rvalue.text));
            if report.is_some() {
                self.raw_pointer_check(frame, &rvalue.reads);
            }
            changed |= state.add(dest, &set);
        }

        changed |= self.run_terminator(frame, state, report);
        changed
    }

    /// The terminator half of [`Self::run_block`].
    fn run_terminator(
        &mut self,
        frame: Frame<'_>,
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let mut changed = false;
        match &frame.block.terminator {
            Terminator::Call {
                dest,
                dest_ty,
                callee,
                indirect,
                args,
                ..
            } => {
                let call = CallOperands {
                    dest,
                    dest_ty: dest_ty.as_deref(),
                    callee: callee.as_deref(),
                    indirect: indirect.as_ref(),
                    args,
                };
                changed |= self.transfer_call(frame, call, state, report.as_deref_mut());
            }
            Terminator::SwitchInt { operand, targets } => {
                if targets.len() >= 2
                    && let Some(report) = report
                {
                    let facts = read_operand(operand, state, false);
                    if !facts.is_empty() {
                        report.branches.push(BranchRecord {
                            block: frame.block.label.clone(),
                            facts,
                            what: operand_text(operand),
                        });
                    }
                }
            }
            Terminator::Assert { .. }
            | Terminator::Goto { .. }
            | Terminator::Return
            | Terminator::Unreachable => {}
            Terminator::Drop { place, .. } => {
                changed |= self.transfer_drop(frame, place, state, report.as_deref_mut());
            }
            Terminator::Other { text, .. } => {
                if report.is_some() && !is_benign_terminator(text) {
                    let detail = if text.trim().is_empty() {
                        "an empty block (truncated dump)".to_string()
                    } else {
                        text.trim().to_string()
                    };
                    self.push_boundary(
                        BoundaryKind::MirParse,
                        &detail,
                        frame.path,
                        &frame.block.label,
                    );
                }
            }
            Terminator::InlineAsm { .. } => {
                if report.is_some() {
                    self.push_boundary(
                        BoundaryKind::InlineAsm,
                        "asm!",
                        frame.path,
                        &frame.block.label,
                    );
                }
            }
        }
        changed
    }

    // ── calls ───────────────────────────────────────────────────────────────

    /// One call terminator: classify it against the model, and let the first
    /// class that applies decide it.
    ///
    /// The order of the arms is the model's documented decision order
    /// ([`crate::model::matcher`]), and each arm is total — it returns without
    /// falling through — because a call that is a sink *and* a source must be
    /// treated as exactly one of them, never as both in sequence.
    fn transfer_call(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let emit = report.is_some();
        let arg_taints: Vec<TaintSet> = call
            .args
            .iter()
            .map(|operand| read_operand(operand, state, false))
            .collect();
        let union = union_of(&arg_taints);

        let Some(callee) = call.callee else {
            if emit {
                let detail = call
                    .indirect
                    .map_or_else(|| frame.block.label.clone(), operand_text);
                self.push_boundary(
                    BoundaryKind::IndirectCall,
                    &detail,
                    frame.path,
                    &frame.block.label,
                );
            }
            return state.add(call.dest, &union);
        };

        let site = self.call_classes(frame, call.dest, call.dest_ty, callee);
        // A reborrow hands back the *same* place under a new name. Without this
        // edge `xs.sort()` lowers to `sort(deref_mut(&mut xs))` and the sanitizer
        // would clear the taint of a temporary instead of the vector's.
        if call.dest.projections.is_empty()
            && REBORROWS.contains(&site.parsed.last_segment())
            && let Some(place) = call.args.first().and_then(operand_place)
        {
            state.alias(call.dest.local, place);
        }

        // Every class the model can attach to a call, in decision order.
        if let Some(rule) = site.forbidden()
            && self.bare_row_applies(frame, &site.printed, &rule.path, rule.receiver.as_deref())
        {
            if emit {
                self.record_forbidden(frame, &site.printed, rule, report.as_deref_mut());
            }
            return false;
        }

        if let Some(rule) = site.sink() {
            let offset = usize::from(site.parsed.receiver.is_some());
            if emit {
                let at = Self::site(frame.path, &frame.block.label, &site.printed);
                self.record_sink(rule, offset, call.args, &arg_taints, &at);
                if let Some(report) = report {
                    report.sinks.push(SinkRecord {
                        block: frame.block.label.clone(),
                        site: at,
                    });
                }
            }
            let opaque: BTreeSet<usize> = rule
                .opaque_closure_args
                .iter()
                .map(|index| index.saturating_add(offset))
                .collect();
            self.descend_closures(frame, call, &arg_taints, state, &opaque);
            // A command's result is recorded in history and replayed verbatim.
            return false;
        }

        if site.registers_handler() {
            self.descend_closures(frame, call, &arg_taints, state, &BTreeSet::new());
            return false;
        }

        if site.is_clean_ctx_call() {
            return false;
        }

        if let Some(rule) = site.source()
            && self.bare_row_applies(frame, &site.printed, &rule.path, rule.receiver.as_deref())
        {
            return self.transfer_source(frame, call, rule, &site.printed, &arg_taints, state);
        }

        if let Some(rule) = site.sanitizer() {
            return self.transfer_sanitizer(frame, call, rule, &arg_taints, state);
        }

        if let Some(rule) = site.reduction() {
            let mut cleared = BTreeSet::new();
            cleared.insert(rule.clears);
            return state.add(call.dest, &union.without(&cleared));
        }

        if let Some(name) = site.unmodeled_ctx_method() {
            if emit {
                let detail = name.to_string();
                self.push_boundary(
                    BoundaryKind::UnmodeledCtxMethod,
                    &detail,
                    frame.path,
                    &frame.block.label,
                );
            }
            return state.add(call.dest, &union);
        }

        // Nothing in the model decides this call: follow it.
        self.follow_call(frame, call, &site.printed, &arg_taints, state, report)
    }

    /// A `[[forbidden]]` effect is a finding on reachability alone, and it also
    /// counts as a sink so that reaching it only on a tainted branch is one too.
    fn record_forbidden(
        &mut self,
        frame: Frame<'_>,
        printed: &str,
        rule: &ForbiddenRule,
        report: Option<&mut Report<'_>>,
    ) {
        let at = Self::site(frame.path, &frame.block.label, printed);
        self.findings.push(Finding {
            kind: FindingKind::ForbiddenEffect,
            taint: TaintKind::Value,
            source: at.clone(),
            sink: at.clone(),
            trace: with_hops(frame.hops, &at, &format!("calls {printed}")),
            message: format!(
                "forbidden effect {printed} reachable from {} ({})",
                frame.path,
                first_sentence(&rule.reason)
            ),
        });
        if let Some(report) = report {
            report.sinks.push(SinkRecord {
                block: frame.block.label.clone(),
                site: at,
            });
        }
    }

    /// A `[[source]]` call: start a fact, unless the row is keyed on an ambient
    /// receiver type and *this* receiver carries no taint.
    ///
    /// That guard is the whole reason `[[ambient_type]]` exists: a `Mutex`
    /// created inside the workflow body reaching `lock()` is not a source, while
    /// the identical call on a `static` one — whose read already tainted the
    /// receiver — is.
    fn transfer_source(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        rule: &SourceRule,
        printed: &str,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
    ) -> bool {
        let ambient = rule
            .receiver
            .as_deref()
            .is_some_and(|receiver| self.model.is_ambient_type(receiver));
        let receiver_tainted = arg_taints.first().is_some_and(|set| !set.is_empty());
        let mut set = union_of(arg_taints);
        if !ambient || receiver_tainted {
            let at = Self::site(frame.path, &frame.block.label, printed);
            let mut hop_chain = frame.hops.to_vec();
            hop_chain.push(Hop {
                function: frame.path.to_string(),
                step: format!("calls {printed} ({})", first_sentence(&rule.reason)),
            });
            set.insert(Fact {
                kind: rule.kind,
                source: at,
                hops: hop_chain,
            });
        }
        self.descend_closures(frame, call, arg_taints, state, &BTreeSet::new());
        let mut changed = state.add(call.dest, &set);
        changed |= Self::write_back_refs(frame.body, call.args, &set, state, None);
        changed
    }

    /// A `[[sanitizer]]` call: clear the row's kind on the receiver and the
    /// result — unless a closure argument is itself tainted.
    ///
    /// A comparator that reads ambient state does not *clear* the order, it
    /// decides it: `sort_by(|a, b| ..COUNTER..)` is an `Order` source, not a
    /// sanitizer.
    fn transfer_sanitizer(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        rule: &SanitizerRule,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
    ) -> bool {
        let union = union_of(arg_taints);
        let closure_taint = self.closure_argument_taint(frame, call, arg_taints, state);
        if closure_taint.is_empty() {
            if let Some(receiver) = call.args.first().and_then(operand_place) {
                state.kill(receiver, rule.clears);
            }
            state.kill(call.dest, rule.clears);
            let mut cleared = BTreeSet::new();
            cleared.insert(rule.clears);
            return state.add(call.dest, &union.without(&cleared));
        }
        let ordered = closure_taint.as_kind(rule.clears);
        let mut changed = state.add(call.dest, &union);
        changed |= state.add(call.dest, &ordered);
        changed |= Self::write_back_refs(frame.body, call.args, &ordered, state, None);
        changed
    }

    // ── per-call-site caches ────────────────────────────────────────────────

    /// The key both caches use: a call site is identified by its body, its
    /// block and the substitution the body is being analyzed under.
    fn site_key(frame: Frame<'_>) -> String {
        let mut key = String::with_capacity(frame.path.len().saturating_add(24));
        key.push_str(frame.path);
        key.push('|');
        key.push_str(&frame.block.label);
        key.push('|');
        key.push_str(&frame.subst.key());
        key
    }

    /// The substituted callee text, its decomposition and its model rows.
    fn call_classes(
        &mut self,
        frame: Frame<'_>,
        dest: &Place,
        dest_ty: Option<&str>,
        callee: &str,
    ) -> Rc<CallClasses<'a>> {
        let key = Self::site_key(frame);
        if let Some(hit) = self.classes.get(&key) {
            return Rc::clone(hit);
        }
        let printed = frame.subst.apply(callee);
        let parsed = CalleePath::parse(&printed);
        let declared = Self::dest_type(frame.body, dest, dest_ty, frame.subst, &parsed);
        let classes = self.model.classify(&parsed, declared.as_deref());
        let facts = Rc::new(CallClasses {
            printed,
            parsed,
            classes,
        });
        self.classes.insert(key, Rc::clone(&facts));
        facts
    }

    /// Where the call goes, and the substitution it induces on the callee.
    fn call_target(&mut self, frame: Frame<'_>, printed: &str, callee: &str) -> Rc<CallTarget> {
        let key = Self::site_key(frame);
        if let Some(hit) = self.targets.get(&key) {
            return Rc::clone(hit);
        }
        let resolution = self.program.resolve_call(frame.path, printed);
        // Only a call that lands in an analyzed body needs a substitution, and
        // computing one costs a scan of the caller's blocks.
        let inner = if matches!(resolution, Resolution::Body(_) | Resolution::Bodies(..)) {
            self.program
                .call_substitution_in(frame.path, callee, frame.subst)
        } else {
            Substitution::new()
        };
        let target = Rc::new(CallTarget {
            resolution,
            subst: inner,
        });
        self.targets.insert(key, Rc::clone(&target));
        target
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

    /// Analyze one resolved callee body and fold its outcome into the caller.
    ///
    /// `note` is appended to the hop text and is non-empty only when the call
    /// site was ambiguous and every candidate is being unioned.
    #[allow(clippy::too_many_arguments)]
    fn descend_into(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        printed: &str,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
        report: Option<&mut Report<'_>>,
        target: &str,
        subst: &Substitution,
        note: &str,
    ) -> bool {
        let qualified = self.program.qualified_name(target);
        let resolved = if qualified == printed {
            String::new()
        } else {
            format!(" -> {qualified}")
        };
        let hop = Hop {
            function: frame.path.to_string(),
            step: format!(
                "calls {printed}{resolved}{}{}{note}",
                self.devirtualized(&CalleePath::parse(printed)),
                subst_note(subst)
            ),
        };
        let mut inner_hops = frame.hops.to_vec();
        inner_hops.push(hop.clone());
        let seeded: Vec<TaintSet> = arg_taints.iter().map(|set| set.with_hop(&hop)).collect();
        let outcome = self.analyze_body(target, subst, &seeded, &inner_hops);
        let mut changed = state.add(call.dest, &outcome.ret);
        changed |= Self::write_back_refs(
            frame.body,
            call.args,
            &TaintSet::new(),
            state,
            Some(&outcome),
        );
        self.descend_closures(frame, call, arg_taints, state, &BTreeSet::new());
        if outcome.has_sink
            && let Some(report) = report
        {
            // A helper that emits commands makes *this call site* a sink
            // for control dependence: `if tainted { dispatch(ctx) }` is
            // the same finding as an inline `ctx.execute_activity_raw`.
            report.sinks.push(SinkRecord {
                block: frame.block.label.clone(),
                site: Self::site(
                    frame.path,
                    &frame.block.label,
                    &format!("commands emitted by {qualified}"),
                ),
            });
        }
        changed
    }

    /// The hop note for a call site whose candidates are all being analyzed, and
    /// the report warning that goes with it.
    ///
    /// Empty when there is only one candidate: nothing was unioned, so there is
    /// nothing for a reader to second-guess.
    fn union_note(
        &mut self,
        caller: &str,
        printed: &str,
        kind: Ambiguity,
        bodies: &[String],
    ) -> String {
        if bodies.len() < 2 {
            return String::new();
        }
        let noun = kind.noun();
        self.warnings.insert(format!(
            "`{printed}` in `{caller}` resolves to {} {noun} bodies in the analyzed \
             set ({}); all of them were analyzed and their findings unioned",
            bodies.len(),
            bodies
                .iter()
                .map(|b| self.program.qualified_name(b))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        format!(" [ambiguous {noun} ({} candidates, unioned)]", bodies.len())
    }

    /// Resolve and descend into an unmodelled call.
    fn follow_call(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        printed: &str,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
        report: Option<&mut Report<'_>>,
    ) -> bool {
        let emit = report.is_some();
        let union = union_of(arg_taints);
        let site = self.call_target(frame, printed, call.callee.unwrap_or_default());
        match &site.resolution {
            Resolution::Body(target) => self.descend_into(
                frame,
                call,
                printed,
                arg_taints,
                state,
                report,
                target,
                &site.subst,
                "",
            ),
            Resolution::Bodies(bodies, kind) => {
                // Two analyzed bodies answer to this printed callee and the text
                // does not say which. For an impl, narrowing on the receiver's
                // declared type — which MIR prints fully qualified — usually
                // picks one; a closure has no such handle, because its printed
                // type IS the span that collided. When narrowing does not
                // decide, EVERY candidate is analyzed and the results are
                // unioned, so a finding in any of them is reported. Picking one
                // would be a coin flip that can hide it.
                let narrowed = match kind {
                    Ambiguity::Closure => bodies.clone(),
                    Ambiguity::Impl => call
                        .args
                        .first()
                        .and_then(operand_place)
                        .and_then(|place| frame.body.locals.get(&place.local))
                        .map(|declared| frame.subst.apply(declared))
                        .map_or_else(
                            || bodies.clone(),
                            |declared| self.program.narrow_by_receiver(bodies, &declared),
                        ),
                };
                let note = self.union_note(frame.path, printed, *kind, &narrowed);
                let mut report = report;
                let mut changed = false;
                for target in &narrowed {
                    changed |= self.descend_into(
                        frame,
                        call,
                        printed,
                        arg_taints,
                        state,
                        report.as_deref_mut(),
                        target,
                        &site.subst,
                        &note,
                    );
                }
                changed
            }
            Resolution::External(_) => {
                if emit && !self.is_trusted_bodyless(frame, call, printed) {
                    self.push_boundary(
                        BoundaryKind::ExternalCrateBody,
                        printed,
                        frame.path,
                        &frame.block.label,
                    );
                }
                let hop = Hop {
                    function: frame.path.to_string(),
                    step: format!("calls {printed}"),
                };
                self.descend_closures(frame, call, arg_taints, state, &BTreeSet::new());
                let mut changed = state.add(call.dest, &union.with_hop(&hop));
                changed |= Self::write_back_refs(frame.body, call.args, &union, state, None);
                changed
            }
            Resolution::Boundary(kind, detail) => {
                if emit {
                    self.push_boundary(*kind, detail, frame.path, &frame.block.label);
                }
                state.add(call.dest, &union)
            }
        }
    }

    /// Is this body-less callee provably std/core/alloc (hence modelled), rather
    /// than a body the analysis never saw?
    ///
    /// rustc prints **trimmed** def-paths, so the overwhelming majority of std
    /// calls arrive as `String::clone` or `format` with no crate root at all —
    /// and so does `now_ish` from a dependency that was never asked for MIR.
    /// Treating every trimmed path as a boundary would make every workflow
    /// `unknown`; treating every trimmed path as an opaque propagator is the
    /// silent `proven` the soundness review found.
    ///
    /// The discriminator has to be evidence about the **callee**. A declared
    /// type at the call site is not: `now_ish() -> std::string::String` names
    /// std in its result and is still a dependency's body. Three things count:
    ///
    ///  1. the callee path text itself is rooted at std/core/alloc or at a
    ///     `[[trusted]]` crate — including the qualifying trait of a
    ///     `<T as std::future::IntoFuture>::into_future`;
    ///  2. the call is a **method**, and its receiver type is std — either a
    ///     primitive (the language reserves inherent impls on those, and rustc
    ///     prints them trimmed: `<u64 as From<u32>>::from`), or the declared type
    ///     of its *receiver argument* is rooted entirely in trusted crates
    ///     (`Vec::<u32>::push(move _12, ..)` with `_12: &mut std::vec::Vec<u32>`),
    ///     or — for an associated function, which has no receiver argument —
    ///     some declared type at the site spells the receiver type itself with a
    ///     trusted root (`DateTime::<Utc>::from_timestamp_millis` returns a
    ///     `std::option::Option<chrono::DateTime<chrono::Utc>>`);
    ///  3. the callee matches a `[[std_free_fn]]` row — the residue of (2), free
    ///     functions with no receiver to reason about.
    ///
    /// (2) is refused when the call goes through a trait some impl in the
    /// analyzed set implements: a user `impl MyTrait for Vec<u32>` is user code,
    /// and `Vec`'s std-ness says nothing about it. In practice `resolve_call`
    /// finds that impl's body first and never gets here, so this is a belt on
    /// top of braces.
    fn is_trusted_bodyless(&self, frame: Frame<'_>, call: CallOperands<'_>, printed: &str) -> bool {
        let parsed = CalleePath::parse(printed);
        if self.model.is_std_free_fn(&parsed) {
            return true;
        }
        // (1) A crate root spelled in the callee PATH itself.
        if path_roots(&callee_path_text(printed)).any(|root| self.is_trusted_root(root)) {
            return true;
        }
        let Some(receiver) = parsed.receiver.as_deref() else {
            return false;
        };
        if let Some(trait_name) = parsed.trait_.as_deref()
            && self
                .program
                .has_impl_method(receiver, Some(trait_name), parsed.last_segment())
        {
            return false;
        }
        // (2) A primitive self type, or a receiver the call site declares with
        // a std-rooted path.
        if PRIMITIVE_TYPES.contains(&receiver) {
            return true;
        }
        // The receiver ARGUMENT: `_1` of a method call is the self value, and
        // MIR declares it fully qualified even where the callee path is trimmed
        // (`<DefaultCallsite as Callsite>::metadata(move _17)` with
        // `_17: &tracing::__macro_support::MacroCallsite`). It is evidence about
        // the callee precisely because it *is* the callee's self type; every
        // root in it must be trusted, so a `&dep::Worker` receiver never is.
        if let Some(ty) = call
            .args
            .first()
            .and_then(operand_place)
            .and_then(|place| frame.body.locals.get(&place.local))
        {
            let ty = frame.subst.apply(ty);
            let ty = peel_refs(&ty);
            let mut roots = path_roots(ty).peekable();
            if roots.peek().is_some() && roots.all(|root| self.is_trusted_root(root)) {
                return true;
            }
        }
        // An ASSOCIATED function has no self argument (`Vec::<u32>::new()`,
        // `DateTime::<Utc>::from_timestamp_millis(0_i64)`), so the receiver has
        // to be found by name among the declared types instead.
        let declared = call
            .args
            .iter()
            .filter_map(operand_place)
            .filter_map(|place| frame.body.locals.get(&place.local))
            .map(String::as_str)
            .chain(frame.body.locals.get(&call.dest.local).map(String::as_str))
            .chain(call.dest_ty);
        for ty in declared {
            let ty = frame.subst.apply(ty);
            if receiver_roots(&ty, receiver).any(|root| self.is_trusted_root(root)) {
                return true;
            }
        }
        false
    }

    /// `std`/`core`/`alloc` or a `[[trusted]]` crate.
    fn is_trusted_root(&self, root: &str) -> bool {
        matches!(root, "std" | "core" | "alloc")
            || self.model.trusted.iter().any(|c| c.name == root)
    }

    /// Does a model row that is keyed on a **bare** name apply at this call site?
    ///
    /// `[[forbidden]] path = "sleep"` and `[[source]] path = "var"` exist because
    /// rustc trims `std::thread::sleep` and `std::env::var` to one segment. The
    /// same suffix match would fire on a user's own `fn sleep`, so a
    /// single-segment row with no receiver applies only where the callee has no
    /// body in the analyzed set — a user function with a body is analyzed
    /// instead, which is strictly better information.
    fn bare_row_applies(
        &self,
        frame: Frame<'_>,
        printed: &str,
        rule_path: &str,
        receiver: Option<&str>,
    ) -> bool {
        if receiver.is_some() || rule_path.contains("::") {
            return true;
        }
        !self.program.names_analyzed_body(frame.path, printed)
    }

    /// A `drop` terminator on a place whose type has a user `impl Drop`.
    ///
    /// Drop glue is a call the user never wrote and MIR never spells out, so it
    /// is invisible to every other transfer function — yet a `Drop` impl is
    /// ordinary code that can read ambient state and emit commands. The glue is
    /// followed with the dropped place as the `&mut self` argument, exactly as
    /// an explicit `Ty::drop(&mut place)` would be.
    ///
    /// Types with no user `Drop` impl (every std type, and any plain struct) run
    /// no user code here, so they contribute nothing. A type whose glue the
    /// analyzer *cannot* pin down is the opposite case and must never be
    /// confused with it: it becomes a [`BoundaryKind::DropGlue`], because a
    /// missing header says nothing about what the glue does.
    ///
    /// Where several same-named types' glue bodies survive the module
    /// narrowing, every one of them is followed and the results unioned, for
    /// the same reason an ambiguous call is.
    fn transfer_drop(
        &mut self,
        frame: Frame<'_>,
        place: &Place,
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let Some(declared) = frame.body.locals.get(&place.local) else {
            return false;
        };
        let declared = frame.subst.apply(declared);
        let targets = self.program.drop_targets(&declared);
        if let Some(detail) = &targets.unresolved
            && report.is_some()
        {
            self.push_boundary(
                BoundaryKind::DropGlue,
                detail,
                frame.path,
                &frame.block.label,
            );
        }
        let note = self.union_note(
            frame.path,
            &format!("drop({declared})"),
            Ambiguity::Impl,
            &targets.bodies,
        );
        let mut changed = false;
        for target in &targets.bodies {
            if self.program.body(target).is_none() {
                if report.is_some() {
                    self.push_boundary(
                        BoundaryKind::DropGlue,
                        &format!("{declared}: the `Drop` impl body is not in the analyzed set"),
                        frame.path,
                        &frame.block.label,
                    );
                }
                continue;
            }
            let hop = Hop {
                function: frame.path.to_string(),
                step: format!(
                    "drops {declared} -> {}{note}",
                    self.program.qualified_name(target)
                ),
            };
            let mut inner_hops = frame.hops.to_vec();
            inner_hops.push(hop.clone());
            let seeded = vec![state.read(place, false).with_hop(&hop)];
            let outcome = self.analyze_body(target, &Substitution::new(), &seeded, &inner_hops);
            changed |= state.add(place, &outcome.ret);
            if let Some(written) = outcome.out.get(&0) {
                changed |= state.add(place, written);
            }
            if outcome.has_sink
                && let Some(report) = report.as_deref_mut()
            {
                report.sinks.push(SinkRecord {
                    block: frame.block.label.clone(),
                    site: Self::site(
                        frame.path,
                        &frame.block.label,
                        &format!("commands emitted while dropping {declared}"),
                    ),
                });
            }
        }
        changed
    }

    /// Analyze every closure passed as an argument, assuming it is invoked, and
    /// fold what it returns into the call's destination.
    fn descend_closures(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
        opaque: &BTreeSet<usize>,
    ) {
        let taint = self.closure_argument_taint_inner(frame, call, arg_taints, state, opaque);
        if !taint.is_empty() {
            state.add(call.dest, &taint);
        }
    }

    /// The taint a call's closure arguments contribute to its result.
    fn closure_argument_taint(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
    ) -> TaintSet {
        self.closure_argument_taint_inner(frame, call, arg_taints, state, &BTreeSet::new())
    }

    /// Every closure argument outside `opaque`, analyzed as if it were invoked
    /// here, with its environment seeded from that argument's own taint and its
    /// remaining parameters from everything else the call was given.
    fn closure_argument_taint_inner(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
        opaque: &BTreeSet<usize>,
    ) -> TaintSet {
        let mut out = TaintSet::new();
        for (index, operand) in call.args.iter().enumerate() {
            if opaque.contains(&index) {
                continue;
            }
            // A closure argument and a bare `fn` item argument are the same
            // thing to a higher-order callee: both are assumed invoked. They
            // differ only in whether parameter 0 is an environment.
            let closure = Self::closure_span_of(frame.body, operand, frame.subst).map(|span| {
                let bodies = self.program.closure_bodies_near(frame.path, &span);
                (span, bodies)
            });
            // A fn item that names two impl bodies (`Worker::run` where two
            // modules define one) is followed into all of them, and so is a
            // `{closure@..}` span two macro expansions share, for the same
            // reason `follow_call` unions an ambiguous call: skipping it, or
            // picking one, can lose the finding.
            let mut note = String::new();
            let targets: Vec<(String, String, bool)> = match closure {
                Some((_, bodies)) if bodies.is_empty() => Vec::new(),
                Some((span, bodies)) => {
                    note = self.union_note(frame.path, &span, Ambiguity::Closure, &bodies);
                    bodies
                        .into_iter()
                        .map(|body| (span.clone(), body, true))
                        .collect()
                }
                None => self
                    .fn_item_targets(frame, operand)
                    .into_iter()
                    .map(|path| (path.clone(), path, false))
                    .collect(),
            };
            for (span, target, has_env) in targets {
                self.invoke_argument_body(
                    frame,
                    InvokedArgument {
                        index,
                        operand,
                        span: &span,
                        target: &target,
                        has_env,
                        note: &note,
                    },
                    arg_taints,
                    state,
                    &mut out,
                );
            }
        }
        out
    }

    /// One `(argument, body)` pair from [`Self::closure_argument_taint_inner`]:
    /// analyze the body as if the higher-order callee invoked it, and fold its
    /// return taint into `out`.
    fn invoke_argument_body(
        &mut self,
        frame: Frame<'_>,
        invoked: InvokedArgument<'_>,
        arg_taints: &[TaintSet],
        state: &mut TaintState,
        out: &mut TaintSet,
    ) {
        {
            let InvokedArgument {
                index,
                operand,
                span,
                target,
                has_env,
                note,
            } = invoked;
            let hop = Hop {
                function: frame.path.to_string(),
                step: if has_env {
                    format!("invokes closure {span}{note}")
                } else {
                    format!("invokes fn item {span}{note}")
                },
            };
            let mut inner_hops = frame.hops.to_vec();
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
            let Some(callee_body) = self.program.body(target) else {
                return;
            };
            let mut seeded = Vec::with_capacity(callee_body.params.len());
            if has_env {
                seeded.push(
                    arg_taints
                        .get(index)
                        .cloned()
                        .unwrap_or_default()
                        .with_hop(&hop),
                );
            }
            while seeded.len() < callee_body.params.len() {
                seeded.push(others.with_hop(&hop));
            }
            let outcome = self.analyze_body(target, &Substitution::new(), &seeded, &inner_hops);
            out.absorb(&outcome.ret);
            // What the closure wrote through its environment is written back
            // onto the locals it captured, which is the only way a capture-by-
            // reference mutation reaches the caller.
            if has_env && let Some(written) = outcome.out.get(&0) {
                Self::write_back_closure_captures(frame.body, operand, written, state);
            }
        }
    }

    /// Every body a bare `fn` item argument names, if the analyzed set has it.
    ///
    /// A fn item is a ZST: MIR passes it as the constant `add_clock`, and its
    /// type is spelled `fn(u64) -> u64 {add_clock}`. Neither carries a
    /// `{closure@..}` span, so the closure path never sees it — yet
    /// `.map(Uuid::new_v4)`, `.unwrap_or_else(Instant::now)` and
    /// `.or_insert_with(SystemTime::now)` are all this shape.
    fn fn_item_targets(&self, frame: Frame<'_>, operand: &Operand) -> Vec<String> {
        let candidate = match operand {
            Operand::Const { text, .. } => {
                let text = frame.subst.apply(text);
                fn_item_path(&text).unwrap_or(text)
            }
            Operand::Copy(place) | Operand::Move(place) => {
                let Some(declared) = frame.body.locals.get(&place.local) else {
                    return Vec::new();
                };
                let declared = frame.subst.apply(declared);
                let Some(path) = fn_item_path(&declared) else {
                    return Vec::new();
                };
                path
            }
        };
        let candidate = candidate.trim();
        if candidate.is_empty() || !candidate.starts_with(is_path_start) {
            return Vec::new();
        }
        let bare = strip_generics_everywhere(candidate);
        match self.program.resolve_call(frame.path, &bare) {
            Resolution::Body(target) => vec![target],
            Resolution::Bodies(targets, _) => targets,
            Resolution::External(_) | Resolution::Boundary(..) => Vec::new(),
        }
    }

    /// Write a closure's environment taint onto the places it captured.
    ///
    /// The captures are the operands of the `{closure@..} { field: move _4, .. }`
    /// aggregate that built the value, so the write-back is a scan of the
    /// caller's own body for that construction. Both the captured place and its
    /// referent are tainted: a capture is either the value itself (`move`) or a
    /// reference to it (`move _4` where `_4 = &mut _2`).
    fn write_back_closure_captures(
        body: &Body,
        operand: &Operand,
        written: &TaintSet,
        state: &mut TaintState,
    ) -> bool {
        let Some(place) = operand_place(operand) else {
            return false;
        };
        let root = state.canonical(place).local;
        let mut changed = false;
        for block in &body.blocks {
            for statement in &block.statements {
                let Statement::Assign { dest, rvalue } = statement else {
                    continue;
                };
                if dest.local != root
                    || !dest.projections.is_empty()
                    || !rvalue.text.contains("{closure@")
                {
                    continue;
                }
                for captured in &rvalue.reads {
                    let Some(captured) = operand_place(captured) else {
                        continue;
                    };
                    changed |= state.add(captured, written);
                    let mut through = captured.clone();
                    through.projections.push(Projection::Deref);
                    changed |= state.add(&through, written);
                }
            }
        }
        changed
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
                        // A directly invoked closure (`<{closure@..} as FnMut>::
                        // call_mut(&mut _3, ..)`) writes through its environment
                        // exactly here; the captures live in the aggregate that
                        // built `_3`.
                        changed |= Self::write_back_closure_captures(body, operand, written, state);
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
        graph: &mut Option<ControlGraph>,
        sinks: &[SinkRecord],
        branches: &[BranchRecord],
    ) {
        if sinks.is_empty() || branches.is_empty() {
            return;
        }
        let graph = graph.get_or_insert_with(|| ControlGraph::new(body));
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
    ///
    /// `text` is the whole operand, whose pointee type (`&Atomic<u64>`) is the
    /// tie-breaker when the footer's name is ambiguous.
    fn static_taint_of_alloc(&mut self, frame: Frame<'_>, alloc: &str, text: &str) -> TaintSet {
        let doc = self.program.doc_of(frame.path);
        let candidates = self.program.statics_of_alloc(doc, alloc);
        self.ambient_of_candidates(frame, &candidates, alloc_pointee(text))
    }

    /// Classify the `static` items one printed name could denote.
    ///
    /// One candidate is the ordinary case. Several mean the name was a bare
    /// last segment two modules both define: the pointee type printed at the
    /// read disambiguates when it picks exactly one, and otherwise the answer
    /// must cover every candidate — ambient if **any** of them is — because
    /// choosing one is exactly the coin flip that made a read of an
    /// `AtomicU64` look like a read of a `u64` that shared its name.
    fn ambient_of_candidates(
        &mut self,
        frame: Frame<'_>,
        candidates: &[&crate::mir::ast::StaticItem],
        pointee: Option<&str>,
    ) -> TaintSet {
        let narrowed: Vec<&crate::mir::ast::StaticItem> = match pointee {
            Some(pointee) if candidates.len() > 1 => {
                let by_type: Vec<&crate::mir::ast::StaticItem> = candidates
                    .iter()
                    .copied()
                    .filter(|item| same_type_name(&item.ty, pointee))
                    .collect();
                if by_type.len() == 1 {
                    by_type
                } else {
                    candidates.to_vec()
                }
            }
            _ => candidates.to_vec(),
        };
        let ambient: Vec<&crate::mir::ast::StaticItem> = narrowed
            .iter()
            .copied()
            .filter(|item| item.is_mut || self.model.is_ambient_type(&item.ty))
            .collect();
        if narrowed.len() > 1 {
            let names: Vec<&str> = narrowed.iter().map(|item| item.path.as_str()).collect();
            self.warnings.insert(format!(
                "the static read in `{}` could be any of {}; it is classified as {}                  because that is the conservative answer over all of them",
                frame.path,
                names.join(", "),
                if ambient.is_empty() { "deterministic" } else { "ambient" }
            ));
        }
        let Some(item) = ambient.first() else {
            return TaintSet::new();
        };
        Self::ambient_fact(frame, &item.path.clone(), &item.ty.clone())
    }

    /// Taint of a `const NAME` / `const f::promoted[0]` / `&/*tls*/ NAME` rvalue.
    fn const_taint(&mut self, frame: Frame<'_>, text: &str) -> TaintSet {
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
        let bare = strip_generics_everywhere(candidate);
        if bare.contains("promoted[") {
            if self.program.body(&bare).is_some() {
                let hop = Hop {
                    function: frame.path.to_string(),
                    step: format!("reads {bare}"),
                };
                let mut inner = frame.hops.to_vec();
                inner.push(hop);
                let outcome = self.analyze_body(&bare, frame.subst, &[], &inner);
                return outcome.ret;
            }
            return TaintSet::new();
        }
        let candidates = self.program.statics_named_all(&bare);
        self.ambient_of_candidates(frame, &candidates, None)
    }

    fn ambient_fact(frame: Frame<'_>, name: &str, ty: &str) -> TaintSet {
        let site = Self::site(frame.path, &frame.block.label, &format!("static {name}"));
        let mut chain = frame.hops.to_vec();
        chain.push(Hop {
            function: frame.path.to_string(),
            step: format!("reads ambient static {name}: {ty}"),
        });
        TaintSet::of(Fact {
            kind: TaintKind::Value,
            source: site,
            hops: chain,
        })
    }

    /// An arbitrary raw-pointer dereference is outside the memory model (D7).
    fn raw_pointer_check(&mut self, frame: Frame<'_>, reads: &[Operand]) {
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
            let Some(ty) = frame.body.locals.get(&place.local) else {
                continue;
            };
            let ty = ty.trim_start();
            // A `*const dyn Trait` is the inside of a `Box`, not a user's raw
            // pointer; flagging it would make every boxed trait object unsafe.
            if (ty.starts_with("*const") || ty.starts_with("*mut")) && !ty.contains("dyn ") {
                self.push_boundary(
                    BoundaryKind::UnsafeRawPointer,
                    &format!("(*_{}): {ty}", place.local.0),
                    frame.path,
                    &frame.block.label,
                );
            }
        }
    }

    /// The declared type of a call's destination, for `dest_type` rule matching.
    fn dest_type(
        body: &Body,
        dest: &Place,
        dest_ty: Option<&str>,
        subst: &Substitution,
        parsed: &CalleePath,
    ) -> Option<String> {
        if dest.projections.is_empty()
            && let Some(ty) = body.locals.get(&dest.local)
        {
            return Some(subst.apply(ty));
        }
        // A projected destination — every local of an `async` workflow lives in
        // the coroutine's state — carries its type in the place syntax instead.
        if let Some(ty) = dest_ty {
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

/// `fn(u64) -> u64 {add_clock}` → `add_clock`; a plain path is returned as-is
/// by the caller.
fn fn_item_path(ty: &str) -> Option<String> {
    let at = ty.rfind('{')?;
    let rest = ty.get(at.saturating_add(1)..)?;
    let end = rest.find('}')?;
    let path = rest.get(..end)?.trim();
    (!path.is_empty() && !path.contains('@') && path.starts_with(is_path_start))
        .then(|| path.to_string())
}

/// Type names the language, not a crate, defines. A method on one of these is a
/// `core` impl however rustc chose to print its path.
const PRIMITIVE_TYPES: &[&str] = &[
    "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize", "f32",
    "f64", "bool", "char", "str",
];

/// Every crate root spelled in a path or type text: each identifier that is
/// followed by `::` and is itself the *start* of a path.
///
/// MIR prints `let` declarations fully qualified even where it trims callee
/// paths, which is what makes this a usable discriminator at all:
/// `&chrono::DateTime<chrono::Utc>` names `chrono` twice, and
/// `<T as std::future::IntoFuture>::into_future` names `std`.
fn path_roots(text: &str) -> impl Iterator<Item = &str> {
    let bytes = text.as_bytes();
    text.match_indices("::").filter_map(move |(at, _)| {
        let mut start = at;
        while start > 0
            && bytes
                .get(start.saturating_sub(1))
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            start = start.saturating_sub(1);
        }
        if start == at {
            return None;
        }
        // A root is not preceded by another path segment.
        let before = bytes.get(start.saturating_sub(1));
        if start > 0 && before.is_some_and(|b| *b == b':' || *b == b'.') {
            return None;
        }
        let root = text.get(start..at)?;
        root.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
            .then_some(root)
    })
}

/// The first character of something that could be a path (`add_clock`, `Uuid`,
/// `<u64 as ..>`), as opposed to a literal (`0_u64`, `"c"`, `()`).
const fn is_path_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == '<'
}

/// A `dest = copy/move (*PLACE).f` whose destination is declared as a
/// reference: a **reborrow of a field read through a reference**.
///
/// MIR reaches a closure's captured `&mut` this way — `_3 = no_retag copy
/// ((*_1).0: &mut u64); (*_3) = ...` — and without the alias the write lands on
/// `_3`, a local the caller has no name for, so the capture never comes back.
///
/// The source place must go *through* a `Deref`. A copy of a field of the local
/// itself (`_54 = copy (_1.0)`, how a coroutine reaches its own state through
/// `Pin`) is deliberately excluded: aliasing there would collapse `_54` and
/// `(*_54)` onto one canonical place, and the coroutine's resume-state
/// `discriminant((*_54))` — exactly-read so that it stays clean — would start
/// seeing every fact the body ever put on that local.
fn copied_reference(body: &Body, dest: &Place, rvalue: &crate::mir::ast::Rvalue) -> Option<Place> {
    let text = rvalue.text.trim();
    let text = text.strip_prefix("no_retag ").unwrap_or(text);
    if !text.starts_with("copy ") && !text.starts_with("move ") {
        return None;
    }
    let declared = body.locals.get(&dest.local)?.trim_start();
    if !declared.starts_with('&')
        && !declared.starts_with("*mut")
        && !declared.starts_with("*const")
    {
        return None;
    }
    let [operand] = rvalue.reads.as_slice() else {
        return None;
    };
    let place = operand_place(operand)?;
    place
        .projections
        .iter()
        .any(|p| matches!(p, Projection::Deref))
        .then(|| place.clone())
}

/// Implicit flow: a value produced *by* a tainted branch carries that branch's
/// taint (D4/D5).
///
/// `Control` taint alone only flags command emissions the branch decides. It
/// says nothing about the *values* the branch decides, so the standard
/// laundering idiom — read ambient state, branch on it, return one of two
/// constants — would otherwise wash a source out completely:
///
/// ```text
/// let shard = if COUNTER.load(SeqCst) % 2 == 0 { 0 } else { 1 };
/// ctx.execute_activity_raw("charge".into(), shard)   // shard is not a constant
/// ```
///
/// Every place written in a block that is control-dependent on a tainted
/// `switchInt` — a statement's destination or a call's destination — therefore
/// gains that branch's facts, re-labelled [`TaintKind::Value`] and carrying a
/// hop that names the branch. Post-dominating blocks are excluded by
/// [`ControlGraph::is_control_dependent`], which is what keeps the code *after*
/// an `if` clean.
///
/// Returns `true` when anything new landed, so the caller can iterate: a
/// control-derived value can itself decide a later branch.
fn implicit_flow(
    frame: Frame<'_>,
    body: &Body,
    graph: &mut Option<ControlGraph>,
    state: &mut TaintState,
) -> bool {
    let branches = tainted_branches(body, state);
    if branches.is_empty() {
        return false;
    }
    let graph = graph.get_or_insert_with(|| ControlGraph::new(body));
    let mut changed = false;
    for branch in &branches {
        let Some(branch_at) = graph.index_of(&branch.block) else {
            continue;
        };
        let hop = Hop {
            function: frame.path.to_string(),
            step: format!(
                "control-dependent on tainted {} at {}",
                branch.what, branch.block
            ),
        };
        let implicit = branch.facts.as_kind(TaintKind::Value).with_hop(&hop);
        if implicit.is_empty() {
            continue;
        }
        for (at, block) in body.blocks.iter().enumerate() {
            if !graph.is_control_dependent(at, branch_at) {
                continue;
            }
            for statement in &block.statements {
                if let Statement::Assign { dest, .. } = statement {
                    changed |= state.add(dest, &implicit);
                }
            }
            if let Terminator::Call { dest, .. } = &block.terminator {
                changed |= state.add(dest, &implicit);
            }
        }
    }
    changed
}

/// Every `switchInt` whose operand carries taint, read against `state`.
///
/// Deliberately side-effect free: the reporting pass collects the same records
/// while it emits findings, but the fixpoint needs them without emitting
/// anything.
fn tainted_branches(body: &Body, state: &TaintState) -> Vec<BranchRecord> {
    let mut out = Vec::new();
    for block in &body.blocks {
        let Terminator::SwitchInt { operand, targets } = &block.terminator else {
            continue;
        };
        if targets.len() < 2 {
            continue;
        }
        let facts = read_operand(operand, state, false);
        if facts.is_empty() {
            continue;
        }
        out.push(BranchRecord {
            block: block.label.clone(),
            facts,
            what: operand_text(operand),
        });
    }
    out
}

/// Statement heads that carry no value flow, so the parser folding them into
/// [`Statement::Other`] loses nothing.
///
/// Every *other* unrecognised statement is a `mir-parse` boundary: it is a shape
/// this parser was not written against, and the honest answer to "what does it
/// do" is "unknown", not "nothing". `discriminant` covers the
/// `discriminant(_3) = 1;` spelling of `SetDiscriminant`.
const BENIGN_STATEMENT_HEADS: &[&str] = &[
    "StorageLive",
    "StorageDead",
    "FakeRead",
    "PlaceMention",
    "nop",
    "Retag",
    "AscribeUserType",
    "Deinit",
    "SetDiscriminant",
    "ConstEvalCounter",
    "Coverage",
    "Intrinsic",
    "BackwardIncompatibleDropHint",
    "discriminant",
];

/// Terminator heads that end a path without deciding anything the taint
/// analysis models (unwinding, coroutine machinery, `false` CFG edges).
const BENIGN_TERMINATOR_HEADS: &[&str] = &[
    "resume",
    "abort",
    "terminate",
    "coroutine_drop",
    "yield",
    "falseEdge",
    "falseUnwind",
    "unwind",
    "return",
    "unreachable",
    "goto",
    "drop",
    "switchInt",
    "assert",
];

/// The leading identifier of a statement or terminator line.
fn head_of(text: &str) -> &str {
    let text = text.trim();
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    text.get(..end).unwrap_or(text)
}

fn is_benign_statement(text: &str) -> bool {
    BENIGN_STATEMENT_HEADS.contains(&head_of(text))
}

fn is_benign_terminator(text: &str) -> bool {
    BENIGN_TERMINATOR_HEADS.contains(&head_of(text))
}

/// The sink and branch records the reporting pass fills in.
struct Report<'r> {
    sinks: &'r mut Vec<SinkRecord>,
    branches: &'r mut Vec<BranchRecord>,
}

/// Crate roots of every fully-qualified spelling of `receiver` inside a
/// declared type.
///
/// MIR prints `let` declarations fully qualified even where it trims the callee
/// path, so a declared type that *contains* the receiver type says which crate
/// the receiver is from — and a receiver is the one thing at a call site that
/// is genuinely about the callee. The receiver is often not the whole type:
/// `DateTime::<Utc>::from_timestamp_millis` takes an `i64` and returns
/// `std::option::Option<chrono::DateTime<chrono::Utc>>`, whose `chrono::DateTime`
/// is the spelling wanted here.
///
/// Only paths whose LAST segment is exactly the receiver count, so the
/// `std::option::Option` wrapper and the `chrono::Utc` parameter contribute
/// nothing: a std-rooted type that is not the receiver is not evidence about
/// the callee, which is the whole point of the rule.
fn receiver_roots<'t>(ty: &'t str, receiver: &'t str) -> impl Iterator<Item = &'t str> {
    ty.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
        .filter(move |token| last_segment(token) == receiver)
        .filter_map(crate::util::crate_root)
}

/// The part of a callee path that names the callee, for a crate-root scan.
///
/// A qualified header is kept whole, because its self type and its trait are
/// both statements about the callee (`<T as std::future::IntoFuture>::into_future`
/// is std's). Turbofish arguments are dropped, because they are statements
/// about the *caller's* type arguments: `helper::<std::string::String>` from a
/// dependency compiled without `--emit=mir` names std without being std.
fn callee_path_text(printed: &str) -> String {
    let text = printed.trim();
    if text.starts_with('<')
        && let Some(close) = crate::util::matching_angle(text)
        && let (Some(header), Some(rest)) =
            (text.get(..=close), text.get(close.saturating_add(1)..))
    {
        return format!("{header}{}", strip_generics_everywhere(rest));
    }
    strip_generics_everywhere(text)
}

/// `const {alloc2: &Atomic<u64>}` → `&Atomic<u64>`.
///
/// The pointee is the one piece of type information printed at a static read,
/// and it is what tells `a::COUNTER: u64` from `b::COUNTER: AtomicU64` when the
/// footer's name was too short to.
fn alloc_pointee(text: &str) -> Option<&str> {
    let at = text.find("{alloc")?;
    let rest = text.get(at..)?;
    let colon = rest.find(':')?;
    let close = rest.rfind('}')?;
    let inner = rest.get(colon.saturating_add(1)..close)?.trim();
    (!inner.is_empty()).then_some(inner)
}

/// Do a static's declared type and a printed pointee name the same type?
fn same_type_name(declared: &str, pointee: &str) -> bool {
    let want = crate::model::callee::TypeName::parse(pointee).name;
    !want.is_empty() && crate::model::callee::TypeName::parse(declared).name == want
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
    let head = rest.split(": ").next()?;
    (!head.is_empty()).then(|| head.to_string())
}
