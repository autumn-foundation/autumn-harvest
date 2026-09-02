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
use crate::resolve::{Program, Resolution, Substitution};
use crate::util::{last_segment, strip_generics_everywhere};
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
    /// `None` for an indirect call (`_8 = copy _5(..)`), which has no path.
    callee: Option<&'c str>,
    /// The callee operand of an indirect call, for the boundary's detail text.
    indirect: Option<&'c Operand>,
    args: &'c [Operand],
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
        // A body with no blocks (a truncated dump) has nothing to walk and no
        // block to anchor a frame on; it is clean, not conservative, because
        // nothing in it could have connected a source to a sink.
        let Some(first) = body.blocks.first() else {
            return BodyOutcome::default();
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
                    changed |= self.run_block(frame.at(block), &mut state, None);
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
            self.run_block(frame.at(block), &mut state, Some(&mut report));
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
    fn run_block(
        &mut self,
        frame: Frame<'_>,
        state: &mut TaintState,
        mut report: Option<&mut Report<'_>>,
    ) -> bool {
        let mut changed = false;
        for statement in &frame.block.statements {
            let Statement::Assign { dest, rvalue } = statement else {
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
                    set.absorb(&self.static_taint_of_alloc(frame, alloc));
                }
            }
            set.absorb(&self.const_taint(frame, &rvalue.text));
            if report.is_some() {
                self.raw_pointer_check(frame, &rvalue.reads);
            }
            changed |= state.add(dest, &set);
        }

        match &frame.block.terminator {
            Terminator::Call {
                dest,
                callee,
                indirect,
                args,
                ..
            } => {
                let call = CallOperands {
                    dest,
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
            | Terminator::Drop { .. }
            | Terminator::Return
            | Terminator::Unreachable
            | Terminator::Other { .. } => {}
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

        let site = self.call_classes(frame, call.dest, callee);
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
        if let Some(rule) = site.forbidden() {
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

        if let Some(rule) = site.source() {
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
        let closure_taint = self.closure_argument_taint(frame, call, arg_taints);
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
        callee: &str,
    ) -> Rc<CallClasses<'a>> {
        let key = Self::site_key(frame);
        if let Some(hit) = self.classes.get(&key) {
            return Rc::clone(hit);
        }
        let printed = frame.subst.apply(callee);
        let parsed = CalleePath::parse(&printed);
        let declared = Self::dest_type(frame.body, dest, frame.subst, &parsed);
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
        let inner = if matches!(resolution, Resolution::Body(_)) {
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
            Resolution::Body(target) => {
                let qualified = self.program.qualified_name(target);
                let resolved = if qualified == printed {
                    String::new()
                } else {
                    format!(" -> {qualified}")
                };
                let hop = Hop {
                    function: frame.path.to_string(),
                    step: format!(
                        "calls {printed}{resolved}{}{}",
                        self.devirtualized(&CalleePath::parse(printed)),
                        subst_note(&site.subst)
                    ),
                };
                let mut inner_hops = frame.hops.to_vec();
                inner_hops.push(hop.clone());
                let seeded: Vec<TaintSet> =
                    arg_taints.iter().map(|set| set.with_hop(&hop)).collect();
                let outcome = self.analyze_body(target, &site.subst, &seeded, &inner_hops);
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
            Resolution::External(_) => {
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
        let taint = self.closure_argument_taint_inner(frame, call, arg_taints, opaque);
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
    ) -> TaintSet {
        self.closure_argument_taint_inner(frame, call, arg_taints, &BTreeSet::new())
    }

    /// Every closure argument outside `opaque`, analyzed as if it were invoked
    /// here, with its environment seeded from that argument's own taint and its
    /// remaining parameters from everything else the call was given.
    fn closure_argument_taint_inner(
        &mut self,
        frame: Frame<'_>,
        call: CallOperands<'_>,
        arg_taints: &[TaintSet],
        opaque: &BTreeSet<usize>,
    ) -> TaintSet {
        let mut out = TaintSet::new();
        for (index, operand) in call.args.iter().enumerate() {
            if opaque.contains(&index) {
                continue;
            }
            let Some(span) = Self::closure_span_of(frame.body, operand, frame.subst) else {
                continue;
            };
            let Some(target) = self.program.closure_body(&span).map(str::to_string) else {
                continue;
            };
            let hop = Hop {
                function: frame.path.to_string(),
                step: format!("invokes closure {span}"),
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
    fn static_taint_of_alloc(&self, frame: Frame<'_>, alloc: &str) -> TaintSet {
        let doc = self.program.doc_of(frame.path);
        let Some(item) = self.program.static_of_alloc(doc, alloc) else {
            return TaintSet::new();
        };
        let ambient = item.is_mut || self.model.is_ambient_type(&item.ty);
        if !ambient {
            return TaintSet::new();
        }
        let name = last_segment(&item.path).to_string();
        Self::ambient_fact(frame, &name, &item.ty)
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
        let Some(item) = self.program.static_named(&bare) else {
            return TaintSet::new();
        };
        if !(item.is_mut || self.model.is_ambient_type(&item.ty)) {
            return TaintSet::new();
        }
        let name = last_segment(&bare).to_string();
        let ty = item.ty.clone();
        Self::ambient_fact(frame, &name, &ty)
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
