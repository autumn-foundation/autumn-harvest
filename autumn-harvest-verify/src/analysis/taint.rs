//! The taint lattice: facts, place-keyed state, aliasing and sanitizer kills (D4).
//!
//! A *fact* is one reason a place is non-deterministic: a kind
//! ([`TaintKind`]), the [`Site`] the taint started at, and the hop chain from
//! the workflow entry to that site. Facts are deduplicated on
//! `(kind, source function, source site)` and their hop chain is frozen the
//! first time the fact reaches a place, which is what makes the fixpoint
//! terminate: the fact *set* only grows, over a finite universe of sources, and
//! a fact already present is never rewritten.
//!
//! # Read semantics (the rule the corpus pins)
//!
//! A read of place `P` is tainted when `P` itself, an **ancestor** of `P`
//! (`_9` for `((_9 as Some).0).1`) or a **descendant** of `P` (`_3.0` when the
//! whole tuple `_3` is read) carries a fact. The descendant direction is what
//! makes `format!` work: the tainted `u64` is buried in a tuple that is then
//! borrowed wholesale into `Arguments::new`.
//!
//! `discriminant(P)` is the single exception: it reads only `P` and its
//! ancestors, never a descendant. Without that, every `async` body would be
//! control-tainted the moment any workflow argument was — the coroutine's
//! resume-state switch reads `discriminant((*_8))` while the workflow's own
//! locals live in `(*_8).1`, `(*_8).2`, ... .

use std::collections::{BTreeMap, BTreeSet};

use crate::mir::ast::{Local, Place, Projection};
use crate::verdict::{Hop, Site, TaintKind};

use super::control::ControlGraph;

/// How many distinct facts of **one kind** one place keeps (see
/// [`TaintSet::insert`]). Beyond this the analysis has all the evidence a
/// report can use for that kind, and more only slows the fixpoint down.
const MAX_FACTS: usize = 6;
/// How long a hop chain may grow before its middle is elided.
const MAX_HOPS: usize = 48;

/// One reason a place is non-deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    pub kind: TaintKind,
    pub source: Site,
    pub hops: Vec<Hop>,
}

impl Fact {
    /// The identity a fact is deduplicated on.
    #[must_use]
    pub const fn key(&self) -> (TaintKind, &str, &str) {
        (
            self.kind,
            self.source.function.as_str(),
            self.source.what.as_str(),
        )
    }

    /// The same fact, reached through one more call.
    #[must_use]
    pub fn with_hop(&self, hop: &Hop) -> Self {
        let mut hops = self.hops.clone();
        if hops.last() != Some(hop) {
            if hops.len() >= MAX_HOPS {
                hops.remove(hops.len() / 2);
            }
            hops.push(hop.clone());
        }
        Self {
            kind: self.kind,
            source: self.source.clone(),
            hops,
        }
    }

    /// The same fact seen as a different kind (an `Order` source collapsing into
    /// a `Value`, or either becoming `Control` at a branch).
    #[must_use]
    pub fn as_kind(&self, kind: TaintKind) -> Self {
        Self {
            kind,
            source: self.source.clone(),
            hops: self.hops.clone(),
        }
    }
}

/// A set of facts, deduplicated and bounded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaintSet {
    facts: Vec<Fact>,
}

impl TaintSet {
    /// The empty (clean) set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A set holding exactly one fact.
    #[must_use]
    pub fn of(fact: Fact) -> Self {
        Self { facts: vec![fact] }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.facts.is_empty()
    }

    #[must_use]
    pub fn facts(&self) -> &[Fact] {
        &self.facts
    }

    /// The kinds present.
    #[must_use]
    pub fn kinds(&self) -> BTreeSet<TaintKind> {
        self.facts.iter().map(|f| f.kind).collect()
    }

    /// True when a fact of `kind` is present.
    #[must_use]
    pub fn has(&self, kind: TaintKind) -> bool {
        self.facts.iter().any(|f| f.kind == kind)
    }

    /// Add one fact; `true` when it was not already present.
    ///
    /// [`MAX_FACTS`] caps facts **per kind**, not the set as a whole. A
    /// place that already holds six `Order` facts must still accept the
    /// first `Value` fact that reaches it. A single global cap let six
    /// `Order` sources fill a collection's slots before a later
    /// clock-derived `Value` source arrived. The `Value` fact was then
    /// silently dropped. A subsequent `sort` cleared the retained `Order`
    /// facts, and the place read as clean even though it carried real
    /// non-determinism.
    pub fn insert(&mut self, fact: Fact) -> bool {
        if self.facts.iter().any(|f| f.key() == fact.key()) {
            return false;
        }
        let same_kind = self.facts.iter().filter(|f| f.kind == fact.kind).count();
        if same_kind >= MAX_FACTS {
            return false;
        }
        self.facts.push(fact);
        true
    }

    /// Union in every fact of `other`; `true` when anything was added.
    pub fn absorb(&mut self, other: &Self) -> bool {
        let mut changed = false;
        for fact in &other.facts {
            changed |= self.insert(fact.clone());
        }
        changed
    }

    /// The same facts with one more hop appended to each.
    #[must_use]
    pub fn with_hop(&self, hop: &Hop) -> Self {
        Self {
            facts: self.facts.iter().map(|f| f.with_hop(hop)).collect(),
        }
    }

    /// The facts whose kind is not in `kinds`.
    #[must_use]
    pub fn without(&self, kinds: &BTreeSet<TaintKind>) -> Self {
        Self {
            facts: self
                .facts
                .iter()
                .filter(|f| !kinds.contains(&f.kind))
                .cloned()
                .collect(),
        }
    }

    /// Every fact re-labelled as `kind` (deduplicated).
    #[must_use]
    pub fn as_kind(&self, kind: TaintKind) -> Self {
        let mut out = Self::new();
        for fact in &self.facts {
            out.insert(fact.as_kind(kind));
        }
        out
    }

    /// A stable signature for memoisation: which sources of which kinds.
    #[must_use]
    pub fn signature(&self) -> String {
        let mut keys: Vec<String> = self
            .facts
            .iter()
            .map(|f| format!("{:?}:{}:{}", f.kind, f.source.function, f.source.what))
            .collect();
        keys.sort();
        keys.join("|")
    }
}

/// One sanitizer kill: `place` lost `kind` at the call site in `block`.
///
/// The block is what makes a kill flow-sensitive (see
/// [`TaintState::read_at`]). MIR always lowers a method call as a block
/// **terminator**. Two calls in sequence — the sink
/// `ctx.execute_activity_raw(.., keys.clone())` and the later
/// `keys.sort()` — therefore never share a block. A read's own block
/// pins it to one side of the kill or the other. Block-level dominance is
/// enough, with no statement position needed within a block.
type Kill = (Place, TaintKind, String);

/// The taint of every place in one body, plus its aliases and sanitizer kills.
#[derive(Debug, Default)]
pub struct TaintState {
    /// Root local → (projections, facts).
    places: BTreeMap<Local, Vec<(Vec<Projection>, TaintSet)>>,
    /// Local → the place it is a reference to (`_6 = &mut _4` ⇒ `_6` ↦ `_4`).
    aliases: BTreeMap<Local, Place>,
    /// Places a sanitizer cleared, of which kinds, and where.
    kills: Vec<Kill>,
}

impl TaintState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `local` is a reference to `target`.
    pub fn alias(&mut self, local: Local, target: &Place) {
        let canonical = self.canonical(target);
        if canonical.local == local {
            return;
        }
        self.aliases.insert(local, canonical);
    }

    /// The place a place really names, following `&`/`&mut` aliases.
    #[must_use]
    pub fn canonical(&self, place: &Place) -> Place {
        let mut current = place.clone();
        for _ in 0..8 {
            let Some(target) = self.aliases.get(&current.local) else {
                return current;
            };
            let mut projections = target.projections.clone();
            // `*_6` where `_6 ↦ _4` is `_4`; `_6.0` where `_6 ↦ _4` is `_4.0`.
            let tail = current
                .projections
                .split_first()
                .filter(|(first, _)| matches!(first, Projection::Deref))
                .map_or(current.projections.as_slice(), |(_, rest)| rest);
            projections.extend(tail.iter().cloned());
            let next = Place {
                local: target.local,
                projections,
            };
            if next == current {
                return current;
            }
            current = next;
        }
        current
    }

    /// Record that a sanitizer at `block` cleared `kind` on `place` (and
    /// everything under it).
    ///
    /// Kills are **monotone**: once discovered, a kill is never retracted.
    /// [`Self::read_at`] decides — per read, from the read's own block —
    /// whether it applies. That is what makes a flow-insensitive round loop
    /// still converge. The kill *set* only grows, over a finite universe of
    /// call sites, so re-running a round with more kills in hand cannot
    /// oscillate.
    pub fn kill(&mut self, place: &Place, kind: TaintKind, block: &str) {
        let canonical = self.canonical(place);
        if !self
            .kills
            .iter()
            .any(|(p, k, b)| *p == canonical && *k == kind && b == block)
        {
            self.kills.push((canonical, kind, block.to_string()));
        }
    }

    /// Start with the kills a previous attempt discovered.
    pub fn seed_kills(&mut self, kills: &[Kill]) {
        self.kills = kills.to_vec();
    }

    /// The kills discovered so far.
    #[must_use]
    pub fn kills(&self) -> &[Kill] {
        &self.kills
    }

    /// Which kinds are killed for `place`, as observed from `at`. A kill
    /// applies only when its own block **dominates** `at`, i.e. every path
    /// from the entry to `at` passes through the sanitizer call. A read
    /// whose block the kill does not dominate happens on a path that could
    /// not have gone through the sanitizer yet, or at all. It must keep
    /// seeing the pre-sanitizer taint.
    fn killed_kinds_at(&self, place: &Place, at: usize, graph: &ControlGraph) -> BTreeSet<TaintKind> {
        self.kills
            .iter()
            .filter(|(p, _, block)| {
                covers(p, place)
                    && graph
                        .index_of(block)
                        .is_some_and(|kill_at| graph.dominates(kill_at, at))
            })
            .map(|(_, k, _)| *k)
            .collect()
    }

    /// Taint of a read of `place`, **not** filtered by any sanitizer kill.
    ///
    /// `discriminant_only` restricts the read to `place` and its ancestors
    /// (see the module docs). Used where no block context is available:
    /// tests, and callers that read the fully-accumulated state rather
    /// than one program point. [`Self::read_at`] is the flow-sensitive
    /// counterpart every transfer function inside a body uses.
    #[must_use]
    pub fn read(&self, place: &Place, discriminant_only: bool) -> TaintSet {
        let place = self.canonical(place);
        let mut out = TaintSet::new();
        let Some(entries) = self.places.get(&place.local) else {
            return out;
        };
        for (projections, set) in entries {
            let ancestor = is_prefix(projections, &place.projections);
            let descendant = is_prefix(&place.projections, projections);
            if ancestor || (descendant && !discriminant_only) {
                out.absorb(set);
            }
        }
        out
    }

    /// [`Self::read`], filtered to the kills that dominate `at_block`.
    #[must_use]
    pub fn read_at(
        &self,
        place: &Place,
        discriminant_only: bool,
        at_block: &str,
        graph: &ControlGraph,
    ) -> TaintSet {
        let raw = self.read(place, discriminant_only);
        if self.kills.is_empty() || raw.is_empty() {
            return raw;
        }
        let Some(at) = graph.index_of(at_block) else {
            return raw;
        };
        let killed = self.killed_kinds_at(&self.canonical(place), at, graph);
        if killed.is_empty() { raw } else { raw.without(&killed) }
    }

    /// Taint of every place rooted at `local` (how an out-parameter is read
    /// back), **not** filtered by any sanitizer kill. See [`Self::read`].
    #[must_use]
    pub fn read_root(&self, local: Local) -> TaintSet {
        let mut out = TaintSet::new();
        for (_, set) in self.places.get(&local).into_iter().flatten() {
            out.absorb(set);
        }
        out
    }

    /// [`Self::read_root`], filtered to the kills that dominate `at_block`.
    #[must_use]
    pub fn read_root_at(&self, local: Local, at_block: &str, graph: &ControlGraph) -> TaintSet {
        let raw = self.read_root(local);
        if self.kills.is_empty() || raw.is_empty() {
            return raw;
        }
        let Some(at) = graph.index_of(at_block) else {
            return raw;
        };
        let root = Place {
            local,
            projections: Vec::new(),
        };
        let killed = self.killed_kinds_at(&root, at, graph);
        if killed.is_empty() { raw } else { raw.without(&killed) }
    }

    /// Add `set` to `place`; `true` when anything new landed.
    ///
    /// Kills are never consulted here: storage keeps every fact exactly as
    /// it was observed. A kill only ever hides facts from a *read* whose
    /// block it dominates ([`Self::read_at`]). Stripping at write time, as
    /// an earlier revision did, destroyed the very distinction this exists
    /// to keep. A value read before the sanitizer ran would already be gone
    /// by the time anything asked for it.
    pub fn add(&mut self, place: &Place, set: &TaintSet) -> bool {
        if set.is_empty() {
            return false;
        }
        let place = self.canonical(place);
        let entries = self.places.entry(place.local).or_default();
        if let Some((_, existing)) = entries
            .iter_mut()
            .find(|(projections, _)| *projections == place.projections)
        {
            return existing.absorb(set);
        }
        entries.push((place.projections, set.clone()));
        true
    }

    /// Every tainted place, for diagnostics and tests.
    #[must_use]
    pub fn tainted_places(&self) -> Vec<(Place, &TaintSet)> {
        let mut out = Vec::new();
        for (local, entries) in &self.places {
            for (projections, set) in entries {
                if !set.is_empty() {
                    out.push((
                        Place {
                            local: *local,
                            projections: projections.clone(),
                        },
                        set,
                    ));
                }
            }
        }
        out
    }
}

/// True when `outer` is `inner` or an ancestor of it.
fn covers(outer: &Place, inner: &Place) -> bool {
    outer.local == inner.local && is_prefix(&outer.projections, &inner.projections)
}

/// True when `short` is a prefix of `long` (both are projection lists).
fn is_prefix(short: &[Projection], long: &[Projection]) -> bool {
    short.len() <= long.len() && long.get(..short.len()).is_some_and(|head| head == short)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(what: &str) -> Site {
        Site {
            function: "f".to_string(),
            block: "bb0".to_string(),
            what: what.to_string(),
            hint: None,
        }
    }

    fn fact(what: &str, kind: TaintKind) -> Fact {
        Fact {
            kind,
            source: site(what),
            hops: Vec::new(),
        }
    }

    fn place(local: u32, projections: &[Projection]) -> Place {
        Place {
            local: Local(local),
            projections: projections.to_vec(),
        }
    }

    fn one(what: &str) -> TaintSet {
        TaintSet::of(fact(what, TaintKind::Value))
    }

    #[test]
    fn a_read_sees_an_ancestors_taint() {
        let mut state = TaintState::new();
        state.add(&place(9, &[]), &one("src"));
        let read = state.read(&place(9, &[Projection::Field(0), Projection::Deref]), false);
        assert!(!read.is_empty(), "taint of `_9` reaches `(*(_9.0))`");
    }

    #[test]
    fn a_read_sees_a_descendants_taint_but_a_discriminant_read_does_not() {
        let mut state = TaintState::new();
        state.add(&place(3, &[Projection::Field(1)]), &one("src"));
        assert!(
            !state.read(&place(3, &[]), false).is_empty(),
            "reading the whole tuple sees the tainted field (this is `format!`)"
        );
        assert!(
            state.read(&place(3, &[]), true).is_empty(),
            "`discriminant(_3)` must NOT see `_3.1` — otherwise every async \
             coroutine's resume switch is control-tainted"
        );
    }

    #[test]
    fn aliases_are_bidirectional_through_the_canonical_place() {
        let mut state = TaintState::new();
        state.alias(Local(6), &place(4, &[]));
        state.add(&place(6, &[Projection::Deref]), &one("src"));
        assert!(
            !state.read(&place(4, &[]), false).is_empty(),
            "`*_6 = x` where `_6 = &mut _4` taints `_4`"
        );
        assert!(!state.read(&place(6, &[]), false).is_empty());
    }

    /// `bb0 -> bb1 -> bb2`, straight line: `bb1` dominates `bb2` but not `bb0`.
    fn chain_graph() -> ControlGraph {
        let text = "fn f() -> u8 {\n    let mut _0: u8;\n\n\
                     bb0: {\n        goto -> bb1;\n    }\n\n\
                     bb1: {\n        goto -> bb2;\n    }\n\n\
                     bb2: {\n        return;\n    }\n}\n";
        let doc = crate::mir::parse("test", "t.mir", text);
        let body = doc.bodies.first().cloned().expect("one body");
        ControlGraph::new(&body)
    }

    #[test]
    fn a_kill_is_flow_sensitive_by_block_dominance() {
        // Issue #1296 P1. A sanitizer kill used to be applied everywhere
        // in the body once discovered, including at reads that execute
        // before the sanitizer ever runs. `read_at` fixes that. A kill
        // only hides a fact from a read whose block the kill's own block
        // dominates.
        let mut state = TaintState::new();
        state.add(
            &place(2, &[]),
            &TaintSet::of(fact("keys", TaintKind::Order)),
        );
        state.kill(&place(2, &[]), TaintKind::Order, "bb1");
        let graph = chain_graph();

        assert!(
            !state.read_at(&place(2, &[]), false, "bb0", &graph).is_empty(),
            "bb0 runs before bb1 (the sanitizer); it must still see the taint"
        );
        assert!(
            state.read_at(&place(2, &[]), false, "bb1", &graph).is_empty(),
            "a read in the sanitizer's own block is dominated by it"
        );
        assert!(
            state.read_at(&place(2, &[]), false, "bb2", &graph).is_empty(),
            "bb2 runs only after bb1, which dominates it: the read is clean"
        );
        assert!(
            !state.read(&place(2, &[]), false).is_empty(),
            "the unfiltered read still sees the raw fact — storage never \
             discards anything a kill covers"
        );
    }

    #[test]
    fn the_per_place_cap_is_per_kind_not_global() {
        // Issue #1296 P1: seven `Order` facts used to fill the cap and cause an
        // eighth fact of a DIFFERENT kind (`Value`) to be silently dropped.
        let mut set = TaintSet::new();
        for i in 0..MAX_FACTS {
            assert!(set.insert(fact(&format!("order-{i}"), TaintKind::Order)));
        }
        assert!(
            !set.insert(fact("one-more-order", TaintKind::Order)),
            "a seventh Order fact past the per-kind cap is still dropped"
        );
        assert!(
            set.insert(fact("value", TaintKind::Value)),
            "a fact of a kind not yet represented must never be dropped just \
             because another kind's slots are full"
        );
        assert!(set.has(TaintKind::Value));
        assert_eq!(set.facts().iter().filter(|f| f.kind == TaintKind::Order).count(), MAX_FACTS);
    }

    #[test]
    fn facts_are_deduplicated_on_kind_and_source() {
        let mut set = TaintSet::new();
        assert!(set.insert(fact("a", TaintKind::Value)));
        assert!(!set.insert(fact("a", TaintKind::Value)));
        assert!(set.insert(fact("a", TaintKind::Order)));
        assert_eq!(set.facts().len(), 2);
    }
}
