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

/// How many distinct facts one place keeps. Beyond this the analysis has all
/// the evidence a report can use, and more only slows the fixpoint down.
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
    pub fn insert(&mut self, fact: Fact) -> bool {
        if self.facts.iter().any(|f| f.key() == fact.key()) {
            return false;
        }
        if self.facts.len() >= MAX_FACTS {
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

/// The taint of every place in one body, plus its aliases and sanitizer kills.
#[derive(Debug, Default)]
pub struct TaintState {
    /// Root local → (projections, facts).
    places: BTreeMap<Local, Vec<(Vec<Projection>, TaintSet)>>,
    /// Local → the place it is a reference to (`_6 = &mut _4` ⇒ `_6` ↦ `_4`).
    aliases: BTreeMap<Local, Place>,
    /// Places a sanitizer cleared, and of which kinds.
    kills: Vec<(Place, TaintKind)>,
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

    /// Record that a sanitizer cleared `kind` on `place` (and everything under it).
    ///
    /// Kills are **monotone**: once a place has been sorted, it stays sorted for
    /// the rest of the fixpoint. Without that, a flow-insensitive round would
    /// re-add the taint the previous round's `sort` had just removed, and the
    /// analysis would oscillate instead of converging.
    pub fn kill(&mut self, place: &Place, kind: TaintKind) {
        let canonical = self.canonical(place);
        if !self
            .kills
            .iter()
            .any(|(p, k)| *p == canonical && *k == kind)
        {
            self.kills.push((canonical, kind));
        }
    }

    /// Start with the kills a previous attempt discovered.
    pub fn seed_kills(&mut self, kills: &[(Place, TaintKind)]) {
        self.kills = kills.to_vec();
    }

    /// The kills discovered so far.
    #[must_use]
    pub fn kills(&self) -> &[(Place, TaintKind)] {
        &self.kills
    }

    /// Drop every fact a kill covers. Returns `true` when anything was removed.
    pub fn apply_kills(&mut self) -> bool {
        if self.kills.is_empty() {
            return false;
        }
        let kills = self.kills.clone();
        let mut changed = false;
        for (local, entries) in &mut self.places {
            for (projections, set) in entries.iter_mut() {
                let place = Place {
                    local: *local,
                    projections: projections.clone(),
                };
                let killed: BTreeSet<TaintKind> = kills
                    .iter()
                    .filter(|(p, _)| covers(p, &place))
                    .map(|(_, k)| *k)
                    .collect();
                if killed.is_empty() {
                    continue;
                }
                let filtered = set.without(&killed);
                if filtered != *set {
                    *set = filtered;
                    changed = true;
                }
            }
        }
        changed
    }

    /// Which kinds are killed for `place`.
    fn killed_kinds(&self, place: &Place) -> BTreeSet<TaintKind> {
        self.kills
            .iter()
            .filter(|(p, _)| covers(p, place))
            .map(|(_, k)| *k)
            .collect()
    }

    /// Taint of a read of `place`.
    ///
    /// `discriminant_only` restricts the read to `place` and its ancestors (see
    /// the module docs).
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

    /// Taint of every place rooted at `local` (how an out-parameter is read back).
    #[must_use]
    pub fn read_root(&self, local: Local) -> TaintSet {
        let mut out = TaintSet::new();
        for (_, set) in self.places.get(&local).into_iter().flatten() {
            out.absorb(set);
        }
        out
    }

    /// Add `set` to `place`; `true` when anything new landed.
    pub fn add(&mut self, place: &Place, set: &TaintSet) -> bool {
        if set.is_empty() {
            return false;
        }
        let place = self.canonical(place);
        let killed = self.killed_kinds(&place);
        let set = if killed.is_empty() {
            set.clone()
        } else {
            set.without(&killed)
        };
        if set.is_empty() {
            return false;
        }
        let entries = self.places.entry(place.local).or_default();
        if let Some((_, existing)) = entries
            .iter_mut()
            .find(|(projections, _)| *projections == place.projections)
        {
            return existing.absorb(&set);
        }
        entries.push((place.projections, set));
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

    #[test]
    fn a_kill_removes_and_then_blocks_a_kind() {
        let mut state = TaintState::new();
        state.add(
            &place(2, &[]),
            &TaintSet::of(fact("keys", TaintKind::Order)),
        );
        state.kill(&place(2, &[]), TaintKind::Order);
        assert!(state.apply_kills());
        assert!(state.read(&place(2, &[]), false).is_empty());
        state.add(
            &place(2, &[]),
            &TaintSet::of(fact("keys", TaintKind::Order)),
        );
        assert!(
            state.read(&place(2, &[]), false).is_empty(),
            "a sorted place stays sorted for the rest of the fixpoint"
        );
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
