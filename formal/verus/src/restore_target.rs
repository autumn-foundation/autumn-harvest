//! A restore target applies only to the database file that it names.
//!
//! `HarvestFleet::restore` keeps a target beside the tenant Harvest file.
//! The next open restores the Harvest file to it. A later restore of the
//! database must supersede it, and a failed one must keep it.
//!
//! The copy below proves `resolve_target` in
//! `autumn-harvest-plugin/src/sqlite/replica.rs` for every input. The spec
//! below models the target file under any sequence of restores, deletes and
//! opens. An identity is a `nat`. A new database file gets any identity
//! other than the current one, so an old identity can come back. Each open
//! applies the target of the last finished `HarvestFleet::restore`, and no
//! target after a direct restore, a delete or an earlier open.
//!
//! Each release before the fleet restore returns writes the pending target
//! again. The last one comes from the drain of the restore. So an earlier
//! release does no harm, such as the release of a stale-epoch open. An
//! open during the drain keeps the pending target.
//!
//! The model takes these engine facts as preconditions. A publish follows
//! the release of its own drain, with no other step between. No other
//! restore publishes between that drain and the return of the fleet
//! restore. Each file write succeeds, except the write of an end. One
//! process owns the tenant files.

use vstd::prelude::*;

verus! {

/// What a target file holds, seen from the current database file.
pub enum Held<T> {
    /// No target.
    Nothing,
    /// A finished restore. `current` is true while its file is current.
    Committed { target: T, current: bool },
    /// A restore whose end did not run. `moved` is true when the database
    /// file changed since its release.
    Pending { target: T, moved: bool, previous: Option<T> },
}

/// The target that applies to the current database file.
pub enum Resolution<T> {
    /// No target applies.
    Nothing,
    /// The open restores the Harvest file to this target.
    Apply(T),
    /// A later restore replaced the database file of the target.
    Superseded,
}

/// `resolve_target` as a spec.
pub open spec fn spec_resolve<T>(held: Held<T>) -> Resolution<T> {
    match held {
        Held::Nothing => Resolution::Nothing,
        Held::Committed { target, current } => if current {
            Resolution::Apply(target)
        } else {
            Resolution::Superseded
        },
        Held::Pending { target, moved, previous } => if moved {
            Resolution::Apply(target)
        } else {
            match previous {
                Some(previous) => Resolution::Apply(previous),
                None => Resolution::Nothing,
            }
        },
    }
}

// mirror: autumn-harvest-plugin/src/sqlite/replica.rs resolve_target
pub fn resolve_target<T>(held: Held<T>) -> (r: Resolution<T>)
    ensures
        r == spec_resolve(held),
        held is Nothing ==> r is Nothing,
        r is Superseded <==> (held is Committed && !held->current),
        held is Pending && held->moved ==> r == Resolution::Apply(held->Pending_target),
{
    match held {
        Held::Nothing => Resolution::Nothing,
        Held::Committed { target, current } => {
            if current {
                Resolution::Apply(target)
            } else {
                Resolution::Superseded
            }
        }
        Held::Pending {
            target,
            moved,
            previous,
        } => match (moved, previous) {
            (true, _) => Resolution::Apply(target),
            (false, Some(previous)) => Resolution::Apply(previous),
            (false, None) => Resolution::Nothing,
        },
    }
}

/// The entry of a target file. Each one names a database file.
pub enum Entry<T> {
    /// A finished restore that published the file `of`.
    Committed { target: T, of: nat },
    /// A restore that started on the file `from`. `previous` applied to it.
    Pending { target: T, from: nat, previous: Option<T> },
}

/// `held` of the engine: `entry`, seen from the file `current`.
pub open spec fn held<T>(entry: Option<Entry<T>>, current: nat) -> Held<T> {
    match entry {
        None => Held::Nothing,
        Some(Entry::Committed { target, of }) => Held::Committed { target, current: of == current },
        Some(Entry::Pending { target, from, previous }) => Held::Pending {
            target,
            moved: from != current,
            previous,
        },
    }
}

/// The target that an open of the file `current` applies to `entry`.
pub open spec fn applies<T>(entry: Option<Entry<T>>, current: nat) -> Option<T> {
    match spec_resolve(held(entry, current)) {
        Resolution::Apply(target) => Some(target),
        _ => None,
    }
}

/// `settle` of the engine: the target that applies, bound to `current`.
pub open spec fn settle<T>(entry: Option<Entry<T>>, current: nat) -> Option<Entry<T>> {
    match applies(entry, current) {
        Some(target) => Some(Entry::Committed { target, of: current }),
        None => None,
    }
}

/// `before_request` of the engine: the entry from before the request.
pub open spec fn before_request<T>(entry: Entry<T>) -> Option<Entry<T>> {
    match entry {
        Entry::Pending { from, previous, .. } => match previous {
            Some(previous) => Some(Entry::Committed { target: previous, of: from }),
            None => None,
        },
        Entry::Committed { .. } => Some(entry),
    }
}

/// The entry that a release or an open of an unreturned restore reads.
pub open spec fn seen<T>(file: Option<Entry<T>>, written: Option<Entry<T>>) -> Option<Entry<T>> {
    match written {
        Some(entry) => if file == Some(entry) {
            before_request(entry)
        } else {
            file
        },
        None => file,
    }
}

/// The `HarvestFleet::restore` in flight.
pub enum Run<T> {
    /// No restore runs.
    Idle,
    /// The restore of `target` did not return. `written` is the entry of
    /// its last release.
    Asked { target: T, written: Option<Entry<T>> },
    /// The fleet restore returned. The end did not run.
    Returned { pending: Entry<T>, ok: bool },
}

/// The tenant: its database file, its target file and the restore in
/// flight. `want` is the target that the next open must apply.
pub struct World<T> {
    pub db: nat,
    pub file: Option<Entry<T>>,
    pub run: Run<T>,
    pub want: Option<T>,
}

/// One event of the tenant.
pub enum Step<T> {
    /// `HarvestFleet::restore` asks for a restore to `target`.
    Ask { target: T },
    /// A release while the restore did not return. A stale-epoch open
    /// makes one, and so does the drain of the restore.
    Release,
    /// The restore publishes the file `to`, or fails, and returns.
    Publish { ok: bool, to: nat },
    /// `end_restore`.
    End,
    /// `end_restore` fails to write. The entry stays.
    EndFails,
    /// Another restore, or a stale-epoch open: a release, then the file
    /// `to`, or a failure.
    Direct { ok: bool, to: nat },
    /// An open. It applies the result.
    Open,
    /// A delete, then a new file `to` of the re-created tenant.
    Delete { to: nat },
}

/// What an open applies in `w`.
pub open spec fn opens<T>(w: World<T>) -> Option<T> {
    match w.run {
        Run::Asked { written, .. } => applies(seen(w.file, written), w.db),
        _ => applies(w.file, w.db),
    }
}

/// The entry that a release of an unreturned restore to `target` writes.
pub open spec fn rewritten<T>(w: World<T>, target: T, written: Option<Entry<T>>) -> Entry<T> {
    Entry::Pending {
        target,
        from: w.db,
        previous: applies(settle(seen(w.file, written), w.db), w.db),
    }
}

/// `w` after a release: the restore in flight writes its target again, or
/// the release binds the entry to the current file.
pub open spec fn released<T>(w: World<T>) -> World<T> {
    match w.run {
        Run::Asked { target, written } => {
            let entry = rewritten(w, target, written);
            World { file: Some(entry), run: Run::Asked { target, written: Some(entry) }, ..w }
        },
        _ => World { file: settle(w.file, w.db), ..w },
    }
}

/// Whether `step` can happen in `w`.
pub open spec fn valid<T>(w: World<T>, step: Step<T>) -> bool {
    match step {
        Step::Ask { .. } => w.run is Idle,
        Step::Release => w.run is Asked,
        Step::Publish { ok, to } => match w.run {
            Run::Asked { written, .. } => match written {
                Some(entry) => w.file == Some(entry) && (ok ==> to != w.db) && match entry {
                    Entry::Pending { from, .. } => from == w.db,
                    Entry::Committed { .. } => false,
                },
                None => false,
            },
            _ => false,
        },
        Step::End => w.run is Returned,
        Step::EndFails => w.run is Returned,
        Step::Direct { ok, to } => ok ==> to != w.db,
        Step::Open => true,
        Step::Delete { .. } => true,
    }
}

/// The world after `step`.
pub open spec fn step<T>(w: World<T>, step: Step<T>) -> World<T> {
    match step {
        Step::Ask { target } => World { run: Run::Asked { target, written: None }, ..w },
        Step::Release => released(w),
        Step::Publish { ok, to } => match w.run {
            Run::Asked { target, written } => match written {
                Some(entry) => World {
                    db: if ok { to } else { w.db },
                    run: Run::Returned { pending: entry, ok },
                    want: if ok { Some(target) } else { w.want },
                    ..w
                },
                None => w,
            },
            _ => w,
        },
        Step::End => match w.run {
            Run::Returned { pending, ok } => {
                if w.file == Some(pending) {
                    match pending {
                        Entry::Pending { target, from, previous } => World {
                            file: if ok {
                                Some(Entry::Committed { target, of: w.db })
                            } else {
                                match previous {
                                    Some(previous) => Some(Entry::Committed { target: previous, of: from }),
                                    None => None,
                                }
                            },
                            run: Run::Idle,
                            ..w
                        },
                        Entry::Committed { .. } => World { run: Run::Idle, ..w },
                    }
                } else {
                    World { run: Run::Idle, ..w }
                }
            },
            _ => w,
        },
        Step::EndFails => World { run: Run::Idle, ..w },
        Step::Direct { ok, to } => {
            let after = released(w);
            World {
                db: if ok { to } else { w.db },
                want: if ok { None } else { w.want },
                ..after
            }
        },
        Step::Open => match w.run {
            Run::Asked { target, written } => match written {
                Some(entry) => if w.file == Some(entry) {
                    match entry {
                        Entry::Pending { target: pending, from, .. } => {
                            let kept = Entry::Pending { target: pending, from, previous: None };
                            World {
                                file: Some(kept),
                                run: Run::Asked { target, written: Some(kept) },
                                want: None,
                                ..w
                            }
                        },
                        Entry::Committed { .. } => World { file: None, want: None, ..w },
                    }
                } else {
                    World { file: None, want: None, ..w }
                },
                None => World { file: None, want: None, ..w },
            },
            _ => World { file: None, want: None, ..w },
        },
        Step::Delete { to } => World { db: to, file: None, want: None, ..w },
    }
}

/// The invariant of the tenant.
pub open spec fn inv<T>(w: World<T>) -> bool {
    match w.run {
        Run::Idle => applies(w.file, w.db) == w.want,
        Run::Asked { target, written } => applies(seen(w.file, written), w.db) == w.want
            && match written {
            Some(entry) => match entry {
                Entry::Pending { target: pending, .. } => pending == target,
                Entry::Committed { .. } => false,
            },
            None => true,
        },
        Run::Returned { pending, ok } => match pending {
            Entry::Pending { target, from, previous } => if w.file == Some(pending) {
                (ok ==> from != w.db && w.want == Some(target)) && (!ok ==> from == w.db
                    && w.want == previous)
            } else {
                applies(w.file, w.db) == w.want
            },
            Entry::Committed { .. } => false,
        },
    }
}

/// A tenant with no target and no restore in flight.
pub open spec fn fresh<T>(db: nat) -> World<T> {
    World { db, file: None, run: Run::Idle, want: None }
}

/// A settle keeps the target that applies to the same file.
proof fn lemma_a_settle_keeps_the_target<T>(entry: Option<Entry<T>>, db: nat)
    ensures
        applies(settle(entry, db), db) == applies(entry, db),
{
}

pub proof fn theorem_a_fresh_tenant_holds_the_invariant<T>(db: nat)
    ensures
        inv(fresh::<T>(db)),
{
}

/// Each step keeps the invariant.
pub proof fn theorem_each_step_keeps_the_invariant<T>(w: World<T>, s: Step<T>)
    requires
        inv(w),
        valid(w, s),
    ensures
        inv(step(w, s)),
{
    lemma_a_settle_keeps_the_target(w.file, w.db);
    match w.run {
        Run::Asked { written, .. } => lemma_a_settle_keeps_the_target(seen(w.file, written), w.db),
        _ => {},
    }
}

/// Each open applies the target that it must apply.
pub proof fn theorem_an_open_applies_the_wanted_target<T>(w: World<T>)
    requires
        inv(w),
    ensures
        opens(w) == w.want,
{
}

/// The world after each of `steps`, or `None` at the first invalid one.
pub open spec fn run_all<T>(w: World<T>, steps: Seq<Step<T>>) -> Option<World<T>>
    decreases steps.len(),
{
    if steps.len() == 0 {
        Some(w)
    } else if valid(w, steps[0]) {
        run_all(step(w, steps[0]), steps.drop_first())
    } else {
        None
    }
}

/// Every valid run keeps the invariant, so each of its opens applies the
/// wanted target.
pub proof fn theorem_every_run_keeps_the_invariant<T>(w: World<T>, steps: Seq<Step<T>>)
    requires
        inv(w),
    ensures
        run_all(w, steps) matches Some(end) ==> inv(end) && opens(end) == end.want,
    decreases steps.len(),
{
    if steps.len() > 0 && valid(w, steps[0]) {
        theorem_each_step_keeps_the_invariant(w, steps[0]);
        theorem_every_run_keeps_the_invariant(step(w, steps[0]), steps.drop_first());
    }
}

/// A finished `HarvestFleet::restore` to `target`, then a direct restore.
pub open spec fn restore_then_direct<T>(w: World<T>, target: T, to: nat, ok: bool, again: nat) -> World<T> {
    let asked = step(w, Step::Ask { target });
    let drained = step(asked, Step::Release);
    let published = step(drained, Step::Publish { ok: true, to });
    let ended = step(published, Step::End);
    step(ended, Step::Direct { ok, to: again })
}

/// The steps of [`restore_then_direct`], each valid and each keeping the
/// invariant.
proof fn lemma_restore_then_direct<T>(w: World<T>, target: T, to: nat, ok: bool, again: nat)
    requires
        inv(w),
        w.run is Idle,
        to != w.db,
        ok ==> again != to,
    ensures
        inv(restore_then_direct(w, target, to, ok, again)),
        restore_then_direct(w, target, to, ok, again).run is Idle,
        restore_then_direct(w, target, to, ok, again).want == if ok {
            None::<T>
        } else {
            Some(target)
        },
{
    let asked = step(w, Step::Ask { target });
    theorem_each_step_keeps_the_invariant(w, Step::Ask { target });
    let drained = step(asked, Step::Release);
    theorem_each_step_keeps_the_invariant(asked, Step::Release);
    let published = step(drained, Step::Publish { ok: true, to });
    theorem_each_step_keeps_the_invariant(drained, Step::Publish { ok: true, to });
    let ended = step(published, Step::End);
    theorem_each_step_keeps_the_invariant(published, Step::End);
    theorem_each_step_keeps_the_invariant(ended, Step::Direct { ok, to: again });
}

/// A direct restore after a `HarvestFleet::restore` supersedes its target.
/// The open applies no target, so the Harvest file goes to its latest
/// state.
pub proof fn theorem_a_direct_restore_supersedes_the_target<T>(
    w: World<T>,
    target: T,
    to: nat,
    again: nat,
)
    requires
        inv(w),
        w.run is Idle,
        to != w.db,
        again != to,
    ensures
        opens(restore_then_direct(w, target, to, true, again)) == None::<T>,
{
    lemma_restore_then_direct(w, target, to, true, again);
}

/// A failed direct restore after a `HarvestFleet::restore` keeps its
/// target. The open applies it.
pub proof fn theorem_a_failed_direct_restore_keeps_the_target<T>(
    w: World<T>,
    target: T,
    to: nat,
    again: nat,
)
    requires
        inv(w),
        w.run is Idle,
        to != w.db,
    ensures
        opens(restore_then_direct(w, target, to, false, again)) == Some(target),
{
    lemma_restore_then_direct(w, target, to, false, again);
}

/// A stale-epoch open releases and replaces the database between the
/// request and the drain of a restore. The open applies the target of the
/// restore all the same.
pub proof fn theorem_a_release_before_the_drain_does_no_harm<T>(
    w: World<T>,
    target: T,
    stale: nat,
    to: nat,
)
    requires
        inv(w),
        w.run is Idle,
        stale != w.db,
        to != stale,
    ensures
        ({
            let asked = step(w, Step::Ask { target });
            let replaced = step(asked, Step::Direct { ok: true, to: stale });
            let opened = step(replaced, Step::Open);
            let drained = step(opened, Step::Release);
            let published = step(drained, Step::Publish { ok: true, to });
            let ended = step(published, Step::End);
            opens(ended) == Some(target)
        }),
{
    let asked = step(w, Step::Ask { target });
    theorem_each_step_keeps_the_invariant(w, Step::Ask { target });
    let replaced = step(asked, Step::Direct { ok: true, to: stale });
    theorem_each_step_keeps_the_invariant(asked, Step::Direct { ok: true, to: stale });
    let opened = step(replaced, Step::Open);
    theorem_each_step_keeps_the_invariant(replaced, Step::Open);
    let drained = step(opened, Step::Release);
    theorem_each_step_keeps_the_invariant(opened, Step::Release);
    let published = step(drained, Step::Publish { ok: true, to });
    theorem_each_step_keeps_the_invariant(drained, Step::Publish { ok: true, to });
    let ended = step(published, Step::End);
    theorem_each_step_keeps_the_invariant(published, Step::End);
}

} // verus!
