//! A NOTIFY cuts each settle once at most.
//!
//! The leader claim loop sleeps a settle after an empty poll that a NOTIFY
//! caused. A NOTIFY in that sleep cuts it, and the rest of the settle stays
//! owed. No NOTIFY cuts the rest. So a fleet polls a bounded number of times
//! per settle, whatever the NOTIFY rate.
//!
//! The copies below prove the kernel in `autumn-harvest/src/notify.rs`. The
//! trace theorem proves the bound for every sequence of leader events.

use vstd::prelude::*;

verus! {

/// What the leader owes after a NOTIFY.
pub enum SettleState<T> {
    /// The leader owes nothing.
    Clear,
    /// The leader owes a fresh settle. One NOTIFY can cut it.
    Due,
    /// The leader owes the rest of a cut settle, up to its end.
    Rest(T),
}

/// The settle that the leader runs now.
pub enum SettlePlan<T> {
    /// No settle.
    Skip,
    /// A fresh settle.
    Fresh,
    /// The rest of a cut settle, up to its end.
    Rest(T),
}

/// `take_settle` as a spec.
pub open spec fn spec_take_settle<T>(state: SettleState<T>) -> SettlePlan<T> {
    match state {
        SettleState::Clear => SettlePlan::Skip,
        SettleState::Due => SettlePlan::Fresh,
        SettleState::Rest(end) => SettlePlan::Rest(end),
    }
}

/// `after_notify_cut` as a spec.
pub open spec fn spec_after_notify_cut<T>(plan: SettlePlan<T>, end: T) -> Option<SettleState<T>> {
    match plan {
        SettlePlan::Fresh => Some(SettleState::Rest(end)),
        _ => None,
    }
}

// mirror: autumn-harvest/src/notify.rs take_settle
pub fn take_settle<T>(state: SettleState<T>) -> (r: SettlePlan<T>)
    ensures
        r == spec_take_settle(state),
        state is Clear <==> r is Skip,
        state is Due <==> r is Fresh,
        forall|end: T| state == SettleState::Rest(end) ==> r == SettlePlan::Rest(end),
{
    match state {
        SettleState::Clear => SettlePlan::Skip,
        SettleState::Due => SettlePlan::Fresh,
        SettleState::Rest(end) => SettlePlan::Rest(end),
    }
}

// mirror: autumn-harvest/src/notify.rs after_notify_cut
pub fn after_notify_cut<T>(plan: SettlePlan<T>, end: T) -> (r: Option<SettleState<T>>)
    ensures
        r == spec_after_notify_cut(plan, end),
        plan is Fresh ==> r == Some(SettleState::Rest(end)),
        !(plan is Fresh) ==> r is None,
{
    match plan {
        SettlePlan::Fresh => Some(SettleState::Rest(end)),
        SettlePlan::Skip | SettlePlan::Rest(_) => None,
    }
}

/// `after_settle` as a spec.
pub open spec fn spec_after_settle<T>(plan: SettlePlan<T>, queued: bool) -> SettleState<T> {
    match plan {
        SettlePlan::Rest(_) => SettleState::Due,
        _ => if queued {
            SettleState::Due
        } else {
            SettleState::Clear
        },
    }
}

// mirror: autumn-harvest/src/notify.rs after_settle
pub fn after_settle<T: Copy>(plan: SettlePlan<T>, queued: bool) -> (r: SettleState<T>)
    ensures
        r == spec_after_settle(plan, queued),
        plan is Rest ==> r is Due,
        !(plan is Rest) && queued ==> r is Due,
        !(plan is Rest) && !queued ==> r is Clear,
{
    match plan {
        SettlePlan::Rest(_) => SettleState::Due,
        SettlePlan::Skip | SettlePlan::Fresh => {
            if queued {
                SettleState::Due
            } else {
                SettleState::Clear
            }
        }
    }
}

/// One event of the leader claim loop.
pub enum Event {
    /// A NOTIFY wakes the idle wait, and the poll after it is empty.
    Wake,
    /// The leader takes what is owed and starts its settle.
    Begin,
    /// A NOTIFY arrives in the settle that ends at the given time.
    Notify(int),
    /// The settle sleeps to its end. The flag tells whether a NOTIFY is
    /// queued then.
    Slept(bool),
    /// A local decision end cuts the settle.
    DecisionEnd,
}

/// The state of the model: the owed state, the running plan, the fresh
/// settles begun, the rests begun and the NOTIFY cuts.
pub struct Run {
    pub state: SettleState<int>,
    pub plan: SettlePlan<int>,
    pub fresh: nat,
    pub rests: nat,
    pub cuts: nat,
}

/// The run after one event.
///
/// An idle wait runs only when nothing is owed and no settle runs, because
/// the leader takes the owed settle before each idle wait. So a wake in any
/// other state changes nothing.
pub open spec fn step(run: Run, event: Event) -> Run {
    match event {
        Event::Wake => if run.state is Clear && run.plan is Skip {
            Run { state: SettleState::Due, ..run }
        } else {
            run
        },
        Event::Begin => {
            let plan = spec_take_settle(run.state);
            Run {
                state: SettleState::Clear,
                plan: plan,
                fresh: if plan is Fresh { run.fresh + 1 } else { run.fresh },
                rests: if plan is Rest { run.rests + 1 } else { run.rests },
                cuts: run.cuts,
            }
        },
        Event::Notify(end) => match spec_after_notify_cut(run.plan, end) {
            Some(state) => Run { state: state, plan: SettlePlan::Skip, cuts: run.cuts + 1, ..run },
            None => run,
        },
        Event::Slept(queued) => if run.plan is Skip {
            run
        } else {
            Run { state: spec_after_settle(run.plan, queued), plan: SettlePlan::Skip, ..run }
        },
        Event::DecisionEnd => if run.plan is Skip {
            run
        } else {
            Run { state: spec_after_settle(run.plan, false), plan: SettlePlan::Skip, ..run }
        },
    }
}

/// The run after each event of `events`, in order.
pub open spec fn run_all(run: Run, events: Seq<Event>) -> Run
    decreases events.len(),
{
    if events.len() == 0 {
        run
    } else {
        run_all(step(run, events[0]), events.drop_first())
    }
}

/// The invariant of the model.
///
/// - The cuts and a running fresh settle never exceed the fresh settles.
/// - Each cut is paid by one rest, or its rest is still owed.
/// - A running settle owes nothing else, so its end cannot drop a rest.
pub open spec fn bounded(run: Run) -> bool {
    &&& run.cuts + (if run.plan is Fresh { 1nat } else { 0nat }) <= run.fresh
    &&& run.cuts == run.rests + (if run.state is Rest { 1nat } else { 0nat })
    &&& !(run.plan is Skip) ==> run.state is Clear
}

proof fn lemma_step_keeps_the_bound(run: Run, event: Event)
    requires
        bounded(run),
    ensures
        bounded(step(run, event)),
{
}

proof fn lemma_run_all_keeps_the_bound(run: Run, events: Seq<Event>)
    requires
        bounded(run),
    ensures
        bounded(run_all(run, events)),
    decreases events.len(),
{
    if events.len() > 0 {
        lemma_step_keeps_the_bound(run, events[0]);
        lemma_run_all_keeps_the_bound(step(run, events[0]), events.drop_first());
    }
}

/// The run before any event.
pub open spec fn start() -> Run {
    Run { state: SettleState::Clear, plan: SettlePlan::Skip, fresh: 0, rests: 0, cuts: 0 }
}

/// For every sequence of leader events, the NOTIFY cuts never exceed the
/// fresh settles begun. So each settle has one cut at most.
pub proof fn theorem_each_settle_has_one_cut_at_most(events: Seq<Event>)
    ensures
        run_all(start(), events).cuts <= run_all(start(), events).fresh,
{
    lemma_run_all_keeps_the_bound(start(), events);
}

/// For every sequence of leader events, each cut is paid by its rest, or
/// that rest is still owed. A wake or the end of a settle cannot drop an
/// owed rest, so the poll at the end of a cut settle always comes.
pub proof fn theorem_each_cut_owes_its_rest(events: Seq<Event>)
    ensures
        run_all(start(), events).cuts == run_all(start(), events).rests + (if run_all(
            start(),
            events,
        ).state is Rest {
            1nat
        } else {
            0nat
        }),
{
    lemma_run_all_keeps_the_bound(start(), events);
}

/// A rest always ends owing a fresh settle. So the NOTIFY that cut the
/// settle gets a full margin of its own, after the poll at the old end.
pub proof fn theorem_a_rest_owes_a_fresh_settle(end: int, queued: bool)
    ensures
        spec_after_settle(SettlePlan::Rest(end), queued) is Due,
        spec_take_settle(spec_after_settle(SettlePlan::Rest(end), queued)) is Fresh,
{
}

/// A cut keeps the end of its settle, so the leader still polls at that end.
pub proof fn theorem_a_cut_keeps_the_end(end: int)
    ensures
        spec_take_settle(spec_after_notify_cut(SettlePlan::<int>::Fresh, end).unwrap())
            == SettlePlan::Rest(end),
{
}

/// No NOTIFY cuts the rest of a settle.
pub proof fn theorem_no_notify_cuts_a_rest(end: int, at: int)
    ensures
        spec_after_notify_cut(SettlePlan::Rest(end), at) is None,
{
}

} // verus!
