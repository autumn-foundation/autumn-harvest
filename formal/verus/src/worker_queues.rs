//! The source of the queues that a Harvest worker polls (issue #2479).
//!
//! Code declares the full set of queues. Config, or the pin of the Autumn
//! role, can only narrow it. `HarvestRunner::start` picks one source. An
//! explicit list always wins. A role pin applies only with no explicit list,
//! and only when it names a declared queue. Otherwise the worker polls every
//! declared queue. The warning for a role pin fires exactly when that pin
//! names no declared queue and so changes nothing.

use vstd::prelude::*;

verus! {

/// `crate::worker_queues::QueueSource`.
pub enum QueueSource {
    Code,
    Explicit,
    Role,
}

impl Clone for QueueSource {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl Copy for QueueSource {}

/// `queue_source` as a spec.
pub open spec fn spec_queue_source(explicit: bool, pin_hits: bool) -> QueueSource {
    if explicit {
        QueueSource::Explicit
    } else if pin_hits {
        QueueSource::Role
    } else {
        QueueSource::Code
    }
}

/// `pin_misses` as a spec.
pub open spec fn spec_pin_misses(explicit: bool, pin_set: bool, pin_hits: bool) -> bool {
    !explicit && pin_set && !pin_hits
}

// mirror: autumn-harvest-plugin/src/worker_queues.rs queue_source
pub fn queue_source(explicit: bool, pin_hits: bool) -> (r: QueueSource)
    ensures
        r == spec_queue_source(explicit, pin_hits),
{
    match (explicit, pin_hits) {
        (true, _) => QueueSource::Explicit,
        (false, true) => QueueSource::Role,
        (false, false) => QueueSource::Code,
    }
}

// mirror: autumn-harvest-plugin/src/worker_queues.rs pin_misses
pub fn pin_misses(explicit: bool, pin_set: bool, pin_hits: bool) -> (r: bool)
    ensures
        r == spec_pin_misses(explicit, pin_set, pin_hits),
{
    !explicit && pin_set && !pin_hits
}

/// An explicit list always wins over the role pin.
pub proof fn theorem_an_explicit_list_wins(pin_hits: bool)
    ensures
        spec_queue_source(true, pin_hits) == QueueSource::Explicit,
{
}

/// With no explicit list, a pin that names a declared queue applies.
pub proof fn theorem_a_hitting_pin_applies()
    ensures
        spec_queue_source(false, true) == QueueSource::Role,
{
}

/// With no explicit list and no hitting pin, the worker polls each declared
/// queue. That is the behaviour before issue #2479.
pub proof fn theorem_no_selection_keeps_the_code_set()
    ensures
        spec_queue_source(false, false) == QueueSource::Code,
{
}

/// The role is the source only when no explicit list is set and the pin
/// names a declared queue.
pub proof fn theorem_the_role_applies_only_below_config(explicit: bool, pin_hits: bool)
    ensures
        (spec_queue_source(explicit, pin_hits) == QueueSource::Role) <==> (!explicit && pin_hits),
{
}

/// The warning fires only when the pin changes nothing. The worker then
/// polls each declared queue.
pub proof fn theorem_a_miss_keeps_the_code_set(explicit: bool, pin_set: bool, pin_hits: bool)
    ensures
        spec_pin_misses(explicit, pin_set, pin_hits) ==> spec_queue_source(explicit, pin_hits)
            == QueueSource::Code,
{
}

/// No warning fires with no pin, or with an explicit list.
pub proof fn theorem_no_pin_or_config_never_warns(explicit: bool, pin_set: bool, pin_hits: bool)
    ensures
        (!pin_set || explicit) ==> !spec_pin_misses(explicit, pin_set, pin_hits),
{
}

} // verus!
