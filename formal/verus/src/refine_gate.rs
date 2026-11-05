//! The adoption gate of a refine run puts a version live only on an
//! activation.
//!
//! The copy below is `gate_step` of `refine/gate.rs` in the agent crate.
//! It maps one event of the gate to the next step. A refused or unreadable
//! decision keeps the wait. Each other event ends the gate.
//!
//! The model of a gate is the sequence of its events. The gate ends at the
//! first event that is not a wait. The theorems show that only an
//! activation adopts, and that an end is final.

use vstd::prelude::*;

verus! {

/// One event of the gate.
pub enum GateEvent {
    Deadline,
    Unreadable,
    Reject,
    Activated,
    Refused,
    Conflict,
}

/// What the gate does after one event.
pub enum GateStep {
    Wait,
    Adopted,
    Rejected,
    TimedOut,
    Superseded,
}

/// `gate_step` as a spec.
pub open spec fn spec_gate_step(event: GateEvent) -> GateStep {
    match event {
        GateEvent::Deadline => GateStep::TimedOut,
        GateEvent::Unreadable => GateStep::Wait,
        GateEvent::Reject => GateStep::Rejected,
        GateEvent::Activated => GateStep::Adopted,
        GateEvent::Refused => GateStep::Wait,
        GateEvent::Conflict => GateStep::Superseded,
    }
}

// mirror: autumn-harvest-agent/src/refine/gate.rs gate_step
pub fn gate_step(event: GateEvent) -> (r: GateStep)
    ensures
        r == spec_gate_step(event),
        r is Adopted <==> event is Activated,
        r is Wait <==> (event is Unreadable || event is Refused),
{
    match event {
        GateEvent::Deadline => GateStep::TimedOut,
        GateEvent::Unreadable => GateStep::Wait,
        GateEvent::Reject => GateStep::Rejected,
        GateEvent::Activated => GateStep::Adopted,
        GateEvent::Refused => GateStep::Wait,
        GateEvent::Conflict => GateStep::Superseded,
    }
}

/// The step of a gate after `events`: the first step that is not a wait,
/// or a wait when every event keeps the wait.
pub open spec fn spec_gate(events: Seq<GateEvent>) -> GateStep
    decreases events.len(),
{
    if events.len() == 0 {
        GateStep::Wait
    } else if spec_gate_step(events[0]) is Wait {
        spec_gate(events.subrange(1, events.len() as int))
    } else {
        spec_gate_step(events[0])
    }
}

/// A gate adopts only when an activation comes before every other end.
/// So a gate with no activation never adopts.
pub proof fn theorem_only_an_activation_adopts(events: Seq<GateEvent>)
    requires
        spec_gate(events) is Adopted,
    ensures
        exists|i: int|
            0 <= i < events.len() && events[i] is Activated && forall|j: int|
                0 <= j < i ==> #[trigger] spec_gate_step(events[j]) is Wait,
    decreases events.len(),
{
    if spec_gate_step(events[0]) is Wait {
        let rest = events.subrange(1, events.len() as int);
        theorem_only_an_activation_adopts(rest);
        let k = choose|k: int|
            0 <= k < rest.len() && rest[k] is Activated && forall|j: int|
                0 <= j < k ==> #[trigger] spec_gate_step(rest[j]) is Wait;
        assert(events[k + 1] == rest[k]);
        assert forall|j: int| 0 <= j < k + 1 implies #[trigger] spec_gate_step(events[j]) is Wait by {
            if j > 0 {
                assert(events[j] == rest[j - 1]);
            }
        }
    } else {
        assert(events[0] is Activated);
    }
}

/// A gate with no activation event never adopts.
pub proof fn theorem_no_activation_never_adopts(events: Seq<GateEvent>)
    requires
        forall|i: int| 0 <= i < events.len() ==> !(#[trigger] events[i] is Activated),
    ensures
        !(spec_gate(events) is Adopted),
{
    if spec_gate(events) is Adopted {
        theorem_only_an_activation_adopts(events);
    }
}

/// An end is final: events after it change nothing.
pub proof fn theorem_an_end_is_final(events: Seq<GateEvent>, more: Seq<GateEvent>)
    requires
        !(spec_gate(events) is Wait),
    ensures
        spec_gate(events + more) == spec_gate(events),
    decreases events.len(),
{
    let all = events + more;
    assert(all[0] == events[0]);
    if spec_gate_step(events[0]) is Wait {
        let rest = events.subrange(1, events.len() as int);
        theorem_an_end_is_final(rest, more);
        assert(all.subrange(1, all.len() as int) =~= rest + more);
    }
}

/// A deadline ends the gate with no activation.
pub proof fn theorem_a_deadline_never_adopts()
    ensures
        spec_gate_step(GateEvent::Deadline) is TimedOut,
{
}

} // verus!
