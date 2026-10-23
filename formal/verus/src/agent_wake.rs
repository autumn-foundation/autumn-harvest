//! A heartbeat tick never ticks in a loop (issue #2266).
//!
//! The agent books its next tick through `next_heartbeat` at the end of each
//! op. The copy below proves it for every input. A tick books the next tick
//! strictly after the time that it ran. A tick that kept its own time would
//! be due again at once, and the agent would tick in a loop.

use vstd::prelude::*;

verus! {

/// The step of a heartbeat: `every`, but at least 1 ms.
pub open spec fn interval(every: i64) -> int {
    if every < 1 {
        1
    } else {
        every as int
    }
}

/// The tick that an op at `now` books: one step later, saturated at
/// `i64::MAX`.
pub open spec fn booked(now: i64, every: i64) -> i64 {
    if now + interval(every) > i64::MAX {
        i64::MAX
    } else {
        (now + interval(every)) as i64
    }
}

/// `next_heartbeat` as a spec.
pub open spec fn spec_next_heartbeat(
    current: Option<i64>,
    every_ms: Option<i64>,
    now: i64,
    ticked: bool,
) -> Option<i64> {
    match every_ms {
        None => None,
        Some(every) => if current.is_some() && !ticked {
            current
        } else {
            Some(booked(now, every))
        },
    }
}

// mirror: autumn-harvest-agent/src/agent/wake.rs next_heartbeat
pub fn next_heartbeat(
    current: Option<i64>,
    every_ms: Option<i64>,
    now: i64,
    ticked: bool,
) -> (r: Option<i64>)
    ensures
        r == spec_next_heartbeat(current, every_ms, now, ticked),
        every_ms.is_none() ==> r.is_none(),
        every_ms.is_some() ==> r.is_some(),
        every_ms.is_some() && !ticked && current.is_some() ==> r == current,
        every_ms.is_some() && (ticked || current.is_none()) ==> r == Some(
            booked(now, every_ms.unwrap()),
        ),
        every_ms.is_some() && ticked && now < i64::MAX ==> r.unwrap() > now,
{
    match every_ms {
        None => None,
        Some(every) => match current {
            Some(at) if !ticked => Some(at),
            _ => {
                let step = if every < 1 { 1 } else { every };
                Some(if now > i64::MAX - step {
                    i64::MAX
                } else {
                    now + step
                })
            }
        },
    }
}

/// Whether a booking is due at `now`.
pub open spec fn due(booking: Option<i64>, now: i64) -> bool {
    booking.is_some() && booking.unwrap() <= now
}

/// A tick is never due again at once. It books the next tick strictly
/// after `now`, and at most one step later. Without saturation, it books
/// exactly one step later.
pub proof fn theorem_a_tick_is_never_due_again_at_once(every: i64, now: i64)
    requires
        now < i64::MAX,
    ensures
        !due(Some(booked(now, every)), now),
        now < booked(now, every) <= now + interval(every),
        now + interval(every) <= i64::MAX ==> booked(now, every) == now + interval(every),
{
}

/// The ops that are not ticks, at the times `nows`, one after another.
pub open spec fn messages(current: Option<i64>, every_ms: Option<i64>, nows: Seq<i64>) -> Option<
    i64,
>
    decreases nows.len(),
{
    if nows.len() == 0 {
        current
    } else {
        messages(spec_next_heartbeat(current, every_ms, nows[0], false), every_ms, nows.drop_first())
    }
}

/// A stream of messages never starves the heartbeat. An op that is not a
/// tick keeps the booked tick, so the tick stays due at its booked time.
pub proof fn theorem_messages_never_starve_the_heartbeat(at: i64, every: i64, nows: Seq<i64>)
    ensures
        messages(Some(at), Some(every), nows) == Some(at),
    decreases nows.len(),
{
    if nows.len() > 0 {
        theorem_messages_never_starve_the_heartbeat(at, every, nows.drop_first());
    }
}

} // verus!
