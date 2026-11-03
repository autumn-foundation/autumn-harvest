//! The refine policy adopts only what the operator allows (issue #2394).
//!
//! The copies below are the kernel of `refine/policy.rs` in the agent
//! crate. `parts_allowed` checks that each changed part is allowed. Each
//! overlay part is one bit. `decide_adoption` decides whether a refined
//! overlay goes live without a person. `gate_timed_out` tells whether an
//! approval gate has waited for its whole timeout.
//!
//! The store counts the auto adoptions of one UTC day under the lock of the
//! head row. The model of a day is a count that grows by one on each
//! adoption. The theorems show that the count never passes the cap.

use vstd::prelude::*;

verus! {

/// What the kernel decides for one refined overlay.
pub enum AdoptDecision {
    Adopt,
    Unchanged,
    NeedsApproval,
    CapReached,
}

/// `parts_allowed` as a spec: no bit of `changed` is outside `allowed`.
pub open spec fn spec_parts_allowed(changed: u8, allowed: u8) -> bool {
    changed & !allowed == 0
}

// mirror: autumn-harvest-agent/src/refine/policy.rs parts_allowed
pub fn parts_allowed(changed: u8, allowed: u8) -> (r: bool)
    ensures
        r == spec_parts_allowed(changed, allowed),
{
    changed & !allowed == 0
}

/// `decide_adoption` as a spec.
pub open spec fn spec_decide_adoption(
    allowed: u8,
    changed: u8,
    used_today: u32,
    cap: Option<u32>,
) -> AdoptDecision {
    if changed == 0 {
        AdoptDecision::Unchanged
    } else if !spec_parts_allowed(changed, allowed) {
        AdoptDecision::NeedsApproval
    } else {
        match cap {
            Some(limit) => if used_today < limit {
                AdoptDecision::Adopt
            } else {
                AdoptDecision::CapReached
            },
            None => AdoptDecision::Adopt,
        }
    }
}

// mirror: autumn-harvest-agent/src/refine/policy.rs decide_adoption
pub fn decide_adoption(allowed: u8, changed: u8, used_today: u32, cap: Option<u32>) -> (r:
    AdoptDecision)
    ensures
        r == spec_decide_adoption(allowed, changed, used_today, cap),
        r is Adopt ==> changed != 0,
        r is Adopt ==> spec_parts_allowed(changed, allowed),
        r is Adopt && cap is Some ==> used_today < cap->0,
{
    if changed == 0 {
        AdoptDecision::Unchanged
    } else if !parts_allowed(changed, allowed) {
        AdoptDecision::NeedsApproval
    } else {
        match cap {
            Some(limit) => {
                if used_today < limit {
                    AdoptDecision::Adopt
                } else {
                    AdoptDecision::CapReached
                }
            }
            None => AdoptDecision::Adopt,
        }
    }
}

/// A change in a part with no allowed bit is never allowed.
proof fn lemma_nothing_allowed(changed: u8)
    by (bit_vector)
    requires
        changed != 0,
    ensures
        changed & !0u8 != 0,
{
}

/// A larger allowed set keeps each allowed change allowed.
proof fn lemma_superset_allows(changed: u8, allowed: u8, wider: u8)
    by (bit_vector)
    requires
        changed & !allowed == 0,
        allowed & !wider == 0,
    ensures
        changed & !wider == 0,
{
}

/// The default policy allows no part. So no overlay goes live without a
/// person.
pub proof fn theorem_the_default_never_adopts(changed: u8, used_today: u32, cap: Option<u32>)
    ensures
        !(spec_decide_adoption(0, changed, used_today, cap) is Adopt),
{
    if changed != 0 {
        lemma_nothing_allowed(changed);
    }
}

/// An overlay that changes nothing never goes live and never counts.
pub proof fn theorem_an_empty_change_is_unchanged(allowed: u8, used_today: u32, cap: Option<u32>)
    ensures
        spec_decide_adoption(allowed, 0, used_today, cap) is Unchanged,
{
}

/// A larger allowed set never turns an adoption into a wait.
pub proof fn theorem_a_wider_policy_keeps_an_adoption(
    allowed: u8,
    wider: u8,
    changed: u8,
    used_today: u32,
    cap: Option<u32>,
)
    requires
        allowed & !wider == 0,
        spec_decide_adoption(allowed, changed, used_today, cap) is Adopt,
    ensures
        spec_decide_adoption(wider, changed, used_today, cap) is Adopt,
{
    lemma_superset_allows(changed, allowed, wider);
}

/// The count of a day after `changes`, from the count `used`. Each adoption
/// adds one.
pub open spec fn spec_day(
    allowed: u8,
    cap: Option<u32>,
    used: u32,
    changes: Seq<u8>,
) -> u32
    decreases changes.len(),
{
    if changes.len() == 0 {
        used
    } else {
        let next = if spec_decide_adoption(allowed, changes[0], used, cap) is Adopt {
            (used + 1) as u32
        } else {
            used
        };
        spec_day(allowed, cap, next, changes.subrange(1, changes.len() as int))
    }
}

/// With a cap of `limit`, a day that starts at or under the cap ends at or
/// under it. So a day of `limit` adoptions adopts no more.
pub proof fn theorem_a_day_never_passes_the_cap(
    allowed: u8,
    limit: u32,
    used: u32,
    changes: Seq<u8>,
)
    requires
        used <= limit,
    ensures
        spec_day(allowed, Some(limit), used, changes) <= limit,
    decreases changes.len(),
{
    if changes.len() > 0 {
        let next = if spec_decide_adoption(allowed, changes[0], used, Some(limit)) is Adopt {
            (used + 1) as u32
        } else {
            used
        };
        theorem_a_day_never_passes_the_cap(
            allowed,
            limit,
            next,
            changes.subrange(1, changes.len() as int),
        );
    }
}

/// A full day refuses each later overlay with `CapReached` or a wait.
pub proof fn theorem_a_full_day_adopts_nothing(allowed: u8, limit: u32, used: u32, changed: u8)
    requires
        used >= limit,
    ensures
        !(spec_decide_adoption(allowed, changed, used, Some(limit)) is Adopt),
{
}

/// `gate_timed_out` as a spec, over the integers.
pub open spec fn spec_gate_timed_out(requested_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms as int >= requested_ms as int + timeout_ms as int
}

// mirror: autumn-harvest-agent/src/refine/policy.rs gate_timed_out
pub fn gate_timed_out(requested_ms: u64, now_ms: u64, timeout_ms: u64) -> (r: bool)
    ensures
        r == spec_gate_timed_out(requested_ms, now_ms, timeout_ms),
{
    now_ms >= requested_ms && now_ms - requested_ms >= timeout_ms
}

/// A gate that timed out stays timed out at each later time.
pub proof fn theorem_a_timeout_is_final(
    requested_ms: u64,
    now_ms: u64,
    later_ms: u64,
    timeout_ms: u64,
)
    requires
        spec_gate_timed_out(requested_ms, now_ms, timeout_ms),
        later_ms >= now_ms,
    ensures
        spec_gate_timed_out(requested_ms, later_ms, timeout_ms),
{
}

} // verus!
