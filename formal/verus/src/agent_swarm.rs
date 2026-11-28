//! The claims of a swarm are fenced, and its merges keep every change
//! (issue #2814).
//!
//! The copies below prove the kernel in `autumn-harvest-agent/src/swarm/kernel.rs`
//! for every input:
//!
//! - A claim never takes an item from another member that holds a live
//!   lease. A merged or dropped item is never taken.
//! - A take increments the epoch. So after any change of holder, a call that
//!   names an older epoch fails the fence. A stale claim never writes.
//! - A three-way pick that is not a conflict keeps the change of each side.
//! - The bar passes a candidate only when it beats main by more than the
//!   margin.

use vstd::prelude::*;

verus! {

/// The status of a work item.
pub enum ItemStatus {
    /// No member holds the item.
    Open,
    /// A member holds the item.
    Claimed,
    /// A merge closed the item.
    Merged,
    /// An operator closed the item.
    Dropped,
}

/// Why a claim does not take an item.
pub enum ClaimRefusal {
    /// Another member holds a live lease on the item.
    Taken,
    /// The item is merged or dropped.
    Closed,
    /// The member holds as many items as the policy allows.
    AtCap,
    /// The epoch counter of the item is full.
    EpochFull,
}

/// What a claim does with an item.
pub enum ClaimDecision {
    /// Take the item at this new epoch.
    Take {
        /// The epoch of the new claim.
        epoch: u64,
    },
    /// The caller already holds the item.
    Keep,
    /// Refuse the claim.
    Refuse(ClaimRefusal),
}

/// `take` as a spec.
pub open spec fn spec_take(epoch: u64, held: u32, cap: u32) -> ClaimDecision {
    if held >= cap {
        ClaimDecision::Refuse(ClaimRefusal::AtCap)
    } else if epoch == u64::MAX {
        ClaimDecision::Refuse(ClaimRefusal::EpochFull)
    } else {
        ClaimDecision::Take { epoch: (epoch + 1) as u64 }
    }
}

/// `decide_claim` as a spec.
pub open spec fn spec_decide_claim(
    status: ItemStatus,
    mine: bool,
    live: bool,
    epoch: u64,
    held: u32,
    cap: u32,
) -> ClaimDecision {
    match status {
        ItemStatus::Merged => ClaimDecision::Refuse(ClaimRefusal::Closed),
        ItemStatus::Dropped => ClaimDecision::Refuse(ClaimRefusal::Closed),
        ItemStatus::Claimed => if mine {
            ClaimDecision::Keep
        } else if live {
            ClaimDecision::Refuse(ClaimRefusal::Taken)
        } else {
            spec_take(epoch, held, cap)
        },
        ItemStatus::Open => spec_take(epoch, held, cap),
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs take
pub fn take(epoch: u64, held: u32, cap: u32) -> (r: ClaimDecision)
    ensures
        r == spec_take(epoch, held, cap),
{
    if held >= cap {
        ClaimDecision::Refuse(ClaimRefusal::AtCap)
    } else if epoch == u64::MAX {
        ClaimDecision::Refuse(ClaimRefusal::EpochFull)
    } else {
        ClaimDecision::Take { epoch: epoch + 1 }
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs decide_claim
pub fn decide_claim(
    status: ItemStatus,
    mine: bool,
    live: bool,
    epoch: u64,
    held: u32,
    cap: u32,
) -> (r: ClaimDecision)
    ensures
        r == spec_decide_claim(status, mine, live, epoch, held, cap),
{
    match status {
        ItemStatus::Merged | ItemStatus::Dropped => ClaimDecision::Refuse(ClaimRefusal::Closed),
        ItemStatus::Claimed => {
            if mine {
                ClaimDecision::Keep
            } else if live {
                ClaimDecision::Refuse(ClaimRefusal::Taken)
            } else {
                take(epoch, held, cap)
            }
        }
        ItemStatus::Open => take(epoch, held, cap),
    }
}

/// A take never overrides a live lease of another member. It never takes a
/// closed item. It stays within the cap and moves the epoch up by one.
pub proof fn theorem_a_take_respects_a_live_lease(
    status: ItemStatus,
    mine: bool,
    live: bool,
    epoch: u64,
    held: u32,
    cap: u32,
    next: u64,
)
    requires
        spec_decide_claim(status, mine, live, epoch, held, cap) == (ClaimDecision::Take { epoch: next }),
    ensures
        status == ItemStatus::Open || (status == ItemStatus::Claimed && !mine && !live),
        held < cap,
        next as int == epoch as int + 1,
{
}

/// A closed item refuses every claim, and the holder keeps its own claim.
pub proof fn theorem_closed_items_refuse_and_holders_keep(
    status: ItemStatus,
    mine: bool,
    live: bool,
    epoch: u64,
    held: u32,
    cap: u32,
)
    ensures
        (status == ItemStatus::Merged || status == ItemStatus::Dropped) ==> spec_decide_claim(
            status,
            mine,
            live,
            epoch,
            held,
            cap,
        ) == ClaimDecision::Refuse(ClaimRefusal::Closed),
        status == ItemStatus::Claimed && mine ==> spec_decide_claim(
            status,
            mine,
            live,
            epoch,
            held,
            cap,
        ) == ClaimDecision::Keep,
{
}

/// `lease_end` as a spec.
pub open spec fn spec_lease_end(now_ms: i64, lease_ms: i64) -> i64 {
    let step: int = if lease_ms < 1 { 1 } else { lease_ms as int };
    if now_ms + step > i64::MAX {
        i64::MAX
    } else {
        (now_ms + step) as i64
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs lease_end
pub fn lease_end(now_ms: i64, lease_ms: i64) -> (r: i64)
    ensures
        r == spec_lease_end(now_ms, lease_ms),
        now_ms < i64::MAX ==> now_ms < r,
        now_ms <= r,
{
    let step = if lease_ms < 1 { 1 } else { lease_ms };
    if now_ms > i64::MAX - step {
        i64::MAX
    } else {
        now_ms + step
    }
}

/// Why the fence refuses a call.
pub enum FenceRefusal {
    /// The item is not claimed.
    NotClaimed,
    /// Another member holds the item.
    NotHolder,
    /// The call names an older epoch.
    StaleEpoch,
}

/// `fence` as a spec.
pub open spec fn spec_fence(status: ItemStatus, holder_matches: bool, epoch_matches: bool) -> Option<
    FenceRefusal,
> {
    match status {
        ItemStatus::Claimed => if !holder_matches {
            Some(FenceRefusal::NotHolder)
        } else if !epoch_matches {
            Some(FenceRefusal::StaleEpoch)
        } else {
            None
        },
        _ => Some(FenceRefusal::NotClaimed),
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs fence
pub fn fence(status: ItemStatus, holder_matches: bool, epoch_matches: bool) -> (r: Option<
    FenceRefusal,
>)
    ensures
        r == spec_fence(status, holder_matches, epoch_matches),
        r.is_none() <==> (status == ItemStatus::Claimed && holder_matches && epoch_matches),
{
    match status {
        ItemStatus::Claimed => {
            if !holder_matches {
                Some(FenceRefusal::NotHolder)
            } else if !epoch_matches {
                Some(FenceRefusal::StaleEpoch)
            } else {
                None
            }
        }
        ItemStatus::Open | ItemStatus::Merged | ItemStatus::Dropped => {
            Some(FenceRefusal::NotClaimed)
        }
    }
}

/// The stored state of one item. Members are numbers in the model.
pub struct Item {
    pub status: ItemStatus,
    pub holder: int,
    pub epoch: u64,
    pub lease_until: i64,
}

/// The item after a claim by `member` at `now`, as the store applies the
/// decision. A take writes the holder, the epoch and the lease. A keep
/// renews the lease. A refusal changes nothing.
pub open spec fn claimed(item: Item, member: int, now: i64, lease: i64, held: u32, cap: u32) -> Item {
    let decision = spec_decide_claim(
        item.status,
        item.holder == member,
        now < item.lease_until,
        item.epoch,
        held,
        cap,
    );
    match decision {
        ClaimDecision::Take { epoch } => Item {
            status: ItemStatus::Claimed,
            holder: member,
            epoch,
            lease_until: spec_lease_end(now, lease),
        },
        ClaimDecision::Keep => Item {
            status: item.status,
            holder: item.holder,
            epoch: item.epoch,
            lease_until: spec_lease_end(now, lease),
        },
        ClaimDecision::Refuse(_) => item,
    }
}

/// A claim never moves the epoch down, and a change of holder always moves
/// it up. So the old holder fails the fence on the new state, whatever
/// epoch it names that it held before.
pub proof fn theorem_a_new_holder_fences_out_the_old_one(
    item: Item,
    member: int,
    now: i64,
    lease: i64,
    held: u32,
    cap: u32,
    old_epoch: u64,
)
    requires
        item.status == ItemStatus::Claimed,
        old_epoch <= item.epoch,
    ensures
        ({
            let next = claimed(item, member, now, lease, held, cap);
            &&& item.epoch <= next.epoch
            &&& next.holder != item.holder ==> item.epoch < next.epoch
            &&& next.holder != item.holder ==> spec_fence(
                next.status,
                item.holder == next.holder,
                old_epoch == next.epoch,
            ).is_some()
            &&& next.holder != item.holder ==> spec_fence(
                next.status,
                true,
                old_epoch == next.epoch,
            ) == Some(FenceRefusal::StaleEpoch)
        }),
{
}

/// A claim never changes the holder of an item that another member holds
/// with a live lease.
pub proof fn theorem_a_live_lease_keeps_its_holder(
    item: Item,
    member: int,
    now: i64,
    lease: i64,
    held: u32,
    cap: u32,
)
    requires
        item.status == ItemStatus::Claimed,
        item.holder != member,
        now < item.lease_until,
    ensures
        claimed(item, member, now, lease, held, cap) == item,
{
}

/// The side that a three-way merge takes for one path.
pub enum Pick {
    /// The file of the candidate.
    Ours,
    /// The file of the current main.
    Theirs,
    /// Both sides changed the path in different ways.
    Conflict,
}

/// `pick` as a spec.
pub open spec fn spec_pick(ours_is_base: bool, theirs_is_base: bool, ours_is_theirs: bool) -> Pick {
    if ours_is_base {
        Pick::Theirs
    } else if theirs_is_base || ours_is_theirs {
        Pick::Ours
    } else {
        Pick::Conflict
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs pick
pub fn pick(ours_is_base: bool, theirs_is_base: bool, ours_is_theirs: bool) -> (r: Pick)
    ensures
        r == spec_pick(ours_is_base, theirs_is_base, ours_is_theirs),
{
    if ours_is_base {
        Pick::Theirs
    } else if theirs_is_base || ours_is_theirs {
        Pick::Ours
    } else {
        Pick::Conflict
    }
}

/// The file that a pick keeps. A missing file is a value too.
pub open spec fn chosen(base: int, ours: int, theirs: int) -> Option<int> {
    match spec_pick(ours == base, theirs == base, ours == theirs) {
        Pick::Ours => Some(ours),
        Pick::Theirs => Some(theirs),
        Pick::Conflict => None,
    }
}

/// A pick that is not a conflict keeps the change of each side. A conflict
/// comes only from two different changes.
pub proof fn theorem_a_merge_keeps_every_change(base: int, ours: int, theirs: int)
    ensures
        chosen(base, ours, theirs).is_none() <==> (ours != base && theirs != base && ours != theirs),
        chosen(base, ours, theirs).is_some() && ours != base ==> chosen(base, ours, theirs) == Some(
            ours,
        ),
        chosen(base, ours, theirs).is_some() && theirs != base ==> chosen(base, ours, theirs)
            == Some(theirs),
        ours == base && theirs == base ==> chosen(base, ours, theirs) == Some(base),
{
}

/// The bar that a candidate must pass.
pub enum Bar {
    /// Beat main by more than `margin` score units.
    Improve {
        /// The gain that does not count.
        margin: i64,
    },
    /// Score at least as well as main.
    NoRegression,
}

/// `passes` as a spec, over unbounded integers.
pub open spec fn spec_passes(bar: Bar, candidate: i64, main: i64) -> bool {
    match bar {
        Bar::NoRegression => candidate >= main,
        Bar::Improve { margin } => {
            let gain: int = if margin < 0 { 0 } else { margin as int };
            candidate > main + gain
        },
    }
}

// mirror: autumn-harvest-agent/src/swarm/kernel.rs passes
pub fn passes(bar: Bar, candidate: i64, main: i64) -> (r: bool)
    ensures
        r == spec_passes(bar, candidate, main),
        r && bar == (Bar::Improve { margin: 0 }) ==> candidate > main,
{
    match bar {
        Bar::NoRegression => candidate >= main,
        Bar::Improve { margin } => {
            let gain = if margin < 0 { 0 } else { margin };
            if main > i64::MAX - gain {
                false
            } else {
                candidate > main + gain
            }
        }
    }
}

} // verus!
