//! The pure rules of a swarm (issue #2814).
//!
//! Each store and the gate call these functions, so every backend decides
//! alike. `formal/verus/src/agent_swarm.rs` proves them for every input.
//!
//! - A claim takes an item only when no other member holds a live lease on
//!   it. A take increments the epoch, so an older claim can never pass the
//!   fence again.
//! - The fence passes only for the holder at the current epoch. It does not
//!   check the lease end. A lapsed claim stays valid until another member
//!   takes the item, and that take moves the epoch.
//! - A three-way merge keeps every change of each side. Two different
//!   changes to one path are a conflict, never a silent choice.
//! - The bar passes a candidate only when it beats the score of main.

/// The status of a work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl ItemStatus {
    /// The stored name, such as `open`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Claimed => "claimed",
            Self::Merged => "merged",
            Self::Dropped => "dropped",
        }
    }

    /// The status with the stored name `name`.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "open" => Some(Self::Open),
            "claimed" => Some(Self::Claimed),
            "merged" => Some(Self::Merged),
            "dropped" => Some(Self::Dropped),
            _ => None,
        }
    }
}

/// Why a claim does not take an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimRefusal {
    /// Another member holds a live lease on the item.
    Taken,
    /// The item is merged or dropped.
    Closed,
    /// The member already holds as many items as the policy allows.
    AtCap,
    /// The epoch counter of the item is full.
    EpochFull,
}

impl ClaimRefusal {
    /// The verdict name that a tool result shows.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Taken => "taken",
            Self::Closed => "closed",
            Self::AtCap => "at_cap",
            Self::EpochFull => "epoch_full",
        }
    }
}

/// What a claim does with an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimDecision {
    /// Take the item at this new epoch.
    Take {
        /// The epoch of the new claim.
        epoch: u64,
    },
    /// The caller already holds the item. Keep its epoch and renew the lease.
    Keep,
    /// Refuse the claim.
    Refuse(ClaimRefusal),
}

/// Decide a claim of one item.
///
/// `mine` says whether the caller is the stored holder. `live` says whether
/// the stored lease is still running. `held` counts the other items that the
/// caller holds, and `cap` bounds it. A take increments `epoch`.
///
/// A second claim by the holder keeps its epoch. So a claim that runs again
/// after a crash does not fence out its own first attempt.
/// `formal/verus/src/agent_swarm.rs` proves the function for every input.
#[must_use]
pub const fn decide_claim(
    status: ItemStatus,
    mine: bool,
    live: bool,
    epoch: u64,
    held: u32,
    cap: u32,
) -> ClaimDecision {
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

/// A take of an item that no live lease holds: the next epoch, within the
/// cap of the caller and the epoch counter.
/// `formal/verus/src/agent_swarm.rs` proves it.
const fn take(epoch: u64, held: u32, cap: u32) -> ClaimDecision {
    if held >= cap {
        ClaimDecision::Refuse(ClaimRefusal::AtCap)
    } else if epoch == u64::MAX {
        ClaimDecision::Refuse(ClaimRefusal::EpochFull)
    } else {
        ClaimDecision::Take { epoch: epoch + 1 }
    }
}

/// Whether a lease that ends at `lease_until_ms` still runs at `now_ms`.
#[must_use]
pub const fn lease_live(lease_until_ms: i64, now_ms: i64) -> bool {
    now_ms < lease_until_ms
}

/// The end of a lease that starts at `now_ms` and lasts `lease_ms`.
///
/// The length is at least 1 ms, so a new lease always runs at `now_ms`. The
/// sum saturates at `i64::MAX`. `formal/verus/src/agent_swarm.rs` proves it.
#[must_use]
pub const fn lease_end(now_ms: i64, lease_ms: i64) -> i64 {
    let step = if lease_ms < 1 { 1 } else { lease_ms };
    if now_ms > i64::MAX - step {
        i64::MAX
    } else {
        now_ms + step
    }
}

/// Why the fence refuses a call on a claimed item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FenceRefusal {
    /// The item is not claimed.
    NotClaimed,
    /// Another member holds the item.
    NotHolder,
    /// The caller holds the item, but at a newer epoch than the call names.
    StaleEpoch,
}

impl FenceRefusal {
    /// The verdict name that a tool result shows.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotClaimed => "not_claimed",
            Self::NotHolder => "not_holder",
            Self::StaleEpoch => "stale_epoch",
        }
    }
}

/// The fence of a renew, a release or a submit.
///
/// It passes only when the item is claimed, the caller is the holder, and
/// the call names the current epoch. It returns `None` when it passes.
/// `formal/verus/src/agent_swarm.rs` proves the function for every input.
#[must_use]
pub const fn fence(
    status: ItemStatus,
    holder_matches: bool,
    epoch_matches: bool,
) -> Option<FenceRefusal> {
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

/// The side that a three-way merge takes for one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pick {
    /// The file of the candidate.
    Ours,
    /// The file of the current main.
    Theirs,
    /// Both sides changed the path in different ways.
    Conflict,
}

/// The pick of a three-way merge for one path.
///
/// `ours_is_base` compares the candidate file with the base file,
/// `theirs_is_base` compares main with the base, and `ours_is_theirs`
/// compares the candidate with main. A missing file counts as a value, so
/// a delete is a change. `formal/verus/src/agent_swarm.rs` proves that a
/// pick that is not a conflict keeps the change of each side.
#[must_use]
pub const fn pick(ours_is_base: bool, theirs_is_base: bool, ours_is_theirs: bool) -> Pick {
    if ours_is_base {
        Pick::Theirs
    } else if theirs_is_base || ours_is_theirs {
        Pick::Ours
    } else {
        Pick::Conflict
    }
}

/// The bar that a candidate must pass to merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bar {
    /// The candidate must beat main by more than `margin` score units.
    Improve {
        /// The gain that does not count, in score units. A negative margin
        /// counts as 0.
        margin: i64,
    },
    /// The candidate must score at least as well as main.
    NoRegression,
}

/// Whether a candidate with the score `candidate` passes `bar` over a main
/// with the score `main`.
///
/// The sum saturates, so a main near `i64::MAX` cannot be beaten by an
/// overflow. `formal/verus/src/agent_swarm.rs` proves the function for every
/// input.
#[must_use]
pub const fn passes(bar: Bar, candidate: i64, main: i64) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn status() -> impl Strategy<Value = ItemStatus> {
        prop_oneof![
            Just(ItemStatus::Open),
            Just(ItemStatus::Claimed),
            Just(ItemStatus::Merged),
            Just(ItemStatus::Dropped),
        ]
    }

    #[test]
    fn a_status_keeps_its_stored_name() {
        for status in [
            ItemStatus::Open,
            ItemStatus::Claimed,
            ItemStatus::Merged,
            ItemStatus::Dropped,
        ] {
            assert_eq!(ItemStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(ItemStatus::parse("done"), None);
    }

    #[test]
    fn an_open_item_is_taken_at_the_next_epoch() {
        let decision = decide_claim(ItemStatus::Open, false, false, 4, 0, 1);
        assert_eq!(decision, ClaimDecision::Take { epoch: 5 });
    }

    #[test]
    fn a_live_lease_of_another_member_refuses_the_claim() {
        let decision = decide_claim(ItemStatus::Claimed, false, true, 4, 0, 1);
        assert_eq!(decision, ClaimDecision::Refuse(ClaimRefusal::Taken));
    }

    #[test]
    fn a_lapsed_lease_lets_another_member_take_the_item() {
        let decision = decide_claim(ItemStatus::Claimed, false, false, 4, 0, 1);
        assert_eq!(decision, ClaimDecision::Take { epoch: 5 });
    }

    #[test]
    fn a_second_claim_by_the_holder_keeps_its_epoch() {
        for live in [true, false] {
            let decision = decide_claim(ItemStatus::Claimed, true, live, 4, 9, 1);
            assert_eq!(decision, ClaimDecision::Keep);
        }
    }

    #[test]
    fn the_cap_and_the_epoch_bound_a_take() {
        assert_eq!(
            decide_claim(ItemStatus::Open, false, false, 0, 2, 2),
            ClaimDecision::Refuse(ClaimRefusal::AtCap)
        );
        assert_eq!(
            decide_claim(ItemStatus::Open, false, false, u64::MAX, 0, 2),
            ClaimDecision::Refuse(ClaimRefusal::EpochFull)
        );
    }

    #[test]
    fn a_lease_ends_after_its_start_and_saturates() {
        assert_eq!(lease_end(100, 50), 150);
        assert_eq!(lease_end(100, 0), 101);
        assert_eq!(lease_end(i64::MAX - 1, 10), i64::MAX);
        assert!(lease_live(lease_end(7, -5), 7));
    }

    #[test]
    fn the_fence_names_each_refusal() {
        assert_eq!(fence(ItemStatus::Claimed, true, true), None);
        assert_eq!(
            fence(ItemStatus::Claimed, false, true),
            Some(FenceRefusal::NotHolder)
        );
        assert_eq!(
            fence(ItemStatus::Claimed, true, false),
            Some(FenceRefusal::StaleEpoch)
        );
        assert_eq!(
            fence(ItemStatus::Merged, true, true),
            Some(FenceRefusal::NotClaimed)
        );
    }

    #[test]
    fn the_bar_needs_a_real_gain() {
        let bar = Bar::Improve { margin: 0 };
        assert!(passes(bar, 11, 10));
        assert!(!passes(bar, 10, 10));
        assert!(!passes(Bar::Improve { margin: 5 }, 15, 10));
        assert!(passes(Bar::Improve { margin: -5 }, 11, 10));
        assert!(!passes(bar, i64::MAX, i64::MAX));
        assert!(passes(Bar::NoRegression, 10, 10));
        assert!(!passes(Bar::NoRegression, 9, 10));
    }

    proptest! {
        #[test]
        fn a_take_never_overrides_a_live_lease_of_another_member(
            status in status(),
            mine: bool,
            live: bool,
            epoch: u64,
            held in 0_u32..4,
            cap in 0_u32..4,
        ) {
            if let ClaimDecision::Take { epoch: next } = decide_claim(status, mine, live, epoch, held, cap) {
                prop_assert!(matches!(status, ItemStatus::Open | ItemStatus::Claimed));
                prop_assert!(!(status == ItemStatus::Claimed && (live || mine)));
                prop_assert_eq!(next, epoch + 1);
                prop_assert!(held < cap);
            }
        }

        #[test]
        fn a_merge_pick_keeps_the_change_of_each_side(base: Option<u8>, ours: Option<u8>, theirs: Option<u8>) {
            let chosen = match pick(ours == base, theirs == base, ours == theirs) {
                Pick::Ours => ours,
                Pick::Theirs => theirs,
                Pick::Conflict => {
                    prop_assert!(ours != base && theirs != base && ours != theirs);
                    return Ok(());
                }
            };
            if ours != base {
                prop_assert_eq!(chosen, ours);
            }
            if theirs != base {
                prop_assert_eq!(chosen, theirs);
            }
        }

        #[test]
        fn a_passing_candidate_beats_main(margin: i64, candidate: i64, main: i64) {
            if passes(Bar::Improve { margin }, candidate, main) {
                prop_assert!(i128::from(candidate) > i128::from(main) + i128::from(margin.max(0)));
            }
            prop_assert_eq!(passes(Bar::NoRegression, candidate, main), candidate >= main);
        }
    }
}
