//! The applied set of peer messages (issue #2269).
//!
//! Each sender numbers its messages from 1. The receiver keeps the numbers
//! that it applied, as a floor and a set above it. A message is a duplicate
//! only when its number is in that set.
//!
//! The TLA+ model `formal/tla/AgentMesh.tla` shows why the receiver must not
//! drop each number at or under the highest one it saw. The outbox can
//! deliver message 2 before message 1. A high-water mark would then drop
//! message 1, and the message would be lost.
//!
//! `formal/verus/src/agent_dedupe.rs` proves the kernel [`is_new`] and
//! [`next_floor`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The most senders that the receiver tracks. A sender past the bound drops
/// the least recent one. The engine dedupes the send key on its own, so an
/// evicted sender still gets each message once.
pub const MAX_SENDERS: usize = 1_024;

/// The most numbers above the floor of one sender.
///
/// A send that failed for good leaves a gap that never fills, so the set
/// would grow without a bound. Past the bound, the set forgets its lowest
/// number. The floor never moves over a gap, so a late message is never
/// taken for a duplicate. A forgotten number relies on the engine key
/// dedupe alone.
pub const MAX_ABOVE: usize = 256;

/// Whether the number `seq` is new for a sender whose floor is `floor`.
///
/// `in_above` says whether the set above the floor holds `seq`.
/// `formal/verus/src/agent_dedupe.rs` proves it.
#[must_use]
pub const fn is_new(floor: u64, seq: u64, in_above: bool) -> bool {
    seq > floor && !in_above
}

/// The floor after one step: it moves up by one when the set above holds
/// `floor + 1`. `formal/verus/src/agent_dedupe.rs` proves that the fold
/// keeps the applied set exact.
#[must_use]
pub const fn next_floor(floor: u64, next_in_above: bool) -> u64 {
    if next_in_above && floor < u64::MAX {
        floor + 1
    } else {
        floor
    }
}

/// The applied numbers of one sender: each number up to `floor`, and each
/// number in `above`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Applied {
    /// Each number up to and including the floor is applied.
    pub floor: u64,
    /// The applied numbers over the floor.
    pub above: BTreeSet<u64>,
    /// The op count at the last message, for eviction.
    pub seen: u64,
}

impl Applied {
    /// Whether `seq` is applied already.
    #[must_use]
    pub fn contains(&self, seq: u64) -> bool {
        !is_new(self.floor, seq, self.above.contains(&seq))
    }

    /// Mark `seq` as applied. Returns `false` when it was applied already.
    pub fn apply(&mut self, seq: u64) -> bool {
        if self.contains(seq) {
            return false;
        }
        self.above.insert(seq);
        loop {
            // A floor at the top of the range has no next number.
            let has_next = self
                .floor
                .checked_add(1)
                .is_some_and(|next| self.above.contains(&next));
            let next = next_floor(self.floor, has_next);
            if next == self.floor {
                break;
            }
            self.above.remove(&next);
            self.floor = next;
        }
        if self.above.len() > MAX_ABOVE {
            self.above.pop_first();
        }
        true
    }
}

/// The applied sets of every sender of one receiver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Inbound {
    /// The applied set of each sender, by sender id.
    pub senders: BTreeMap<String, Applied>,
    /// Peer messages taken, for eviction order.
    pub taken: u64,
}

impl Inbound {
    /// Mark the message `seq` of `sender` as applied. Returns `false` for a
    /// duplicate.
    pub fn apply(&mut self, sender: &str, seq: u64) -> bool {
        self.taken = self.taken.saturating_add(1);
        let taken = self.taken;
        if !self.senders.contains_key(sender) && self.senders.len() >= MAX_SENDERS {
            let oldest = self
                .senders
                .iter()
                .min_by_key(|(_, applied)| applied.seen)
                .map(|(id, _)| id.clone());
            if let Some(oldest) = oldest {
                self.senders.remove(&oldest);
            }
        }
        let applied = self.senders.entry(sender.to_owned()).or_default();
        applied.seen = taken;
        applied.apply(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn in_order_numbers_fold_into_the_floor() {
        let mut applied = Applied::default();
        for seq in 1..=5 {
            assert!(applied.apply(seq));
        }
        assert_eq!(applied.floor, 5);
        assert!(applied.above.is_empty());
        assert!(!applied.apply(3), "a repeat is a duplicate");
    }

    #[test]
    fn a_late_message_is_not_dropped() {
        // The trace that `AgentMeshLastSeq.cfg` finds: 2 lands before 1.
        let mut applied = Applied::default();
        assert!(applied.apply(2));
        assert!(applied.apply(1), "message 1 is new, not a duplicate");
        assert_eq!(applied.floor, 2);
        assert!(!applied.apply(1));
        assert!(!applied.apply(2));
    }

    #[test]
    fn the_set_over_the_floor_has_a_bound_and_never_drops_a_gap() {
        let mut applied = Applied::default();
        // Number 1 never comes, so the set grows past its bound.
        for seq in 2..=(u64::try_from(MAX_ABOVE).unwrap() + 2) {
            applied.apply(seq);
        }
        assert_eq!(applied.above.len(), MAX_ABOVE);
        assert_eq!(applied.floor, 0, "the floor never moves over a gap");
        assert!(applied.apply(1), "a very late message still applies");
    }

    #[test]
    fn a_floor_at_the_top_of_the_range_does_not_overflow() {
        let mut applied = Applied {
            floor: u64::MAX - 1,
            ..Applied::default()
        };
        assert!(applied.apply(u64::MAX));
        assert_eq!(applied.floor, u64::MAX);
        assert!(!applied.apply(u64::MAX), "a repeat is a duplicate");
    }

    #[test]
    fn the_sender_map_has_a_bound() {
        let mut inbound = Inbound::default();
        for n in 0..=MAX_SENDERS {
            assert!(inbound.apply(&format!("s{n}"), 1));
        }
        assert_eq!(inbound.senders.len(), MAX_SENDERS);
        assert!(!inbound.senders.contains_key("s0"), "the oldest goes");
    }

    proptest! {
        /// Any order and any repeats: each number applies exactly once.
        #[test]
        fn each_number_applies_once(order in prop::collection::vec(1_u64..40, 0..120)) {
            let mut applied = Applied::default();
            let mut plain = BTreeSet::new();
            for seq in order {
                prop_assert_eq!(applied.apply(seq), plain.insert(seq));
                prop_assert!(applied.contains(seq));
            }
            for seq in 1..40 {
                prop_assert_eq!(applied.contains(seq), plain.contains(&seq));
            }
        }
    }
}
