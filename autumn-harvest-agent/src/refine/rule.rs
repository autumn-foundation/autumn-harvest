//! The rule pick of a refine run.
//!
//! The rule orders the probed candidates of one round:
//!
//! 1. A live failure has no key, so the rule never picks it.
//! 2. A completed replay end comes before an end that did not complete.
//! 3. Fewer stubbed tool calls come first.
//! 4. The lowest index comes first.
//!
//! The select step and the fallback of the run use the same kernel.

/// The key of one candidate, or `None` for a live failure. A lower key
/// comes first.
///
/// `formal/verus/src/refine_rule.rs` proves that a completed end comes
/// before each end that did not complete.
#[must_use]
#[allow(clippy::cast_lossless)]
pub const fn rule_key(live_failed: bool, completed: bool, stubbed: u32) -> Option<u64> {
    if live_failed {
        None
    } else if completed {
        Some(stubbed as u64)
    } else {
        Some(0x1_0000_0000 + stubbed as u64)
    }
}

/// The better of `best` and the entry `next`.
///
/// An entry is an index and a key. A live failure has no key.
/// `formal/verus/src/refine_rule.rs` proves the kernel.
const fn rule_step(best: Option<(u32, u64)>, next: (u32, Option<u64>)) -> Option<(u32, u64)> {
    match next.1 {
        None => best,
        Some(key) => match best {
            None => Some((next.0, key)),
            Some(b) => {
                if key < b.1 || (key == b.1 && next.0 < b.0) {
                    Some((next.0, key))
                } else {
                    best
                }
            }
        },
    }
}

/// The best entry of `entries`: the lowest key, then the lowest index.
///
/// `formal/verus/src/refine_rule.rs` proves two properties. A live failure
/// never wins, so the result is `None` only when each entry is one. The
/// winner has the lowest key.
pub const fn rule_best(entries: &[(u32, Option<u64>)]) -> Option<(u32, u64)> {
    let mut best = None;
    let mut i = 0;
    while i < entries.len() {
        best = rule_step(best, entries[i]);
        i += 1;
    }
    best
}

/// The position in `entries` of the rule pick. Each entry holds three
/// values: whether it is a live failure, whether its replay completed, and
/// its stubbed call count.
pub fn rule_pick(entries: impl IntoIterator<Item = (bool, bool, u32)>) -> Option<usize> {
    let keyed: Vec<(u32, Option<u64>)> = entries
        .into_iter()
        .enumerate()
        .map(|(at, (live_failed, completed, stubbed))| {
            (
                u32::try_from(at).unwrap_or(u32::MAX),
                rule_key(live_failed, completed, stubbed),
            )
        })
        .collect();
    rule_best(&keyed).and_then(|(at, _key)| usize::try_from(at).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_live_failure_is_never_picked() {
        assert_eq!(rule_pick([(true, true, 0)]), None);
        assert_eq!(rule_pick([(true, true, 0), (false, false, 9)]), Some(1));
        assert_eq!(rule_pick(std::iter::empty()), None);
    }

    #[test]
    fn a_completed_end_comes_before_fewer_stubs() {
        assert_eq!(rule_pick([(false, false, 0), (false, true, 7)]), Some(1));
        assert_eq!(rule_pick([(false, true, 3), (false, true, 2)]), Some(1));
    }

    #[test]
    fn a_tie_goes_to_the_lowest_index() {
        assert_eq!(rule_pick([(false, true, 2), (false, true, 2)]), Some(0));
        assert_eq!(
            rule_pick([(true, true, 0), (false, false, 1), (false, false, 1)]),
            Some(1)
        );
    }

    proptest! {
        /// The kernel picks what a plain model of the rule picks. The model
        /// skips each live failure. It then takes a completed end, then the
        /// fewest stubs, then the lowest index.
        #[test]
        fn the_rule_picks_the_lowest_key(
            entries in prop::collection::vec(
                (any::<bool>(), any::<bool>(), prop_oneof![0u32..4, any::<u32>()]),
                0..12,
            )
        ) {
            let model = entries
                .iter()
                .enumerate()
                .filter(|(_, (live_failed, _, _))| !live_failed)
                .min_by_key(|(at, (_, completed, stubbed))| (!completed, *stubbed, *at))
                .map(|(at, _)| at);
            prop_assert_eq!(rule_pick(entries.iter().copied()), model);
        }
    }
}
