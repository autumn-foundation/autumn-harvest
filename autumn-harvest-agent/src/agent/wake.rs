//! The wake kernel of a durable agent (issue #2266).
//!
//! The kernel is pure. `formal/verus/src/agent_wake.rs` proves that a tick
//! always books the next tick strictly after the time it ran. A tick that
//! did not move the next one would be due again at once, and the agent would
//! tick in a loop.

/// The next heartbeat tick, in epoch milliseconds.
///
/// `current` is the booked tick. `every_ms` is the interval of the heartbeat
/// that applies. `now` is the recorded time of the op. `ticked` says whether
/// the op was a tick. With no heartbeat, nothing is booked. A tick, or a missing
/// booking, books `now + every_ms`. Any other op keeps the booking.
///
/// The interval is at least 1 ms. The sum saturates at `i64::MAX`.
/// `formal/verus/src/agent_wake.rs` proves the function for every input.
#[must_use]
pub const fn next_heartbeat(
    current: Option<i64>,
    every_ms: Option<i64>,
    now: i64,
    ticked: bool,
) -> Option<i64> {
    match every_ms {
        None => None,
        Some(every) => match current {
            Some(at) if !ticked => Some(at),
            _ => {
                // Verus has no spec of `i64::saturating_add`, so the sum
                // saturates here. The step is positive, so only the top
                // bound applies.
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn no_heartbeat_books_nothing() {
        assert_eq!(next_heartbeat(Some(5), None, 1, true), None);
    }

    #[test]
    fn a_first_op_books_one_interval_ahead() {
        assert_eq!(
            next_heartbeat(None, Some(60_000), 1_000, false),
            Some(61_000)
        );
    }

    #[test]
    fn another_op_keeps_the_booking() {
        assert_eq!(
            next_heartbeat(Some(5_000), Some(60_000), 9_000, false),
            Some(5_000)
        );
    }

    #[test]
    fn the_booking_saturates_and_the_step_is_at_least_one() {
        assert_eq!(
            next_heartbeat(None, Some(10), i64::MAX - 5, true),
            Some(i64::MAX)
        );
        assert_eq!(
            next_heartbeat(None, Some(5), i64::MAX - 5, true),
            Some(i64::MAX)
        );
        assert_eq!(next_heartbeat(Some(3), Some(0), 7, true), Some(8));
        assert_eq!(next_heartbeat(Some(3), Some(-4), 7, true), Some(8));
    }

    proptest! {
        /// A tick always books the next tick after `now`, so it is never due
        /// again at once.
        #[test]
        fn a_tick_books_strictly_later(
            current in proptest::option::of(any::<i64>()),
            every in 1_i64..i64::MAX,
            now in i64::MIN..i64::MAX,
        ) {
            let next = next_heartbeat(current, Some(every), now, true).unwrap();
            prop_assert!(next > now);
        }
    }
}
