//! Property tests for [`autumn_harvest::task_duration`] (Tier A target 5).
//!
//! Contract under test:
//! - a generated `(value, unit)` round-trips to `Duration::from_secs(value *
//!   unit_secs)` for `value >= 1` and no overflow;
//! - the parser never panics on arbitrary input (total function);
//! - a compound value (`"1h 30m"`) sums its components.

use std::time::Duration;

use autumn_harvest::task_duration;
use proptest::prelude::*;

use super::prop_config::config;

fn unit_secs(unit: char) -> u64 {
    match unit {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        _ => unreachable!(),
    }
}

proptest! {
    #![proptest_config(config())]

    /// A single `(value, unit)` string round-trips to the expected `Duration`.
    /// `value` is bounded so `value * 86400` cannot overflow `u64`.
    #[test]
    fn single_component_round_trips(
        value in 1u64..=1_000_000,
        unit in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')],
    ) {
        let s = format!("{value}{unit}");
        let expected = Duration::from_secs(value * unit_secs(unit));
        prop_assert_eq!(task_duration(&s), Some(expected), "round-trip failed for {:?}", s);
    }

    /// Two components sum (space-separated, as the parser allows spaces).
    #[test]
    fn two_components_sum(
        v1 in 1u64..=100_000,
        u1 in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')],
        v2 in 1u64..=100_000,
        u2 in prop_oneof![Just('s'), Just('m'), Just('h'), Just('d')],
    ) {
        let s = format!("{v1}{u1} {v2}{u2}");
        let expected = Duration::from_secs(v1 * unit_secs(u1) + v2 * unit_secs(u2));
        prop_assert_eq!(task_duration(&s), Some(expected), "sum failed for {:?}", s);
    }

    /// Total function: never panics on arbitrary input (garbage, unicode, empty).
    #[test]
    fn never_panics_on_arbitrary_input(s in any::<String>()) {
        let _ = task_duration(&s);
    }

    /// A string containing a control/punctuation character that is neither an
    /// ASCII digit, ASCII letter, nor space is always rejected (never a panic,
    /// never an accidental parse).
    #[test]
    fn rejects_strings_with_disallowed_chars(
        prefix in "[0-9]{0,4}",
        bad in prop_oneof![Just('!'), Just('/'), Just('@'), Just('.'), Just('\n'), Just('\t')],
        suffix in "[0-9a-z]{0,4}",
    ) {
        let s = format!("{prefix}{bad}{suffix}");
        prop_assert_eq!(task_duration(&s), None, "disallowed char accepted in {:?}", s);
    }
}
