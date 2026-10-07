//! Property tests for the calendar helpers (issue #1968).
//!
//! Contract under test:
//! - `add_business_days` is monotone in `n`: when `n` resolves, each `m < n`
//!   resolves too.
//! - `apply_skip_policy` does not panic, also at the `NaiveDate` range limits.

use std::collections::BTreeSet;

use autumn_harvest::policy::SkipPolicy;
use autumn_harvest::{add_business_days, apply_skip_policy};
use chrono::{Days, NaiveDate};
use proptest::prelude::*;

use super::prop_config::config;

/// The first date of the generated calendars.
fn origin() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
}

/// Holiday closures as `(start offset, length)` pairs, in days from
/// [`origin`]. Lengths go past 30, so some runs are too long to cross.
fn closures() -> impl Strategy<Value = Vec<(u64, u64)>> {
    prop::collection::vec((0u64..120, 1u64..45), 0..4)
}

fn calendar(closures: &[(u64, u64)]) -> BTreeSet<NaiveDate> {
    closures
        .iter()
        .flat_map(|&(start, len)| start..start + len)
        .map(|offset| origin() + Days::new(offset))
        .collect()
}

fn skip_policy() -> impl Strategy<Value = SkipPolicy> {
    prop_oneof![
        Just(SkipPolicy::Skip),
        Just(SkipPolicy::RunNextBusinessDay),
        Just(SkipPolicy::RunPrevBusinessDay),
    ]
}

proptest! {
    #![proptest_config(config())]

    /// A smaller `n` resolves when a larger `n` resolves.
    #[test]
    fn add_business_days_is_monotone_in_n(
        closures in closures(),
        anchor_offset in 0u64..120,
        n in 1u32..20,
        m_back in 1u32..20,
    ) {
        let cal = calendar(&closures);
        let anchor = (origin() + Days::new(anchor_offset))
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let m = n.saturating_sub(m_back);
        if add_business_days(anchor, n, &cal).is_ok() {
            prop_assert!(
                add_business_days(anchor, m, &cal).is_ok(),
                "n = {n} resolves, m = {m} rejects"
            );
        }
    }

    /// The skip scan does not panic near `NaiveDate::MAX` or `NaiveDate::MIN`.
    /// The proptest harness makes a panic a failed case.
    #[test]
    fn apply_skip_policy_is_total_at_the_range_limits(
        near_max in any::<bool>(),
        offset in 0u64..10,
        excluded_offsets in prop::collection::vec(0u64..10, 0..10),
        policy in skip_policy(),
        exclude_weekends in any::<bool>(),
    ) {
        let at = |o: u64| {
            if near_max {
                NaiveDate::MAX - Days::new(o)
            } else {
                NaiveDate::MIN + Days::new(o)
            }
        };
        let excluded: Vec<NaiveDate> = excluded_offsets.iter().map(|&o| at(o)).collect();
        let _ = apply_skip_policy(at(offset), policy, &excluded, exclude_weekends);
    }
}
