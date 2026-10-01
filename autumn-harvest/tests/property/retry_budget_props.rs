//! Property tests for the per-activity-type retry budget (issue #1793).
//!
//! Contract under test:
//! - Retries that run never exceed
//!   `max_tokens + ratio * first_attempts + min_retries_per_sec * elapsed`.
//! - A first attempt always runs.
//! - Available tokens stay in `[0, max_tokens]`.
//! - A deferral delay stays in the documented band.

use std::time::{Duration, Instant};

use autumn_harvest::policy::RetryBudgetPolicy;
use autumn_harvest::retry_budget::{
    Admission, MAX_RETRY_BUDGET_DEFER, MIN_RETRY_BUDGET_DEFER, RetryBudgetConfig,
    RetryBudgetRegistry,
};
use proptest::prelude::*;

use super::prop_config::config;

/// One step of a simulated worker: an attempt, then a clock advance.
#[derive(Debug, Clone, Copy)]
struct Step {
    is_retry: bool,
    advance_ms: u64,
}

fn step() -> impl Strategy<Value = Step> {
    (any::<bool>(), 0u64..=200).prop_map(|(is_retry, advance_ms)| Step {
        is_retry,
        advance_ms,
    })
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn retries_never_exceed_the_budget(
        ratio in 0.0f64..=1.0,
        max_tokens in 1.0f64..=50.0,
        floor in 0.0f64..=20.0,
        steps in prop::collection::vec(step(), 1..400),
    ) {
        let policy = RetryBudgetPolicy::new(ratio, max_tokens, floor);
        let reg = RetryBudgetRegistry::new(RetryBudgetConfig::disabled().with_default(Some(policy)));
        let start = Instant::now();
        let mut now = start;
        let mut first_attempts = 0_u32;
        let mut retries_run = 0_u32;

        for s in steps {
            let admission = reg.admit("act", s.is_retry, now);
            match admission {
                Admission::Admitted { available, .. } => {
                    prop_assert!((0.0..=policy.max_tokens + 1e-9).contains(&available));
                    if s.is_retry {
                        retries_run += 1;
                    } else {
                        first_attempts += 1;
                    }
                }
                Admission::Deferred { retry_after, available } => {
                    prop_assert!(s.is_retry, "a first attempt was deferred");
                    prop_assert!(available < 1.0);
                    prop_assert!(retry_after >= MIN_RETRY_BUDGET_DEFER);
                    prop_assert!(retry_after <= MAX_RETRY_BUDGET_DEFER);
                }
                Admission::Untracked => prop_assert!(false, "a budgeted type was untracked"),
            }
            now += Duration::from_millis(s.advance_ms);
        }

        let elapsed = now.duration_since(start).as_secs_f64();
        let budget = policy.min_retries_per_sec.mul_add(
            elapsed,
            policy.ratio.mul_add(f64::from(first_attempts), policy.max_tokens),
        );
        prop_assert!(
            f64::from(retries_run) <= budget + 1e-6,
            "{} retries ran; budget is {}", retries_run, budget
        );
    }
}
