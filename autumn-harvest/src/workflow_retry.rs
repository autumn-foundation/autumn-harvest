//! The operator override of a workflow retry policy (issue #2409).
//!
//! [`WorkflowInfo::retry_policy`](crate::info::WorkflowInfo::retry_policy)
//! decides whether a failed run starts another attempt. Each start freezes a
//! policy on its execution row. A failure reads that stored policy. An
//! operator override replaces three of its fields at runtime:
//! `max_attempts`, `initial_interval` and `max_interval`. The other fields
//! stay as declared.
//!
//! ## Where the override applies
//!
//! The failure path applies the override over the stored policy of the
//! failed run. So it applies to the next failure of any run of the type, in
//! flight or new. A retry successor copies the stored policy, not the
//! effective one. So a clear restores the code value at the next failure.
//!
//! ## A schedule policy wins
//!
//! A schedule can declare its own `retry_policy`. It wins for the runs that
//! the schedule starts, and the override does not apply to them. The stored
//! JSON of such a run holds the key [`SOURCE_KEY`] with the value
//! [`SCHEDULE_SOURCE`]. Each schedule path picks its policy through
//! [`schedule_retry_policy`]. An older binary ignores the key.

use std::time::Duration;

use crate::policy::RetryPolicy;

/// The JSON key that names the source of a stored retry policy.
pub const SOURCE_KEY: &str = "_harvest_retry_source";

/// The [`SOURCE_KEY`] value of a policy that a schedule declares.
pub const SCHEDULE_SOURCE: &str = "schedule";

/// Who chose the retry policy that a run stores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetryPolicySource {
    /// The workflow type declares the policy in code. An override applies.
    #[default]
    Type,
    /// The schedule that started the run declares the policy. It wins over
    /// an override.
    Schedule,
}

/// The retry fields that an override can set.
///
/// `None` inherits the stored value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryLimits {
    /// The maximum number of attempts, the first included. `1` stops the
    /// retries.
    pub max_attempts: Option<u32>,
    /// The delay before the first retry.
    pub initial_interval: Option<Duration>,
    /// The upper bound on the delay between retries.
    pub max_interval: Option<Duration>,
}

impl RetryLimits {
    /// The three fields of `policy`, each set.
    #[must_use]
    pub const fn of(policy: &RetryPolicy) -> Self {
        Self {
            max_attempts: Some(policy.max_attempts),
            initial_interval: Some(policy.initial_interval),
            max_interval: Some(policy.max_interval),
        }
    }

    /// Whether the override sets no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_attempts.is_none()
            && self.initial_interval.is_none()
            && self.max_interval.is_none()
    }

    /// Apply these limits over `policy`.
    ///
    /// Each set field replaces the field of `policy`. `max_interval` caps each
    /// delay, so a cap below the first delay hides that delay. When the
    /// limits set an interval, `max_interval` therefore rises to
    /// `initial_interval` if it is lower. A longer first delay alone then
    /// slows the retries.
    #[must_use]
    pub fn apply(&self, mut policy: RetryPolicy) -> RetryPolicy {
        if let Some(max_attempts) = self.max_attempts {
            policy.max_attempts = max_attempts;
        }
        if let Some(initial_interval) = self.initial_interval {
            policy.initial_interval = initial_interval;
        }
        if let Some(max_interval) = self.max_interval {
            policy.max_interval = max_interval;
        }
        if self.initial_interval.is_some() || self.max_interval.is_some() {
            policy.max_interval = policy.max_interval.max(policy.initial_interval);
        }
        policy
    }
}

/// A partial change to stored [`RetryLimits`].
///
/// Each field is tri-state. The outer `None` keeps the stored value.
/// `Some(None)` returns the field to the code value. `Some(Some(v))` sets `v`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryLimitsPatch {
    /// The maximum number of attempts.
    pub max_attempts: Option<Option<u32>>,
    /// The delay before the first retry.
    pub initial_interval: Option<Option<Duration>>,
    /// The upper bound on the delay between retries.
    pub max_interval: Option<Option<Duration>>,
}

impl RetryLimitsPatch {
    /// Merge this patch over the stored limits.
    #[must_use]
    pub fn merge(&self, stored: RetryLimits) -> RetryLimits {
        RetryLimits {
            max_attempts: self.max_attempts.unwrap_or(stored.max_attempts),
            initial_interval: self.initial_interval.unwrap_or(stored.initial_interval),
            max_interval: self.max_interval.unwrap_or(stored.max_interval),
        }
    }

    /// Whether this patch names no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.max_attempts.is_none()
            && self.initial_interval.is_none()
            && self.max_interval.is_none()
    }

    /// Whether this patch sets a field. A `null` sets none.
    #[must_use]
    pub const fn sets_any(&self) -> bool {
        matches!(self.max_attempts, Some(Some(_)))
            || matches!(self.initial_interval, Some(Some(_)))
            || matches!(self.max_interval, Some(Some(_)))
    }
}

/// The JSON that a run stores for `policy` from `source`.
///
/// A schedule policy carries [`SOURCE_KEY`]. A type policy has no extra key,
/// so its JSON is the same as before issue #2409.
#[must_use]
pub fn encode_stored(policy: &RetryPolicy, source: RetryPolicySource) -> serde_json::Value {
    let mut value = serde_json::to_value(policy).unwrap_or(serde_json::Value::Null);
    if source == RetryPolicySource::Schedule
        && let Some(object) = value.as_object_mut()
    {
        object.insert(
            SOURCE_KEY.to_string(),
            serde_json::Value::from(SCHEDULE_SOURCE),
        );
    }
    value
}

/// The source that a stored retry policy names.
///
/// JSON with no [`SOURCE_KEY`] is a type policy. A run that an older binary
/// started has no key.
#[must_use]
pub fn stored_source(value: &serde_json::Value) -> RetryPolicySource {
    if value.get(SOURCE_KEY).and_then(serde_json::Value::as_str) == Some(SCHEDULE_SOURCE) {
        RetryPolicySource::Schedule
    } else {
        RetryPolicySource::Type
    }
}

/// Decode a stored retry policy and its source.
///
/// Returns `None` when `value` is not a retry policy.
#[must_use]
pub fn decode_stored(value: &serde_json::Value) -> Option<(RetryPolicy, RetryPolicySource)> {
    let policy = serde_json::from_value(value.clone()).ok()?;
    Some((policy, stored_source(value)))
}

/// Decode an optional stored retry policy and its source.
///
/// `None`, or a value that is not a retry policy, gives no policy, as a
/// plain decode did before issue #2409.
#[must_use]
pub fn decode_optional(
    value: Option<&serde_json::Value>,
) -> (Option<RetryPolicy>, RetryPolicySource) {
    match value.and_then(decode_stored) {
        Some((policy, source)) => (Some(policy), source),
        None => (None, RetryPolicySource::Type),
    }
}

/// The retry policy and its source for a run that a schedule starts.
///
/// `schedule` is the stored `retry_policy` of the schedule row. A schedule
/// policy wins. A value that does not decode falls back to `code`, as it did
/// before issue #2409.
/// `code` is the policy of the workflow type.
#[must_use]
pub fn schedule_retry_policy(
    schedule: Option<&serde_json::Value>,
    code: Option<&RetryPolicy>,
) -> (Option<RetryPolicy>, RetryPolicySource) {
    schedule
        .and_then(|value| serde_json::from_value::<RetryPolicy>(value.clone()).ok())
        .map_or_else(
            || (code.cloned(), RetryPolicySource::Type),
            |own| (Some(own), RetryPolicySource::Schedule),
        )
}

/// The JSON form of [`schedule_retry_policy`], for a start option that holds
/// JSON.
#[must_use]
pub fn schedule_retry_value(
    schedule: Option<&serde_json::Value>,
    code: Option<&RetryPolicy>,
) -> Option<serde_json::Value> {
    let (policy, source) = schedule_retry_policy(schedule, code);
    policy.map(|policy| encode_stored(&policy, source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::JitterPolicy;

    fn code() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            initial_interval: Duration::from_millis(500),
            backoff_coefficient: 2.0,
            max_interval: Duration::from_secs(30),
            non_retryable_errors: vec!["Fatal".to_string()],
            jitter: JitterPolicy::Equal,
        }
    }

    #[test]
    fn empty_limits_keep_the_policy() {
        let applied = RetryLimits::default().apply(code());
        assert_eq!(applied.max_attempts, 5);
        assert_eq!(applied.initial_interval, Duration::from_millis(500));
        assert_eq!(applied.max_interval, Duration::from_secs(30));
        assert!(RetryLimits::default().is_empty());
    }

    #[test]
    fn each_set_field_replaces_and_the_rest_inherit() {
        let limits = RetryLimits {
            max_attempts: Some(1),
            ..RetryLimits::default()
        };
        let applied = limits.apply(code());
        assert_eq!(applied.max_attempts, 1);
        assert_eq!(applied.initial_interval, Duration::from_millis(500));
        assert_eq!(applied.max_interval, Duration::from_secs(30));
        assert!((applied.backoff_coefficient - 2.0).abs() < f64::EPSILON);
        assert_eq!(applied.non_retryable_errors, vec!["Fatal".to_string()]);
        assert_eq!(applied.jitter, JitterPolicy::Equal);
        assert!(!limits.is_empty());
    }

    #[test]
    fn a_longer_first_delay_alone_raises_the_cap_to_it() {
        let limits = RetryLimits {
            initial_interval: Some(Duration::from_secs(120)),
            ..RetryLimits::default()
        };
        let applied = limits.apply(code());
        assert_eq!(applied.initial_interval, Duration::from_secs(120));
        assert_eq!(applied.max_interval, Duration::from_secs(120));
    }

    #[test]
    fn a_shorter_first_delay_alone_keeps_the_cap() {
        let limits = RetryLimits {
            initial_interval: Some(Duration::from_secs(1)),
            ..RetryLimits::default()
        };
        assert_eq!(limits.apply(code()).max_interval, Duration::from_secs(30));
    }

    #[test]
    fn a_set_cap_is_kept_as_set() {
        let limits = RetryLimits {
            initial_interval: Some(Duration::from_secs(10)),
            max_interval: Some(Duration::from_secs(60)),
            ..RetryLimits::default()
        };
        let applied = limits.apply(code());
        assert_eq!(applied.initial_interval, Duration::from_secs(10));
        assert_eq!(applied.max_interval, Duration::from_secs(60));
    }

    #[test]
    fn a_cap_alone_below_the_stored_first_delay_rises_to_it() {
        let limits = RetryLimits {
            max_interval: Some(Duration::from_millis(100)),
            ..RetryLimits::default()
        };
        let applied = limits.apply(code());
        assert_eq!(applied.initial_interval, Duration::from_millis(500));
        assert_eq!(applied.max_interval, Duration::from_millis(500));
    }

    #[test]
    fn attempts_alone_keep_a_code_cap_below_the_first_delay() {
        let mut odd = code();
        odd.max_interval = Duration::from_millis(100);
        let applied = RetryLimits {
            max_attempts: Some(2),
            ..RetryLimits::default()
        }
        .apply(odd);
        assert_eq!(applied.max_interval, Duration::from_millis(100));
    }

    #[test]
    fn of_sets_every_field() {
        assert_eq!(
            RetryLimits::of(&code()),
            RetryLimits {
                max_attempts: Some(5),
                initial_interval: Some(Duration::from_millis(500)),
                max_interval: Some(Duration::from_secs(30)),
            }
        );
    }

    #[test]
    fn patch_merge_sets_clears_and_keeps() {
        let stored = RetryLimits {
            max_attempts: Some(3),
            initial_interval: Some(Duration::from_secs(5)),
            max_interval: Some(Duration::from_secs(50)),
        };
        let patch = RetryLimitsPatch {
            max_attempts: Some(Some(1)),
            initial_interval: Some(None),
            max_interval: None,
        };
        assert_eq!(
            patch.merge(stored),
            RetryLimits {
                max_attempts: Some(1),
                initial_interval: None,
                max_interval: Some(Duration::from_secs(50)),
            }
        );
        assert!(!patch.is_empty());
        assert!(patch.sets_any());
    }

    #[test]
    fn a_null_only_patch_names_a_field_but_sets_none() {
        let patch = RetryLimitsPatch {
            max_attempts: Some(None),
            ..RetryLimitsPatch::default()
        };
        assert!(!patch.is_empty());
        assert!(!patch.sets_any());
        assert!(RetryLimitsPatch::default().is_empty());
    }

    #[test]
    fn a_type_policy_stores_the_plain_json() {
        let value = encode_stored(&code(), RetryPolicySource::Type);
        assert_eq!(value, serde_json::to_value(code()).unwrap());
        assert_eq!(stored_source(&value), RetryPolicySource::Type);
    }

    #[test]
    fn a_schedule_policy_round_trips_with_its_source() {
        let value = encode_stored(&code(), RetryPolicySource::Schedule);
        assert_eq!(value[SOURCE_KEY], SCHEDULE_SOURCE);
        let (policy, source) = decode_stored(&value).expect("decodes");
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(source, RetryPolicySource::Schedule);
    }

    #[test]
    fn an_older_binary_reads_a_marked_policy() {
        let value = encode_stored(&code(), RetryPolicySource::Schedule);
        let policy: RetryPolicy = serde_json::from_value(value).expect("unknown key ignored");
        assert_eq!(policy.max_attempts, 5);
    }

    #[test]
    fn an_unknown_source_value_is_a_type_policy() {
        let mut value = encode_stored(&code(), RetryPolicySource::Type);
        value[SOURCE_KEY] = serde_json::json!("other");
        assert_eq!(stored_source(&value), RetryPolicySource::Type);
    }

    #[test]
    fn decode_optional_keeps_the_source_and_drops_a_bad_value() {
        let marked = encode_stored(&code(), RetryPolicySource::Schedule);
        let (policy, source) = decode_optional(Some(&marked));
        assert_eq!(policy.expect("policy").max_attempts, 5);
        assert_eq!(source, RetryPolicySource::Schedule);
        let (policy, source) = decode_optional(Some(&serde_json::json!(1)));
        assert!(policy.is_none());
        assert_eq!(source, RetryPolicySource::Type);
        assert!(decode_optional(None).0.is_none());
    }

    #[test]
    fn a_value_that_is_not_a_policy_does_not_decode() {
        assert!(decode_stored(&serde_json::json!({"not": "a policy"})).is_none());
    }

    #[test]
    fn a_schedule_policy_wins_and_is_marked() {
        let own = RetryPolicy::fixed(2, Duration::from_secs(9));
        let value = serde_json::to_value(&own).unwrap();
        let (policy, source) = schedule_retry_policy(Some(&value), Some(&code()));
        assert_eq!(policy.expect("own policy").max_attempts, 2);
        assert_eq!(source, RetryPolicySource::Schedule);
        let stored = schedule_retry_value(Some(&value), Some(&code())).expect("value");
        assert_eq!(stored[SOURCE_KEY], SCHEDULE_SOURCE);
    }

    #[test]
    fn a_schedule_with_no_policy_takes_the_type_policy() {
        let (policy, source) = schedule_retry_policy(None, Some(&code()));
        assert_eq!(policy.expect("code policy").max_attempts, 5);
        assert_eq!(source, RetryPolicySource::Type);
        let stored = schedule_retry_value(None, Some(&code())).expect("value");
        assert!(stored.get(SOURCE_KEY).is_none());
    }

    #[test]
    fn a_schedule_policy_that_does_not_decode_takes_the_type_policy() {
        let bad = serde_json::json!({"not": "a policy"});
        let (policy, source) = schedule_retry_policy(Some(&bad), Some(&code()));
        assert_eq!(policy.expect("code policy").max_attempts, 5);
        assert_eq!(source, RetryPolicySource::Type);
    }

    #[test]
    fn no_schedule_and_no_code_policy_stores_none() {
        let (policy, source) = schedule_retry_policy(None, None);
        assert!(policy.is_none());
        assert_eq!(source, RetryPolicySource::Type);
        assert!(schedule_retry_value(None, None).is_none());
    }
}
