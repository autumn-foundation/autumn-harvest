//! The depth cap on payloads that Harvest stores (issues #2157, #2216, #2225).
//!
//! `serde_json` reads at most 128 levels. Postgres `jsonb` and `SQLite` store more.
//! A stored value nested past the read limit makes every later history load fail.
//! The workflow cannot handle that failure, because it happens in the driver.
//!
//! This module holds the one cap, the walk that checks it, and the failure that
//! replaces a result nested past it. Both backends use it.

use serde_json::Value;

/// The deepest nesting that Harvest accepts in a stored payload.
///
/// `serde_json` reads at most 128 levels. Event storage wraps a payload in a
/// few more levels. The cap leaves room for that wrapping.
pub const MAX_PAYLOAD_DEPTH: usize = 100;

/// The `error_type` of the activity failure that replaces a result nested past
/// [`MAX_PAYLOAD_DEPTH`]. The failure is non-retryable.
pub const PAYLOAD_TOO_DEEP_ERROR_TYPE: &str = "PayloadTooDeep";

/// Returns `true` when `value` has more than [`MAX_PAYLOAD_DEPTH`] open arrays
/// and objects on a path.
#[must_use]
pub fn nests_past_cap(_value: &Value) -> bool {
    false
}

/// The non-retryable failure payload for a result nested past the cap.
///
/// `subject` names the producer, for example `activity 'charge'`.
#[must_use]
pub fn too_deep_failure_payload(_subject: &str) -> String {
    String::new()
}

/// Turn a successful activity result nested past the cap into a failure.
///
/// Other results pass unchanged.
#[must_use]
pub fn refuse_deep_result(
    _activity_name: &str,
    result: Result<Value, String>,
) -> Result<Value, String> {
    result
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::{Value, json};

    use super::{
        MAX_PAYLOAD_DEPTH, PAYLOAD_TOO_DEEP_ERROR_TYPE, nests_past_cap, refuse_deep_result,
        too_deep_failure_payload,
    };
    use crate::failure::parse_error_payload_full;

    /// A value of `depth` nested one-element arrays around the number 1.
    fn nested_arrays(depth: usize) -> Value {
        let mut value = json!(1);
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    /// A value of `depth` nested one-key objects around the number 1.
    fn nested_objects(depth: usize) -> Value {
        let mut value = json!(1);
        for _ in 0..depth {
            value = json!({ "k": value });
        }
        value
    }

    #[test]
    fn the_cap_is_below_the_serde_json_read_limit() {
        // Storage wraps a payload in a few levels, and serde_json reads 128.
        assert!(MAX_PAYLOAD_DEPTH + 8 <= 128);
    }

    #[test]
    fn a_scalar_has_no_depth() {
        for scalar in [json!(null), json!(true), json!(1), json!("text")] {
            assert!(!nests_past_cap(&scalar));
        }
    }

    #[test]
    fn arrays_at_the_cap_pass_and_one_level_over_fails() {
        assert!(!nests_past_cap(&nested_arrays(MAX_PAYLOAD_DEPTH)));
        assert!(nests_past_cap(&nested_arrays(MAX_PAYLOAD_DEPTH + 1)));
    }

    #[test]
    fn objects_at_the_cap_pass_and_one_level_over_fails() {
        assert!(!nests_past_cap(&nested_objects(MAX_PAYLOAD_DEPTH)));
        assert!(nests_past_cap(&nested_objects(MAX_PAYLOAD_DEPTH + 1)));
    }

    #[test]
    fn the_issue_depth_fails() {
        // Issue #2225: a value nested 126 levels made the history unreadable.
        assert!(nests_past_cap(&nested_arrays(126)));
    }

    #[test]
    fn mixed_containers_count_each_level() {
        let mut value = json!(1);
        for level in 0..=MAX_PAYLOAD_DEPTH {
            value = if level % 2 == 0 {
                json!([value])
            } else {
                json!({ "k": value })
            };
        }
        assert!(nests_past_cap(&value));
    }

    #[test]
    fn a_deep_branch_after_a_shallow_one_fails() {
        let value = json!([nested_arrays(2), nested_arrays(MAX_PAYLOAD_DEPTH + 1)]);
        assert!(nests_past_cap(&value));
    }

    #[test]
    fn a_wide_shallow_value_passes() {
        let wide = Value::Array((0..50_000).map(|n| json!([n])).collect());
        assert!(!nests_past_cap(&wide));
    }

    #[test]
    fn the_walk_does_not_overflow_the_stack_on_deep_input() {
        // The walk must not recurse over the input that it refuses.
        assert!(nests_past_cap(&nested_arrays(1_000)));
    }

    #[test]
    fn the_failure_is_non_retryable_and_typed() {
        let payload = too_deep_failure_payload("activity 'act'");
        let failure = parse_error_payload_full(&payload);
        assert_eq!(failure.error_type, PAYLOAD_TOO_DEEP_ERROR_TYPE);
        assert!(failure.non_retryable);
        assert_eq!(
            failure.message,
            "activity 'act' result nests more than 100 levels"
        );
    }

    #[test]
    fn a_deep_result_becomes_a_failure() {
        let refused = refuse_deep_result("act", Ok(nested_arrays(MAX_PAYLOAD_DEPTH + 1)));
        let payload = refused.expect_err("a deep result must fail");
        let failure = parse_error_payload_full(&payload);
        assert_eq!(failure.error_type, PAYLOAD_TOO_DEEP_ERROR_TYPE);
        assert_eq!(
            failure.message,
            "activity 'act' result nests more than 100 levels"
        );
    }

    #[test]
    fn a_result_at_the_cap_passes_unchanged() {
        let capped = nested_arrays(MAX_PAYLOAD_DEPTH);
        assert_eq!(refuse_deep_result("act", Ok(capped.clone())), Ok(capped));
    }

    #[test]
    fn an_error_result_passes_unchanged() {
        let failed = Err("boom".to_string());
        assert_eq!(refuse_deep_result("act", failed.clone()), failed);
    }

    /// The depth of `value`, found by recursion. The oracle for the walk.
    fn depth_by_recursion(value: &Value) -> usize {
        match value {
            Value::Array(items) => 1 + items.iter().map(depth_by_recursion).max().unwrap_or(0),
            Value::Object(map) => 1 + map.values().map(depth_by_recursion).max().unwrap_or(0),
            _ => 0,
        }
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            "[a-z]{0,4}".prop_map(Value::String),
        ];
        leaf.prop_recursive(130, 600, 3, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..3).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{1,2}", inner), 0..3)
                    .prop_map(|pairs| { Value::Object(pairs.into_iter().collect()) }),
            ]
        })
    }

    proptest! {
        #[test]
        fn the_walk_agrees_with_the_recursive_oracle(value in arb_value()) {
            prop_assert_eq!(
                nests_past_cap(&value),
                depth_by_recursion(&value) > MAX_PAYLOAD_DEPTH
            );
        }

        #[test]
        fn a_chain_fails_exactly_past_the_cap(depth in 0usize..160) {
            prop_assert_eq!(
                nests_past_cap(&nested_arrays(depth)),
                depth > MAX_PAYLOAD_DEPTH
            );
        }
    }
}
