//! The depth cap on payloads that Harvest keeps (issues #2157, #2216, #2225).
//!
//! `serde_json` reads at most 128 levels. Postgres `jsonb` and `SQLite` store any
//! depth. A stored value nested past the read limit makes every later load of
//! the run history fail. The workflow cannot handle that failure, because the
//! load fails before workflow code runs.
//!
//! This module holds the one cap, the walk that checks it, and the failure that
//! replaces an activity result nested past it. The Postgres backend applies the
//! cap to activity results, external completions and heartbeat checkpoints. The
//! `SQLite` backend applies it to those and to ingress payloads.

use serde_json::Value;

/// The deepest nesting that Harvest keeps in an activity result or a heartbeat
/// checkpoint. The `SQLite` backend also applies it to ingress payloads.
///
/// `serde_json` reads at most 128 levels. Event storage wraps a payload in a
/// few more levels. The cap leaves room for that wrapping.
pub const MAX_PAYLOAD_DEPTH: usize = 100;

/// The `error_type` of the activity failure that replaces a result nested past
/// [`MAX_PAYLOAD_DEPTH`]. The failure is non-retryable.
///
/// A local activity failure keeps only its message, as every local failure
/// does. So the workflow sees the error type `Error` for a local activity.
pub const PAYLOAD_TOO_DEEP_ERROR_TYPE: &str = "PayloadTooDeep";

/// The children of a JSON container, as one iterator type.
enum Children<'a> {
    Array(std::slice::Iter<'a, Value>),
    Object(serde_json::map::Values<'a>),
}

impl<'a> Iterator for Children<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Array(items) => items.next(),
            Self::Object(values) => values.next(),
        }
    }
}

/// The children of `value`, or `None` for a scalar.
fn children(value: &Value) -> Option<Children<'_>> {
    match value {
        Value::Array(items) => Some(Children::Array(items.iter())),
        Value::Object(map) => Some(Children::Object(map.values())),
        _ => None,
    }
}

/// Returns `true` when `value` nests more than [`MAX_PAYLOAD_DEPTH`] levels of
/// arrays and objects on a path.
///
/// The walk keeps one iterator per level on the heap. A recursive walk would
/// use call stack in proportion to the depth of the input that it must refuse.
/// The cap bounds the memory of this walk, not the width of the value.
///
/// Call it before any code that recurses over the value, such as a schema
/// check.
#[must_use]
pub fn nests_past_cap(value: &Value) -> bool {
    let mut open: Vec<Children<'_>> = children(value).into_iter().collect();
    while let Some(top) = open.last_mut() {
        let Some(child) = top.next() else {
            open.pop();
            continue;
        };
        if let Some(inner) = children(child) {
            if open.len() >= MAX_PAYLOAD_DEPTH {
                return true;
            }
            open.push(inner);
        }
    }
    false
}

/// The message of a failure for a result nested past the cap.
///
/// `subject` names the producer, for example `external activity`.
pub(crate) fn too_deep_message(subject: &str) -> String {
    format!("{subject} result nests more than {MAX_PAYLOAD_DEPTH} levels")
}

/// The non-retryable failure payload for a result nested past the cap.
///
/// `subject` names the producer, for example `activity 'charge'`.
pub(crate) fn too_deep_failure_payload(subject: &str) -> String {
    use crate::failure::IntoActivityErrorString as _;
    crate::failure::ActivityFailure::non_retryable(
        PAYLOAD_TOO_DEEP_ERROR_TYPE,
        too_deep_message(subject),
    )
    .into_error_payload()
}

/// Turn a successful activity result nested past the cap into a non-retryable
/// [`PAYLOAD_TOO_DEEP_ERROR_TYPE`] failure.
///
/// The workflow sees the failure in place of the result. Other results pass
/// unchanged. The `SQLite` backend calls this function. It is not a stable API.
#[doc(hidden)]
pub fn refuse_deep_result(
    activity_name: &str,
    result: Result<Value, String>,
) -> Result<Value, String> {
    match result {
        Ok(output) if nests_past_cap(&output) => Err(too_deep_failure_payload(&format!(
            "activity '{activity_name}'"
        ))),
        other => other,
    }
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

    // Storage wraps a payload in a few levels, and serde_json reads 128.
    const _: () = assert!(MAX_PAYLOAD_DEPTH + 8 <= 128);

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
    fn the_walk_does_not_recurse_over_deep_input() {
        // A recursive walk needs far more than 256 KiB of stack for 100,000
        // levels. Dropping a `Value` recurses, so the test leaks the value.
        let handle = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let deep = nested_arrays(100_000);
                let past_cap = nests_past_cap(&deep);
                std::mem::forget(deep);
                past_cap
            })
            .expect("spawn the small-stack thread");
        assert!(handle.join().expect("the walk must not overflow the stack"));
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

    /// Random shallow JSON. Its depth stays far below the cap.
    fn arb_shallow() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| json!(n)),
            "[a-z]{0,4}".prop_map(Value::String),
        ];
        leaf.prop_recursive(4, 40, 3, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..3).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{1,2}", inner), 0..3)
                    .prop_map(|pairs| { Value::Object(pairs.into_iter().collect()) }),
            ]
        })
    }

    /// A shallow value wrapped in 85 to 115 containers, each with shallow
    /// siblings. The depth straddles the cap, so both outcomes occur.
    fn arb_wrapped() -> impl Strategy<Value = Value> {
        (
            arb_shallow(),
            85usize..=115,
            prop::collection::vec(arb_shallow(), 0..3),
            any::<bool>(),
        )
            .prop_map(|(inner, layers, siblings, use_objects)| {
                let mut value = inner;
                for layer in 0..layers {
                    value = if use_objects && layer % 2 == 0 {
                        json!({ "deep": value, "side": siblings.clone() })
                    } else {
                        let mut items = siblings.clone();
                        items.insert(layer % (items.len() + 1), value);
                        Value::Array(items)
                    };
                }
                value
            })
    }

    proptest! {
        #[test]
        fn the_walk_agrees_with_the_recursive_oracle_on_shallow_values(value in arb_shallow()) {
            prop_assert_eq!(
                nests_past_cap(&value),
                depth_by_recursion(&value) > MAX_PAYLOAD_DEPTH
            );
        }

        #[test]
        fn the_walk_agrees_with_the_recursive_oracle_across_the_cap(value in arb_wrapped()) {
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

    #[test]
    fn the_wrapped_generator_reaches_both_sides_of_the_cap() {
        // The property above is empty if the generator stays on one side.
        use proptest::strategy::{Strategy, ValueTree};
        use proptest::test_runner::TestRunner;
        let mut runner = TestRunner::deterministic();
        let (mut over, mut under) = (0, 0);
        for _ in 0..200 {
            let value = arb_wrapped().new_tree(&mut runner).expect("tree").current();
            if depth_by_recursion(&value) > MAX_PAYLOAD_DEPTH {
                over += 1;
            } else {
                under += 1;
            }
        }
        assert!(over >= 40 && under >= 40, "over {over}, under {under}");
    }
}
