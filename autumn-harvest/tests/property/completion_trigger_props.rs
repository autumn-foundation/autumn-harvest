//! Property tests for [`autumn_harvest::completion_trigger`] condition
//! evaluation (Tier B target 11).
//!
//! Contract under test:
//! - `TriggerCondition::evaluate` is **total** (never panics) for a bounded
//!   condition against an arbitrary JSON value;
//! - `gate_stored_condition` is **total**, including on an arbitrary/corrupt
//!   stored `Value` (which must resolve to `Invalid`, never a panic);
//! - for a valid, round-trippable condition, `gate_stored_condition(Some(json),
//!   v)` agrees with `evaluate`: `Pass` iff `evaluate` is `true`, else `Unmet`
//!   (this also exercises the `TriggerCondition` serde round-trip);
//! - `gate_stored_condition(None, _)` is always `Pass` (legacy unconditional).

use autumn_harvest::completion_trigger::{ConditionGate, TriggerCondition, gate_stored_condition};
use proptest::prelude::*;
use serde_json::Value;

use super::prop_config::config;

/// Bounded, finite JSON leaf values.
fn json_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        (-1.0e6f64..1.0e6).prop_map(Value::from),
        "[a-z0-9]{0,8}".prop_map(Value::from),
    ]
}

/// Bounded recursive JSON value (depth <= 3): the evaluation target.
fn json_value() -> impl Strategy<Value = Value> {
    json_leaf().prop_recursive(3, 16, 5, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            proptest::collection::hash_map("[a-c]", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

/// Dotted path with no empty segments (or the whole-output empty path).
fn path() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[a-c](\\.[a-c]){0,2}"]
}

/// Bounded recursive `TriggerCondition` (well within the depth/node caps so
/// `validate()` is `Ok`).
fn condition() -> impl Strategy<Value = TriggerCondition> {
    let leaf = prop_oneof![
        (path(), json_value()).prop_map(|(p, v)| TriggerCondition::Eq { path: p, value: v }),
        (path(), json_value()).prop_map(|(p, v)| TriggerCondition::NotEq { path: p, value: v }),
        (path(), json_value())
            .prop_map(|(p, v)| TriggerCondition::GreaterThan { path: p, value: v }),
        (path(), json_value())
            .prop_map(|(p, v)| TriggerCondition::GreaterThanOrEq { path: p, value: v }),
        (path(), json_value()).prop_map(|(p, v)| TriggerCondition::LessThan { path: p, value: v }),
        (path(), json_value())
            .prop_map(|(p, v)| TriggerCondition::LessThanOrEq { path: p, value: v }),
        (path(), proptest::collection::vec(json_value(), 0..8))
            .prop_map(|(p, values)| TriggerCondition::In { path: p, values }),
        path().prop_map(|p| TriggerCondition::Exists { path: p }),
        path().prop_map(|p| TriggerCondition::IsNull { path: p }),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(TriggerCondition::All),
            proptest::collection::vec(inner.clone(), 0..4).prop_map(TriggerCondition::Any),
            inner.prop_map(|c| TriggerCondition::Not(Box::new(c))),
        ]
    })
}

proptest! {
    #![proptest_config(config())]

    /// `evaluate` never panics for a bounded condition and arbitrary value.
    #[test]
    fn evaluate_is_total(cond in condition(), value in json_value()) {
        let _ = cond.evaluate(&value);
    }

    /// For a valid condition, the stored-gate agrees with direct evaluation, and
    /// never returns `Invalid` (it round-trips through JSON cleanly).
    #[test]
    fn gate_agrees_with_evaluate_for_valid_conditions(
        cond in condition(),
        value in json_value(),
    ) {
        // The generator stays within the caps, but guard defensively so the
        // property is honest even if a future generator tweak drifts over.
        prop_assume!(cond.validate().is_ok());

        let json = serde_json::to_value(&cond).unwrap();
        let gate = gate_stored_condition(Some(&json), &value);
        let expected = if cond.evaluate(&value) {
            ConditionGate::Pass
        } else {
            ConditionGate::Unmet
        };
        prop_assert_eq!(gate, expected, "gate disagreed with evaluate for {:?}", cond);
    }

    /// `gate_stored_condition` is total on an arbitrary/corrupt stored value —
    /// it never panics; an undeserializable guard resolves to `Invalid`.
    #[test]
    fn gate_is_total_on_arbitrary_stored(stored in json_value(), value in json_value()) {
        let _ = gate_stored_condition(Some(&stored), &value);
    }

    /// No stored condition is always `Pass` (byte-identical legacy behaviour).
    #[test]
    fn gate_none_is_always_pass(value in json_value()) {
        prop_assert_eq!(gate_stored_condition(None, &value), ConditionGate::Pass);
    }
}
