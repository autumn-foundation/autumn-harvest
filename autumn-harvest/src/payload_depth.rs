//! Depth cap for workflow-side payloads (issue #2224).
//!
//! `serde_json` writes any depth but refuses to read past 128 levels. A
//! stored value that deep makes every later drive of the run fail. The
//! context refuses such a value at the call site, as it does for the size
//! caps. The workflow sees a typed error and can handle it.
//!
//! The walk does not recurse. Dropping or serializing a value thousands of
//! levels deep still recurses in `serde_json`. So the cap is sound for values
//! that a workflow builds by hand. It is not sound for a hostile value.

use serde_json::Value;

/// The deepest nesting that a payload may have.
///
/// Event storage wraps a payload in a few more levels. The cap leaves room
/// for that wrapping.
pub const MAX_PAYLOAD_DEPTH: usize = 100;

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

/// Returns `true` when `value` has more than [`MAX_PAYLOAD_DEPTH`] open
/// arrays and objects on a path.
///
/// The walk keeps one iterator per open container. A recursive walk would
/// overflow the call stack on the input it must refuse. Memory use is bound
/// by the cap, not by the width of the value.
///
/// Call it before any code that recurses over the value, such as a schema
/// check or a settlement decoder.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn nested(depth: usize) -> Value {
        let mut value = json!(1);
        for _ in 0..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    /// Drop a deep value without recursion.
    fn tear_down(mut value: Value) {
        while let Value::Array(mut items) = value {
            value = items.pop().unwrap_or(Value::Null);
        }
    }

    #[test]
    fn a_value_at_the_cap_passes() {
        assert!(!nests_past_cap(&nested(MAX_PAYLOAD_DEPTH)));
    }

    #[test]
    fn a_value_one_level_over_the_cap_fails() {
        assert!(nests_past_cap(&nested(MAX_PAYLOAD_DEPTH + 1)));
    }

    #[test]
    fn a_scalar_passes() {
        assert!(!nests_past_cap(&json!(1)));
    }

    #[test]
    fn a_huge_value_is_walked_without_recursion() {
        let deep = nested(100_000);
        assert!(nests_past_cap(&deep));
        tear_down(deep);
    }
}
