//! The parts that differ between two overlays (issue #2374).
//!
//! [`diff`] walks the JSON of both overlays. It goes down into each object
//! that both sides hold. So a change to one tool guidance entry shows only
//! that entry. An unset field is absent from the JSON, so it shows as
//! `null`. An empty map is unset too, so a map that only one side holds
//! shows as one change.

use serde::Serialize;
use serde_json::{Map, Value};

use super::overlay::ProfileOverlay;

/// One part that differs between two overlays.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OverlayChange {
    /// The keys from the root of the overlay JSON to the part, such as
    /// `["tool_guidance", "lookup"]`.
    pub path: Vec<String>,
    /// The value in the first overlay, or `null` when it is unset.
    pub a: Value,
    /// The value in the second overlay, or `null` when it is unset.
    pub b: Value,
}

/// Each part that differs between `a` and `b`, in key order.
///
/// Two equal overlays give no change.
#[must_use]
pub fn diff(a: &ProfileOverlay, b: &ProfileOverlay) -> Vec<OverlayChange> {
    let mut changes = Vec::new();
    walk(&mut Vec::new(), &json(a), &json(b), &mut changes);
    changes
}

/// The JSON object of an overlay. An overlay holds strings, numbers,
/// booleans and maps only, so it always encodes.
fn json(overlay: &ProfileOverlay) -> Map<String, Value> {
    match serde_json::to_value(overlay) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    }
}

/// Add the changes between the objects `a` and `b` under `path`.
fn walk(
    path: &mut Vec<String>,
    a: &Map<String, Value>,
    b: &Map<String, Value>,
    changes: &mut Vec<OverlayChange>,
) {
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();
    for key in keys {
        let (left, right) = (a.get(key), b.get(key));
        if left == right {
            continue;
        }
        path.push(key.clone());
        if let (Some(Value::Object(left)), Some(Value::Object(right))) = (left, right) {
            walk(path, left, right, changes);
        } else {
            changes.push(OverlayChange {
                path: path.clone(),
                a: left.cloned().unwrap_or(Value::Null),
                b: right.cloned().unwrap_or(Value::Null),
            });
        }
        path.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skill::SkillText;
    use proptest::prelude::*;
    use serde_json::json;

    fn path(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|key| (*key).to_owned()).collect()
    }

    fn base() -> ProfileOverlay {
        ProfileOverlay {
            system: Some("One.".to_owned()),
            max_steps: Some(4),
            tool_guidance: [("lookup".to_owned(), "Find.".to_owned())].into(),
            ..ProfileOverlay::default()
        }
    }

    #[test]
    fn a_change_to_system_only_shows_only_system() {
        let b = ProfileOverlay {
            system: Some("Two.".to_owned()),
            ..base()
        };
        assert_eq!(
            diff(&base(), &b),
            [OverlayChange {
                path: path(&["system"]),
                a: json!("One."),
                b: json!("Two."),
            }]
        );
    }

    #[test]
    fn equal_overlays_have_no_change() {
        assert_eq!(diff(&base(), &base()), []);
        assert_eq!(
            diff(&ProfileOverlay::default(), &ProfileOverlay::default()),
            []
        );
    }

    #[test]
    fn an_unset_side_is_null() {
        let b = ProfileOverlay {
            guardrails: Some("Never share keys.".to_owned()),
            max_steps: None,
            ..base()
        };
        assert_eq!(
            diff(&base(), &b),
            [
                OverlayChange {
                    path: path(&["guardrails"]),
                    a: Value::Null,
                    b: json!("Never share keys."),
                },
                OverlayChange {
                    path: path(&["max_steps"]),
                    a: json!(4),
                    b: Value::Null,
                },
            ]
        );
    }

    #[test]
    fn a_map_change_shows_only_the_entry() {
        let mut b = base();
        b.tool_guidance
            .insert("lookup".to_owned(), "Find fast.".to_owned());
        b.tool_guidance.insert("a".to_owned(), "New.".to_owned());
        b.skills
            .insert("refunds".to_owned(), SkillText::new("When.", "How."));
        let mut c = b.clone();
        c.skills
            .insert("refunds".to_owned(), SkillText::new("When.", "How now."));
        assert_eq!(
            diff(&base(), &b),
            [
                OverlayChange {
                    path: path(&["skills"]),
                    a: Value::Null,
                    b: json!({"refunds": {"summary": "When.", "body": "How."}}),
                },
                OverlayChange {
                    path: path(&["tool_guidance", "a"]),
                    a: Value::Null,
                    b: json!("New."),
                },
                OverlayChange {
                    path: path(&["tool_guidance", "lookup"]),
                    a: json!("Find."),
                    b: json!("Find fast."),
                },
            ]
        );
        assert_eq!(
            diff(&b, &c),
            [OverlayChange {
                path: path(&["skills", "refunds", "body"]),
                a: json!("How."),
                b: json!("How now."),
            }]
        );
    }

    fn overlay() -> impl Strategy<Value = ProfileOverlay> {
        let text = || proptest::option::of("[a-c]{0,2}");
        let bound = || proptest::option::of(0_u32..3);
        let skill = ("[a-c]{0,1}", "[a-c]{0,1}").prop_map(|(s, b)| SkillText::new(s, b));
        (
            (text(), text(), text(), proptest::option::of(any::<bool>())),
            (bound(), bound(), bound()),
            proptest::collection::btree_map("[a-c]", "[a-c]{0,2}", 0..3),
            proptest::collection::btree_map("[a-c]", skill, 0..3),
        )
            .prop_map(
                |(
                    (system, guardrails, model, read_only),
                    (max_steps, max_total_tokens, max_output_tokens),
                    tool_guidance,
                    skills,
                )| ProfileOverlay {
                    system,
                    guardrails,
                    model,
                    max_steps,
                    max_total_tokens,
                    max_output_tokens,
                    read_only,
                    tool_guidance,
                    skills,
                },
            )
    }

    proptest! {
        /// A diff is empty only for equal overlays. Swapping the sides swaps
        /// each value and keeps each path.
        #[test]
        fn a_diff_is_exact_and_symmetric(a in overlay(), b in overlay()) {
            let forward = diff(&a, &b);
            prop_assert_eq!(forward.is_empty(), a == b);
            let back: Vec<OverlayChange> = diff(&b, &a)
                .into_iter()
                .map(|change| OverlayChange { path: change.path, a: change.b, b: change.a })
                .collect();
            prop_assert_eq!(&forward, &back);
            for change in &forward {
                prop_assert_ne!(&change.a, &change.b);
            }
        }
    }
}
