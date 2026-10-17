//! Guards that the corpus covers every primitive (issue #2030). No Docker.
//!
//! - Each command kind that `SQLite` accepts has a scenario that compares it
//!   with Postgres.
//! - Each command kind that `SQLite` rejects has a rejection scenario.
//! - Each named rejection in the `SQLite` source has a rejection scenario, or
//!   a reason in [`UNREACHABLE_REJECTIONS`].

use std::collections::BTreeSet;

use crate::commands::{ALL, CommandKind, Support, support};
use crate::scenario::Expect;

/// The `SQLite` runtime source. Its rejection messages name each feature.
const SQLITE_RUNTIME: &str = include_str!("../../../autumn-harvest-sqlite/src/runtime.rs");

/// The named rejections that no `WorkflowContext` call can reach.
///
/// Each entry gives the reason. A test fails when an entry is no longer in
/// the source, so the list cannot go stale.
const UNREACHABLE_REJECTIONS: &[(&str, &str)] = &[
    (
        "ScheduleActivity.session_id",
        "A session activity needs a granted session. The acquire is rejected first.",
    ),
    (
        "ScheduleActivity.result_writer",
        "The context needs a PayloadStore first. Neither runtime has one here. \
         `fan_out_result_writer_needs_a_store` shows the same failure on both.",
    ),
];

/// The feature names that `SQLite` rejects by name in its runtime source.
///
/// Each message starts with `"<Feature> — `. The feature is a command name,
/// optionally with a field.
fn named_rejections() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in SQLITE_RUNTIME.lines() {
        let Some(open) = line.find('"') else { continue };
        let rest = &line[open + 1..];
        let Some(dash) = rest.find(" \u{2014}") else {
            continue;
        };
        let name = &rest[..dash];
        let valid = !name.is_empty()
            && name.chars().next().is_some_and(char::is_uppercase)
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_');
        if valid {
            out.insert(name.to_owned());
        }
    }
    out
}

fn rejected_features() -> BTreeSet<&'static str> {
    crate::scenarios()
        .into_iter()
        .filter_map(|(_, s)| match s.expect {
            Expect::Rejected { feature } => Some(feature),
            Expect::Same => None,
        })
        .collect()
}

#[test]
fn every_scenario_is_named_after_its_constructor() {
    for (constructor, scenario) in crate::scenarios() {
        assert_eq!(
            constructor, scenario.name,
            "a failure message must name the test that failed"
        );
    }
}

#[test]
fn every_accepted_command_has_a_conformance_scenario() {
    let covered: BTreeSet<CommandKind> = crate::scenarios()
        .into_iter()
        .filter(|(_, s)| matches!(s.expect, Expect::Same))
        .flat_map(|(_, s)| s.covers.iter().copied())
        .collect();
    let missing: Vec<CommandKind> = ALL
        .iter()
        .copied()
        .filter(|k| matches!(support(*k), Support::Accepted { .. }) && !covered.contains(k))
        .collect();
    assert!(
        missing.is_empty(),
        "SQLite accepts these commands, but no scenario compares them with Postgres: {missing:?}"
    );
}

#[test]
fn every_rejected_command_has_a_rejection_scenario() {
    let rejected = rejected_features();
    let missing: Vec<CommandKind> = ALL
        .iter()
        .copied()
        .filter(|k| {
            matches!(support(*k), Support::Rejected)
                && !rejected.contains(format!("{k:?}").as_str())
        })
        .collect();
    assert!(
        missing.is_empty(),
        "SQLite rejects these commands, but no scenario shows the rejection: {missing:?}"
    );
}

#[test]
fn every_named_rejection_has_a_scenario_or_a_reason() {
    let named = named_rejections();
    assert!(
        named.len() >= 5,
        "the scan found too few rejections, so the message format changed: {named:?}"
    );
    let rejected = rejected_features();
    let missing: Vec<&String> = named
        .iter()
        .filter(|n| {
            !rejected.contains(n.as_str())
                && !UNREACHABLE_REJECTIONS.iter().any(|(u, _)| u == n)
        })
        .collect();
    assert!(
        missing.is_empty(),
        "SQLite rejects these features by name, but no scenario shows it: {missing:?}"
    );
    for (feature, _) in UNREACHABLE_REJECTIONS {
        assert!(
            named.contains(*feature),
            "`{feature}` is no longer a named rejection. Remove its entry."
        );
    }
}

#[test]
fn every_scenario_name_is_unique() {
    let names: Vec<&str> = crate::scenarios().iter().map(|(n, _)| *n).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(names.len(), unique.len(), "two scenarios share a name");
}
