//! Workflow entry-point discovery.
//!
//! Every `__autumn_workflow_info_X` companion fn the `#[workflow]` macro emits
//! marks `X` (same module) as a workflow; its analyzable body is `X` (sync) or
//! `X::{closure#0}` (async).
//!
//! Discovery reads **two** places, and the second one is the interesting one.
//! A companion whose *header* the MIR parser could not read never reaches
//! [`MirDoc::bodies`] at all, so a pass that only walked the parsed bodies would
//! answer "this target has no workflows" for a target whose markers are merely
//! unreadable — `analyzed 0`, exit `0`, even under `--strict`. That is the one
//! answer a verifier must never give. So [`MirDoc::parse_failures`] is scanned
//! for the marker too, and the entry is synthesized from the failed header's
//! text. Its body may itself be missing or unparsed; the analysis then attaches
//! a `missing-body` or `mir-parse` boundary and the verdict is `unknown` — the
//! workflow is *reported as not analyzed*, never silently absent.

use std::collections::BTreeSet;

use crate::mir::MirDoc;
use crate::util::split_last;

/// Prefix of the companion fn the `#[workflow]` macro emits next to every
/// workflow. `__autumn_activity_info_*` is deliberately *not* matched.
const MARKER: &str = "__autumn_workflow_info_";

/// A discovered workflow entry point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub crate_name: String,
    /// `module::name` path of the workflow fn.
    pub workflow: String,
    /// Path of the MIR body to analyze.
    pub body: String,
}

/// Discover entries across the given docs.
#[must_use]
pub fn discover(docs: &[MirDoc]) -> Vec<Entry> {
    let mut entries = Vec::new();
    for doc in docs {
        let paths: BTreeSet<&str> = doc.bodies.iter().map(|b| b.path.as_str()).collect();
        for body in &doc.bodies {
            if let Some(workflow) = workflow_of_marker_path(&body.path) {
                entries.push(entry_for(doc, &paths, workflow));
            }
        }
        // The unparsed half: a marker header the parser rejected still names its
        // workflow, and the name is all discovery needs.
        for failure in &doc.parse_failures {
            if let Some(workflow) = workflow_in_failed_item(&failure.item) {
                entries.push(entry_for(doc, &paths, workflow));
            }
        }
    }
    entries.sort_by(|a, b| {
        (&a.crate_name, &a.workflow, &a.body).cmp(&(&b.crate_name, &b.workflow, &b.body))
    });
    entries.dedup();
    entries
}

/// The entry for `workflow`, pointing at the async coroutine body when the dump
/// has one and at the fn's own body otherwise (absent bodies are the analysis's
/// problem, and it reports them as boundaries).
fn entry_for(doc: &MirDoc, paths: &BTreeSet<&str>, workflow: String) -> Entry {
    let coroutine = format!("{workflow}::{{closure#0}}");
    let body = if paths.contains(coroutine.as_str()) {
        coroutine
    } else {
        workflow.clone()
    };
    Entry {
        crate_name: doc.crate_name.clone(),
        workflow,
        body,
    }
}

/// `m::__autumn_workflow_info_wf` → `m::wf`, for a body path that parsed.
fn workflow_of_marker_path(path: &str) -> Option<String> {
    let (prefix, last) = split_last(path).unwrap_or(("", path));
    let name = last.strip_prefix(MARKER)?;
    if name.is_empty() {
        return None;
    }
    Some(join_path(prefix, name))
}

/// The workflow named by an item that failed to parse.
///
/// The recorded item is the offending header verbatim (truncated), not a path:
/// `__autumn_workflow_info_wf(_1: u8 -> u8`, or `fn m::__autumn_workflow_info_wf()`
/// for a truncated dump. So the marker is located inside the text, the name is
/// read forward from it and the module prefix backward — and anything that does
/// not look like a path is dropped rather than guessed at, leaving the workflow
/// unqualified (which still yields an entry, and so still yields a verdict).
fn workflow_in_failed_item(item: &str) -> Option<String> {
    let at = item.find(MARKER)?;
    let rest = item.get(at.checked_add(MARKER.len())?..)?;
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let name = rest.get(..end)?;
    if name.is_empty() {
        return None;
    }
    // `__autumn_workflow_info_w::{closure#0}` is a *nested* item of the
    // companion, not the companion: the same rule the parsed half applies by
    // matching only the last path segment.
    if rest.get(end..).is_some_and(|tail| tail.starts_with("::")) {
        return None;
    }
    Some(join_path(module_prefix(item.get(..at)?), name))
}

/// The `module::` prefix that immediately precedes the marker, or `""` when the
/// text before it is not a path (a garbled header may hold anything at all).
fn module_prefix(before: &str) -> &str {
    let candidate = before
        .rsplit(|c: char| c.is_whitespace())
        .next()
        .unwrap_or_default();
    let path = candidate.strip_suffix("::").unwrap_or("");
    if !path.is_empty()
        && path
            .split("::")
            .all(|seg| !seg.is_empty() && seg.chars().all(|c| c.is_alphanumeric() || c == '_'))
    {
        path
    } else {
        ""
    }
}

/// `("m", "wf")` → `"m::wf"`; an empty prefix yields the bare name.
fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix}::{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mir;

    fn doc(crate_name: &str, text: &str) -> MirDoc {
        mir::parse(crate_name, "test.mir", text)
    }

    #[test]
    fn async_workflow_resolves_to_its_coroutine_body() {
        let d = doc(
            "demo",
            "fn m::__autumn_workflow_info_wf() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n\
             fn m::wf() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n\
             fn m::wf::{closure#0}() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n",
        );
        let entries = discover(&[d]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].crate_name, "demo");
        assert_eq!(entries[0].workflow, "m::wf");
        assert_eq!(entries[0].body, "m::wf::{closure#0}");
    }

    #[test]
    fn sync_workflow_and_activity_companions() {
        let d = doc(
            "demo",
            "fn __autumn_workflow_info_sync_wf() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n\
             fn sync_wf() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n\
             fn __autumn_activity_info_act() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n",
        );
        let entries = discover(&[d]);
        assert_eq!(
            entries.len(),
            1,
            "activities are not workflows: {entries:?}"
        );
        assert_eq!(entries[0].workflow, "sync_wf");
        assert_eq!(entries[0].body, "sync_wf");
    }

    #[test]
    fn nested_closures_of_the_companion_are_not_entries() {
        let d = doc(
            "demo",
            "fn __autumn_workflow_info_w::{closure#0}() -> u8 {\n    let mut _0: u8;\n\
             \n    bb0: {\n        return;\n    }\n}\n",
        );
        assert!(discover(&[d]).is_empty());
    }
    #[test]
    fn a_marker_header_that_failed_to_parse_still_yields_the_entry() {
        // The exact shape `parse::fail` records for a bad `fn` header: the
        // signature text, verbatim and truncated, with no closing paren.
        let mut d = doc(
            "demo",
            "fn m::wf() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        d.parse_failures.push(mir::ParseFailure {
            item: "m::__autumn_workflow_info_wf(_1: u8 -> u8".to_string(),
            reason: "malformed fn header".to_string(),
            line: 1,
        });
        let entries = discover(&[d]);
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].workflow, "m::wf");
        assert_eq!(entries[0].body, "m::wf", "the workflow body still parsed");
    }

    #[test]
    fn an_entry_from_a_failure_still_prefers_the_coroutine_body() {
        let mut d = doc(
            "demo",
            "fn wf::{closure#0}() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        d.parse_failures.push(mir::ParseFailure {
            item: "fn __autumn_workflow_info_wf(".to_string(),
            reason: "malformed fn header".to_string(),
            line: 1,
        });
        let entries = discover(&[d]);
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].body, "wf::{closure#0}");
    }

    #[test]
    fn a_marker_discovered_twice_is_one_entry() {
        // Belt and braces: a dump that both parsed the marker and recorded a
        // failure mentioning it must not report the workflow twice.
        let mut d = doc(
            "demo",
            "fn __autumn_workflow_info_wf() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n\
             fn wf() -> u8 {\n    let mut _0: u8;\n\n    bb0: {\n        return;\n    }\n}\n",
        );
        d.parse_failures.push(mir::ParseFailure {
            item: "__autumn_workflow_info_wf(_1: u8 -> u8".to_string(),
            reason: "malformed fn header".to_string(),
            line: 1,
        });
        assert_eq!(discover(&[d]).len(), 1);
    }

    #[test]
    fn a_failed_activity_companion_is_not_a_workflow() {
        let mut d = doc("demo", "");
        d.parse_failures.push(mir::ParseFailure {
            item: "__autumn_activity_info_act(_1: u8 -> u8".to_string(),
            reason: "malformed fn header".to_string(),
            line: 1,
        });
        assert!(discover(&[d]).is_empty());
    }

    #[test]
    fn a_nested_item_of_a_failed_companion_is_not_an_entry() {
        let mut d = doc("demo", "");
        d.parse_failures.push(mir::ParseFailure {
            item: "__autumn_workflow_info_w::{closure#0}(_1: u8 -> u8".to_string(),
            reason: "malformed fn header".to_string(),
            line: 1,
        });
        assert!(discover(&[d]).is_empty());
    }

    #[test]
    fn a_module_prefix_is_kept_only_when_it_looks_like_a_path() {
        assert_eq!(module_prefix("m::"), "m");
        assert_eq!(module_prefix("fn a::b::"), "a::b");
        assert_eq!(module_prefix(""), "");
        assert_eq!(module_prefix("fn "), "");
        assert_eq!(
            module_prefix("<impl at s.rs:1:1>::"),
            "",
            "garbage before the marker is dropped, not turned into a path"
        );
        assert_eq!(
            workflow_in_failed_item("!!!__autumn_workflow_info_wf(").as_deref(),
            Some("wf"),
            "an unreadable prefix still leaves a discoverable workflow"
        );
        assert_eq!(workflow_in_failed_item("fn helper() -> u8"), None);
    }
}
