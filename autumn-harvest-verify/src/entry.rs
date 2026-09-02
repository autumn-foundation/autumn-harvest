//! Workflow entry-point discovery.
//!
//! Every `__autumn_workflow_info_X` companion fn the `#[workflow]` macro emits
//! marks `X` (same module) as a workflow; its analyzable body is `X` (sync) or
//! `X::{closure#0}` (async).

use std::collections::BTreeSet;

use crate::mir::MirDoc;

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
            let (prefix, last) = split_last_segment(&body.path);
            let Some(name) = last.strip_prefix(MARKER) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let workflow = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}::{name}")
            };
            let coroutine = format!("{workflow}::{{closure#0}}");
            let target = if paths.contains(coroutine.as_str()) {
                coroutine
            } else {
                workflow.clone()
            };
            entries.push(Entry {
                crate_name: doc.crate_name.clone(),
                workflow,
                body: target,
            });
        }
    }
    entries.sort_by(|a, b| {
        (&a.crate_name, &a.workflow, &a.body).cmp(&(&b.crate_name, &b.workflow, &b.body))
    });
    entries.dedup();
    entries
}

/// Splits `a::b::c` into `("a::b", "c")`; `("", "c")` for a bare path.
fn split_last_segment(path: &str) -> (&str, &str) {
    path.rfind("::")
        .and_then(|at| Some((path.get(..at)?, path.get(at + 2..)?)))
        .unwrap_or(("", path))
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
    fn splitting_paths() {
        assert_eq!(split_last_segment("a::b::c"), ("a::b", "c"));
        assert_eq!(split_last_segment("c"), ("", "c"));
        assert_eq!(split_last_segment(""), ("", ""));
    }
}
