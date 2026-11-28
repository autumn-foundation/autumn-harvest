//! The three-way merge of the files of a swarm (issue #2814).
//!
//! A submit merges the files of a member onto the current main. The base is
//! the main version that the claim checked out. Each path goes through
//! [`pick`]. A change of one side wins over an unchanged other side. Two
//! different changes of one path are a conflict. A missing file counts
//! as a value, so a delete is a change too.

use std::collections::{BTreeMap, BTreeSet};

use super::kernel::{Pick, pick};
use crate::workspace::WorkspaceFile;

/// The files of one side of a merge, by path.
pub type Files = BTreeMap<String, WorkspaceFile>;

/// The result of a three-way merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResult {
    /// The merged files.
    Clean(Files),
    /// The paths that both sides changed in different ways, in path order.
    Conflict(Vec<String>),
}

/// Merge `ours` onto `theirs`, against `base`.
///
/// The result keeps each change of each side. A path that both sides
/// changed to the same file is not a conflict.
#[must_use]
pub fn three_way(base: &Files, ours: &Files, theirs: &Files) -> MergeResult {
    let paths: BTreeSet<&String> = base
        .keys()
        .chain(ours.keys())
        .chain(theirs.keys())
        .collect();
    let mut merged = Files::new();
    let mut conflicts = Vec::new();
    for path in paths {
        let (b, o, t) = (base.get(path), ours.get(path), theirs.get(path));
        let chosen = match pick(o == b, t == b, o == t) {
            Pick::Ours => o,
            Pick::Theirs => t,
            Pick::Conflict => {
                conflicts.push(path.clone());
                continue;
            }
        };
        if let Some(file) = chosen {
            merged.insert(path.clone(), file.clone());
        }
    }
    if conflicts.is_empty() {
        MergeResult::Clean(merged)
    } else {
        MergeResult::Conflict(conflicts)
    }
}

/// The files under `prefix`, with the prefix cut off. An empty prefix is
/// every file.
#[must_use]
pub fn under(files: &Files, prefix: &str) -> Files {
    files
        .iter()
        .filter_map(|(path, file)| {
            path.strip_prefix(prefix)
                .filter(|rest| !rest.is_empty())
                .map(|rest| (rest.to_owned(), file.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn file(text: &str) -> WorkspaceFile {
        WorkspaceFile {
            data: text.as_bytes().to_vec(),
            executable: false,
        }
    }

    fn files(pairs: &[(&str, &str)]) -> Files {
        pairs
            .iter()
            .map(|(path, text)| ((*path).to_owned(), file(text)))
            .collect()
    }

    #[test]
    fn each_side_keeps_its_own_change() {
        let base = files(&[("a", "1"), ("b", "1")]);
        let ours = files(&[("a", "2"), ("b", "1")]);
        let theirs = files(&[("a", "1"), ("b", "3"), ("c", "new")]);
        assert_eq!(
            three_way(&base, &ours, &theirs),
            MergeResult::Clean(files(&[("a", "2"), ("b", "3"), ("c", "new")]))
        );
    }

    #[test]
    fn a_delete_is_a_change() {
        let base = files(&[("a", "1"), ("b", "1")]);
        let ours = files(&[("b", "1")]);
        let theirs = files(&[("a", "1")]);
        assert_eq!(
            three_way(&base, &ours, &theirs),
            MergeResult::Clean(Files::new())
        );
    }

    #[test]
    fn two_different_changes_conflict() {
        let base = files(&[("a", "1"), ("b", "1")]);
        let ours = files(&[("a", "2"), ("b", "2")]);
        let theirs = files(&[("a", "3"), ("b", "2")]);
        assert_eq!(
            three_way(&base, &ours, &theirs),
            MergeResult::Conflict(vec!["a".to_owned()])
        );
    }

    #[test]
    fn under_cuts_the_prefix() {
        let all = files(&[("repo/a", "1"), ("repo/x/b", "2"), ("notes", "3")]);
        assert_eq!(under(&all, "repo/"), files(&[("a", "1"), ("x/b", "2")]));
        assert_eq!(under(&all, ""), all);
    }

    fn side() -> impl Strategy<Value = Files> {
        proptest::collection::btree_map("[a-d]", "[0-2]", 0..4).prop_map(|map| {
            map.into_iter()
                .map(|(path, text)| (path, file(&text)))
                .collect()
        })
    }

    proptest! {
        #[test]
        fn a_clean_merge_keeps_every_change(base in side(), ours in side(), theirs in side()) {
            if let MergeResult::Clean(merged) = three_way(&base, &ours, &theirs) {
                let paths: BTreeSet<&String> =
                    base.keys().chain(ours.keys()).chain(theirs.keys()).collect();
                for path in paths {
                    let got = merged.get(path);
                    if ours.get(path) != base.get(path) {
                        prop_assert_eq!(got, ours.get(path));
                    }
                    if theirs.get(path) != base.get(path) {
                        prop_assert_eq!(got, theirs.get(path));
                    }
                    if ours.get(path) == base.get(path) && theirs.get(path) == base.get(path) {
                        prop_assert_eq!(got, base.get(path));
                    }
                }
            }
        }

        #[test]
        fn merging_onto_an_unchanged_main_gives_ours(base in side(), ours in side()) {
            prop_assert_eq!(three_way(&base, &ours, &base), MergeResult::Clean(ours));
        }
    }
}
