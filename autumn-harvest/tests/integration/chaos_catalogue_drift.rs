//! Chaos injection-point catalogue drift guard (issue #940).
//!
//! A no-DB, no-`chaos`-feature source scan asserting that every point in
//! `autumn_harvest::chaos::points::ALL` is wired at **exactly one** production
//! call site in `src/` via the matching macro. `points` is unconditional
//! (compiled into every build), so this test runs in the normal test build and
//! can never silently pass while a catalogue entry has lost its wiring — the
//! failure mode that would otherwise make an injection point a dead no-op.

use std::path::{Path, PathBuf};

/// Expected `(point ident, wiring macro)` for every catalogue entry. Kept in
/// lockstep with `points::ALL` by the assertions below.
const EXPECTED: &[(&str, &str)] = &[
    ("QUEUE_PARK_BEFORE_UPDATE", "chaos_point"),
    ("WORKER_PERSIST_BEFORE_COMMIT", "chaos_point"),
    ("WORKER_AFTER_OUTER_COMMIT", "chaos_point"),
    ("OUTBOX_INLINE_AFTER_REQUESTED", "chaos_point"),
    ("SCHED_AFTER_CLAIM", "chaos_point"),
    ("SCHED_AFTER_START_BEFORE_ADVANCE", "chaos_point"),
    ("POISON_RECLAIM_BEFORE_LOAD", "chaos_fallible"),
    ("NOTIFY_TASK_ENQUEUED", "chaos_drop_notify"),
];

fn src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Recursively collect the contents of every `.rs` file under `src/`, EXCEPT
/// the two files where the chaos vocabulary is *defined* rather than *used*:
/// `chaos.rs` (the point consts + catalogue) and `lib.rs` (the
/// `chaos_point!` / `chaos_fallible!` / `chaos_drop_notify!` `macro_rules`
/// definitions, whose bodies and doc examples reference the macro names and a
/// `NAME` placeholder). Neither is production wiring.
fn production_sources() -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![src_dir()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read_dir src") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "chaos.rs" || name == "lib.rs" {
                    continue;
                }
                let body = std::fs::read_to_string(&path).expect("read source");
                out.push((path.display().to_string(), body));
            }
        }
    }
    out
}

/// Count wiring invocations `<macro>!(<ident>)` across the production sources.
fn wiring_count(sources: &[(String, String)], macro_name: &str, ident: &str) -> usize {
    let needle = format!("{macro_name}!({ident})");
    sources
        .iter()
        .map(|(_, body)| body.matches(&needle).count())
        .sum()
}

#[test]
fn every_catalogue_point_is_wired_exactly_once() {
    let sources = production_sources();
    for (ident, macro_name) in EXPECTED {
        let count = wiring_count(&sources, macro_name, ident);
        assert_eq!(
            count, 1,
            "chaos point {ident} must be wired at exactly one production call site \
             via {macro_name}!(...); found {count}. Wire it (or remove it from the \
             catalogue) — a catalogue entry with no wiring is a dead no-op."
        );
    }
}

#[test]
fn expected_list_matches_catalogue_all() {
    use autumn_harvest::chaos::points::ALL;
    assert_eq!(
        ALL.len(),
        EXPECTED.len(),
        "the drift-test EXPECTED list ({}) must match points::ALL ({}); \
         update both when adding or removing a point",
        EXPECTED.len(),
        ALL.len()
    );
    // Every catalogue point's `name()` uses dotted namespacing and is unique;
    // the count assertion above binds `ALL` and `EXPECTED` to the same length,
    // and `every_catalogue_point_is_wired_exactly_once` binds each `EXPECTED`
    // ident to a real wired call site — so neither list can drift alone.
    let mut names: Vec<&str> = ALL.iter().map(|p| p.name()).collect();
    let before = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(before, names.len(), "two catalogue points share a name()");
}

/// No production wiring may reference an ident that is not in the catalogue —
/// otherwise a stray `chaos_point!(TYPO)` would compile-fail (good) or a
/// renamed-but-not-removed point would leave an orphan call site.
#[test]
fn no_orphan_chaos_macro_call_sites() {
    let sources = production_sources();
    let known: Vec<&str> = EXPECTED.iter().map(|(ident, _)| *ident).collect();
    for (path, body) in &sources {
        for macro_name in ["chaos_point", "chaos_fallible", "chaos_drop_notify"] {
            let open = format!("{macro_name}!(");
            let mut search = body.as_str();
            while let Some(pos) = search.find(&open) {
                let after = &search[pos + open.len()..];
                let end = after
                    .find(')')
                    .expect("chaos macro call must close its paren");
                let ident = after[..end].trim();
                assert!(
                    known.contains(&ident),
                    "orphan chaos macro call {macro_name}!({ident}) in {path}: \
                     ident is not in the catalogue drift list"
                );
                search = &after[end..];
            }
        }
    }
}
