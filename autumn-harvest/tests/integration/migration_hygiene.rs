//! Migration hygiene guards — no DB, no feature gate.
//!
//! These run in the cheap no-DB CI step (`cargo test -p autumn-harvest
//! --no-default-features`) on every OS, so they catch two classes of drift at
//! PR time rather than in production:
//!
//!   1. Duplicate migration timestamp prefixes (two `migrations/<ts>_*` dirs
//!      sharing a `<ts>`), which make Diesel's ordering ambiguous.
//!   2. An incomplete `full_migrations_sql()` bundle (the paved-path helper new
//!      testcontainers tests apply), proving it is regenerated from the whole
//!      `migrations/` tree.
//!
//! This replaces the previously-unreachable `harvest_migration_versions_are_unique`
//! test in `dag_unified_tests.rs`, which was gated
//! `#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]` and so
//! never ran in any CI step.

/// The comma-joined, sorted list of migration directory names emitted by
/// `build.rs` via `cargo:rustc-env=HARVEST_MIGRATIONS_LIST`. Available to this
/// integration-test target because a build script's `rustc-env` applies to
/// every target of the package.
const MIGRATIONS_LIST: &str = env!("HARVEST_MIGRATIONS_LIST");

/// Return any timestamp prefix (the leading run of chars before the first `_`
/// in a migration directory name) that appears more than once in `list`.
///
/// Pure — takes the raw comma-joined `HARVEST_MIGRATIONS_LIST` value so it can
/// be unit-tested with synthetic input.
fn duplicate_version_prefixes(list: &str) -> Vec<String> {
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for name in list.split(',') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let prefix = name.split('_').next().unwrap_or(name);
        *seen.entry(prefix).or_insert(0) += 1;
    }
    let mut dups: Vec<String> = seen
        .into_iter()
        .filter(|&(_, n)| n > 1)
        .map(|(p, _)| p.to_string())
        .collect();
    dups.sort();
    dups
}

#[test]
fn duplicate_version_prefixes_detects_a_synthetic_dup() {
    // Two dirs sharing the `20260101000000` timestamp must be flagged (RED
    // demonstration): this is exactly the ambiguity the real-tree test below
    // guards against.
    let synthetic = "20260101000000_first,20260102000000_middle,20260101000000_collision";
    let dups = duplicate_version_prefixes(synthetic);
    assert_eq!(
        dups,
        vec!["20260101000000".to_string()],
        "the collision prefix must be detected"
    );
}

#[test]
fn duplicate_version_prefixes_is_empty_for_distinct_prefixes() {
    let ok = "20260101000000_a,20260102000000_b,20260103000000_c";
    assert!(
        duplicate_version_prefixes(ok).is_empty(),
        "distinct prefixes must produce no duplicates"
    );
}

#[test]
fn real_migrations_have_unique_version_prefixes() {
    let dups = duplicate_version_prefixes(MIGRATIONS_LIST);
    assert!(
        dups.is_empty(),
        "migrations/ has duplicate timestamp prefixes {dups:?}; Diesel ordering \
         is ambiguous. Rename one migration to a distinct timestamp. Full list: {MIGRATIONS_LIST}"
    );
}

#[test]
fn full_migrations_sql_bundle_is_complete() {
    let bundle = autumn_harvest::full_migrations_sql();
    assert!(!bundle.is_empty(), "migration bundle must not be empty");

    // A sentinel from the very first migration (the initial schema) proves the
    // bundle carries real SQL content, not just headers.
    assert!(
        bundle.contains("CREATE TABLE harvest_workflow_executions"),
        "bundle must include the initial-schema migration"
    );

    // STRONG completeness: build.rs prepends `-- harvest-migration: <dir>` before
    // each migration's up.sql, so the bundle must contain exactly one such header
    // for EVERY entry in HARVEST_MIGRATIONS_LIST. This catches a bundle truncated
    // after ANY migration — not merely the tail — which a single mid-list column
    // sentinel (the old `target_build_id` check) would miss.
    let names: Vec<&str> = MIGRATIONS_LIST
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let mut missing: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !bundle.contains(&format!("-- harvest-migration: {name}\n")))
        .collect();
    missing.sort_unstable();
    assert!(
        missing.is_empty(),
        "full_migrations_sql() is missing {} of {} migration(s) — the bundle drifted from \
         the migrations/ tree: {missing:?}",
        missing.len(),
        names.len()
    );

    // Explicit tail check: the newest (last, since the list is timestamp-sorted)
    // migration's header must be present, so a truncated bundle can never pass.
    let newest = names.last().expect("at least one migration");
    assert!(
        bundle.contains(&format!("-- harvest-migration: {newest}\n")),
        "bundle must include the newest migration ({newest}); a missing tail is the exact \
         drift class this guard prevents"
    );

    let migration_count = names.len();
    assert!(
        migration_count >= 70,
        "expected the full migration set ({migration_count} found)"
    );
    assert!(
        bundle.len() > 10_000,
        "bundle ({} bytes) is implausibly small for {migration_count} migrations",
        bundle.len()
    );
}
