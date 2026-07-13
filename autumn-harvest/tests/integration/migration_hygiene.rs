//! Migration hygiene guards — no DB, no feature gate.
//!
//! These run in the cheap no-DB CI step (`cargo test -p autumn-harvest
//! --no-default-features`) on every OS, so they catch three classes of drift at
//! PR time rather than in production:
//!
//!   1. Duplicate migration timestamp prefixes (two `migrations/<ts>_*` dirs
//!      sharing a `<ts>`), which make Diesel's ordering ambiguous.
//!   2. An incomplete `full_migrations_sql()` bundle (the paved-path helper new
//!      testcontainers tests apply), proving it is regenerated from the whole
//!      `migrations/` tree.
//!   3. A test fixture that *reintroduces* a hand-rolled migration bundle
//!      (`concat!(include_str!("…migrations/…up.sql"), …)`) outside a small,
//!      documented allowlist — the drift the paved-path sweep (PR #1045) exists
//!      to prevent. Without this, a NEW fixture can silently reintroduce a
//!      hand-picked schema that rots as migrations land (failing only at runtime
//!      with `column/relation … does not exist`); neither guard (1) nor the
//!      run-coverage guard in `ci_run_coverage.rs` catches it.
//!
//! This replaces the previously-unreachable `harvest_migration_versions_are_unique`
//! test in `dag_unified_tests.rs`, which was gated
//! `#[cfg(all(feature = "testing", feature = "unified-dag-execution"))]` and so
//! never ran in any CI step.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

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

// ── Paved-path reintroduction guard (PR #1031 / PR #1045 follow-up) ───────────
//
// PR #1031 added a completeness guard for `full_migrations_sql()` itself (above),
// and PR #1045 swept every DB fixture onto that paved path. Neither prevents a
// NEW fixture from *reintroducing* a hand-rolled
// `concat!(include_str!("…migrations/…up.sql"), …)` bundle — which silently
// drifts as migrations land, failing only at runtime with
// `column/relation … does not exist`. The guard below makes the paved path
// self-enforcing: any file under the two test dirs that reintroduces a
// hand-rolled migration include must be in the small, documented allowlist.

/// Test-fixture files (workspace-root-relative paths) that legitimately still
/// hand-roll a migration bundle instead of calling
/// `autumn_harvest::full_migrations_sql()`. Each entry documents WHY the fixture
/// needs a bespoke — usually deliberately-partial — schema. Adding a new partial
/// fixture requires a conscious edit here with a reason; that is the point.
const ALLOWED_HANDROLLED_MIGRATION_INCLUDES: &[&str] = &[
    // Legacy upgrade-path fixture: drives a real historical schema upgrade and is
    // deliberately excluded from the workflow-start-uniqueness column set, so it
    // must build a partial, hand-picked schema rather than the full bundle.
    "autumn-harvest/tests/integration/integration_e2e.rs",
    // Omits `harvest_start_throttle` on purpose to exercise the `to_regclass`
    // graceful-degradation path (the scheduler tolerates the table's absence).
    "autumn-harvest-plugin/tests/schedule_update_integration.rs",
    // Inserts into `harvest_dag_runs`, which the full bundle drops
    // (`20260514000000_drop_harvest_dag_runs`); needs the pre-drop schema.
    "autumn-harvest-plugin/tests/timeline_integration.rs",
    // Separate plugin app-DB `harvest_workflow_outbox` migration — not part of
    // the core `migrations/` bundle that `full_migrations_sql()` emits.
    "autumn-harvest-plugin/tests/outbox_integration.rs",
];

/// True when a source line reintroduces a hand-rolled migration bundle: a single
/// `include_str!("…/migrations/…/up.sql")` reference. Each bundle is one such
/// include per line inside a `concat!`, so a per-line all-three-tokens test is
/// robust to path shape (`../migrations/…`, `../../autumn-harvest/migrations/…`,
/// etc.) while never matching the paved-path helper's own non-migration includes
/// (e.g. `include_str!("…/ci.yml")`, which lacks both `migrations` and `up.sql`).
///
/// Accepted limitation (intentional): the check is per-line, so a bundle written
/// with EVERY include's path split onto the line after `include_str!(` (as
/// rustfmt does for very long paths) could evade it. Any single-line include and
/// any copy-paste reintroduction is still caught, so this residual gap is a
/// deliberate, low-likelihood trade-off rather than a full-parse detector.
fn line_is_handrolled_migration_include(line: &str) -> bool {
    line.contains("include_str!") && line.contains("migrations") && line.contains("up.sql")
}

/// The workspace root — the parent of this crate's `CARGO_MANIFEST_DIR` — that
/// both test dirs hang off. Used to render clean, `..`-free, root-relative keys.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate manifest dir has a parent (the workspace root)")
        .to_path_buf()
}

/// The two directories scanned: this crate's `tests/` (recursively — so
/// `tests/integration/` is covered) and the plugin crate's `tests/`. Both are
/// built without `..` segments so root-relative keys are clean.
fn scanned_test_roots() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = workspace_root();
    vec![
        manifest.join("tests"),
        workspace.join("autumn-harvest-plugin").join("tests"),
    ]
}

/// Recursively collect every `.rs` file under `dir` into `out`. A *missing* dir
/// is tolerated silently (both roots exist in-tree; `ci_run_coverage.rs` reads
/// the same plugin dir at runtime, so it is present when this test runs), but any
/// OTHER `read_dir`/entry error PANICS — a permission or IO error must never be
/// masked, as that could let the guard silently skip a file it should scan.
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A missing directory is fine (documented assumption).
            return;
        }
        Err(e) => panic!(
            "migration guard: failed to read directory '{}': {e}",
            dir.display()
        ),
    };
    for entry in entries {
        // A DirEntry we just enumerated but then fail to read is unexpected —
        // panic rather than silently skip it (a skipped file could hide a
        // reintroduced hand-rolled migration bundle).
        let entry = entry.unwrap_or_else(|e| {
            panic!(
                "migration guard: failed to read a directory entry under '{}': {e}",
                dir.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn line_detector_flags_a_handrolled_include_and_ignores_others() {
    // Real bundle-include shapes from the four allowlisted fixtures.
    assert!(line_is_handrolled_migration_include(
        r#"    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),"#
    ));
    assert!(line_is_handrolled_migration_include(
        r#"    include_str!("../migrations/20260409010000_harvest_workflow_outbox/up.sql");"#
    ));
    // A non-migration `include_str!` (the paved-path helper's own CI includes)
    // must NOT be flagged — it lacks both `migrations` and `up.sql`.
    assert!(!line_is_handrolled_migration_include(
        r#"const CI_YAML: &str = include_str!("../../../.github/workflows/ci.yml");"#
    ));
    // The paved path itself is not a hand-rolled include.
    assert!(!line_is_handrolled_migration_include(
        "    let sql = autumn_harvest::full_migrations_sql();"
    ));
}

/// The self-enforcing guard: no file under `autumn-harvest/tests/**` or
/// `autumn-harvest-plugin/tests/**` may reintroduce a hand-rolled migration
/// bundle outside `ALLOWED_HANDROLLED_MIGRATION_INCLUDES`, and every allowlisted
/// entry must still actually contain one (a stale entry — a fixture later
/// converted to `full_migrations_sql()` — fails and must be removed).
#[test]
fn no_new_handrolled_migration_bundles_outside_allowlist() {
    let root = workspace_root();

    let mut files = Vec::new();
    for r in scanned_test_roots() {
        collect_rs_files(&r, &mut files);
    }
    files.sort();

    let allow: BTreeSet<&str> = ALLOWED_HANDROLLED_MIGRATION_INCLUDES
        .iter()
        .copied()
        .collect();

    // Workspace-root-relative keys of every file that reintroduces a bundle.
    let mut offender_keys: BTreeSet<String> = BTreeSet::new();
    for path in &files {
        // Exclude THIS guard file: it carries the detection tokens (`include_str!`,
        // `migrations`, `up.sql`) as string literals in its logic and messages and
        // would otherwise flag itself.
        if path.file_name().and_then(|n| n.to_str()) == Some("migration_hygiene.rs") {
            continue;
        }
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        if !src.lines().any(line_is_handrolled_migration_include) {
            continue;
        }
        let key = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        offender_keys.insert(key);
    }

    // Direction 1: every offender must be allowlisted.
    let mut new_offenders: Vec<&String> = offender_keys
        .iter()
        .filter(|k| !allow.contains(k.as_str()))
        .collect();
    new_offenders.sort();
    assert!(
        new_offenders.is_empty(),
        "these test fixtures reintroduce a hand-rolled migration bundle \
         (include_str!(\"…migrations/…up.sql\")). Use autumn_harvest::full_migrations_sql() \
         instead (see PR #1045), or — only if the fixture genuinely needs a partial schema — \
         add it to ALLOWED_HANDROLLED_MIGRATION_INCLUDES with a documented reason:\n  {}",
        new_offenders
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );

    // Direction 2: keep the allowlist honest — no stale entries.
    let mut stale: Vec<&str> = allow
        .iter()
        .copied()
        .filter(|k| !offender_keys.contains(*k))
        .collect();
    stale.sort_unstable();
    assert!(
        stale.is_empty(),
        "ALLOWED_HANDROLLED_MIGRATION_INCLUDES has stale entries — these no longer contain a \
         hand-rolled migration include (converted to full_migrations_sql()?). Remove them:\n  {}",
        stale.join("\n  ")
    );
}
