//! Migration hygiene guards — no DB, no feature gate.
//!
//! These run in the cheap no-DB CI step (`cargo test -p autumn-harvest
//! --no-default-features`) on every OS, so they catch three classes of drift at
//! PR time rather than in production:
//!
//!   1. Duplicate migration timestamp prefixes (two `migrations/<ts>_*` dirs
//!      sharing a `<ts>`), which make Diesel's ordering ambiguous.
//!   2. An incomplete `test_init_sql()` bundle (the paved-path helper new
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

/// Migration directory names in the **plugin's** harvest-database tree.
///
/// Read from disk rather than an env var because `build.rs` belongs to the
/// core crate. A missing directory yields an empty list: the guard's job is to
/// detect collisions, not to require the plugin to own any migrations.
fn plugin_harvest_migration_names() -> Vec<String> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../autumn-harvest-plugin/migrations/harvest");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    names.sort();
    names
}

/// Plugin migration names whose version is already taken by a core migration.
///
/// Pure, so the guard below and its RED demonstration exercise the *same*
/// predicate rather than two copies that can drift apart.
fn versions_colliding_with_core<'a>(plugin: &'a [String], core_list: &str) -> Vec<&'a str> {
    let core: BTreeSet<&str> = core_list
        .split(',')
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .filter_map(|n| n.split('_').next())
        .collect();
    plugin
        .iter()
        .filter(|name| {
            name.split('_')
                .next()
                .is_some_and(|version| core.contains(version))
        })
        .map(String::as_str)
        .collect()
}

#[test]
fn plugin_and_core_migrations_never_share_a_version() {
    // Diesel identifies an applied migration by its **version alone** and
    // records it in one `__diesel_schema_migrations` table. The plugin's
    // migrations run against the SAME harvest database as the core ones, and
    // `ensure_runtime_migrations` runs core first — so a plugin migration
    // sharing a core version is silently considered already applied and its
    // SQL NEVER EXECUTES. The table it was meant to create simply does not
    // exist, and the failure surfaces far away at runtime.
    //
    // The sibling `real_migrations_have_unique_version_prefixes` cannot catch
    // this: it only sees the core tree.
    let plugin = plugin_harvest_migration_names();
    if plugin.is_empty() {
        return;
    }
    let collisions = versions_colliding_with_core(&plugin, MIGRATIONS_LIST);

    assert!(
        collisions.is_empty(),
        "plugin migration(s) {collisions:?} reuse a version already taken by a core \
         migration. Diesel keys on version alone and core runs first, so the plugin \
         migration would be skipped entirely and its table never created. Rename it to a \
         version unused anywhere in the repository."
    );
}

#[test]
fn version_collision_detection_covers_both_trees() {
    // RED demonstration for the guard above, on the SAME predicate: a shared
    // version must be reported even though the two directory names differ.
    let core = "20260716000000_core_thing,20260101000000_other";
    let plugin = ["20260716000000_plugin_thing".to_string()];
    assert_eq!(
        versions_colliding_with_core(&plugin, core),
        vec!["20260716000000_plugin_thing"],
        "a version shared with a core migration must be detected"
    );

    // A distinct version is not a collision, so the guard cannot be vacuously
    // green by flagging everything.
    let distinct = ["20260719000000_plugin_thing".to_string()];
    assert!(versions_colliding_with_core(&distinct, core).is_empty());
}

#[test]
fn full_migrations_sql_bundle_is_complete() {
    let bundle = autumn_harvest::test_init_sql();
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
        "test_init_sql() is missing {} of {} migration(s) — the bundle drifted from \
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
// PR #1031 added a completeness guard for `test_init_sql()` itself (above),
// and PR #1045 swept every DB fixture onto that paved path. Neither prevents a
// NEW fixture from *reintroducing* a hand-rolled
// `concat!(include_str!("…migrations/…up.sql"), …)` bundle — which silently
// drifts as migrations land, failing only at runtime with
// `column/relation … does not exist`. The guard below makes the paved path
// self-enforcing: any file under the two test dirs that reintroduces a
// hand-rolled migration include must be in the small, documented allowlist.

/// Test-fixture files (workspace-root-relative paths) that legitimately still
/// hand-roll a migration bundle instead of calling
/// `autumn_harvest::test_init_sql()`. Each entry documents WHY the fixture
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
    // the core `migrations/` bundle that `test_init_sql()` emits.
    "autumn-harvest-plugin/tests/outbox_integration.rs",
    // The three connector suites (issue #944) each build
    // `test_init_sql()` and then append the plugin-owned
    // `harvest_connector_dead_letters` migration, which likewise lives in
    // `autumn-harvest-plugin/migrations/harvest/` rather than the core bundle.
    // The paved path is used for everything it covers; only the one plugin
    // table is hand-appended.
    "autumn-harvest-plugin/tests/connector_integration.rs",
    "autumn-harvest-plugin/tests/connector_kafka_broker.rs",
    "autumn-harvest-plugin/tests/connector_sqs_broker.rs",
];

/// True when a single source line reintroduces a hand-rolled migration bundle: a
/// one-line `include_str!("…/migrations/…/up.sql")` reference. The all-three-tokens
/// test is robust to path shape (`../migrations/…`, `../../autumn-harvest/migrations/…`,
/// etc.) while never matching the paved-path helper's own non-migration includes
/// (e.g. `include_str!("…/ci.yml")`, which lacks both `migrations` and `up.sql`).
///
/// The offender scan itself uses the whole-file
/// [`detects_handrolled_migration_include`], which also catches the rustfmt-wrapped
/// form where `include_str!(` and its path string land on different lines; this
/// single-line predicate is retained for its focused unit test.
fn line_is_handrolled_migration_include(line: &str) -> bool {
    line.contains("include_str!") && line.contains("migrations") && line.contains("up.sql")
}

/// True when `contents` (a whole `.rs` file) reintroduces a hand-rolled migration
/// bundle. Robust to rustfmt wrapping the `include_str!` argument onto the next
/// line — the exact gap the per-line scan missed:
///
/// ```text
/// concat!(
///     include_str!(
///         "../../migrations/20260101000000_foo/up.sql",
///     ),
/// )
/// ```
///
/// Here `include_str!(` and the `"…/migrations/…/up.sql"` literal are on different
/// lines, so no single line carries all three tokens. This detector collapses all
/// whitespace and then structurally checks each `include_str!` occurrence: it must
/// be followed (across any newlines/indent) by `(` and a string literal whose value
/// contains `migrations` and ends in `up.sql`. It catches both the one-line and the
/// rustfmt-wrapped shapes, and is STRICTLY MORE PRECISE than a per-line substring
/// test — the tokens must belong to one real `include_str!` argument — so
/// prose/doc comments merely mentioning `migrations/.../up.sql`, or a
/// `test_init_sql()` reference, are never flagged.
fn detects_handrolled_migration_include(contents: &str) -> bool {
    // Collapse every run of whitespace (incl. newlines/indent) to a single space
    // so a wrapped `include_str!(\n  "..."\n)` becomes `include_str!( "..." )`, and
    // a one-line `include_str!("...")` is handled by the same scan. Paths carry no
    // internal whitespace, so their content is unaffected.
    let normalized = contents.split_whitespace().collect::<Vec<_>>().join(" ");
    for (idx, _) in normalized.match_indices("include_str!") {
        let rest = normalized[idx + "include_str!".len()..].trim_start();
        let Some(after_paren) = rest.strip_prefix('(') else {
            continue;
        };
        let Some(after_quote) = after_paren.trim_start().strip_prefix('"') else {
            continue;
        };
        // A path literal has no embedded quotes, so it ends at the next `"`.
        let Some(end) = after_quote.find('"') else {
            continue;
        };
        let literal = &after_quote[..end];
        if literal.contains("migrations") && literal.ends_with("up.sql") {
            return true;
        }
    }
    false
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
        r#"    include_str!("../migrations/app/20260409010000_harvest_workflow_outbox/up.sql");"#
    ));
    // A non-migration `include_str!` (the paved-path helper's own CI includes)
    // must NOT be flagged — it lacks both `migrations` and `up.sql`.
    assert!(!line_is_handrolled_migration_include(
        r#"const CI_YAML: &str = include_str!("../../../.github/workflows/ci.yml");"#
    ));
    // The paved path itself is not a hand-rolled include.
    assert!(!line_is_handrolled_migration_include(
        "    let sql = autumn_harvest::test_init_sql();"
    ));
}

#[test]
fn whole_file_detector_flags_a_rustfmt_wrapped_include() {
    // rustfmt wraps a long `include_str!` argument onto the next line, so
    // `include_str!(` and the "…/migrations/…/up.sql" string literal land on
    // DIFFERENT lines. The per-line scan alone MISSES this (asserted below); the
    // whole-file detector the offender scan uses must catch it.
    let wrapped = "concat!(\n    include_str!(\n        \"../../migrations/20260101000000_foo/up.sql\",\n    ),\n)";
    // Guard-of-the-guard: the per-line scan genuinely cannot see it — proving the
    // whole-file detector is doing real work, not shadowing the fast path.
    assert!(
        !wrapped.lines().any(line_is_handrolled_migration_include),
        "sanity: the wrapped form must be invisible to the per-line scan"
    );
    assert!(
        detects_handrolled_migration_include(wrapped),
        "the whole-file detector must flag a rustfmt-wrapped include_str! bundle"
    );

    // A single-line include is still caught (the fast path).
    assert!(detects_handrolled_migration_include(
        r#"    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),"#
    ));
    // No false positives: a non-migration wrapped include, and prose/doc comments
    // merely mentioning the path, must NOT be flagged.
    assert!(!detects_handrolled_migration_include(
        "const CI: &str = include_str!(\n    \"../../../.github/workflows/ci.yml\"\n);"
    ));
    assert!(!detects_handrolled_migration_include(
        "// this fixture used to include_str! a migrations/foo/up.sql bundle; now paved.\nlet sql = autumn_harvest::test_init_sql();"
    ));
}

/// The self-enforcing guard: no file under `autumn-harvest/tests/**` or
/// `autumn-harvest-plugin/tests/**` may reintroduce a hand-rolled migration
/// bundle outside `ALLOWED_HANDROLLED_MIGRATION_INCLUDES`, and every allowlisted
/// entry must still actually contain one (a stale entry — a fixture later
/// converted to `test_init_sql()` — fails and must be removed).
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
        if !detects_handrolled_migration_include(&src) {
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
         (include_str!(\"…migrations/…up.sql\")). Use autumn_harvest::test_init_sql() \
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
         hand-rolled migration include (converted to test_init_sql()?). Remove them:\n  {}",
        stale.join("\n  ")
    );
}

/// The release-upgrade guide whose migration inventory this guard keeps honest.
///
/// Root-relative so the failure message names the file an author must edit.
const UPGRADE_GUIDE: &str = "docs/upgrading/0.5.0.md";

/// Extract the migration directory names listed in the upgrade guide's
/// inventory table, i.e. the leading `` `<dir>` `` cell of every row shaped
/// `` | `<dir>` | … | ``.
///
/// Pure — takes the guide's text so it can be unit-tested with synthetic input.
fn migrations_listed_in_guide(markdown: &str) -> BTreeSet<String> {
    markdown
        .lines()
        .filter_map(|line| {
            let cell = line.trim().strip_prefix("| `")?;
            let (name, _) = cell.split_once('`')?;
            // Inventory rows only: a migration dir is `<timestamp>_<slug>`, so
            // require a leading all-digit timestamp. This skips every other
            // backtick-leading table in the guide (config keys, metric names).
            let prefix = name.split('_').next()?;
            (prefix.len() >= 8 && prefix.chars().all(|c| c.is_ascii_digit()))
                .then(|| name.to_string())
        })
        .collect()
}

#[test]
fn guide_row_extractor_reads_migration_rows_and_ignores_others() {
    let synthetic = "\
| Migration dir | Adds |\n\
|---------------|------|\n\
| `20260618000001_harvest_debounce` | `harvest_debounce` table (#499) |\n\
| `20260715000000_harvest_queue_pause` | `harvest_queue_pauses` (#619) |\n\
| `harvest.queue.paused` | a metric, not a migration |\n\
plain prose mentioning `20260101000000_not_a_row`\n";
    let found = migrations_listed_in_guide(synthetic);
    assert_eq!(
        found,
        [
            "20260618000001_harvest_debounce",
            "20260715000000_harvest_queue_pause"
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect::<BTreeSet<String>>(),
        "only timestamped migration rows must be extracted"
    );
}

/// Every migration in this release must appear in the upgrade guide's inventory.
///
/// The guide states it lists the migrations `0.5.0` adds, and non-`dev` profiles
/// **refuse to start** with a pending migration — so an omitted row means an
/// operator following the guide applies an incomplete set and the deploy fails
/// (or, if they bypass the plugin check, the new queries fail against a missing
/// relation). Issue #619 shipped `20260715000000_harvest_queue_pause` without
/// its row; nothing caught it, because this is the one inventory in the repo
/// with no drift guard. This is that guard.
///
/// **Scope** is every migration at or after the guide's own earliest listed
/// entry, so migrations from prior releases are correctly out of scope and a
/// NEW migration — which always sorts later — is always in scope. That makes
/// the boundary self-maintaining rather than a hardcoded date this guard would
/// itself have to keep current.
#[test]
fn every_release_migration_is_in_the_upgrade_guide() {
    let path = workspace_root().join(UPGRADE_GUIDE);
    let markdown = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("upgrade guide '{}' must be readable: {e}", path.display()));

    let listed = migrations_listed_in_guide(&markdown);
    assert!(
        !listed.is_empty(),
        "no migration rows parsed from {UPGRADE_GUIDE} — the inventory table's shape changed, \
         so this guard is silently vacuous. Fix the parser, do not delete the test."
    );

    // The guide's earliest listed migration is the release boundary.
    let first = listed
        .iter()
        .next()
        .expect("non-empty, checked above")
        .clone();

    let mut missing: Vec<&str> = MIGRATIONS_LIST
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty() && *name >= first.as_str())
        .filter(|name| !listed.contains(*name))
        .collect();
    missing.sort_unstable();

    assert!(
        missing.is_empty(),
        "these migrations are missing from the inventory table in {UPGRADE_GUIDE}:\n  {}\n\
         An operator following that guide would apply an incomplete migration set, and a \
         non-`dev` profile refuses to start with a migration pending. Add a row per migration \
         (release boundary: the guide's earliest listed entry, `{first}`).",
        missing.join("\n  ")
    );
}
