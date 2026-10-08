//! CI run-step coverage guard — no DB, no feature gate.
//!
//! Purpose: catch the "silently-never-run DB test" class of gap. A DB-gated
//! integration test (core or plugin) can *compile* in CI — via `--no-run` and
//! clippy — yet never be *executed* against a live Postgres, so any real
//! runtime failure stays invisible. `workflow_retry_tests` is a live example:
//! six of its nine DB tests never ran in CI (the run step is limited to a
//! `::workflow_typed` sub-filter), hiding three genuine workflow-level-retry
//! bugs.
//!
//! This is a *run-coverage* guard, deliberately distinct from *migration
//! drift* (a hand-rolled migration bundle missing a migration, which would
//! fail `column/relation ... does not exist` at runtime). Drift is guarded
//! separately in `migration_hygiene.rs`; this guard only answers "is every
//! DB-gated test actually RUN by some CI step?" — never whether its schema
//! bundle is complete.
//!
//! Source of truth: the per-suite CI runs are now DATA in the manifest
//! `.github/ci/integration-suites.txt` (executed by `.github/ci/run-suites.sh`,
//! which the `test` job invokes), not copy-pasted `cargo test` steps in
//! `ci.yml`. This guard parses that structured manifest instead of scraping
//! `run:` lines — the columns are already split, so the old `--test-threads=1`
//! special-casing, `--no-run` string-grepping, and `module::filter`
//! re-tokenization are gone. A target is NOT credited as run when its manifest
//! row is `compileonly` (an explicit column, never a `--no-run` grep), when the
//! row selects only a `module::filter` sub-slice (the `::workflow_typed` trap),
//! or when every one of its `#[test]`/`#[tokio::test]` fns is `#[ignore]`d (a
//! run then executes nothing). Every DB-gated test must either have a covering
//! `linux`/`allos` manifest row or be listed — with a reason — in `ALLOWLIST`.
//! `ALLOWLIST` is fail-closed debt to SHRINK by adding manifest rows, not to
//! grow. A hard ratchet holds its cap equal to its length (see below).
//!
//! A NEW assertion guards against the manifest being silently ignored: `ci.yml`
//! must actually invoke the runner against the manifest, else the guard would
//! read a manifest CI never executes (green-but-not-run). A second NEW assertion
//! keeps the `merge=union` manifest sorted + unique (a union-merge artifact is a
//! benign, one-command-fix CI failure, never a conflict).
//!
//! A third assertion (issue #1790) parses every workflow file. It also checks
//! that each workflow that an `ALLOWLIST` or `FEATURE_GATE_EXEMPT` reason cites
//! exists. A workflow that does not parse never runs, so a reason that cites it
//! is false.
//!
//! On failure the panic message lists every uncovered test and the exact
//! manifest line to add.

use std::collections::BTreeSet;
use std::path::PathBuf;

// ── Inputs embedded at compile time (recompile when they change) ────────────

/// The whole CI workflow, so a reformat that breaks the invocation assertion
/// recompiles — and trips the self-test below — rather than silently passing.
const CI_YAML: &str = include_str!("../../../.github/workflows/ci.yml");

/// The manifest: the single source of truth for which per-suite runs CI does.
const MANIFEST: &str = include_str!("../../../.github/ci/integration-suites.txt");

/// The runner script, so we can assert it actually reads the manifest (closing
/// the "runner step present but pointed elsewhere" gap).
const RUN_SCRIPT: &str = include_str!("../../../.github/ci/run-suites.sh");

/// The integration submodule declarations (source of truth for which core
/// suites exist and their cfg gates).
const CORE_MOD_RS: &str = include_str!("mod.rs");

// ── DB classification ───────────────────────────────────────────────────────

/// A test file "needs a live DB" (== a Docker-backed CI run) iff its *code*
/// spins up / connects to Postgres. Two families of markers:
///   * the classic testcontainers style (`with_init_sql(` / `Postgres::default(`)
///     and the `HARVEST_TEST_DATABASE_URL` opt-in override, and
///   * the paved `autumn_web::test::TestDb` + `run_pending(MIGRATIONS)` harness
///     (`run_pending(` / `TestDb` / a real `testcontainers` import), which the
///     original three-token list missed — so `mcp_tools_integration`,
///     `webhook_receiver_integration`, and `webhook_durable_integration` evaded
///     classification entirely (fail-open). Broadened here so they are caught.
///
/// A third family calls a *shared* fixture helper instead of building the
/// container itself: `integration_e2e::setup_test_database_url` and
/// `setup_test_database_url_or_env`. The caller carries none of the tokens
/// above, so the caller was fail-open. The two helper names are tokens as
/// well, which classifies every caller of the shared fixture.
///
/// Matching is against comment-stripped code only (see [`strip_line_comments`]):
/// several genuinely no-DB HTTP tests reference their sibling `testcontainers`
/// suite in `//!` prose, and the deliberately env-gated `status_summary_localpg`
/// mentions `testcontainers` only in a doc comment — none of these must be
/// misclassified. A file that no-ops unless `DATABASE_URL` is set (its only
/// container mention being prose) is therefore left unmatched: it never runs in
/// CI and cannot be a run-coverage gap.
const LIVE_DB_TOKENS: &[&str] = &[
    "with_init_sql(",
    "Postgres::default(",
    "HARVEST_TEST_DATABASE_URL",
    "run_pending(",
    "TestDb",
    "testcontainers",
    "setup_test_database_url(",
    "setup_test_database_url_or_env(",
];

/// Drop whole-line comments (`//`, `///`, `//!`) so container tokens that
/// appear only in prose don't classify a no-DB test as needing a live DB. A
/// leading-`//` check is enough: every observed false positive lives in a
/// `//!` doc-comment block, and stripping only leading-comment lines never
/// mangles mid-line code such as a `postgres://…` URL literal.
fn strip_line_comments(source: &str) -> String {
    source
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn needs_live_db(source: &str) -> bool {
    let code = strip_line_comments(source);
    LIVE_DB_TOKENS.iter().any(|t| code.contains(t))
}

/// True when a file declares `#[test]`/`#[tokio::test]` fns but every one is
/// `#[ignore]`d, so a run targeting it executes nothing and must NOT be
/// credited as covering it (the target must be allowlisted instead). Counts
/// `#[ignore]` against the total test count; a file with no tests is not
/// "all-ignored".
///
/// The count is taken over comment-stripped code (see [`strip_line_comments`]).
/// A module doc comment can name an `#[ignore]`d case in prose. Such prose
/// would otherwise inflate the ignored count above the real test count, and a
/// suite with live cases would read as running nothing.
fn all_tests_ignored(source: &str) -> bool {
    let code = strip_line_comments(source);
    let tests = code.matches("#[tokio::test").count() + code.matches("#[test]").count();
    let ignored = code.matches("#[ignore").count();
    tests > 0 && ignored >= tests
}

/// Meta-test files that are not DB tests but mention the classifier tokens as
/// string literals (this guard defines `LIVE_DB_TOKENS`), so they'd otherwise
/// be misclassified as needing a live DB.
const SELF_EXCLUDE: &[&str] = &["ci_run_coverage", "migration_hygiene"];

/// True when a file carries live-DB machinery but declares no test that could
/// execute it — i.e. it is a shared **harness** consumed by a real suite, not a
/// suite itself.
///
/// Every DB-backed suite in this tree drives its database from an async test
/// (`#[tokio::test]`) or an explicit `block_on`. A file with neither cannot run
/// a DB test no matter how it is invoked, so requiring a manifest row for it
/// would demand a CI step that executes nothing — the inverse of what this
/// guard exists to prevent. Its DB code is covered transitively by whichever
/// suite consumes it (which does need, and has, its own row).
///
/// Deliberately narrow: the moment such a file gains a `#[tokio::test]` or a
/// `block_on` it stops being a harness and the guard demands coverage again.
fn is_db_harness_only(source: &str) -> bool {
    let code = strip_line_comments(source);
    !code.contains("#[tokio::test") && !code.contains("block_on")
}

// ── Allowlist: DB-gated tests without a covering manifest row ──────────────
//
// Keyed `core:<module>` / `plugin:<file-stem>`. Every entry carries a reason.
// Issue #1799 wired every suite that passes against Docker Postgres. What is
// left is either debt with an owner, or a suite that CI does not run by
// design. `docs/testing/ci-db-suite-allowlist.md` lists each entry with its
// owner. A reason that `BY_DESIGN_REASONS` does not list is debt. An entry
// that a covering row makes stale must go.
// This guard PROVED it bites: during development `nd_block_tests` was left
// out and the guard failed naming it (the TDD red step).

// mcp_tools_integration / webhook_durable_integration: paved-path DB tests
// (autumn_web::test::TestDb + run_pending(MIGRATIONS)) that are feature-gated
// AND have every test #[ignore]d, so no CI run can execute them. Issue #1959
// tracks the fix.
const ALLOWLIST_MCP_IGNORED_REASON: &str = "mcp-feature-gated (only `compileonly` in the manifest) AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness) — no CI run can execute it; tracked in #1959";
const ALLOWLIST_KAFKA_BROKER_REASON: &str = "kafka-feature-gated: DOES run in CI, via a dedicated Linux-only \
     ci.yml step (it needs an apt libcurl/cmake install first, and the manifest's compile mode would try to build \
     vendored librdkafka on macOS/Windows). Not a coverage gap — see the `Run plugin Kafka broker connector tests` step.";
const ALLOWLIST_WEBHOOKS_IGNORED_REASON: &str = "webhooks-feature-gated — not run in CI (no manifest row) — AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness); tracked in #1959";
const ALLOWLIST_CHAOS_REASON: &str = "chaos-feature-gated (issue #940): not in the manifest's `test` job. \
     The `chaos` feature is off by default, and the seeded sweep is slow, so each PR does not run it. \
     It runs only in the nightly and manual job in .github/workflows/chaos.yml, with at least 5 seeds. \
     `chaos_workflow_runs_the_chaos_suite_nightly` checks that claim. \
     .github/workflows/chaos-watchdog.yml opens an issue when no scheduled run succeeds in 48 h. \
     Before the fix for issue #1790, chaos.yml did not parse, and this suite never ran. Not a coverage gap.";
const ALLOWLIST_PERF_EVIDENCE_REASON: &str = "manual pg_stat_statements perf-evidence generator (issue #1620): its \
     one test is #[ignore]d by design, run by hand per docs/performance-outbox-start-relay.md's `Reproduce` \
     section against a real local Postgres — no CI run should execute it automatically. Not a coverage gap.";
const ALLOWLIST_EVIDENCE_HARNESS_REASON: &str = "single-purpose evidence-capture harness (Ledger, \
     issue #1272) — its only test is #[ignore]d by design (500k-row fixture, VACUUM FULL); a manifest \
     row would add a CI step that compiles it but runs nothing. Invoked manually with --ignored.";

const ALLOWLIST: &[(&str, &str)] = &[
    // ── core (autumn-harvest/tests/integration) ──
    (
        "core:audit_log_unexported_idx_write_cost_perf",
        ALLOWLIST_EVIDENCE_HARNESS_REASON,
    ),
    ("core:chaos_tests", ALLOWLIST_CHAOS_REASON),
    // ── plugin (autumn-harvest-plugin/tests) ──
    (
        "plugin:connector_kafka_broker",
        ALLOWLIST_KAFKA_BROKER_REASON,
    ),
    ("plugin:mcp_tools_integration", ALLOWLIST_MCP_IGNORED_REASON),
    (
        "plugin:outbox_start_relay_perf",
        ALLOWLIST_PERF_EVIDENCE_REASON,
    ),
    (
        "plugin:webhook_durable_integration",
        ALLOWLIST_WEBHOOKS_IGNORED_REASON,
    ),
    // `webhook_receiver_integration` is intentionally absent: its current-thread
    // `TestApp::plugin` deadlock was fixed (multi-thread flavor) and its tests
    // un-ignored, so it is now wired to a covering `linux` manifest row and runs
    // for real against Docker Postgres in CI.
];

/// Hard ratchet: the cap must equal `ALLOWLIST.len()` (issue #1799).
///
/// When you wire a suite, remove its entry and lower this number. To add an
/// entry, raise this number in the same change and give the reason there.
/// A cap above the length would let the list grow back without review.
///
/// History: 77, then 74. Three test-harness bugs were fixed, and three
/// suites were wired: `core:workflow_retry_tests`,
/// `core:completion_callback_tests` and `core:event_batch_tests`. A
/// whole-module row replaced the `::workflow_typed` sub-filter of the first.
/// Then 73 with `plugin:workflow_reachability_integration` (issue #700).
/// Then 75 with `core:chaos_tests` (issue #940), which runs nightly in
/// `chaos.yml`.
/// Then 55: issue #1799 wired five suites. It also removed three entries
/// for suites that CI already ran.
/// Then 6: issue #1799 wired the other 49 suites, 27 core and 22 plugin.
/// Four of them needed a test fix first.
const ALLOWLIST_MAX_LEN: usize = 6;

fn allowlisted(key: &str) -> bool {
    ALLOWLIST.iter().any(|&(k, _)| k == key)
}

// ── Manifest parsing (structured — columns already split) ───────────────────

/// One manifest record: `osclass crate target features filter`.
struct SuiteRow {
    /// `linux` | `linuxpart` | `allos` | `compileonly`.
    osclass: String,
    /// `autumn-harvest` | `autumn-harvest-plugin` | `autumn-harvest-redis` |
    /// `standalone-runner`.
    krate: String,
    /// The `--test <target>` binary (core suites use `integration`).
    target: String,
    /// Comma-list of features, or `-`.
    features: String,
    /// Positional filter after `--`, or `-` for the whole target.
    filter: String,
}

impl SuiteRow {
    /// Rows that CREDIT a suite as covered.
    ///
    /// `linuxpart` deliberately does NOT: it re-runs a suite against the
    /// opt-in partitioned layout (issue #958), which is *additional* evidence,
    /// never a substitute for running against the layout every deployment
    /// actually has. A suite listed only as `linuxpart` would otherwise satisfy
    /// this guard while never running on the default layout at all — the exact
    /// silently-never-run gap the guard exists to catch, in a new disguise.
    fn runs(&self) -> bool {
        self.osclass == "linux" || self.osclass == "allos"
    }

    /// Rows that EXECUTE in CI, whichever layout they execute against.
    fn executes(&self) -> bool {
        self.runs() || self.osclass == "linuxpart"
    }

    /// Runs against a live Docker Postgres, so the `db` default feature is in
    /// play. `linuxpart` is `linux` with `HARVEST_TEST_PARTITIONED=1` — the
    /// same suite, re-run against the opt-in partitioned `harvest_events`
    /// layout (issue #958, AC2).
    fn is_live_db(&self) -> bool {
        self.osclass == "linux" || self.osclass == "linuxpart"
    }

    /// The `--features` tokens as a set (`-` ⇒ empty).
    fn feature_set(&self) -> BTreeSet<&str> {
        if self.features == "-" {
            BTreeSet::new()
        } else {
            self.features.split(',').collect()
        }
    }
}

fn parse_manifest() -> Vec<SuiteRow> {
    // Fail-closed value allowlists: an unknown `osclass` routes a suite to NO
    // run-mode (`runs()`/`do_compile` both ignore it) → a silently-never-run
    // suite, exactly the gap this guard exists to catch; an unknown `crate`
    // would never match a coverage lookup. Reject either as a typo.
    // extend this set when a new crate/osclass is introduced.
    const VALID_OSCLASS: &[&str] = &["linux", "linuxpart", "allos", "compileonly"];
    const VALID_CRATE: &[&str] = &[
        "autumn-harvest",
        "autumn-harvest-plugin",
        "autumn-harvest-redis",
        // Issue #1615: the example's live-Postgres acceptance suite.
        "standalone-runner",
    ];
    let mut out = Vec::new();
    for (n, line) in MANIFEST.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        let cols: Vec<&str> = t.split_whitespace().collect();
        assert_eq!(
            cols.len(),
            5,
            "manifest line {} must have exactly 5 whitespace-separated columns \
             (osclass crate target features filter), got {}: {t:?}",
            n + 1,
            cols.len()
        );
        // Fail-closed value checks (allowlists hoisted above the loop).
        assert!(
            VALID_OSCLASS.contains(&cols[0]),
            "manifest line {} has unknown osclass {:?} (expected one of {VALID_OSCLASS:?})",
            n + 1,
            cols[0]
        );
        assert!(
            VALID_CRATE.contains(&cols[1]),
            "manifest line {} has unknown crate {:?} (expected one of {VALID_CRATE:?})",
            n + 1,
            cols[1]
        );
        out.push(SuiteRow {
            osclass: cols[0].to_string(),
            krate: cols[1].to_string(),
            target: cols[2].to_string(),
            features: cols[3].to_string(),
            filter: cols[4].to_string(),
        });
    }
    out
}

// ── Core coverage ───────────────────────────────────────────────────────────

/// A core `integration` submodule is covered when one executing row meets
/// three conditions. The row is `linux` or `allos` and enables `db`. It
/// enables every other feature in `required`, so the module compiles. Its
/// filter selects the whole module (see [`filter_runs_whole_module`]).
///
/// `autumn-harvest` has `default = ["db", "unified-dag-execution", "tls"]`; the runner
/// keeps defaults for `linux`/`linuxpart` integration rows (Docker Postgres) and strips them
/// (`--no-default-features`) for `allos` integration rows (no live DB). So a
/// `linux` integration row always has `db`; an `allos` integration row has it
/// only if it lists it explicitly. `testing` is never a default, so it must be
/// listed regardless of osclass.
fn core_covers(rows: &[SuiteRow], module: &str, required: &BTreeSet<String>) -> bool {
    let mut with_db = required.clone();
    with_db.insert("db".to_string());
    core_module_executes(rows, module, &with_db)
}

/// True when a row with `filter` runs every test in `module`.
///
/// libtest matches a filter as a substring of the full test path
/// (`module::test_fn`). A filter that occurs in the module name thus selects
/// the whole module. A `module::test` filter can select a slice. A module
/// name has no `::`, so such a filter never credits the whole module. `-`
/// selects the whole target.
fn filter_runs_whole_module(filter: &str, module: &str) -> bool {
    filter == "-" || module.contains(filter)
}

// ── mod.rs parsing: (module, cfg line) ──────────────────────────────────────

struct CoreModule {
    name: String,
    /// The `#[cfg(...)]` line above `mod name;`, or an empty string.
    cfg: String,
}

fn parse_core_modules() -> Vec<CoreModule> {
    let mut out = Vec::new();
    let mut pending_cfg: Option<String> = None;
    for line in CORE_MOD_RS.lines() {
        let t = line.trim();
        if t.starts_with("#[cfg(") {
            pending_cfg = Some(t.to_string());
            continue;
        }
        if let Some(rest) = t.strip_prefix("mod ") {
            let name = rest.trim_end_matches(';').trim().to_string();
            let cfg = pending_cfg.take().unwrap_or_default();
            out.push(CoreModule { name, cfg });
        } else if !t.is_empty() {
            // Any non-cfg, non-mod line clears a dangling cfg.
            pending_cfg = None;
        }
    }
    out
}

// ── Plugin coverage ─────────────────────────────────────────────────────────

/// Features required to even compile a plugin test file (from a top-level
/// `#![cfg(feature = "X")]`), so a covering run must carry them.
fn plugin_required_features(source: &str) -> Vec<String> {
    let mut feats = Vec::new();
    for line in source.lines() {
        let t = line.trim();
        if let Some(idx) = t.find("#![cfg(") {
            let seg = &t[idx..];
            for feat in [
                "webhooks",
                "mcp",
                "metrics",
                "unified-dag-execution",
                "dev-runtime",
            ] {
                let needle = format!("feature = \"{feat}\"");
                if seg.contains(&needle) {
                    feats.push(feat.to_string());
                }
            }
        }
    }
    feats
}

/// A plugin test file is covered iff an executing (`linux`/`allos`) manifest row
/// targets it whole (`filter == "-"` — a positional filter would slice it, the
/// mirror of the core partial-filter guard) and lists every required feature.
/// `compileonly` rows never cover.
fn plugin_covered(rows: &[SuiteRow], stem: &str, required: &[String]) -> bool {
    rows.iter().any(|r| {
        r.krate == "autumn-harvest-plugin" && r.target == stem && r.runs() && r.filter == "-" && {
            let feats = r.feature_set();
            required.iter().all(|f| feats.contains(f.as_str()))
        }
    })
}

// ── Paths ───────────────────────────────────────────────────────────────────

fn core_integration_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/integration")
}

fn plugin_tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../autumn-harvest-plugin/tests")
}

fn read_source(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

// ── Self-test: the manifest parser must find known rows ─────────────────────

#[test]
fn manifest_parser_finds_known_rows() {
    let rows = parse_manifest();
    assert!(
        !rows.is_empty(),
        "parsed no manifest rows — parser or manifest format broke"
    );

    // Known core `linux` integration filters.
    let core_filters: BTreeSet<&str> = rows
        .iter()
        .filter(|r| r.krate == "autumn-harvest" && r.target == "integration" && r.runs())
        .map(|r| r.filter.as_str())
        .collect();
    for expected in [
        "integration_e2e",
        "force_fail",
        "typed_workflow_failure_tests",
    ] {
        assert!(
            core_filters.contains(expected),
            "self-test: expected a core `integration` row filtered on {expected}, \
             found {core_filters:?}"
        );
    }

    // Known plugin `linux` targets.
    let plugin_targets: BTreeSet<&str> = rows
        .iter()
        .filter(|r| r.krate == "autumn-harvest-plugin" && r.runs())
        .map(|r| r.target.as_str())
        .collect();
    for expected in [
        "api_scheduler_integration",
        "ui_integration",
        "query_integration",
    ] {
        assert!(
            plugin_targets.contains(expected),
            "self-test: expected a plugin run row for {expected}, found {plugin_targets:?}"
        );
    }
    assert!(
        plugin_targets.len() >= 15,
        "self-test: expected ≥15 plugin run targets, found {} — parser likely broke",
        plugin_targets.len()
    );
}

// ── A `compileonly` row is never credited as covered ─────────────────────────

#[test]
fn compileonly_row_is_not_credited_as_covered() {
    let rows = parse_manifest();
    // `mcp_tools_integration` is `compileonly` in the manifest — it must not be
    // treated as covered.
    assert!(
        rows.iter()
            .any(|r| r.target == "mcp_tools_integration" && r.osclass == "compileonly"),
        "expected `mcp_tools_integration` to be a `compileonly` manifest row"
    );
    assert!(
        !plugin_covered(&rows, "mcp_tools_integration", &[]),
        "a `compileonly` target must NOT be credited as covered"
    );
    // Sanity: a genuine `linux` run target IS covered.
    assert!(plugin_covered(&rows, "api_scheduler_integration", &[]));
}

// ── A `module::filter` sub-selection must not credit the whole module ────────

#[test]
fn subfilter_does_not_credit_whole_module() {
    // The exact manifest shape that would mask 6 of `workflow_retry_tests`' 9 DB
    // tests: a partial `module::test` filter.
    let sub = vec![SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "-".into(),
        filter: "workflow_retry_tests::workflow_typed".into(),
    }];
    assert!(
        !core_covers(&sub, "workflow_retry_tests", &feats(&[])),
        "a `module::test` sub-filter must NOT credit the whole module as covered"
    );
    assert!(
        !core_module_executes(&sub, "workflow_retry_tests", &feats(&[])),
        "the feature-gate guard must not credit a sub-filter either"
    );

    // Whereas a whole-module filter DOES cover it.
    let whole = vec![SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "-".into(),
        filter: "workflow_retry_tests".into(),
    }];
    assert!(
        core_covers(&whole, "workflow_retry_tests", &feats(&[])),
        "a whole-module filter must credit the module"
    );
}

/// A feature set built from string literals.
fn feats(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|f| (*f).to_string()).collect()
}

// ── A row covers a module only when it enables every feature gate ───────────

/// A row without a module's feature compiles the module out, so it runs zero
/// tests. Such a row must not credit the module. Otherwise the stale rule
/// would force the removal of a valid allowlist entry.
#[test]
fn row_without_a_feature_gate_does_not_cover_the_module() {
    let plain = vec![SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "-".into(),
        filter: "chaos_tests".into(),
    }];
    assert!(
        !core_covers(&plain, "chaos_tests", &feats(&["chaos"])),
        "a row without `chaos` runs no `chaos_tests` test"
    );
    let gated = vec![SuiteRow {
        features: "chaos".into(),
        ..plain.into_iter().next().unwrap()
    }];
    assert!(core_covers(&gated, "chaos_tests", &feats(&["chaos"])));
}

// ── A filter credits every module that libtest runs with it ─────────────────

/// libtest matches a filter as a substring of the full test path. A
/// `pause_tests` row thus runs every test in `scheduler_auto_pause_tests`.
/// The guard must credit what CI runs, so it matches by substring too.
#[test]
fn substring_filter_credits_every_module_it_runs() {
    let rows = vec![SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "-".into(),
        filter: "pause_tests".into(),
    }];
    assert!(core_covers(
        &rows,
        "scheduler_auto_pause_tests",
        &feats(&[])
    ));
    assert!(core_module_executes(
        &rows,
        "scheduler_auto_pause_tests",
        &BTreeSet::new()
    ));
    assert!(
        !core_covers(&rows, "pause_test_helpers", &feats(&[])),
        "a module whose name does not contain the filter is not run"
    );
}

// ── An `allos` core `integration` row without `db` does not cover a db module ─

#[test]
fn allos_row_without_db_does_not_cover_a_db_module() {
    // Mirrors the real `allos autumn-harvest integration testing -` replayer row:
    // it runs `--no-default-features --features testing`, so it has NO db and
    // must not credit a db-gated module.
    let allos_no_db = vec![SuiteRow {
        osclass: "allos".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "testing".into(),
        filter: "-".into(),
    }];
    assert!(
        !core_covers(&allos_no_db, "some_db_module", &feats(&[])),
        "an allos row without `db` (--no-default-features) must not cover a db-gated module"
    );
    // A `linux` row (defaults on ⇒ db) covers it.
    let linux = vec![SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "-".into(),
        filter: "-".into(),
    }];
    assert!(core_covers(&linux, "some_db_module", &feats(&[])));
}

// ── Fail-CLOSED classifier & honesty about env-gated / no-DB HTTP tests ──────

#[test]
fn paved_path_db_tests_are_classified_and_flagged_all_ignored() {
    let dir = plugin_tests_dir();
    // These spin a real container via `autumn_web::test::TestDb` +
    // `run_pending(MIGRATIONS)` — the paved path the original three-token list
    // missed. They must now classify as DB tests, and each is fully `#[ignore]`d.
    // (`webhook_receiver_integration` was un-ignored + wired to a `linux`
    // manifest row, so it no longer belongs in this all-ignored list.)
    for stem in ["mcp_tools_integration", "webhook_durable_integration"] {
        let src = read_source(&dir.join(format!("{stem}.rs")));
        assert!(
            needs_live_db(&src),
            "{stem} uses the TestDb/run_pending paved DB path and must be classified as DB-gated"
        );
        assert!(
            all_tests_ignored(&src),
            "{stem}: all its tests are #[ignore]d — a run could not execute it"
        );
    }
}

#[test]
fn shared_fixture_callers_are_classified_as_db_gated() {
    let dir = core_integration_dir();
    // These suites build no container of their own. They call the shared
    // `integration_e2e` fixture, so only the helper name identifies them.
    for stem in ["dispatch_tests", "ctx_info_tests"] {
        let src = read_source(&dir.join(format!("{stem}.rs")));
        assert!(
            needs_live_db(&src),
            "{stem} calls the shared database fixture and must classify as DB-gated"
        );
    }
}

#[test]
fn all_tests_ignored_counts_code_and_not_prose() {
    let source = "//! - [`zz_evidence`] -- `#[ignore]`d prose.\n                  #[tokio::test]\nasync fn live() {}\n                  #[tokio::test]\n#[ignore = \"slow\"]\nasync fn evidence() {}\n";
    assert!(
        !all_tests_ignored(source),
        "a doc comment that names an ignored case must not hide the live case"
    );
}

#[test]
fn env_gated_and_no_db_http_tests_are_not_classified() {
    let dir = plugin_tests_dir();
    // `status_summary_localpg` is a no-op unless `DATABASE_URL` is set and only
    // mentions `testcontainers` in prose — it must stay excluded (verifies the
    // deliberate DATABASE_URL-only exclusion holds after broadening the tokens).
    // The two `*_http_tests` are genuinely no-DB harnesses whose only container
    // reference is a `//!` pointer to their sibling suite.
    for stem in [
        "status_summary_localpg",
        "mcp_tools_http_tests",
        "webhook_receiver_http_tests",
    ] {
        let src = read_source(&dir.join(format!("{stem}.rs")));
        assert!(
            !needs_live_db(&src),
            "{stem} must NOT be classified as needing a live DB (env-gated / no-DB; \
             its container tokens live only in comments)"
        );
    }
}

#[test]
fn db_harness_without_async_tests_is_not_treated_as_a_suite() {
    // A shared harness: real DB machinery, but nothing that could execute it.
    let harness = "use testcontainers_modules::postgres::Postgres;\npub async fn setup() { Postgres::default(); }\n#[cfg(test)]\nmod t { #[test] fn pure_math() { assert!(true); } }";
    assert!(
        needs_live_db(harness),
        "the harness genuinely carries live-DB machinery"
    );
    assert!(
        is_db_harness_only(harness),
        "no #[tokio::test] and no block_on ⇒ it cannot run a DB test itself"
    );

    // The moment it gains an async test it IS a suite and must be covered.
    let suite = format!("{harness}\n#[tokio::test] async fn real_db_test() {{}}");
    assert!(
        !is_db_harness_only(&suite),
        "a #[tokio::test] makes it a suite again — the guard must demand a row"
    );

    // ...and likewise for a sync test that drives the runtime explicitly.
    let block_on_suite = format!("{harness}\n#[test] fn t() {{ rt.block_on(async {{}}); }}");
    assert!(
        !is_db_harness_only(&block_on_suite),
        "an explicit block_on makes it a suite again"
    );
}

#[test]
fn claim_bench_support_is_classified_as_a_harness_not_a_suite() {
    // The concrete case this rule exists for (issue #786): the claim/enqueue
    // benchmark harness is shared by `benches/claim_bench.rs` and the wired
    // `claim_budget_tests` suite. Its DB code is covered transitively.
    let src = read_source(&core_integration_dir().join("claim_bench_support.rs"));
    assert!(needs_live_db(&src), "it does carry live-DB machinery");
    assert!(
        is_db_harness_only(&src),
        "claim_bench_support must declare no async/DB-driving test of its own —          if it grows one, give it a manifest row instead of relaxing this"
    );
}

/// Core suites that issue #1799 wired, with the features each row needs.
///
/// The first four are resilience suites the issue names. The fifth,
/// `erase_payloads_integration`, is in [`ISSUE_1799_PLUGIN`]. The other 27
/// are the second batch. Each must have a covering row and no allowlist entry.
const ISSUE_1799_CORE: &[(&str, &[&str])] = &[
    ("scheduler_ha_tests", &[]),
    ("poison_pill_tests", &[]),
    ("signal_tests", &[]),
    ("replayer_integration_tests", &["testing"]),
    ("audit_tests", &[]),
    ("build_routing_tests", &[]),
    ("cache_delta_load_tests", &[]),
    ("child_policy_tests", &[]),
    ("cross_workflow_cancel_tests", &[]),
    ("debounce_tests", &[]),
    ("delayed_start_tests", &[]),
    ("legal_hold_tests", &[]),
    ("payload_offload_db_tests", &[]),
    ("queue_fairness_tests", &[]),
    ("replay_canary_tests", &["testing"]),
    ("retry_now_tests", &[]),
    ("schedule_decisions", &[]),
    ("schedule_to_close_tests", &[]),
    ("schedule_update_tests", &[]),
    ("scheduled_time_tests", &["testing"]),
    ("scheduler_bounded_runs_tests", &[]),
    ("scheduler_carryover_tests", &["testing"]),
    ("scheduler_catchup_tests", &[]),
    ("signal_with_start_tests", &[]),
    ("sla_breach_tests", &[]),
    ("sticky_routing_tests", &[]),
    ("telemetry_span_tests", &[]),
    ("throttle_tests", &[]),
    ("transactional_activity_tests", &["testing"]),
    ("typed_stubs_tests", &[]),
    ("updt_with_start_tests", &[]),
];

/// Plugin suites that issue #1799 wired.
const ISSUE_1799_PLUGIN: &[&str] = &[
    "erase_payloads_integration",
    "archival_integration",
    "build_routing_ui_integration",
    "dag_retry_integration",
    "event_batch_integration",
    "external_handoffs_integration",
    "history_export_integration",
    "outbox_integration",
    "preflight_integration",
    "replay_canary_integration",
    "retirement_check_integration",
    "scaling_api_tests",
    "schedule_update_integration",
    "shard_health_integration",
    "signal_with_start_integration",
    "stalled_workflow_tests",
    "telemetry_propagation_tests",
    "usage_integration",
    "version_usage_integration",
    "workflow_count_integration",
    "workflow_filter_integration",
    "workflow_history_pagination_integration",
    "workflow_result_integration",
];

/// The suites that issue #1799 wired must keep a covering row. Removing a row
/// fails this test. To move a suite back to the allowlist, also edit this
/// list, so review sees it.
#[test]
fn issue_1799_suites_have_covering_rows() {
    let rows = parse_manifest();
    for &(module, required) in ISSUE_1799_CORE {
        assert!(
            core_covers(&rows, module, &feats(required)),
            "core:{module} needs a covering `linux` manifest row"
        );
        assert!(
            !allowlisted(&format!("core:{module}")),
            "core:{module} is wired; remove its ALLOWLIST entry and lower ALLOWLIST_MAX_LEN"
        );
    }
    for &stem in ISSUE_1799_PLUGIN {
        let required =
            plugin_required_features(&read_source(&plugin_tests_dir().join(format!("{stem}.rs"))));
        assert!(
            plugin_covered(&rows, stem, &required),
            "plugin:{stem} needs a covering `linux` manifest row"
        );
        assert!(
            !allowlisted(&format!("plugin:{stem}")),
            "plugin:{stem} is wired; remove its ALLOWLIST entry and lower ALLOWLIST_MAX_LEN"
        );
    }
}

/// Plugin suites that issue #1959 wired.
const ISSUE_1959_PLUGIN: &[&str] = &["mcp_tools_integration", "webhook_durable_integration"];

/// The suites that issue #1959 wired must run in CI and must keep their two
/// fixes. On a current-thread runtime, `TestApp::plugin` deadlocks. `TestDb`
/// starts Postgres 11, where the worker claim query fails on `MATERIALIZED`.
#[test]
fn issue_1959_suites_run_in_ci() {
    let rows = parse_manifest();
    for &stem in ISSUE_1959_PLUGIN {
        let src = read_source(&plugin_tests_dir().join(format!("{stem}.rs")));
        let code = strip_line_comments(&src);
        assert!(
            plugin_covered(&rows, stem, &plugin_required_features(&src)),
            "plugin:{stem} needs a covering `linux` manifest row"
        );
        assert!(
            !all_tests_ignored(&src),
            "plugin:{stem} must not `#[ignore]` every test"
        );
        assert!(
            !allowlisted(&format!("plugin:{stem}")),
            "plugin:{stem} is wired; remove its ALLOWLIST entry and lower ALLOWLIST_MAX_LEN"
        );
        assert!(
            !code.contains("#[tokio::test]"),
            "plugin:{stem} must use the multi-thread flavor, not a current-thread runtime"
        );
        assert!(
            !code.contains("TestDb::"),
            "plugin:{stem} must pin a supported Postgres, not the `TestDb` default (Postgres 11)"
        );
    }
}

#[test]
fn claim_budget_gate_has_a_covering_manifest_row() {
    // The gate is the whole point of issue #786; it must actually run in CI.
    let rows = parse_manifest();
    assert!(
        core_covers(&rows, "claim_budget_tests", &feats(&[])),
        "the claim-path budget gate must have a covering `linux` manifest row —          a performance gate that never runs is not a gate"
    );
}

/// Issue #1615. Each test target of the `standalone-runner` example runs
/// from a manifest row. The guard above scans only the core and plugin
/// test directories, so a deleted row would go unseen.
#[test]
fn standalone_runner_test_targets_have_a_running_row() {
    let rows = parse_manifest();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples/standalone-runner/tests");
    let targets: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "rs").then(|| path.file_stem()?.to_str().map(str::to_owned))?
        })
        .collect();
    assert!(!targets.is_empty(), "the example has an acceptance suite");
    for target in targets {
        assert!(
            rows.iter().any(|r| r.krate == "standalone-runner"
                && r.target == target
                && r.osclass == "linux"
                && r.filter == "-"),
            "examples/standalone-runner/tests/{target}.rs needs a `linux standalone-runner {target}` row"
        );
    }
}

#[test]
fn strip_line_comments_drops_prose_container_tokens() {
    let src = "//! see the testcontainers suite\nlet x = 1; // TestDb reference in prose\ncode();";
    let code = strip_line_comments(src);
    assert!(
        !code.contains("testcontainers"),
        "leading `//!` line must be dropped"
    );
    // A trailing inline comment survives (its line isn't a leading comment), but
    // that's fine: real code markers are function calls / types, not prose.
    assert!(code.contains("code();"));
}

#[test]
fn core_required_features_reads_mod_cfg_and_file_cfg() {
    let file_gate = "#![cfg(all(feature = \"db\", feature = \"testing\"))]\nfn a() {}";
    assert_eq!(core_required_features("", file_gate), feats(&["testing"]));
    let mod_gate = "#[cfg(all(feature = \"testing\", feature = \"db\"))]";
    assert_eq!(core_required_features(mod_gate, ""), feats(&["testing"]));
    assert_eq!(
        core_required_features("", "#![cfg(feature = \"db\")]\n"),
        feats(&[])
    );
    // A mid-file `#[cfg(feature = "testing")]` on an item is not a file gate.
    assert_eq!(
        core_required_features("", "use x;\n#[cfg(feature = \"testing\")]\nfn a() {}"),
        feats(&[])
    );
}

// ── The manifest must actually be executed by CI ─────────────────────────────

#[test]
fn ci_yaml_invokes_the_runner_against_the_manifest() {
    // Without this, deleting a runner step (but keeping the manifest) would make
    // the guard read a manifest CI never executes → green-but-not-run.
    for needle in [
        "run-suites.sh compile",
        "run-suites.sh run allos",
        "run-suites.sh run linux",
    ] {
        assert!(
            CI_YAML.contains(needle),
            "ci.yml must invoke `bash .github/ci/{needle}` — the manifest is executed by the \
             runner script, and a missing invocation would silently stop running suites while \
             this guard stayed green"
        );
    }
    assert!(
        RUN_SCRIPT.contains("integration-suites.txt"),
        "the runner script (.github/ci/run-suites.sh) must reference the manifest \
         `integration-suites.txt`, else it would execute some other list"
    );
}

// ── The `merge=union` manifest must stay sorted + unique ─────────────────────

#[test]
fn manifest_is_sorted_and_unique() {
    let rows: Vec<&str> = MANIFEST
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            !t.is_empty() && !t.starts_with('#')
        })
        .collect();
    let mut want = rows.clone();
    want.sort_unstable();
    want.dedup();
    assert_eq!(
        rows, want,
        "\n.github/ci/integration-suites.txt data lines are not sorted+unique — a merge=union \
         artifact (two PRs' additions interleaved or duplicated). This is a benign, one-command \
         fix, never a conflict. Re-sort the DATA lines (keep the header comment on top), e.g.:\n\
         \x20 LC_ALL=C sort -o .github/ci/integration-suites.txt \\\n\
         \x20   <(grep -E '^[[:space:]]*#' .github/ci/integration-suites.txt) \\\n\
         \x20   <(grep -vE '^[[:space:]]*(#|$)' .github/ci/integration-suites.txt | LC_ALL=C sort -u)"
    );
}

// ── The guard ───────────────────────────────────────────────────────────────

#[test]
fn every_db_gated_test_has_a_ci_run_step_or_is_allowlisted() {
    let rows = parse_manifest();

    let core_dir = core_integration_dir();
    let plugin_dir = plugin_tests_dir();

    let mut uncovered: Vec<String> = Vec::new();
    // Track which allowlist keys correspond to a real, still-DB-gated test, so
    // stale entries (test deleted or de-DB-ified) are surfaced.
    let mut live_db_keys: BTreeSet<String> = BTreeSet::new();
    // Keys that a manifest row covers. An allowlist entry for one is stale.
    let mut covered_keys: BTreeSet<String> = BTreeSet::new();

    // Core: mod.rs is the source of truth for which suites exist + their cfg.
    for m in parse_core_modules() {
        let path = core_dir.join(format!("{}.rs", m.name));
        if !path.is_file() {
            continue; // e.g. a module whose file was renamed; not our concern here
        }
        if SELF_EXCLUDE.contains(&m.name.as_str()) {
            continue;
        }
        let src = read_source(&path);
        if !needs_live_db(&src) {
            continue;
        }
        // A shared harness (DB machinery, no test that could run it) is covered
        // transitively by the suite that consumes it; demanding its own manifest
        // row would add a CI step that executes nothing.
        if is_db_harness_only(&src) {
            continue;
        }
        let key = format!("core:{}", m.name);
        live_db_keys.insert(key.clone());
        // The covering row must enable every feature the module needs. They
        // come from the `mod.rs` cfg line and the file's own `#![cfg]`.
        let required = core_required_features(&m.cfg, &src);
        let covered = core_covers(&rows, &m.name, &required) && !all_tests_ignored(&src);
        if covered {
            covered_keys.insert(key);
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        let feats = if required.is_empty() {
            "-".to_string()
        } else {
            required.into_iter().collect::<Vec<_>>().join(",")
        };
        uncovered.push(format!(
            "{key} (add manifest line `linux  autumn-harvest  integration  {feats}  {}` to \
             .github/ci/integration-suites.txt)",
            m.name
        ));
    }

    // Plugin: each test file is its own target; enumerate the directory.
    let mut plugin_files: Vec<PathBuf> = std::fs::read_dir(&plugin_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", plugin_dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    plugin_files.sort();
    for path in plugin_files {
        let src = read_source(&path);
        if !needs_live_db(&src) {
            continue;
        }
        if is_db_harness_only(&src) {
            continue;
        }
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let key = format!("plugin:{stem}");
        live_db_keys.insert(key.clone());
        let req = plugin_required_features(&src);
        // A covering run can't credit an all-`#[ignore]`d target — it runs
        // nothing — so force it uncovered (→ must be allowlisted).
        let covered = plugin_covered(&rows, &stem, &req) && !all_tests_ignored(&src);
        if covered {
            covered_keys.insert(key);
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        let feats = if req.is_empty() {
            "-".to_string()
        } else {
            req.join(",")
        };
        uncovered.push(format!(
            "{key} (add manifest line `linux  autumn-harvest-plugin  {stem}  {feats}  -` to \
             .github/ci/integration-suites.txt)"
        ));
    }

    let keys: Vec<&str> = ALLOWLIST.iter().map(|&(k, _)| k).collect();
    let mut stale = stale_allowlist_keys(&keys, &live_db_keys, &covered_keys);
    stale.sort_unstable();

    assert!(
        uncovered.is_empty(),
        "these DB-gated integration tests have NO covering manifest row and are NOT allowlisted \
         — they compile in CI but never run against a live Postgres (the nd_block class of \
         invisible bug):\n  {}",
        uncovered.join("\n  ")
    );
    assert!(
        stale.is_empty(),
        "ALLOWLIST has stale entries (test deleted, no longer DB-gated, or now covered by a \
         manifest row) — remove them and lower ALLOWLIST_MAX_LEN:\n  {}",
        stale.join("\n  ")
    );
}

/// Allowlist keys that are dead debt. A key is stale when it names no live
/// DB-gated suite, or when a manifest row already covers that suite.
fn stale_allowlist_keys<'a>(
    keys: &[&'a str],
    live: &BTreeSet<String>,
    covered: &BTreeSet<String>,
) -> Vec<&'a str> {
    keys.iter()
        .copied()
        .filter(|k| !live.contains(*k) || covered.contains(*k))
        .collect()
}

#[test]
fn covered_or_dead_allowlist_keys_are_stale() {
    let live = feats(&["core:covered", "core:debt"]);
    let covered = feats(&["core:covered"]);
    let keys = ["core:covered", "core:debt", "core:deleted"];
    assert_eq!(
        stale_allowlist_keys(&keys, &live, &covered),
        ["core:covered", "core:deleted"],
        "a covered suite and a deleted suite are stale; a live, uncovered suite is debt"
    );
}

#[test]
fn allowlist_cap_equals_its_length() {
    assert_eq!(
        ALLOWLIST.len(),
        ALLOWLIST_MAX_LEN,
        "ALLOWLIST has {} entries but ALLOWLIST_MAX_LEN is {ALLOWLIST_MAX_LEN}. The cap must equal \
         the length, so the list cannot grow back. When you remove an entry, lower the cap to match. \
         To add an entry, raise the cap and give the reason in the same change.",
        ALLOWLIST.len()
    );
}

#[test]
fn allowlist_entries_are_unique() {
    let mut seen = BTreeSet::new();
    for &(k, _) in ALLOWLIST {
        assert!(seen.insert(k), "duplicate ALLOWLIST entry: {k}");
    }
}

// ── Tracking table: every allowlist entry has an owner or a reason (#1799) ──

/// The tracking table for the allowlist (issue #1799).
const TRACKING_DOC: &str = include_str!("../../../docs/testing/ci-db-suite-allowlist.md");

/// The owner value of a suite that CI does not run by design.
const WONT_WIRE: &str = "won't wire";

/// Reasons that mark a suite that CI does not run by design. Every other
/// reason is debt, so a new reason needs an owner until it is listed here.
const BY_DESIGN_REASONS: &[&str] = &[
    ALLOWLIST_EVIDENCE_HARNESS_REASON,
    ALLOWLIST_CHAOS_REASON,
    ALLOWLIST_KAFKA_BROKER_REASON,
    ALLOWLIST_PERF_EVIDENCE_REASON,
];

/// True when an allowlist entry with `reason` is debt that needs an owner.
fn entry_is_debt(reason: &str) -> bool {
    !BY_DESIGN_REASONS.contains(&reason)
}

/// One table row: the suite key, the owner and the reason.
struct TrackingRow {
    key: String,
    owner: String,
    reason: String,
}

/// Reads the rows between the table markers of [`TRACKING_DOC`].
fn parse_tracking_table(doc: &str) -> Vec<TrackingRow> {
    let begin = doc
        .find("<!-- allowlist-tracking:begin -->")
        .expect("the tracking doc must have a begin marker");
    let end = doc
        .find("<!-- allowlist-tracking:end -->")
        .expect("the tracking doc must have an end marker");
    doc[begin..end]
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("| `"))
        .map(|l| {
            let cols: Vec<&str> = l.trim_matches('|').split('|').map(str::trim).collect();
            assert_eq!(cols.len(), 3, "a tracking row has 3 columns: {l:?}");
            TrackingRow {
                key: cols[0].trim_matches('`').to_string(),
                owner: cols[1].to_string(),
                reason: cols[2].to_string(),
            }
        })
        .collect()
}

/// True when `owner` names a person (`@login`) or a tracking issue (`#123`).
/// A cell that says [`WONT_WIRE`] names no owner.
fn names_an_owner(owner: &str) -> bool {
    let is_login = |w: &str| {
        w.strip_prefix('@').is_some_and(|l| {
            !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        })
    };
    let is_issue = |w: &str| {
        w.strip_prefix('#')
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
    };
    !owner.contains(WONT_WIRE)
        && owner
            .split_whitespace()
            .map(|w| w.trim_matches(|c| matches!(c, '(' | ')' | ',')))
            .any(|w| is_login(w) || is_issue(w))
}

/// Issue #1799 asks for a table of the remaining suites. Each row has an
/// owner, or [`WONT_WIRE`] and a reason. The table and `ALLOWLIST` must
/// agree, so the table cannot go stale.
#[test]
fn tracking_table_lists_every_allowlist_entry_with_an_owner_or_reason() {
    let rows = parse_tracking_table(TRACKING_DOC);
    let table: BTreeSet<&str> = rows.iter().map(|r| r.key.as_str()).collect();
    let code: BTreeSet<&str> = ALLOWLIST.iter().map(|&(k, _)| k).collect();
    assert_eq!(
        rows.len(),
        table.len(),
        "the tracking table has a duplicate row"
    );
    assert_eq!(
        table, code,
        "docs/testing/ci-db-suite-allowlist.md must list each ALLOWLIST key once, and no other key"
    );
    for row in &rows {
        let &(_, reason) = ALLOWLIST.iter().find(|&&(k, _)| k == row.key).unwrap();
        assert!(!row.reason.is_empty(), "{}: the reason is empty", row.key);
        if entry_is_debt(reason) {
            assert!(
                names_an_owner(&row.owner),
                "{}: debt needs an `@login` or `#issue` owner, got {:?}",
                row.key,
                row.owner
            );
        } else {
            assert_eq!(
                row.owner, WONT_WIRE,
                "{}: CI does not run it by design, so its owner is {WONT_WIRE:?}",
                row.key
            );
        }
    }
}

#[test]
fn tracking_table_parser_reads_owner_and_reason() {
    let doc = "intro\n<!-- allowlist-tracking:begin -->\n| Suite | Owner | Reason |\n|---|---|---|\n| `core:a` | @someone (#12) | debt |\n| `plugin:b` | won't wire | manual |\n<!-- allowlist-tracking:end -->\n| `core:outside` | x | y |\n";
    let rows = parse_tracking_table(doc);
    assert_eq!(rows.len(), 2, "rows outside the markers do not count");
    assert_eq!(rows[0].key, "core:a");
    assert!(names_an_owner(&rows[0].owner));
    assert_eq!(rows[1].owner, WONT_WIRE);
    assert!(!names_an_owner(&rows[1].owner));
    assert_eq!(rows[1].reason, "manual");

    let padded = "<!-- allowlist-tracking:begin -->\n| `core:a` | #12 | debt |  \n<!-- allowlist-tracking:end -->";
    assert_eq!(
        parse_tracking_table(padded).len(),
        1,
        "trailing spaces do not add a column"
    );
}

#[test]
fn names_an_owner_needs_a_login_or_an_issue_number() {
    for owner in ["@madmax983", "#1959", "@madmax983 (#1959)"] {
        assert!(names_an_owner(owner), "{owner:?} names an owner");
    }
    for owner in ["#", "@", "(#)", "won't wire (#1959)", "TBD"] {
        assert!(!names_an_owner(owner), "{owner:?} names no owner");
    }
}

/// A new reason is debt until it is listed as by design. A forgotten list
/// edit then asks for an owner, not for [`WONT_WIRE`].
#[test]
fn a_new_reason_counts_as_debt() {
    assert!(entry_is_debt("a reason that no list names"));
    assert!(entry_is_debt(ALLOWLIST_MCP_IGNORED_REASON));
    assert!(!entry_is_debt(ALLOWLIST_CHAOS_REASON));
}

// ── Feature-gate coverage (DB or not) ───────────────────────────────────────

/// Features a core suite needs to compile, from its `mod.rs` cfg and its own
/// leading `#![cfg]`. `db` is left out: the DB guard above owns it.
fn core_required_features(mod_cfg: &str, source: &str) -> BTreeSet<String> {
    let mut text = mod_cfg.to_string();
    for line in source.lines() {
        let t = line.trim();
        if t.starts_with("#![cfg(") {
            text.push_str(t);
        }
    }
    let mut out = BTreeSet::new();
    let mut rest = text.as_str();
    while let Some(idx) = rest.find("feature = \"") {
        let after = &rest[idx + "feature = \"".len()..];
        let Some(end) = after.find('"') else { break };
        let feat = &after[..end];
        if feat != "db" {
            out.insert(feat.to_string());
        }
        rest = &after[end..];
    }
    out
}

/// The features one manifest row compiles the core `integration` target with.
///
/// A `linux` or `linuxpart` row keeps the crate defaults. An `allos` row
/// strips them (`--no-default-features`), so it has only what it lists.
fn core_row_features(row: &SuiteRow) -> BTreeSet<String> {
    let mut feats: BTreeSet<String> = row.feature_set().into_iter().map(str::to_string).collect();
    if row.is_live_db() {
        for default in ["db", "unified-dag-execution", "tls"] {
            feats.insert(default.to_string());
        }
    }
    feats
}

/// True when some executing row selects `module` whole and compiles it.
fn core_module_executes(rows: &[SuiteRow], module: &str, required: &BTreeSet<String>) -> bool {
    rows.iter().any(|r| {
        if r.krate != "autumn-harvest" || r.target != "integration" || !r.runs() {
            return false;
        }
        if !filter_runs_whole_module(&r.filter, module) {
            return false;
        }
        let feats = core_row_features(r);
        required.iter().all(|f| feats.contains(f))
    })
}

/// Core suites whose required features no manifest row enables, with the reason.
///
/// Each entry must still be gated and still be unexecuted, so this list only
/// shrinks. The DB guard above covers the `db` feature separately.
const FEATURE_GATE_EXEMPT: &[(&str, &str)] = &[(
    "chaos_tests",
    "chaos-feature-gated: runs only in the nightly and manual job in .github/workflows/chaos.yml",
)];

/// A suite behind a feature gate can compile in every CI job and still run in
/// none. The unified-DAG suites were such a gap: no row enabled both
/// `testing` and `unified-dag-execution`, so 31 `dag_unified_tests` never ran.
/// The DB guard above missed it because those suites need no database.
#[test]
fn every_feature_gated_core_suite_executes_in_some_row() {
    let rows = parse_manifest();
    let core_dir = core_integration_dir();
    let mut unexecuted = Vec::new();
    let mut gated = BTreeSet::new();
    let mut executing = BTreeSet::new();

    let mut pending_cfg = String::new();
    for line in CORE_MOD_RS.lines() {
        let t = line.trim();
        if t.starts_with("#[cfg(") {
            pending_cfg = t.to_string();
            continue;
        }
        let Some(rest) = t.strip_prefix("mod ") else {
            if !t.is_empty() {
                pending_cfg.clear();
            }
            continue;
        };
        let name = rest.trim_end_matches(';').trim().to_string();
        let cfg = std::mem::take(&mut pending_cfg);
        let path = core_dir.join(format!("{name}.rs"));
        if !path.is_file() || SELF_EXCLUDE.contains(&name.as_str()) {
            continue;
        }
        let src = read_source(&path);
        if is_db_harness_only(&src) || all_tests_ignored(&src) {
            continue;
        }
        let required = core_required_features(&cfg, &src);
        if required.is_empty() {
            continue;
        }
        gated.insert(name.clone());
        if core_module_executes(&rows, &name, &required) {
            executing.insert(name);
            continue;
        }
        if FEATURE_GATE_EXEMPT.iter().any(|(m, _)| *m == name) {
            continue;
        }
        let feats = required.into_iter().collect::<Vec<_>>().join(",");
        unexecuted.push(format!(
            "{name} (add manifest line `linux  autumn-harvest  integration  {feats}  {name}`)"
        ));
    }

    assert!(
        unexecuted.is_empty(),
        "these feature-gated core suites compile but no manifest row enables their features, \
         so CI never executes them:\n  {}",
        unexecuted.join("\n  ")
    );
    let stale: Vec<&str> = FEATURE_GATE_EXEMPT
        .iter()
        .map(|(m, _)| *m)
        .filter(|m| !gated.contains(*m) || executing.contains(*m))
        .collect();
    assert!(
        stale.is_empty(),
        "FEATURE_GATE_EXEMPT has entries that now execute or are no longer gated; remove them:\n  {}",
        stale.join("\n  ")
    );
}

#[test]
fn core_row_features_keep_defaults_only_on_live_db_rows() {
    let linux = SuiteRow {
        osclass: "linux".into(),
        krate: "autumn-harvest".into(),
        target: "integration".into(),
        features: "testing".into(),
        filter: "dag_unified_tests".into(),
    };
    let allos = SuiteRow {
        osclass: "allos".into(),
        ..linux_clone(&linux)
    };
    assert!(core_row_features(&linux).contains("unified-dag-execution"));
    assert!(!core_row_features(&allos).contains("unified-dag-execution"));
    let required: BTreeSet<String> = ["testing", "unified-dag-execution"]
        .into_iter()
        .map(str::to_string)
        .collect();
    assert!(core_module_executes(
        &[linux],
        "dag_unified_tests",
        &required
    ));
    assert!(!core_module_executes(
        &[allos],
        "dag_unified_tests",
        &required
    ));
}

fn linux_clone(row: &SuiteRow) -> SuiteRow {
    SuiteRow {
        osclass: row.osclass.clone(),
        krate: row.krate.clone(),
        target: row.target.clone(),
        features: row.features.clone(),
        filter: row.filter.clone(),
    }
}

// ── Every workflow must parse (issue #1790) ─────────────────────────────────

/// The repository root.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// Workflow files that this guard cites as proof that a suite runs.
///
/// The set is `ci.yml` plus each `.github/workflows/` path that an `ALLOWLIST`
/// or `FEATURE_GATE_EXEMPT` reason names. A new citation is checked with no
/// edit here.
fn cited_workflows() -> BTreeSet<String> {
    let mut out = BTreeSet::from([".github/workflows/ci.yml".to_string()]);
    let reasons = ALLOWLIST.iter().chain(FEATURE_GATE_EXEMPT).map(|&(_, r)| r);
    for reason in reasons {
        let mut rest = reason;
        while let Some(idx) = rest.find(".github/workflows/") {
            let tail = &rest[idx..];
            let end = tail
                .find(|c: char| !(c.is_ascii_alphanumeric() || "./-_".contains(c)))
                .unwrap_or(tail.len());
            out.insert(tail[..end].trim_end_matches('.').to_string());
            rest = &tail[end..];
        }
    }
    out
}

/// Parses workflow text as YAML 1.2, which keeps `on` as a string key.
///
/// `serde_yaml` also rejects a repeated mapping key. GitHub rejects that file
/// too, but `PyYAML` keeps the last value and hides the defect.
///
/// This is a YAML syntax check only. It does not check the GitHub workflow
/// schema, so an unknown key or a bad expression still passes. For
/// `chaos.yml`, the watchdog reports that case within about 2.5 days.
pub fn parse_workflow_text(text: &str) -> Result<serde_yaml::Value, String> {
    serde_yaml::from_str(text).map_err(|e| e.to_string())
}

/// Reads and parses one workflow. Panics with the path and the parse error.
pub fn parse_workflow(rel: &str) -> serde_yaml::Value {
    let text = read_source(&repo_root().join(rel));
    parse_workflow_text(&text).unwrap_or_else(|e| {
        panic!(
            "{rel} does not parse as YAML: {e}\nGitHub runs an unparsable workflow as a \
             zero-job failure and never fires its triggers. Quote a `run:` value that holds \
             `: ` (for example `chaos:: --`)."
        )
    })
}

/// The `run:` text of every step in every job of a parsed workflow.
pub fn workflow_run_commands(doc: &serde_yaml::Value) -> Vec<String> {
    let Some(jobs) = doc.get("jobs").and_then(serde_yaml::Value::as_mapping) else {
        return Vec::new();
    };
    jobs.values()
        .filter_map(|job| job.get("steps").and_then(serde_yaml::Value::as_sequence))
        .flatten()
        .filter_map(|step| step.get("run").and_then(serde_yaml::Value::as_str))
        .map(str::to_string)
        .collect()
}

/// The `cron` strings under a parsed workflow's `on.schedule`.
pub fn workflow_crons(doc: &serde_yaml::Value) -> Vec<String> {
    doc.get("on")
        .and_then(|on| on.get("schedule"))
        .and_then(serde_yaml::Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("cron").and_then(serde_yaml::Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Self-test: the parser rejects the defect classes that it must catch.
#[test]
fn workflow_parser_rejects_the_chaos_yml_defect_classes() {
    let unquoted = "jobs:\n  a:\n    steps:\n      - run: cargo test chaos:: -- --nocapture\n";
    assert!(
        parse_workflow_text(unquoted).is_err(),
        "an unquoted `run:` value that holds `:: ` must not parse"
    );
    let quoted = "jobs:\n  a:\n    steps:\n      - run: 'cargo test chaos:: -- --nocapture'\n";
    let doc = parse_workflow_text(quoted).expect("the quoted form must parse");
    assert_eq!(
        workflow_run_commands(&doc),
        ["cargo test chaos:: -- --nocapture"]
    );

    let repeated = "jobs:\n  a:\n    runs-on: x\n  a:\n    runs-on: y\n";
    assert!(
        parse_workflow_text(repeated).is_err(),
        "a repeated mapping key must not parse"
    );

    let scheduled = "on:\n  schedule:\n    - cron: \"17 4 * * *\"\njobs: {}\n";
    let doc = parse_workflow_text(scheduled).expect("a schedule must parse");
    assert_eq!(
        workflow_crons(&doc),
        ["17 4 * * *"],
        "`on` must stay a string key (YAML 1.2), not the boolean `true` (YAML 1.1)"
    );
}

/// An `ALLOWLIST` reason that cites a workflow claims that the workflow runs
/// the suite. That claim is false when the workflow does not parse. The
/// chaos suite stayed in that state from its first commit (issue #1790).
#[test]
fn every_workflow_parses_and_has_jobs() {
    let cited = cited_workflows();
    assert!(
        cited.contains(".github/workflows/chaos.yml"),
        "the `core:chaos_tests` reason must cite .github/workflows/chaos.yml; found {cited:?}"
    );
    let all = all_workflows();
    for rel in &cited {
        assert!(
            all.contains(rel),
            "{rel} is cited but is not a workflow file"
        );
    }
    for rel in &all {
        let doc = parse_workflow(rel);
        let jobs = doc.get("jobs").and_then(serde_yaml::Value::as_mapping);
        assert!(
            jobs.is_some_and(|j| !j.is_empty()),
            "{rel} must have a non-empty `jobs` mapping"
        );
    }
}

/// Every `.yml` or `.yaml` file in `.github/workflows/`, as a repository path.
///
/// The parse check covers all of them, not only the cited ones. An uncited
/// workflow that does not parse fails as silently as `chaos.yml` did.
fn all_workflows() -> BTreeSet<String> {
    let dir = repo_root().join(".github/workflows");
    std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("workflow dir entry").file_name())
        .filter_map(|name| name.into_string().ok())
        .filter(|name| {
            std::path::Path::new(name).extension().is_some_and(|ext| {
                ext.eq_ignore_ascii_case("yml") || ext.eq_ignore_ascii_case("yaml")
            })
        })
        .map(|name| format!(".github/workflows/{name}"))
        .collect()
}

/// `cargo test` flags that run no test, or less than the whole module.
pub const NO_FULL_RUN_FLAGS: &[&str] = &["--no-run", "--exact", "--skip", "--ignored", "--list"];

/// Shell text that joins a second command to the step, or runs the command
/// inside another one. Either can hide a failed `cargo test` or replace it.
pub const SHELL_OPERATORS: &[&str] = &[";", "&", "|", "\n", "`", "$("];

/// True when a job or step has no `if` and no `continue-on-error: true`.
///
/// An `if` can skip the step on the nightly. A `continue-on-error` makes a
/// failed suite look green to the watchdog, which counts successful runs.
pub fn ungated(node: &serde_yaml::Value) -> bool {
    let soft = node
        .get("continue-on-error")
        .is_some_and(|v| v.as_bool() != Some(false));
    node.get("if").is_none() && !soft
}

/// True when an ungated step in an ungated job runs the whole `chaos_tests`
/// module with the `chaos` feature.
///
/// The step must be one plain `cargo test` command. Text that only contains
/// the right arguments, such as an `echo`, runs no test.
fn runs_the_chaos_suite_unconditionally(doc: &serde_yaml::Value) -> bool {
    let Some(jobs) = doc.get("jobs").and_then(serde_yaml::Value::as_mapping) else {
        return false;
    };
    jobs.values()
        .filter(|job| ungated(job))
        .filter_map(|job| job.get("steps").and_then(serde_yaml::Value::as_sequence))
        .flatten()
        .filter(|step| ungated(step))
        .filter_map(|step| step.get("run").and_then(serde_yaml::Value::as_str))
        .any(|run| {
            run.contains("--features chaos")
                && run.contains("--test integration chaos_tests:: ")
                && !run
                    .split_whitespace()
                    .any(|word| NO_FULL_RUN_FLAGS.iter().any(|f| word.starts_with(f)))
                && run.trim().starts_with("cargo test ")
                && !SHELL_OPERATORS.iter().any(|op| run.trim().contains(op))
        })
}

/// Self-test: a gate or a flag that runs nothing must not count as a run.
#[test]
fn chaos_suite_check_rejects_gated_and_empty_runs() {
    let run = "cargo test -p autumn-harvest --features chaos --test integration chaos_tests:: -- --test-threads=1";
    let workflow = |job_extra: &str, step_extra: &str, run: &str| {
        let text = format!(
            "jobs:\n  chaos:\n    runs-on: x\n{job_extra}    steps:\n      - run: '{run}'\n{step_extra}"
        );
        parse_workflow_text(&text).expect("synthetic workflow must parse")
    };
    assert!(runs_the_chaos_suite_unconditionally(&workflow("", "", run)));

    let job_if = "    if: github.event_name == 'workflow_dispatch'\n";
    let job_soft = "    continue-on-error: true\n";
    let step_if = "        if: false\n";
    let step_soft = "        continue-on-error: true\n";
    for (job_extra, step_extra) in [(job_if, ""), (job_soft, ""), ("", step_if), ("", step_soft)] {
        assert!(
            !runs_the_chaos_suite_unconditionally(&workflow(job_extra, step_extra, run)),
            "a gate must not count: job {job_extra:?}, step {step_extra:?}"
        );
    }

    let kept = "        continue-on-error: false\n";
    assert!(
        runs_the_chaos_suite_unconditionally(&workflow("", kept, run)),
        "`continue-on-error: false` is the default, so it must count"
    );

    for masked in [
        format!("{run} || true"),
        format!("{run}; exit 0"),
        format!("set +e; {run}"),
        format!("{run}; true"),
        format!("{run} & wait"),
        format!("echo {}", run.trim_start_matches("cargo test ")),
        format!("echo $({run})"),
    ] {
        assert!(
            !runs_the_chaos_suite_unconditionally(&workflow("", "", &masked)),
            "`{masked}` hides a failed suite or runs no test, so it must not count"
        );
    }

    for flag in [
        "--no-run",
        "--exact",
        "--skip chaos_tests",
        "--ignored",
        "--list",
    ] {
        let flagged = format!("{run} {flag}");
        assert!(
            !runs_the_chaos_suite_unconditionally(&workflow("", "", &flagged)),
            "`{flag}` runs no test or not the whole module, so it must not count"
        );
    }
}

/// The `core:chaos_tests` exemption is true only when `chaos.yml` runs the
/// whole `chaos_tests` module, with the `chaos` feature, on a cron.
#[test]
fn chaos_workflow_runs_the_chaos_suite_nightly() {
    let doc = parse_workflow(".github/workflows/chaos.yml");
    assert!(
        !workflow_crons(&doc).is_empty(),
        "chaos.yml must have an `on.schedule` cron; without one no nightly run exists"
    );
    assert!(
        runs_the_chaos_suite_unconditionally(&doc),
        "chaos.yml must have a step that runs \
         `--features chaos --test integration chaos_tests:: ` with no `if`, no \
         `continue-on-error` and no flag that skips tests; found {:?}",
        workflow_run_commands(&doc)
    );
}

/// The total cases that ungated jobs of `doc` run for a step that contains
/// `target`. A job counts `PROPTEST_CASES` once per entry of its `shard`
/// matrix. A gated job or step counts nothing. A step that sets its own
/// `PROPTEST_CASES`, or drops the default `db` feature, counts nothing
/// either (issue #1829).
fn deep_cases(doc: &serde_yaml::Value, target: &str) -> u64 {
    let Some(jobs) = doc.get("jobs").and_then(serde_yaml::Value::as_mapping) else {
        return 0;
    };
    jobs.values()
        .filter(|job| ungated(job))
        .filter(|job| {
            job.get("steps")
                .and_then(serde_yaml::Value::as_sequence)
                .into_iter()
                .flatten()
                .filter(|step| ungated(step))
                .filter(|step| {
                    step.get("env")
                        .and_then(|env| env.get("PROPTEST_CASES"))
                        .is_none()
                })
                .filter_map(|step| step.get("run").and_then(serde_yaml::Value::as_str))
                .any(|run| {
                    let run = run.trim();
                    run.starts_with("cargo test ")
                        && run.contains(target)
                        && !run.contains("--no-default-features")
                        && !SHELL_OPERATORS.iter().any(|op| run.contains(op))
                        && !run
                            .split_whitespace()
                            .any(|word| NO_FULL_RUN_FLAGS.iter().any(|f| word.starts_with(f)))
                })
        })
        .map(|job| {
            let cases = job
                .get("env")
                .and_then(|env| env.get("PROPTEST_CASES"))
                .and_then(serde_yaml::Value::as_str)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            let shards = job
                .get("strategy")
                .and_then(|s| s.get("matrix"))
                .and_then(|m| m.get("shard"))
                .and_then(serde_yaml::Value::as_sequence)
                .map_or(1, |s| s.len() as u64);
            cases * shards
        })
        .sum()
}

/// The nightly deep pass runs the property suites and the stateful
/// lifecycle model with at least 100000 cases each (issue #1829).
#[test]
fn proptest_nightly_runs_both_deep_passes() {
    const DEEP: u64 = 100_000;
    let doc = parse_workflow(".github/workflows/proptest-nightly.yml");
    assert!(
        !workflow_crons(&doc).is_empty(),
        "proptest-nightly.yml must have an `on.schedule` cron"
    );
    for target in [
        "--test property",
        "--test integration lifecycle_model_props:: ",
    ] {
        let cases = deep_cases(&doc, target);
        assert!(
            cases >= DEEP,
            "the nightly runs `{target}` for {cases} cases; it must run at least {DEEP}"
        );
    }
}

/// Self-test: a gate, a missing case count or a flag that runs nothing does
/// not count toward the deep pass.
#[test]
fn deep_case_count_rejects_gated_and_empty_runs() {
    let workflow = |job_extra: &str, run: &str| {
        let text = format!(
            "jobs:\n  deep:\n    runs-on: x\n{job_extra}    strategy:\n      matrix:\n        \
             shard: [1, 2]\n    steps:\n      - run: '{run}'\n"
        );
        parse_workflow_text(&text).expect("synthetic workflow must parse")
    };
    let env = "    env:\n      PROPTEST_CASES: \"50000\"\n";
    let run = "cargo test -p autumn-harvest --test property";
    assert_eq!(deep_cases(&workflow(env, run), "--test property"), 100_000);
    assert_eq!(deep_cases(&workflow("", run), "--test property"), 0);
    let gated = format!("{env}    if: false\n");
    assert_eq!(deep_cases(&workflow(&gated, run), "--test property"), 0);
    let empty = format!("{run} --no-run");
    assert_eq!(deep_cases(&workflow(env, &empty), "--test property"), 0);
    // `--no-default-features` drops the `db` feature, so the db suites
    // compile out and run nothing.
    let no_db = format!("{run} --no-default-features");
    assert_eq!(deep_cases(&workflow(env, &no_db), "--test property"), 0);
    let joined = format!("{run} && true");
    assert_eq!(deep_cases(&workflow(env, &joined), "--test property"), 0);
    // A step `if` or a step `env` that lowers the case count must not count.
    let step = |extra: &str| {
        let text = format!(
            "jobs:\n  deep:\n    runs-on: x\n{env}    steps:\n      - run: '{run}'\n{extra}"
        );
        parse_workflow_text(&text).expect("synthetic workflow must parse")
    };
    assert_eq!(deep_cases(&step(""), "--test property"), 50_000);
    assert_eq!(
        deep_cases(&step("        if: false\n"), "--test property"),
        0
    );
    let lowered = "        env:\n          PROPTEST_CASES: \"1\"\n";
    assert_eq!(deep_cases(&step(lowered), "--test property"), 0);
}
