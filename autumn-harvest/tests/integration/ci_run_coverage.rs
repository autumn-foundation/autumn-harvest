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
//! drift* (a hand-rolled `INIT_SQL` bundle missing a migration, which would
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
//! grow (a soft ratchet caps its length below).
//!
//! A NEW assertion guards against the manifest being silently ignored: `ci.yml`
//! must actually invoke the runner against the manifest, else the guard would
//! read a manifest CI never executes (green-but-not-run). A second NEW assertion
//! keeps the `merge=union` manifest sorted + unique (a union-merge artifact is a
//! benign, one-command-fix CI failure, never a conflict).
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
fn all_tests_ignored(source: &str) -> bool {
    let tests = source.matches("#[tokio::test").count() + source.matches("#[test]").count();
    let ignored = source.matches("#[ignore").count();
    tests > 0 && ignored >= tests
}

/// Whether a file's own leading `#![cfg(...)]` gates it on `feature = "testing"`.
/// The dominant convention is a file-level `#![cfg(...)]` with a plain `mod X;`
/// in `mod.rs`, so the covering-run `--features testing` requirement must be
/// read from the file, not only from the `mod.rs` cfg line. The two are combined
/// (unioned) by the caller — either gate requiring testing means the run must
/// carry it — so the `mod.rs`-gated `all(testing, db)` submodules stay correct.
fn file_requires_testing(source: &str) -> bool {
    source
        .lines()
        .take_while(|l| {
            let t = l.trim_start();
            t.starts_with("#![") || t.starts_with("//") || t.is_empty()
        })
        .any(|l| l.trim_start().starts_with("#![cfg") && l.contains("feature = \"testing\""))
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

// ── Allowlist: DB-gated tests without a covering manifest row (technical debt) ─
//
// Keyed `core:<module>` / `plugin:<file-stem>`. Every entry carries a reason.
// Seeded fail-closed with the tests that currently lack a covering manifest row
// so the guard is green on commit. SHRINK this by adding manifest rows; the
// ratchet below forbids silent growth. This guard PROVED it bites: during
// development `nd_block_tests` was left out and the guard failed naming it (the
// TDD red step). `nd_block_tests`, `worker_session_tests`, and the plugin
// `build_ramp_integration` suites are wired (they have `linux` manifest rows),
// so they are no longer here.

const ALLOWLIST_DEBT_REASON: &str =
    "DB integration test not yet wired to a covering manifest row; test-coverage debt to shrink";
const ALLOWLIST_TESTING_REASON: &str = "DB+testing-gated integration test not yet wired to a covering manifest row (needs the `testing` feature when wired)";
// mcp_tools_integration / webhook_* integration: paved-path DB tests
// (autumn_web::test::TestDb + run_pending(MIGRATIONS)) that are feature-gated
// AND have every test #[ignore]d, so no CI run can execute them. The mcp one is
// only `compileonly` in the manifest; the webhooks feature is enabled by no CI
// run. Wiring real Docker-backed runs for #[ignore]d/feature-gated suites is
// out of scope for this test-infra PR — tracked here honestly instead.
const ALLOWLIST_MCP_IGNORED_REASON: &str = "mcp-feature-gated (only `compileonly` in the manifest) AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness) — no CI run can execute it; tracked";
const ALLOWLIST_KAFKA_BROKER_REASON: &str = "kafka-feature-gated: DOES run in CI, via a dedicated Linux-only \
     ci.yml step (it needs an apt libcurl/cmake install first, and the manifest's compile mode would try to build \
     vendored librdkafka on macOS/Windows). Not a coverage gap — see the `Run plugin Kafka broker connector tests` step.";
const ALLOWLIST_WEBHOOKS_IGNORED_REASON: &str = "webhooks-feature-gated — not run in CI (no manifest row) — AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness); tracked";

const ALLOWLIST: &[(&str, &str)] = &[
    // ── core (autumn-harvest/tests/integration) ──
    ("core:audit_tests", ALLOWLIST_DEBT_REASON),
    ("core:build_routing_tests", ALLOWLIST_DEBT_REASON),
    ("core:cache_delta_load_tests", ALLOWLIST_DEBT_REASON),
    ("core:cancellation_tests", ALLOWLIST_DEBT_REASON),
    ("core:child_policy_tests", ALLOWLIST_DEBT_REASON),
    ("core:cross_workflow_cancel_tests", ALLOWLIST_DEBT_REASON),
    ("core:cross_workflow_signal_tests", ALLOWLIST_DEBT_REASON),
    ("core:debounce_tests", ALLOWLIST_DEBT_REASON),
    ("core:delayed_start_tests", ALLOWLIST_DEBT_REASON),
    ("core:legal_hold_tests", ALLOWLIST_DEBT_REASON),
    ("core:pause_tests", ALLOWLIST_DEBT_REASON),
    ("core:payload_offload_db_tests", ALLOWLIST_DEBT_REASON),
    ("core:poison_pill_tests", ALLOWLIST_DEBT_REASON),
    ("core:queue_fairness_tests", ALLOWLIST_DEBT_REASON),
    ("core:redrive_tests", ALLOWLIST_DEBT_REASON),
    ("core:replay_canary_tests", ALLOWLIST_TESTING_REASON),
    ("core:replayer_integration_tests", ALLOWLIST_TESTING_REASON),
    ("core:retry_now_tests", ALLOWLIST_DEBT_REASON),
    ("core:schedule_decisions", ALLOWLIST_DEBT_REASON),
    ("core:schedule_to_close_tests", ALLOWLIST_DEBT_REASON),
    ("core:schedule_update_tests", ALLOWLIST_DEBT_REASON),
    ("core:scheduled_time_tests", ALLOWLIST_TESTING_REASON),
    ("core:scheduler_auto_pause_tests", ALLOWLIST_DEBT_REASON),
    ("core:scheduler_bounded_runs_tests", ALLOWLIST_DEBT_REASON),
    ("core:scheduler_carryover_tests", ALLOWLIST_TESTING_REASON),
    ("core:scheduler_catchup_tests", ALLOWLIST_DEBT_REASON),
    ("core:scheduler_ha_tests", ALLOWLIST_DEBT_REASON),
    ("core:signal_tests", ALLOWLIST_DEBT_REASON),
    ("core:signal_with_start_tests", ALLOWLIST_DEBT_REASON),
    ("core:sla_breach_tests", ALLOWLIST_DEBT_REASON),
    ("core:slot_tuner_tests", ALLOWLIST_DEBT_REASON),
    ("core:sticky_routing_tests", ALLOWLIST_DEBT_REASON),
    ("core:telemetry_span_tests", ALLOWLIST_DEBT_REASON),
    ("core:throttle_tests", ALLOWLIST_DEBT_REASON),
    ("core:transactional_activity_tests", ALLOWLIST_DEBT_REASON),
    ("core:typed_stubs_tests", ALLOWLIST_DEBT_REASON),
    ("core:updt_with_start_tests", ALLOWLIST_DEBT_REASON),
    ("core:workflow_handle_tests", ALLOWLIST_DEBT_REASON),
    ("core:workflow_task_timeout_tests", ALLOWLIST_DEBT_REASON),
    // ── plugin (autumn-harvest-plugin/tests) ──
    ("plugin:archival_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:batch_operations_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:build_routing_ui_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:connector_kafka_broker",
        ALLOWLIST_KAFKA_BROKER_REASON,
    ),
    ("plugin:dag_retry_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:dlq_redrive_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:erase_payloads_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:event_batch_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:external_handoffs_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    ("plugin:history_export_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:mcp_tools_integration", ALLOWLIST_MCP_IGNORED_REASON),
    ("plugin:outbox_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:preflight_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:replay_canary_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:retirement_check_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:scaling_api_tests", ALLOWLIST_DEBT_REASON),
    ("plugin:schedule_update_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:shard_health_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:signal_with_start_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    ("plugin:stalled_workflow_tests", ALLOWLIST_DEBT_REASON),
    ("plugin:telemetry_propagation_tests", ALLOWLIST_DEBT_REASON),
    ("plugin:terminate_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:usage_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:version_usage_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:webhook_durable_integration",
        ALLOWLIST_WEBHOOKS_IGNORED_REASON,
    ),
    // `webhook_receiver_integration` is intentionally absent: its current-thread
    // `TestApp::plugin` deadlock was fixed (multi-thread flavor) and its tests
    // un-ignored, so it is now wired to a covering `linux` manifest row and runs
    // for real against Docker Postgres in CI.
    ("plugin:workflow_count_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:workflow_filter_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:workflow_history_pagination_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    ("plugin:workflow_reset_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:workflow_result_integration", ALLOWLIST_DEBT_REASON),
];

/// Soft ratchet: the allowlist may shrink but must never silently grow. Bump
/// this ONLY with a deliberate justification (it should trend toward zero).
/// 74 = the prior 77 minus the three wired to covering `linux` manifest rows
/// (each was a test-harness bug, now fixed, so the whole module runs green):
/// `core:workflow_retry_tests` (its `::workflow_typed` sub-filter is replaced by
/// a whole-module row), `core:completion_callback_tests`, and
/// `core:event_batch_tests`. 73 = minus `plugin:workflow_reachability_integration`,
/// now wired to a covering `linux` manifest row (issue #700).
const ALLOWLIST_MAX_LEN: usize = 74;

fn allowlisted(key: &str) -> bool {
    ALLOWLIST.iter().any(|&(k, _)| k == key)
}

// ── Manifest parsing (structured — columns already split) ───────────────────

/// One manifest record: `osclass crate target features filter`.
struct SuiteRow {
    /// `linux` | `allos` | `compileonly`.
    osclass: String,
    /// `autumn-harvest` | `autumn-harvest-plugin`.
    krate: String,
    /// The `--test <target>` binary (core suites use `integration`).
    target: String,
    /// Comma-list of features, or `-`.
    features: String,
    /// Positional filter after `--`, or `-` for the whole target.
    filter: String,
}

impl SuiteRow {
    /// `linux`/`allos` rows execute in CI; `compileonly` never does.
    fn runs(&self) -> bool {
        self.osclass == "linux" || self.osclass == "allos"
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
    const VALID_OSCLASS: &[&str] = &["linux", "allos", "compileonly"];
    const VALID_CRATE: &[&str] = &["autumn-harvest", "autumn-harvest-plugin"];
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

/// A core `integration` submodule is covered iff some executing (`linux`/`allos`)
/// manifest row enables `db` (so the module compiles + its tests exist), carries
/// `testing` when the module needs it, and targets it whole (a whole-target run,
/// or a filter whose first `::`-segment prefixes the module name — a partial
/// `module::test` filter never credits the whole module).
///
/// `autumn-harvest` has `default = ["db", "unified-dag-execution"]`; the runner
/// keeps defaults for `linux` integration rows (Docker Postgres) and strips them
/// (`--no-default-features`) for `allos` integration rows (no live DB). So a
/// `linux` integration row always has `db`; an `allos` integration row has it
/// only if it lists it explicitly. `testing` is never a default, so it must be
/// listed regardless of osclass.
fn core_covers(rows: &[SuiteRow], module: &str, needs_testing: bool) -> bool {
    rows.iter().any(|r| {
        if r.krate != "autumn-harvest" || r.target != "integration" || !r.runs() {
            return false;
        }
        let feats = r.feature_set();
        let has_db = r.osclass == "linux" || feats.contains("db");
        let has_testing = feats.contains("testing");
        if !has_db {
            return false;
        }
        if needs_testing && !has_testing {
            return false;
        }
        if r.filter == "-" {
            return true; // whole target
        }
        if r.filter.contains("::") {
            return false; // partial slice — no whole-module credit
        }
        // No `::` here (guarded above), so the whole filter is the module segment.
        let seg = r.filter.as_str();
        module == seg || module.starts_with(seg)
    })
}

// ── mod.rs parsing: (module, needs_testing) ─────────────────────────────────

struct CoreModule {
    name: String,
    needs_testing: bool,
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
            let needs_testing = cfg.contains("feature = \"testing\"");
            out.push(CoreModule {
                name,
                needs_testing,
            });
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
            for feat in ["webhooks", "mcp", "metrics", "unified-dag-execution"] {
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
        !core_covers(&sub, "workflow_retry_tests", false),
        "a `module::test` sub-filter must NOT credit the whole module as covered"
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
        core_covers(&whole, "workflow_retry_tests", false),
        "a whole-module filter must credit the module"
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
        !core_covers(&allos_no_db, "some_db_module", false),
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
    assert!(core_covers(&linux, "some_db_module", false));
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

#[test]
fn claim_budget_gate_has_a_covering_manifest_row() {
    // The gate is the whole point of issue #786; it must actually run in CI.
    let rows = parse_manifest();
    assert!(
        core_covers(&rows, "claim_budget_tests", false),
        "the claim-path budget gate must have a covering `linux` manifest row —          a performance gate that never runs is not a gate"
    );
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
fn file_requires_testing_reads_leading_file_cfg() {
    assert!(file_requires_testing(
        "#![cfg(all(feature = \"db\", feature = \"testing\"))]\nfn a() {}"
    ));
    assert!(file_requires_testing("#![cfg(feature = \"testing\")]\n"));
    assert!(!file_requires_testing("#![cfg(feature = \"db\")]\n"));
    // A mid-file `#[cfg(feature = \"testing\")]` on some item is not a file gate.
    assert!(!file_requires_testing(
        "use x;\n#[cfg(feature = \"testing\")]\nfn a() {}"
    ));
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
        // The covering-run `--features testing` requirement is the UNION of the
        // `mod.rs` cfg and the file's own leading `#![cfg]` (either gate requiring
        // testing means the run must carry it). This keeps the `mod.rs`-only
        // `all(testing, db)` submodules correct while also picking up file-level
        // `#![cfg(feature = "testing")]` on plain-`mod` files.
        let needs_testing = m.needs_testing || file_requires_testing(&src);
        // A covering run can't credit a target whose tests are all `#[ignore]`d —
        // it would execute nothing.
        let covered = core_covers(&rows, &m.name, needs_testing) && !all_tests_ignored(&src);
        if covered {
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        uncovered.push(format!(
            "{key} (add manifest line `linux  autumn-harvest  integration  {}  {}` to \
             .github/ci/integration-suites.txt)",
            if needs_testing { "testing" } else { "-" },
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

    // Stale-allowlist check: every allowlisted key must still name a real,
    // DB-gated test (otherwise the debt entry is dead and should be removed).
    let mut stale: Vec<&str> = ALLOWLIST
        .iter()
        .map(|&(k, _)| k)
        .filter(|k| !live_db_keys.contains(*k))
        .collect();
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
        "ALLOWLIST has stale entries (test deleted or no longer DB-gated) — remove them:\n  {}",
        stale.join("\n  ")
    );
}

#[test]
fn allowlist_does_not_grow_silently() {
    assert!(
        ALLOWLIST.len() <= ALLOWLIST_MAX_LEN,
        "ALLOWLIST grew to {} entries (cap {ALLOWLIST_MAX_LEN}). It is technical debt to SHRINK \
         by adding manifest rows, not to grow. If a new DB test genuinely cannot run in CI yet, \
         raise the cap deliberately with justification.",
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
