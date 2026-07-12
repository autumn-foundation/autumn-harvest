//! CI run-step coverage guard — no DB, no feature gate.
//!
//! Prevents the "silently-never-run DB test" class of bug: a testcontainers /
//! `HARVEST_TEST_DATABASE_URL` integration test that compiles in CI (via
//! `--no-run` and clippy) but is never *executed* against a live Postgres, so a
//! real runtime failure stays invisible until it reaches production. Example:
//! `nd_block_tests` was missing the `build_policy_ramp` migration, making every
//! workflow start fail `column "target_build_id" does not exist`.
//!
//! This guard, running in the cheap no-DB CI step on every OS, cross-checks the
//! set of DB-gated integration tests (core + plugin) against the set of tests
//! that `.github/workflows/ci.yml` actually *runs* (not merely `--no-run`
//! compiles). Every DB-gated test must either have a covering run step or be
//! listed — with a reason — in `ALLOWLIST`. `ALLOWLIST` is seeded fail-closed
//! with the tests currently lacking a run step; it is technical debt to shrink,
//! not to grow (a soft ratchet caps its length below).
//!
//! On failure the panic message lists every uncovered test so the fix is
//! actionable: either add a Docker-backed run step in `ci.yml` or (rarely, with
//! a documented reason) extend `ALLOWLIST`.

use std::collections::BTreeSet;
use std::path::PathBuf;

// ── Inputs embedded at compile time (recompile when they change) ────────────

/// The whole CI workflow, so a reformat that breaks parsing recompiles — and
/// trips the self-test below — rather than silently passing.
const CI_YAML: &str = include_str!("../../../.github/workflows/ci.yml");

/// The integration submodule declarations (source of truth for which core
/// suites exist and their cfg gates).
const CORE_MOD_RS: &str = include_str!("mod.rs");

// ── DB classification ───────────────────────────────────────────────────────

/// A test file "needs a live DB" (== a Docker-backed CI run step) iff it
/// actually spins up / connects to Postgres. These three tokens are the
/// codebase's inline convention for that; tests that instead apply
/// `autumn_harvest::MIGRATIONS` via `autumn_web::migrate::run_pending`, or that
/// no-op unless `DATABASE_URL` is set, are deliberately *not* matched (they
/// can't drift, or don't run in CI).
const LIVE_DB_TOKENS: &[&str] = &[
    "with_init_sql(",
    "Postgres::default(",
    "HARVEST_TEST_DATABASE_URL",
];

fn needs_live_db(source: &str) -> bool {
    LIVE_DB_TOKENS.iter().any(|t| source.contains(t))
}

/// Meta-test files that are not DB tests but mention the classifier tokens as
/// string literals (this guard defines `LIVE_DB_TOKENS`), so they'd otherwise
/// be misclassified as needing a live DB.
const SELF_EXCLUDE: &[&str] = &["ci_run_coverage", "migration_hygiene"];

// ── Allowlist: DB-gated tests without a CI run step (technical debt) ─────────
//
// Keyed `core:<module>` / `plugin:<file-stem>`. Every entry carries a reason.
// Seeded fail-closed with the tests that currently lack a Docker-backed run
// step so the guard is green on commit. SHRINK this by wiring run steps; the
// ratchet below forbids silent growth. This guard PROVED it bites: during
// development `nd_block_tests` was left out and the guard failed naming it (the
// TDD red step). `nd_block_tests`, `worker_session_tests`, and the plugin
// `build_ramp_integration` suites were fixed and wired to Docker-backed run
// steps in this PR, so they are no longer here.

const ALLOWLIST_DEBT_REASON: &str = "DB integration test not yet wired to a Docker-backed CI run step; test-coverage debt to shrink";
const ALLOWLIST_TESTING_REASON: &str = "DB+testing-gated integration test not yet wired to a Docker-backed CI run step (needs --features testing when wired)";
// completion_callback_tests: the migration drift that blocked it is FIXED in
// this PR (swapped to `full_migrations_sql()`), so 19/20 tests pass. But a
// single PRE-EXISTING, non-schema test-logic bug blocks a green run step:
// `scanner_scopes_claims_to_the_assigned_shard_when_shards_share_a_pool`
// inserts two executions sharing `(workflow_name, workflow_id)` into one
// shared pool and collides on the `harvest_we_workflow_name_workflow_id_active`
// unique index (a duplicate-key panic in the #605 test setup, unrelated to
// migrations). Allowlisted pending a follow-up by the #605 owner; see PR report.
const ALLOWLIST_COMPLETION_CALLBACK_REASON: &str = "migration drift fixed (now uses full_migrations_sql); one pre-existing non-schema \
     test-logic bug (scanner_scopes… duplicate key on the active-uniqueness index) blocks a \
     green run step — handed off to the #605 owner (see PR report)";

const ALLOWLIST: &[(&str, &str)] = &[
    // ── core (autumn-harvest/tests/integration) ──
    ("core:audit_tests", ALLOWLIST_DEBT_REASON),
    ("core:build_routing_tests", ALLOWLIST_DEBT_REASON),
    ("core:cache_delta_load_tests", ALLOWLIST_DEBT_REASON),
    ("core:cancellation_tests", ALLOWLIST_DEBT_REASON),
    ("core:child_policy_tests", ALLOWLIST_DEBT_REASON),
    (
        "core:completion_callback_tests",
        ALLOWLIST_COMPLETION_CALLBACK_REASON,
    ),
    ("core:cross_workflow_cancel_tests", ALLOWLIST_DEBT_REASON),
    ("core:cross_workflow_signal_tests", ALLOWLIST_DEBT_REASON),
    ("core:debounce_tests", ALLOWLIST_DEBT_REASON),
    ("core:delayed_start_tests", ALLOWLIST_DEBT_REASON),
    ("core:event_batch_tests", ALLOWLIST_DEBT_REASON),
    ("core:legal_hold_tests", ALLOWLIST_DEBT_REASON),
    ("core:metrics_integration", ALLOWLIST_DEBT_REASON),
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
    ("plugin:dag_retry_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:dlq_aggregate_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:dlq_bulk_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:dlq_redrive_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:erase_payloads_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:event_batch_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:external_handoffs_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    ("plugin:history_export_integration", ALLOWLIST_DEBT_REASON),
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
    ("plugin:start_throttle_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:telemetry_propagation_tests", ALLOWLIST_DEBT_REASON),
    ("plugin:terminate_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:usage_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:version_usage_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:workflow_count_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:workflow_filter_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:workflow_history_pagination_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    (
        "plugin:workflow_reachability_integration",
        ALLOWLIST_DEBT_REASON,
    ),
    ("plugin:workflow_reset_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:workflow_result_integration", ALLOWLIST_DEBT_REASON),
];

/// Soft ratchet: the allowlist may shrink but must never silently grow. Bump
/// this ONLY with a deliberate justification (it should trend toward zero).
/// 73 = the 76 never-run DB suites at PR time minus the three fixed and wired
/// to Docker-backed run steps in this PR (`nd_block_tests`,
/// `worker_session_tests`, plugin `build_ramp_integration`).
const ALLOWLIST_MAX_LEN: usize = 73;

fn allowlisted(key: &str) -> bool {
    ALLOWLIST.iter().any(|&(k, _)| k == key)
}

// ── ci.yml parsing (tolerant, token-based over the whole text) ──────────────

/// A single `run: cargo test ...` command that is an actual RUN (not `--no-run`).
struct RunCommand {
    text: String,
}

fn run_commands() -> Vec<RunCommand> {
    let mut out = Vec::new();
    for line in CI_YAML.lines() {
        let line = line.trim();
        // Commands live on `run:` lines (single-line in this workflow).
        let Some(idx) = line.find("cargo test") else {
            continue;
        };
        let cmd = &line[idx..];
        if cmd.contains("--no-run") {
            continue; // compile-only, not a run
        }
        out.push(RunCommand {
            text: cmd.to_string(),
        });
    }
    out
}

/// Extract the value following `--features` in a command (comma-list), if any.
fn features_of(cmd: &str) -> Option<&str> {
    let idx = cmd.find("--features")?;
    let rest = cmd[idx + "--features".len()..].trim_start();
    Some(rest.split_whitespace().next().unwrap_or(""))
}

// ── Core coverage ───────────────────────────────────────────────────────────

/// (filter-first-segment, `has_testing`, `has_db`)
struct CoreFilter {
    seg: String,
    has_testing: bool,
    has_db: bool,
}

struct CoreCoverage {
    filters: Vec<CoreFilter>,
    /// A whole-target `--test integration` run that includes the db feature.
    whole_db: bool,
    whole_db_testing: bool,
}

fn parse_core_coverage(cmds: &[RunCommand]) -> CoreCoverage {
    let mut filters = Vec::new();
    let mut whole_db = false;
    let mut whole_db_testing = false;
    for c in cmds {
        let cmd = &c.text;
        // core crate only (plugin check FIRST because it is a superstring).
        if cmd.contains("-p autumn-harvest-plugin") {
            continue;
        }
        if !cmd.contains("-p autumn-harvest") {
            continue;
        }
        if !cmd.contains("--test integration") {
            continue;
        }
        let has_no_default = cmd.contains("--no-default-features");
        let feats = features_of(cmd).unwrap_or("");
        let has_db = !has_no_default || feats.split(',').any(|f| f == "db");
        let has_testing = feats.split(',').any(|f| f == "testing");
        // Is there a filter after `--test integration -- `?
        if let Some(pos) = cmd.find("--test integration") {
            let after = cmd[pos + "--test integration".len()..].trim_start();
            if let Some(rest) = after.strip_prefix("--") {
                let rest = rest.trim_start();
                let first = rest.split_whitespace().next().unwrap_or("");
                if !first.is_empty() && first != "--test-threads=1" {
                    let seg = first.split("::").next().unwrap_or(first).to_string();
                    filters.push(CoreFilter {
                        seg,
                        has_testing,
                        has_db,
                    });
                    continue;
                }
            }
            // No filter → whole-target run.
            if has_db {
                whole_db = true;
                whole_db_testing = whole_db_testing || has_testing;
            }
        }
    }
    CoreCoverage {
        filters,
        whole_db,
        whole_db_testing,
    }
}

impl CoreCoverage {
    /// A core module is covered iff a db-carrying run targets it (whole-target
    /// or a filter whose first segment prefixes the module name). When the
    /// module is `testing`-gated the covering run must also carry `--features
    /// testing`, else the module compiles to nothing and never runs.
    fn covers(&self, module: &str, needs_testing: bool) -> bool {
        if self.whole_db && (!needs_testing || self.whole_db_testing) {
            return true;
        }
        self.filters.iter().any(|f| {
            f.has_db
                && (!needs_testing || f.has_testing)
                && (module == f.seg || module.starts_with(&f.seg))
        })
    }
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
/// `#![cfg(feature = "X")]`), so a covering run step must carry them.
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

struct PluginRun {
    tests: BTreeSet<String>,
    features: String,
}

fn parse_plugin_runs(cmds: &[RunCommand]) -> Vec<PluginRun> {
    let mut out = Vec::new();
    for c in cmds {
        let cmd = &c.text;
        if !cmd.contains("-p autumn-harvest-plugin") {
            continue;
        }
        let mut tests = BTreeSet::new();
        // Collect every `--test <name>` token.
        let toks: Vec<&str> = cmd.split_whitespace().collect();
        let mut i = 0;
        while i < toks.len() {
            if toks[i] == "--test" {
                if let Some(name) = toks.get(i + 1) {
                    tests.insert((*name).to_string());
                }
                i += 2;
            } else {
                i += 1;
            }
        }
        if tests.is_empty() {
            continue;
        }
        let features = features_of(cmd).unwrap_or("").to_string();
        out.push(PluginRun { tests, features });
    }
    out
}

fn plugin_covered(file_stem: &str, required_features: &[String], runs: &[PluginRun]) -> bool {
    runs.iter().any(|r| {
        r.tests.contains(file_stem)
            && required_features
                .iter()
                .all(|f| r.features.split(',').any(|rf| rf == f))
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

// ── Self-test: the parser must find known-existing run steps ────────────────

#[test]
fn ci_parser_finds_known_run_steps() {
    let cmds = run_commands();
    assert!(
        !cmds.is_empty(),
        "found no `run: cargo test` commands in ci.yml — parser or workflow format broke"
    );

    let core = parse_core_coverage(&cmds);
    let core_segs: BTreeSet<&str> = core.filters.iter().map(|f| f.seg.as_str()).collect();
    for expected in [
        "integration_e2e",
        "force_fail",
        "typed_workflow_failure_tests",
    ] {
        assert!(
            core_segs.contains(expected),
            "self-test: expected a core `--test integration -- {expected}` run filter, \
             found {core_segs:?}. Did ci.yml formatting change?"
        );
    }
    assert!(
        core.filters.len() >= 8,
        "self-test: expected ≥8 core run filters, found {} — parser likely broke",
        core.filters.len()
    );

    let plugin_runs = parse_plugin_runs(&cmds);
    let plugin_tests: BTreeSet<&str> = plugin_runs
        .iter()
        .flat_map(|r| r.tests.iter().map(String::as_str))
        .collect();
    for expected in [
        "api_scheduler_integration",
        "ui_integration",
        "query_integration",
    ] {
        assert!(
            plugin_tests.contains(expected),
            "self-test: expected a plugin `--test {expected}` run step, found {plugin_tests:?}"
        );
    }
    assert!(
        plugin_tests.len() >= 15,
        "self-test: expected ≥15 plugin run targets, found {} — parser likely broke",
        plugin_tests.len()
    );
}

// ── The guard ───────────────────────────────────────────────────────────────

#[test]
fn every_db_gated_test_has_a_ci_run_step_or_is_allowlisted() {
    let cmds = run_commands();
    let core_cov = parse_core_coverage(&cmds);
    let plugin_runs = parse_plugin_runs(&cmds);

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
        let key = format!("core:{}", m.name);
        live_db_keys.insert(key.clone());
        if core_cov.covers(&m.name, m.needs_testing) {
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        uncovered.push(format!(
            "{key} (add a `cargo test -p autumn-harvest{} --test integration -- {} --test-threads=1` \
             run step in ci.yml, guarded `if: runner.os == 'Linux'`)",
            if m.needs_testing { " --features testing" } else { "" },
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
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let key = format!("plugin:{stem}");
        live_db_keys.insert(key.clone());
        let req = plugin_required_features(&src);
        if plugin_covered(&stem, &req, &plugin_runs) {
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        let feat_flag = if req.is_empty() {
            String::new()
        } else {
            format!(" --features {}", req.join(","))
        };
        uncovered.push(format!(
            "{key} (add a `cargo test -p autumn-harvest-plugin{feat_flag} --test {stem} -- --test-threads=1` \
             run step in ci.yml, guarded `if: runner.os == 'Linux'`)"
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
        "these DB-gated integration tests have NO CI run step and are NOT allowlisted \
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
         by wiring run steps, not to grow. If a new DB test genuinely cannot run in CI yet, raise \
         the cap deliberately with justification.",
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
