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
//! DB-gated test actually RUN by some `ci.yml` step?" — never whether its
//! schema bundle is complete.
//!
//! Mechanics (runs in the cheap no-DB step on every OS): cross-check the set of
//! DB-gated integration tests against the set of tests `.github/workflows/ci.yml`
//! actually *runs*. A target is NOT credited as run when it is only
//! `--no-run`-compiled, when the step selects only a `module::filter`
//! sub-slice of it (the `::workflow_typed` trap above), or when every one of
//! its `#[test]`/`#[tokio::test]` fns is `#[ignore]`d (a run step then executes
//! nothing). Every DB-gated test must either have a genuine covering run step
//! or be listed — with a reason — in `ALLOWLIST`. `ALLOWLIST` is fail-closed
//! debt to SHRINK by wiring run steps, not to grow (a soft ratchet caps its
//! length below).
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

/// A test file "needs a live DB" (== a Docker-backed CI run step) iff its
/// *code* spins up / connects to Postgres. Two families of markers:
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
/// `#[ignore]`d, so a run step targeting it executes nothing and must NOT be
/// credited as covering it (the target must be allowlisted instead). Counts
/// `#[ignore]` against the total test count; a file with no tests is not
/// "all-ignored".
fn all_tests_ignored(source: &str) -> bool {
    let tests = source.matches("#[tokio::test").count() + source.matches("#[test]").count();
    let ignored = source.matches("#[ignore").count();
    tests > 0 && ignored >= tests
}

/// Whether a file's own leading `#![cfg(...)]` gates it on `feature = "testing"`.
/// Fix #3: the dominant convention is a file-level `#![cfg(...)]` with a plain
/// `mod X;` in `mod.rs`, so the covering-run `--features testing` requirement
/// must be read from the file, not only from the `mod.rs` cfg line. The two are
/// combined (unioned) by the caller — either gate requiring testing means the
/// run must carry it — so the `mod.rs`-gated `all(testing, db)` submodules stay
/// correct.
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
// workflow_retry_tests: the run step is deliberately limited to the
// `::workflow_typed` sub-filter (issue #767 FIX B). Running the WHOLE module
// fails 3 pre-existing, non-schema #523 workflow-level-retry bugs:
// cancelled_workflow_is_not_retried, workflow_non_retryable_error_no_retry,
// workflow_retry_exhaustion_counts_as_one_failure. Verified NOT migration
// drift: the identical 3 fail under `full_migrations_sql()` too. Handed off to
// the #523 owner; run step stays limited to workflow_typed. See PR report.
const ALLOWLIST_WORKFLOW_RETRY_REASON: &str = "run step limited to `workflow_retry_tests::workflow_typed` (issue #767 FIX B); the full \
     module hits 3 pre-existing NON-schema #523 retry bugs (cancelled_workflow_is_not_retried, \
     workflow_non_retryable_error_no_retry, workflow_retry_exhaustion_counts_as_one_failure — \
     identical failures under full_migrations_sql, so NOT drift). Owner: #523. See PR report.";
// mcp_tools_integration / webhook_* integration: paved-path DB tests
// (autumn_web::test::TestDb + run_pending(MIGRATIONS)) that are feature-gated
// AND have every test #[ignore]d, so no CI step can execute them. The mcp one
// is `--no-run`-compiled only; the webhooks feature is enabled by no CI step.
// Wiring real Docker-backed run steps for #[ignore]d/feature-gated suites is
// out of scope for this test-infra PR — tracked here honestly instead.
const ALLOWLIST_MCP_IGNORED_REASON: &str = "mcp-feature-gated (only `--no-run`-compiled in CI) AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness) — no CI step can execute it; tracked";
const ALLOWLIST_WEBHOOKS_IGNORED_REASON: &str = "webhooks-feature-gated — not run in CI (compiled only via `--no-run`) — AND all tests are #[ignore]d \
     (TestDb/run_pending paved-path DB harness); tracked";

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
    ("core:workflow_retry_tests", ALLOWLIST_WORKFLOW_RETRY_REASON),
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
    ("plugin:start_throttle_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:telemetry_propagation_tests", ALLOWLIST_DEBT_REASON),
    ("plugin:terminate_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:usage_integration", ALLOWLIST_DEBT_REASON),
    ("plugin:version_usage_integration", ALLOWLIST_DEBT_REASON),
    (
        "plugin:webhook_durable_integration",
        ALLOWLIST_WEBHOOKS_IGNORED_REASON,
    ),
    (
        "plugin:webhook_receiver_integration",
        ALLOWLIST_WEBHOOKS_IGNORED_REASON,
    ),
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
/// 77 = the prior 73 plus the four surfaced by this hardening pass:
///   * `core:workflow_retry_tests` — Fix #1 stopped a `::workflow_typed`
///     sub-filter from masking the whole module (3 genuine #523 bugs);
///   * `plugin:mcp_tools_integration`, `plugin:webhook_durable_integration`,
///     `plugin:webhook_receiver_integration` — Fix #2 stopped the paved
///     `TestDb`/`run_pending` DB harness from evading classification (all three
///     are feature-gated + fully `#[ignore]`d, so no CI step can run them).
const ALLOWLIST_MAX_LEN: usize = 77;

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
        // ASSUMPTION: every `run: cargo test …` command is a single physical
        // line (no YAML block scalars `|`/`>` spanning multiple lines). True for
        // this workflow; a multi-line command would parse only its first line.
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

/// (filter-first-segment, `partial`, `has_testing`, `has_db`)
struct CoreFilter {
    seg: String,
    /// The run filter was `module::something`, i.e. it selects only a *slice*
    /// of the module's tests (e.g. `workflow_retry_tests::workflow_typed` runs
    /// 3 of 9). Such a filter must NOT be credited as whole-module coverage.
    partial: bool,
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
                    // A `module::test` filter runs only that slice, so it must
                    // not credit the whole module (the `::workflow_typed` trap).
                    let partial = first.contains("::");
                    let seg = first.split("::").next().unwrap_or(first).to_string();
                    filters.push(CoreFilter {
                        seg,
                        partial,
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
            !f.partial
                && f.has_db
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
    /// A positional test filter followed the `--` separator (something other
    /// than `--test-threads=N`), so the step runs only a slice — mirror of the
    /// core `CoreFilter::partial` guard (Fix #6). Not triggered by any current
    /// ci.yml plugin step, but keeps the two parsers symmetric so a future
    /// `--test foo -- some_filter` can't over-credit `foo`.
    partial: bool,
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
        // Everything AFTER the first standalone `--` is a positional filter (not
        // a cargo flag); anything there other than `--test-threads=N` slices the
        // target's tests and must not be credited as full coverage.
        let sep = toks.iter().position(|t| *t == "--");
        let partial = sep.is_some_and(|s| {
            toks[s + 1..]
                .iter()
                .any(|t| !t.starts_with("--test-threads"))
        });
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
        out.push(PluginRun {
            tests,
            features,
            partial,
        });
    }
    out
}

fn plugin_covered(file_stem: &str, required_features: &[String], runs: &[PluginRun]) -> bool {
    runs.iter().any(|r| {
        !r.partial
            && r.tests.contains(file_stem)
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

// ── Fix #6: `--no-run` compile-only steps are never counted as runs ──────────

#[test]
fn no_run_compile_only_target_is_not_covered() {
    let cmds = run_commands();
    let plugin_runs = parse_plugin_runs(&cmds);
    let run_targets: BTreeSet<&str> = plugin_runs
        .iter()
        .flat_map(|r| r.tests.iter().map(String::as_str))
        .collect();
    // `mcp_tools_integration` appears in ci.yml ONLY on a `--no-run` compile
    // line, never a real run step — it must not be treated as covered.
    assert!(
        !run_targets.contains("mcp_tools_integration"),
        "a `--no-run` compile-only target must NOT appear in the covered run set"
    );
    // Sanity: a genuine run target IS present.
    assert!(run_targets.contains("api_scheduler_integration"));
}

// ── Fix #1: a `module::filter` sub-selection must not credit the whole module ─

#[test]
fn subfilter_does_not_credit_whole_module() {
    // The exact ci.yml shape that masked 6 of `workflow_retry_tests`' 9 DB tests.
    let sub = vec![RunCommand {
        text: "cargo test -p autumn-harvest --test integration -- \
               workflow_retry_tests::workflow_typed --test-threads=1"
            .to_string(),
    }];
    let cov = parse_core_coverage(&sub);
    assert!(
        !cov.covers("workflow_retry_tests", false),
        "a `module::test` sub-filter must NOT credit the whole module as covered"
    );

    // Whereas the whole-module filter DOES cover it.
    let whole = vec![RunCommand {
        text: "cargo test -p autumn-harvest --test integration -- \
               workflow_retry_tests --test-threads=1"
            .to_string(),
    }];
    assert!(
        parse_core_coverage(&whole).covers("workflow_retry_tests", false),
        "a whole-module filter must credit the module"
    );
}

// ── Fix #2: classifier is fail-CLOSED for the paved live-DB path & honest about
//    env-gated / no-DB HTTP tests ──────────────────────────────────────────────

#[test]
fn paved_path_db_tests_are_classified_and_flagged_all_ignored() {
    let dir = plugin_tests_dir();
    // These three spin a real container via `autumn_web::test::TestDb` +
    // `run_pending(MIGRATIONS)` — the paved path the original three-token list
    // missed. They must now classify as DB tests, and each is fully `#[ignore]`d.
    for stem in [
        "mcp_tools_integration",
        "webhook_receiver_integration",
        "webhook_durable_integration",
    ] {
        let src = read_source(&dir.join(format!("{stem}.rs")));
        assert!(
            needs_live_db(&src),
            "{stem} uses the TestDb/run_pending paved DB path and must be classified as DB-gated"
        );
        assert!(
            all_tests_ignored(&src),
            "{stem}: all its tests are #[ignore]d — a run step could not execute it"
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
        // Fix #3: the covering-run `--features testing` requirement is the UNION
        // of the `mod.rs` cfg and the file's own leading `#![cfg]` (either gate
        // requiring testing means the run must carry it). This keeps the
        // `mod.rs`-only `all(testing, db)` submodules correct while also picking
        // up file-level `#![cfg(feature = "testing")]` on plain-`mod` files.
        let needs_testing = m.needs_testing || file_requires_testing(&src);
        // A covering run step can't credit a target whose tests are all
        // `#[ignore]`d — it would execute nothing.
        let covered = core_cov.covers(&m.name, needs_testing) && !all_tests_ignored(&src);
        if covered {
            continue;
        }
        if allowlisted(&key) {
            continue;
        }
        uncovered.push(format!(
            "{key} (add a `cargo test -p autumn-harvest{} --test integration -- {} --test-threads=1` \
             run step in ci.yml, guarded `if: runner.os == 'Linux'`)",
            if needs_testing { " --features testing" } else { "" },
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
        // A covering run step can't credit an all-`#[ignore]`d target — it runs
        // nothing — so force it uncovered (→ must be allowlisted).
        let covered = plugin_covered(&stem, &req, &plugin_runs) && !all_tests_ignored(&src);
        if covered {
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
