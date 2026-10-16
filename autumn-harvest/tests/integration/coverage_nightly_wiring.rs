//! Guard for the nightly coverage workflow (issue #1818). No DB, no feature
//! gate.
//!
//! `coverage-nightly.yml` must measure the unit tests and every `linux`
//! manifest row on a cron, with branch data. It must publish the report also
//! after a failed shard. The ratchet must gate each critical module against
//! its floor in `.github/ci/coverage-baseline.json`.

use std::collections::BTreeSet;

use super::ci_run_coverage::{
    DRAFT_SKIP, SHELL_OPERATORS, alerts_on_scheduled_failure, dated_nightly, job_needs,
    parse_workflow, parse_workflow_text, repo_root, runs_on_every_schedule, soft, steps, str_at,
    ungated, workflow_crons,
};

const NIGHTLY: &str = ".github/workflows/coverage-nightly.yml";
const BASELINE: &str = ".github/ci/coverage-baseline.json";
const SCRIPT: &str = ".github/ci/coverage.py";

/// The critical modules that issue #1818 names.
const CRITICAL: &[&str] = &[
    "autumn-harvest/src/codec_rotation.rs",
    "autumn-harvest/src/erase.rs",
    "autumn-harvest/src/poison_pill.rs",
    "autumn-harvest/src/queue.rs",
    "autumn-harvest/src/scheduler.rs",
    "autumn-harvest/src/timeout.rs",
    "autumn-harvest/src/worker.rs",
];

/// The `coverage-lcov-*` artifact prefix that the report job downloads.
const LCOV_PREFIX: &str = "coverage-lcov-";

/// The `if` of the ratchet step. A failed test cuts coverage short, so a
/// drop in that run is noise. The failed job already fails the run.
const RATCHET_IF: &str = "needs.unit.result == 'success' && needs.db.result == 'success'";

/// The script lines of the `db` step that runs the suites, in order.
///
/// The assignment fails the step when `show-env` fails. The `clean` must
/// come before the suites, or it deletes their profile data.
const DB_RUN: &[&str] = &[
    "coverage_env=\"$(cargo llvm-cov show-env --sh --branch)\"",
    "eval \"$coverage_env\"",
    "cargo llvm-cov clean --workspace",
    "bash .github/ci/run-suites.sh run linux",
];

/// The `if` of the `report` job. It also runs after a failed shard, so the
/// report shows what the other shards measured.
fn report_if() -> String {
    format!("always() && ({DRAFT_SKIP})")
}

/// An expression with its whitespace removed, so `${{ x }}` equals `${{x}}`.
fn compact(expr: &str) -> String {
    expr.split_whitespace().collect()
}

/// True when `run` is one command with no shell operator.
fn plain(run: &str) -> bool {
    !SHELL_OPERATORS.iter().any(|op| run.trim().contains(op))
}

/// True when `tokens` holds `pair` as two adjacent tokens.
fn has_pair(tokens: &[&str], pair: [&str; 2]) -> bool {
    tokens.windows(2).any(|w| w == pair)
}

/// The non-blank, non-comment lines of a `run` script, trimmed.
fn script_lines(run: &str) -> Vec<&str> {
    run.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

/// True when a test job installs the pinned nightly and its LLVM tools, and
/// does not override the workflow `RUSTUP_TOOLCHAIN`.
fn uses_the_coverage_toolchain(job: &serde_yaml::Value) -> bool {
    let installs = steps(job).iter().any(|s| {
        str_at(s, &["uses"]).starts_with("dtolnay/rust-toolchain@")
            && compact(str_at(s, &["with", "toolchain"])) == "${{env.RUSTUP_TOOLCHAIN}}"
            && str_at(s, &["with", "components"]).contains("llvm-tools-preview")
    });
    installs
        && job
            .get("env")
            .and_then(|e| e.get("RUSTUP_TOOLCHAIN"))
            .is_none()
}

/// True when a job exports its coverage and uploads it as LCOV, also after a
/// failed test.
///
/// The LCOV exporter of LLVM 23 crashes on `worker.rs`, so the job exports
/// JSON and the script converts it.
fn exports_lcov_always(job: &serde_yaml::Value) -> bool {
    let steps = steps(job);
    let export = steps.iter().position(|s| {
        let run = str_at(s, &["run"]);
        run.contains("cargo llvm-cov report --branch --json")
            && run.contains(&format!("{SCRIPT} convert"))
            && str_at(s, &["if"]).contains("always()")
    });
    let upload = steps.iter().position(|s| {
        str_at(s, &["uses"]).starts_with("actions/upload-artifact@")
            && str_at(s, &["with", "name"]).starts_with(LCOV_PREFIX)
            && str_at(s, &["if"]).contains("always()")
    });
    matches!((export, upload), (Some(e), Some(u)) if e < u)
}

/// True when one ungated step runs the autumn-harvest lib tests under
/// `cargo llvm-cov --branch`, as one plain command.
fn runs_the_lib_tests_instrumented(job: &serde_yaml::Value) -> bool {
    steps(job).iter().any(|s| {
        let run = str_at(s, &["run"]).trim();
        let tokens: Vec<&str> = run.split_whitespace().collect();
        ungated(s)
            && plain(run)
            && run.starts_with("cargo llvm-cov ")
            && tokens.contains(&"--branch")
            && tokens.contains(&"--lib")
            && has_pair(&tokens, ["-p", "autumn-harvest"])
    })
}

/// True when one ungated step runs exactly the [`DB_RUN`] lines.
///
/// One step must hold them all. A later step does not see the variables
/// that `eval` exports.
fn runs_the_linux_rows_instrumented(job: &serde_yaml::Value) -> bool {
    steps(job)
        .iter()
        .any(|s| ungated(s) && script_lines(str_at(s, &["run"])) == DB_RUN)
}

/// The number at `path`, written as a YAML number or a string.
fn number_at(node: &serde_yaml::Value, path: &[&str]) -> Option<u64> {
    let mut cur = node;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_u64().or_else(|| cur.as_str()?.parse().ok())
}

/// True when the `db` matrix is exactly `0..SEMAPHORE_SHARD_COUNT`.
///
/// `run-suites.sh` keeps a row when `ordinal % count == index`. A missing
/// index drops its rows in silence.
fn shards_cover_every_row(db: &serde_yaml::Value) -> bool {
    let shards: Option<Vec<u64>> = db["strategy"]["matrix"]["shard"]
        .as_sequence()
        .map(|s| s.iter().filter_map(serde_yaml::Value::as_u64).collect());
    let count = number_at(db, &["env", "SEMAPHORE_SHARD_COUNT"]).unwrap_or(0);
    count > 0
        && shards == Some((0..count).collect())
        && compact(str_at(db, &["env", "SEMAPHORE_SHARD_INDEX"])) == "${{matrix.shard}}"
}

/// True when the ratchet step gates the run.
///
/// It is one plain `coverage.py ratchet` command against the committed
/// baseline. `--update` rewrites the baseline and always passes, so it is
/// rejected.
fn ratchets(step: &serde_yaml::Value) -> bool {
    let run = str_at(step, &["run"]).trim();
    let tokens: Vec<&str> = run.split_whitespace().collect();
    run.starts_with(&format!("python3 {SCRIPT} ratchet "))
        && plain(run)
        && has_pair(&tokens, ["--baseline", BASELINE])
        && !tokens.contains(&"--update")
        && str_at(step, &["if"]) == RATCHET_IF
        && !soft(step)
}

#[test]
fn coverage_nightly_runs_on_a_cron_and_alerts() {
    let doc = parse_workflow(NIGHTLY);
    assert!(
        !workflow_crons(&doc).is_empty(),
        "{NIGHTLY} must have an `on.schedule` cron"
    );
    assert!(
        alerts_on_scheduled_failure(&doc["jobs"]["alert"], &["unit", "db", "report"]),
        "the `alert` job must open an issue when a scheduled run fails"
    );
}

/// `-Zcoverage-options=branch` fails on stable, and `rust-toolchain.toml`
/// pins stable. `RUSTUP_TOOLCHAIN` wins over that file. A job `env` cannot
/// read `env`, so the pin is set once for the whole workflow.
#[test]
fn test_jobs_use_a_dated_nightly_with_branch_coverage() {
    let doc = parse_workflow(NIGHTLY);
    let pin = str_at(&doc, &["env", "RUSTUP_TOOLCHAIN"]);
    assert!(
        dated_nightly(pin),
        "{NIGHTLY} must pin `env.RUSTUP_TOOLCHAIN: nightly-YYYY-MM-DD`; found {pin:?}"
    );
    for name in ["unit", "db"] {
        let job = &doc["jobs"][name];
        assert!(
            runs_on_every_schedule(job),
            "the `{name}` job must have no `continue-on-error` and no `if` but the draft-skip"
        );
        assert!(
            uses_the_coverage_toolchain(job),
            "the `{name}` job must install `env.RUSTUP_TOOLCHAIN` with llvm-tools-preview \
             and must not override it"
        );
        assert!(
            exports_lcov_always(job),
            "the `{name}` job must export JSON, convert it with {SCRIPT} and upload a \
             `{LCOV_PREFIX}*` artifact, each with `if: always()`"
        );
    }
    assert!(
        runs_the_lib_tests_instrumented(&doc["jobs"]["unit"]),
        "one ungated `unit` step must run `cargo llvm-cov --branch` over the autumn-harvest \
         lib tests, as one plain command"
    );
}

#[test]
fn db_shards_run_every_linux_row_under_coverage() {
    let doc = parse_workflow(NIGHTLY);
    let db = &doc["jobs"]["db"];
    assert!(
        runs_the_linux_rows_instrumented(db),
        "one ungated `db` step must run exactly these lines: {DB_RUN:?}"
    );
    assert!(
        shards_cover_every_row(db),
        "the `db` matrix must be exactly 0..SEMAPHORE_SHARD_COUNT, and each shard must set \
         SEMAPHORE_SHARD_INDEX to its own `matrix.shard`"
    );
}

#[test]
fn report_job_publishes_and_ratchets() {
    let doc = parse_workflow(NIGHTLY);
    let report = &doc["jobs"]["report"];
    assert!(
        job_needs(report) == BTreeSet::from(["unit", "db"])
            && str_at(report, &["if"]) == report_if()
            && !soft(report),
        "the `report` job must need `unit` and `db`, run with `if: {}`, and not set \
         `continue-on-error`",
        report_if()
    );
    let steps = steps(report);
    let position = |pred: &dyn Fn(&serde_yaml::Value) -> bool| steps.iter().position(|s| pred(s));
    let download = position(&|s| {
        str_at(s, &["uses"]).starts_with("actions/download-artifact@")
            && str_at(s, &["with", "pattern"]) == format!("{LCOV_PREFIX}*")
    });
    let summary = position(&|s| {
        let run = str_at(s, &["run"]);
        run.contains(&format!("{SCRIPT} merge"))
            && run.contains(&format!("{SCRIPT} summary"))
            && run.contains(&format!("--baseline {BASELINE}"))
            && run.contains("$GITHUB_STEP_SUMMARY")
    });
    let upload = position(&|s| {
        str_at(s, &["uses"]).starts_with("actions/upload-artifact@")
            && str_at(s, &["with", "name"]) == "coverage-report"
            && str_at(s, &["if"]).contains("always()")
    });
    let ratchet = position(&|s| ratchets(s));
    assert!(
        matches!((download, summary, upload, ratchet), (Some(d), Some(s), Some(u), Some(r)) if d < s && s < u && s < r),
        "the `report` job must download the {LCOV_PREFIX}* artifacts, merge and summarize \
         them into $GITHUB_STEP_SUMMARY, upload `coverage-report` with `if: always()`, and \
         ratchet against {BASELINE} with `if: {RATCHET_IF}`"
    );
}

/// Self-test: each way to soften the ratchet fails the check.
#[test]
fn ratchet_check_rejects_a_soft_gate() {
    let run = "python3 .github/ci/coverage.py ratchet --baseline .github/ci/coverage-baseline.json --summary s.json";
    let step = |run: &str, condition: &str, extra: &str| {
        let text = format!("run: '{run}'\nif: \"{condition}\"\n{extra}");
        ratchets(&parse_workflow_text(&text).expect("synthetic step must parse"))
    };
    assert!(step(run, RATCHET_IF, ""));
    assert!(
        !step(run, RATCHET_IF, "continue-on-error: true\n"),
        "a soft ratchet"
    );
    let other = run.replace("coverage-baseline.json", "other.json");
    assert!(!step(&other, RATCHET_IF, ""), "another baseline");
    let update = run.replace(" ratchet ", " ratchet --update ");
    assert!(!step(&update, RATCHET_IF, ""), "`--update` always passes");
    assert!(
        !step(&format!("{run} || true"), RATCHET_IF, ""),
        "a swallowed failure"
    );
    assert!(
        !step(&format!("echo {run}"), RATCHET_IF, ""),
        "a command that ratchets nothing"
    );
    let half = RATCHET_IF.replace(" && needs.db.result == 'success'", "");
    assert!(!step(run, &half, ""), "a ratchet over a failed shard");
    let either = RATCHET_IF.replace(" && ", " || ");
    assert!(!step(run, &either, ""), "a ratchet over a failed shard");
    let pr_only = format!("{RATCHET_IF} && github.event_name == 'pull_request'");
    assert!(!step(run, &pr_only, ""), "a ratchet that skips the cron");
}

/// Self-test: each way to break the instrumented `db` step fails the check.
#[test]
fn db_step_check_rejects_a_broken_run() {
    let job = |lines: &[&str], extra: &str| {
        let body: String = lines.iter().map(|l| format!("          {l}\n")).collect();
        let text = format!("steps:\n  - name: run\n    {extra}run: |\n{body}");
        let text = text.replace("\n          ", "\n        ");
        runs_the_linux_rows_instrumented(
            &parse_workflow_text(&text).expect("synthetic job must parse"),
        )
    };
    assert!(job(DB_RUN, ""));
    assert!(!job(DB_RUN, "if: false\n    "), "a gated step");
    assert!(
        !job(&[DB_RUN[0], DB_RUN[2], DB_RUN[3]], ""),
        "no instrumentation"
    );
    assert!(
        !job(&[DB_RUN[0], DB_RUN[1], DB_RUN[3], DB_RUN[2]], ""),
        "a clean after the suites"
    );
    let swallowed = format!("{} || true", DB_RUN[3]);
    assert!(
        !job(&[DB_RUN[0], DB_RUN[1], DB_RUN[2], &swallowed], ""),
        "a swallowed failure"
    );
}

/// Self-test: the shard check needs every index once.
#[test]
fn shard_check_needs_every_index() {
    let db = |shards: &str, count: &str| {
        let text = format!(
            "strategy:\n  matrix:\n    shard: {shards}\nenv:\n  SEMAPHORE_SHARD_COUNT: {count}\n  \
             SEMAPHORE_SHARD_INDEX: ${{{{ matrix.shard }}}}\n"
        );
        shards_cover_every_row(&parse_workflow_text(&text).expect("synthetic job must parse"))
    };
    assert!(db("[0, 1, 2]", "\"3\""));
    assert!(db("[0, 1, 2]", "3"), "an unquoted count");
    assert!(!db("[1, 2, 3]", "\"3\""), "index 0 missing");
    assert!(!db("[0, 1, 1]", "\"3\""), "a repeated index");
    assert!(!db("[0, 1]", "\"3\""), "a short matrix");
}

/// Self-test: a job gate that skips the cron fails the check.
#[test]
fn job_gate_check_rejects_a_skipped_cron() {
    let job = |extra: &str| {
        let text = format!("runs-on: x\n{extra}");
        runs_on_every_schedule(&parse_workflow_text(&text).expect("synthetic job must parse"))
    };
    assert!(job(""));
    assert!(job(&format!("if: \"{DRAFT_SKIP}\"\n")));
    assert!(!job("if: github.event_name == 'pull_request'\n"));
    assert!(!job("continue-on-error: true\n"));
}

/// The JSON number at `key`, or `None` for `null` or a missing key.
fn floor(module: &serde_json::Value, key: &str) -> Option<f64> {
    module.get(key).and_then(serde_json::Value::as_f64)
}

#[test]
fn baseline_lists_the_critical_modules_with_recorded_floors() {
    let path = repo_root().join(BASELINE);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {BASELINE}: {e}"));
    let doc: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{BASELINE} is not JSON: {e}"));
    let tolerance = doc["tolerance"].as_f64().unwrap_or(-1.0);
    assert!(
        tolerance > 0.0 && tolerance <= 2.0,
        "{BASELINE} `tolerance` must be in (0, 2] points; found {tolerance}"
    );
    let modules = doc["modules"]
        .as_object()
        .unwrap_or_else(|| panic!("{BASELINE} must have a `modules` object"));
    let listed: BTreeSet<&str> = modules.keys().map(String::as_str).collect();
    assert_eq!(
        listed,
        CRITICAL.iter().copied().collect(),
        "{BASELINE} must list exactly the critical modules of issue #1818"
    );
    for (name, module) in modules {
        assert!(
            repo_root().join(name).is_file(),
            "{BASELINE} names {name}, which does not exist. Move its floor with the file."
        );
        for key in ["lines", "branches"] {
            let value = floor(module, key);
            assert!(
                value.is_some_and(|v| (0.0..=100.0).contains(&v)),
                "{BASELINE} must record a `{key}` floor in [0, 100] for {name}; found {value:?}"
            );
        }
    }
}

#[test]
fn lint_runs_the_script_self_test_and_prs_run_the_pipeline() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let self_test = format!("python3 {SCRIPT} --self-test");
    let lint = steps(&ci["jobs"]["lint"])
        .iter()
        .any(|s| str_at(s, &["run"]) == self_test && ungated(s));
    assert!(lint, "the ci.yml `lint` job must run `{self_test}`");

    let doc = parse_workflow(NIGHTLY);
    let paths: BTreeSet<&str> = doc["on"]["pull_request"]["paths"]
        .as_sequence()
        .into_iter()
        .flatten()
        .filter_map(serde_yaml::Value::as_str)
        .collect();
    for file in [NIGHTLY, SCRIPT, BASELINE] {
        assert!(
            paths.contains(file),
            "{NIGHTLY} `on.pull_request.paths` must list {file}"
        );
    }
}
