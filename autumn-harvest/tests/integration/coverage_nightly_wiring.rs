//! Guard for the nightly coverage workflow (issue #1818). No DB, no feature
//! gate.
//!
//! `coverage-nightly.yml` must measure the unit tests and every `linux`
//! manifest row on a cron, with branch data. It must publish the report also
//! after a failed shard. The ratchet must gate each critical module against
//! its floor in `.github/ci/coverage-baseline.json`.

use std::collections::BTreeSet;

use super::ci_run_coverage::{
    alerts_on_scheduled_failure, dated_nightly, job_needs, parse_workflow, parse_workflow_text,
    repo_root, soft, steps, str_at, ungated, workflow_crons,
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

/// True when a test job installs the pinned nightly and its LLVM tools, and
/// does not override the workflow `RUSTUP_TOOLCHAIN`.
fn uses_the_coverage_toolchain(job: &serde_yaml::Value) -> bool {
    let installs = steps(job).iter().any(|s| {
        str_at(s, &["uses"]).starts_with("dtolnay/rust-toolchain@")
            && str_at(s, &["with", "toolchain"]) == "${{ env.RUSTUP_TOOLCHAIN }}"
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

/// True when one ungated step sources `show-env --branch` and then runs the
/// `linux` manifest rows. A second step would not see the exported variables.
fn runs_the_linux_rows_instrumented(job: &serde_yaml::Value) -> bool {
    steps(job).iter().any(|s| {
        let run = str_at(s, &["run"]);
        let env = run.find("cargo llvm-cov show-env");
        let rows = run.find("run-suites.sh run linux");
        ungated(s) && run.contains("--branch") && matches!((env, rows), (Some(e), Some(r)) if e < r)
    })
}

/// True when the ratchet step gates the run.
///
/// It must run only when both test jobs passed: a failed test cuts coverage
/// short, so a drop then is noise. It must not soften its own failure.
fn ratchets(step: &serde_yaml::Value) -> bool {
    let run = str_at(step, &["run"]);
    let condition = str_at(step, &["if"]);
    run.contains(&format!("{SCRIPT} ratchet"))
        && run.contains(&format!("--baseline {BASELINE}"))
        && condition.contains("needs.unit.result == 'success'")
        && condition.contains("needs.db.result == 'success'")
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
            uses_the_coverage_toolchain(job),
            "the `{name}` job must install `env.RUSTUP_TOOLCHAIN` with llvm-tools-preview \
             and must not override it"
        );
        assert!(
            exports_lcov_always(job),
            "the `{name}` job must export JSON, convert it with {SCRIPT} and upload a \
             `{LCOV_PREFIX}*` artifact, each with `if: always()`"
        );
        assert!(
            !soft(job),
            "the `{name}` job must not set `continue-on-error`"
        );
    }
    let unit = steps(&doc["jobs"]["unit"]).into_iter().any(|s| {
        let run = str_at(s, &["run"]);
        run.contains("cargo llvm-cov")
            && run.contains("--branch")
            && run.contains("-p autumn-harvest")
            && run.contains("--lib")
    });
    assert!(
        unit,
        "the `unit` job must run `cargo llvm-cov --branch` over the autumn-harvest lib tests"
    );
}

#[test]
fn db_shards_run_every_linux_row_under_coverage() {
    let doc = parse_workflow(NIGHTLY);
    let db = &doc["jobs"]["db"];
    assert!(
        runs_the_linux_rows_instrumented(db),
        "one ungated `db` step must source `cargo llvm-cov show-env --branch` and then run \
         `run-suites.sh run linux`"
    );
    let shards = db["strategy"]["matrix"]["shard"]
        .as_sequence()
        .map_or(0, Vec::len);
    let count: usize = str_at(db, &["env", "SEMAPHORE_SHARD_COUNT"])
        .parse()
        .unwrap_or(0);
    assert!(
        shards > 0 && shards == count,
        "`db` must have one matrix shard per SEMAPHORE_SHARD_COUNT; found {shards} and {count}"
    );
    assert_eq!(
        str_at(db, &["env", "SEMAPHORE_SHARD_INDEX"]),
        "${{ matrix.shard }}",
        "each `db` shard must run its own slice of the manifest"
    );
}

#[test]
fn report_job_publishes_and_ratchets() {
    let doc = parse_workflow(NIGHTLY);
    let report = &doc["jobs"]["report"];
    assert!(
        job_needs(report) == BTreeSet::from(["unit", "db"])
            && str_at(report, &["if"]).contains("always()"),
        "the `report` job must need `unit` and `db` and run with `if: always()`"
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
         ratchet against {BASELINE} when both test jobs passed"
    );
}

/// Self-test: each way to soften the ratchet fails the check.
#[test]
fn ratchet_check_rejects_a_soft_gate() {
    let good = "run: python3 .github/ci/coverage.py ratchet --baseline .github/ci/coverage-baseline.json --summary s.json\n\
                if: needs.unit.result == 'success' && needs.db.result == 'success'\n";
    let parse = |text: &str| parse_workflow_text(text).expect("synthetic step must parse");
    assert!(ratchets(&parse(good)));
    let soft = format!("{good}continue-on-error: true\n");
    assert!(!ratchets(&parse(&soft)), "a soft ratchet");
    let other = good.replace("coverage-baseline.json", "other.json");
    assert!(!ratchets(&parse(&other)), "another baseline");
    let half = good.replace(" && needs.db.result == 'success'", "");
    assert!(!ratchets(&parse(&half)), "a ratchet over a failed shard");
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
