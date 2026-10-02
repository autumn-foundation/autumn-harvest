//! CI wiring guard for the concurrency model targets (issue #1800).
//!
//! The loom and Shuttle targets compile to empty crates under a normal build.
//! A normal `cargo test` therefore never runs them. Only a CI job that sets the
//! matching `--cfg` runs them, so this guard checks that such a job exists.
//!
//! This is a wiring guard. It checks that `ci.yml` runs each target on pull
//! requests. It does not check that the models pass. If a job is renamed,
//! update this test. Do not delete it.

use serde_yaml::Value;

use super::ci_run_coverage::{parse_workflow, repo_root};

/// One model target and the job that must run it.
struct ModelJob {
    /// The `ci.yml` job id.
    job: &'static str,
    /// The `--test` target that the job runs.
    target: &'static str,
    /// The custom cfg that turns the target on.
    cfg: &'static str,
}

const MODEL_JOBS: &[ModelJob] = &[
    ModelJob {
        job: "loom",
        target: "loom_models",
        cfg: "loom",
    },
    ModelJob {
        job: "shuttle",
        target: "shuttle_models",
        cfg: "shuttle",
    },
];

fn ci() -> Value {
    parse_workflow(".github/workflows/ci.yml")
}

fn ci_job<'a>(ci: &'a Value, id: &str) -> &'a Value {
    ci.get("jobs")
        .and_then(|jobs| jobs.get(id))
        .unwrap_or_else(|| panic!("ci.yml must define a `{id}` job (issue #1800)"))
}

/// The `RUSTFLAGS` value that applies to a step: step env first, then job env.
fn rustflags<'a>(job: &'a Value, step: &'a Value) -> Option<&'a str> {
    let read = |v: &'a Value| {
        v.get("env")
            .and_then(|env| env.get("RUSTFLAGS"))
            .and_then(Value::as_str)
    };
    read(step).or_else(|| read(job))
}

#[test]
fn ci_yml_runs_on_pull_requests() {
    let ci = ci();
    let on = ci.get("on").expect("ci.yml must declare `on:`");
    assert!(
        on.get("pull_request").is_some(),
        "ci.yml must trigger on `pull_request`, so the model jobs run on every PR"
    );
}

#[test]
fn each_model_target_runs_in_ci_under_its_cfg() {
    let ci = ci();
    for model in MODEL_JOBS {
        let job = ci_job(&ci, model.job);
        let steps = job
            .get("steps")
            .and_then(Value::as_sequence)
            .unwrap_or_else(|| panic!("job `{}` must have steps", model.job));
        let needle = format!("--test {}", model.target);
        let cfg = format!("--cfg {}", model.cfg);
        let runs = steps.iter().any(|step| {
            let Some(run) = step.get("run").and_then(Value::as_str) else {
                return false;
            };
            run.contains("cargo test")
                && run.contains(&needle)
                && rustflags(job, step).is_some_and(|flags| flags.contains(&cfg))
        });
        assert!(
            runs,
            "job `{}` must run `cargo test ... {needle}` with RUSTFLAGS containing `{cfg}`. \
             Without the cfg the target compiles to an empty crate and runs nothing.",
            model.job
        );
    }
}

/// A job that only runs on manual dispatch, or that may fail without a red
/// check, is not a PR gate.
#[test]
fn model_jobs_are_pr_gates() {
    let ci = ci();
    for model in MODEL_JOBS {
        let job = ci_job(&ci, model.job);
        let condition = job.get("if").and_then(Value::as_str).unwrap_or_default();
        assert!(
            !condition.contains("workflow_dispatch"),
            "job `{}` must not be limited to manual dispatch: `if: {condition}`",
            model.job
        );
        assert_ne!(
            job.get("continue-on-error").and_then(Value::as_bool),
            Some(true),
            "job `{}` must not set `continue-on-error: true`, or a model failure stays green",
            model.job
        );
    }
}

/// The manual-only `loom.yml` workflow is replaced by the `loom` job in
/// `ci.yml`. Two copies would drift apart.
#[test]
fn manual_only_loom_workflow_is_gone() {
    let path = repo_root().join(".github/workflows/loom.yml");
    assert!(
        !path.exists(),
        "{} must not exist. The `loom` job in ci.yml replaces it (issue #1800).",
        path.display()
    );
}

/// The issue asks for at least one Shuttle model per module. The guard reads
/// test names, so a renamed model must keep its module prefix.
#[test]
fn shuttle_target_models_slot_tuner_and_heartbeat() {
    let path = repo_root().join("autumn-harvest/tests/shuttle_models.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    for prefix in ["fn slot_tuner_", "fn heartbeat_"] {
        assert!(
            source.contains(prefix),
            "{} must define a Shuttle model whose name starts with `{}`",
            path.display(),
            prefix.trim_start_matches("fn ")
        );
    }
}
