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

use super::ci_run_coverage::{
    NO_FULL_RUN_FLAGS, SHELL_OPERATORS, parse_workflow, parse_workflow_text, repo_root, ungated,
};

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

/// The job whose `if:` the model jobs copy: a draft skip and a docs-only skip.
const GATE_TEMPLATE_JOB: &str = "msrv";

/// The `RUSTFLAGS` value that applies to a step: step env first, then job env.
fn rustflags<'a>(job: &'a Value, step: &'a Value) -> Option<&'a str> {
    let read = |v: &'a Value| {
        v.get("env")
            .and_then(|env| env.get("RUSTFLAGS"))
            .and_then(Value::as_str)
    };
    read(step).or_else(|| read(job))
}

/// True when `run` is one plain `cargo test` command that runs all of `target`.
///
/// Text that only contains the right arguments, such as an `echo` or a
/// `--no-run` build, runs no model.
fn is_full_run_of(run: &str, target: &str) -> bool {
    let run = run.trim();
    let words: Vec<&str> = run.split_whitespace().collect();
    let joined = format!("--test={target}");
    let names_target = words
        .windows(2)
        .any(|pair| pair[0] == "--test" && pair[1] == target)
        || words.contains(&joined.as_str());
    run.starts_with("cargo test ")
        && names_target
        && !SHELL_OPERATORS.iter().any(|op| run.contains(op))
        && !words
            .iter()
            .any(|word| NO_FULL_RUN_FLAGS.iter().any(|flag| word.starts_with(flag)))
}

/// Why `doc` does not run `model` on every PR, or `None` when it does.
///
/// The job must have the same `if:` as [`GATE_TEMPLATE_JOB`] and must not set
/// `continue-on-error`. One ungated step in it must run the whole target
/// with RUSTFLAGS that hold the target's `--cfg`.
fn model_job_defect(doc: &Value, model: &ModelJob) -> Option<String> {
    let jobs = doc.get("jobs");
    let Some(job) = jobs.and_then(|j| j.get(model.job)) else {
        return Some(format!("ci.yml must define a `{}` job", model.job));
    };
    let template_if = jobs
        .and_then(|j| j.get(GATE_TEMPLATE_JOB))
        .and_then(|j| j.get("if"));
    if job.get("if") != template_if {
        return Some(format!(
            "job `{}` must use the same `if:` as `{GATE_TEMPLATE_JOB}` (draft and docs-only \
             skips only). Any other condition can keep it from running on a PR.",
            model.job
        ));
    }
    if job
        .get("continue-on-error")
        .is_some_and(|v| v.as_bool() != Some(false))
    {
        return Some(format!(
            "job `{}` must not set `continue-on-error`, or a model failure stays green",
            model.job
        ));
    }
    let cfg = format!("--cfg {}", model.cfg);
    let runs = job
        .get("steps")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter(|step| ungated(step))
        .any(|step| {
            step.get("run")
                .and_then(Value::as_str)
                .is_some_and(|run| is_full_run_of(run, model.target))
                && rustflags(job, step).is_some_and(|flags| flags.contains(&cfg))
        });
    (!runs).then(|| {
        format!(
            "job `{}` must have an ungated step that is one plain `cargo test ... --test {}` \
             command, with RUSTFLAGS containing `{cfg}`. Without the cfg the target compiles \
             to an empty crate and runs nothing.",
            model.job, model.target
        )
    })
}

#[test]
fn ci_yml_runs_on_pull_requests() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    let on = ci.get("on").expect("ci.yml must declare `on:`");
    assert!(
        on.get("pull_request").is_some(),
        "ci.yml must trigger on `pull_request`, so the model jobs run on every PR"
    );
}

#[test]
fn each_model_target_runs_on_every_pr_under_its_cfg() {
    let ci = parse_workflow(".github/workflows/ci.yml");
    for model in MODEL_JOBS {
        if let Some(defect) = model_job_defect(&ci, model) {
            panic!("{defect} (issue #1800)");
        }
    }
}

/// A synthetic workflow: a template `msrv` job and a `loom` job with one step.
fn synthetic(loom_if: &str, job_extra: &str, run: &str, step_extra: &str) -> Value {
    let gate = "(github.event_name != 'pull_request' || github.event.pull_request.draft == false)";
    let text = format!(
        "jobs:\n  msrv:\n    if: \"{gate}\"\n    steps: []\n  loom:\n    if: \"{loom_if}\"\n\
         {job_extra}    steps:\n      - run: '{run}'\n{step_extra}"
    );
    parse_workflow_text(&text).expect("synthetic workflow must parse")
}

/// Self-test: a gate, a soft failure or a run that tests nothing must not
/// count.
#[test]
fn model_job_check_rejects_gated_and_empty_runs() {
    let model = &MODEL_JOBS[0];
    let gate = "(github.event_name != 'pull_request' || github.event.pull_request.draft == false)";
    let run = "cargo test -p autumn-harvest --no-default-features --test loom_models --release";
    let env = "    env:\n      RUSTFLAGS: \"--cfg loom\"\n";
    let step_env = "        env:\n          RUSTFLAGS: \"--cfg loom\"\n";

    let accepted = [
        ("job RUSTFLAGS", synthetic(gate, env, run, "")),
        ("step RUSTFLAGS", synthetic(gate, "", run, step_env)),
        (
            "`continue-on-error: false` is the default",
            synthetic(gate, env, run, "        continue-on-error: false\n"),
        ),
    ];
    for (case, doc) in &accepted {
        assert_eq!(model_job_defect(doc, model), None, "`{case}` must count");
    }

    let dispatch = "github.event_name == 'workflow_dispatch'";
    let renamed = run.replace("loom_models", "loom_model");
    let rejected = [
        ("dispatch-only job", synthetic(dispatch, env, run, "")),
        ("no RUSTFLAGS", synthetic(gate, "", run, "")),
        (
            "wrong cfg",
            synthetic(
                gate,
                "    env:\n      RUSTFLAGS: \"--cfg shuttle\"\n",
                run,
                "",
            ),
        ),
        (
            "job continue-on-error",
            synthetic(
                gate,
                &format!("    continue-on-error: true\n{env}"),
                run,
                "",
            ),
        ),
        (
            "job continue-on-error expression",
            synthetic(
                gate,
                &format!("    continue-on-error: ${{{{ true }}}}\n{env}"),
                run,
                "",
            ),
        ),
        ("step if", synthetic(gate, env, run, "        if: false\n")),
        (
            "step continue-on-error",
            synthetic(gate, env, run, "        continue-on-error: true\n"),
        ),
        ("renamed target", synthetic(gate, env, &renamed, "")),
    ];
    for (case, doc) in &rejected {
        assert!(
            model_job_defect(doc, model).is_some(),
            "`{case}` must not count as a run"
        );
    }

    for masked in [
        format!("{run} || true"),
        format!("{run}; exit 0"),
        format!("echo {}", run.trim_start_matches("cargo test ")),
        format!("{run} --no-run"),
        format!("{run} -- --list"),
        format!("{run} -- --skip circuit_breaker"),
    ] {
        assert!(
            model_job_defect(&synthetic(gate, env, &masked, ""), model).is_some(),
            "`{masked}` hides a failure or runs no model, so it must not count"
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

/// The issue asks for at least one Shuttle test per module. The guard matches
/// `#[test]` functions by name prefix, so a renamed test must keep the prefix.
#[test]
fn shuttle_target_tests_slot_tuner_and_heartbeat() {
    let path = repo_root().join("autumn-harvest/tests/shuttle_models.rs");
    let source =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let lines: Vec<&str> = source.lines().map(str::trim).collect();
    for prefix in ["fn slot_tuner_", "fn heartbeat_"] {
        let tests = lines
            .windows(2)
            .filter(|pair| pair[0] == "#[test]" && pair[1].starts_with(prefix))
            .count();
        assert!(
            tests > 0,
            "{} must define a `#[test]` whose name starts with `{}`",
            path.display(),
            prefix.trim_start_matches("fn ")
        );
    }
}
