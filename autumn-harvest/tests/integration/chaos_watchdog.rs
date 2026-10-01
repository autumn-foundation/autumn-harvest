//! Chaos nightly watchdog guard (issue #1790). No DB, no feature gate.
//!
//! `chaos.yml` did not parse from its first commit, so its nightly run never
//! fired, and no alert told anyone. The watchdog workflow is a separate file,
//! so a defect in `chaos.yml` cannot also stop the alert.
//!
//! The behaviour tests run `.github/ci/chaos-watchdog.sh` with a stub `gh` on
//! `PATH`. The stub records each call and returns canned JSON. The script reads
//! that JSON with the real `jq`, so the tests also check its filters. These
//! tests run on Linux only. The script uses GNU `date` and `jq`, which the
//! `ubuntu-latest` runner has.

use super::ci_run_coverage::{parse_workflow, repo_root, workflow_crons, workflow_run_commands};

const WATCHDOG_WORKFLOW: &str = ".github/workflows/chaos-watchdog.yml";
const WATCHDOG_SCRIPT: &str = ".github/ci/chaos-watchdog.sh";

/// The alert issue title. The script finds its open alert by this exact title.
#[cfg(target_os = "linux")]
const ALERT_TITLE: &str = "Chaos nightly: no successful scheduled run in 48 h";

/// The title of the issue for a failed watchdog run.
#[cfg(target_os = "linux")]
const WATCHDOG_FAILED_TITLE: &str = "Chaos watchdog: a watchdog run failed";

/// The watchdog must fire on its own cron and run the script with the
/// permissions that the script needs.
#[test]
fn watchdog_workflow_runs_the_script_daily() {
    let doc = parse_workflow(WATCHDOG_WORKFLOW);
    assert!(
        !workflow_crons(&doc).is_empty(),
        "{WATCHDOG_WORKFLOW} must have an `on.schedule` cron"
    );

    let perm = |key: &str| {
        doc.get("permissions")
            .and_then(|p| p.get(key))
            .and_then(serde_yaml::Value::as_str)
    };
    assert_eq!(perm("actions"), Some("read"), "the script lists runs");
    assert_eq!(perm("issues"), Some("write"), "the script opens issues");

    let runs = workflow_run_commands(&doc);
    assert!(
        runs.iter().any(|run| run.contains(WATCHDOG_SCRIPT)),
        "{WATCHDOG_WORKFLOW} must run {WATCHDOG_SCRIPT}; found {runs:?}"
    );
}

/// A job timeout cancels the job, and `if: failure()` steps do not run after
/// a cancel. A step timeout is a step failure, so the report step still runs.
/// So each step needs its own timeout, and together they must fit inside the
/// job timeout. A hung checkout then fails its step and is reported.
#[test]
fn watchdog_steps_time_out_before_the_job() {
    let doc = parse_workflow(WATCHDOG_WORKFLOW);
    let jobs = doc
        .get("jobs")
        .and_then(serde_yaml::Value::as_mapping)
        .expect("jobs");
    for (name, job) in jobs {
        let job_limit = job
            .get("timeout-minutes")
            .and_then(serde_yaml::Value::as_u64)
            .unwrap_or_else(|| panic!("job {name:?} must set `timeout-minutes`"));
        let steps = job
            .get("steps")
            .and_then(serde_yaml::Value::as_sequence)
            .expect("steps");
        let mut total = 0;
        for step in steps {
            let limit = step
                .get("timeout-minutes")
                .and_then(serde_yaml::Value::as_u64)
                .unwrap_or_else(|| panic!("step {step:?} must set `timeout-minutes`"));
            total += limit;
        }
        assert!(
            total < job_limit,
            "job {name:?}: the step timeouts sum to {total} min, which must be under \
             the {job_limit}-min job timeout"
        );
    }
}

/// GitHub tells only the last editor of a cron when a scheduled run fails.
/// So a red watchdog run is silent, as the dead nightly was. A final step must
/// put that failure on the alert issue.
#[test]
fn watchdog_workflow_reports_its_own_failure() {
    let doc = parse_workflow(WATCHDOG_WORKFLOW);
    let reports = doc
        .get("jobs")
        .and_then(serde_yaml::Value::as_mapping)
        .into_iter()
        .flat_map(|jobs| jobs.values())
        .filter_map(|job| job.get("steps").and_then(serde_yaml::Value::as_sequence))
        .flatten()
        .any(|step| {
            let on_failure = step
                .get("if")
                .and_then(serde_yaml::Value::as_str)
                .is_some_and(|cond| cond.contains("failure()") && !cond.contains('!'));
            let run = step.get("run").and_then(serde_yaml::Value::as_str);
            on_failure
                && run.is_some_and(|r| r.contains(WATCHDOG_SCRIPT) && r.contains("self-failed"))
        });
    assert!(
        reports,
        "{WATCHDOG_WORKFLOW} must have an `if: failure()` step that runs \
         `{WATCHDOG_SCRIPT} self-failed`"
    );
}

/// The canned answers that the stub `gh` gives.
#[cfg(target_os = "linux")]
struct Stub {
    /// JSON body of the run query, or `None` to fail as an API error does.
    runs: Option<&'static str>,
    /// JSON body of the issue query, or `None` to fail as an API error does.
    issues: Option<String>,
    /// The script argument, if any.
    arg: Option<&'static str>,
}

#[cfg(target_os = "linux")]
impl Stub {
    /// A run query that finds `count` successful runs.
    const fn runs(count: u32) -> &'static str {
        match count {
            0 => r#"{"total_count":0,"workflow_runs":[]}"#,
            1 => r#"{"total_count":1,"workflow_runs":[{}]}"#,
            _ => r#"{"total_count":2,"workflow_runs":[{}]}"#,
        }
    }

    /// An issue query that finds one open issue with this number and title.
    fn issue(number: u32, title: &str) -> String {
        format!(r#"[{{"number":{number},"title":"{title}"}}]"#)
    }

    /// An issue query that finds no open issue.
    fn no_issue() -> String {
        "[]".to_string()
    }
}

/// The result of one watchdog run against the stub.
#[cfg(target_os = "linux")]
struct Outcome {
    success: bool,
    /// One line for each `gh` call, with its arguments.
    calls: Vec<String>,
}

#[cfg(target_os = "linux")]
impl Outcome {
    fn called(&self, prefix: &str) -> bool {
        self.calls.iter().any(|c| c.starts_with(prefix))
    }

    /// True when the script wrote to an issue (create, comment or close).
    fn wrote_an_issue(&self) -> bool {
        ["issue create", "issue comment", "issue close"]
            .iter()
            .any(|p| self.called(p))
    }
}

/// Runs the watchdog script with a stub `gh` first on `PATH`.
#[cfg(target_os = "linux")]
fn run_watchdog(stub: &Stub) -> Outcome {
    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join(WATCHDOG_SCRIPT));
    if let Some(arg) = stub.arg {
        cmd.arg(arg);
    }
    run_stubbed(stub, cmd)
}

/// Runs `cmd` with a stub `gh` first on `PATH` and the runner variables set.
#[cfg(target_os = "linux")]
fn run_stubbed(stub: &Stub, mut cmd: std::process::Command) -> Outcome {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("gh.log");
    let gh = dir.path().join("gh");
    std::fs::write(
        &gh,
        r#"#!/usr/bin/env bash
printf '%s\n' "$*" >> "$STUB_LOG"
case "$1 $2" in
  "api "*)
    if [ "$STUB_RUNS_FAIL" = 1 ]; then echo "HTTP 502" >&2; exit 1; fi
    printf '%s\n' "$STUB_RUNS" ;;
  "issue list")
    if [ "$STUB_ISSUES_FAIL" = 1 ]; then echo "HTTP 502" >&2; exit 1; fi
    printf '%s\n' "$STUB_ISSUES" ;;
esac
"#,
    )
    .expect("write stub gh");
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).expect("chmod stub gh");

    let path = format!(
        "{}:{}",
        dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = cmd
        .env("PATH", path)
        .env("GITHUB_REPOSITORY", "owner/repo")
        .env("GITHUB_SERVER_URL", "https://github.com")
        .env("GITHUB_RUN_ID", "4242")
        .env("STUB_LOG", &log)
        .env("STUB_RUNS", stub.runs.unwrap_or_default())
        .env(
            "STUB_RUNS_FAIL",
            if stub.runs.is_none() { "1" } else { "0" },
        )
        .env("STUB_ISSUES", stub.issues.as_deref().unwrap_or_default())
        .env(
            "STUB_ISSUES_FAIL",
            if stub.issues.is_none() { "1" } else { "0" },
        )
        .output()
        .expect("run bash");
    let calls = std::fs::read_to_string(&log)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect();
    Outcome {
        success: output.status.success(),
        calls,
    }
}

/// The run query asks only for successful scheduled runs of `chaos.yml` in
/// the last 48 h. A manual run on a branch must not hide a dead nightly.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_queries_successful_scheduled_chaos_runs_in_48_hours() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(1)),
        issues: Some(Stub::no_issue()),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    let api = out
        .calls
        .iter()
        .find(|c| c.starts_with("api "))
        .unwrap_or_else(|| panic!("no run query; calls: {:?}", out.calls));
    for needle in [
        "repos/owner/repo/actions/workflows/chaos.yml/runs",
        "event=schedule",
        "status=success",
        "created=>=",
    ] {
        assert!(api.contains(needle), "run query lacks {needle:?}: {api}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn watchdog_opens_an_issue_when_no_nightly_run_succeeded() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(0)),
        issues: Some(Stub::no_issue()),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue create"), "calls: {:?}", out.calls);
    assert!(!out.called("issue comment"), "calls: {:?}", out.calls);
}

/// The search is fuzzy. Only an exact title match is the open alert.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_ignores_an_open_issue_with_a_similar_title() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(0)),
        issues: Some(Stub::issue(5, &format!("{ALERT_TITLE} (old)"))),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue create"), "calls: {:?}", out.calls);
    assert!(!out.called("issue comment 5"), "calls: {:?}", out.calls);
}

/// A second alert goes on the open issue, so a long gap gives one issue.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_comments_on_the_open_issue_instead_of_a_duplicate() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(0)),
        issues: Some(Stub::issue(77, ALERT_TITLE)),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue comment 77"), "calls: {:?}", out.calls);
    assert!(!out.called("issue create"), "calls: {:?}", out.calls);
}

/// A stale open alert would teach people to ignore the next one.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_closes_the_open_issue_after_a_success() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(2)),
        issues: Some(Stub::issue(77, ALERT_TITLE)),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue close 77"), "calls: {:?}", out.calls);
    assert!(!out.called("issue create"), "calls: {:?}", out.calls);
}

#[cfg(target_os = "linux")]
#[test]
fn watchdog_is_silent_when_the_nightly_is_green() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(1)),
        issues: Some(Stub::no_issue()),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}

/// An API error must turn the watchdog run red. It must not read as zero
/// runs (a false alert) or as a green nightly (a missed alert). The failure
/// step then reports the red run.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_fails_closed_when_the_run_query_fails() {
    let out = run_watchdog(&Stub {
        runs: None,
        issues: Some(Stub::no_issue()),
        arg: None,
    });
    assert!(!out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}

/// A 404 body has no `total_count`. That is an error, not zero runs.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_fails_closed_when_the_run_query_has_no_count() {
    let out = run_watchdog(&Stub {
        runs: Some(r#"{"message":"Not Found"}"#),
        issues: Some(Stub::no_issue()),
        arg: None,
    });
    assert!(!out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}

#[cfg(target_os = "linux")]
#[test]
fn watchdog_fails_closed_when_the_issue_query_fails() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(0)),
        issues: None,
        arg: None,
    });
    assert!(!out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}

/// In `self-failed` mode the script reports the red watchdog run on its own
/// issue. It does not query runs, because that query can be the part that
/// failed. It must not use the nightly alert title, because the nightly can
/// be green.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_self_failed_mode_opens_its_own_issue() {
    let out = run_watchdog(&Stub {
        runs: None,
        issues: Some(Stub::issue(77, ALERT_TITLE)),
        arg: Some("self-failed"),
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(!out.called("api "), "calls: {:?}", out.calls);
    assert!(!out.called("issue comment 77"), "calls: {:?}", out.calls);
    let create = out
        .calls
        .iter()
        .find(|c| c.starts_with("issue create"))
        .unwrap_or_else(|| panic!("no issue opened; calls: {:?}", out.calls));
    assert!(
        create.contains(WATCHDOG_FAILED_TITLE) && !create.contains(ALERT_TITLE),
        "the issue must use the watchdog-failure title: {create}"
    );
    assert!(
        create.contains("actions/runs/4242"),
        "the issue must link the failed run: {create}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn watchdog_self_failed_mode_comments_on_its_open_issue() {
    let out = run_watchdog(&Stub {
        runs: None,
        issues: Some(Stub::issue(88, WATCHDOG_FAILED_TITLE)),
        arg: Some("self-failed"),
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue comment 88"), "calls: {:?}", out.calls);
    assert!(!out.called("issue create"), "calls: {:?}", out.calls);
}

/// A watchdog run that completes its query shows the watchdog works again.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_closes_its_failure_issue_after_a_clean_run() {
    let out = run_watchdog(&Stub {
        runs: Some(Stub::runs(1)),
        issues: Some(Stub::issue(88, WATCHDOG_FAILED_TITLE)),
        arg: None,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue close 88"), "calls: {:?}", out.calls);
}

/// Two runs that overlap can both find no open issue and both open one. A
/// concurrency group runs one watchdog job at a time. It must queue, not
/// cancel, because a cancelled run skips the `if: failure()` step.
#[test]
fn watchdog_workflow_runs_one_job_at_a_time() {
    let doc = parse_workflow(WATCHDOG_WORKFLOW);
    let concurrency = doc.get("concurrency");
    assert!(
        concurrency.and_then(|c| c.get("group")).is_some(),
        "{WATCHDOG_WORKFLOW} must set a top-level `concurrency.group`"
    );
    let cancels = concurrency
        .and_then(|c| c.get("cancel-in-progress"))
        .and_then(serde_yaml::Value::as_bool)
        .unwrap_or(false);
    assert!(!cancels, "`cancel-in-progress` must be false");
}

/// The `run:` text of the watchdog's `if: failure()` step.
#[cfg(target_os = "linux")]
fn report_step_run() -> String {
    let doc = parse_workflow(WATCHDOG_WORKFLOW);
    doc.get("jobs")
        .and_then(serde_yaml::Value::as_mapping)
        .into_iter()
        .flat_map(|jobs| jobs.values())
        .filter_map(|job| job.get("steps").and_then(serde_yaml::Value::as_sequence))
        .flatten()
        .filter(|step| {
            step.get("if")
                .and_then(serde_yaml::Value::as_str)
                .is_some_and(|cond| cond.contains("failure()"))
        })
        .find_map(|step| step.get("run").and_then(serde_yaml::Value::as_str))
        .expect("an `if: failure()` step with a `run:`")
        .to_string()
}

/// Runs the report step in an empty directory, as after a failed checkout.
#[cfg(target_os = "linux")]
fn run_report_step_without_checkout(issues: String) -> Outcome {
    let empty = tempfile::tempdir().expect("tempdir");
    let mut cmd = std::process::Command::new("bash");
    cmd.arg("-c")
        .arg(report_step_run())
        .current_dir(empty.path());
    run_stubbed(
        &Stub {
            runs: None,
            issues: Some(issues),
            arg: None,
        },
        cmd,
    )
}

/// When `actions/checkout` fails, the script is not on disk. The report step
/// must still open the watchdog-failure issue.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_report_step_works_without_checkout() {
    let out = run_report_step_without_checkout(Stub::no_issue());
    assert!(out.success, "calls: {:?}", out.calls);
    let create = out
        .calls
        .iter()
        .find(|c| c.starts_with("issue create"))
        .unwrap_or_else(|| panic!("no issue opened; calls: {:?}", out.calls));
    assert!(
        create.contains(WATCHDOG_FAILED_TITLE) && create.contains("actions/runs/4242"),
        "the issue must use the watchdog-failure title and link the run: {create}"
    );
}

/// A checkout outage can fail several runs. Each one must comment on the open
/// watchdog-failure issue, not open a duplicate.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_report_step_without_checkout_comments_on_the_open_issue() {
    let out = run_report_step_without_checkout(Stub::issue(88, WATCHDOG_FAILED_TITLE));
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue comment 88"), "calls: {:?}", out.calls);
    assert!(!out.called("issue create"), "calls: {:?}", out.calls);
}
