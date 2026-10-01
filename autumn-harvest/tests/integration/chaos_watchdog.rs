//! Chaos nightly watchdog guard (issue #1790). No DB, no feature gate.
//!
//! `chaos.yml` did not parse from its first commit, so its nightly run never
//! fired. Nobody saw the gap for months. The watchdog workflow is a separate
//! file, so a defect in `chaos.yml` cannot also stop the alert.
//!
//! The behaviour tests run `.github/ci/chaos-watchdog.sh` with a stub `gh` on
//! `PATH`. The stub records each call and returns canned answers. They run on
//! Linux only, because the script uses GNU `date`, as the `ubuntu-latest`
//! runner does.

use super::ci_run_coverage::{parse_workflow, repo_root, workflow_crons, workflow_run_commands};

const WATCHDOG_WORKFLOW: &str = ".github/workflows/chaos-watchdog.yml";
const WATCHDOG_SCRIPT: &str = ".github/ci/chaos-watchdog.sh";

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

/// The answers that the stub `gh` gives.
#[cfg(target_os = "linux")]
struct Stub {
    /// Output of the run query: the count of successful runs.
    successes: &'static str,
    /// Output of the issue query: an open alert issue number, or empty.
    open_issue: &'static str,
    /// When true, the run query fails as an API error does.
    api_fails: bool,
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
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("gh.log");
    let gh = dir.path().join("gh");
    std::fs::write(
        &gh,
        r#"#!/usr/bin/env bash
printf '%s\n' "$*" >> "$STUB_LOG"
case "$1" in
  api)
    if [ "$STUB_API_FAILS" = 1 ]; then echo "HTTP 502" >&2; exit 1; fi
    echo "$STUB_SUCCESSES" ;;
  issue)
    if [ "$2" = list ]; then echo "$STUB_OPEN_ISSUE"; fi ;;
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
    let output = std::process::Command::new("bash")
        .arg(repo_root().join(WATCHDOG_SCRIPT))
        .env("PATH", path)
        .env("GITHUB_REPOSITORY", "owner/repo")
        .env("GITHUB_SERVER_URL", "https://github.com")
        .env("STUB_LOG", &log)
        .env("STUB_SUCCESSES", stub.successes)
        .env("STUB_OPEN_ISSUE", stub.open_issue)
        .env("STUB_API_FAILS", if stub.api_fails { "1" } else { "0" })
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
        successes: "1",
        open_issue: "",
        api_fails: false,
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
        successes: "0",
        open_issue: "",
        api_fails: false,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue create"), "calls: {:?}", out.calls);
    assert!(!out.called("issue comment"), "calls: {:?}", out.calls);
}

/// A second alert goes on the open issue, so a long gap gives one issue.
#[cfg(target_os = "linux")]
#[test]
fn watchdog_comments_on_the_open_issue_instead_of_a_duplicate() {
    let out = run_watchdog(&Stub {
        successes: "0",
        open_issue: "77",
        api_fails: false,
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
        successes: "2",
        open_issue: "77",
        api_fails: false,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(out.called("issue close 77"), "calls: {:?}", out.calls);
    assert!(!out.called("issue create"), "calls: {:?}", out.calls);
}

#[cfg(target_os = "linux")]
#[test]
fn watchdog_is_silent_when_the_nightly_is_green() {
    let out = run_watchdog(&Stub {
        successes: "1",
        open_issue: "",
        api_fails: false,
    });
    assert!(out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}

/// An API error must turn the watchdog run red. It must not read as zero
/// runs (a false alert) or as a green nightly (a missed alert).
#[cfg(target_os = "linux")]
#[test]
fn watchdog_fails_closed_when_the_run_query_fails() {
    let out = run_watchdog(&Stub {
        successes: "",
        open_issue: "",
        api_fails: true,
    });
    assert!(!out.success, "calls: {:?}", out.calls);
    assert!(!out.wrote_an_issue(), "calls: {:?}", out.calls);
}
