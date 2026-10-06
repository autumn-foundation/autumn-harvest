//! Supply-chain CI guard (issue #1826). No DB, no feature gate.
//!
//! Four parts, each checked here:
//!
//! - A daily `cargo deny check advisories` scan. New RUSTSEC advisories land
//!   with no code change, so the change-gated CI job does not see them.
//! - A SHA pin on every `uses:`. `docs/audits/action-sha-pin.py` checks each
//!   line in the `lint` job. This file checks that the job runs it.
//! - A Dependabot file for `cargo` and `github-actions`.
//! - A release that builds auditable binaries, a CycloneDX SBOM, a Sigstore
//!   signature and GitHub artifact attestations.
//!
//! The scan behaviour tests run `.github/ci/advisory-scan.sh` with a stub
//! `cargo` and a stub `gh` first on `PATH`. They run on Linux only, as the
//! chaos watchdog tests do.

use super::ci_run_coverage::{parse_workflow, repo_root, workflow_crons, workflow_run_commands};
use serde_yaml::Value;

const SCAN_WORKFLOW: &str = ".github/workflows/advisory-scan.yml";
const SCAN_SCRIPT: &str = ".github/ci/advisory-scan.sh";
const CI_WORKFLOW: &str = ".github/workflows/ci.yml";
const RELEASE_WORKFLOW: &str = ".github/workflows/release.yml";
const DEPENDABOT: &str = ".github/dependabot.yml";
const SHA_PIN_AUDIT: &str = "docs/audits/action-sha-pin.py";

/// The alert issue title. The script finds its open alert by this exact title.
#[cfg(target_os = "linux")]
const ALERT_TITLE: &str = "Advisory scan: cargo deny check advisories failed";

// ── Shared readers ──────────────────────────────────────────────────────────

/// The steps of job `name`. Panics when the job or its steps are missing.
fn job_steps<'a>(doc: &'a Value, file: &str, name: &str) -> &'a Vec<Value> {
    doc.get("jobs")
        .and_then(|jobs| jobs.get(name))
        .and_then(|job| job.get("steps"))
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{file} must have a `{name}` job with steps"))
}

/// The job `name`. Panics when it is missing.
fn job<'a>(doc: &'a Value, file: &str, name: &str) -> &'a Value {
    doc.get("jobs")
        .and_then(|jobs| jobs.get(name))
        .unwrap_or_else(|| panic!("{file} must have a `{name}` job"))
}

/// The string value at `key` in `node`, if any.
fn text<'a>(node: &'a Value, key: &str) -> Option<&'a str> {
    node.get(key).and_then(Value::as_str)
}

/// The `uses:` value of a step, without its `@ref`.
fn action_of(step: &Value) -> Option<&str> {
    text(step, "uses").map(|u| u.split('@').next().unwrap_or(u))
}

/// Every step of every job in a parsed workflow.
fn all_steps(doc: &Value) -> Vec<&Value> {
    doc.get("jobs")
        .and_then(Value::as_mapping)
        .into_iter()
        .flat_map(|jobs| jobs.values())
        .filter_map(|job| job.get("steps").and_then(Value::as_sequence))
        .flatten()
        .collect()
}

/// The `with.tool` input of each `taiki-e/install-action` step in `steps`
/// that installs `cargo-deny`.
fn cargo_deny_tools<'a>(steps: impl IntoIterator<Item = &'a Value>) -> Vec<&'a str> {
    steps
        .into_iter()
        .filter(|s| action_of(s) == Some("taiki-e/install-action"))
        .filter_map(|s| s.get("with").and_then(|w| text(w, "tool")))
        .filter(|tool| tool.starts_with("cargo-deny"))
        .collect()
}

/// The `matrix.include[].target` values of job `name`.
fn matrix_targets(doc: &Value, file: &str, name: &str) -> Vec<String> {
    job(doc, file, name)
        .get("strategy")
        .and_then(|s| s.get("matrix"))
        .and_then(|m| m.get("include"))
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{file} job `{name}` must have `strategy.matrix.include`"))
        .iter()
        .filter_map(|row| text(row, "target").map(str::to_string))
        .collect()
}

/// The permission `key` of `node`, if it is set.
fn permission<'a>(node: &'a Value, key: &str) -> Option<&'a str> {
    node.get("permissions").and_then(|p| text(p, key))
}

/// True when `step` uses `action` (any ref).
fn uses(step: &Value, action: &str) -> bool {
    action_of(step) == Some(action)
}

// ── Daily advisory scan ─────────────────────────────────────────────────────

/// The scan must fire on its own cron, on demand, and on a pull request that
/// changes it. The pull request run proves one green run before merge.
#[test]
fn advisory_scan_runs_daily_on_demand_and_on_its_own_changes() {
    let doc = parse_workflow(SCAN_WORKFLOW);
    assert!(
        !workflow_crons(&doc).is_empty(),
        "{SCAN_WORKFLOW} must have an `on.schedule` cron"
    );
    let on = doc.get("on").expect("on");
    assert!(
        on.get("workflow_dispatch").is_some(),
        "{SCAN_WORKFLOW} must allow a manual run"
    );
    let paths: Vec<&str> = on
        .get("pull_request")
        .and_then(|pr| pr.get("paths"))
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{SCAN_WORKFLOW} must run on `pull_request` with `paths`"))
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for path in [SCAN_WORKFLOW, SCAN_SCRIPT, "deny.toml"] {
        assert!(
            paths.contains(&path),
            "{SCAN_WORKFLOW} must run on a pull request that changes {path}; paths: {paths:?}"
        );
    }
}

/// The script opens issues and reads the code. It needs nothing else.
#[test]
fn advisory_scan_has_least_privilege() {
    let doc = parse_workflow(SCAN_WORKFLOW);
    let perms = doc
        .get("permissions")
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("{SCAN_WORKFLOW} must set top-level `permissions`"));
    let mut granted: Vec<(String, String)> = perms
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().unwrap_or_default().to_string(),
                v.as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    granted.sort();
    assert_eq!(
        granted,
        vec![
            ("contents".to_string(), "read".to_string()),
            ("issues".to_string(), "write".to_string()),
        ],
        "{SCAN_WORKFLOW} must grant `contents: read` and `issues: write` only"
    );
}

/// The scan step runs the script. It alerts on the cron run only, so a pull
/// request run never opens an issue.
#[test]
fn advisory_scan_alerts_on_scheduled_runs_only() {
    let doc = parse_workflow(SCAN_WORKFLOW);
    let steps = job_steps(&doc, SCAN_WORKFLOW, "scan");
    let scan = steps
        .iter()
        .find(|s| text(s, "run").is_some_and(|r| r.trim() == format!("bash {SCAN_SCRIPT}")))
        .unwrap_or_else(|| {
            panic!("{SCAN_WORKFLOW} must have a step that runs `bash {SCAN_SCRIPT}`")
        });
    let alert = scan
        .get("env")
        .and_then(|e| text(e, "ADVISORY_ALERT"))
        .unwrap_or_default();
    assert_eq!(
        alert, "${{ github.event_name == 'schedule' }}",
        "the scan step must set ADVISORY_ALERT from the event name"
    );
    assert_eq!(
        text(scan, "id"),
        Some("scan"),
        "the report step reads this id"
    );
}

/// A failed setup step stops the scan before the script runs. The script
/// then reports nothing, so a final step must report it.
#[test]
fn advisory_scan_reports_a_failed_setup() {
    let doc = parse_workflow(SCAN_WORKFLOW);
    let steps = job_steps(&doc, SCAN_WORKFLOW, "scan");
    let reports = steps.iter().any(|step| {
        let cond = text(step, "if").unwrap_or_default();
        let run = text(step, "run").unwrap_or_default();
        cond.contains("failure()")
            && cond.contains("github.event_name == 'schedule'")
            && cond.contains("steps.scan.outcome != 'failure'")
            && run.contains(SCAN_SCRIPT)
            && run.contains("setup-failed")
    });
    assert!(
        reports,
        "{SCAN_WORKFLOW} must have an `if: failure()` step for scheduled runs that runs \
         `{SCAN_SCRIPT} setup-failed` when the scan step itself did not fail"
    );
}

/// A job timeout cancels the job, and then no `if: failure()` step runs. Step
/// timeouts must sum to less than the job timeout (the issue #1790 lesson).
#[test]
fn advisory_scan_steps_time_out_before_the_job() {
    let doc = parse_workflow(SCAN_WORKFLOW);
    let job_limit = job(&doc, SCAN_WORKFLOW, "scan")
        .get("timeout-minutes")
        .and_then(Value::as_u64)
        .expect("the scan job must set `timeout-minutes`");
    let total: u64 = job_steps(&doc, SCAN_WORKFLOW, "scan")
        .iter()
        .map(|step| {
            step.get("timeout-minutes")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| panic!("step {step:?} must set `timeout-minutes`"))
        })
        .sum();
    assert!(
        total < job_limit,
        "the step timeouts sum to {total} min, which must be under the {job_limit}-min job timeout"
    );
}

/// The daily scan and the CI gate must run the same `cargo-deny`. A version
/// change can change `[advisories]` defaults, as `deny.toml` records.
#[test]
fn advisory_scan_and_ci_gate_pin_the_same_cargo_deny() {
    let scan = parse_workflow(SCAN_WORKFLOW);
    let ci = parse_workflow(CI_WORKFLOW);
    let scan_tools = cargo_deny_tools(job_steps(&scan, SCAN_WORKFLOW, "scan"));
    let ci_tools = cargo_deny_tools(job_steps(&ci, CI_WORKFLOW, "dependency-audit"));
    assert_eq!(scan_tools.len(), 1, "{SCAN_WORKFLOW}: {scan_tools:?}");
    assert_eq!(ci_tools.len(), 1, "{CI_WORKFLOW}: {ci_tools:?}");
    let version = scan_tools[0];
    assert!(
        version.starts_with("cargo-deny@") && version["cargo-deny@".len()..].contains('.'),
        "pin an exact cargo-deny version, not a range: {version}"
    );
    assert_eq!(scan_tools, ci_tools, "the scan and the CI gate disagree");
    let ci_runs = workflow_run_commands(&ci);
    assert!(
        ci_runs
            .iter()
            .any(|r| r.contains("cargo deny --all-features") && r.contains(" check")),
        "{CI_WORKFLOW} `dependency-audit` must still run `cargo deny --all-features check`"
    );
}

// ── Scan script behaviour (stub `cargo` and `gh`) ───────────────────────────

/// The canned answers that the stubs give.
#[cfg(target_os = "linux")]
struct Stub {
    /// Exit code of `cargo deny`.
    deny_exit: i32,
    /// Output of `cargo deny`.
    deny_output: String,
    /// JSON of the open-issue listing, or `None` to fail as an API error does.
    issues: Option<String>,
    /// Value of `ADVISORY_ALERT`.
    alert: bool,
    /// The script argument, if any.
    arg: Option<&'static str>,
}

#[cfg(target_os = "linux")]
impl Stub {
    /// A scheduled scan with exit code `deny_exit` and no open issue.
    fn scheduled(deny_exit: i32) -> Self {
        Self {
            deny_exit,
            deny_output: if deny_exit == 0 {
                "advisories ok\n".to_string()
            } else {
                "error[vulnerability]: RUSTSEC-2099-0001 bad crate\n\
                 error[unsound]: RUSTSEC-2099-0002 worse crate\n\
                 error[vulnerability]: RUSTSEC-2099-0001 bad crate (second path)\n\
                 advisories FAILED\n"
                    .to_string()
            },
            issues: Some("[]".to_string()),
            alert: true,
            arg: None,
        }
    }

    /// The same stub with one open issue of this number and title.
    fn with_issue(mut self, number: u32, title: &str) -> Self {
        self.issues = Some(format!(r#"[{{"number":{number},"title":"{title}"}}]"#));
        self
    }
}

/// The result of one script run against the stubs.
#[cfg(target_os = "linux")]
struct Outcome {
    code: Option<i32>,
    stdout: String,
    /// One line for each `gh` call. Newlines in arguments become spaces.
    gh: Vec<String>,
    /// One line for each `cargo` call.
    cargo: Vec<String>,
    /// The last `--body-file` content passed to `gh`.
    body: String,
}

#[cfg(target_os = "linux")]
impl Outcome {
    fn called(&self, prefix: &str) -> bool {
        self.gh.iter().any(|c| c.starts_with(prefix))
    }

    fn wrote_an_issue(&self) -> bool {
        ["issue create", "issue comment", "issue close"]
            .iter()
            .any(|p| self.called(p))
    }
}

/// Runs the scan script with stub `cargo` and `gh` first on `PATH`.
#[cfg(target_os = "linux")]
fn run_scan(stub: &Stub) -> Outcome {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let write_stub = |name: &str, body: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write stub");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod stub");
    };
    // A file, not an env var: one env string is capped at 128 KiB.
    std::fs::write(dir.path().join("deny-output.txt"), &stub.deny_output)
        .expect("write deny output");
    write_stub(
        "cargo",
        r#"#!/usr/bin/env bash
printf '%s\n' "$*" >> "$STUB_DIR/cargo.log"
cat "$STUB_DIR/deny-output.txt"
exit "$STUB_DENY_EXIT"
"#,
    );
    write_stub(
        "gh",
        r#"#!/usr/bin/env bash
printf '%s' "$*" | tr '\n' ' ' >> "$STUB_DIR/gh.log"
echo >> "$STUB_DIR/gh.log"
prev=
for arg in "$@"; do
  if [ "$prev" = --body-file ]; then cp "$arg" "$STUB_DIR/body.txt"; fi
  prev="$arg"
done
if [ "$1" = api ]; then
  if [ "$STUB_ISSUES_FAIL" = 1 ]; then echo "HTTP 502" >&2; exit 1; fi
  printf '%s\n' "$STUB_ISSUES"
fi
"#,
    );

    let mut cmd = std::process::Command::new("bash");
    cmd.arg(repo_root().join(SCAN_SCRIPT));
    if let Some(arg) = stub.arg {
        cmd.arg(arg);
    }
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
        .env("ADVISORY_ALERT", if stub.alert { "true" } else { "false" })
        .env("STUB_DIR", dir.path())
        .env("STUB_DENY_EXIT", stub.deny_exit.to_string())
        .env("STUB_ISSUES", stub.issues.as_deref().unwrap_or_default())
        .env(
            "STUB_ISSUES_FAIL",
            if stub.issues.is_none() { "1" } else { "0" },
        )
        .output()
        .expect("run bash");
    let lines = |name: &str| -> Vec<String> {
        std::fs::read_to_string(dir.path().join(name))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    };
    Outcome {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        gh: lines("gh.log"),
        cargo: lines("cargo.log"),
        body: std::fs::read_to_string(dir.path().join("body.txt")).unwrap_or_default(),
    }
}

/// The script runs the same command as the CI gate, scoped to advisories.
#[cfg(target_os = "linux")]
#[test]
fn scan_runs_cargo_deny_check_advisories() {
    let out = run_scan(&Stub::scheduled(0));
    assert_eq!(out.code, Some(0), "gh: {:?}", out.gh);
    assert_eq!(
        out.cargo,
        vec!["deny --all-features --color never check advisories".to_string()],
        "the script must run exactly one cargo deny scan"
    );
    assert!(
        out.stdout.contains("advisories ok"),
        "the log must show the scan: {}",
        out.stdout
    );
}

#[cfg(target_os = "linux")]
#[test]
fn scan_is_silent_when_clean_and_no_alert_is_open() {
    let out = run_scan(&Stub::scheduled(0));
    assert_eq!(out.code, Some(0), "gh: {:?}", out.gh);
    assert!(!out.wrote_an_issue(), "gh: {:?}", out.gh);
}

/// A stale open alert would teach people to ignore the next one.
#[cfg(target_os = "linux")]
#[test]
fn scan_closes_the_open_alert_after_a_clean_run() {
    let out = run_scan(&Stub::scheduled(0).with_issue(31, ALERT_TITLE));
    assert_eq!(out.code, Some(0), "gh: {:?}", out.gh);
    assert!(out.called("issue close 31"), "gh: {:?}", out.gh);
}

/// A finding opens an issue that names each advisory once and links the run.
/// The run is also red.
#[cfg(target_os = "linux")]
#[test]
fn scan_opens_an_issue_for_a_finding() {
    let out = run_scan(&Stub::scheduled(1));
    assert_eq!(out.code, Some(1), "the run must be red; gh: {:?}", out.gh);
    let create = out
        .gh
        .iter()
        .find(|c| c.starts_with("issue create"))
        .unwrap_or_else(|| panic!("no issue opened; gh: {:?}", out.gh));
    assert!(create.contains(ALERT_TITLE), "{create}");
    assert!(
        out.body.contains("RUSTSEC-2099-0001 RUSTSEC-2099-0002"),
        "the body must list each advisory id once, sorted: {}",
        out.body
    );
    assert!(out.body.contains("actions/runs/4242"), "{}", out.body);
    assert!(
        out.body.contains("advisories FAILED"),
        "the body must quote the scan: {}",
        out.body
    );
}

/// A second finding goes on the open issue, so one problem gives one issue.
#[cfg(target_os = "linux")]
#[test]
fn scan_comments_on_the_open_alert_instead_of_a_duplicate() {
    let out = run_scan(&Stub::scheduled(1).with_issue(31, ALERT_TITLE));
    assert_eq!(out.code, Some(1), "gh: {:?}", out.gh);
    assert!(out.called("issue comment 31"), "gh: {:?}", out.gh);
    assert!(!out.called("issue create"), "gh: {:?}", out.gh);
}

/// Only an exact title match is the open alert.
#[cfg(target_os = "linux")]
#[test]
fn scan_ignores_an_open_issue_with_a_similar_title() {
    let out = run_scan(&Stub::scheduled(1).with_issue(31, &format!("{ALERT_TITLE} (old)")));
    assert!(out.called("issue create"), "gh: {:?}", out.gh);
    assert!(!out.called("issue comment 31"), "gh: {:?}", out.gh);
}

/// A pull request run must not write to issues. Its exit code is still the
/// scan's, so a finding still turns the check red.
#[cfg(target_os = "linux")]
#[test]
fn scan_without_alert_only_reports_its_exit_code() {
    for exit in [0, 1] {
        let mut stub = Stub::scheduled(exit);
        stub.alert = false;
        let out = run_scan(&stub);
        assert_eq!(out.code, Some(exit), "gh: {:?}", out.gh);
        assert!(
            out.gh.is_empty(),
            "no gh call without ADVISORY_ALERT: {:?}",
            out.gh
        );
    }
}

/// An API error must turn the run red with no issue write. It must not read
/// as "no open alert", which would open a duplicate.
#[cfg(target_os = "linux")]
#[test]
fn scan_fails_closed_when_the_issue_query_fails() {
    for exit in [0, 1] {
        let mut stub = Stub::scheduled(exit);
        stub.issues = None;
        let out = run_scan(&stub);
        assert_ne!(out.code, Some(0), "gh: {:?}", out.gh);
        assert!(!out.wrote_an_issue(), "gh: {:?}", out.gh);
    }
}

/// GitHub caps an issue body at 65536 characters. The script quotes the tail
/// of a long scan, so the alert still posts.
#[cfg(target_os = "linux")]
#[test]
fn scan_truncates_a_long_report() {
    let mut stub = Stub::scheduled(1);
    stub.deny_output = format!("{}\nRUSTSEC-2099-0003 last line\n", "x".repeat(200_000));
    let out = run_scan(&stub);
    assert!(out.called("issue create"), "gh: {:?}", out.gh);
    assert!(
        out.body.chars().count() < 65_536,
        "body is {} characters",
        out.body.chars().count()
    );
    assert!(
        out.body.contains("RUSTSEC-2099-0003 last line"),
        "keep the tail"
    );
}

/// In `setup-failed` mode the script reports a red run on the alert issue.
/// It does not scan, because the setup that failed installs the scanner.
#[cfg(target_os = "linux")]
#[test]
fn scan_setup_failed_mode_reports_the_run() {
    let mut stub = Stub::scheduled(0);
    stub.arg = Some("setup-failed");
    let out = run_scan(&stub);
    assert_eq!(out.code, Some(0), "gh: {:?}", out.gh);
    assert!(
        out.cargo.is_empty(),
        "no scan in setup-failed mode: {:?}",
        out.cargo
    );
    assert!(out.called("issue create"), "gh: {:?}", out.gh);
    assert!(out.body.contains("actions/runs/4242"), "{}", out.body);

    let out = run_scan(&Stub {
        arg: Some("setup-failed"),
        ..Stub::scheduled(0).with_issue(31, ALERT_TITLE)
    });
    assert!(out.called("issue comment 31"), "gh: {:?}", out.gh);
}

// ── SHA pins ────────────────────────────────────────────────────────────────

/// The pin lint is the enforcement of "every `uses:` references a SHA". The
/// `lint` job must run its self-test and its full scan.
#[test]
fn lint_job_runs_the_sha_pin_audit() {
    let doc = parse_workflow(CI_WORKFLOW);
    let runs: Vec<&str> = job_steps(&doc, CI_WORKFLOW, "lint")
        .iter()
        .filter_map(|s| text(s, "run"))
        .collect();
    let lines: Vec<&str> = runs.iter().flat_map(|r| r.lines()).map(str::trim).collect();
    assert!(
        lines.contains(&format!("python3 {SHA_PIN_AUDIT} --self-test").as_str()),
        "the lint job must run `python3 {SHA_PIN_AUDIT} --self-test`"
    );
    assert!(
        lines.contains(&format!("python3 {SHA_PIN_AUDIT}").as_str()),
        "the lint job must run `python3 {SHA_PIN_AUDIT}`"
    );
}

/// A SHA pin drops what a branch ref implied. `dtolnay/rust-toolchain@stable`
/// takes its toolchain from the ref, and `taiki-e/install-action@git-cliff`
/// takes its tool from the ref. A pinned step must name them as inputs.
#[test]
fn pinned_ref_actions_name_their_inputs() {
    let dir = repo_root().join(".github/workflows");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read workflows")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "yml" || x == "yaml"))
        .collect();
    files.sort();
    let mut missing = Vec::new();
    for path in files {
        let rel = format!(
            ".github/workflows/{}",
            path.file_name().unwrap().to_string_lossy()
        );
        let doc = parse_workflow(&rel);
        for step in all_steps(&doc) {
            let input = match action_of(step) {
                Some("dtolnay/rust-toolchain") => "toolchain",
                Some("taiki-e/install-action") => "tool",
                _ => continue,
            };
            if step.get("with").and_then(|w| text(w, input)).is_none() {
                missing.push(format!(
                    "{rel}: {} needs `with.{input}`",
                    text(step, "uses").unwrap()
                ));
            }
        }
    }
    assert!(missing.is_empty(), "{missing:#?}");
}

// ── Dependabot ──────────────────────────────────────────────────────────────

/// Dependabot must watch both ecosystems, and open each pull request against
/// `trunk-dev`. `trunk` is the release branch.
#[test]
fn dependabot_watches_cargo_and_actions_on_trunk_dev() {
    let doc = parse_workflow(DEPENDABOT);
    assert_eq!(doc.get("version").and_then(Value::as_u64), Some(2));
    let updates = doc
        .get("updates")
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{DEPENDABOT} must have `updates`"));
    let dirs_of = |eco: &str| -> Vec<String> {
        updates
            .iter()
            .filter(|u| text(u, "package-ecosystem") == Some(eco))
            .flat_map(|u| {
                let one = text(u, "directory").map(str::to_string);
                let many = u
                    .get("directories")
                    .and_then(Value::as_sequence)
                    .into_iter()
                    .flatten()
                    .filter_map(|d| d.as_str().map(str::to_string));
                one.into_iter().chain(many).collect::<Vec<_>>()
            })
            .collect()
    };
    let cargo = dirs_of("cargo");
    assert!(
        cargo.contains(&"/".to_string()),
        "cargo must watch `/`: {cargo:?}"
    );
    assert!(
        cargo.contains(&"/fuzz".to_string()),
        "cargo must watch `/fuzz`, its own workspace: {cargo:?}"
    );
    let actions = dirs_of("github-actions");
    assert!(
        actions.contains(&"/".to_string()),
        "github-actions must watch `/`: {actions:?}"
    );
    for update in updates {
        let eco = text(update, "package-ecosystem").unwrap_or("?");
        assert_eq!(
            text(update, "target-branch"),
            Some("trunk-dev"),
            "{eco}: every update must target trunk-dev"
        );
        assert!(
            update
                .get("schedule")
                .and_then(|s| text(s, "interval"))
                .is_some(),
            "{eco}: every update must set `schedule.interval`"
        );
    }
}

// ── Release: auditable build, SBOM, signature, attestations ─────────────────

/// A tag push releases. A manual run and a pull request that changes the
/// workflow are dry runs.
#[test]
fn release_runs_on_tags_and_as_a_dry_run() {
    let doc = parse_workflow(RELEASE_WORKFLOW);
    let on = doc.get("on").expect("on");
    assert!(
        on.get("push").and_then(|p| p.get("tags")).is_some(),
        "tag push"
    );
    assert!(on.get("workflow_dispatch").is_some(), "manual dry run");
    let paths: Vec<&str> = on
        .get("pull_request")
        .and_then(|pr| pr.get("paths"))
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{RELEASE_WORKFLOW} must run on `pull_request` with `paths`"))
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(paths.contains(&RELEASE_WORKFLOW), "paths: {paths:?}");
    assert_eq!(
        permission(&doc, "contents"),
        Some("read"),
        "the default token must be read-only; a job asks for more"
    );
}

/// The build runs third-party `build.rs` code, so it must not hold a token
/// that can sign or attest. It builds with `cargo auditable` and writes a
/// CycloneDX SBOM for the same package and target.
#[test]
fn release_builds_auditable_binaries_and_an_sbom() {
    let doc = parse_workflow(RELEASE_WORKFLOW);
    let binaries = job(&doc, RELEASE_WORKFLOW, "binaries");
    for key in ["id-token", "attestations", "contents"] {
        assert_ne!(
            permission(binaries, key),
            Some("write"),
            "binaries: `{key}: write`"
        );
    }
    assert!(
        binaries.get("permissions").is_some(),
        "binaries must set `permissions` so it cannot inherit a wider default"
    );
    let runs: Vec<&str> = job_steps(&doc, RELEASE_WORKFLOW, "binaries")
        .iter()
        .filter_map(|s| text(s, "run"))
        .collect();
    let joined = runs.join("\n");
    for needle in [
        "cargo auditable build --release --locked -p autumn-harvest-cli --target",
        "cargo cyclonedx",
        "--target",
        "--format json",
    ] {
        assert!(
            joined.contains(needle),
            "binaries must run `{needle}`:\n{joined}"
        );
    }
    let targets = matrix_targets(&doc, RELEASE_WORKFLOW, "binaries");
    for target in [
        "x86_64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
    ] {
        assert!(
            targets.contains(&target.to_string()),
            "targets: {targets:?}"
        );
    }
}

/// Signing is keyless and verified in the same job. Provenance and the SBOM
/// are attested against the archive digest.
#[test]
fn release_signs_verifies_and_attests_each_archive() {
    let doc = parse_workflow(RELEASE_WORKFLOW);
    let sign = job(&doc, RELEASE_WORKFLOW, "sign");
    assert_eq!(
        permission(sign, "id-token"),
        Some("write"),
        "keyless signing needs OIDC"
    );
    assert_eq!(permission(sign, "attestations"), Some("write"));
    assert_ne!(permission(sign, "contents"), Some("write"));
    assert_eq!(
        matrix_targets(&doc, RELEASE_WORKFLOW, "sign"),
        matrix_targets(&doc, RELEASE_WORKFLOW, "binaries"),
        "sign must cover every built target"
    );
    let cond = text(sign, "if").unwrap_or_default();
    assert!(
        cond.contains("github.event.pull_request.head.repo.full_name == github.repository"),
        "a fork pull request has no OIDC token, so sign must skip it: {cond}"
    );
    let steps = job_steps(&doc, RELEASE_WORKFLOW, "sign");
    assert!(steps.iter().any(|s| uses(s, "sigstore/cosign-installer")));
    assert!(
        steps
            .iter()
            .any(|s| uses(s, "actions/attest-build-provenance"))
    );
    assert!(steps.iter().any(|s| uses(s, "actions/attest-sbom")));
    let joined: String = steps
        .iter()
        .filter_map(|s| text(s, "run"))
        .collect::<Vec<_>>()
        .join("\n");
    for needle in [
        "cosign sign-blob --yes --bundle",
        "cosign verify-blob --bundle",
        "--certificate-identity",
        "--certificate-oidc-issuer https://token.actions.githubusercontent.com",
    ] {
        assert!(
            joined.contains(needle),
            "sign must run `{needle}`:\n{joined}"
        );
    }
}

/// Only a tag push publishes. It ships the archives with their SBOMs and
/// Sigstore bundles, after validation and signing.
#[test]
fn release_publishes_only_on_a_tag_push() {
    let doc = parse_workflow(RELEASE_WORKFLOW);
    let release = job(&doc, RELEASE_WORKFLOW, "release");
    let cond = text(release, "if").unwrap_or_default();
    assert!(
        cond.contains("github.event_name == 'push'") && cond.contains("refs/tags/"),
        "release must run on a tag push only: {cond:?}"
    );
    let needs: Vec<&str> = release
        .get("needs")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    for need in ["validate", "sign"] {
        assert!(
            needs.contains(&need),
            "release must need `{need}`: {needs:?}"
        );
    }
    let publish = job_steps(&doc, RELEASE_WORKFLOW, "release")
        .iter()
        .find(|s| uses(s, "softprops/action-gh-release"))
        .expect("a softprops/action-gh-release step");
    let files = publish
        .get("with")
        .and_then(|w| text(w, "files"))
        .unwrap_or_default();
    for needle in [".tar.gz", ".cdx.json", ".sigstore.json", "SHA256SUMS"] {
        assert!(
            files.contains(needle),
            "the release must ship `{needle}`: {files}"
        );
    }
}
