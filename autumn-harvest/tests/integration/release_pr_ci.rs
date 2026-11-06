//! CI wiring guard for the release PR from `trunk-dev` into `trunk`.
//!
//! `ci.yml` runs on each push to `trunk-dev`. It ran on the release PR too,
//! so each push to `trunk-dev` ran the full matrix twice on one tree. Now
//! `ci.yml` runs on pull requests into `trunk-dev` only. The check runs of the
//! push run sit on the head commit of the release PR, so they report there.
//!
//! `release-pr.yml` keeps what the second run gave. It fails when the merge
//! result differs from the head, because the push run did not test that
//! merge. It also runs the two gates that diff against the PR base. They
//! cover the whole release there. A push run diffs against one push only, and
//! a newer push can cancel it.

use serde_yaml::Value;

use super::ci_run_coverage::{parse_workflow, workflow_run_commands};

const CI: &str = ".github/workflows/ci.yml";
const RELEASE_PR: &str = ".github/workflows/release-pr.yml";

/// The branches of one trigger of a workflow.
fn branches<'a>(doc: &'a Value, event: &str) -> Vec<&'a str> {
    doc.get("on")
        .and_then(|on| on.get(event))
        .and_then(|e| e.get("branches"))
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

/// The run commands of one job, joined.
fn job_runs(doc: &Value, job: &str) -> String {
    let job = doc
        .get("jobs")
        .and_then(|jobs| jobs.get(job))
        .unwrap_or_else(|| panic!("{RELEASE_PR} must have a `{job}` job"));
    job.get("steps")
        .and_then(Value::as_sequence)
        .into_iter()
        .flatten()
        .filter_map(|step| step.get("run").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn ci_runs_on_pushes_to_trunk_dev_but_not_on_the_release_pr() {
    let ci = parse_workflow(CI);
    assert!(
        branches(&ci, "push").contains(&"trunk-dev"),
        "the push run on trunk-dev is the full run that the release PR relies on"
    );
    let pr = branches(&ci, "pull_request");
    assert_eq!(
        pr,
        ["trunk-dev"],
        "ci.yml must run on pull requests into trunk-dev only. A run on the \
         release PR repeats the push run on the same tree."
    );
}

#[test]
fn release_pr_workflow_runs_on_pull_requests_into_trunk() {
    let doc = parse_workflow(RELEASE_PR);
    assert_eq!(branches(&doc, "pull_request"), ["trunk"]);
    let types: Vec<&str> = doc["on"]["pull_request"]["types"]
        .as_sequence()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        types.contains(&"ready_for_review"),
        "a draft that becomes ready must run: {types:?}"
    );
    assert_eq!(
        doc["permissions"]["contents"].as_str(),
        Some("read"),
        "the token must be read-only"
    );
}

#[test]
fn release_pr_fails_when_the_merge_differs_from_the_head() {
    let doc = parse_workflow(RELEASE_PR);
    let runs = job_runs(&doc, "merge-equals-head");
    for needle in ["HEAD^{tree}", "^{tree}", "exit 1"] {
        assert!(
            runs.contains(needle),
            "the tree check must use `{needle}`:\n{runs}"
        );
    }
}

#[test]
fn release_pr_ratchets_comment_hygiene_over_the_whole_release() {
    let doc = parse_workflow(RELEASE_PR);
    let runs = job_runs(&doc, "comment-hygiene");
    for needle in [
        "cargo xtask audit comment-hygiene --self-test",
        "cargo xtask audit comment-hygiene --base \"$BASE\"",
        "git merge-base --is-ancestor",
        // The commit that added the gate. The gate first landed at this
        // path, and `git log` still finds that commit after the port.
        "--diff-filter=A --format=%H -1 -- docs/audits/comment-hygiene.py",
    ] {
        assert!(
            runs.contains(needle),
            "the release ratchet must run `{needle}`:\n{runs}"
        );
    }
}

#[test]
fn release_pr_checks_schema_acknowledgements_since_trunk() {
    let doc = parse_workflow(RELEASE_PR);
    let runs = job_runs(&doc, "schema-acknowledgement");
    for needle in [
        "--emit-contract",
        "schema check --baseline",
        "--acknowledged-in",
        "pull_request.base.sha",
    ] {
        let found = runs.contains(needle)
            || serde_yaml::to_string(&doc["jobs"]["schema-acknowledgement"])
                .is_ok_and(|job| job.contains(needle));
        assert!(
            found,
            "the release schema gate must use `{needle}`:\n{runs}"
        );
    }
}

/// The release workflow never builds the full matrix.
#[test]
fn release_pr_workflow_runs_no_test_suite() {
    let doc = parse_workflow(RELEASE_PR);
    for run in workflow_run_commands(&doc) {
        assert!(
            !run.contains("cargo test"),
            "the push run on trunk-dev runs the tests: {run}"
        );
    }
}

/// A push to `trunk-dev` decides docs-only against `trunk`, not the last push.
///
/// A newer push cancels the run of an older one. If the newer push changes
/// docs only, a diff against it skips the tests, and the code of the older
/// push is never tested. The release PR run once caught that, because it
/// diffed against `trunk`. The push run does the same now.
#[test]
fn a_trunk_dev_push_decides_docs_only_against_trunk() {
    let ci = parse_workflow(CI);
    let runs = ci["jobs"]["changes"]["steps"]
        .as_sequence()
        .into_iter()
        .flatten()
        .filter_map(|step| step.get("run").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    for needle in [
        "refs/heads/trunk-dev",
        "git fetch --no-tags origin trunk",
        "git diff --name-only --no-renames \"$diff_base\" \"$head\"",
    ] {
        assert!(
            runs.contains(needle),
            "the changes job must use `{needle}`:\n{runs}"
        );
    }
}
