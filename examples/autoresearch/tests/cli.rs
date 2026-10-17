//! The binary, as a reader runs it: `init`, `run`, `status`, the inbox
//! commands, `verify`, `eval` and `purge`. Offline, with the defaults.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::process::{Command, Output};

fn autoresearch(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(args)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .output()
        .unwrap()
}

fn ok(args: &[&str]) -> String {
    let out = autoresearch(args);
    assert!(
        out.status.success(),
        "{args:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn lab_arg(lab: &Path) -> String {
    lab.display().to_string()
}

#[test]
fn a_night_runs_offline_from_one_command() {
    let dir = tempfile::tempdir().unwrap();
    let lab = dir.path().join("lab");
    let lab = lab_arg(&lab);

    let out = ok(&["init", &lab]);
    assert!(out.contains("autoresearch run --lab"), "{out}");
    // A second init refuses the used directory.
    assert!(!autoresearch(&["init", &lab]).status.success());

    let out = ok(&["run", "--lab", &lab, "--tag", "demo", "--experiments", "3"]);
    assert!(out.contains("baseline"), "{out}");
    assert!(out.contains("best"), "{out}");
    let results = std::fs::read_to_string(Path::new(&lab).join("results.tsv")).unwrap();
    assert_eq!(results.lines().count(), 1 + 4, "{results}");

    let status = ok(&["status", "--lab", &lab]);
    assert!(status.contains("autoresearch/demo"), "{status}");
    assert!(status.contains("COMPLETED"), "{status}");

    let verify = ok(&["verify", "--lab", &lab]);
    assert!(verify.contains("0 failed"), "{verify}");

    // A run with the same tag resumes nothing and refuses the tag.
    let again = autoresearch(&["run", "--lab", &lab, "--tag", "demo", "--experiments", "1"]);
    assert!(!again.status.success());
}

#[test]
fn inbox_commands_queue_for_the_runner() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    ok(&["hint", "--lab", &lab, "--tag", "t", "paper:warmdown"]);
    ok(&["stop", "--lab", &lab, "--tag", "t"]);
    let refused = autoresearch(&["policy", "--lab", &lab, "--tag", "t", "--line-cost", "-1"]);
    assert!(!refused.status.success(), "a negative line cost is refused");
    let inbox = Path::new(&lab).join(".autoresearch").join("inbox");
    let queued = std::fs::read_dir(&inbox)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().is_file())
        .count();
    assert_eq!(queued, 2);
    // The runner delivers both: the hint goes to the first experiment, and
    // the stop ends the session after it.
    let out = ok(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "t",
        "--experiments",
        "0",
        "--approval-timeout",
        "1",
    ]);
    assert!(out.contains("stopped"), "{out}");
    let queued = std::fs::read_dir(&inbox)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().is_file())
        .count();
    assert_eq!(queued, 0);
}

#[test]
fn eval_and_purge_work_on_a_finished_night() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    ok(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "e",
        "--experiments",
        "1",
        "--no-report",
    ]);
    let candidate = dir.path().join("candidate.md");
    std::fs::write(&candidate, "# program\nPrefer simplifications.\n").unwrap();
    let out = ok(&[
        "eval",
        "--lab",
        &lab,
        "--candidate",
        candidate.to_str().unwrap(),
    ]);
    assert!(out.contains("first divergence"), "{out}");
    let out = ok(&["purge", "--lab", &lab, "--older-than-days", "30"]);
    assert!(out.contains("removed 0"), "{out}");
    let out = ok(&["purge", "--lab", &lab, "--older-than-days", "0"]);
    assert!(!out.contains("removed 0"), "{out}");
    // The branch of the purged night stays, so its tag stays used.
    let again = autoresearch(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "e",
        "--experiments",
        "1",
        "--no-report",
    ]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&again.stdout),
        String::from_utf8_lossy(&again.stderr)
    );
    assert!(!again.status.success(), "{text}");
    assert!(text.contains("autoresearch/e exists"), "{text}");
}

#[test]
fn a_run_longer_than_the_train_budget_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let out = autoresearch(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "slow",
        "--train-seconds",
        "2000",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("budget"));
}

/// A training run is code that the model wrote, and it runs as the same
/// user. It must not read the key from the environment of the runner.
/// Root can read any process, so the check needs another user.
#[cfg(target_os = "linux")]
#[test]
fn a_run_cannot_read_the_environment_of_the_runner() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let lab = dir.path().join("lab");
    ok(&["init", &lab_arg(&lab)]);
    // A stand-in for `uv run train.py` that reads its parent: the runner.
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let uv = bin.join("uv");
    std::fs::write(
        &uv,
        "#!/bin/sh\ncat /proc/$PPID/environ >> leak 2>/dev/null\n\
         echo ---\necho 'val_bpb:          0.990000'\necho 'peak_vram_mb:     1024.0'\n",
    )
    .unwrap();
    std::fs::set_permissions(&uv, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let out = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab_arg(&lab), "--tag", "env"])
        .args(["--trainer", "command", "--experiments", "1", "--no-report"])
        .env("PATH", path)
        .env("AUTORESEARCH_TEST_SECRET", "hunter2")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = std::fs::read_to_string(lab.join("results.tsv")).unwrap();
    assert!(rows.contains("0.990000"), "the stand-in trained: {rows}");
    if rustix::process::geteuid().is_root() {
        eprintln!("skipped: root reads every process");
        return;
    }
    let leak = std::fs::read(lab.join("leak")).unwrap_or_default();
    assert!(
        !String::from_utf8_lossy(&leak).contains("hunter2"),
        "the run read the runner's environment"
    );
}

#[test]
fn claude_needs_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let out = autoresearch(&["run", "--lab", &lab, "--tag", "k", "--model", "claude"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("ANTHROPIC_API_KEY"));
}
