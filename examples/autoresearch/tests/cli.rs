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
    // `=` keeps clap from reading `-1` as a flag, so the policy check runs.
    let refused = autoresearch(&["policy", "--lab", &lab, "--tag", "t", "--line-cost=-1"]);
    assert!(!refused.status.success(), "a negative line cost is refused");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("line_cost"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
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

/// A stop that a person posts while the night runs lands before the next
/// experiment. It does not wait for the night to continue as new.
#[test]
fn a_stop_posted_mid_night_lands_before_the_next_experiment() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let mut night = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab, "--tag", "mid", "--experiments", "0"])
        .args(["--per-run", "50", "--train-seconds", "1", "--tick-ms", "50"])
        .args(["--pace", "3"])
        .args(["--approval-timeout", "1", "--no-report"])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let results = Path::new(&lab).join("results.tsv");
    let rows = || {
        std::fs::read_to_string(&results).map_or(0, |text| text.lines().count().saturating_sub(1))
    };
    // The baseline and one experiment.
    let started = (0..600).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        rows() >= 2
    });
    if !started {
        night.kill().unwrap();
    }
    assert!(started, "the night ran two rows");
    ok(&["stop", "--lab", &lab, "--tag", "mid"]);
    let at_stop = rows();
    let ended = (0..600).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        night.try_wait().unwrap().is_some()
    });
    if !ended {
        night.kill().unwrap();
    }
    let out = night.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(ended, "the night ended: {stdout}");
    assert!(stdout.contains("stopped"), "{stdout}");
    // The stop lands in the pause, so no experiment starts after it.
    assert_eq!(rows(), at_stop, "{} rows after {at_stop}", rows());
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

/// A runner that died mid-night is back with the same command and no tag,
/// as after a night that crossed midnight. It resumes the one running
/// session.
#[test]
fn run_with_no_tag_resumes_the_one_running_session() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let mut first = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab, "--tag", "night", "--experiments", "1"])
        .args(["--train-seconds", "2", "--no-report"])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let running = (0..300).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let status = autoresearch(&["status", "--lab", &lab]);
        String::from_utf8_lossy(&status.stdout).contains("RUNNING")
    });
    first.kill().unwrap();
    first.wait().unwrap();
    assert!(running, "the night started");
    // History records the model. A resume with another one is refused.
    let other = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab, "--no-report", "--model", "claude"])
        .env("ANTHROPIC_API_KEY", "not-a-key")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .output()
        .unwrap();
    assert!(!other.status.success());
    let why = String::from_utf8_lossy(&other.stderr);
    assert!(why.contains("the model scripted"), "{why}");
    let out = ok(&["run", "--lab", &lab, "--no-report"]);
    assert!(
        out.contains("resuming the session with the tag \"night\""),
        "{out}"
    );
    assert!(
        out.contains("done: autoresearch/night: 1 experiments"),
        "{out}"
    );
}

/// A command that reads the database refuses a path with no database. It
/// does not make an empty one, which `verify` would pass.
#[test]
fn a_missing_database_is_an_error_not_a_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let typo = dir.path().join("harvst.db");
    let typo_arg = typo.display().to_string();
    let candidate = dir.path().join("candidate.md");
    std::fs::write(&candidate, "# program\n").unwrap();
    for args in [
        vec!["verify", "--lab", &lab, "--db", &typo_arg],
        vec!["purge", "--lab", &lab, "--db", &typo_arg],
        vec![
            "eval",
            "--lab",
            &lab,
            "--db",
            &typo_arg,
            "--candidate",
            candidate.to_str().unwrap(),
        ],
    ] {
        let out = autoresearch(&args);
        assert!(!out.status.success(), "{args:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("no database"),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!typo.exists(), "{args:?} made a database");
    }
}

/// Run the binary for at most `secs` seconds. `None` when it ran on.
fn run_for(args: &[&str], secs: u64) -> Option<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(args)
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let ended = (0..secs * 10).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        child.try_wait().unwrap().is_some()
    });
    if !ended {
        child.kill().unwrap();
        child.wait().unwrap();
        return None;
    }
    Some(child.wait_with_output().unwrap())
}

/// One runner drives a database. A new session waits while another one
/// runs that no runner drives, so `run` refuses to start it. The person
/// stops the other one first.
#[test]
fn a_new_session_waits_until_the_running_one_ends() {
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    let mut first = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab, "--tag", "y", "--experiments", "0"])
        .args(["--train-seconds", "2", "--no-report"])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let running = (0..300).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let status = autoresearch(&["status", "--lab", &lab]);
        String::from_utf8_lossy(&status.stdout).contains("RUNNING")
    });
    first.kill().unwrap();
    first.wait().unwrap();
    assert!(running, "the first night started");
    let refused = run_for(&["run", "--lab", &lab, "--tag", "w", "--no-report"], 30)
        .expect("the second night does not hang");
    let text = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "{text}");
    assert!(text.contains("tagged \"y\""), "{text}");
    ok(&["stop", "--lab", &lab, "--tag", "y"]);
    let out = ok(&["run", "--lab", &lab, "--tag", "y", "--no-report"]);
    assert!(out.contains("stopped"), "{out}");
    let out = ok(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "w",
        "--experiments",
        "1",
        "--no-report",
    ]);
    assert!(out.contains("done: autoresearch/w"), "{out}");
}

/// `serve` drives a night that a stopped `run` left, and names its end. A
/// night that ended before `serve` started is not news.
#[test]
fn serve_names_a_night_that_ends_once() {
    use std::io::BufRead as _;
    let dir = tempfile::tempdir().unwrap();
    let lab = lab_arg(&dir.path().join("lab"));
    ok(&["init", &lab]);
    ok(&[
        "run",
        "--lab",
        &lab,
        "--tag",
        "old",
        "--experiments",
        "1",
        "--no-report",
    ]);
    let mut first = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["run", "--lab", &lab, "--tag", "night", "--experiments", "1"])
        .args(["--train-seconds", "2", "--no-report"])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let running = (0..300).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let status = autoresearch(&["status", "--lab", &lab]);
        String::from_utf8_lossy(&status.stdout).contains("autoresearch/night  RUNNING")
    });
    first.kill().unwrap();
    first.wait().unwrap();
    assert!(running, "the night started");
    let mut serve = Command::new(env!("CARGO_BIN_EXE_autoresearch"))
        .args(["serve", "--lab", &lab])
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("AUTORESEARCH_LAB")
        .env_remove("AUTORESEARCH_DB")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let (lines, seen) = std::sync::mpsc::channel();
    let stdout = serve.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let mut out = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while std::time::Instant::now() < deadline && !out.contains("done: autoresearch/night") {
        if let Ok(line) = seen.recv_timeout(std::time::Duration::from_millis(200)) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    // A last look for a second report of the same end.
    std::thread::sleep(std::time::Duration::from_secs(3));
    out.extend(seen.try_iter().map(|line| line + "\n"));
    serve.kill().unwrap();
    serve.wait().unwrap();
    assert!(
        out.contains("done: autoresearch/night: 1 experiments"),
        "{out}"
    );
    assert_eq!(out.matches("done: autoresearch/night").count(), 1, "{out}");
    assert!(!out.contains("autoresearch/old"), "{out}");
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
