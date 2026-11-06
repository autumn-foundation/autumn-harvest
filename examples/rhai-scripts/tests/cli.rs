//! The `harvest-rhai` binary, including a real `kill -9` partway through a run.

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// The built binary.
const BIN: &str = env!("CARGO_BIN_EXE_harvest-rhai");

/// A `harvest-rhai` command that runs in `dir` with the database there.
fn harvest(dir: &Path) -> Command {
    let mut command = Command::new(BIN);
    command
        .current_dir(dir)
        .arg("--db")
        .arg(dir.join("runs.db"));
    command
}

/// Run `command` and capture its output.
fn output(command: &mut Command) -> Output {
    command.output().expect("the binary runs")
}

/// The standard output of `out` as text.
fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The lines of `name` in `dir`.
fn lines(dir: &Path, name: &str) -> Vec<String> {
    std::fs::read_to_string(dir.join(name))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Wait until `cond` holds, for at most 30 seconds.
fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(unix)]
#[test]
fn a_killed_run_resumes_after_its_last_saved_step() {
    let dir = TempDir::new().expect("a temp dir");
    let script = dir.path().join("job.rhai");
    // Step two blocks until the file `go` exists, so the kill lands inside it.
    std::fs::write(
        &script,
        r#"
        sh("echo one >> steps.log");
        sh("echo two >> steps.log; while [ ! -f go ]; do sleep 0.02; done");
        sh("echo three >> steps.log");
        "done"
        "#,
    )
    .expect("write the script");

    let mut child = harvest(dir.path())
        .args(["run", "job.rhai", "--id", "job-1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    wait_until("step two to start", || {
        lines(dir.path(), "steps.log").len() == 2
    });
    // `kill` sends SIGKILL, so the process saves nothing on the way out.
    child.kill().expect("kill -9");
    child.wait().expect("reap");

    // The run keeps the source it started with, so this edit changes nothing.
    std::fs::write(&script, r#"sh("echo edited >> steps.log"); "edited""#).expect("edit");
    std::fs::write(dir.path().join("go"), "").expect("release step two");

    let resumed = output(harvest(dir.path()).args(["resume", "job-1"]));
    assert_eq!(resumed.status.code(), Some(0), "{resumed:?}");
    assert_eq!(stdout(&resumed).trim(), "done");
    // Step one ran once. Step two was in flight at the kill, so it ran again.
    assert_eq!(
        lines(dir.path(), "steps.log"),
        ["one", "two", "two", "three"]
    );
}

#[cfg(unix)]
#[test]
fn approve_continues_a_waiting_run_and_history_shows_each_step() {
    let dir = TempDir::new().expect("a temp dir");
    std::fs::write(
        dir.path().join("release.rhai"),
        r#"
        progress("build");
        let v = sh(`echo ${ARGS[0]}`);
        print(`built ${v}`);
        progress("waiting");
        let who = wait_for("approve");
        #{ version: v, by: who }
        "#,
    )
    .expect("write the script");

    let run =
        output(harvest(dir.path()).args(["run", "release.rhai", "--id", "r1", "--", "1.2.3"]));
    assert_eq!(run.status.code(), Some(3), "a waiting run exits 3: {run:?}");
    assert_eq!(stdout(&run).trim(), "built 1.2.3");

    let status = output(harvest(dir.path()).args(["status", "r1"]));
    let status = stdout(&status);
    assert!(status.contains("RUNNING"), "{status}");
    assert!(status.contains("progress  waiting"), "{status}");

    let approved = output(harvest(dir.path()).args(["approve", "r1", "--payload", "\"ann\""]));
    assert_eq!(approved.status.code(), Some(0), "{approved:?}");
    // The replay of the first steps prints nothing again.
    assert_eq!(
        stdout(&approved).trim(),
        r#"{"by":"ann","version":"1.2.3"}"#
    );

    let history = stdout(&output(harvest(dir.path()).args(["history", "r1"])));
    assert!(
        history.contains(r#"sh("echo 1.2.3")  ->  1.2.3"#),
        "{history}"
    );
    assert!(history.contains("signal `approve`  ->  ann"), "{history}");
    assert!(history.contains("log  built 1.2.3"), "{history}");

    let listed = stdout(&output(harvest(dir.path()).arg("status")));
    assert!(
        listed.contains("COMPLETED") && listed.contains("r1"),
        "{listed}"
    );
}

#[test]
fn a_script_that_does_not_parse_starts_no_run() {
    let dir = TempDir::new().expect("a temp dir");
    std::fs::write(dir.path().join("bad.rhai"), "let x = ;").expect("write");

    let run = output(harvest(dir.path()).args(["run", "bad.rhai"]));
    assert_eq!(run.status.code(), Some(2), "{run:?}");
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("bad.rhai"),
        "{run:?}"
    );
    let listed = output(harvest(dir.path()).arg("status"));
    assert_eq!(listed.status.code(), Some(0), "{listed:?}");
    assert!(
        stdout(&listed).trim().is_empty(),
        "no run starts: {listed:?}"
    );
}

#[test]
fn an_unknown_run_name_is_an_error() {
    let dir = TempDir::new().expect("a temp dir");
    let resumed = output(harvest(dir.path()).args(["resume", "missing"]));
    assert_eq!(resumed.status.code(), Some(2), "{resumed:?}");
    assert!(String::from_utf8_lossy(&resumed.stderr).contains("no run is named `missing`"));
}

#[cfg(unix)]
#[test]
fn a_closed_stdout_ends_history_quietly() {
    let dir = TempDir::new().expect("a temp dir");
    let lines = "print(\"a line\");\n".repeat(200);
    std::fs::write(dir.path().join("chatty.rhai"), lines).expect("write");
    let run = output(harvest(dir.path()).args(["run", "chatty.rhai", "--id", "c1"]));
    assert_eq!(run.status.code(), Some(0), "{run:?}");

    // As `history c1 | head -0`: the reader is gone before the first write.
    let mut child = harvest(dir.path())
        .args(["history", "c1"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    drop(child.stdout.take());
    let done = child.wait_with_output().expect("wait");
    assert_eq!(done.status.code(), Some(0), "{done:?}");
    assert!(done.stderr.is_empty(), "no panic: {done:?}");
}

#[cfg(unix)]
#[test]
fn status_shows_progress_beside_a_live_run() {
    let dir = TempDir::new().expect("a temp dir");
    std::fs::write(
        dir.path().join("busy.rhai"),
        r#"
        progress("busy");
        sh("touch started; while [ ! -f go ]; do sleep 0.02; done");
        "ok"
        "#,
    )
    .expect("write the script");
    let child = harvest(dir.path())
        .args(["run", "busy.rhai", "--id", "b1"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    wait_until("the step to start", || dir.path().join("started").exists());

    // The live `run` holds the writer lock while its step runs.
    let status = stdout(&output(harvest(dir.path()).args(["status", "b1"])));
    assert!(status.contains("state     RUNNING"), "{status}");
    assert!(status.contains("progress  busy"), "{status}");

    std::fs::write(dir.path().join("go"), "").expect("release the step");
    let done = child.wait_with_output().expect("wait");
    assert_eq!(done.status.code(), Some(0), "{done:?}");
    assert_eq!(String::from_utf8_lossy(&done.stdout).trim(), "ok");
}

#[test]
fn status_lists_every_run_past_one_page() {
    let dir = TempDir::new().expect("a temp dir");
    // More runs than one listing page holds (100).
    let total = 105;
    {
        let mut rt = rhai_scripts::open(dir.path().join("runs.db")).expect("open");
        for _ in 0..total {
            let input = serde_json::to_value(rhai_scripts::ScriptInput {
                path: "noop.rhai".to_string(),
                source: "1".to_string(),
                args: Vec::new(),
                cwd: dir.path().display().to_string(),
                max_operations: rhai_scripts::bridge::MAX_OPERATIONS,
            })
            .expect("input");
            rt.start_workflow(rhai_scripts::WORKFLOW, input)
                .expect("start");
        }
    }

    let listed = output(harvest(dir.path()).arg("status"));
    assert_eq!(listed.status.code(), Some(0), "{listed:?}");
    assert_eq!(stdout(&listed).lines().count(), total);
}
