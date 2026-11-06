//! `cargo xtask check formal-models`: model-check every row of
//! `formal/tla/models.txt` with TLC (issue #1819).
//!
//! This is the port of `scripts/check-formal-models.sh`.
//!
//! A `pass` row must finish with no error. A `violation:<Inv>` row must stop
//! with TLC exit code 12 (a safety violation) on that invariant.
//!
//! `TLA2TOOLS_JAR` names a local `tla2tools.jar`. Without it, the command
//! downloads the pinned release and checks its SHA-256.
//!
//! The module also holds the helpers that the other formal gates share. They
//! are the TLC pin, the checked download, and the exit code of a failed step.

use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ExitCode, ExitStatus};

use smop::prelude::*;

use crate::util::repo_path;

/// The pinned TLC release. `cargo xtask check formal-traces` uses it too.
pub const TLC_VERSION: &str = "1.7.4";

/// The SHA-256 of `tla2tools.jar` of [`TLC_VERSION`].
pub const TLC_SHA256: &str = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88";

/// The manifest that lists each model run.
const MANIFEST: &str = "formal/tla/models.txt";

/// The exit code that TLC gives for a safety violation.
const TLC_VIOLATION: i32 = 12;

/// The exit code of a shell for a command that it cannot find.
const NOT_FOUND: i32 = 127;

/// Arguments of `cargo xtask check formal-models`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask check formal-models`.
pub fn run(_args: Args) -> Result<ExitCode> {
    finish(check_models())
}

/// One row of the manifest.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    spec: String,
    config: String,
    expect: String,
}

/// Splits the manifest into rows, as `read -r spec config expect` does.
///
/// The third field takes the rest of the line. An empty line and a line
/// whose first field starts with `#` hold no row.
fn rows(manifest: &str) -> Vec<Row> {
    let mut lines: Vec<&str> = manifest.split('\n').collect();
    if manifest.ends_with('\n') {
        lines.pop();
    }
    lines
        .into_iter()
        .filter_map(|line| {
            let (spec, rest) = shell_field(line);
            let (config, rest) = shell_field(rest);
            let expect = rest.trim_matches(is_ifs);
            if spec.is_empty() || spec.starts_with('#') {
                return None;
            }
            Some(Row {
                spec: spec.to_string(),
                config: config.to_string(),
                expect: expect.to_string(),
            })
        })
        .collect()
}

/// True for a character of the default shell `IFS`.
const fn is_ifs(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n')
}

/// Splits off the first `IFS` field of `text`, and returns it with the rest.
fn shell_field(text: &str) -> (&str, &str) {
    let text = text.trim_start_matches(is_ifs);
    let end = text.find(is_ifs).unwrap_or(text.len());
    (&text[..end], text[end..].trim_start_matches(is_ifs))
}

/// The verdict of one row, from the TLC exit code and the TLC log.
fn verdict(expect: &str, code: i32, log: &str) -> String {
    if expect == "pass" {
        if code != 0 || !log.contains("No error has been found") {
            return format!("FAIL: want no error, TLC exit {code}");
        }
        return "ok".to_string();
    }
    if let Some(inv) = expect.strip_prefix("violation:") {
        if code != TLC_VIOLATION {
            return format!("FAIL: want a violation of {inv}, TLC exit {code}");
        }
        if !log.contains(&format!("Invariant {inv} is violated")) {
            return format!("FAIL: TLC violated another invariant, not {inv}");
        }
        return "ok".to_string();
    }
    format!("FAIL: bad expect field '{expect}'")
}

/// Checks every manifest row, and returns the exit code of the gate.
fn check_models() -> Result<ExitCode> {
    let root = crate::util::repo_root();
    let manifest = repo_path(MANIFEST);
    let work = fs::temp_dir()?;
    let jar = tla2tools_jar(work.path())?;

    let text = match std::fs::read(&manifest) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(error) => {
            eprintln!("{}: {}", manifest.display(), strerror(&error));
            return Ok(ExitCode::FAILURE);
        }
    };
    let rows = rows(&text);
    let mut failures = 0;
    for row in &rows {
        let log_path = work.path().join(format!("{}.log", row.config));
        let meta = work.path().join(format!("meta-{}", row.config));
        let code = run_logged(
            std::process::Command::new("java")
                .args(["-XX:+UseParallelGC", "-cp"])
                .arg(&jar)
                .args(["tlc2.TLC", "-config", &row.config, "-metadir"])
                .arg(&meta)
                .args(["-workers", "auto", "-cleanup"])
                .arg(format!("{}.tla", row.spec))
                .current_dir(root.join("formal/tla")),
            &log_path,
        )?;
        let log = std::fs::read(&log_path).unwrap_or_default();
        let verdict = verdict(&row.expect, code, &String::from_utf8_lossy(&log));
        println!("{} {} ({}): {verdict}", row.spec, row.config, row.expect);
        if verdict != "ok" {
            failures += 1;
            let mut out = std::io::stdout().lock();
            out.write_all(&log)?;
            out.flush()?;
        }
    }

    if rows.is_empty() {
        eprintln!("no rows in {}", manifest.display());
        return Ok(ExitCode::FAILURE);
    }
    if failures != 0 {
        eprintln!("{failures} of {} model checks failed", rows.len());
        return Ok(ExitCode::FAILURE);
    }
    println!("all {} model checks passed", rows.len());
    Ok(ExitCode::SUCCESS)
}

/// Runs `command` with stdout and stderr in the file `log`.
///
/// Returns the exit code, as a shell reports it. A program that does not
/// start gives 127, as in the shell, and its error goes to the log.
fn run_logged(command: &mut std::process::Command, log: &Path) -> Result<i32> {
    let file = std::fs::File::create(log).with_context(|| format!("creating {}", log.display()))?;
    let err = file.try_clone()?;
    match command.stdout(file).stderr(err).status() {
        Ok(status) => Ok(shell_code(status)),
        Err(error) => {
            let program = command.get_program().to_string_lossy().into_owned();
            let message = if error.kind() == std::io::ErrorKind::NotFound {
                format!("{program}: command not found\n")
            } else {
                format!("{program}: {}\n", strerror(&error))
            };
            std::fs::write(log, message)?;
            Ok(NOT_FOUND)
        }
    }
}

/// The text of an OS error without the ` (os error N)` suffix of Rust.
pub fn strerror(error: &std::io::Error) -> String {
    let text = error.to_string();
    match text.rfind(" (os error ") {
        Some(at) => text[..at].to_string(),
        None => text,
    }
}

/// The exit code of `status`, as a shell reports it.
///
/// A process that a signal kills gives 128 plus the signal number.
pub fn shell_code(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    1
}

/// A step that failed with this exit code.
///
/// The scripts ran under `set -e`, so a failed step ended the script with the
/// exit code of that step. [`finish`] turns this error back into that code.
#[derive(Debug)]
pub struct StepFailed(pub i32);

impl fmt::Display for StepFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a step failed with exit code {}", self.0)
    }
}

impl std::error::Error for StepFailed {}

/// Runs one step with inherited stdio. A failed step ends the command.
///
/// A program that does not start fails as in the shell, with exit code 127
/// or 126.
pub fn step(command: sh::CommandBuilder) -> Result<()> {
    let line = command.to_string();
    let program = line
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string();
    let status = match command.status() {
        Ok(status) => status,
        Err(error) => {
            let Some((code, reason)) = start_failure(&error, Path::new(&program)) else {
                return Err(error);
            };
            eprintln!("{reason}");
            return Err(StepFailed(code).into());
        }
    };
    if status.success() {
        Ok(())
    } else {
        Err(StepFailed(shell_code(status)).into())
    }
}

/// The shell exit code and message of a program that did not start.
///
/// The shell gives 127 for a missing program and 126 for one that it cannot
/// run. The message is the text of the shell without the prefix of the
/// script name and line. Another error gives `None`.
pub fn start_failure(error: &smop::anyhow::Error, program: &Path) -> Option<(i32, String)> {
    let io = error.root_cause().downcast_ref::<std::io::Error>()?;
    let program = program.display().to_string();
    let named = program.contains('/');
    let (code, reason) = match io.kind() {
        std::io::ErrorKind::NotFound if named => (NOT_FOUND, "No such file or directory"),
        std::io::ErrorKind::NotFound => (NOT_FOUND, "command not found"),
        std::io::ErrorKind::PermissionDenied => (126, "Permission denied"),
        _ => return None,
    };
    Some((code, format!("{program}: {reason}")))
}

/// Maps a [`StepFailed`] error to the exit code of its step.
pub fn finish(outcome: Result<ExitCode>) -> Result<ExitCode> {
    match outcome {
        Err(error) => match error.downcast_ref::<StepFailed>() {
            Some(StepFailed(code)) => Ok(ExitCode::from(u8::try_from(*code).unwrap_or(1))),
            None => Err(error),
        },
        ok => ok,
    }
}

/// Returns the TLC jar: `TLA2TOOLS_JAR`, or the pinned release in `work`.
///
/// The download must match [`TLC_SHA256`].
pub fn tla2tools_jar(work: &Path) -> Result<PathBuf> {
    if let Some(jar) = std::env::var_os("TLA2TOOLS_JAR").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(jar));
    }
    let jar = work.join("tla2tools.jar");
    step(
        sh::cmd("curl")
            .args(["-fsSL", "--retry", "3", "-o"])
            .arg(&jar)
            .arg(format!(
                "https://github.com/tlaplus/tlaplus/releases/download/v{TLC_VERSION}/tla2tools.jar"
            )),
    )?;
    verify_sha256(&jar, TLC_SHA256)?;
    Ok(jar)
}

/// Checks the SHA-256 of `file` with `sha256sum --check --quiet`.
pub fn verify_sha256(file: &Path, sha256: &str) -> Result<()> {
    let mut list = file.as_os_str().to_owned();
    list.push(".sha256");
    let list = PathBuf::from(list);
    fs::write_string(&list, format!("{sha256}  {}\n", file.display()))?;
    step(sh::cmd("sha256sum").args(["--check", "--quiet"]).arg(&list))
}

/// Runs `command` with stdout and stderr on one pipe, as `2>&1 | tee` does.
///
/// The output goes to stdout as it arrives. The result holds the exit
/// status and a copy of the output.
pub fn tee_merged(command: &mut std::process::Command) -> Result<(ExitStatus, Vec<u8>)> {
    let (mut reader, writer) = std::io::pipe().context("creating a pipe")?;
    let line = format!("{command:?}");
    let mut child = command
        .stdout(writer.try_clone()?)
        .stderr(writer)
        .spawn()
        .with_context(|| format!("Failed to execute command: {line}"))?;
    // The command keeps a copy of each pipe end. Drop them both, or the
    // read below never sees the end of the output.
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut copy = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut out = std::io::stdout();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        out.write_all(&buffer[..n])?;
        out.flush()?;
        copy.extend_from_slice(&buffer[..n]);
    }
    let status = child.wait()?;
    Ok((status, copy))
}

/// The lines of `text`, split on `\n` only, as `grep` reads them.
pub fn grep_lines(text: &str) -> impl Iterator<Item = &str> {
    text.strip_suffix('\n').unwrap_or(text).split('\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_read_like_the_shell_loop() {
        let text =
            "# c\n\nA  A.cfg   pass\n  # indented\nB B.cfg violation:X \nC C.cfg pass extra\nD";
        assert_eq!(
            rows(text),
            vec![
                Row {
                    spec: "A".into(),
                    config: "A.cfg".into(),
                    expect: "pass".into()
                },
                Row {
                    spec: "B".into(),
                    config: "B.cfg".into(),
                    expect: "violation:X".into()
                },
                Row {
                    spec: "C".into(),
                    config: "C.cfg".into(),
                    expect: "pass extra".into()
                },
                Row {
                    spec: "D".into(),
                    config: String::new(),
                    expect: String::new()
                },
            ]
        );
        assert_eq!(rows(""), []);
        assert_eq!(rows("# only\n"), []);
    }

    #[test]
    fn the_real_manifest_has_rows() {
        let text = std::fs::read_to_string(repo_path(MANIFEST)).expect("read the manifest");
        let rows = rows(&text);
        assert_ne!(rows.len(), 0);
        assert!(
            rows.iter()
                .all(|r| r.expect == "pass" || r.expect.starts_with("violation:"))
        );
    }

    #[test]
    fn verdicts_follow_the_expect_field() {
        let pass = "Model checking completed. No error has been found.";
        assert_eq!(verdict("pass", 0, pass), "ok");
        assert_eq!(
            verdict("pass", 0, "other"),
            "FAIL: want no error, TLC exit 0"
        );
        assert_eq!(verdict("pass", 1, pass), "FAIL: want no error, TLC exit 1");
        let violated = "Error: Invariant Inv is violated.";
        assert_eq!(verdict("violation:Inv", 12, violated), "ok");
        assert_eq!(
            verdict("violation:Inv", 0, violated),
            "FAIL: want a violation of Inv, TLC exit 0"
        );
        assert_eq!(
            verdict("violation:Other", 12, violated),
            "FAIL: TLC violated another invariant, not Other"
        );
        assert_eq!(verdict("", 0, pass), "FAIL: bad expect field ''");
    }

    #[test]
    fn a_missing_program_logs_like_the_shell() {
        let dir = fs::temp_dir().expect("temp dir");
        let log = dir.path().join("x.log");
        let code = run_logged(
            &mut std::process::Command::new("no-such-program-xtask"),
            &log,
        )
        .expect("run");
        assert_eq!(code, NOT_FOUND);
        assert_eq!(
            std::fs::read_to_string(&log).expect("log"),
            "no-such-program-xtask: command not found\n"
        );
    }

    #[test]
    fn a_failed_step_keeps_its_exit_code() {
        let outcome =
            finish(step(sh::cmd("sh").args(["-c", "exit 3"])).map(|()| ExitCode::SUCCESS));
        assert_eq!(outcome.expect("mapped"), ExitCode::from(3));
        let other = finish(Err(anyhow!("plain")));
        assert!(other.is_err());
    }

    #[test]
    fn a_program_that_does_not_start_fails_as_in_the_shell() {
        let outcome = finish(step(sh::cmd("no-such-program-xtask")).map(|()| ExitCode::SUCCESS));
        assert_eq!(outcome.expect("mapped"), ExitCode::from(127));
        let denied = anyhow!(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            start_failure(&denied, Path::new("/x/verus")),
            Some((126, "/x/verus: Permission denied".to_string()))
        );
        let missing = anyhow!(std::io::Error::from(std::io::ErrorKind::NotFound));
        assert_eq!(
            start_failure(&missing, Path::new("/x/verus")),
            Some((127, "/x/verus: No such file or directory".to_string()))
        );
        assert_eq!(
            start_failure(&missing, Path::new("cargo")),
            Some((127, "cargo: command not found".to_string()))
        );
        assert_eq!(start_failure(&anyhow!("plain"), Path::new("x")), None);
    }

    #[test]
    fn tee_merged_captures_both_streams() {
        let (status, out) =
            tee_merged(std::process::Command::new("sh").args(["-c", "echo a; echo b >&2; exit 4"]))
                .expect("run");
        assert_eq!(status.code(), Some(4));
        assert_eq!(String::from_utf8_lossy(&out), "a\nb\n");
    }

    #[test]
    fn grep_lines_split_on_newline_only() {
        assert_eq!(grep_lines("a\r\nb\n").collect::<Vec<_>>(), ["a\r", "b"]);
        assert_eq!(grep_lines("a").collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn the_checksum_check_rejects_a_changed_file() {
        let dir = fs::temp_dir().expect("temp dir");
        let file = dir.path().join("f");
        std::fs::write(&file, "x").expect("write");
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let outcome = finish(verify_sha256(&file, empty).map(|()| ExitCode::SUCCESS));
        assert_eq!(outcome.expect("mapped"), ExitCode::from(1));
        std::fs::write(&file, "").expect("write");
        assert!(verify_sha256(&file, empty).is_ok());
    }
}
