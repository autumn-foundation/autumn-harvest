//! `cargo xtask dev run-standalone-chapter`: the port of
//! `scripts/run-standalone-chapter.sh`.
//!
//! Runs the bash blocks of `docs/getting-started/standalone-axum.md` as
//! written (issue #1614). The two previous defects in the example docs were
//! both "the documented command does not work when pasted". This command
//! pastes them.
//!
//! Each shell block in the chapter has a marker line before it:
//!
//! ```text
//! <!-- chapter-run: serve -->         Start in the background. Wait for health.
//! <!-- chapter-run: expect TEXT -->   Run once. Exit 0. Output contains TEXT.
//! <!-- chapter-run: preflight -->     Run once. Exit 0 (pass) or 2 (warn).
//! <!-- chapter-run: skip REASON -->   Do not run. REASON says why.
//! ```
//!
//! After the last block, the command sends Ctrl-C (SIGINT) to the server.
//! The server must then log "Harvest stopped". `main.rs` logs it after
//! `harvest.stop()`. So CI also runs the shutdown path.
//!
//! `cargo xtask audit standalone-chapter-sync` checks that every shell block
//! has a marker and that each marker parses. This command also fails when
//! the number of markers and the number of extracted blocks differ.
//!
//! Linux only (`setsid`). Run it from a clean checkout with the Postgres of
//! the chapter up:
//!
//! ```sh
//! docker compose -f examples/standalone-quickstart/compose.yaml up -d --wait
//! cargo xtask dev run-standalone-chapter
//! ```
//!
//! `CHAPTER_HEALTH_TIMEOUT_SECS` sets the health wait (default 180). The
//! wait counts its probes as `seq 1 VALUE` does. A value that is not a
//! number gives no probe, so the wait fails at once.
//! Each failure exits with 1.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::Duration;

use smop::prelude::*;

use crate::util::repo_root;

/// The chapter, relative to the repository root.
const DOC: &str = "docs/getting-started/standalone-axum.md";

/// The health route of the chapter server.
const HEALTH_URL: &str = "http://localhost:3000/api/harvest/health";

/// The line that the server logs after a clean stop.
const STOP_LOG: &str = "Harvest stopped";

/// The default health wait, in seconds.
const DEFAULT_HEALTH_TIMEOUT_SECS: u64 = 180;

/// Returns the number of health probes for a timeout value, as
/// `seq 1 VALUE` counts them.
///
/// `seq` takes a decimal or a hex number, and a fraction rounds down. A
/// value that `seq` refuses gives no probe, so the wait times out at once.
// The value is a whole number of at least 1 before the cast. The cast
// saturates, so an infinite value waits as long as `seq` does.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn health_probes(value: &str) -> Option<u64> {
    let text = value.trim_start();
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).ok();
    }
    let number = text.parse::<f64>().ok().filter(|number| !number.is_nan())?;
    Some(if number < 1.0 {
        0
    } else {
        number.floor() as u64
    })
}

/// How long the server has to drain after Ctrl-C, in seconds.
const STOP_WAIT_SECS: u64 = 60;

/// Arguments of `cargo xtask dev run-standalone-chapter`. It takes none.
#[derive(Debug, Args)]
pub struct Args {}

/// One marked shell block of the chapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// The text of the marker between `chapter-run: ` and ` -->`.
    pub kind: String,
    /// The lines of the block, each one ended by a newline.
    pub body: String,
}

/// What a marker asks the runner to do.
#[derive(Debug, PartialEq, Eq)]
pub enum Kind<'a> {
    /// Start the block in the background and wait for health.
    Serve,
    /// Run the block once. Its output must contain the text.
    Expect(&'a str),
    /// Run the block once. It must exit 0 or 2.
    Preflight,
    /// Do not run the block, for the reason given.
    Skip(&'a str),
    /// A kind that the runner does not know.
    Unknown,
}

/// Classifies a marker kind, as the `case` of the old script did.
pub fn classify(kind: &str) -> Kind<'_> {
    if kind == "serve" {
        Kind::Serve
    } else if kind == "preflight" {
        Kind::Preflight
    } else if let Some(want) = kind.strip_prefix("expect ") {
        Kind::Expect(want)
    } else if let Some(reason) = kind.strip_prefix("skip ") {
        Kind::Skip(reason)
    } else {
        Kind::Unknown
    }
}

/// Returns the kind of a marker line, or `None` for another line.
fn marker(line: &str) -> Option<&str> {
    line.strip_prefix("<!-- chapter-run: ")?
        .strip_suffix(" -->")
}

/// Extracts each marked bash block of `doc`.
///
/// The rules and their order are those of the old awk program. A marker
/// line sets the pending kind, even inside a block. The next "```bash"
/// line after a marker opens a block. A bare "```" line closes it. Each
/// line loses one trailing carriage return first.
pub fn extract_blocks(doc: &str) -> Vec<Block> {
    let mut blocks: Vec<Block> = Vec::new();
    let mut kind = String::new();
    let mut pending = false;
    let mut inblock = false;
    for raw in awk_lines(doc) {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if let Some(found) = marker(line) {
            found.clone_into(&mut kind);
            pending = true;
            continue;
        }
        if pending && line == "```bash" {
            blocks.push(Block {
                kind: kind.clone(),
                body: String::new(),
            });
            inblock = true;
            pending = false;
            continue;
        }
        if inblock && line == "```" {
            inblock = false;
            continue;
        }
        if inblock && let Some(block) = blocks.last_mut() {
            block.body.push_str(line);
            block.body.push('\n');
        }
    }
    blocks
}

/// Splits `doc` into lines as awk reads them. A carriage return stays.
fn awk_lines(doc: &str) -> impl Iterator<Item = &str> {
    let body = doc.strip_suffix('\n').unwrap_or(doc);
    body.split('\n').filter(move |_| !doc.is_empty())
}

/// Counts the lines of `doc` that name `chapter-run`, as `grep -c` does.
pub fn count_markers(doc: &str) -> usize {
    awk_lines(doc)
        .filter(|line| line.contains("chapter-run"))
        .count()
}

/// Runs `cargo xtask dev run-standalone-chapter`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    std::env::set_current_dir(repo_root()).context("cannot enter the repository root")?;
    if !Path::new(DOC).is_file() {
        eprintln!("{DOC}: not found");
        return Ok(ExitCode::FAILURE);
    }
    let timeout = std::env::var("CHAPTER_HEALTH_TIMEOUT_SECS")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_HEALTH_TIMEOUT_SECS.to_string());
    let mut session = Session {
        work: smop::fs::temp_dir()?,
        serve: None,
        timeout,
    };
    let outcome = session.run_blocks();
    session.cleanup(matches!(outcome, Ok(true)));
    Ok(crate::util::gate_exit(outcome?))
}

/// The state that the cleanup of the old script read.
struct Session {
    work: smop::fs::TempDir,
    serve: Option<Child>,
    /// The raw value of `CHAPTER_HEALTH_TIMEOUT_SECS`. The timeout message
    /// names it as given.
    timeout: String,
}

impl Session {
    fn serve_log(&self) -> PathBuf {
        self.work.path().join("serve.log")
    }

    /// Runs each block. Returns `false` after a failure that it reports.
    fn run_blocks(&mut self) -> Result<bool> {
        let doc = std::fs::read_to_string(DOC).with_context(|| format!("cannot read {DOC}"))?;
        let blocks = extract_blocks(&doc);
        let markers = count_markers(&doc);
        if blocks.len() != markers {
            eprintln!(
                "::error::{DOC} has {markers} chapter-run lines but {} extracted blocks",
                blocks.len()
            );
            return Ok(false);
        }

        for (index, block) in blocks.iter().enumerate() {
            let path = self.work.path().join(format!("{:02}", index + 1));
            std::fs::write(&path, &block.body)
                .with_context(|| format!("cannot write {}", path.display()))?;
            println!("--- chapter-run: {}", block.kind);
            print!("{}", block.body);
            if !self.run_block(&block.kind, &path)? {
                return Ok(false);
            }
        }

        let Some(mut serve) = self.serve.take() else {
            eprintln!("::error::{DOC} has no chapter-run: serve block");
            return Ok(false);
        };
        // Press Ctrl-C, as the chapter tells the reader to. Then wait for the drain.
        signal_group("-INT", serve.id());
        for _ in 0..STOP_WAIT_SECS {
            if serve
                .try_wait()
                .context("cannot poll the server")?
                .is_some()
            {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let log = std::fs::read(self.serve_log()).unwrap_or_default();
        if !String::from_utf8_lossy(&log).contains(STOP_LOG) {
            self.serve = Some(serve);
            eprintln!("::error::the server did not log '{STOP_LOG}' after Ctrl-C");
            return Ok(false);
        }
        println!("every chapter-run block ran as written, and the server stopped cleanly");
        Ok(true)
    }

    /// Runs one block by its kind. Returns `false` after a failure.
    fn run_block(&mut self, kind: &str, path: &Path) -> Result<bool> {
        match classify(kind) {
            Kind::Serve => {
                if curl(&["-s", "-o", "/dev/null", HEALTH_URL]) {
                    eprintln!("::error::a server already answers on {HEALTH_URL}");
                    return Ok(false);
                }
                let log = File::create(self.serve_log()).context("cannot create serve.log")?;
                let err = log.try_clone().context("cannot share serve.log")?;
                // The serve block runs in its own process group. The cleanup
                // stops cargo and the app together.
                let spawned = Command::new("setsid")
                    .arg("bash")
                    .arg(path)
                    .stdin(Stdio::null())
                    .stdout(log)
                    .stderr(err)
                    .spawn();
                match spawned {
                    Ok(child) => self.serve = Some(child),
                    Err(err) => {
                        // A background job that cannot start writes its error
                        // to its log. The wait then sees a server that exited.
                        std::fs::write(self.serve_log(), format!("setsid: {err}\n"))
                            .context("cannot write serve.log")?;
                        self.serve = None;
                    }
                }
                self.wait_for_health()
            }
            Kind::Expect(want) => {
                if want.replace(' ', "").is_empty() {
                    eprintln!("::error::an expect marker has no text");
                    return Ok(false);
                }
                let (code, output) = run_captured(path)?;
                println!("{output}");
                if code != Some(0) {
                    eprintln!("::error::the block exited non-zero");
                    return Ok(false);
                }
                if !output.contains(want) {
                    eprintln!("::error::the output does not contain '{want}'");
                    return Ok(false);
                }
                Ok(true)
            }
            Kind::Preflight => {
                let (code, output) = run_captured(path)?;
                let status = code.unwrap_or(1);
                println!("{output}");
                println!("exit={status}");
                if status != 0 && status != 2 {
                    eprintln!("::error::preflight must pass (0) or warn (2), got {status}");
                    return Ok(false);
                }
                Ok(true)
            }
            Kind::Skip(reason) => {
                println!("skipped: {reason}");
                Ok(true)
            }
            Kind::Unknown => {
                eprintln!("::error::unknown chapter-run kind: {kind}");
                Ok(false)
            }
        }
    }

    /// Waits until the health route answers. Returns `false` on a timeout
    /// or when the server exits first.
    fn wait_for_health(&mut self) -> Result<bool> {
        let probes = health_probes(&self.timeout).unwrap_or_else(|| {
            eprintln!(
                "run-standalone-chapter: CHAPTER_HEALTH_TIMEOUT_SECS is not a number: '{}'",
                self.timeout
            );
            0
        });
        for second in 1..=probes {
            if curl(&["-sf", "-o", "/dev/null", HEALTH_URL]) {
                println!("healthy after {second}s");
                return Ok(true);
            }
            let exited = match self.serve.as_mut() {
                Some(serve) => serve
                    .try_wait()
                    .context("cannot poll the server")?
                    .is_some(),
                None => true,
            };
            if exited {
                eprintln!("::error::the serve block exited before it became healthy");
                return Ok(false);
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        eprintln!("::error::{HEALTH_URL} did not answer in {}s", self.timeout);
        Ok(false)
    }

    /// Stops the server group, and prints its log after a failure.
    fn cleanup(&mut self, passed: bool) {
        if let Some(serve) = self.serve.take() {
            // The serve block runs in its own process group. Stop cargo and the app.
            signal_group("-TERM", serve.id());
        }
        let log = self.serve_log();
        if !passed && log.is_file() {
            eprintln!("--- server log");
            if let Ok(text) = std::fs::read(&log) {
                use std::io::Write as _;
                let _ = std::io::stderr().write_all(&text);
            }
        }
    }
}

/// Runs `curl` with `args` and reports whether it exited 0.
///
/// A `curl` that cannot start counts as a failed probe, as in the old
/// script. The wait then times out.
fn curl(args: &[&str]) -> bool {
    let status = Command::new("curl")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    match status {
        Ok(status) => status.success(),
        Err(err) => {
            eprintln!("run-standalone-chapter: curl: {err}");
            false
        }
    }
}

/// Sends `signal` to the process group that `pid` leads, as
/// `kill SIGNAL -- -PID` does. A gone group is not an error.
fn signal_group(signal: &str, pid: u32) {
    let _ = Command::new("kill")
        .args([signal, "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Runs a block with `bash -eo pipefail`, with its standard error merged into
/// its standard output.
///
/// Returns the exit code and the output without its trailing newlines, as a
/// shell command substitution gives it.
fn run_captured(path: &Path) -> Result<(Option<i32>, String)> {
    use std::io::Read as _;
    let (mut reader, writer) = std::io::pipe().context("cannot make a pipe")?;
    let mut command = Command::new("bash");
    command
        .args(["-eo", "pipefail"])
        .arg(path)
        .stdout(writer.try_clone().context("cannot share the pipe")?)
        .stderr(writer);
    let mut child = command.spawn().context("cannot start bash")?;
    // The command holds the write ends. Drop it, so the read sees the end.
    drop(command);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .context("cannot read the block output")?;
    let status = child.wait().context("cannot wait for the block")?;
    let mut output = String::from_utf8_lossy(&bytes).replace('\0', "");
    while output.ends_with('\n') {
        output.pop();
    }
    Ok((shell_code(status), output))
}

/// Returns the exit code that a shell reports for `status`.
fn shell_code(status: std::process::ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return Some(128 + signal);
        }
    }
    status.code()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = "\
intro
<!-- chapter-run: skip a reason -->\r
```bash\r
docker compose up\r
```\r
<!-- chapter-run: expect migration(s) applied -->
```bash
cargo run -p cli -- migrate run \\
  --database-url x
```
```bash
echo unmarked
```
<!-- chapter-run: serve -->
```sh
not bash
```
```bash
cargo run -p app
```
";

    #[test]
    fn blocks_follow_their_markers() {
        let blocks = extract_blocks(FIXTURE);
        let kinds: Vec<&str> = blocks.iter().map(|b| b.kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["skip a reason", "expect migration(s) applied", "serve"]
        );
        assert_eq!(blocks[0].body, "docker compose up\n");
        assert_eq!(
            blocks[1].body,
            "cargo run -p cli -- migrate run \\\n  --database-url x\n"
        );
        // A marker stays pending past a block that is not bash.
        assert_eq!(blocks[2].body, "cargo run -p app\n");
        assert_eq!(count_markers(FIXTURE), 3);
    }

    #[test]
    fn a_marker_needs_its_exact_frame() {
        assert_eq!(marker("<!-- chapter-run: serve -->"), Some("serve"));
        assert_eq!(marker("<!-- chapter-run:  -->"), Some(""));
        assert_eq!(marker("<!-- chapter-run: -->"), None);
        assert_eq!(marker(" <!-- chapter-run: serve -->"), None);
        assert_eq!(marker("<!-- chapter-run: a --> -->"), Some("a -->"));
        // A frame without a block still counts as a marker line.
        let doc = "<!-- chapter-run: serve -->\ntext\n";
        assert_eq!(extract_blocks(doc), Vec::<Block>::new());
        assert_eq!(count_markers(doc), 1);
    }

    #[test]
    fn an_empty_or_open_block_still_counts() {
        let blocks = extract_blocks("<!-- chapter-run: serve -->\n```bash\n```\n");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].body, "");
        let open = extract_blocks("<!-- chapter-run: serve -->\n```bash\nrun\n");
        assert_eq!(open[0].body, "run\n");
        // Awk strips one carriage return of each line, not two.
        let doubled = extract_blocks("<!-- chapter-run: serve -->\n```bash\r\nrun\r\r\n```\n");
        assert_eq!(doubled[0].body, "run\r\n");
    }

    #[test]
    fn kinds_parse_as_the_case_statement_did() {
        assert_eq!(classify("serve"), Kind::Serve);
        assert_eq!(classify("preflight"), Kind::Preflight);
        assert_eq!(
            classify("expect \"output\": \"sent\""),
            Kind::Expect("\"output\": \"sent\"")
        );
        assert_eq!(classify("expect  "), Kind::Expect(" "));
        assert_eq!(classify("skip CI runs it"), Kind::Skip("CI runs it"));
        assert_eq!(classify("expect"), Kind::Unknown);
        assert_eq!(classify("serve now"), Kind::Unknown);
    }

    #[test]
    fn the_health_wait_counts_probes_as_seq_does() {
        assert_eq!(health_probes("180"), Some(180));
        assert_eq!(health_probes("0"), Some(0));
        assert_eq!(health_probes("-5"), Some(0));
        assert_eq!(health_probes("3.5"), Some(3));
        assert_eq!(health_probes("1e1"), Some(10));
        assert_eq!(health_probes(" 4"), Some(4));
        assert_eq!(health_probes("0x3"), Some(3));
        // `seq` refuses these, so the wait gets no probe.
        assert_eq!(health_probes("abc"), None);
        assert_eq!(health_probes("4 "), None);
        assert_eq!(health_probes("nan"), None);
    }

    #[test]
    fn the_real_chapter_has_one_block_per_marker() {
        let doc = crate::util::read_repo_file(DOC).unwrap();
        let blocks = extract_blocks(&doc);
        assert_eq!(blocks.len(), count_markers(&doc));
        assert!(blocks.iter().any(|b| classify(&b.kind) == Kind::Serve));
        assert!(blocks.iter().all(|b| classify(&b.kind) != Kind::Unknown));
    }
}
