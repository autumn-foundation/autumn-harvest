//! Helpers that the `cargo xtask check` guards share.
//!
//! The helpers copy the exact behavior of the shell idioms that the old
//! `scripts/check-*.sh` guards used. Each helper names the idiom that it
//! replaces, so a reader can compare the port with the original script.

use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use smop::prelude::*;

/// One line of guard output and the stream that it goes to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Line {
    Out(String),
    Err(String),
}

/// The output and the exit code of one guard run.
///
/// A guard collects its output here and does not print it directly. So a
/// unit test can assert on the exact text and the exit code.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    lines: Vec<Line>,
    code: u8,
}

impl Report {
    /// Returns an empty report with exit code 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one line to stdout, like `echo "$text"`.
    pub fn out(&mut self, text: impl Into<String>) {
        self.lines.push(Line::Out(text.into()));
    }

    /// Adds one line to stderr, like `echo "$text" >&2`.
    pub fn err(&mut self, text: impl Into<String>) {
        self.lines.push(Line::Err(text.into()));
    }

    /// Sets the exit code. A later call replaces an earlier one.
    pub const fn exit(&mut self, code: u8) {
        self.code = code;
    }

    /// Adds one line to stderr and sets exit code 1.
    pub fn fail(&mut self, text: impl Into<String>) {
        self.err(text);
        self.exit(1);
    }

    /// Returns the exit code.
    pub const fn code(&self) -> u8 {
        self.code
    }

    /// Returns the stdout text. Each line ends with a newline.
    #[cfg(test)]
    pub fn stdout(&self) -> String {
        self.collect(true)
    }

    /// Returns the stderr text. Each line ends with a newline.
    #[cfg(test)]
    pub fn stderr(&self) -> String {
        self.collect(false)
    }

    #[cfg(test)]
    fn collect(&self, want_out: bool) -> String {
        let mut text = String::new();
        for line in &self.lines {
            match (line, want_out) {
                (Line::Out(s), true) | (Line::Err(s), false) => {
                    text.push_str(s);
                    text.push('\n');
                }
                _ => {}
            }
        }
        text
    }

    /// Prints each line to its stream in order, and returns the exit code.
    pub fn emit(self) -> ExitCode {
        let mut stdout = std::io::stdout().lock();
        let mut stderr = std::io::stderr().lock();
        for line in self.lines {
            // A closed stream must not turn a finding into a panic.
            let _ = match line {
                Line::Out(s) => writeln!(stdout, "{s}").and_then(|()| stdout.flush()),
                Line::Err(s) => writeln!(stderr, "{s}").and_then(|()| stderr.flush()),
            };
        }
        ExitCode::from(self.code)
    }
}

/// Reads a file as text, or returns `None` when it cannot be read.
///
/// The old guards ran `grep`, `sed` or `awk` on the file. Those tools treat a
/// missing file as an empty input, so the guard reports its normal finding.
pub fn read_or_none(path: &Path) -> Option<String> {
    std::fs::read(path)
        .ok()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
}

/// Reads a file as text, or returns an empty string when it cannot be read.
pub fn read_or_empty(path: &Path) -> String {
    read_or_none(path).unwrap_or_default()
}

/// Splits text into lines the way `awk` and `grep` read them.
///
/// A final newline does not start an extra empty line.
pub fn lines(text: &str) -> Vec<&str> {
    let body = text.strip_suffix('\n').unwrap_or(text);
    if text.is_empty() {
        Vec::new()
    } else {
        body.split('\n').collect()
    }
}

/// Removes every trailing newline, like a `$(...)` command substitution.
pub fn trim_newlines(text: &str) -> &str {
    text.trim_end_matches('\n')
}

/// Joins lines with newlines and removes the trailing newlines.
///
/// This copies `x="$(awk ... )"`: awk prints each line with a newline, and the
/// substitution removes the trailing ones.
pub fn join_captured(parts: &[&str]) -> String {
    trim_newlines(&parts.join("\n")).to_owned()
}

/// Returns the body of the Nth (1-based) ```` ```rust ```` fence of `text`.
///
/// This is the `rust_block` awk function of the old guards:
///
/// ```text
/// /^```rust$/ { n++; if (n == want) { p = 1; next } }
/// p && /^```$/ { exit }
/// p { print }
/// ```
pub fn rust_block(text: &str, want: usize) -> String {
    let mut seen = 0;
    let mut printing = false;
    let mut body = Vec::new();
    for line in lines(text) {
        if line == "```rust" {
            seen += 1;
            if seen == want {
                printing = true;
                continue;
            }
        }
        if printing && line == "```" {
            break;
        }
        if printing {
            body.push(line);
        }
    }
    join_captured(&body)
}

/// Returns the lines from the first line equal to `start` up to the next
/// ```` ``` ```` line, which it leaves out.
///
/// This is the `awk '/^<start>$/ { p = 1 } p && /^```$/ { exit } p { print }'`
/// window of the old guards. The start line is part of the window.
pub fn window_from(text: &str, start: &str) -> String {
    let mut printing = false;
    let mut body = Vec::new();
    for line in lines(text) {
        if line == start {
            printing = true;
        }
        if printing && line == "```" {
            break;
        }
        if printing {
            body.push(line);
        }
    }
    join_captured(&body)
}

/// Lists every `*.md` file under `root`, the way `grep -r --include="*.md" .`
/// finds them.
///
/// Each entry holds the `./`-prefixed path that grep prints and the full
/// path. The walk includes hidden directories and does not follow symlinks,
/// like `grep -r`. It leaves out `./target/`, like the
/// `grep -vE "^\./target/"` filter of the old guards.
///
/// Grep walks the tree depth first and lists each directory in the order that
/// the file system returns. The walk keeps that same order and does not sort.
/// So a finding in two files prints in the same order as the old guard.
pub fn markdown_files(root: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let Ok(relative) = entry.path().strip_prefix(root) else {
            continue;
        };
        let shown = format!("./{}", relative.to_string_lossy());
        // `--include="*.md"` matches the name case-sensitively, and it also
        // matches a file named `.md`, which has no extension for `Path`.
        // The match reads the raw bytes, so a name that is not UTF-8 counts.
        let is_markdown = entry.file_name().as_encoded_bytes().ends_with(b".md");
        if is_markdown && !shown.starts_with("./target/") {
            found.push((shown, entry.path().to_path_buf()));
        }
    }
    found
}

/// Counts the lines of `output` that start with `prefix`, like
/// `grep -c '^<prefix>'`.
pub fn count_prefixed(output: &str, prefix: &str) -> usize {
    lines(output)
        .into_iter()
        .filter(|line| line.starts_with(prefix))
        .count()
}

/// Splits a line at the first colon, the way `IFS=: read -r a b` does.
///
/// Bash drops one trailing colon from the last field when that field holds no
/// other colon. So `1:x:` gives `x`, but `1:x::` gives `x::`.
pub fn bash_read_colon_rest(rest: &str) -> &str {
    match rest.strip_suffix(':') {
        Some(head) if !head.contains(':') => head,
        _ => rest,
    }
}

/// Returns the program that runs `cargo`.
///
/// This is the rule of `smop::sh::cargo`. Cargo sets `CARGO` for each process
/// that `cargo run` starts, so a nested call uses the same toolchain.
pub fn cargo_program() -> OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}

/// Runs `cargo` with `args` in `dir` and merges stdout with stderr.
///
/// This copies `output="$(cargo ... 2>&1)"`. Both streams share one pipe, so
/// the lines keep the order in which cargo wrote them. The result holds the
/// exit status and the output without its trailing newlines.
pub fn cargo_merged(dir: &Path, args: &[&str]) -> Result<(bool, String)> {
    let program = cargo_program();
    let line = format!("{} {}", program.to_string_lossy(), args.join(" "));
    let (mut reader, writer) = std::io::pipe().context("creating a pipe for cargo")?;
    let writer_err = writer.try_clone().context("cloning the cargo pipe")?;
    // The `Command` value holds the write end of the pipe. It drops at the
    // end of this statement, so the read below sees end of file.
    let mut child = Command::new(&program)
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::inherit())
        .stdout(writer)
        .stderr(writer_err)
        .spawn()
        .with_context(|| format!("Failed to execute command: {line}"))?;
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading the output of: {line}"))?;
    let status = child
        .wait()
        .with_context(|| format!("waiting for: {line}"))?;
    let text = String::from_utf8_lossy(&bytes);
    Ok((status.success(), trim_newlines(&text).to_owned()))
}

/// A generated example source file that is removed again on drop.
///
/// This copies the `trap 'rm -f "$example_path"' EXIT` cleanup of the old
/// guards. The constructor also removes a file that a killed run left behind.
#[derive(Debug)]
pub struct ScratchExample {
    path: PathBuf,
}

impl ScratchExample {
    /// Removes any leftover file at `path` and claims the path.
    pub fn claim(path: PathBuf) -> Self {
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// Writes the example source.
    pub fn write(&self, source: &str) -> Result<()> {
        fs::write_string(&self.path, source)
    }
}

impl Drop for ScratchExample {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Builds the text of a generated file from `echo` lines.
///
/// Each part ends with one newline, like one `echo "$part"` call.
pub fn echo_lines(parts: &[&str]) -> String {
    let mut text = String::new();
    for part in parts {
        let _ = writeln!(text, "{part}");
    }
    text
}

/// Writes a generated example, runs `cargo` on it, and reports the result.
///
/// This is the shared tail of the snippet guards. On success it prints
/// `ok_line`. On failure it prints `<failure_head> (<n> <noun>):` and the
/// cargo output, where `n` counts the output lines that start with
/// `error_prefix`.
pub struct SnippetCompile<'a> {
    /// The cargo arguments, for example `check --quiet -p <crate>`.
    pub cargo_args: &'a [&'a str],
    /// The line that a passed check prints.
    pub ok_line: &'a str,
    /// The start of the first failure line.
    pub failure_head: &'a str,
    /// The noun after the error count, for example `error(s)`.
    pub count_noun: &'a str,
    /// The line prefix that counts as one error.
    pub error_prefix: &'a str,
}

impl SnippetCompile<'_> {
    /// Writes `source` to `example`, runs cargo in `root`, and fills a report.
    pub fn run(&self, root: &Path, example: &ScratchExample, source: &str) -> Result<Report> {
        example.write(source)?;
        let (passed, output) = cargo_merged(root, self.cargo_args)?;
        let mut report = Report::new();
        if passed {
            report.out(self.ok_line);
            return Ok(report);
        }
        let errors = count_prefixed(&output, self.error_prefix);
        report.err(format!(
            "{} ({errors} {}):",
            self.failure_head, self.count_noun
        ));
        report.fail(output);
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str =
        "intro\n```rust\nfn a() {}\n```\ntext\n```rust\nfn b() {}\n\n```\n```bash\necho\n```\n";

    #[test]
    fn rust_block_picks_the_nth_fence() {
        assert_eq!(rust_block(DOC, 1), "fn a() {}");
        assert_eq!(rust_block(DOC, 2), "fn b() {}");
        assert_eq!(rust_block(DOC, 3), "");
    }

    #[test]
    fn rust_block_without_a_close_runs_to_the_end() {
        assert_eq!(rust_block("```rust\nx\ny\n", 1), "x\ny");
    }

    #[test]
    fn window_from_keeps_the_start_line_and_stops_at_the_fence() {
        let text = "a\n#[w]\nfn f() {}\n```\nafter\n";
        assert_eq!(window_from(text, "#[w]"), "#[w]\nfn f() {}");
        assert_eq!(window_from(text, "#[missing]"), "");
    }

    #[test]
    fn bash_read_drops_one_lone_trailing_colon() {
        assert_eq!(bash_read_colon_rest("x:"), "x");
        assert_eq!(bash_read_colon_rest("x::"), "x::");
        assert_eq!(bash_read_colon_rest("a:b:"), "a:b:");
        assert_eq!(bash_read_colon_rest(":"), "");
        assert_eq!(bash_read_colon_rest("a"), "a");
    }

    #[test]
    fn count_prefixed_counts_line_starts_only() {
        let output = "error[E1]: x\n  error[E2]\nerror[E3]: y\nwarning: z";
        assert_eq!(count_prefixed(output, "error["), 2);
        assert_eq!(count_prefixed("", "error["), 0);
    }

    #[test]
    fn report_splits_streams_and_keeps_the_code() {
        let mut report = Report::new();
        report.out("ok");
        report.fail("bad");
        assert_eq!(report.stdout(), "ok\n");
        assert_eq!(report.stderr(), "bad\n");
        assert_eq!(report.code(), 1);
    }

    #[test]
    fn markdown_files_match_the_grep_include_rule() {
        let dir = fs::temp_dir().expect("temp dir");
        for file in [
            "a.md",
            ".hidden/b.md",
            "docs/target/c.md",
            "docs/.md",
            "target/d.md",
            "e.MD",
            "f.txt",
        ] {
            let full = dir.path().join(file);
            std::fs::create_dir_all(full.parent().expect("parent")).expect("mkdir");
            std::fs::write(full, "x").expect("write");
        }
        let mut shown: Vec<String> = markdown_files(dir.path())
            .into_iter()
            .map(|(shown, _)| shown)
            .collect();
        shown.sort();
        assert_eq!(
            shown,
            [
                "./.hidden/b.md",
                "./a.md",
                "./docs/.md",
                "./docs/target/c.md"
            ]
        );
    }

    #[test]
    fn scratch_example_removes_its_file() {
        let dir = fs::temp_dir().expect("temp dir");
        let path = dir.path().join("example.rs");
        std::fs::write(&path, "stale").expect("seed");
        {
            let example = ScratchExample::claim(path.clone());
            assert!(!path.exists(), "claim removes a leftover file");
            example.write("fn main() {}").expect("write");
            assert!(path.exists());
        }
        assert!(!path.exists(), "drop removes the file");
    }
}
