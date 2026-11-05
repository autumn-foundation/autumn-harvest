//! `cargo xtask dev flake-rerun`: the same-commit rerun protocol for one test
//! binary (Semaphore, CI health).
//!
//! This is the port of the old `scripts/flake-rerun.sh`.
//!
//! Usage: `cargo xtask dev flake-rerun <test-binary> <test-name> <N>
//! [cpu-spinners]`. Run it from the crate directory with
//! `HARVEST_TEST_DATABASE_URL` set. `cargo xtask` keeps the current
//! directory, so a relative binary path resolves from there.
//!
//! It runs `<test-binary> <test-name> --exact` N times. It prints the failure
//! count, and the Postgres event order of each failure. Each CPU spinner is a
//! busy thread that loads one core while the runs go on.

use std::process::{Command, ExitCode, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use regex::Regex;
use smop::prelude::*;

/// Arguments of `cargo xtask dev flake-rerun`.
#[derive(Debug, Args)]
pub struct Args {
    /// The test binary.
    ///
    /// The command splits it into words and expands each glob, as the shell
    /// did. So it can hold a program and its leading arguments, or a glob
    /// such as `target/debug/deps/integration-*`.
    #[arg(value_name = "TEST_BINARY")]
    pub bin: String,
    /// The exact name of the test to run.
    #[arg(value_name = "TEST_NAME")]
    pub test: String,
    /// The number of runs.
    #[arg(value_name = "N")]
    pub runs: u64,
    /// The number of busy threads that load the CPU during the runs.
    #[arg(value_name = "CPU_SPINNERS", default_value_t = 0)]
    pub spinners: u32,
}

/// Runs `cargo xtask dev flake-rerun`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {
        bin,
        test,
        runs,
        spinners,
    } = args;
    let command_line = test_argv(&bin, &test);
    let pattern = Regex::new("postgres:.*(Activity|Signal)").context("the event pattern")?;

    let stop = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = (0..spinners)
        .map(|_| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();

    let mut failures = 0_u64;
    for index in 1..=runs {
        let (passed, output) = run_once(&command_line)?;
        if !passed {
            failures += 1;
            println!("run {index} FAIL: {}", event_order(&pattern, &output));
        }
    }

    stop.store(true, Ordering::Relaxed);
    for spinner in threads {
        // A spinner never panics, so a join error has nothing to report.
        let _ = spinner.join();
    }
    println!("RESULT {test}: {failures}/{runs} failed (spinners={spinners})");
    Ok(ExitCode::SUCCESS)
}

/// The command line of one run, as the shell built `$bin "$t" --exact`.
///
/// The shell split the unquoted `$bin` on spaces, tabs and newlines. Then it
/// expanded each word that holds a glob character. A pattern that matches no
/// path stays as it is, as bash does without `nullglob`. An empty `$bin` gave
/// no word, so the test name became the program.
fn test_argv(bin: &str, test: &str) -> Vec<String> {
    let mut argv: Vec<String> = bin
        .split([' ', '\t', '\n'])
        .filter(|word| !word.is_empty())
        .flat_map(expand_word)
        .collect();
    argv.extend([test.to_string(), "--exact".to_string()]);
    argv
}

/// Expands one unquoted word as a shell glob.
///
/// A word with no glob character, a pattern that matches nothing, or a
/// pattern that cannot be read stays as it is.
fn expand_word(word: &str) -> Vec<String> {
    if !word.contains(['*', '?', '[']) {
        return vec![word.to_string()];
    }
    match fs::glob(word) {
        Ok(paths) if !paths.is_empty() => paths
            .into_iter()
            .map(|path| with_dot_prefix(word, &path.to_string_lossy()))
            .collect(),
        _ => vec![word.to_string()],
    }
}

/// Gives the match `path` of the pattern `word` the leading `./` of `word`.
///
/// The glob crate drops a leading `./`, and bash keeps it. Without it, a bare
/// name resolves on `PATH` instead of the current directory.
fn with_dot_prefix(word: &str, path: &str) -> String {
    if word.starts_with("./") && !path.starts_with("./") {
        format!("./{path}")
    } else {
        path.to_string()
    }
}

/// Runs `command_line` once. It returns whether the run passed and
/// its output.
///
/// The output holds stdout and stderr, interleaved, as `2>&1` gave them. A
/// binary that cannot start is a failed run, as it was for the shell.
fn run_once(command_line: &[String]) -> Result<(bool, String)> {
    let Some((program, rest)) = command_line.split_first() else {
        bail!("the test command line is empty");
    };
    let scratch = fs::temp_dir()?;
    let path = scratch.path().join("run.log");
    let file =
        std::fs::File::create(&path).with_context(|| format!("creating {}", path.display()))?;
    let stderr = file.try_clone().context("opening the run log")?;
    let status = Command::new(program)
        .args(rest)
        .stdin(Stdio::inherit())
        .stdout(Stdio::from(file))
        .stderr(Stdio::from(stderr))
        .status();
    let output = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(match status {
        Ok(status) => (
            status.success(),
            String::from_utf8_lossy(&output).into_owned(),
        ),
        Err(error) => (false, format!("{program}: {error}")),
    })
}

/// The Postgres activity and signal lines of `output`, joined by `|`.
///
/// Repeated spaces collapse to one, and each line ends with `|`. This is the
/// shell pipeline `grep -E "postgres:.*(Activity|Signal)" | tr -s " " | tr
/// "\n" "|"`.
fn event_order(pattern: &Regex, output: &str) -> String {
    let mut joined = String::new();
    for line in output.trim_end_matches('\n').split('\n') {
        if pattern.is_match(line) {
            joined.push_str(&squeeze_spaces(line));
            joined.push('|');
        }
    }
    joined
}

/// Collapses each run of spaces in `line` to one space, as `tr -s " "` does.
fn squeeze_spaces(line: &str) -> String {
    let mut squeezed = String::with_capacity(line.len());
    let mut previous_space = false;
    for ch in line.chars() {
        if ch == ' ' && previous_space {
            continue;
        }
        previous_space = ch == ' ';
        squeezed.push(ch);
    }
    squeezed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern() -> Regex {
        Regex::new("postgres:.*(Activity|Signal)").unwrap()
    }

    #[test]
    fn the_event_order_keeps_postgres_activity_and_signal_lines() {
        let output = "running 1 test\n\
                      postgres:   ActivityScheduled   a\n\
                      other: Activity\n\
                      postgres: SignalReceived  s\n\
                      test failed\n\n";
        assert_eq!(
            event_order(&pattern(), output),
            "postgres: ActivityScheduled a|postgres: SignalReceived s|"
        );
    }

    #[test]
    fn an_output_with_no_event_gives_an_empty_order() {
        assert_eq!(event_order(&pattern(), ""), "");
        assert_eq!(event_order(&pattern(), "postgres: TimerFired\n"), "");
    }

    #[test]
    fn a_failing_binary_counts_as_a_failure() {
        let (passed, _) = run_once(&test_argv("false", "t")).unwrap();
        assert!(!passed);
        let (passed, output) = run_once(&test_argv("echo postgres:", "Activity")).unwrap();
        assert!(passed);
        assert_eq!(output, "postgres: Activity --exact\n");
        let (passed, _) = run_once(&test_argv("definitely-not-a-test-binary", "t")).unwrap();
        assert!(!passed);
    }

    #[test]
    fn the_binary_splits_into_words_as_the_shell_did() {
        assert_eq!(
            test_argv(" env\tFOO=1  ./bin\n", "a::b"),
            ["env", "FOO=1", "./bin", "a::b", "--exact"]
        );
        assert_eq!(test_argv("", "a::b"), ["a::b", "--exact"]);
        assert_eq!(
            test_argv("./bin\u{a0}x", "t"),
            ["./bin\u{a0}x", "t", "--exact"]
        );
    }

    #[test]
    fn a_glob_in_the_binary_expands_or_stays() {
        let dir = fs::temp_dir().unwrap();
        for name in ["integration-b", "integration-a"] {
            fs::write_string(dir.path().join(name), "").unwrap();
        }
        let pattern = format!("{}/integration-*", dir.path().display());
        assert_eq!(
            expand_word(&pattern),
            [
                format!("{}/integration-a", dir.path().display()),
                format!("{}/integration-b", dir.path().display()),
            ]
        );
        let none = format!("{}/nothing-*", dir.path().display());
        assert_eq!(expand_word(&none), std::slice::from_ref(&none));
        assert_eq!(expand_word("./plain"), ["./plain"]);
        assert_eq!(with_dot_prefix("./test*", "testbin"), "./testbin");
        assert_eq!(with_dot_prefix("./test*", "./testbin"), "./testbin");
        assert_eq!(with_dot_prefix("target/x-*", "target/x-1"), "target/x-1");
    }
}
