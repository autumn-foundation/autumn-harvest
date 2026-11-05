//! `cargo xtask dev formal-traces`: check NDJSON engine traces against the
//! TLA+ trace specs (issue #2003).
//!
//! This is the port of `scripts/formal_traces.py`.
//! `cargo xtask check formal-traces` calls [`check`] directly.
//!
//! Each trace file holds the history of one task row. Line 1 is a header:
//!
//! ```text
//! {"spec": "ActivityClaim", "case": "...", "checks": {"fixed": "accept"}}
//! ```
//!
//! Each other line is one committed transaction on the row. The first of
//! them is the insert of the row, with op `init`.
//!
//! For each check, the command writes a TLA+ module that holds the trace as
//! a sequence. It then runs TLC on `formal/tla/trace/<spec>Trace.tla` with
//! the guard constants of that check. `accept` means that a behavior of the
//! spec matches every line. `reject` means that no behavior does.
//!
//! The command fails in three cases. A result differs from its expectation,
//! TLC fails for another reason, or a directory has no trace for a spec.
//!
//! The port reads JSON as the `json` module of Python 3.12 does. The
//! [`py_json`] module holds that decoder. Each error names a value in the
//! `repr` form of Python.

mod py_json;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Command as Process, ExitCode, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use py_json::{Decode, Json, PyStr, str_repr};
use regex::Regex;
use smop::prelude::*;

/// Arguments of `cargo xtask dev formal-traces`.
#[derive(Debug, Args)]
pub struct Args {
    /// The `tla2tools.jar` of TLC.
    #[arg(long)]
    pub jar: String,
    /// The directory of the TLA+ specs, `formal/tla`.
    #[arg(long)]
    pub tla: String,
    /// A scratch directory for the TLC runs.
    #[arg(long)]
    pub work: String,
    /// The directories of NDJSON traces.
    #[arg(required = true)]
    pub dirs: Vec<String>,
}

/// Runs `cargo xtask dev formal-traces`.
#[expect(
    clippy::needless_pass_by_value,
    reason = "every command takes its `Args` by value, so each dispatch arm has one shape"
)]
pub fn run(args: Args) -> Result<ExitCode> {
    check(
        Path::new(&args.jar),
        Path::new(&args.tla),
        Path::new(&args.work),
        &args.dirs,
    )
}

/// The specs that a trace can name.
const SPECS: [&str; 2] = ["ActivityClaim", "WorkflowTaskClaim"];

/// The guard constants of each check. `fixed` is the code today.
fn guards(spec: &str) -> &'static [(&'static str, &'static [(&'static str, &'static str)])] {
    match spec {
        "ActivityClaim" => &[
            ("fixed", &[("Fenced", "TRUE")]),
            ("pre-fix", &[("Fenced", "FALSE")]),
        ],
        "WorkflowTaskClaim" => &[
            (
                "fixed",
                &[("ChecksAttempt", "TRUE"), ("CapMissGuard", "\"epoch\"")],
            ),
            (
                "pre-fix",
                &[("ChecksAttempt", "FALSE"), ("CapMissGuard", "\"strikes\"")],
            ),
        ],
        _ => &[],
    }
}

/// The guard constants of `guard` for `spec`.
fn guard_constants(spec: &str, guard: &str) -> &'static [(&'static str, &'static str)] {
    guards(spec)
        .iter()
        .find(|(name, _)| *name == guard)
        .map_or(&[], |(_, constants)| constants)
}

/// The step kinds that a trace line can have.
const OPS: [&str; 4] = ["init", "write", "start", "heartbeat"];

/// A state name. Python's `$` also matches before a final newline.
const STATE_PATTERN: &str = r"\A[A-Z_]+\n?\z";

/// An expectation. `reject@N` also names the file line that no behavior may
/// match.
const EXPECT_PATTERN: &str = r"\A(accept|reject|reject@[1-9][0-9]*)\n?\z";

/// The count of distinct states in a TLC log.
const STATES_PATTERN: &str = r"(\d+) distinct states? found";

/// The depth of the search in a TLC log.
const DEPTH_PATTERN: &str = r"The depth of the complete state graph search is (\d+)\.";

/// A trace check takes seconds. The bound stops a hung JVM.
const TLC_TIMEOUT_SECS: u64 = 900;

/// TLC integers are 32-bit.
const MAX_INT: i128 = (1 << 31) - 1;

/// A trace file that the command cannot check.
#[derive(Debug)]
struct TraceError(String);

/// True for an int in `[low, MAX_INT]`. A bool is not an int here.
fn is_count(value: &Json, low: i128) -> bool {
    matches!(value, Json::Int(n) if (low..=MAX_INT).contains(n))
}

/// True for a worker name: a string or null.
const fn is_name(value: &Json) -> bool {
    matches!(value, Json::Null | Json::Str(_))
}

/// The compiled patterns of the command.
struct Patterns {
    state: Regex,
    expect: Regex,
    states: Regex,
    depth: Regex,
}

impl Patterns {
    fn new() -> Result<Self> {
        Ok(Self {
            state: Regex::new(STATE_PATTERN)?,
            expect: Regex::new(EXPECT_PATTERN)?,
            states: Regex::new(STATES_PATTERN)?,
            depth: Regex::new(DEPTH_PATTERN)?,
        })
    }
}

/// Fails when one step line is malformed.
///
/// Every field that reaches the TLA+ text is checked here, so a trace
/// cannot inject TLA+ code.
fn check_line(p: &Patterns, at: &str, index: usize, line: &Json) -> Result<(), TraceError> {
    if !matches!(line, Json::Dict(_)) {
        return Err(TraceError(format!("{at}: a line must be a JSON object")));
    }
    let op = line.field("op");
    let known = op.as_str().is_some_and(|op| OPS.contains(&op));
    if !known || (op.as_str() == Some("init")) != (index == 0) {
        return Err(TraceError(format!("{at}: bad op {}", op.repr())));
    }
    let state = line.field("state");
    if !state.as_str().is_some_and(|s| p.state.is_match(s)) {
        return Err(TraceError(format!("{at}: bad state {}", state.repr())));
    }
    for key in ["attempt", "strikes", "terminal"] {
        if !is_count(line.field(key), 0) {
            return Err(TraceError(format!(
                "{at}: bad {key} {}",
                line.field(key).repr()
            )));
        }
    }
    for key in ["worker", "by"] {
        if !is_name(line.field(key)) {
            return Err(TraceError(format!(
                "{at}: bad {key} {}",
                line.field(key).repr()
            )));
        }
    }
    let actor = line.field("actor");
    let good_actor = match actor {
        Json::Null => true,
        Json::Dict(_) => {
            actor.field("worker").as_str().is_some() && is_count(actor.field("attempt"), 1)
        }
        _ => false,
    };
    if !good_actor {
        return Err(TraceError(format!("{at}: bad actor {}", actor.repr())));
    }
    Ok(())
}

/// The lines of `text`, split as Python's `str.splitlines` does.
fn split_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        let end = match c {
            '\r' => {
                if chars.peek().is_some_and(|(_, next)| *next == '\n') {
                    chars.next();
                    at + 2
                } else {
                    at + 1
                }
            }
            '\n' | '\u{b}' | '\u{c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}'
            | '\u{2029}' => at + c.len_utf8(),
            _ => continue,
        };
        lines.push(&text[start..at]);
        start = end;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// True for a line that holds only white space, as `str.strip` sees it.
fn is_blank(text: &str) -> bool {
    text.chars().all(|c| c.is_whitespace() || c == '\u{1f}')
}

/// One parsed trace file: its spec, its checks and its step lines.
struct Trace {
    spec: String,
    checks: Vec<(String, String)>,
    lines: Vec<Json>,
}

/// Returns the header and the step lines of the text of one trace file.
///
/// `path` names the file in each error. Only the last line may be blank, so
/// a reported line number is the line number in the file.
fn parse(p: &Patterns, path: &str, text: &str) -> Result<Trace, TraceError> {
    let mut rows = Vec::new();
    for (number, line) in split_lines(text).into_iter().enumerate() {
        let number = number + 1;
        if is_blank(line) {
            return Err(TraceError(format!("{path}:{number}: blank line")));
        }
        // A crash of Python also ended the script with exit code 1. Its
        // traceback named the same exception.
        let row = py_json::loads(line).map_err(|error| {
            let (Decode::Invalid(text) | Decode::Crash(text)) = error;
            TraceError(format!("{path}:{number}: {text}"))
        })?;
        rows.push(row);
    }
    if rows.len() < 2 {
        return Err(TraceError(format!(
            "{path}: want a header and at least one line"
        )));
    }
    let lines = rows.split_off(1);
    let header = rows.remove(0);
    if !matches!(header, Json::Dict(_)) {
        return Err(TraceError(format!(
            "{path}:1: the header must be a JSON object"
        )));
    }
    let spec = header.field("spec");
    let Some(spec) = spec.as_str().filter(|s| SPECS.contains(s)) else {
        return Err(TraceError(format!("{path}: unknown spec {}", spec.repr())));
    };
    let checks = match header.field("checks") {
        Json::Dict(items) if !items.is_empty() => items,
        _ => return Err(TraceError(format!("{path}: the header has no checks"))),
    };
    let mut parsed = Vec::new();
    for (guard, expect) in checks {
        let known = guard
            .as_str()
            .filter(|g| guards(spec).iter().any(|(name, _)| name == g));
        let text = expect.as_str().filter(|e| p.expect.is_match(e));
        match (known, text) {
            (Some(guard), Some(text)) => parsed.push((guard.to_string(), text.to_string())),
            _ => {
                return Err(TraceError(format!(
                    "{path}: bad check {}: {}",
                    guard.repr(),
                    expect.repr()
                )));
            }
        }
    }
    for (index, line) in lines.iter().enumerate() {
        check_line(p, &format!("{path}:{}", index + 2), index, line)?;
    }
    Ok(Trace {
        spec: spec.to_string(),
        checks: parsed,
        lines,
    })
}

/// One normalized step: op, state, worker, attempt, strikes, terminal,
/// actor, actor attempt and the `by` worker.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Step {
    op: String,
    state: String,
    worker: String,
    attempt: i128,
    strikes: i128,
    terminal: i128,
    actor: String,
    actor_attempt: i128,
    by: String,
}

/// Renames the workers `w1`, `w2`, ... in order of first use.
///
/// The specs are symmetric in their workers, so the rename keeps the
/// verdict. Equal traces then share one TLC run.
fn normalize(lines: &[Json]) -> (Vec<Step>, Vec<String>) {
    let mut names: Vec<(PyStr, String)> = Vec::new();
    let mut name = |worker: &Json| -> String {
        let Json::Str(worker) = worker else {
            return String::new();
        };
        if let Some((_, short)) = names.iter().find(|(long, _)| long == worker) {
            return short.clone();
        }
        let short = format!("w{}", names.len() + 1);
        names.push((worker.clone(), short.clone()));
        short
    };
    let count = |value: &Json| match value {
        Json::Int(n) => *n,
        _ => 0,
    };
    let mut steps = Vec::new();
    for line in lines {
        let actor = line.field("actor");
        let worker = name(line.field("worker"));
        let (actor_name, actor_attempt) = if matches!(actor, Json::Dict(_)) {
            (name(actor.field("worker")), count(actor.field("attempt")))
        } else {
            (String::new(), 0)
        };
        steps.push(Step {
            op: line.field("op").as_str().unwrap_or_default().to_string(),
            state: line.field("state").as_str().unwrap_or_default().to_string(),
            worker,
            attempt: count(line.field("attempt")),
            strikes: count(line.field("strikes")),
            terminal: count(line.field("terminal")),
            actor: actor_name,
            actor_attempt,
            by: name(line.field("by")),
        });
    }
    let mut workers: Vec<String> = names.into_iter().map(|(_, short)| short).collect();
    workers.sort();
    (steps, workers)
}

/// The TLA+ record of one step.
fn tla_record(step: &Step) -> String {
    format!(
        "[op |-> \"{}\", state |-> \"{}\", worker |-> \"{}\", \
         attempt |-> {}, strikes |-> {}, terminal |-> {}, \
         actor |-> \"{}\", actorAttempt |-> {}, by |-> \"{}\"]",
        step.op,
        step.state,
        step.worker,
        step.attempt,
        step.strikes,
        step.terminal,
        step.actor,
        step.actor_attempt,
        step.by
    )
}

/// The text of `TraceRun.tla` for one check.
fn model_text(spec: &str, steps: &[Step], workers: &[String]) -> String {
    let records: Vec<String> = steps.iter().map(tla_record).collect();
    let worker_set: Vec<String> = workers.iter().map(|w| format!("\"{w}\"")).collect();
    format!(
        "---- MODULE TraceRun ----\n\
         EXTENDS {spec}Trace\n\
         TraceLog == <<\n    {}\n>>\n\
         TraceWorkers == {{{}}}\n\
         ====\n",
        records.join(",\n    "),
        worker_set.join(", ")
    )
}

/// The text of `TraceRun.cfg` for one check.
fn config_text(spec: &str, steps: &[Step], guard: &str) -> String {
    let max_claims = steps
        .iter()
        .map(|s| s.attempt)
        .chain(std::iter::once(
            i128::try_from(steps.len()).unwrap_or(i128::MAX),
        ))
        .max()
        .unwrap_or(0);
    let mut constants = vec![
        ("NoWorker".to_string(), "NoWorker".to_string()),
        ("MaxClaims".to_string(), max_claims.to_string()),
    ];
    if spec == "WorkflowTaskClaim" {
        let max_strikes = steps.iter().map(|s| s.strikes).max().unwrap_or(0);
        constants.push(("MaxStrikes".to_string(), max_strikes.to_string()));
    }
    for (key, value) in guard_constants(spec, guard) {
        constants.push(((*key).to_string(), (*value).to_string()));
    }
    let mut body = String::new();
    for (k, v) in &constants {
        let _ = writeln!(body, "    {k} = {v}");
    }
    format!(
        "CONSTANTS\n    Log <- TraceLog\n    Workers <- TraceWorkers\n{body}\
         INIT TraceInit\nNEXT TraceNext\nINVARIANT LogNotConsumed\nCHECK_DEADLOCK FALSE\n"
    )
}

/// The `.tla` files of `dir`, as `Path.glob("*.tla")` finds them.
fn tla_files(dir: &str) -> Vec<String> {
    entries(dir, ".tla")
}

/// The entries of `dir` whose name ends with `suffix`, sorted by name.
///
/// A missing directory has no entries, as for `Path.glob`.
fn entries(dir: &str, suffix: &str) -> Vec<String> {
    let Ok(read) = std::fs::read_dir(if dir.is_empty() { "." } else { dir }) else {
        return Vec::new();
    };
    let mut names: Vec<String> = read
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.ends_with(suffix))
        .collect();
    names.sort();
    names.into_iter().map(|n| join(dir, &n)).collect()
}

/// Writes `TraceRun.tla` and `TraceRun.cfg` for one check into `run_dir`.
fn write_model(
    run_dir: &str,
    tla_dir: &str,
    spec: &str,
    steps: &[Step],
    workers: &[String],
    guard: &str,
) -> Result<()> {
    if Path::new(run_dir).exists() {
        bail!("[Errno 17] File exists: {}", str_repr(run_dir));
    }
    std::fs::create_dir_all(run_dir).with_context(|| format!("creating {run_dir}"))?;
    let sources = tla_files(tla_dir)
        .into_iter()
        .chain(tla_files(&join(tla_dir, "trace")));
    for source in sources {
        let name = Path::new(&source)
            .file_name()
            .context("a source has a file name")?;
        std::fs::copy(&source, Path::new(run_dir).join(name))
            .with_context(|| format!("copying {source}"))?;
    }
    fs::write_string(
        join(run_dir, "TraceRun.tla"),
        model_text(spec, steps, workers),
    )?;
    fs::write_string(
        join(run_dir, "TraceRun.cfg"),
        config_text(spec, steps, guard),
    )?;
    Ok(())
}

/// The outcome of one TLC run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Accept,
    /// No behavior matches. The depth names the first unmatched line.
    Reject(u64),
    /// TLC failed for another reason. The text holds its log.
    Error(String),
}

/// The outcome of a TLC run from its exit code and its log.
fn classify(p: &Patterns, code: Option<i32>, log: &str) -> Outcome {
    if code == Some(12) && log.contains("Invariant LogNotConsumed is violated") {
        return Outcome::Accept;
    }
    if code == Some(0) && log.contains("No error has been found") {
        // No initial state means that the init line does not match Init.
        let states = p
            .states
            .captures(log)
            .and_then(|c| c[1].parse::<u64>().ok());
        if states == Some(0) {
            return Outcome::Reject(0);
        }
        if let Some(depth) = p.depth.captures(log).and_then(|c| c[1].parse().ok()) {
            return Outcome::Reject(depth);
        }
    }
    Outcome::Error(log.to_string())
}

/// The argv of the TLC run in `run_dir`.
fn tlc_argv(jar: &Path, run_dir: &str) -> Vec<String> {
    vec![
        "java".to_string(),
        "-XX:+UseParallelGC".to_string(),
        format!("-Djava.io.tmpdir={}", join(run_dir, "tmp")),
        "-cp".to_string(),
        jar.display().to_string(),
        "tlc2.TLC".to_string(),
        "-config".to_string(),
        "TraceRun.cfg".to_string(),
        "-metadir".to_string(),
        join(run_dir, "meta"),
        "-workers".to_string(),
        "1".to_string(),
        "-cleanup".to_string(),
        "TraceRun.tla".to_string(),
    ]
}

/// Runs TLC in `run_dir`.
fn run_tlc(p: &Patterns, jar: &Path, run_dir: &str) -> Result<Outcome> {
    // Each JVM gets its own temp dir. Parallel TLC runs that share one
    // sometimes fail to parse a module.
    std::fs::create_dir(join(run_dir, "tmp")).with_context(|| format!("creating {run_dir}/tmp"))?;
    let argv = tlc_argv(jar, run_dir);
    // smop runs a command with no time bound, so this uses std directly.
    let child = Process::new(&argv[0])
        .args(&argv[1..])
        .current_dir(run_dir)
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        Err(error) => {
            let reason = error.raw_os_error().map_or_else(
                || crate::check::formal_models::strerror(&error),
                |code| {
                    format!(
                        "[Errno {code}] {}: {}",
                        crate::check::formal_models::strerror(&error),
                        str_repr(&argv[0])
                    )
                },
            );
            return Ok(Outcome::Error(format!("TLC did not run: {reason}")));
        }
    };
    let stdout = reader(child.stdout.take());
    let stderr = reader(child.stderr.take());
    let deadline = Instant::now() + Duration::from_secs(TLC_TIMEOUT_SECS);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let out = join_reader(stdout);
    let err = join_reader(stderr);
    let Some(status) = status else {
        let words: Vec<String> = argv.iter().map(|a| str_repr(a)).collect();
        return Ok(Outcome::Error(format!(
            "TLC did not run: Command '[{}]' timed out after {TLC_TIMEOUT_SECS} seconds",
            words.join(", ")
        )));
    };
    let code = status
        .code()
        .or_else(|| signal_of(status).map(|signal| -signal));
    let log = format!("{}{}", python_text(&out)?, python_text(&err)?);
    Ok(classify(p, code, &log))
}

/// The text of a pipe, as `subprocess.run(..., text=True)` reads it.
///
/// Python decodes the bytes as UTF-8 and fails on a bad byte. It then turns
/// each `\r\n` and each lone `\r` into `\n`.
fn python_text(bytes: &[u8]) -> Result<String> {
    let text = std::str::from_utf8(bytes).context("decoding the TLC output as UTF-8")?;
    Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// The signal that ended a process, as Python reports it.
#[cfg_attr(
    not(unix),
    allow(
        clippy::missing_const_for_fn,
        reason = "only the Unix body makes a call that is not const"
    )
)]
fn signal_of(status: std::process::ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}

/// Reads one pipe to its end on a thread.
fn reader<R: std::io::Read + Send + 'static>(pipe: Option<R>) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        if let Some(mut pipe) = pipe {
            let _ = pipe.read_to_end(&mut bytes);
        }
        bytes
    })
}

/// The bytes that a [`reader`] thread read.
fn join_reader(handle: std::thread::JoinHandle<Vec<u8>>) -> Vec<u8> {
    handle.join().unwrap_or_default()
}

/// The file line that TLC could not match, from the search depth.
///
/// `TraceInit` matches line 2 at depth 1. Depth d leaves cursor = d + 1, so
/// line d + 2 is the first line that no behavior matches.
const fn reject_line(depth: u64) -> u64 {
    depth + 2
}

/// True when a verdict such as `reject@7` meets an expectation.
fn meets(expect: &str, verdict: &str) -> bool {
    if expect == "reject" {
        return verdict.starts_with("reject");
    }
    verdict == expect
}

/// Names the first line that no behavior of the spec matches.
fn describe_reject(depth: u64, steps: &[Step]) -> String {
    if depth == 0 {
        return "the init line is not an initial state of the spec".to_string();
    }
    // TraceInit matches Log[1] at depth 1. Depth d leaves cursor = d + 1.
    let index = usize::try_from(depth + 1).unwrap_or(usize::MAX);
    if index > steps.len() {
        return "every line matched, but TLC did not report the end".to_string();
    }
    format!(
        "line {} is not a step of the spec: {}",
        index + 1,
        tla_record(&steps[index - 1])
    )
}

/// The key of one TLC run: the spec, the normalized steps and the guard.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    spec: String,
    steps: Vec<Step>,
    guard: String,
}

/// One TLC run and the files that share it.
struct Group {
    key: Key,
    workers: Vec<String>,
    /// Each file path and its expectation.
    uses: Vec<(String, String)>,
}

/// Groups the trace files by normalized content, in order of first use.
fn collect(p: &Patterns, dirs: &[String]) -> Result<Result<Vec<Group>, TraceError>> {
    let mut groups: Vec<Group> = Vec::new();
    let mut index: HashMap<Key, usize> = HashMap::new();
    for directory in dirs {
        let files = entries(&py_path(directory), ".ndjson");
        if files.is_empty() {
            return Ok(Err(TraceError(format!("{directory}: no traces"))));
        }
        let mut seen = Vec::new();
        for path in files {
            let bytes = std::fs::read(&path).with_context(|| format!("reading {path}"))?;
            let text = String::from_utf8(bytes).with_context(|| format!("decoding {path}"))?;
            let trace = match parse(p, &path, &text) {
                Ok(trace) => trace,
                Err(error) => return Ok(Err(error)),
            };
            seen.push(trace.spec.clone());
            let (steps, workers) = normalize(&trace.lines);
            for (guard, expect) in &trace.checks {
                let key = Key {
                    spec: trace.spec.clone(),
                    steps: steps.clone(),
                    guard: guard.clone(),
                };
                let at = *index.entry(key.clone()).or_insert_with(|| {
                    groups.push(Group {
                        key,
                        workers: workers.clone(),
                        uses: Vec::new(),
                    });
                    groups.len() - 1
                });
                groups[at].uses.push((path.clone(), expect.clone()));
            }
        }
        let missing: Vec<&str> = SPECS
            .iter()
            .copied()
            .filter(|s| !seen.iter().any(|t| t == s))
            .collect();
        if !missing.is_empty() {
            return Ok(Err(TraceError(format!(
                "{directory}: no trace for {}",
                missing.join(", ")
            ))));
        }
    }
    Ok(Ok(groups))
}

/// Checks every trace in `dirs`, and returns the exit code of the gate.
///
/// The TLC runs use `work` as their scratch directory.
pub fn check(jar: &Path, tla: &Path, work: &Path, dirs: &[String]) -> Result<ExitCode> {
    // TLC runs in a temp dir, so a relative jar path must be made absolute.
    let jar = resolve(jar)?;
    let jar = jar.as_path();
    let p = Patterns::new()?;
    let tla = py_path(&tla.to_string_lossy());
    let work = py_path(&work.to_string_lossy());
    let groups = match collect(&p, dirs)? {
        Ok(groups) => groups,
        Err(error) => {
            eprintln!("error: {}", error.0);
            return Ok(ExitCode::FAILURE);
        }
    };

    let results = run_all(&p, jar, &tla, &work, &groups)?;

    let mut failures = 0;
    let mut files = 0;
    for (group, outcome) in groups.iter().zip(results) {
        let (verdict, note, detail) = match outcome {
            Outcome::Accept => ("accept".to_string(), String::new(), None),
            Outcome::Reject(depth) => (
                format!("reject@{}", reject_line(depth)),
                format!(" ({})", describe_reject(depth, &group.key.steps)),
                None,
            ),
            Outcome::Error(log) => ("error".to_string(), String::new(), Some(log)),
        };
        for (path, expect) in &group.uses {
            files += 1;
            let ok = meets(expect, &verdict);
            let mark = if ok { "" } else { "  <-- FAIL" };
            println!(
                "{path} [{}, {}]: want {expect}, got {verdict}{note}{mark}",
                group.key.spec, group.key.guard
            );
            if !ok {
                failures += 1;
                if let Some(log) = &detail {
                    println!("{log}");
                }
            }
        }
    }
    println!("{} TLC runs for {files} checks", groups.len());
    if failures != 0 {
        println!("{failures} of {files} trace checks failed");
        return Ok(ExitCode::FAILURE);
    }
    println!("all {files} trace checks passed");
    Ok(ExitCode::SUCCESS)
}

/// Runs TLC for each group on a pool of threads, and keeps the order.
fn run_all(
    p: &Patterns,
    jar: &Path,
    tla: &str,
    work: &str,
    groups: &[Group],
) -> Result<Vec<Outcome>> {
    let threads = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
    let next = AtomicUsize::new(0);
    let results: Vec<Mutex<Option<Result<Outcome>>>> =
        groups.iter().map(|_| Mutex::new(None)).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads.min(groups.len()) {
            scope.spawn(|| {
                loop {
                    let number = next.fetch_add(1, Ordering::SeqCst);
                    let Some(group) = groups.get(number) else {
                        break;
                    };
                    let run_dir = join(work, &format!("{number:05}"));
                    let outcome = write_model(
                        &run_dir,
                        tla,
                        &group.key.spec,
                        &group.key.steps,
                        &group.workers,
                        &group.key.guard,
                    )
                    .and_then(|()| run_tlc(p, jar, &run_dir));
                    let _ = std::fs::remove_dir_all(&run_dir);
                    if let Ok(mut slot) = results[number].lock() {
                        *slot = Some(outcome);
                    }
                }
            });
        }
    });
    results
        .into_iter()
        .map(|slot| {
            slot.into_inner()
                .map_err(|_| anyhow!("a TLC thread panicked"))?
                .context("a TLC run has no result")?
        })
        .collect()
}

/// The string of a path as Python's `pathlib` prints it.
///
/// `pathlib` drops empty and `.` parts. An empty path prints as `.`.
/// POSIX gives a path that starts with exactly two slashes its own meaning,
/// so `pathlib` keeps both of them.
fn py_path(text: &str) -> String {
    let root = if text.starts_with("//") && !text.starts_with("///") {
        "//"
    } else if text.starts_with('/') {
        "/"
    } else {
        ""
    };
    let parts: Vec<&str> = text
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    let body = parts.join("/");
    match (root, body.is_empty()) {
        ("", true) => ".".to_string(),
        _ => format!("{root}{body}"),
    }
}

/// Joins `name` onto the `pathlib` path `base`.
fn join(base: &str, name: &str) -> String {
    match base {
        "." | "" => name.to_string(),
        "/" | "//" => format!("{base}{name}"),
        _ => format!("{base}/{name}"),
    }
}

/// Makes `jar` absolute, as Python's `Path.resolve` does.
///
/// A path that does not exist is made absolute without symlink lookups.
fn resolve(jar: &Path) -> Result<PathBuf> {
    if let Ok(real) = std::fs::canonicalize(jar) {
        return Ok(real);
    }
    let full = std::env::current_dir()?.join(jar);
    let mut out = PathBuf::new();
    for part in full.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns() -> Patterns {
        Patterns::new().expect("patterns compile")
    }

    const HEADER: &str = r#"{"spec":"ActivityClaim","checks":{"fixed":"accept"}}"#;
    const INIT: &str = r#"{"op":"init","state":"PENDING","worker":null,"attempt":0,"strikes":0,"terminal":0,"actor":null}"#;

    fn trace(lines: &[&str]) -> String {
        let mut text = lines.join("\n");
        text.push('\n');
        text
    }

    fn error_of(text: &str) -> String {
        match parse(&patterns(), "t.ndjson", text) {
            Ok(_) => panic!("want an error for {text}"),
            Err(error) => error.0,
        }
    }

    #[test]
    fn a_good_trace_parses() {
        let parsed = parse(&patterns(), "t", &trace(&[HEADER, INIT])).expect("parses");
        assert_eq!(parsed.spec, "ActivityClaim");
        assert_eq!(parsed.checks, [("fixed".to_string(), "accept".to_string())]);
        assert_eq!(parsed.lines.len(), 1);
    }

    #[test]
    fn malformed_traces_name_the_defect_as_python_does() {
        assert_eq!(
            error_of(HEADER),
            "t.ndjson: want a header and at least one line"
        );
        assert_eq!(
            error_of(&format!("{HEADER}\n\n{INIT}\n")),
            "t.ndjson:2: blank line"
        );
        assert_eq!(
            error_of(&trace(&["[1]", INIT])),
            "t.ndjson:1: the header must be a JSON object"
        );
        assert_eq!(
            error_of(&trace(&[&HEADER.replace("ActivityClaim", "X"), INIT])),
            "t.ndjson: unknown spec 'X'"
        );
        assert_eq!(
            error_of(&trace(&[r#"{"checks":{"fixed":"accept"}}"#, INIT])),
            "t.ndjson: unknown spec None"
        );
        assert_eq!(
            error_of(&trace(&[r#"{"spec":"ActivityClaim","checks":{}}"#, INIT])),
            "t.ndjson: the header has no checks"
        );
        assert_eq!(
            error_of(&trace(&[&HEADER.replace("accept", "reject@0"), INIT])),
            "t.ndjson: bad check 'fixed': 'reject@0'"
        );
        assert_eq!(
            error_of(&trace(&[&HEADER.replace("\"accept\"", "7"), INIT])),
            "t.ndjson: bad check 'fixed': 7"
        );
        assert_eq!(
            error_of(&trace(&[&HEADER.replace("fixed", "loose"), INIT])),
            "t.ndjson: bad check 'loose': 'accept'"
        );
        assert_eq!(
            error_of(&trace(&[HEADER, &INIT.replace("init", "write")])),
            "t.ndjson:2: bad op 'write'"
        );
        assert_eq!(
            error_of(&trace(&[HEADER, INIT, INIT])),
            "t.ndjson:3: bad op 'init'"
        );
        assert_eq!(
            error_of(&trace(&[HEADER, &INIT.replace("PENDING", "pending")])),
            "t.ndjson:2: bad state 'pending'"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"attempt\":0", "\"attempt\":true")
            ])),
            "t.ndjson:2: bad attempt True"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"strikes\":0", "\"strikes\":-1")
            ])),
            "t.ndjson:2: bad strikes -1"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"terminal\":0", "\"terminal\":2147483648")
            ])),
            "t.ndjson:2: bad terminal 2147483648"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"terminal\":0", "\"terminal\":1.0")
            ])),
            "t.ndjson:2: bad terminal 1.0"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"worker\":null", "\"worker\":5")
            ])),
            "t.ndjson:2: bad worker 5"
        );
        assert_eq!(
            error_of(&trace(&[
                HEADER,
                &INIT.replace("\"actor\":null", r#""actor":{"worker":"w","attempt":0}"#)
            ])),
            "t.ndjson:2: bad actor {'worker': 'w', 'attempt': 0}"
        );
        assert_eq!(
            error_of(&trace(&[HEADER, "[]"])),
            "t.ndjson:2: a line must be a JSON object"
        );
    }

    #[test]
    fn python_regex_dollar_accepts_a_final_newline() {
        let p = patterns();
        assert!(p.expect.is_match("accept\n"));
        assert!(p.expect.is_match("reject@12"));
        assert!(!p.expect.is_match("reject@012"));
        assert!(!p.expect.is_match("accept\n\n"));
        assert!(p.state.is_match("RUNNING\n"));
        assert!(!p.state.is_match("RUN NING"));
    }

    #[test]
    fn repr_matches_python() {
        let value = py_json::loads(r#"{"b":[1,-2.5,null,true],"a":"it's","b":{"x":"q\"'"}}"#)
            .expect("parses");
        assert_eq!(value.repr(), r#"{'b': {'x': 'q"\''}, 'a': "it's"}"#);
        assert_eq!(str_repr("tab\there\u{1}\u{a0}é"), r"'tab\there\x01\xa0é'");
    }

    #[test]
    fn json_numbers_keep_their_python_type() {
        let attempt = |value: &str| {
            let line = INIT.replace("\"attempt\":0", &format!("\"attempt\":{value}"));
            parse(&patterns(), "t", &trace(&[HEADER, &line])).map(|_| ())
        };
        // Python reads `-0` as the int 0, which is a count.
        assert!(attempt("-0").is_ok());
        let error = |value: &str| attempt(value).err().map(|e| e.0);
        assert_eq!(error("-0.0").as_deref(), Some("t:2: bad attempt -0.0"));
        assert_eq!(error("NaN").as_deref(), Some("t:2: bad attempt nan"));
        assert_eq!(
            error("123456789012345678901234567890123456789012").as_deref(),
            Some("t:2: bad attempt 123456789012345678901234567890123456789012")
        );
    }

    #[test]
    fn python_json_quirks_keep_a_trace_valid() {
        // Python accepts each of these values in a field that the check
        // does not read, so the trace stays valid.
        for value in ["NaN", "-Infinity", "\"\\ud800\"", "1e400"] {
            let header = HEADER.replace("\"checks\"", &format!("\"case\":{value},\"checks\""));
            assert!(
                parse(&patterns(), "t", &trace(&[&header, INIT])).is_ok(),
                "{value}"
            );
        }
        let lone = INIT.replace("\"worker\":null", "\"worker\":\"\\udc00\"");
        assert!(parse(&patterns(), "t", &trace(&[HEADER, &lone])).is_ok());
        let bad = INIT.replace("PENDING", "\\ud800");
        assert_eq!(
            parse(&patterns(), "t", &trace(&[HEADER, &bad]))
                .err()
                .map(|e| e.0),
            Some("t:2: bad state '\\ud800'".to_string())
        );
        assert_eq!(
            parse(&patterns(), "t", &trace(&[HEADER, "{\"op\":\"init\",}"]))
                .err()
                .map(|e| e.0),
            Some(
                "t:2: Expecting property name enclosed in double quotes: line 1 column 14 (char 13)"
                    .to_string()
            )
        );
    }

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(
            split_lines("a\nb\r\nc\rd\u{2028}e"),
            ["a", "b", "c", "d", "e"]
        );
        assert_eq!(split_lines("a\n"), ["a"]);
        assert_eq!(split_lines("a\n\n"), ["a", ""]);
        assert_eq!(split_lines(""), Vec::<&str>::new());
    }

    #[test]
    fn normalize_renames_workers_in_order_of_first_use() {
        let lines: Vec<Json> = [
            INIT.to_string(),
            r#"{"op":"write","state":"RUNNING","worker":"zed","attempt":1,"strikes":0,"terminal":0,"actor":{"worker":"amy","attempt":1},"by":"zed"}"#.to_string(),
            r#"{"op":"write","state":"RUNNING","worker":"bo","attempt":2,"strikes":0,"terminal":0,"actor":null,"by":"amy"}"#.to_string(),
        ]
        .iter()
        .map(|l| py_json::loads(l).expect("parses"))
        .collect();
        let (steps, workers) = normalize(&lines);
        assert_eq!(workers, ["w1", "w2", "w3"]);
        assert_eq!(steps[0].worker, "");
        assert_eq!(steps[1].worker, "w1");
        assert_eq!(steps[1].actor, "w2");
        assert_eq!(steps[1].actor_attempt, 1);
        assert_eq!(steps[1].by, "w1");
        assert_eq!(steps[2].worker, "w3");
        assert_eq!(steps[2].by, "w2");
        assert_eq!(
            tla_record(&steps[1]),
            "[op |-> \"write\", state |-> \"RUNNING\", worker |-> \"w1\", attempt |-> 1, \
             strikes |-> 0, terminal |-> 0, actor |-> \"w2\", actorAttempt |-> 1, by |-> \"w1\"]"
        );
    }

    #[test]
    fn the_model_and_config_match_the_python_text() {
        let lines: Vec<Json> = [
            INIT,
            &INIT
                .replace("init", "write")
                .replace("\"strikes\":0", "\"strikes\":3"),
        ]
        .iter()
        .map(|l| py_json::loads(l).expect("parses"))
        .collect();
        let (steps, workers) = normalize(&lines);
        let model = model_text("WorkflowTaskClaim", &steps, &workers);
        assert!(model.starts_with("---- MODULE TraceRun ----\nEXTENDS WorkflowTaskClaimTrace\nTraceLog == <<\n    [op |-> \"init\""));
        assert!(model.ends_with(">>\nTraceWorkers == {}\n====\n"));
        assert_eq!(
            config_text("WorkflowTaskClaim", &steps, "pre-fix"),
            "CONSTANTS\n    Log <- TraceLog\n    Workers <- TraceWorkers\n    NoWorker = NoWorker\n    \
             MaxClaims = 2\n    MaxStrikes = 3\n    ChecksAttempt = FALSE\n    \
             CapMissGuard = \"strikes\"\nINIT TraceInit\nNEXT TraceNext\nINVARIANT LogNotConsumed\n\
             CHECK_DEADLOCK FALSE\n"
        );
        assert_eq!(
            config_text("ActivityClaim", &steps, "fixed"),
            "CONSTANTS\n    Log <- TraceLog\n    Workers <- TraceWorkers\n    NoWorker = NoWorker\n    \
             MaxClaims = 2\n    Fenced = TRUE\nINIT TraceInit\nNEXT TraceNext\nINVARIANT LogNotConsumed\n\
             CHECK_DEADLOCK FALSE\n"
        );
    }

    #[test]
    fn tlc_logs_classify_as_the_python_does() {
        let p = patterns();
        assert_eq!(
            classify(&p, Some(12), "Error: Invariant LogNotConsumed is violated."),
            Outcome::Accept
        );
        assert_eq!(
            classify(
                &p,
                Some(0),
                "No error has been found.\n0 distinct states found"
            ),
            Outcome::Reject(0)
        );
        assert_eq!(
            classify(
                &p,
                Some(0),
                "No error has been found.\n9 distinct states found\n\
                 The depth of the complete state graph search is 5."
            ),
            Outcome::Reject(5)
        );
        assert!(matches!(
            classify(
                &p,
                Some(0),
                "No error has been found.\n1 distinct state found"
            ),
            Outcome::Error(_)
        ));
        assert!(matches!(classify(&p, Some(1), "boom"), Outcome::Error(_)));
        assert!(matches!(
            classify(&p, Some(12), "Invariant Other is violated"),
            Outcome::Error(_)
        ));
    }

    #[test]
    fn verdict_helpers_match_the_python() {
        assert_eq!(reject_line(0), 2);
        assert_eq!(reject_line(5), 7);
        assert!(meets("reject", "reject@4"));
        assert!(meets("reject@4", "reject@4"));
        assert!(!meets("reject@5", "reject@4"));
        assert!(!meets("accept", "error"));
        let lines: Vec<Json> = [INIT, &INIT.replace("init", "write")]
            .iter()
            .map(|l| py_json::loads(l).expect("parses"))
            .collect();
        let (steps, _) = normalize(&lines);
        assert_eq!(
            describe_reject(0, &steps),
            "the init line is not an initial state of the spec"
        );
        assert!(
            describe_reject(1, &steps)
                .starts_with("line 3 is not a step of the spec: [op |-> \"write\"")
        );
        assert_eq!(
            describe_reject(2, &steps),
            "every line matched, but TLC did not report the end"
        );
    }

    #[test]
    fn paths_print_as_pathlib_does() {
        assert_eq!(py_path("./a//b/./"), "a/b");
        assert_eq!(py_path(""), ".");
        assert_eq!(py_path("/x/../y"), "/x/../y");
        assert_eq!(py_path("//x/./y/"), "//x/y");
        assert_eq!(py_path("///x"), "/x");
        assert_eq!(join("//", "f"), "//f");
        assert_eq!(join(".", "f"), "f");
        assert_eq!(join("a", "f"), "a/f");
    }

    #[test]
    fn the_repo_fixtures_group_into_runs() {
        let dir = crate::util::repo_path("formal/tla/trace/fixtures");
        let dirs = [dir.display().to_string()];
        let groups = collect(&patterns(), &dirs)
            .expect("reads")
            .unwrap_or_else(|e| panic!("{}", e.0));
        let uses: usize = groups.iter().map(|g| g.uses.len()).sum();
        assert!(groups.len() <= uses);
        assert!(groups.iter().any(|g| g.key.spec == "ActivityClaim"));
        assert!(groups.iter().any(|g| g.key.spec == "WorkflowTaskClaim"));
    }

    #[test]
    fn a_directory_needs_a_trace_for_each_spec() {
        let dir = fs::temp_dir().expect("temp dir");
        let dirs = [dir.path().display().to_string()];
        let empty = collect(&patterns(), &dirs).expect("reads");
        assert!(matches!(empty, Err(TraceError(ref m)) if m.ends_with(": no traces")));
        std::fs::write(dir.path().join("a.ndjson"), trace(&[HEADER, INIT])).expect("write");
        let one = collect(&patterns(), &dirs).expect("reads");
        assert!(
            matches!(one, Err(TraceError(ref m)) if m.ends_with(": no trace for WorkflowTaskClaim"))
        );
    }

    #[test]
    fn tlc_output_reads_as_python_text() {
        assert_eq!(python_text(b"a\r\nb\rc\n").expect("utf-8"), "a\nb\nc\n");
        assert!(python_text(b"\xff").is_err());
    }

    #[test]
    fn tlc_argv_names_the_run_dir() {
        let dir = fs::temp_dir().expect("temp dir");
        let run_dir = dir.path().join("00000").display().to_string();
        std::fs::create_dir(&run_dir).expect("mkdir");
        let argv = tlc_argv(Path::new("/x.jar"), &run_dir);
        assert_eq!(argv[2], format!("-Djava.io.tmpdir={run_dir}/tmp"));
        assert_eq!(argv[9], format!("{run_dir}/meta"));
    }
}
