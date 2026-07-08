//! Deterministic workflow guardrails.
//!
//! Statically analyses Rust source files for common non-determinism patterns that
//! break Harvest's deterministic replay contract inside `#[workflow]` functions.
//!
//! # Rule catalog
//!
//! | ID     | Severity | Pattern family                                 |
//! |--------|----------|------------------------------------------------|
//! | DET001 | Error    | Wall-clock time reads                          |
//! | DET002 | Error    | Random number generation                       |
//! | DET003 | Error    | Ad-hoc UUID generation                         |
//! | DET004 | Error    | Environment variable / argument reads          |
//! | DET005 | Warning  | Process-global state reads                     |
//! | DET006 | Error    | Direct sleep / timer primitives                |
//! | DET007 | Error    | Background task spawning                       |
//! | DET008 | Error    | Direct network / filesystem I/O                |
//! | DET009 | Warning  | Bare tracing calls (log amplification)         |
//! | DET010 | Error*   | `HashMap`/`HashSet` iteration order (issue #785) |
//! | DET011 | Error    | `select!` / futures select combinators (issue #799) |
//!
//! DET011 is the det_check twin of guardrail HVG010 (SelectMacro, issue #600):
//! `tokio::select!` / `futures::select!` / `select_biased!` and the
//! `futures::future::{select, select_all, select_ok, try_select}` combinators
//! (in both the fully-qualified `futures::future::…` and the short `future::…`
//! forms) race ctx-managed awaitables non-deterministically (the winning branch
//! differs between the live run and a replay). HVG010 is the compile-time /
//! catalog id; det_check surfaces the same hazard as DET011 (DET010 was the
//! prior det_check maximum).
//!
//! \* DET010 is command-aware: a flagged loop whose body schedules commands
//! (`.execute_activity*`, `.spawn_child_workflow*`, `.execute_local_activity*`,
//! `.timer(`, `.side_effect(`) is an Error; a command-free loop is a Warning.
//! DET010 is the det_check twin of guardrail HVG011 (the issue proposed
//! HVG010/DET010, but HVG010 was permanently assigned to SelectMacro/#600).
//!
//! # Suppression
//!
//! Place a `// harvest-suppress: RULE_ID "reason"` comment on the line
//! **immediately preceding** the flagged expression (or on the same line).
//! The reason string is required and must be non-empty.
//!
//! ```rust,ignore
//! // harvest-suppress: DET001 "timestamp comes from the signal payload"
//! let recorded_at = std::time::SystemTime::now();
//! ```
//!
//! Suppressions are always reported in [`DetCheckReport::suppressions`] so they
//! remain auditable in machine-readable output.

use std::path::Path;

// ── Public types ─────────────────────────────────────────────────────────────

/// Severity of a determinism finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetSeverity {
    /// Breaks replay determinism; counted as a hard blocker by [`DetCheckReport::has_hard_blockers`].
    Error,
    /// May break determinism in edge cases; reported but does not fail CI by default.
    Warning,
}

/// Source location reported in a finding.
#[derive(Debug, Clone)]
pub struct DetLocation {
    /// Path (or label) of the file being analysed, as passed to [`check_source`].
    pub file: String,
    /// 1-indexed line number within that file.
    pub line: u32,
}

/// A single determinism violation found in a workflow function body.
#[derive(Debug, Clone)]
pub struct DetFinding {
    /// Stable rule identifier (e.g. `"DET001"`).
    pub rule_id: &'static str,
    /// How serious the violation is.
    pub severity: DetSeverity,
    /// Name of the `#[workflow]` function where the violation was found.
    pub workflow_name: Option<String>,
    /// Source location, when available.
    pub location: Option<DetLocation>,
    /// Human-readable description of the violation.
    pub message: String,
    /// A deterministic Harvest-shaped alternative.
    pub alternative: &'static str,
}

/// An active suppression comment found in the source.
#[derive(Debug, Clone)]
pub struct DetSuppression {
    /// The rule ID that was suppressed.
    pub rule_id: String,
    /// The required reason string from the comment.
    pub reason: String,
    /// Where the suppressed expression (not the comment) was located.
    pub location: DetLocation,
}

/// The result of checking one or more source files.
#[derive(Debug, Default)]
pub struct DetCheckReport {
    /// Violations that were *not* suppressed.
    pub findings: Vec<DetFinding>,
    /// Suppressions that were applied (always reported for auditability).
    pub suppressions: Vec<DetSuppression>,
}

impl DetCheckReport {
    /// Returns `true` if any unsuppressed [`DetSeverity::Error`] finding is present.
    /// Warnings alone do not constitute a hard blocker.
    #[must_use]
    pub fn has_hard_blockers(&self) -> bool {
        self.findings
            .iter()
            .any(|f| matches!(f.severity, DetSeverity::Error))
    }

    fn merge(&mut self, other: Self) {
        self.findings.extend(other.findings);
        self.suppressions.extend(other.suppressions);
    }
}

// ── Rule catalog ──────────────────────────────────────────────────────────────

struct Rule {
    id: &'static str,
    severity: DetSeverity,
    patterns: &'static [&'static str],
    message: &'static str,
    alternative: &'static str,
}

const RULES: &[Rule] = &[
    Rule {
        id: "DET001",
        severity: DetSeverity::Error,
        patterns: &[
            "SystemTime::now()",
            "Instant::now()",
            "Utc::now()",
            "Local::now()",
            "chrono::Utc::now",
            "chrono::Local::now",
            "OffsetDateTime::now_utc()",
        ],
        message: "Wall-clock time read inside a workflow function breaks replay determinism. \
                  Each replay may observe a different instant, causing non-deterministic branching.",
        alternative: "Use `ctx.timer(id, secs)` for durable waits. To capture an exact timestamp, \
                      record it inside an activity and return it to the workflow, or pass it as \
                      workflow input.",
    },
    Rule {
        id: "DET002",
        severity: DetSeverity::Error,
        patterns: &[
            "rand::random",
            "rand::thread_rng(",
            "thread_rng(",
            "SmallRng::from_entropy",
            "StdRng::from_entropy",
            "rand::rngs::OsRng",
        ],
        message: "Random number generation inside a workflow function is non-deterministic. \
                  The RNG state differs on each replay, producing different values.",
        alternative: "Generate random values inside an activity and return them to the workflow. \
                      Use `ctx.random_uuid(label)` for durable, replay-safe UUIDs.",
    },
    Rule {
        id: "DET003",
        severity: DetSeverity::Error,
        patterns: &[
            "Uuid::new_v4(",
            "Uuid::new_v7(",
            "Uuid::new_v1(",
            "Uuid::now_v7(",
            "uuid::Uuid::new_v4",
            "uuid::Uuid::new_v7",
            "uuid::Uuid::new_v1",
            "uuid::Uuid::now_v7",
        ],
        message: "Ad-hoc UUID generation inside a workflow function is non-deterministic. \
                  A new random UUID is produced on every replay, breaking event correlation.",
        alternative: "Use `ctx.random_uuid(label)` which records the UUID in the event history \
                      and replays the same value deterministically.",
    },
    Rule {
        id: "DET004",
        severity: DetSeverity::Error,
        patterns: &[
            "std::env::var(",
            "env::var(",
            "std::env::args(",
            "env::args(",
            "std::env::vars(",
            "env::vars(",
        ],
        message: "Environment variable / argument reads inside a workflow function are \
                  non-deterministic: values can differ across workers and across replays.",
        alternative: "Pass configuration as workflow input, inject it via `ctx.state::<T>()`, \
                      or read it inside an activity.",
    },
    Rule {
        id: "DET005",
        severity: DetSeverity::Warning,
        patterns: &["std::process::id(", "process::id("],
        message: "Process-global state read inside a workflow function may differ across \
                  replays running on different worker processes.",
        alternative: "Avoid reading process-global state from workflow code. Pass values as \
                      input or use `ctx.state::<T>()` for injected dependencies.",
    },
    Rule {
        id: "DET006",
        severity: DetSeverity::Error,
        patterns: &[
            "tokio::time::sleep(",
            "time::sleep(",
            "std::thread::sleep(",
            "thread::sleep(",
        ],
        message: "Direct sleep / timer primitive inside a workflow function is non-durable and \
                  breaks replay: the sleep re-executes on every replay rather than being \
                  skipped after the first completion.",
        alternative: "Use `ctx.timer(id, secs)` for durable, replay-safe waits that resume \
                      correctly after a worker crash or restart.",
    },
    Rule {
        id: "DET007",
        severity: DetSeverity::Error,
        patterns: &[
            "tokio::spawn(",
            "tokio::task::spawn(",
            "task::spawn(",
            "std::thread::spawn(",
            "thread::spawn(",
            "tokio::task::spawn_blocking(",
            "task::spawn_blocking(",
        ],
        message: "Background task spawning inside a workflow function is invisible to the replay \
                  engine. The spawned task's side effects cannot be replayed deterministically.",
        alternative: "Run concurrent work via parallel activity execution using `futures::join!` \
                      with multiple `ctx.execute_activity_raw` calls. Blocking I/O belongs inside \
                      an activity.",
    },
    Rule {
        id: "DET008",
        severity: DetSeverity::Error,
        patterns: &[
            "std::fs::",
            "tokio::fs::",
            "File::open(",
            "File::create(",
            "reqwest::",
            "hyper::",
            "std::net::TcpStream",
            "TcpStream::connect(",
            "UdpSocket::bind(",
        ],
        message: "Direct network / filesystem I/O inside a workflow function is non-deterministic. \
                  The response or file contents may change between the original execution and \
                  subsequent replays.",
        alternative: "Move all I/O into activities. Activities are the durable side-effect boundary \
                      in Harvest; their results are recorded in the event history and replayed \
                      without re-executing the I/O.",
    },
    Rule {
        id: "DET009",
        severity: DetSeverity::Warning,
        patterns: &[
            // Fully-qualified spellings
            "tracing::info!(",
            "tracing::warn!(",
            "tracing::error!(",
            "tracing::debug!(",
            "tracing::trace!(",
            "tracing::event!(",
            // Imported spellings — `use tracing::{info, warn, …}` then bare call
            "info!(",
            "warn!(",
            "error!(",
            "debug!(",
            "trace!(",
        ],
        message: "Bare tracing macro inside a workflow function fires once per replay cycle. \
                  A workflow that suspends N times will emit N copies of this log line, \
                  amplifying log volume and producing duplicate events without correlation keys.",
        alternative: "Use `ctx.logger().info(msg)`, `ctx.logger().warn(msg)`, \
                      `ctx.logger().error(msg)`, or the convenience shorthands \
                      `ctx.log_info(msg)` / `ctx.log_warn(msg)` / `ctx.log_error(msg)`. \
                      These are replay-aware: output is suppressed during replay cycles and \
                      each event is auto-tagged with workflow_id, execution_id, and workflow_type. \
                      See guardrail HVG009 in the catalog for the full rationale.",
    },
    Rule {
        id: "DET011",
        severity: DetSeverity::Error,
        patterns: &[
            // Select MACROS — the `!` makes these unambiguous macro
            // invocations (no ident/method call contains it). `select!` alone
            // matches `tokio::select!`, `futures::select!`, and a bare
            // `select!`; `select_biased!` needs its own pattern (it does not
            // contain the `select!` substring). Both are matched only at an
            // identifier boundary (see `DET011_MACRO_BOUNDARY_PATTERNS`): the
            // byte immediately preceding a match must not be an identifier
            // byte, so an unrelated macro whose name merely ENDS in these
            // tokens (`sql_select! {}`, `my_select!()`, `foo_select_biased!()`)
            // is not flagged. This keeps det_check in agreement with the
            // compile-time HVG010 guardrail, which matches macro paths exactly
            // and accepts those unrelated macros (#799 P2 review). A preceding
            // `::` (`tokio::select!` / `futures::select!`), brace, whitespace,
            // or line start is a boundary and still matches.
            "select!",
            "select_biased!",
            // Combinator FUNCTIONS. These candidate patterns cheaply locate a
            // possible combinator call; the actual decision is made by the
            // path-precise `matches_futures_combinator_pattern` (see
            // `DET011_COMBINATOR_FN_PATTERNS`), which extracts the FULL
            // qualified path ending at the call and matches it exactly against
            // the same allowed-path set as the compile-time HVG010 guardrail's
            // `is_select_combinator_path`. That precision is required so a
            // qualified call by an UNRELATED helper of the same tail name is
            // NOT flagged: `crate::future::select(`, `my_dsl::select_all(`,
            // `bar::future::select_ok(`, and `foo::try_select(` all resolve to
            // paths outside the allowed set and are rejected, exactly as the
            // macro lint rejects them. The allowed set is: the
            // `futures`-anchored qualified forms (`futures::future::{select,
            // select_all, select_ok, try_select}`), their `future::…` short
            // forms (the idiomatic `use futures::future;` spelling), and the
            // genuinely BARE distinctive names `select_all`/`select_ok`/
            // `try_select` (a bare free-fn call by any of these three is, in
            // practice, always the futures combinator via
            // `use futures::future::select_all;`). Bare `select(` is
            // deliberately NOT in the allowed set (`select` is a common
            // free-fn / query-builder name), and a preceding `.` (method call,
            // e.g. `q.select_all()`) is rejected — mirroring the AST macro
            // visitor's structural exclusion of method calls.
            "future::select(",
            "select_all(",
            "select_ok(",
            "try_select(",
        ],
        message: "select! / futures select combinator inside a workflow function races ctx-managed \
                  awaitables non-deterministically. tokio::select! polls its branches in a \
                  randomized order by design, so the branch that wins can differ between the first \
                  live run and a later replay, silently diverging the execution; the dropped loser \
                  branches also do not durably cancel the underlying activity or timer.",
        alternative: "Use ctx.race() (WorkflowContext), the deterministic race/select primitive \
                      (issue #600): it records the winning branch durably and cancels the losers. \
                      For a signal bounded by a deadline use ctx.receive_signal_timeout() / \
                      ctx.wait_for_signal_timeout(); to fan out many activities in parallel and \
                      collect their results in a deterministic order use ctx.execute_activity_fan_out*; \
                      to block until a predicate holds use ctx.await_condition_timeout(). Inside an \
                      #[activity] body, select! is fine — only the activity's recorded result matters.",
    },
];

// ── Public entry points ───────────────────────────────────────────────────────

/// Check Rust source code text for determinism violations inside `#[workflow]` functions.
///
/// `file` is used only for source-location reporting; no file is read.
/// The function always returns a report; it never panics on malformed input.
#[must_use]
pub fn check_source(source: &str, file: &str) -> DetCheckReport {
    let mut report = DetCheckReport::default();
    let lines: Vec<&str> = source.lines().collect();

    for (wf_name, body) in extract_workflow_bodies(&lines) {
        report.merge(check_body(&wf_name, &body, file));
    }

    report
}

/// Check a single `.rs` file for determinism violations.
///
/// # Errors
/// Returns an error if the file cannot be read.
pub fn check_file(path: &Path) -> std::io::Result<DetCheckReport> {
    let source = std::fs::read_to_string(path)?;
    let file = path.to_string_lossy();
    Ok(check_source(&source, &file))
}

/// Recursively check all `.rs` files under `dir` for determinism violations.
///
/// # Errors
/// Returns an error if the directory cannot be read or any file read fails.
pub fn check_dir(dir: &Path) -> std::io::Result<DetCheckReport> {
    let mut report = DetCheckReport::default();
    collect_rs_files(dir, &mut report)?;
    Ok(report)
}

fn collect_rs_files(dir: &Path, report: &mut DetCheckReport) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, report)?;
        } else if path.extension().is_some_and(|e| e == "rs") {
            report.merge(check_file(&path)?);
        }
    }
    Ok(())
}

// ── Workflow body extraction ───────────────────────────────────────────────────

/// Returns `(workflow_name, body_lines)` for every `#[workflow]`-annotated
/// `async fn` in the source.  Each body line carries its 1-indexed line number.
/// `#[activity]` bodies are not returned.
fn extract_workflow_bodies<'a>(lines: &[&'a str]) -> Vec<(String, Vec<(u32, &'a str)>)> {
    let mut result = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let trimmed = lines[i].trim();

        if !is_workflow_attr(trimmed) {
            i += 1;
            continue;
        }

        // Found `#[workflow]` — skip forward past any intermediate attributes / doc comments
        i += 1;
        while i < lines.len() {
            let t = lines[i].trim();
            if t.starts_with("//") || t.starts_with("#[") || t.is_empty() {
                i += 1;
            } else {
                break;
            }
        }

        if i >= lines.len() {
            break;
        }

        // Extract function name from `(pub)? async fn NAME`
        let fn_line = lines[i].trim();
        let Some(name) = extract_fn_name(fn_line) else {
            i += 1;
            continue;
        };

        // Scan forward to find the opening `{` of the function body.
        // The signature may span multiple lines.
        let mut j = i;
        let brace_line = loop {
            if lines[j].contains('{') {
                break j;
            }
            j += 1;
            if j >= lines.len() {
                break usize::MAX; // sentinel: not found
            }
        };
        if brace_line == usize::MAX {
            i += 1;
            continue;
        }

        // Start collecting from brace_line itself so that code on the same line
        // as `{` is included (e.g. single-line `fn wf() { expr }`).
        let brace_pos = lines[brace_line].find('{').unwrap_or(0);
        let mut depth = 1u32;
        let mut body: Vec<(u32, &'a str)> = Vec::new(); // (1-indexed line, text)

        j = brace_line;
        let mut on_brace_line = true;

        while j < lines.len() && depth > 0 {
            // On the opening-brace line only scan content after the `{`.
            let line: &'a str = if on_brace_line {
                on_brace_line = false;
                &lines[j][brace_pos + 1..]
            } else {
                lines[j]
            };
            let line_num = u32::try_from(j + 1).unwrap_or(u32::MAX);

            if let Some(pos) = scan_braces_outside_literals(line, &mut depth) {
                // Include any code on the closing-brace line that precedes the `}`
                let before = &line[..pos];
                if !before.trim().is_empty() {
                    body.push((line_num, before));
                }
            } else if !line.trim().is_empty() {
                body.push((line_num, line));
            }

            j += 1;
        }

        result.push((name, body));
        i = j;
    }

    result
}

/// Returns `true` for `#[workflow]` and `#[workflow(...)]` attribute lines.
fn is_workflow_attr(s: &str) -> bool {
    s == "#[workflow]" || s.starts_with("#[workflow(") || s.starts_with("#[workflow ]")
}

/// Extracts the function name from a line like `pub async fn my_wf(`.
fn extract_fn_name(line: &str) -> Option<String> {
    let pos = line.find("fn ")?;
    let after = &line[pos + 3..];
    let name: String = after
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() { None } else { Some(name) }
}

// ── Body checker ──────────────────────────────────────────────────────────────

fn check_body(wf_name: &str, body_lines: &[(u32, &str)], file: &str) -> DetCheckReport {
    let mut report = DetCheckReport::default();

    for (idx, &(source_line, line)) in body_lines.iter().enumerate() {
        // Code portion with string literals and line comments stripped to
        // prevent false positives from patterns appearing in string data or comments.
        let code_part = strip_unparseable_content(line);
        // Previous body line — checked for a preceding suppression comment.
        let prev_line = if idx > 0 { body_lines[idx - 1].1 } else { "" };

        'rules: for rule in RULES {
            for &pattern in rule.patterns {
                let matched = if DET011_COMBINATOR_FN_PATTERNS.contains(&pattern) {
                    matches_futures_combinator_pattern(&code_part, pattern)
                } else if DET011_MACRO_BOUNDARY_PATTERNS.contains(&pattern) {
                    matches_at_ident_boundary(&code_part, pattern)
                } else {
                    code_part.contains(pattern)
                };
                if !matched {
                    continue;
                }

                // Pattern matched — check for a suppression comment.
                if let Some(reason) = find_suppression(rule.id, line, prev_line) {
                    report.suppressions.push(DetSuppression {
                        rule_id: rule.id.to_string(),
                        reason,
                        location: DetLocation {
                            file: file.to_string(),
                            line: source_line,
                        },
                    });
                } else {
                    report.findings.push(DetFinding {
                        rule_id: rule.id,
                        severity: rule.severity.clone(),
                        workflow_name: Some(wf_name.to_string()),
                        location: Some(DetLocation {
                            file: file.to_string(),
                            line: source_line,
                        }),
                        message: format!(
                            "[{}] {} (matched pattern: `{}`)",
                            rule.id, rule.message, pattern
                        ),
                        alternative: rule.alternative,
                    });
                }
                continue 'rules; // one finding per rule per line
            }
        }
    }

    // NOTE: DET010 findings are appended AFTER the substring-table findings
    // above, so a report's findings are grouped by pass, not strictly sorted
    // by source line across rules. Consumers needing line order should sort
    // on `location.line`.
    report.merge(check_hash_iteration(wf_name, body_lines, file));

    report
}

// ── DET010: HashMap/HashSet iteration order (issue #785) ─────────────────────
//
// The substring-table `Rule` engine above cannot express this rule (it needs
// binding tracking plus loop-body extent), so DET010 is a bespoke pass over
// the same extracted workflow bodies. It shares `DetFinding`/`DetSeverity`/
// `DetSuppression`, `find_suppression`, and `strip_unparseable_content`, and
// fires from the same `check_source`/`check_file`/`check_dir` front doors.
//
// Rule-ID note: issue #785's text proposed "DET/HVG010", but HVG010 was
// already permanently assigned to SelectMacro (issue #600) and DET009 was the
// previous det_check maximum — this rule ships as DET010 here and HVG011 in
// the guardrail catalog / `#[workflow]` macro lint.

const DET010_ID: &str = "DET010";

/// Iteration methods on `HashMap`/`HashSet` whose order is hash-randomized.
/// Only a SINGLE argument-free call from this set on a tracked ident is
/// flagged — longer chains (`map.keys().sorted()`, `.collect::<Vec<_>>()`)
/// are never flagged, which is how "already-sorted iterators are never
/// flagged" holds.
const DET010_ITER_METHODS: &[&str] = &[
    "iter",
    "iter_mut",
    "keys",
    "values",
    "values_mut",
    "drain",
    "into_iter",
    "into_keys",
    "into_values",
];

/// Command markers for the severity decision: a loop body containing any of
/// these schedules history-ordered durable commands, so hash-order iteration
/// is an Error; a command-free loop is a Warning.
const DET010_COMMAND_MARKERS: &[&str] = &[
    ".execute_activity",
    ".spawn_child_workflow",
    ".execute_local_activity",
    ".timer(",
    ".side_effect(",
    // ctx.race() schedules durable commands per branch (issue #600).
    ".race(",
];

const DET010_MESSAGE: &str = "Iterating a HashMap/HashSet inside a workflow function observes hash-randomized \
     iteration order, which can differ between the original run and any replay on another \
     worker process. Commands scheduled inside the loop are recorded in history in iteration \
     order, so a reordered replay produces a different command sequence and diverges \
     (non-determinism error / nd-block).";

const DET010_ALTERNATIVE: &str = "Use a BTreeMap/BTreeSet for any collection the workflow iterates, or collect the keys \
     into a Vec and sort() it before iterating: `let mut keys: Vec<_> = \
     map.keys().cloned().collect(); keys.sort();`.";

/// The guardrail-catalog spelling of this rule. Honored as a suppression
/// alias (AC5, issue #785): `// harvest-suppress: HVG011 "reason"` suppresses
/// a DET010 finding exactly like the DET010 spelling, and is echoed into
/// [`DetCheckReport::suppressions`] with the id the author actually wrote.
const DET010_HVG_ALIAS: &str = "HVG011";

/// Bound on how many continuation lines a multi-line `let` statement is
/// joined across before the binding parse gives up (defaults to not-tracked).
const DET010_LET_JOIN_BUDGET: usize = 16;

/// A positional event on one stripped source line, processed in byte order so
/// braces, `let` bindings, and `for` loops interleave exactly as written.
enum Det010Event {
    Open,
    Close,
    Let,
    For,
}

/// Outcome of parsing one `let` statement for DET010 binding tracking.
enum Det010LetBinding {
    /// A single plain ident binding — tracked (`is_hash`) or masked (`!is_hash`).
    Simple { ident: String, is_hash: bool },
    /// A pattern binding (destructuring, enum variant, let-else): every
    /// plausibly-bound ident is masked (never tracked — unknown shapes
    /// default to NOT flagging, false positives are the top risk).
    PatternMask(Vec<String>),
}

/// Single ordered pass over a workflow body: track hash-typed `let` bindings
/// in a lexical scope stack (brace depth opens/closes scopes; a non-hash
/// binding MASKS an outer tracked binding rather than deleting it, so scope
/// exit restores the outer state — PR #970 review, P1-C), then flag
/// `for … in` loops over a tracked binding, with command-aware severity and
/// `harvest-suppress` support (DET010 or its HVG011 catalog alias).
///
/// LEXER CAVEAT (shared with DET001–DET009, pre-existing): the line-based
/// lexer does not track multi-line string literals — the continuation lines
/// of a string opened on an earlier line are lexed as code, so string content
/// that *looks like* a `let`/`for`/brace can perturb binding tracking. Keep
/// multi-line strings out of workflow bodies (they are almost always test
/// fixtures) or suppress the resulting finding.
fn check_hash_iteration(wf_name: &str, body_lines: &[(u32, &str)], file: &str) -> DetCheckReport {
    use std::collections::HashMap;

    let mut report = DetCheckReport::default();
    let stripped: Vec<String> = body_lines
        .iter()
        .map(|&(_, line)| strip_unparseable_content(line))
        .collect();
    // Per-line brace/marker metadata, computed once (single pass) so the
    // per-finding loop-body severity scan is O(1) per interior line —
    // adversarial unbalanced-brace input can no longer make it quadratic
    // (PR #970 review, P2-A).
    let meta: Vec<Det010LineMeta> = stripped.iter().map(|l| det010_line_meta(l)).collect();

    // Lexical scope stack: ident → is-hash. Lookup walks innermost-out.
    let mut scopes: Vec<HashMap<String, bool>> = vec![HashMap::new()];
    // For-loop pattern idents awaiting the loop body's `{`: masked into the
    // scope that brace opens, so the mask covers exactly the body extent.
    let mut pending_masks: Vec<String> = Vec::new();

    for (idx, &(source_line, raw_line)) in body_lines.iter().enumerate() {
        let code = &stripped[idx];

        for (pos, event) in det010_line_events(code) {
            match event {
                Det010Event::Open => {
                    let mut scope = HashMap::new();
                    for ident in std::mem::take(&mut pending_masks) {
                        scope.insert(ident, false);
                    }
                    scopes.push(scope);
                }
                Det010Event::Close => {
                    if scopes.len() > 1 {
                        scopes.pop();
                    }
                }
                Det010Event::Let => {
                    let stmt = det010_let_statement_text(&stripped, idx, pos + 3);
                    match det010_parse_let(&stmt) {
                        Det010LetBinding::Simple { ident, is_hash } => {
                            if let Some(scope) = scopes.last_mut() {
                                scope.insert(ident, is_hash);
                            }
                        }
                        Det010LetBinding::PatternMask(idents) => {
                            if let Some(scope) = scopes.last_mut() {
                                for ident in idents {
                                    scope.insert(ident, false);
                                }
                            }
                        }
                    }
                }
                Det010Event::For => {
                    let after_for = &code[pos + 3..];
                    let header = after_for
                        .find('{')
                        .map_or(after_for, |brace| &after_for[..brace]);
                    let Some(in_pos) = header.rfind(" in ") else {
                        continue;
                    };
                    // Queue the loop pattern's idents: they mask outer
                    // bindings for the body extent (applied at the body `{`).
                    pending_masks.extend(det010_pattern_mask_idents(&header[..in_pos]));

                    let Some(ident) = det010_for_target(&header[in_pos + 4..], &scopes) else {
                        continue;
                    };

                    let prev_line = if idx > 0 { body_lines[idx - 1].1 } else { "" };
                    let severity = if det010_loop_body_has_command(&stripped, &meta, idx, pos) {
                        DetSeverity::Error
                    } else {
                        DetSeverity::Warning
                    };
                    det010_emit_for_finding(
                        &mut report,
                        wf_name,
                        file,
                        source_line,
                        (raw_line, prev_line),
                        &ident,
                        severity,
                    );
                }
            }
        }
    }

    report
}

/// Collects the positional events of one stripped line, sorted by byte offset.
fn det010_line_events(code: &str) -> Vec<(usize, Det010Event)> {
    let mut events: Vec<(usize, Det010Event)> = Vec::new();
    for (pos, ch) in code.char_indices() {
        match ch {
            '{' => events.push((pos, Det010Event::Open)),
            '}' => events.push((pos, Det010Event::Close)),
            _ => {}
        }
    }
    for pos in word_positions(code, "let") {
        events.push((pos, Det010Event::Let));
    }
    for pos in word_positions(code, "for") {
        events.push((pos, Det010Event::For));
    }
    events.sort_by_key(|&(pos, _)| pos);
    events
}

/// Records a DET010 finding — or, when a `harvest-suppress` comment for
/// DET010 (or its HVG011 catalog alias, AC5) is present, the suppression,
/// echoed with the exact id the author wrote.
fn det010_emit_for_finding(
    report: &mut DetCheckReport,
    wf_name: &str,
    file: &str,
    source_line: u32,
    (raw_line, prev_line): (&str, &str),
    ident: &str,
    severity: DetSeverity,
) {
    let suppression = find_suppression(DET010_ID, raw_line, prev_line)
        .map(|reason| (DET010_ID.to_string(), reason))
        .or_else(|| {
            find_suppression(DET010_HVG_ALIAS, raw_line, prev_line)
                .map(|reason| (DET010_HVG_ALIAS.to_string(), reason))
        });
    if let Some((rule_id, reason)) = suppression {
        report.suppressions.push(DetSuppression {
            rule_id,
            reason,
            location: DetLocation {
                file: file.to_string(),
                line: source_line,
            },
        });
        return;
    }

    report.findings.push(DetFinding {
        rule_id: DET010_ID,
        severity,
        workflow_name: Some(wf_name.to_string()),
        location: Some(DetLocation {
            file: file.to_string(),
            line: source_line,
        }),
        message: format!("[{DET010_ID}] {DET010_MESSAGE} (iterated hash-typed binding: `{ident}`)"),
        alternative: DET010_ALTERNATIVE,
    });
}

const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// DET011 candidate patterns for the futures combinator FUNCTIONS. Each is a
/// cheap substring locator; the accept/reject decision is made by the
/// path-precise [`matches_futures_combinator_pattern`], which extracts the
/// full qualified path ending at the call and matches it exactly against the
/// allowed set. This keeps `det_check` in lock-step with the compile-time HVG010
/// guardrail's `is_select_combinator_path`, which matches whole paths exactly
/// (#980 Codex P2 review).
const DET011_COMBINATOR_FN_PATTERNS: &[&str] = &[
    "future::select(",
    "select_all(",
    "select_ok(",
    "try_select(",
];

/// The exact set of paths that name a futures wait-first combinator FUNCTION.
///
/// This mirrors, case-for-case, `is_select_combinator_path` in the macro
/// crate's `determinism_lint.rs`: the `futures`-anchored qualified forms, their
/// `future::…` short forms, and the genuinely bare distinctive names
/// `select_all`/`select_ok`/`try_select`. A qualified path with any OTHER root
/// (`crate::future::select`, `my_dsl::select_all`, `bar::future::select_ok`,
/// `foo::try_select`) is deliberately absent, and bare `select` is deliberately
/// absent (too ambiguous a free-fn / query-builder name to hard-block).
fn is_allowed_combinator_path(path: &str) -> bool {
    matches!(
        path,
        "futures::future::select"
            | "futures::future::select_all"
            | "futures::future::select_ok"
            | "futures::future::try_select"
            | "future::select"
            | "future::select_all"
            | "future::select_ok"
            | "future::try_select"
            | "select_all"
            | "select_ok"
            | "try_select"
    )
}

/// True if a candidate combinator `pattern` occurs in `code` as a genuine
/// futures combinator call. For each occurrence of `pattern` (which ends in the
/// `(` of the call), the full qualified path ending at that call is extracted by
/// walking backward over the contiguous run of path bytes (identifier bytes and
/// `:`), then matched exactly against [`is_allowed_combinator_path`]. A match
/// whose path run is immediately preceded by `.` is a method call and is
/// rejected. This is path-precise, so an unrelated helper of the same tail name
/// under a different root (`crate::future::select(`, `my_dsl::select_all(`) is
/// never flagged — mirroring the macro lint's exact-path matching (#980 P2).
fn matches_futures_combinator_pattern(code: &str, pattern: &str) -> bool {
    let bytes = code.as_bytes();
    // `name_len` = pattern length minus the trailing `(`.
    let name_len = pattern.len() - 1;
    code.match_indices(pattern).any(|(pos, _)| {
        // `name_end` is the byte index just past the last char of the call
        // name (i.e. the position of the `(`).
        let name_end = pos + name_len;
        // Walk backward over the contiguous path run (ident bytes and `:`).
        let mut run_start = pos;
        while run_start > 0 {
            let b = bytes[run_start - 1];
            if is_ident_byte(b) || b == b':' {
                run_start -= 1;
            } else {
                break;
            }
        }
        // A `.` immediately before the path run is a method call — reject.
        if run_start > 0 && bytes[run_start - 1] == b'.' {
            return false;
        }
        let path = &code[run_start..name_end];
        is_allowed_combinator_path(path)
    })
}

/// DET011 macro patterns matched only at an identifier boundary — the select
/// macros. A preceding identifier byte means the token is only a *suffix* of a
/// longer, unrelated macro name (`sql_select!`, `my_select!`,
/// `foo_select_biased!`), so it is not flagged; this mirrors the compile-time
/// HVG010 guardrail, which matches macro paths exactly and accepts those
/// unrelated macros (#799 P2 review). A preceding `::` (as in `tokio::select!`
/// / `futures::select!`), brace, whitespace, or line start is a boundary and
/// still matches.
const DET011_MACRO_BOUNDARY_PATTERNS: &[&str] = &["select!", "select_biased!"];

/// True if `pattern` occurs in `code` at an identifier boundary: the byte
/// immediately preceding a match is not an identifier byte.
fn matches_at_ident_boundary(code: &str, pattern: &str) -> bool {
    let bytes = code.as_bytes();
    code.match_indices(pattern)
        .any(|(pos, _)| pos == 0 || !is_ident_byte(bytes[pos - 1]))
}

/// Byte positions of whole-word occurrences of `word` in `code`.
fn word_positions(code: &str, word: &str) -> Vec<usize> {
    let bytes = code.as_bytes();
    code.match_indices(word)
        .filter(|&(pos, _)| {
            let before_ok = pos == 0 || !is_ident_byte(bytes[pos - 1]);
            let after = pos + word.len();
            let after_ok = after >= bytes.len() || !is_ident_byte(bytes[after]);
            before_ok && after_ok
        })
        .map(|(pos, _)| pos)
        .collect()
}

const fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Text of one `let` statement (everything after the `let` keyword, up to but
/// not including the terminating `;`), joining continuation lines when the
/// statement spans multiple source lines (PR #970 review, P2-D).
///
/// Join rules (conservative — a failed join defaults to not-tracked):
/// - the same-line remainder ends the join at the first `;` or brace;
/// - up to [`DET010_LET_JOIN_BUDGET`] brace-free continuation lines are
///   appended (a continuation line with a brace aborts the join so block
///   structure is never swallowed);
/// - the join is a non-consuming LOOKAHEAD: the main event loop still
///   processes the continuation lines normally afterwards.
fn det010_let_statement_text(stripped: &[String], idx: usize, after_let_start: usize) -> String {
    let remainder = &stripped[idx][after_let_start..];
    if let Some(end) = remainder.find(';') {
        return remainder[..end].to_string();
    }
    if let Some(end) = remainder.find(['{', '}']) {
        return remainder[..end].to_string();
    }

    let mut joined = remainder.to_string();
    for line in stripped.iter().skip(idx + 1).take(DET010_LET_JOIN_BUDGET) {
        if line.contains(['{', '}']) {
            break;
        }
        joined.push(' ');
        if let Some(end) = line.find(';') {
            joined.push_str(&line[..end]);
            break;
        }
        joined.push_str(line);
    }
    joined
}

/// Collects the idents a pattern plausibly binds so they can be masked.
/// Lowercase-/underscore-initial words only (skips type and variant names
/// like `Some`/`Point`) and the binding-mode keywords. Over-collection is
/// safe: masking means "not hash", so at worst a legitimate finding is
/// missed, never a false positive introduced.
fn det010_pattern_mask_idents(pattern: &str) -> Vec<String> {
    let bytes = pattern.as_bytes();
    let mut idents = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if is_ident_byte(bytes[i]) {
            let start = i;
            while i < bytes.len() && is_ident_byte(bytes[i]) {
                i += 1;
            }
            let word = &pattern[start..i];
            let first = word.chars().next().unwrap_or('0');
            if (first.is_ascii_lowercase() || first == '_')
                && !matches!(word, "mut" | "ref" | "box" | "let")
            {
                idents.push(word.to_string());
            }
        } else {
            i += 1;
        }
    }
    idents
}

/// Parses one `let` statement (text after the `let` keyword) into a DET010
/// binding decision. Positional, not word-containment: a statement that
/// merely *mentions* `HashMap` somewhere (`Vec<HashMap<..>>` annotation,
/// `load_ids::<HashMap<..>>()` call turbofish) is NOT tracked — only a type
/// annotation that *starts with* `HashMap`/`HashSet` (path prefix stripped),
/// a `HashMap::new()`-family constructor call, or a
/// `.collect::<HashMap<..>>()` turbofish (incl. `Result`/`Option`-wrapped)
/// tracks the binding (PR #970 review, P1-B).
fn det010_parse_let(stmt: &str) -> Det010LetBinding {
    let s = stmt.trim_start();
    let s = s.strip_prefix("mut ").map_or(s, str::trim_start);
    let ident: String = s.chars().take_while(|&c| is_ident_char(c)).collect();
    let rest = s[ident.len()..].trim_start();

    // Pattern shapes — destructuring `(a, b)`, enum variants `Some(x)`,
    // struct patterns `Point { x }` — bind no single trackable ident: mask
    // every plausibly-bound ident instead (shadowing, never tracking).
    if ident.is_empty() || rest.starts_with('(') || rest.starts_with('{') || rest.starts_with("::")
    {
        let pattern_part = stmt.split('=').next().unwrap_or(stmt);
        return Det010LetBinding::PatternMask(det010_pattern_mask_idents(pattern_part));
    }

    let mut is_hash = false;
    if let Some(after_colon) = rest.strip_prefix(':') {
        let (type_text, init) = det010_split_type_and_init(after_colon);
        is_hash = det010_type_is_hash(type_text) || init.is_some_and(det010_init_is_hash);
    } else if let Some(init) = rest.strip_prefix('=') {
        is_hash = det010_init_is_hash(init);
    }
    Det010LetBinding::Simple { ident, is_hash }
}

/// Splits `": Type = init"` content (text after the annotation colon) into
/// the type text and the optional initializer. The initializer `=` is the
/// first `=` at angle-bracket depth <= 0 that does not begin `==` (an
/// associated-type binding `Item = X` inside `<..>` is at depth > 0; the `>`
/// of a fn-pointer `->` return arrow can push the running depth negative,
/// hence `<=`).
fn det010_split_type_and_init(after_colon: &str) -> (&str, Option<&str>) {
    let bytes = after_colon.as_bytes();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'<' => depth += 1,
            b'>' => depth -= 1,
            b'=' if depth <= 0 && bytes.get(i + 1) != Some(&b'=') => {
                return (&after_colon[..i], Some(&after_colon[i + 1..]));
            }
            _ => {}
        }
        i += 1;
    }
    (after_colon, None)
}

/// `true` when a type annotation text names a hash collection at its ROOT:
/// optional leading path segments (`std::collections::`), then
/// `HashMap`/`HashSet` followed by `<`, whitespace, or end. `Vec<HashMap<..>>`
/// and `Option<HashMap<..>>` do not match — the hash type is nested, not the
/// binding's own type.
fn det010_type_is_hash(type_text: &str) -> bool {
    let mut t = type_text.trim();
    t = t.strip_prefix("::").unwrap_or(t).trim_start();
    loop {
        let ident_len = t.bytes().take_while(|&b| is_ident_byte(b)).count();
        if ident_len == 0 {
            return false;
        }
        let (head, tail) = t.split_at(ident_len);
        let tail = tail.trim_start();
        if head == "HashMap" || head == "HashSet" {
            return tail.is_empty() || tail.starts_with('<');
        }
        match tail.strip_prefix("::") {
            Some(rest) => t = rest.trim_start(),
            None => return false,
        }
    }
}

const DET010_HASH_CTORS: &[&str] = &[
    "new",
    "from",
    "from_iter",
    "with_capacity",
    "with_hasher",
    "with_capacity_and_hasher",
    "default",
];

/// Byte index just past the `>` matching a leading `<`, or `None`.
fn det010_skip_angle(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'<') {
        return None;
    }
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'<' => depth += 1,
            b'>' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
    }
    None
}

/// `true` when the initializer contains a `HashMap::ctor(..)` /
/// `HashSet::ctor(..)` constructor call (any path prefix; tolerates a type
/// turbofish, `HashMap::<K, V>::new()`).
fn det010_init_has_hash_ctor(init: &str) -> bool {
    for word in ["HashMap", "HashSet"] {
        for pos in word_positions(init, word) {
            let mut rest = &init[pos + word.len()..];
            let Some(r) = rest.strip_prefix("::") else {
                continue;
            };
            rest = r;
            if rest.starts_with('<') {
                let Some(end) = det010_skip_angle(rest) else {
                    continue;
                };
                let Some(r) = rest[end..].strip_prefix("::") else {
                    continue;
                };
                rest = r;
            }
            let ctor_len = rest.bytes().take_while(|&b| is_ident_byte(b)).count();
            let (ctor, after) = rest.split_at(ctor_len);
            if DET010_HASH_CTORS.contains(&ctor) && after.trim_start().starts_with('(') {
                return true;
            }
        }
    }
    false
}

/// `true` when a `.collect::<..>()` turbofish in the initializer targets a
/// hash collection, including the `Result<HashMap<..>, E>` /
/// `Option<HashMap<..>>` wrapped forms (the wrapper's first generic argument
/// is what `?`/unwrap yields). `.collect::<Vec<HashMap<..>>>()` does not
/// match — the produced collection is the Vec.
fn det010_collect_targets_hash(init: &str) -> bool {
    const NEEDLE: &str = ".collect::<";
    let mut search = init;
    while let Some(pos) = search.find(NEEDLE) {
        let after = &search[pos + NEEDLE.len()..];
        if det010_turbofish_is_hash(after) {
            return true;
        }
        search = after;
    }
    false
}

fn det010_turbofish_is_hash(turbofish: &str) -> bool {
    let mut t = turbofish.trim_start();
    t = t.strip_prefix("::").unwrap_or(t).trim_start();
    loop {
        let ident_len = t.bytes().take_while(|&b| is_ident_byte(b)).count();
        if ident_len == 0 {
            return false;
        }
        let (head, tail) = t.split_at(ident_len);
        let tail = tail.trim_start();
        match head {
            "HashMap" | "HashSet" => {
                return tail.is_empty()
                    || tail.starts_with('<')
                    || tail.starts_with('>')
                    || tail.starts_with(',');
            }
            // Fallible / optional collect: unwrap into the first generic arg.
            "Result" | "Option" => match tail.strip_prefix('<') {
                Some(rest) => t = rest.trim_start(),
                None => return false,
            },
            _ => match tail.strip_prefix("::") {
                Some(rest) => t = rest.trim_start(),
                None => return false,
            },
        }
    }
}

fn det010_init_is_hash(init: &str) -> bool {
    det010_init_has_hash_ctor(init) || det010_collect_targets_hash(init)
}

/// Innermost-out scope lookup: the nearest binding of `ident` decides.
fn det010_scope_lookup(scopes: &[std::collections::HashMap<String, bool>], ident: &str) -> bool {
    for scope in scopes.iter().rev() {
        if let Some(&is_hash) = scope.get(ident) {
            return is_hash;
        }
    }
    false
}

/// If a `for` loop header's iterated expression (`expr` is the text after
/// ` in `) is a tracked hash-typed binding — a bare ident, `&ident` /
/// `&mut ident`, or exactly one argument-free [`DET010_ITER_METHODS`] call on
/// the ident — returns the binding ident.
fn det010_for_target(
    expr: &str,
    scopes: &[std::collections::HashMap<String, bool>],
) -> Option<String> {
    let expr = expr.trim();
    let expr = expr.strip_prefix("&mut ").unwrap_or(expr);
    let expr = expr.strip_prefix('&').unwrap_or(expr).trim();

    // Bare tracked ident.
    if !expr.is_empty() && expr.chars().all(is_ident_char) {
        return det010_scope_lookup(scopes, expr).then(|| expr.to_string());
    }

    // Exactly one argument-free iteration method call: `ident.method()`.
    let (ident, call) = expr.split_once('.')?;
    if ident.is_empty() || !ident.chars().all(is_ident_char) || call.contains('.') {
        return None;
    }
    let method = call.strip_suffix("()")?;
    if !DET010_ITER_METHODS.contains(&method) {
        return None;
    }
    det010_scope_lookup(scopes, ident).then(|| ident.to_string())
}

/// Per-line metadata for the loop-body severity scan, computed once per body.
struct Det010LineMeta {
    /// Net brace delta of the whole line.
    delta: i32,
    /// Minimum running brace delta within the line (detects a close that
    /// dips the depth to zero even when the net delta does not).
    min_delta: i32,
    /// Whether the line contains any `{`.
    has_open: bool,
    /// Whether the line contains any [`DET010_COMMAND_MARKERS`] substring.
    has_marker: bool,
}

fn det010_line_meta(line: &str) -> Det010LineMeta {
    let mut delta = 0i32;
    let mut min_delta = 0i32;
    let mut has_open = false;
    for ch in line.chars() {
        match ch {
            '{' => {
                delta += 1;
                has_open = true;
            }
            '}' => {
                delta -= 1;
                min_delta = min_delta.min(delta);
            }
            _ => {}
        }
    }
    Det010LineMeta {
        delta,
        min_delta,
        has_open,
        has_marker: det010_contains_marker(line),
    }
}

fn det010_contains_marker(text: &str) -> bool {
    DET010_COMMAND_MARKERS
        .iter()
        .any(|marker| text.contains(marker))
}

/// Char-level brace walk over one line segment. Updates `depth`/`entered` and
/// returns the byte position (within the segment) of the brace that closes
/// the loop body, if reached.
fn det010_scan_segment(segment: &str, depth: &mut i64, entered: &mut bool) -> Option<usize> {
    for (pos, ch) in segment.char_indices() {
        match ch {
            '{' => {
                *depth += 1;
                *entered = true;
            }
            '}' => {
                if *depth > 0 {
                    *depth -= 1;
                }
                if *entered && *depth == 0 {
                    return Some(pos);
                }
            }
            _ => {}
        }
    }
    None
}

/// Scans the loop body for command markers to decide DET010 severity.
///
/// The scan starts at the `for` token's byte offset on the `for` line
/// (`for_pos`) — a command or an enclosing `{` BEFORE the `for` on the same
/// line can neither inflate the severity nor drag the depth accounting past
/// the loop's own close (PR #970 review, P3.2). Interior lines use the
/// precomputed per-line [`Det010LineMeta`] so each is O(1) unless it is the
/// line the body actually closes on — unbalanced-brace input therefore costs
/// O(lines) per finding instead of an O(bytes-to-EOF) rescan (P2-A).
fn det010_loop_body_has_command(
    stripped: &[String],
    meta: &[Det010LineMeta],
    start: usize,
    for_pos: usize,
) -> bool {
    let mut depth: i64 = 0;
    let mut entered = false;

    // First line: char scan from the `for` token.
    let first = &stripped[start][for_pos..];
    if let Some(close) = det010_scan_segment(first, &mut depth, &mut entered) {
        return det010_contains_marker(&first[..close]);
    }
    if det010_contains_marker(first) {
        return true;
    }

    for j in (start + 1)..stripped.len() {
        let line = &stripped[j];
        if entered {
            if depth + i64::from(meta[j].min_delta) <= 0 {
                // The body closes somewhere on this line — exact-position scan.
                if let Some(close) = det010_scan_segment(line, &mut depth, &mut entered) {
                    return det010_contains_marker(&line[..close]);
                }
                if det010_contains_marker(line) {
                    return true;
                }
            } else {
                if meta[j].has_marker {
                    return true;
                }
                depth += i64::from(meta[j].delta);
            }
        } else if meta[j].has_open {
            // Multi-line loop header: char-scan the line that opens the body.
            if let Some(close) = det010_scan_segment(line, &mut depth, &mut entered) {
                return det010_contains_marker(&line[..close]);
            }
            if det010_contains_marker(line) {
                return true;
            }
        } else if meta[j].has_marker {
            return true;
        }
    }

    false
}

// ── Lexer helpers ─────────────────────────────────────────────────────────────

/// Returns the portion of `line` that precedes the first `//` (if any).
/// This avoids flagging patterns that appear only inside comments.
fn strip_line_comment(line: &str) -> &str {
    line_comment_start(line).map_or(line, |pos| &line[..pos])
}

/// Returns the byte position immediately after the closing `*/` of a block
/// comment, scanning from `start` (the position right after the opening `/*`).
/// Returns `line.len()` if the comment is not closed on this line.
/// Nested block comments are not supported.
fn block_comment_end(line: &str, start: usize) -> usize {
    let bytes = line.as_bytes();
    let mut pos = start;
    while pos < bytes.len() {
        if bytes[pos] == b'*' && pos + 1 < bytes.len() && bytes[pos + 1] == b'/' {
            return pos + 2;
        }
        pos += 1;
    }
    line.len()
}

/// Updates `depth` for braces in code, ignoring braces inside simple Rust
/// string/character literals, line comments, and block comments. Returns the
/// byte position of the closing brace that returns the depth to zero, if present.
fn scan_braces_outside_literals(line: &str, depth: &mut u32) -> Option<usize> {
    let mut pos = 0;

    while pos < line.len() {
        if let Some(end) = raw_string_end(line, pos) {
            pos = end;
            continue;
        }

        let Some((ch, next_pos)) = next_char(line, pos) else {
            break;
        };

        match ch {
            '"' => pos = normal_string_end(line, pos),
            '\'' => {
                pos = char_literal_end(line, pos).unwrap_or(next_pos);
            }
            '/' if line[next_pos..].starts_with('/') => break,
            '/' if line[next_pos..].starts_with('*') => {
                pos = block_comment_end(line, next_pos + 1);
            }
            '{' => {
                *depth = depth.saturating_add(1);
                pos = next_pos;
            }
            '}' => {
                *depth = depth.saturating_sub(1);
                if *depth == 0 {
                    return Some(pos);
                }
                pos = next_pos;
            }
            _ => pos = next_pos,
        }
    }

    None
}

/// Returns the byte position of the first `//` outside simple Rust
/// string/character literals and block comments.
fn line_comment_start(line: &str) -> Option<usize> {
    let mut pos = 0;

    while pos < line.len() {
        if let Some(end) = raw_string_end(line, pos) {
            pos = end;
            continue;
        }

        let Some((ch, next_pos)) = next_char(line, pos) else {
            break;
        };

        match ch {
            '"' => pos = normal_string_end(line, pos),
            '\'' => {
                pos = char_literal_end(line, pos).unwrap_or(next_pos);
            }
            '/' if line[next_pos..].starts_with('/') => return Some(pos),
            '/' if line[next_pos..].starts_with('*') => {
                pos = block_comment_end(line, next_pos + 1);
            }
            _ => pos = next_pos,
        }
    }

    None
}

fn next_char(line: &str, pos: usize) -> Option<(char, usize)> {
    let ch = line[pos..].chars().next()?;
    Some((ch, pos + ch.len_utf8()))
}

fn normal_string_end(line: &str, start: usize) -> usize {
    let mut pos = start + 1;
    let mut escaped = false;

    while pos < line.len() {
        let Some((ch, next_pos)) = next_char(line, pos) else {
            break;
        };

        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return next_pos;
        }

        pos = next_pos;
    }

    line.len()
}

fn char_literal_end(line: &str, start: usize) -> Option<usize> {
    let content_start = start + 1;
    let (first, first_end) = next_char(line, content_start)?;

    if is_lifetime_start(first) {
        let ident_end = consume_lifetime_ident(line, first_end);
        if !line[ident_end..].starts_with('\'') {
            return None;
        }
    }

    let mut pos = content_start;
    let mut escaped = false;

    while pos < line.len() {
        let (ch, next_pos) = next_char(line, pos)?;

        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '\'' && pos != content_start {
            return Some(next_pos);
        }

        pos = next_pos;
    }

    None
}

const fn is_lifetime_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

const fn is_lifetime_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn consume_lifetime_ident(line: &str, mut pos: usize) -> usize {
    while pos < line.len() {
        let Some((ch, next_pos)) = next_char(line, pos) else {
            break;
        };
        if !is_lifetime_continue(ch) {
            break;
        }
        pos = next_pos;
    }
    pos
}

fn raw_string_end(line: &str, start: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut pos = start;

    match bytes.get(pos).copied()? {
        b'r' => pos += 1,
        b'b' | b'c' if bytes.get(pos + 1).copied() == Some(b'r') => pos += 2,
        _ => return None,
    }

    let hash_start = pos;
    while bytes.get(pos).copied() == Some(b'#') {
        pos += 1;
    }
    if bytes.get(pos).copied() != Some(b'"') {
        return None;
    }

    let hashes = pos - hash_start;
    pos += 1;

    while pos < bytes.len() {
        if bytes[pos] == b'"' {
            let marker_start = pos + 1;
            let marker_end = marker_start + hashes;
            if marker_end <= bytes.len()
                && bytes[marker_start..marker_end]
                    .iter()
                    .all(|&byte| byte == b'#')
            {
                return Some(marker_end);
            }
        }
        pos += 1;
    }

    Some(line.len())
}

/// Returns a copy of `line` with string literal and char literal content removed
/// and everything from the first `//` comment stripped.
/// This prevents pattern matches inside string data or comments.
fn strip_unparseable_content(line: &str) -> String {
    let code = strip_line_comment(line);
    let mut result = String::with_capacity(code.len());
    let mut pos = 0;

    while pos < code.len() {
        if let Some(end) = raw_string_end(code, pos) {
            pos = end;
            continue;
        }

        let Some((ch, next_pos)) = next_char(code, pos) else {
            break;
        };

        match ch {
            '"' => pos = normal_string_end(code, pos),
            '\'' => pos = char_literal_end(code, pos).unwrap_or(next_pos),
            '/' if code[next_pos..].starts_with('*') => {
                pos = block_comment_end(code, next_pos + 1);
            }
            _ => {
                result.push(ch);
                pos = next_pos;
            }
        }
    }

    result
}

/// Returns the suppression reason if either `line` or `prev_line` contains a
/// valid `// harvest-suppress: RULE_ID "reason"` comment for `rule_id`.
///
/// `prev_line` is only eligible when it is a *standalone* comment line
/// (no code before `//`). A trailing inline comment on `prev_line` is
/// scoped to that line only and must not suppress violations on the next line.
fn find_suppression(rule_id: &str, line: &str, prev_line: &str) -> Option<String> {
    let prev_is_standalone =
        line_comment_start(prev_line).is_some_and(|pos| prev_line[..pos].trim().is_empty());
    if prev_is_standalone {
        parse_suppression_comment(rule_id, prev_line)
    } else {
        None
    }
    .or_else(|| parse_suppression_comment(rule_id, line))
}

/// Parses `// harvest-suppress: RULE_ID "reason string"` from a line.
/// The marker may appear anywhere in the line (supports both standalone comment
/// lines and trailing inline comments such as `let x = foo(); // harvest-suppress: …`).
/// Returns the reason string if valid and non-empty, `None` otherwise.
fn parse_suppression_comment(rule_id: &str, line: &str) -> Option<String> {
    let comment_start = line_comment_start(line)?;
    let rest = line[comment_start + 2..].trim_start();
    let rest = rest.strip_prefix("harvest-suppress:")?;
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(rule_id)?.trim_start();
    let inner = rest.strip_prefix('"')?;
    let end = inner.find('"')?;
    let reason = &inner[..end];
    if reason.is_empty() {
        None
    } else {
        Some(reason.to_string())
    }
}

// ── Module-level unit tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_workflow_attr_matches_bare_and_parameterised() {
        assert!(is_workflow_attr("#[workflow]"));
        assert!(is_workflow_attr("#[workflow()]"));
        assert!(!is_workflow_attr("#[activity]"));
        assert!(!is_workflow_attr("#[derive(Debug)]"));
        assert!(!is_workflow_attr("// #[workflow]"));
    }

    #[test]
    fn extract_fn_name_handles_visibility_and_async() {
        assert_eq!(
            extract_fn_name("async fn my_wf("),
            Some("my_wf".to_string())
        );
        assert_eq!(
            extract_fn_name("pub async fn billing("),
            Some("billing".to_string())
        );
        assert_eq!(
            extract_fn_name("pub(crate) async fn inner("),
            Some("inner".to_string())
        );
        assert_eq!(extract_fn_name("let x = 1;"), None);
    }

    #[test]
    fn parse_suppression_comment_requires_quoted_reason() {
        assert_eq!(
            parse_suppression_comment("DET001", "// harvest-suppress: DET001 \"safe here\""),
            Some("safe here".to_string())
        );
        // No reason → None
        assert_eq!(
            parse_suppression_comment("DET001", "// harvest-suppress: DET001"),
            None
        );
        // Empty reason → None
        assert_eq!(
            parse_suppression_comment("DET001", "// harvest-suppress: DET001 \"\""),
            None
        );
        // Wrong rule → None
        assert_eq!(
            parse_suppression_comment("DET002", "// harvest-suppress: DET001 \"reason\""),
            None
        );
    }

    #[test]
    fn strip_unparseable_content_removes_comments_and_strings() {
        assert_eq!(
            strip_unparseable_content("let x = 1; // comment"),
            "let x = 1; "
        );
        assert_eq!(strip_unparseable_content("let x = 1;"), "let x = 1;");
        assert_eq!(strip_unparseable_content("// full comment"), "");
        // String literal content is silently removed.
        assert_eq!(
            strip_unparseable_content(r#"let s = "std::fs::read";"#),
            "let s = ;"
        );
        // Escaped quote inside string does not prematurely close it.
        assert_eq!(
            strip_unparseable_content(r#"let s = "say \"hi\"";"#),
            "let s = ;"
        );
    }

    #[test]
    fn parse_suppression_comment_handles_inline_trailing_comment() {
        // Same-line trailing suppression comment.
        assert_eq!(
            parse_suppression_comment(
                "DET001",
                r#"let _ = SystemTime::now(); // harvest-suppress: DET001 "safe""#
            ),
            Some("safe".to_string())
        );
    }

    #[test]
    fn strip_unparseable_content_removes_block_comments() {
        // Block comment content is stripped.
        assert_eq!(
            strip_unparseable_content("let x = /* std::fs::read */ 5;"),
            "let x =  5;"
        );
        // Block comment with `}` inside must not confuse callers.
        assert_eq!(
            strip_unparseable_content("foo() /* } */ .bar()"),
            "foo()  .bar()"
        );
        // `/* // */` must not trick line_comment_start into discarding real code.
        assert_eq!(
            strip_unparseable_content("/* // */ let x = 1;"),
            " let x = 1;"
        );
    }

    #[test]
    fn check_source_empty_source_produces_no_findings() {
        let report = check_source("", "empty.rs");
        assert!(report.findings.is_empty());
        assert!(report.suppressions.is_empty());
    }

    #[test]
    fn check_source_no_workflow_annotation_produces_no_findings() {
        let src = "async fn foo() { let _ = std::time::SystemTime::now(); }\n";
        let report = check_source(src, "test.rs");
        assert!(report.findings.is_empty());
    }
}
