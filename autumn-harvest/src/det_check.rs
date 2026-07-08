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
                if !code_part.contains(pattern) {
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
];

const DET010_MESSAGE: &str =
    "Iterating a HashMap/HashSet inside a workflow function observes hash-randomized \
     iteration order, which can differ between the original run and any replay on another \
     worker process. Commands scheduled inside the loop are recorded in history in iteration \
     order, so a reordered replay produces a different command sequence and diverges \
     (non-determinism error / nd-block).";

const DET010_ALTERNATIVE: &str =
    "Use a BTreeMap/BTreeSet for any collection the workflow iterates, or collect the keys \
     into a Vec and sort() it before iterating: `let mut keys: Vec<_> = \
     map.keys().cloned().collect(); keys.sort();`.";

/// Single ordered pass over a workflow body: track hash-typed `let` bindings
/// (last-binding-wins per ident), then flag `for … in` loops over a tracked
/// binding, with command-aware severity and `harvest-suppress` support.
fn check_hash_iteration(wf_name: &str, body_lines: &[(u32, &str)], file: &str) -> DetCheckReport {
    use std::collections::HashSet;

    let mut report = DetCheckReport::default();
    let mut tracked: HashSet<String> = HashSet::new();
    let stripped: Vec<String> = body_lines
        .iter()
        .map(|&(_, line)| strip_unparseable_content(line))
        .collect();

    for (idx, &(source_line, raw_line)) in body_lines.iter().enumerate() {
        let code = &stripped[idx];

        // Bindings first: a `let` and a `for` on the same line appear in that
        // source order.
        record_det010_bindings(code, &mut tracked);

        let Some(ident) = det010_for_loop_target(code, &tracked) else {
            continue;
        };

        let prev_line = if idx > 0 { body_lines[idx - 1].1 } else { "" };
        if let Some(reason) = find_suppression(DET010_ID, raw_line, prev_line) {
            report.suppressions.push(DetSuppression {
                rule_id: DET010_ID.to_string(),
                reason,
                location: DetLocation {
                    file: file.to_string(),
                    line: source_line,
                },
            });
            continue;
        }

        let severity = if det010_loop_body_has_command(&stripped, idx) {
            DetSeverity::Error
        } else {
            DetSeverity::Warning
        };
        report.findings.push(DetFinding {
            rule_id: DET010_ID,
            severity,
            workflow_name: Some(wf_name.to_string()),
            location: Some(DetLocation {
                file: file.to_string(),
                line: source_line,
            }),
            message: format!(
                "[{DET010_ID}] {DET010_MESSAGE} (iterated hash-typed binding: `{ident}`)"
            ),
            alternative: DET010_ALTERNATIVE,
        });
    }

    report
}

const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
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

fn contains_word(code: &str, word: &str) -> bool {
    !word_positions(code, word).is_empty()
}

const fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Updates `tracked` for every `let` binding on this (comment/string-stripped)
/// line. A binding whose statement (up to the next `;` or end of line)
/// mentions `HashMap`/`HashSet` as a whole word — type annotation,
/// constructor call, or `.collect::<HashMap<..>>()` turbofish alike — marks
/// the ident; any other `let` re-binding of that ident untracks it
/// (last-binding-wins / shadowing). Destructuring patterns bind no single
/// ident and are ignored — unknown binding shapes default to NOT tracking,
/// since false positives are the top risk.
fn record_det010_bindings(code: &str, tracked: &mut std::collections::HashSet<String>) {
    for pos in word_positions(code, "let") {
        let after_let = code[pos + 3..].trim_start();
        let after_mut = after_let.strip_prefix("mut ").unwrap_or(after_let);
        let ident: String = after_mut
            .trim_start()
            .chars()
            .take_while(|&c| is_ident_char(c))
            .collect();
        if ident.is_empty() {
            continue;
        }

        let stmt_end = code[pos..].find(';').map_or(code.len(), |rel| pos + rel);
        let stmt = &code[pos..stmt_end];
        if contains_word(stmt, "HashMap") || contains_word(stmt, "HashSet") {
            tracked.insert(ident);
        } else {
            tracked.remove(&ident);
        }
    }
}

/// If this stripped line contains a `for … in <expr> {` loop whose iterated
/// expression is a tracked hash-typed binding — a bare ident, `&ident` /
/// `&mut ident`, or exactly one argument-free [`DET010_ITER_METHODS`] call on
/// the ident — returns the binding ident.
fn det010_for_loop_target(
    code: &str,
    tracked: &std::collections::HashSet<String>,
) -> Option<String> {
    let for_pos = word_positions(code, "for").into_iter().next()?;
    let after_for = &code[for_pos + 3..];
    let header = after_for
        .find('{')
        .map_or(after_for, |brace| &after_for[..brace]);
    let in_pos = header.rfind(" in ")?;
    let expr = header[in_pos + 4..].trim();
    let expr = expr.strip_prefix("&mut ").unwrap_or(expr);
    let expr = expr.strip_prefix('&').unwrap_or(expr).trim();

    // Bare tracked ident.
    if !expr.is_empty() && expr.chars().all(is_ident_char) {
        return tracked.contains(expr).then(|| expr.to_string());
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
    tracked.contains(ident).then(|| ident.to_string())
}

/// Scans the loop body starting at `start` (the `for` line itself) for
/// command markers, using brace depth over the stripped lines to bound the
/// body's extent. On the line where the loop closes, only text before the
/// closing brace is searched.
fn det010_loop_body_has_command(stripped: &[String], start: usize) -> bool {
    let mut depth: u32 = 0;
    let mut entered = false;

    for line in stripped.iter().skip(start) {
        let mut close_pos = None;
        for (pos, ch) in line.char_indices() {
            match ch {
                '{' => {
                    depth += 1;
                    entered = true;
                }
                '}' => {
                    depth = depth.saturating_sub(1);
                    if entered && depth == 0 {
                        close_pos = Some(pos);
                        break;
                    }
                }
                _ => {}
            }
        }

        let search = close_pos.map_or(line.as_str(), |pos| &line[..pos]);
        if DET010_COMMAND_MARKERS
            .iter()
            .any(|marker| search.contains(marker))
        {
            return true;
        }
        if close_pos.is_some() {
            return false;
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
