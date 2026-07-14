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
//! # Heuristic pre-check vs. authoritative guardrail
//!
//! `det_check` is a best-effort **text** scanner: a fast, CLI-friendly
//! pre-check that mirrors the compile-time guardrails so problems surface early
//! (in review or CI) without waiting for a build. Being text-based, it can lag
//! the guardrails on exotic syntax (turbofish, unusual spacing, multi-line
//! calls). The **authoritative** determinism gate is the syn-based proc-macro
//! guardrail that runs during compilation (e.g. HVG010 for DET011): it operates
//! on the parsed AST, so it always hard-blocks these forms even where the text
//! scanner would miss them. When the two disagree, trust the compile-time
//! guardrail — it is the safety net, and `#[workflow(allow_nondeterministic_apis)]`
//! (or a `// harvest-suppress:` comment for det_check) is the escape hatch.
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
///
/// Serializes as `"error"` / `"warning"` (issue #778 `--format json`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DetSeverity {
    /// Breaks replay determinism; counted as a hard blocker by [`DetCheckReport::has_hard_blockers`].
    Error,
    /// May break determinism in edge cases; reported but does not fail CI by default.
    Warning,
}

/// Source location reported in a finding.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DetLocation {
    /// Path (or label) of the file being analysed, as passed to [`check_source`].
    pub file: String,
    /// 1-indexed line number within that file.
    pub line: u32,
    /// 1-indexed column of the matched pattern within the source line.
    ///
    /// Best-effort: a real column is computed from the matched substring for the
    /// substring-table rules (DET001–DET009, DET011); the bespoke DET010 pass
    /// and any site lacking an offset report `1`. Always `> 0`.
    pub col: u32,
}

/// A single determinism violation found in a workflow function body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DetFinding {
    /// Stable rule identifier (e.g. `"DET001"`).
    pub rule_id: &'static str,
    /// How serious the violation is.
    pub severity: DetSeverity,
    /// Name of the `#[workflow]` function the violation is reachable from (the
    /// entry point). For a transitive finding this is the workflow that reaches
    /// the offending helper, not the helper itself.
    pub workflow_name: Option<String>,
    /// Source location, when available.
    pub location: Option<DetLocation>,
    /// Human-readable description of the violation.
    pub message: String,
    /// A deterministic Harvest-shaped alternative.
    pub alternative: &'static str,
    /// For a transitive finding (issue #778, one first-party hop): the name of
    /// the first-party helper function — reachable via a direct call from the
    /// `workflow_name` entry point — that contains the offending call. `None`
    /// for a direct-in-body finding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_helper: Option<String>,
}

/// An active suppression comment found in the source.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DetSuppression {
    /// The rule ID that was suppressed.
    pub rule_id: String,
    /// The required reason string from the comment.
    pub reason: String,
    /// Where the suppressed expression (not the comment) was located.
    pub location: DetLocation,
}

/// The result of checking one or more source files.
#[derive(Debug, Default, serde::Serialize)]
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
            // Select MACROS — the trailing `!` makes these unambiguous macro
            // invocations (no ident/method call contains it) and bounds the
            // macro NAME on the right, so `select_biasedx!` and a `select!`
            // match inside `select_biased!` are excluded. The candidate pattern
            // only LOCATES the macro name; the accept/reject decision is made by
            // the path-precise `matches_select_macro_pattern` (see
            // `DET011_MACRO_PATTERNS`), which extracts the FULL qualified macro
            // path (walking back over the contiguous path run, stripping an
            // optional leading `::`) and matches it EXACTLY against
            // `is_allowed_select_macro_path`. That precision is required so a
            // qualified macro under an UNRELATED root is NOT flagged:
            // `crate::ui::select! {}`, `my::select_biased! {}`,
            // `foo::bar::select! {}` all resolve to paths outside the allowed
            // set and are rejected — exactly as the compile-time HVG010
            // guardrail's `visit_macro` rejects them (#980 Codex P2 — the
            // earlier ident-boundary-only check false-positived on any `::`
            // prefix). `::tokio::select!` normalizes to `tokio::select` and IS
            // flagged; `sql_select!` / `my_select!` are absorbed into a
            // non-matching path by the backward walk and stay unflagged. The
            // allowed set mirrors `visit_macro` case-for-case: bare `select`,
            // `select_biased`, `tokio::select`, `futures::select`,
            // `futures::select_biased` (note `tokio::select_biased` is absent
            // there too).
            "select!",
            "select_biased!",
            // Combinator FUNCTIONS. These candidate patterns are the bare
            // combinator NAME anchors (no trailing `(`); they cheaply locate a
            // possible combinator call and the actual decision is made by the
            // path-precise `matches_futures_combinator_pattern` (see
            // `DET011_COMBINATOR_FN_PATTERNS`), which (a) confirms the name is a
            // complete token, (b) requires a call `(` to follow — after an
            // OPTIONAL turbofish `::<…>` and any surrounding whitespace, so a
            // turbofished call `future::select::<_, _>(a, b)` is caught too
            // (#980 Codex P2) — and (c) extracts the FULL qualified path ending
            // at the call and matches it exactly against the same allowed-path
            // set as the compile-time HVG010 guardrail's
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
            // `use futures::future::select_all;`). Bare `select` is
            // deliberately NOT in the allowed set (`select` is a common
            // free-fn / query-builder name), and a preceding `.` (method call,
            // e.g. `q.select_all()`) is rejected — mirroring the AST macro
            // visitor's structural exclusion of method calls.
            "future::select",
            "select_all",
            "select_ok",
            "try_select",
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

/// Check Rust source code text for determinism violations reachable from
/// `#[workflow]` functions.
///
/// Each `#[workflow]` body is scanned directly, and — one first-party hop
/// (issue #778) — every plain helper function it calls directly **that is
/// declared in the workflow's own module** (same file + module path) is scanned
/// too, attributing any finding to the workflow entry point via
/// [`DetFinding::via_helper`]. A helper in a different module / file (reached via
/// a `use` import the line-based scanner cannot see) is NOT resolved — a safe
/// false-negative (see [`resolve_helper`]). `#[activity]` bodies are never
/// scanned (activities are allowed to be non-deterministic by design), and
/// neither are the bodies of helpers a workflow does not reach.
///
/// `file` is used only for source-location reporting; no file is read.
/// The function always returns a report; it never panics on malformed input.
#[must_use]
pub fn check_source(source: &str, file: &str) -> DetCheckReport {
    let lines: Vec<&str> = source.lines().collect();
    let mut fns = extract_all_functions(&lines);
    for f in &mut fns {
        f.file = file.to_string();
    }
    scan_functions(&fns)
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
/// One-hop first-party helper reachability (issue #778) is resolved
/// **same-module only** — a helper in the caller's own module (same file +
/// module path; see [`resolve_helper`]); a cross-file / cross-module helper is
/// not resolved.
///
/// Build-output, hidden, and trybuild-fixture directories are skipped so a scan
/// of a repository root stays fast, never lints generated code, and never lints
/// the deliberately-broken `compile_fail/` fixtures (see [`skip_scan_dir`]).
///
/// # Errors
/// Returns an error if the directory cannot be read or any file read fails.
pub fn check_dir(dir: &Path) -> std::io::Result<DetCheckReport> {
    let mut sources: Vec<(String, String)> = Vec::new();
    collect_rs_sources(dir, &mut sources)?;
    Ok(scan_sources(&sources))
}

/// Check an explicit set of source paths (files and/or directories) for
/// determinism violations.
///
/// All paths are collected into a **single shared index** before scanning, so
/// every file is de-duplicated across arguments (the changed-files CI pattern
/// `det-check $(git diff --name-only '*.rs')`) and no file is scanned twice.
/// One-hop first-party helper reachability (issue #778) is nevertheless resolved
/// **same-module only** (same file + module path; see [`resolve_helper`]): a
/// cross-file transitive helper is deliberately NOT resolved (a safe
/// false-negative), because the line-based scanner cannot see the `use` import
/// that would make such a bare call reach across files — resolving it risks the
/// Codex round-4/round-5 false-positive family, which for a CI gate is the worse
/// outcome.
///
/// A file argument is scanned directly; a directory argument is walked
/// recursively with [`skip_scan_dir`] applied. Symlinked files and directories
/// are **not** followed — neither those discovered during a directory walk nor
/// one passed directly as a top-level path argument (classified with
/// `symlink_metadata`, which does not follow, and skipped with a warning). A
/// self-referential directory symlink cannot explode the scan, and a `../..`
/// symlink cannot pull out-of-tree files into the report.
/// Overlapping arguments (a directory and a file inside it, or a repeated path)
/// are de-duplicated by canonicalized path, so no file is scanned twice. A
/// single unreadable / non-UTF-8 `.rs` file discovered during a directory walk
/// is skipped with a warning naming the file rather than aborting the whole
/// scan.
///
/// # Errors
/// Returns an error if a top-level path does not exist or cannot be classified,
/// if a directory cannot be read, or if an explicitly-named file argument cannot
/// be read.
pub fn check_paths(paths: &[&Path]) -> std::io::Result<DetCheckReport> {
    let mut sources: Vec<(String, String)> = Vec::new();
    let mut seen: std::collections::HashSet<std::path::PathBuf> = std::collections::HashSet::new();
    for &path in paths {
        // Classify with `symlink_metadata` (which does NOT follow) so a
        // top-level symlink argument is skipped rather than followed — mirroring
        // the nested walker's no-follow behavior (#778, Codex r9). Following a
        // symlinked file would lint its target, and a symlinked directory would
        // walk out-of-tree contents into the report; both contradict the
        // documented no-follow contract. `metadata` (which follows) is still used
        // for the resolved file-vs-dir decision below, but only after confirming
        // the argument is not itself a symlink.
        let link_md = std::fs::symlink_metadata(path)?;
        if link_md.file_type().is_symlink() {
            eprintln!("det-check: skipping symlink {}", path.display());
            continue;
        }
        // Erroring on a missing top-level path (a nonexistent argument) is a
        // real usage error, distinct from a per-file read error mid-walk.
        if link_md.is_dir() {
            collect_rs_sources_robust(path, &mut sources, &mut seen)?;
        } else {
            add_source_strict(path, &mut sources, &mut seen)?;
        }
    }
    Ok(scan_sources(&sources))
}

/// Builds a single first-party function index across every collected file and
/// scans it once. The shared index de-duplicates and orders the files, but bare
/// helper calls resolve **same-module only** ([`resolve_helper`]): a workflow in
/// one file never resolves a helper defined in another.
fn scan_sources(sources: &[(String, String)]) -> DetCheckReport {
    let mut all_fns: Vec<FnDef> = Vec::new();
    for (label, content) in sources {
        let lines: Vec<&str> = content.lines().collect();
        let mut fns = extract_all_functions(&lines);
        for f in &mut fns {
            f.file.clone_from(label);
        }
        all_fns.extend(fns);
    }
    scan_functions(&all_fns)
}

/// Whether a directory should be skipped during a recursive scan: build
/// artifacts (`target`), hidden directories (any name starting with `.`, e.g.
/// `.git`), and trybuild compile-fail fixtures (`compile_fail`, which hold
/// deliberately-broken sources with committed `.stderr` snapshots — true
/// positives that exist to be rejected by the compile-time guardrail and must
/// never be linted or edited by the text pre-check).
fn skip_scan_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n == "target" || n == "compile_fail" || n.starts_with('.'))
}

fn collect_rs_sources(dir: &Path, sources: &mut Vec<(String, String)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if !skip_scan_dir(&path) {
                collect_rs_sources(&path, sources)?;
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            let content = std::fs::read_to_string(&path)?;
            sources.push((path.to_string_lossy().into_owned(), content));
        }
    }
    Ok(())
}

/// Symlink-safe, de-duplicating recursive `.rs` collector used by
/// [`check_paths`]. Symlinked entries are skipped (no cycles, no out-of-tree
/// escapes); `skip_scan_dir` directories are pruned; a per-file read/UTF-8 error
/// is warned-and-skipped rather than aborting the walk; each file is added at
/// most once, keyed by its canonicalized path.
fn collect_rs_sources_robust(
    dir: &Path,
    sources: &mut Vec<(String, String)>,
    seen: &mut std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // Do not follow symlinks — a directory symlink can form a cycle
        // (unbounded recursion) and a `../..` symlink can escape the tree.
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            if !skip_scan_dir(&path) {
                collect_rs_sources_robust(&path, sources, seen)?;
            }
        } else if file_type.is_file() && path.extension().is_some_and(|e| e == "rs") {
            add_source_skip(&path, sources, seen);
        }
    }
    Ok(())
}

/// Adds a discovered file's source, de-duplicated by canonicalized path. A
/// read/UTF-8 error skips the file with a warning (a non-UTF-8 file cannot be
/// valid Rust source anyway) rather than aborting the enclosing scan.
fn add_source_skip(
    path: &Path,
    sources: &mut Vec<(String, String)>,
    seen: &mut std::collections::HashSet<std::path::PathBuf>,
) {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if !seen.insert(canonical) {
        return;
    }
    match std::fs::read_to_string(path) {
        Ok(content) => sources.push((path.to_string_lossy().into_owned(), content)),
        Err(err) => eprintln!("det-check: skipping {}: {err}", path.display()),
    }
}

/// Adds an explicitly-named file argument, de-duplicated by canonicalized path.
/// Unlike [`add_source_skip`], a read error is propagated (an operator who names
/// a file directly should hear that it could not be read).
fn add_source_strict(
    path: &Path,
    sources: &mut Vec<(String, String)>,
    seen: &mut std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<()> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if !seen.insert(canonical) {
        return Ok(());
    }
    let content = std::fs::read_to_string(path)?;
    sources.push((path.to_string_lossy().into_owned(), content));
    Ok(())
}

// ── Function extraction & reachability (issue #778) ──────────────────────────

/// A top-level function definition captured from one source file. Bodies are
/// owned so a cross-file index can outlive the per-file line buffers.
struct FnDef {
    /// Function name.
    name: String,
    /// Identifiers bound by the parameter list (over-collected; see
    /// [`extract_fn_params`]). Used to shadow-suppress a call to a same-named
    /// fn-pointer / closure parameter so it is never resolved to a same-named
    /// free helper (#778 review, zero-FP).
    params: Vec<String>,
    /// `(1-indexed source line, text)` for every non-blank body line.
    body: Vec<(u32, String)>,
    /// Whether the function carries a `#[workflow]` attribute.
    is_workflow: bool,
    /// Whether the function carries an `#[activity]` attribute.
    is_activity: bool,
    /// File label the function was extracted from (set by the caller).
    file: String,
    /// `mod`-nesting path the function is declared in, joined by `::` (empty at
    /// file root). Used for Rust-accurate scope-aware resolution of a bare call
    /// to a same-named free helper (#778, Codex P1): the caller's own module
    /// sibling wins over an unrelated same-named helper elsewhere.
    module_path: String,
}

/// The (entry-point workflow, optional via-helper, file) a body is scanned under.
struct BodyScope<'a> {
    /// The `#[workflow]` function the finding is attributed to.
    entry_workflow: &'a str,
    /// For a transitive finding, the helper containing the offending call.
    via_helper: Option<&'a str>,
    /// File label for the location.
    file: &'a str,
}

/// A brace scope the extractor is currently nested inside. A `Module` brace is
/// transparent — its items are module scope and are scanned; every other brace
/// (`impl`/`trait`/`struct`/`fn` body / bare block) is `Opaque` and its contents
/// are skipped for item extraction (the #386 method-exclusion boundary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeKind {
    Module,
    Opaque,
}

/// Returns every **module-scope** function definition (top level and inside
/// `mod NAME { … }` blocks, at any nesting), with a flag for whether it carries
/// `#[workflow]` / `#[activity]`. Methods inside `impl`/`trait` blocks are
/// deliberately not captured, so they can never be mistaken for free-function
/// helper callees (#386 boundary). Module-nested `#[workflow]` entry points and
/// their module-scope helpers are scanned (issue #778, Codex P1).
fn extract_all_functions(lines: &[&str]) -> Vec<FnDef> {
    let mut result = Vec::new();
    // Brace scopes we are currently inside, innermost last. Once an `Opaque`
    // scope is entered it forms a contiguous suffix (an `impl`/`trait`/`fn`/block
    // never contains a module), so the innermost scope decides whether we are
    // item-scanning (`Module`/empty) or skipping (`Opaque`). `fn` bodies are
    // consumed wholesale by [`extract_fn_body`] and never pushed here.
    let mut scopes: Vec<ScopeKind> = Vec::new();
    // Names of the `mod NAME` blocks we are currently nested inside, maintained
    // in lockstep with the `Module` entries of `scopes` (#778, Codex P1).
    let mut mod_stack: Vec<String> = Vec::new();
    let mut pending_wf = false;
    let mut pending_act = false;
    // A `mod NAME` declaration whose opening `{` is on a later line — the next
    // `{` we open should be a transparent `Module` scope, not an `Opaque` one.
    let mut pending_mod = false;
    // The name of a `pending_mod` block, carried until its `{` is seen.
    let mut pending_mod_name: Option<String> = None;
    let mut i = 0;

    while i < lines.len() {
        // Inside an opaque scope: skip the line, only tracking brace balance so
        // we know when the opaque (and any nested) scope closes and item
        // scanning resumes. A closing `}` may pop past the opaque suffix into an
        // enclosing module on the same line, so `mod_stack` is maintained here too.
        if matches!(scopes.last(), Some(ScopeKind::Opaque)) {
            apply_line_braces_scoped(
                &strip_unparseable_content(lines[i]),
                &mut scopes,
                &mut mod_stack,
                false,
                None,
            );
            i += 1;
            continue;
        }

        let trimmed = lines[i].trim();

        // Accumulate the workflow/activity attribute state, keeping it across
        // intervening attributes, doc comments, and blank lines.
        if is_workflow_attr(trimmed) {
            pending_wf = true;
            i += 1;
            continue;
        }
        if is_activity_attr(trimmed) {
            pending_act = true;
            i += 1;
            continue;
        }
        if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
            i += 1;
            continue;
        }

        if let Some(name) = fn_decl_name(trimmed)
            && let Some((body, next_i)) = extract_fn_body(lines, i)
        {
            let params = extract_fn_params(lines, i);
            result.push(FnDef {
                name,
                params,
                body,
                is_workflow: pending_wf,
                is_activity: pending_act,
                file: String::new(),
                module_path: mod_stack.join("::"),
            });
            pending_wf = false;
            pending_act = false;
            pending_mod = false;
            pending_mod_name = None;
            i = next_i;
            continue;
        }

        // A module-scope line that is not a fn declaration — reset pending
        // attributes and classify its braces. The first `{` opened on a
        // `mod NAME { … }` line (or continuing a `pending_mod` from a prior line)
        // pushes a transparent `Module` scope so items inside keep being scanned;
        // every other `{` (`impl … {`, `struct … {`, a bare block) pushes an
        // `Opaque` scope whose contents are skipped.
        pending_wf = false;
        pending_act = false;
        let stripped = strip_unparseable_content(lines[i]);
        let trimmed_stripped = stripped.trim();
        // Resolve the module name for a `Module` scope this line may open: either
        // continuing a `pending_mod` from a prior line, or a fresh `mod NAME {`.
        let (mod_first, mod_name) = if pending_mod {
            (true, pending_mod_name.take())
        } else if is_mod_block_decl(trimmed_stripped) {
            (true, extract_mod_name(trimmed_stripped))
        } else {
            (false, None)
        };
        apply_line_braces_scoped(
            &stripped,
            &mut scopes,
            &mut mod_stack,
            mod_first,
            mod_name.as_deref(),
        );
        // A `mod NAME` line whose `{` has not appeared yet keeps the pending flag
        // (and its name) so the next line's `{` opens the module scope.
        if mod_first && !stripped.contains('{') {
            pending_mod = true;
            pending_mod_name = mod_name;
        } else {
            pending_mod = false;
            pending_mod_name = None;
        }
        i += 1;
    }

    result
}

/// Extracts the module name from an inline `mod NAME { … }` declaration line
/// (already whitespace-trimmed). Returns `None` if no identifier follows `mod`.
/// Assumes [`is_mod_block_decl`] has confirmed the line is an inline module.
fn extract_mod_name(trimmed: &str) -> Option<String> {
    let s = strip_leading_visibility(trimmed);
    let rest = s.strip_prefix("mod")?.trim_start();
    let name: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() { None } else { Some(name) }
}

/// Extracts a function body starting at `fn_idx` (the line carrying the `fn`
/// keyword). Returns the owned `(line, text)` body lines and the index just past
/// the closing brace, or `None` if no body brace is found.
fn extract_fn_body(lines: &[&str], fn_idx: usize) -> Option<(Vec<(u32, String)>, usize)> {
    // Scan forward for the opening `{` (the signature may span lines).
    let mut j = fn_idx;
    let brace_line = loop {
        if lines[j].contains('{') {
            break j;
        }
        j += 1;
        if j >= lines.len() {
            return None;
        }
    };

    let brace_pos = lines[brace_line].find('{').unwrap_or(0);
    let mut depth = 1u32;
    let mut body: Vec<(u32, String)> = Vec::new();

    j = brace_line;
    let mut on_brace_line = true;

    while j < lines.len() && depth > 0 {
        let line: &str = if on_brace_line {
            on_brace_line = false;
            &lines[j][brace_pos + 1..]
        } else {
            lines[j]
        };
        let line_num = u32::try_from(j + 1).unwrap_or(u32::MAX);

        if let Some(pos) = scan_braces_outside_literals(line, &mut depth) {
            let before = &line[..pos];
            if !before.trim().is_empty() {
                body.push((line_num, before.to_string()));
            }
        } else if !line.trim().is_empty() {
            body.push((line_num, line.to_string()));
        }

        j += 1;
    }

    Some((body, j))
}

/// Pushes/pops the scope stack for each brace on an already-literal-stripped
/// line, maintaining `mod_stack` in lockstep with the `Module` entries of
/// `scopes` (#778). The **first** `{` on the line opens a transparent
/// [`ScopeKind::Module`] scope (pushing `mod_name`) when `mod_first` is true (a
/// `mod NAME { … }` opener); every other `{` opens an [`ScopeKind::Opaque`]
/// scope. Each `}` pops the innermost scope, popping `mod_stack` only when the
/// popped scope was a `Module`. A `}` with no matching open (a stray close, or
/// the strip-caveat of a multi-line block comment) is a harmless no-op.
fn apply_line_braces_scoped(
    stripped: &str,
    scopes: &mut Vec<ScopeKind>,
    mod_stack: &mut Vec<String>,
    mut mod_first: bool,
    mod_name: Option<&str>,
) {
    for c in stripped.chars() {
        match c {
            '{' => {
                if mod_first {
                    scopes.push(ScopeKind::Module);
                    mod_stack.push(mod_name.unwrap_or("").to_string());
                    mod_first = false;
                } else {
                    scopes.push(ScopeKind::Opaque);
                }
            }
            '}' => {
                if matches!(scopes.pop(), Some(ScopeKind::Module)) {
                    mod_stack.pop();
                }
            }
            _ => {}
        }
    }
}

/// Whether `trimmed` (a module-scope line, already whitespace-trimmed) begins an
/// **inline** module declaration `mod NAME { … }` (which opens a transparent
/// scope) as opposed to an external module `mod NAME;` (no inline body) or a
/// non-module item. Only an optional leading visibility (`pub`, `pub(crate)`,
/// `pub(in …)`) is accepted before `mod`; the presence of a `;` before any body
/// brace marks an external module and returns `false`.
fn is_mod_block_decl(trimmed: &str) -> bool {
    let s = strip_leading_visibility(trimmed);
    let Some(rest) = s.strip_prefix("mod") else {
        return false;
    };
    // `mod` must be a whole keyword followed by whitespace, then an identifier.
    match rest.chars().next() {
        Some(c) if c.is_whitespace() => {}
        _ => return false,
    }
    let rest = rest.trim_start();
    // An external module (`mod foo;`) has a `;` before any body `{` — not inline.
    let body_at = rest.find('{');
    let semi_at = rest.find(';');
    if let Some(semi) = semi_at
        && body_at.is_none_or(|b| semi < b)
    {
        return false;
    }
    rest.chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_')
}

/// Strips an optional leading Rust visibility modifier (`pub`, `pub(crate)`,
/// `pub(super)`, `pub(in path)`) from a trimmed line, returning the remainder.
fn strip_leading_visibility(trimmed: &str) -> &str {
    let Some(rest) = trimmed.strip_prefix("pub") else {
        return trimmed;
    };
    match rest.chars().next() {
        Some('(') => rest
            .find(')')
            .map_or(trimmed, |close| rest[close + 1..].trim_start()),
        Some(c) if c.is_whitespace() => rest.trim_start(),
        // `public` / `pubfoo` — not a visibility modifier.
        _ => trimmed,
    }
}

/// Scans every extracted `#[workflow]` body directly, then follows one hop of
/// direct first-party helper calls into plain (non-workflow, non-activity)
/// helpers and scans those too, attributing findings to the workflow entry
/// point. Findings are deduped by `(rule_id, entry-workflow, file, line)`.
fn scan_functions(fns: &[FnDef]) -> DetCheckReport {
    use std::collections::HashMap;

    // Candidate helper index: only plain helpers are eligible callees. A name may
    // map to more than one plain helper (same name in different modules/files);
    // resolution of a bare call is deferred to [`resolve_helper`], which resolves
    // ONLY a candidate in the caller's own module (same file + same module path)
    // and treats every cross-module / cross-file / imported / aliased case as
    // ambiguous — so a resolution can never introduce a false positive (#778,
    // Codex r4/r5).
    let mut index: HashMap<&str, Vec<usize>> = HashMap::new();
    for (i, f) in fns.iter().enumerate() {
        if f.is_workflow || f.is_activity {
            continue;
        }
        index.entry(&f.name).or_default().push(i);
    }

    let mut report = DetCheckReport::default();
    for wf in fns.iter().filter(|f| f.is_workflow) {
        let view = body_view(&wf.body);
        let scope = BodyScope {
            entry_workflow: &wf.name,
            via_helper: None,
            file: &wf.file,
        };
        report.merge(check_body(&scope, &view));

        // Names locally bound in the workflow — parameters (fn-pointer /
        // closure params), body `let` bindings (closures, shadows), and
        // body-local `use` imports (incl. an unnameable glob) — are
        // shadow-suppressed so a call to one is never resolved to a same-named
        // free helper (#778 review, zero-FP; #386 excludes fn pointers/closures).
        // Suppression differs by binding kind, matching Rust scoping:
        //   * params bind for the whole body and always suppress;
        //   * a `let` shadow is **source-order + column-aware** — it suppresses a
        //     call on a strictly-later source line, or a same-line call past the
        //     binding's statement-terminating `;` (a call before the binding is
        //     in scope resolves to the free helper);
        //   * a body-local `use` import (and glob) shadows the name for the
        //     **whole body** — a Rust block `use` item is in scope for the
        //     entire enclosing block regardless of textual position, so a call
        //     may precede the `use` and still resolve to the import (#778,
        //     Codex r7). Only module/file-level `use` (outside the body) is
        //     never a body shadow.
        let params: std::collections::HashSet<String> = wf.params.iter().cloned().collect();
        let let_binding_lines = collect_body_let_binding_lines(&view);
        let (use_bindings, has_glob_use) = collect_body_use_bindings(&view);
        for callee in collect_direct_free_fn_calls(
            &view,
            &index,
            &params,
            &let_binding_lines,
            &use_bindings,
            has_glob_use,
        ) {
            if let Some(cands) = index.get(callee.as_str())
                && let Some(idx) = resolve_helper(cands, fns, wf)
            {
                let helper = &fns[idx];
                let hview = body_view(&helper.body);
                let hscope = BodyScope {
                    entry_workflow: &wf.name,
                    via_helper: Some(&helper.name),
                    file: &helper.file,
                };
                report.merge(check_body(&hscope, &hview));
            }
        }
    }

    dedupe_findings(&mut report.findings);
    report
}

/// Resolves a bare call to a first-party free helper from `caller`'s scope,
/// restricted to **same-module candidates only** (#778, Codex r4/r5 — ends the
/// cross-module false-positive family structurally):
///
/// - a candidate in the caller's **own module** (same `file` + same `module_path`)
///   resolves — the one case a bare call `name()` provably reaches without an
///   import the line-based scanner cannot see;
/// - **everything else** (cross-module, cross-file, `use`-imported, aliased via
///   `use ... as name`, and even a *globally-unique* helper that is not in the
///   caller's own module) is treated as ambiguous and NOT resolved — a safe
///   false-negative, never a false-positive.
///
/// This is a deliberate scope decision: the line-based front-door cannot resolve
/// `use` imports/aliases, so ANY cross-module resolution keeps producing the
/// Codex round-4 (same-file-different-module) and round-5 (aliased-import,
/// globally-unique) false-positive family. For a CI GATE a false positive
/// (failing CI on innocent code) is the worst outcome and outranks marginal
/// recall, so the earlier globally-unique (`[only]`) and same-file-different-module
/// fallbacks are removed. What is skipped here is a false-negative that the
/// body-only compile-time `#[workflow]` guardrail (#386) also misses; the backstop
/// for it is runtime determinism detection (`WorkflowReplayer` and the live
/// `HistoryMatcher`) plus manual review, not #386.
fn resolve_helper(candidates: &[usize], fns: &[FnDef], caller: &FnDef) -> Option<usize> {
    // Same module only (same file + same module path). Exactly one such candidate
    // resolves; two same-named helpers in one module is not valid Rust, and none
    // means the target is out of the caller's own module scope (cross-module,
    // cross-file, `use`-imported, or aliased) — treat as ambiguous and skip.
    let same_module: Vec<usize> = candidates
        .iter()
        .copied()
        .filter(|&i| fns[i].file == caller.file && fns[i].module_path == caller.module_path)
        .collect();
    if let [only] = same_module.as_slice() {
        return Some(*only);
    }
    None
}

/// Borrows an owned body as the `&[(u32, &str)]` view the per-body analysis expects.
fn body_view(body: &[(u32, String)]) -> Vec<(u32, &str)> {
    body.iter().map(|(n, s)| (*n, s.as_str())).collect()
}

/// Collects the names of first-party helpers a body calls via a **direct
/// free-function call** (`helper(`), excluding method calls (`x.helper(`),
/// path-qualified calls (`m::helper(`), and identifier-continuation. Only names
/// present in `candidates` and **not shadowed in scope at the call site** are
/// returned: a call to a name the caller binds itself (a fn-pointer/closure
/// parameter, a local closure, or a shadowing `let`) is never resolved to a
/// same-named free helper (#778 review, zero-FP; #386 excludes fn
/// pointers/closures).
///
/// Shadow suppression matches Rust scoping by binding kind (#778, Codex P2/r7/r9):
/// `params` bind for the whole body and always suppress; a `let` shadow (in
/// `let_binding_lines`, name → earliest [`LetBinding`] scope-entry point) is
/// **source-order + column-aware** — it suppresses a call on a strictly-later
/// source line, or on the SAME line only when the call's column is past the
/// binding's statement-terminating `;` (the point the binding enters scope). A
/// call that runs before the binding is in scope resolves to the free helper,
/// matching Rust — correct for `let helper = ...; helper();` (the same-line call
/// after the `;` is suppressed), for the RHS of `let bad = bad();` (before the
/// `;`, so flagged), and for a pre-shadow `bad(); let bad = ...;` (before the
/// binding, so flagged).
///
/// A **block-local `use` import** rebinds a name for the **whole body**, not
/// source-order: a Rust block `use` item is in scope for the entire enclosing
/// block regardless of textual position, so a bare call may PRECEDE the `use`
/// and still resolve to the import. Any name in `use_bindings` (from an explicit
/// / aliased / grouped body-local `use`) is therefore suppressed on every line,
/// and `has_glob_use` (an unnameable glob `use ...::*;` anywhere in the body)
/// conservatively suppresses every same-module call in the body — a call to such
/// a name is never resolved to a same-module free helper (#778, Codex r6/r7; the
/// module/file-level `use` is not a body line, so the common top-of-file prelude
/// case is unaffected).
///
/// The suppression is safe-direction: it only ever suppresses (never adds a
/// call), so it introduces no new false-positive. A `let` shadow bound in an
/// inner block that has already closed before an outer call is still treated as
/// in scope (source-order, not full brace-scope tracking); that over-suppresses
/// the outer call, an accepted false-negative — never a false-positive.
fn collect_direct_free_fn_calls(
    body: &[(u32, &str)],
    candidates: &std::collections::HashMap<&str, Vec<usize>>,
    params: &std::collections::HashSet<String>,
    let_binding_lines: &std::collections::HashMap<String, LetBinding>,
    use_bindings: &std::collections::HashSet<String>,
    has_glob_use: bool,
) -> std::collections::BTreeSet<String> {
    let mut called = std::collections::BTreeSet::new();
    for &(src_line, line) in body {
        let code = strip_unparseable_content(line);
        let bytes = code.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if !is_ident_byte(bytes[i]) {
                i += 1;
                continue;
            }
            let start = i;
            while i < bytes.len() && is_ident_byte(bytes[i]) {
                i += 1;
            }
            // Raw-identifier call: `r#gen(` tokenizes as `r`/`#`/`gen` under the
            // ident-byte scan (`#` is not an ident byte), so re-attach the `r#`
            // prefix and the following ident run — a call `r#gen(` then keys on
            // `r#gen`, matching a `fn r#gen` def (indexed under `r#gen`, Codex r4)
            // and completing raw-ident support on the CALL side (#778, Codex r6).
            // `start` still points at `r`, so the `.`/`:` before-guard below keeps
            // excluding `x.r#m()` / `Type::r#a()`, and the turbofish skip still
            // applies (`r#gen::<T>()`).
            if &code[start..i] == "r"
                && i + 1 < bytes.len()
                && bytes[i] == b'#'
                && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_')
            {
                i += 1; // consume `#`
                while i < bytes.len() && is_ident_byte(bytes[i]) {
                    i += 1;
                }
            }
            let token = &code[start..i];
            // Must start with a letter or `_` (not a number literal).
            let first_ok = token
                .as_bytes()
                .first()
                .is_some_and(|&b| b.is_ascii_alphabetic() || b == b'_');
            // Preceding boundary: not part of a longer ident, not a method call
            // (`.`) and not a path-qualified call (`:`).
            let before_ok = start == 0 || {
                let b = bytes[start - 1];
                !is_ident_byte(b) && b != b'.' && b != b':'
            };
            // Immediate call paren, allowing an optional turbofish `::<…>` between
            // the ident and the `(` — e.g. `bad::<u64>()` / `bad::<Vec<u8>>()`
            // (#778, Codex P2). The `.`/`:` before-guard above already excludes
            // method (`x.m::<T>()`) and associated (`Type::a::<T>()`) turbofish.
            let call_at = if code[i..].starts_with("::<") {
                skip_balanced_angles(bytes, i + 2)
            } else {
                Some(i)
            };
            let after_ok = matches!(call_at, Some(p) if p < bytes.len() && bytes[p] == b'(');
            // Shadowed in scope at this call site? A param shadows everywhere; a
            // `let` binding shadows a call on a strictly-later source line, or —
            // on the SAME line — a call whose column is past the binding's
            // statement-terminating `;` (the point the binding enters scope), so
            // `let helper = ...; helper();` suppresses the call while the RHS of
            // `let bad = bad();` and a pre-shadow `bad(); let bad = ...;` call
            // stay flagged (source-order + column-aware, #778 Codex P2/r9). A
            // block-local `use` import and a block-local glob `use ...::*;` shadow
            // the name across the WHOLE body — a Rust block `use` item is in scope
            // for the entire block regardless of position, so a call may precede
            // the `use` and still resolve to the import (#778, Codex r7).
            let shadowed_here = params.contains(token)
                || let_binding_lines.get(token).is_some_and(|b| {
                    b.line < src_line || (b.line == src_line && start > b.in_scope_col)
                })
                || use_bindings.contains(token)
                || has_glob_use;
            if first_ok && before_ok && after_ok && candidates.contains_key(token) && !shadowed_here
            {
                called.insert(token.to_string());
            }
        }
    }
    called
}

/// Given `open_lt` pointing at the `<` opening a turbofish (or generic) segment,
/// returns the byte index just past the matching balanced `>` (accounting for
/// nested generics like `::<Vec<u8>>`), or `None` if unbalanced. Used to skip an
/// optional turbofish between an ident and its call `(` (#778, Codex P2).
fn skip_balanced_angles(bytes: &[u8], open_lt: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut k = open_lt;
    while k < bytes.len() {
        match bytes[k] {
            b'<' => depth += 1,
            b'>' => {
                depth -= 1;
                if depth == 0 {
                    return Some(k + 1);
                }
            }
            _ => {}
        }
        k += 1;
    }
    None
}

/// Collects the identifiers bound by a function's parameter list, so a call to
/// a same-named local (a fn-pointer / closure parameter) is never resolved to a
/// same-named free helper (#778 review, zero-FP). The param list is the first
/// balanced `(...)` after the `fn` keyword (string/comment content is stripped
/// first); every lowercase-/underscore-initial ident inside it is collected via
/// [`det010_pattern_mask_idents`]. Over-collection (picking up a lowercase TYPE
/// ident like `fn`/`i64`) is safe: an extra shadow only ever suppresses a
/// resolution (a false-negative), never introduces a false-positive.
fn extract_fn_params(lines: &[&str], fn_idx: usize) -> Vec<String> {
    // Join signature lines (from the `fn` line) up to the body `{`.
    let mut sig = String::new();
    for raw in lines.iter().skip(fn_idx).take(64) {
        let stripped = strip_unparseable_content(raw);
        if let Some(brace) = stripped.find('{') {
            sig.push_str(&stripped[..brace]);
            break;
        }
        sig.push_str(&stripped);
        sig.push(' ');
    }
    let bytes = sig.as_bytes();
    let Some(open) = sig.find('(') else {
        return Vec::new();
    };
    // Match the param-list `(` to its `)`, accounting for nested parens
    // (e.g. a `fn(i64)->i64` param type).
    let mut depth: i32 = 0;
    let mut close = None;
    for (k, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(k);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close) = close else {
        return Vec::new();
    };
    det010_pattern_mask_idents(&sig[open + 1..close])
}

/// The scope-entry point of a `let` binding: the source line it is bound on and,
/// for that line, the byte column (in stripped-code coordinates) of the
/// statement-terminating `;` at which the binding comes into scope. `in_scope_col`
/// is [`usize::MAX`] when the `let` statement has no depth-0 `;` on its keyword
/// line (a multi-line `let`), so no same-line call is ever suppressed by it.
#[derive(Clone, Copy)]
struct LetBinding {
    /// Source line the `let` keyword is on.
    line: u32,
    /// Byte column of the statement-terminating `;` on that line (the point the
    /// binding enters scope), or [`usize::MAX`] for a multi-line `let`.
    in_scope_col: usize,
}

/// Collects the identifiers bound by `let` statements in a function body,
/// mapping each to the **earliest** point at which it enters scope (earliest
/// source line, and on that line the earliest statement-terminating `;`), so a
/// call to a same-named local (a closure binding, a shadowing `let`) is never
/// resolved to a same-named free helper (#778 review, zero-FP) — but only for
/// calls that come after the binding is in scope (source-order suppression, #778
/// Codex P2; column-aware within a line, #778 Codex r9). Reuses
/// [`det010_parse_let`] for robust pattern handling (simple, `mut`, annotated,
/// and destructuring / enum-variant patterns). Single-line handling only; a
/// multi-line `let` is a safe-direction under-collection.
fn collect_body_let_binding_lines(
    body: &[(u32, &str)],
) -> std::collections::HashMap<String, LetBinding> {
    let mut lines: std::collections::HashMap<String, LetBinding> = std::collections::HashMap::new();
    let mut record = |ident: String, binding: LetBinding| {
        lines
            .entry(ident)
            .and_modify(|b| {
                // Keep the earliest scope-entry point: earliest line, and on the
                // same line the earliest terminating `;` (earliest in-scope col).
                if (binding.line, binding.in_scope_col) < (b.line, b.in_scope_col) {
                    *b = binding;
                }
            })
            .or_insert(binding);
    };
    for &(src_line, line) in body {
        let code = strip_unparseable_content(line);
        for pos in word_positions(&code, "let") {
            let binding = LetBinding {
                line: src_line,
                in_scope_col: let_stmt_terminator_col(&code, pos),
            };
            match det010_parse_let(&code[pos + 3..]) {
                Det010LetBinding::Simple { ident, .. } => record(ident, binding),
                Det010LetBinding::PatternMask(ids) => {
                    for id in ids {
                        record(id, binding);
                    }
                }
            }
        }
    }
    lines
}

/// Returns the byte column of the depth-0 `;` that terminates the `let` statement
/// beginning at `let_pos` in already-stripped `code` (strings/comments removed by
/// [`strip_unparseable_content`], so a `;` inside a string cannot mislead it), or
/// [`usize::MAX`] if the statement is not terminated on this line (a multi-line
/// `let`). Depth tracks `()`/`[]`/`{}` so a `;` inside a closure block (e.g.
/// `let f = || { a(); b() };`) or a nested expression is skipped — the terminator
/// is the point at which the binding enters scope, so a same-line call before it
/// (the `let`'s own RHS, incl. a self-referencing `let x = x();`) is not
/// suppressed, while a same-line call after it is.
fn let_stmt_terminator_col(code: &str, let_pos: usize) -> usize {
    let bytes = code.as_bytes();
    let mut depth: i32 = 0;
    let mut k = let_pos;
    while k < bytes.len() {
        match bytes[k] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b';' if depth == 0 => return k,
            _ => {}
        }
        k += 1;
    }
    usize::MAX
}

/// Collects the set of names bound by **block-local `use` imports** anywhere in
/// a workflow body, plus whether the body contains any **glob** `use ...::*;`.
/// Unlike the `let` collector, this is presence-based (no source line): a Rust
/// block `use` item is in scope for the entire enclosing block regardless of
/// textual position, so a bare call may PRECEDE the `use` and still resolve to
/// the import — the shadow must apply to the WHOLE body, not just later lines
/// (#778, Codex r7). A glob names no specific binding, so `has_glob_use`
/// conservatively suppresses every same-module call in the body (safe-direction
/// over-suppression, never a false positive; block-local globs are rare).
///
/// A body-local `use crate::x::helper;` rebinds `helper` to a clean import, so
/// Rust resolves a bare `helper()` call to the import, not a same-module sibling
/// `fn helper` — the same-module resolver must not walk the sibling (#778,
/// Codex r6/r7). Only `use` statements INSIDE the body are seen (`extract_fn_body`
/// captures body lines only), so a module/file-level `use` — including the
/// common top-of-file prelude glob — is never a body shadow by construction.
///
/// Single-line handling only; a multi-line `use` is a safe-direction
/// under-collection (matching the `let` collector). A `use<'a>` precise-capture
/// clause (edition 2024 RPIT) is ignored — an import always has whitespace after
/// the `use` keyword.
fn collect_body_use_bindings(body: &[(u32, &str)]) -> (std::collections::HashSet<String>, bool) {
    let mut names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut has_glob = false;
    for &(_src_line, line) in body {
        let code = strip_unparseable_content(line);
        for pos in word_positions(&code, "use") {
            let after = &code[pos + 3..];
            // Require whitespace after `use` (an import is `use <path>`); this
            // excludes the `use<'a>` precise-capturing bound in RPIT.
            if !after.starts_with(char::is_whitespace) {
                continue;
            }
            let use_body = after.find(';').map_or(after, |end| &after[..end]);
            let (bound, is_glob) = parse_use_bindings(use_body);
            if is_glob {
                has_glob = true;
            }
            names.extend(bound);
        }
    }
    (names, has_glob)
}

/// Parses the text of a single `use` statement (everything after `use`, up to
/// but not including the `;`) into `(bound_names, is_glob)`. Handles the common
/// forms: `path::name` → binds `name`; `path as alias` / `path::orig as alias`
/// → binds `alias`; grouped `path::{a, b as c}` → binds `a` and `c`; glob
/// `path::*` → `is_glob = true` (no nameable binding). A raw-identifier segment
/// (`r#gen`) keeps its `r#` so it keys on the same string as a `fn r#gen` def
/// (#778, Codex r6). Conservative on anything it cannot confidently parse
/// (nested group, unnameable item) → `is_glob = true`, i.e. over-suppress
/// (safe-direction, never a false positive).
fn parse_use_bindings(use_body: &str) -> (Vec<String>, bool) {
    let s = use_body.trim();
    if s.is_empty() {
        return (Vec::new(), false);
    }
    // Grouped import: `path::{a, b as c}`.
    if let Some(open) = s.find('{') {
        let Some(close) = s.rfind('}') else {
            return (Vec::new(), true); // malformed / multi-line group → conservative
        };
        let inner = &s[open + 1..close];
        // A nested group is beyond this simple parser — over-suppress.
        if inner.contains('{') {
            return (Vec::new(), true);
        }
        let mut names = Vec::new();
        let mut glob = false;
        for item in inner.split(',') {
            let item = item.trim();
            if item.is_empty() || item == "self" {
                continue;
            }
            if item == "*" || item.ends_with("::*") {
                glob = true;
                continue;
            }
            match parse_use_single(item) {
                Some(name) => names.push(name),
                None => glob = true, // couldn't name it → conservative
            }
        }
        return (names, glob);
    }
    // Glob: `path::*` or bare `*`.
    if s == "*" || s.ends_with("::*") {
        return (Vec::new(), true);
    }
    // Simple or aliased single import.
    parse_use_single(s).map_or_else(|| (Vec::new(), false), |name| (vec![name], false))
}

/// Extracts the bound name of a single (non-grouped, non-glob) `use` item: the
/// alias after `as`, else the last `::`-separated path segment. Retains an `r#`
/// raw-identifier prefix (#778, Codex r6). Returns `None` if no valid identifier
/// is found (e.g. an `as _` throwaway).
fn parse_use_single(item: &str) -> Option<String> {
    let item = item.trim();
    let seg = item.rfind(" as ").map_or_else(
        || item.rsplit("::").next().unwrap_or(item).trim(),
        |idx| item[idx + 4..].trim(),
    );
    extract_use_ident(seg)
}

/// Reads a leading (optionally raw `r#`-prefixed) identifier from a `use`
/// segment, retaining the `r#` so it keys on the same string as a `fn r#name`
/// def (#778, Codex r6). Returns `None` for an empty ident or the `_` wildcard
/// binding (`use path as _;` binds no nameable local).
fn extract_use_ident(seg: &str) -> Option<String> {
    let seg = seg.trim();
    let raw = seg.starts_with("r#");
    let rest = if raw { &seg[2..] } else { seg };
    let ident: String = rest.chars().take_while(|&c| is_ident_char(c)).collect();
    if ident.is_empty() || ident == "_" {
        None
    } else if raw {
        Some(format!("r#{ident}"))
    } else {
        Some(ident)
    }
}

/// Removes duplicate findings, keeping one per
/// `(rule_id, entry-workflow, file, line)`.
fn dedupe_findings(findings: &mut Vec<DetFinding>) {
    let mut seen = std::collections::HashSet::new();
    findings.retain(|f| {
        let key = (
            f.rule_id,
            f.workflow_name.clone(),
            f.location.as_ref().map(|l| (l.file.clone(), l.line)),
        );
        seen.insert(key)
    });
}

/// Returns `true` for `#[workflow]` and `#[workflow(...)]` attribute lines.
fn is_workflow_attr(s: &str) -> bool {
    s == "#[workflow]" || s.starts_with("#[workflow(") || s.starts_with("#[workflow ]")
}

/// Returns `true` for `#[activity]` and `#[activity(...)]` attribute lines.
fn is_activity_attr(s: &str) -> bool {
    s == "#[activity]" || s.starts_with("#[activity(") || s.starts_with("#[activity ]")
}

/// Extracts the function name from a top-level function declaration line such as
/// `pub(crate) async fn my_wf(` after stripping visibility and fn qualifiers.
/// Returns `None` when the line is not a function definition.
fn fn_decl_name(trimmed: &str) -> Option<String> {
    let mut s = trimmed;

    // Optional visibility: `pub`, `pub(crate)`, `pub(super)`, …
    if let Some(rest) = s.strip_prefix("pub") {
        match rest.chars().next() {
            Some('(') => {
                let close = rest.find(')')?;
                s = rest[close + 1..].trim_start();
            }
            Some(c) if c.is_whitespace() => s = rest.trim_start(),
            _ => {} // `public`/`pubfoo` — not a visibility modifier.
        }
    }

    // Fn qualifiers, in any order, plus an optional `extern "ABI"`.
    loop {
        let mut advanced = false;
        for kw in ["const ", "async ", "unsafe ", "extern "] {
            if let Some(rest) = s.strip_prefix(kw) {
                s = rest.trim_start();
                advanced = true;
            }
        }
        if s.starts_with('"')
            && let Some(end) = s[1..].find('"')
        {
            s = s[end + 2..].trim_start();
            advanced = true;
        }
        if !advanced {
            break;
        }
    }

    let after = s.strip_prefix("fn")?;
    if !after.starts_with(char::is_whitespace) {
        return None;
    }
    let after = after.trim_start();
    // Optional raw-identifier prefix (`r#gen`, `r#type`, …) — a valid Rust fn name
    // whose identifier may itself be a reserved keyword (`gen` is reserved in
    // edition 2024). Keep the `r#` on the extracted name so a def and any
    // `r#name(` call site key on the same string (#778, Codex r4).
    let raw_prefix_len = if after.starts_with("r#") { 2 } else { 0 };
    let ident: String = after[raw_prefix_len..]
        .chars()
        .take_while(|c| is_ident_char(*c))
        .collect();
    if ident.is_empty() {
        return None;
    }
    let name = format!("{}{ident}", &after[..raw_prefix_len]);
    // Sanity: the name must be followed by `(`, `<` (generics), or nothing.
    let tail = after[name.len()..].trim_start();
    if tail.is_empty() || tail.starts_with('(') || tail.starts_with('<') {
        Some(name)
    } else {
        None
    }
}

// ── Body checker ──────────────────────────────────────────────────────────────

fn check_body(scope: &BodyScope, body_lines: &[(u32, &str)]) -> DetCheckReport {
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
                } else if DET011_MACRO_PATTERNS.contains(&pattern) {
                    matches_select_macro_pattern(&code_part, pattern)
                } else {
                    code_part.contains(pattern)
                };
                if !matched {
                    continue;
                }

                // Best-effort 1-based column of the matched pattern in the raw line.
                let col = det_col(line, pattern);

                // Pattern matched — check for a suppression comment.
                if let Some(reason) = find_suppression(rule.id, line, prev_line) {
                    report.suppressions.push(DetSuppression {
                        rule_id: rule.id.to_string(),
                        reason,
                        location: DetLocation {
                            file: scope.file.to_string(),
                            line: source_line,
                            col,
                        },
                    });
                } else {
                    report.findings.push(DetFinding {
                        rule_id: rule.id,
                        severity: rule.severity.clone(),
                        workflow_name: Some(scope.entry_workflow.to_string()),
                        location: Some(DetLocation {
                            file: scope.file.to_string(),
                            line: source_line,
                            col,
                        }),
                        message: format!(
                            "[{}] {} (matched pattern: `{}`)",
                            rule.id, rule.message, pattern
                        ),
                        alternative: rule.alternative,
                        via_helper: scope.via_helper.map(str::to_string),
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
    report.merge(check_hash_iteration(scope, body_lines));

    report
}

/// Best-effort 1-based **column** of `pattern` within the raw source `line`.
/// The pattern is located in the raw line (not the stripped form) so the column
/// aligns with what an operator sees; a byte offset is converted to a 1-based
/// character column. Falls back to `1` when the pattern is not found in the raw
/// line (e.g. it only survives in the stripped form). Always `> 0`.
fn det_col(line: &str, pattern: &str) -> u32 {
    line.find(pattern).map_or(1, |byte| {
        u32::try_from(line[..byte].chars().count() + 1).unwrap_or(1)
    })
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
fn check_hash_iteration(scope_ctx: &BodyScope, body_lines: &[(u32, &str)]) -> DetCheckReport {
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
                        scope_ctx,
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
    scope: &BodyScope,
    source_line: u32,
    (raw_line, prev_line): (&str, &str),
    ident: &str,
    severity: DetSeverity,
) {
    // The bespoke DET010 pass has no per-match byte offset, so it reports col 1.
    let col = 1;
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
                file: scope.file.to_string(),
                line: source_line,
                col,
            },
        });
        return;
    }

    report.findings.push(DetFinding {
        rule_id: DET010_ID,
        severity,
        workflow_name: Some(scope.entry_workflow.to_string()),
        location: Some(DetLocation {
            file: scope.file.to_string(),
            line: source_line,
            col,
        }),
        message: format!("[{DET010_ID}] {DET010_MESSAGE} (iterated hash-typed binding: `{ident}`)"),
        alternative: DET010_ALTERNATIVE,
        via_helper: scope.via_helper.map(str::to_string),
    });
}

const fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// DET011 candidate patterns for the futures combinator FUNCTIONS. Each is the
/// bare combinator NAME (no trailing `(`) — a cheap substring locator; the
/// accept/reject decision is made by the path-precise
/// [`matches_futures_combinator_pattern`], which confirms the name is a complete
/// token, requires a call `(` (skipping an optional turbofish and whitespace),
/// and extracts the full qualified path ending at the call to match it exactly
/// against the allowed set. This keeps `det_check` in lock-step with the
/// compile-time HVG010 guardrail's `is_select_combinator_path`, which matches
/// whole paths exactly after `syn` has already stripped any turbofish
/// (#980 Codex P2 review).
const DET011_COMBINATOR_FN_PATTERNS: &[&str] =
    &["future::select", "select_all", "select_ok", "try_select"];

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

/// True if a candidate combinator `pattern` (a bare combinator NAME, e.g.
/// `future::select` or `select_all`) occurs in `code` as a genuine futures
/// combinator call. For each occurrence of `pattern` the check is:
///
/// 1. The name must be a complete token — the byte immediately after it must
///    not be an identifier byte, so `future::select` inside `future::selected`
///    / `future::select_all`, or `select_all` inside `select_all_flag`, is not
///    a false hit (the longer name is caught by its own anchor).
/// 2. A call `(` must follow once an OPTIONAL turbofish `::<…>` and any
///    surrounding whitespace are skipped (see [`call_paren_follows`]). This is
///    what makes `future::select::<_, _>(a, b)` and `select_all::<Vec<_>>(v)`
///    match — the syn-based HVG010 macro lint strips such path arguments before
///    matching, so `det_check` must tolerate them too (#980 Codex P2).
/// 3. The full qualified path ending at the name is extracted by walking
///    backward over the contiguous run of path bytes (identifier bytes and
///    `:`), then matched exactly against [`is_allowed_combinator_path`] after a
///    single optional leading `::` is stripped (so an absolute path like
///    `::futures::future::select` matches, mirroring the macro lint's
///    `path_to_string`, which ignores `leading_colon`). A path run immediately
///    preceded by `.` is a method call and is rejected.
///
/// This is path-precise, so an unrelated helper of the same tail name under a
/// different root (`crate::future::select`, `my_dsl::select_all`,
/// `::my_dsl::select_all`) is never flagged — mirroring the macro lint's
/// exact-path matching — with or without a leading `::` or a turbofish.
fn matches_futures_combinator_pattern(code: &str, pattern: &str) -> bool {
    let bytes = code.as_bytes();
    let name_len = pattern.len();
    code.match_indices(pattern).any(|(pos, _)| {
        // `name_end` is the byte index just past the last char of the name.
        let name_end = pos + name_len;
        // (1) The name must be a complete token, not a prefix of a longer one.
        if name_end < bytes.len() && is_ident_byte(bytes[name_end]) {
            return false;
        }
        // (2) A call `(` must follow (after an optional turbofish + whitespace).
        if !call_paren_follows(bytes, name_end) {
            return false;
        }
        // (3) Extract the full qualified path ending at the name (walking back
        // over the contiguous path run and stripping an optional leading `::`),
        // and match it exactly against the allowed set. A `.`-preceded run is a
        // method call → `None` → rejected.
        extract_qualified_path(code, pos, name_end).is_some_and(is_allowed_combinator_path)
    })
}

/// Walk backward from `name_start` over the contiguous path run (identifier
/// bytes and `:`) and return the full qualified path from the run start through
/// `name_end`, with a single optional leading `::` stripped (so an absolute
/// path like `::tokio::select` normalizes to `tokio::select`, mirroring the
/// syn-based HVG010 macro lint's `path_to_string`, which ignores
/// `leading_colon`). Returns `None` when the run is immediately preceded by a
/// `.` — a method call, which is never a qualified path/macro invocation.
///
/// Shared by the DET011 combinator-FUNCTION matcher
/// ([`matches_futures_combinator_pattern`]) and the DET011 select-MACRO matcher
/// ([`matches_select_macro_pattern`]) so both extract the path identically. The
/// caller supplies `name_start`/`name_end` — for a function the whole pattern
/// (`future::select`) is the tail run; for a macro the `!` is excluded from
/// `name_end` so only the macro PATH (`tokio::select`) is returned.
fn extract_qualified_path(code: &str, name_start: usize, name_end: usize) -> Option<&str> {
    let bytes = code.as_bytes();
    let mut run_start = name_start;
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
        return None;
    }
    let path = &code[run_start..name_end];
    Some(path.strip_prefix("::").unwrap_or(path))
}

/// From byte index `after_name` (just past a combinator name token), returns
/// `true` if a call `(` follows once an OPTIONAL turbofish `::<…>` and any
/// surrounding whitespace are skipped. The turbofish's angle brackets are
/// matched with a balanced-depth scan (their contents may contain nested `<>`,
/// commas, paths, and `_`); the scan is bounded by `bytes.len()`, so it cannot
/// run away, and an unbalanced group is rejected rather than treated as a call.
///
/// Recognized shapes (all → `true`):
/// `name(`, `name (`, `name::<T>(`, `name::<_, _> (`, `name::<Vec<Box<T>>>(`.
/// Anything else after the name (`.`, `::foo`, `,`, end of segment) → `false`.
fn call_paren_follows(bytes: &[u8], after_name: usize) -> bool {
    let mut i = after_name;
    // Optional whitespace before an (optional) turbofish.
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    // Optional turbofish `::<…>`.
    if i + 2 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' && bytes[i + 2] == b'<' {
        i += 3; // skip `::<`, entering the angle-bracket group at depth 1.
        let mut depth: u32 = 1;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'<' => depth += 1,
                b'>' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        if depth != 0 {
            return false; // unbalanced angle brackets — not a turbofish call.
        }
    }
    // Optional whitespace after the turbofish (or the name).
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    // Require the call `(`.
    i < bytes.len() && bytes[i] == b'('
}

/// DET011 candidate patterns for the select MACROS. Each carries a trailing
/// `!` (so the macro NAME is a complete token bounded on the right by the bang,
/// which keeps `select_biasedx!` and a `select` prefix inside `select_biased!`
/// from matching); the accept/reject decision is made by the path-precise
/// [`matches_select_macro_pattern`], which extracts the full qualified macro
/// path and matches it EXACTLY against [`is_allowed_select_macro_path`]. This
/// keeps `det_check` in lock-step with the compile-time HVG010 guardrail's
/// `visit_macro`, which matches macro paths exactly (#799 / #980 Codex P2).
const DET011_MACRO_PATTERNS: &[&str] = &["select!", "select_biased!"];

/// The exact set of paths that name a select MACRO flagged by HVG010.
///
/// This mirrors, case-for-case, the allowed-path set in the macro crate's
/// `determinism_lint.rs::visit_macro`: bare `select`/`select_biased`,
/// `tokio::select`, `futures::select`, and `futures::select_biased`. Note that
/// `tokio::select_biased` is deliberately ABSENT — it is not in `visit_macro`'s
/// set either. A qualified path with any OTHER root (`crate::ui::select`,
/// `my::select_biased`, `foo::bar::select`) is absent, so those are never
/// flagged, exactly as the compile-time guardrail leaves them un-flagged
/// (#980 Codex P2 — the qualified-macro-path false-positive fix).
fn is_allowed_select_macro_path(path: &str) -> bool {
    matches!(
        path,
        "select" | "select_biased" | "tokio::select" | "futures::select" | "futures::select_biased"
    )
}

/// True if a select-macro `pattern` (`select!` or `select_biased!`, trailing
/// `!` included) occurs in `code` as an invocation whose FULL qualified macro
/// path is in the allowed set. For each occurrence the check is:
///
/// 1. The trailing `!` in `pattern` guarantees the macro name is a complete
///    token on the right (`select_biasedx!` and a `select!` match inside
///    `select_biased!` are excluded by `match_indices` itself).
/// 2. The full qualified path ending at the macro name (the `!` excluded) is
///    extracted by [`extract_qualified_path`] — walking backward over the
///    contiguous path run and stripping an optional leading `::` — then matched
///    exactly against [`is_allowed_select_macro_path`]. A `.`-preceded run
///    (`None`) is rejected.
///
/// This is path-precise, so an unrelated macro of the same tail name under a
/// different root (`crate::ui::select!`, `my::select_biased!`,
/// `foo::bar::select!`) is never flagged — mirroring `visit_macro`'s
/// exact-path matching — while `::tokio::select!` normalizes to `tokio::select`
/// and IS flagged. `sql_select!` / `my_select!` are absorbed into a
/// non-matching path (`sql_select` / `my_select`) by the backward walk and stay
/// unflagged.
fn matches_select_macro_pattern(code: &str, pattern: &str) -> bool {
    // `pattern` ends with `!`; exclude it so only the macro PATH is extracted.
    let name_end_offset = pattern.len() - 1;
    code.match_indices(pattern).any(|(pos, _)| {
        extract_qualified_path(code, pos, pos + name_end_offset)
            .is_some_and(is_allowed_select_macro_path)
    })
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
    fn fn_decl_name_handles_visibility_and_async() {
        assert_eq!(fn_decl_name("async fn my_wf("), Some("my_wf".to_string()));
        assert_eq!(
            fn_decl_name("pub async fn billing("),
            Some("billing".to_string())
        );
        assert_eq!(
            fn_decl_name("pub(crate) async fn inner("),
            Some("inner".to_string())
        );
        assert_eq!(
            fn_decl_name("const fn helper() -> i64 {"),
            Some("helper".to_string())
        );
        assert_eq!(
            fn_decl_name("fn plain<T>(x: T) {"),
            Some("plain".to_string())
        );
        // Raw identifiers — the `r#` prefix is retained on the extracted name
        // (#778, Codex r4).
        assert_eq!(fn_decl_name("async fn r#gen("), Some("r#gen".to_string()));
        assert_eq!(
            fn_decl_name("pub fn r#type() -> i64 {"),
            Some("r#type".to_string())
        );
        assert_eq!(fn_decl_name("let x = 1;"), None);
        assert_eq!(fn_decl_name("impl Foo {"), None);
        assert_eq!(fn_decl_name("struct Bar {"), None);
    }

    #[test]
    fn is_activity_attr_matches_bare_and_parameterised() {
        assert!(is_activity_attr("#[activity]"));
        assert!(is_activity_attr("#[activity(start_to_close = \"30s\")]"));
        assert!(!is_activity_attr("#[workflow]"));
    }

    #[test]
    fn is_mod_block_decl_matches_inline_modules_only() {
        // Inline module blocks (transparent scopes).
        assert!(is_mod_block_decl("mod workflows {"));
        assert!(is_mod_block_decl("pub mod workflows {"));
        assert!(is_mod_block_decl("pub(crate) mod workflows {"));
        assert!(is_mod_block_decl("pub(in crate::a) mod m {"));
        // Brace on a later line — still an inline module opener.
        assert!(is_mod_block_decl("mod workflows"));
        // External modules (`mod foo;`) have no inline body.
        assert!(!is_mod_block_decl("mod external;"));
        assert!(!is_mod_block_decl("pub mod external;"));
        // Non-module items / statements.
        assert!(!is_mod_block_decl("impl Foo {"));
        assert!(!is_mod_block_decl("struct S {"));
        assert!(!is_mod_block_decl("fn helper() {"));
        assert!(!is_mod_block_decl("let model = 1;"));
        assert!(!is_mod_block_decl("modify(x);"));
    }

    #[test]
    fn strip_leading_visibility_handles_pub_forms() {
        assert_eq!(strip_leading_visibility("pub mod m"), "mod m");
        assert_eq!(strip_leading_visibility("pub(crate) fn f()"), "fn f()");
        assert_eq!(strip_leading_visibility("pub(in crate::x) mod m"), "mod m");
        // No visibility modifier — unchanged.
        assert_eq!(strip_leading_visibility("mod m"), "mod m");
        // `public`/`pubfoo` are not visibility modifiers.
        assert_eq!(strip_leading_visibility("public_fn()"), "public_fn()");
    }

    #[test]
    fn apply_line_braces_classifies_module_vs_opaque() {
        let mut scopes: Vec<ScopeKind> = Vec::new();
        let mut mods: Vec<String> = Vec::new();
        // `mod NAME {` opens a transparent module scope, pushing its name.
        apply_line_braces_scoped(
            "mod workflows {",
            &mut scopes,
            &mut mods,
            true,
            Some("workflows"),
        );
        assert_eq!(scopes, vec![ScopeKind::Module]);
        assert_eq!(mods, vec!["workflows".to_string()]);
        // A nested `impl` opens an opaque scope (no module name).
        apply_line_braces_scoped("impl Foo {", &mut scopes, &mut mods, false, None);
        assert_eq!(scopes, vec![ScopeKind::Module, ScopeKind::Opaque]);
        assert_eq!(mods, vec!["workflows".to_string()]);
        // Closing the impl pops back to module scope (mod stack unchanged).
        apply_line_braces_scoped("}", &mut scopes, &mut mods, false, None);
        assert_eq!(scopes, vec![ScopeKind::Module]);
        assert_eq!(mods, vec!["workflows".to_string()]);
        // Closing the module empties both stacks.
        apply_line_braces_scoped("}", &mut scopes, &mut mods, false, None);
        assert!(scopes.is_empty());
        assert!(mods.is_empty());
    }

    #[test]
    fn apply_line_braces_scoped_pops_module_when_impl_and_mod_close_on_one_line() {
        // `} }` closing an impl (opaque) then the enclosing module on one line —
        // the mod stack must pop exactly once (for the Module), not for the impl.
        let mut scopes = vec![ScopeKind::Module, ScopeKind::Opaque];
        let mut mods = vec!["outer".to_string()];
        apply_line_braces_scoped("} }", &mut scopes, &mut mods, false, None);
        assert!(scopes.is_empty());
        assert!(mods.is_empty());
    }

    #[test]
    fn extract_mod_name_reads_the_module_identifier() {
        assert_eq!(
            extract_mod_name("mod workflows {"),
            Some("workflows".into())
        );
        assert_eq!(extract_mod_name("pub mod a {"), Some("a".into()));
        assert_eq!(
            extract_mod_name("pub(crate) mod inner_2 {"),
            Some("inner_2".into())
        );
        // No identifier after `mod` → None.
        assert_eq!(extract_mod_name("mod {"), None);
    }

    #[test]
    fn skip_balanced_angles_matches_nested_generics() {
        // `::<u64>(` — open `<` at index 2, close `>` at index 6, `(` follows.
        let s = b"::<u64>(";
        assert_eq!(skip_balanced_angles(s, 2), Some(7));
        assert_eq!(s[7], b'(');
        // Nested `::<Vec<u8>>(` — the outer `>` is the second-to-last.
        let s = b"::<Vec<u8>>(";
        let end = skip_balanced_angles(s, 2).unwrap();
        assert_eq!(s[end], b'(');
        // Unbalanced → None.
        assert_eq!(skip_balanced_angles(b"::<u64", 2), None);
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
