//! `harvest debug` — the time-travel replay debugger's terminal front end
//! (issue #949).
//!
//! # What this command can and cannot do
//!
//! The shipped `harvest` binary is statically linked and **cannot register your
//! application's `#[workflow]` handler functions**. This is the same constraint
//! `harvest-replay` already states ("copy this file into your application, add
//! your handlers to `build_replayer()`"), and it splits issue #949's surface
//! cleanly in two:
//!
//! | Capability | Needs your code? | Where |
//! |---|---|---|
//! | Step through a history; inspect open awaitables, version/patch gates, side effects, markers | **no** | this command |
//! | Run to a breakpoint (event type / index / activity / signal) | **no** | this command |
//! | Diff **two fixture histories** | **no** | `harvest debug diff` |
//! | Per-step pending `WorkflowCommand`s | yes | [`autumn_harvest::debugger::ReplayDebugger`] |
//! | Diff **two workflow-code registrations** | yes | [`autumn_harvest::debugger::diff_traces`] |
//!
//! Both arms produce the same [`ReplayTrace`] type, so a snippet that starts in
//! this command graduates to the library without reshaping anything.
//!
//! Everything here is **local and read-only**: no HTTP, no database, no
//! activity execution.

use std::fmt::Write as _;
use std::path::Path;

use autumn_harvest::debugger::{
    Breakpoint, DebugStep, DiffKind, ReplayTrace, StepOutcome, TraceDiff, diff_traces,
    is_redacted_export,
};
use autumn_harvest::testing::HistorySnapshot;

use crate::CliError;

/// How `harvest debug` renders its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum DebugFormat {
    /// Human-readable, richly formatted terminal output (default).
    #[default]
    Text,
    /// Machine-readable JSON (the full `ReplayTrace` / `TraceDiff`).
    Json,
}

/// Load and parse an exported history snapshot from disk.
///
/// AC4: the input is the `HistorySnapshot` JSON round-trip format written by
/// `harvest history export`, so a production history is debugged locally with
/// zero database access.
fn load_snapshot(path: &Path) -> Result<HistorySnapshot, CliError> {
    let text = std::fs::read_to_string(path).map_err(|e| CliError::ReadJson {
        label: "history snapshot",
        path: path.display().to_string(),
        source: e,
    })?;
    // A redacted export rewrites every payload field in place. The
    // handler-free CLI trace only *displays* payloads, so this degrades rather
    // than fabricates — but the same file handed to `ReplayDebugger` would
    // invent a divergence at the first payload-bearing activity, so warn here
    // and refuse there.
    if is_redacted_export(&text) {
        eprintln!(
            "warning: this history was exported with redacted payloads; payload \
             fields will display as redaction stubs. Re-export with \
             `harvest history export <ID> --payload-policy full` for real values."
        );
    }
    serde_json::from_str(&text).map_err(|e| CliError::InvalidJson {
        label: "history snapshot",
        source: e,
    })
}

/// Resolve the at-most-one breakpoint flag the user supplied.
///
/// Returns `None` when no breakpoint was requested. The flags are declared
/// mutually exclusive at the clap layer, so at most one can be `Some` here.
#[must_use]
pub fn resolve_breakpoint(
    event_type: Option<&str>,
    index: Option<usize>,
    activity: Option<&str>,
    signal: Option<&str>,
) -> Option<Breakpoint> {
    event_type
        .map(|t| Breakpoint::EventType(t.to_string()))
        .or_else(|| index.map(Breakpoint::EventIndex))
        .or_else(|| activity.map(|a| Breakpoint::ActivityName(a.to_string())))
        .or_else(|| signal.map(|s| Breakpoint::SignalName(s.to_string())))
}

/// The step the session should land on: the breakpoint hit if one was
/// requested and matched, else the explicit `--step`, else the first step.
///
/// Pure so the resolution order is unit-testable without a terminal.
#[must_use]
pub fn resolve_focus(
    trace: &ReplayTrace,
    breakpoint: Option<&Breakpoint>,
    step: Option<usize>,
) -> FocusResolution {
    if let Some(bp) = breakpoint {
        return trace
            .find_breakpoint(bp, 0)
            .map_or(FocusResolution::BreakpointMissed, |index| {
                FocusResolution::BreakpointHit { index }
            });
    }
    match step {
        Some(index) if index < trace.steps.len() => FocusResolution::Step { index },
        Some(index) => FocusResolution::OutOfRange {
            index,
            len: trace.steps.len(),
        },
        None => FocusResolution::Whole,
    }
}

/// Where [`resolve_focus`] decided the session should land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusResolution {
    /// No breakpoint and no `--step`: render the whole trace.
    Whole,
    /// A breakpoint matched at this step index.
    BreakpointHit {
        /// The matched step index.
        index: usize,
    },
    /// A breakpoint was requested but no step matched.
    BreakpointMissed,
    /// An explicit `--step` index within range.
    Step {
        /// The requested step index.
        index: usize,
    },
    /// An explicit `--step` index past the end of the trace.
    OutOfRange {
        /// The requested step index.
        index: usize,
        /// How many steps the trace actually has.
        len: usize,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Rendering
// ─────────────────────────────────────────────────────────────────────────────

/// Render one line per step: index, event type, and an open-awaitable count.
#[must_use]
pub fn render_trace_overview(trace: &ReplayTrace) -> String {
    let mut out = format!(
        "workflow: {}\nexecution: {}\nevents: {}{}\n\n",
        trace.workflow_name,
        trace.execution_id,
        trace.total_events,
        if trace.truncated {
            format!(" (trace capped at {} steps)", trace.steps.len())
        } else {
            String::new()
        }
    );
    out.push_str("  idx  event                             open  detail\n");
    out.push_str("  ---  --------------------------------  ----  ------\n");
    for step in &trace.steps {
        let _ = writeln!(
            out,
            "  {:>3}  {:<32}  {:>4}  {}",
            step.index,
            step.event_type,
            step.open_awaitables.len(),
            step_headline(step),
        );
    }
    out
}

/// A one-line "what happened here" for the overview table.
fn step_headline(step: &DebugStep) -> String {
    if let Some(div) = &step.divergence {
        return format!("DIVERGED: expected {}, got {}", div.expected, div.actual);
    }
    if let Some(name) = &step.signal_name {
        return format!("signal {name}");
    }
    step.open_awaitables
        .iter()
        .find(|a| a.opened_at == step.index)
        .map_or_else(
            || {
                step.commands
                    .first()
                    .map_or_else(String::new, |c| format!("-> {c}"))
            },
            |opened| {
                format!(
                    "opens {} {}",
                    opened.kind.as_str(),
                    opened
                        .name
                        .as_deref()
                        .or(opened.id.as_deref())
                        .unwrap_or("<unnamed>")
                )
            },
        )
}

/// Render the full inspection view of a single step (AC2's "inspect the
/// current snapshot").
#[must_use]
pub fn render_step_detail(trace: &ReplayTrace, index: usize) -> String {
    let Some(step) = trace.step(index) else {
        return format!(
            "step {index} is out of range (trace has {} steps)\n",
            trace.steps.len()
        );
    };

    // A capped trace must never read as a complete one: `step 1/1` for a
    // 3-event history looks like the last event.
    let cap_note = if trace.truncated {
        format!(
            " (trace capped at {} of {} events)",
            trace.steps.len(),
            trace.total_events
        )
    } else {
        String::new()
    };
    let mut out = format!(
        "step {}/{}  event {}  [{}]{cap_note}\n",
        step.index,
        trace.steps.len().saturating_sub(1),
        step.event_type,
        step.outcome.as_str(),
    );

    // The handler-free CLI trace carries no commands. Absence of a "pending
    // commands" section would otherwise read as "this step has none".
    if step.outcome == StepOutcome::NotReplayed {
        out.push_str(
            "  pending commands: <unavailable — no workflow handler registered; \
             use the library API, see docs/replay-debugger.md>\n",
        );
    }

    if let Some(div) = &step.divergence {
        let _ = writeln!(
            out,
            "\n  !! DIVERGENCE at event {}\n     expected: {}\n     actual:   {}",
            div.event_index, div.expected, div.actual
        );
    }

    out.push_str(&section("pending commands", &step.commands, |c| {
        c.to_string()
    }));
    out.push_str(&section("open awaitables", &step.open_awaitables, |a| {
        format!(
            "{:<16} {:<28} opened at event {}",
            a.kind.as_str(),
            a.name.as_deref().or(a.id.as_deref()).unwrap_or("<unnamed>"),
            a.opened_at
        )
    }));
    out.push_str(&section("side effects", &step.side_effects, |s| {
        format!(
            "{:<8} {:<20} {}",
            s.kind,
            s.name.as_deref().unwrap_or("-"),
            compact(&s.value)
        )
    }));
    out.push_str(&section("markers", &step.markers, |m| {
        format!("{:<28} {}", m.name, compact(&m.details))
    }));

    if !step.version_gates.is_empty() {
        out.push_str("\n  version gates:\n");
        for (change_id, version) in &step.version_gates {
            let _ = writeln!(out, "    {change_id} = {version}");
        }
    }
    if !step.patch_gates.is_empty() {
        out.push_str("\n  patch gates:\n");
        for patch_id in &step.patch_gates {
            let _ = writeln!(out, "    {patch_id}");
        }
    }
    if let Some(payload) = &step.resolved_payload {
        let _ = writeln!(out, "\n  resolved payload: {}", compact(payload));
    }
    out
}

/// Render a titled list section, or nothing when the list is empty.
fn section<T>(title: &str, items: &[T], render: impl Fn(&T) -> String) -> String {
    if items.is_empty() {
        return String::new();
    }
    let mut out = format!("\n  {title}:\n");
    for item in items {
        let _ = writeln!(out, "    {}", render(item));
    }
    out
}

/// A single-line JSON rendering, truncated for terminal display.
///
/// Truncation is **display only** — diffing always compares the exact,
/// untruncated value carried on the snapshot.
fn compact(value: &serde_json::Value) -> String {
    const MAX: usize = 120;
    let text = value.to_string();
    if text.chars().count() <= MAX {
        return text;
    }
    let head: String = text.chars().take(MAX).collect();
    format!("{head}… ({} bytes total)", text.len())
}

/// Render a [`TraceDiff`] as the "old build vs new build" report of AC3.
#[must_use]
pub fn render_diff(diff: &TraceDiff, left_label: &str, right_label: &str) -> String {
    let Some(div) = &diff.divergence else {
        return format!(
            "no divergence: {left_label} and {right_label} agree across all {} steps\n",
            diff.left_steps.min(diff.right_steps)
        );
    };

    let mut out = format!(
        "first divergence at step {}\n  left  ({left_label}): {} steps\n  right ({right_label}): {} steps\n\n",
        div.step_index, diff.left_steps, diff.right_steps
    );

    let kind_line = match &div.kind {
        DiffKind::HistoryFacts { field } => {
            format!("  the recorded histories differ ({field})\n")
        }
        DiffKind::CommandMismatch { command_index } => {
            format!("  command #{command_index} differs\n")
        }
        DiffKind::CommandCount { left, right } => {
            format!("  command count differs: {left} vs {right}\n")
        }
        DiffKind::Outcome { left, right } => {
            format!(
                "  outcome differs: {} vs {}\n",
                left.as_str(),
                right.as_str()
            )
        }
        DiffKind::Divergence { left, right } => {
            let mut line = String::from("  one side no longer matches the recording\n");
            for (label, side) in [("left ", left), ("right", right)] {
                match side {
                    Some(d) => {
                        let _ = writeln!(
                            line,
                            "    {label}: at event {} expected {}, got {}",
                            d.event_index, d.expected, d.actual
                        );
                    }
                    None => {
                        let _ = writeln!(line, "    {label}: replays cleanly");
                    }
                }
            }
            line
        }
        DiffKind::TraceLength {
            left,
            right,
            capped,
        } => {
            let note = if *capped {
                // Not a property of the traces: one was cut short by
                // `--max-steps`. Saying so beats reporting a divergence the
                // two builds do not actually have.
                "  (at least one trace was capped by --max-steps, so this length\n   difference is an artefact of the cap, not of the traces)\n"
            } else {
                ""
            };
            format!("  trace length differs: {left} vs {right} steps\n{note}")
        }
        // `DiffKind` is `#[non_exhaustive]`: a new variant added upstream must
        // still render something rather than failing the build here.
        other => format!("  {other:?}\n"),
    };
    out.push_str(&kind_line);

    // When the divergence is a *history fact*, name the field AND show it. The
    // handler-free CLI arm emits no commands at all, so rendering only
    // commands would print "the recorded histories differ (open_awaitables)"
    // followed by two identical `<no pending commands>` lines — naming a
    // difference the operator cannot see is worse than useless on a
    // divergence-finding tool.
    let field = match &div.kind {
        DiffKind::HistoryFacts { field } => Some(*field),
        _ => None,
    };
    out.push_str(&side("left ", left_label, div.left.as_ref(), field));
    out.push_str(&side("right", right_label, div.right.as_ref(), field));
    out
}

/// Render the one history-derived field a [`DiffKind::HistoryFacts`]
/// divergence named, for a single side.
fn history_fact_line(step: &DebugStep, field: &str) -> String {
    let value = match field {
        "event_type" => step.event_type.to_string(),
        "signal_name" => step.signal_name.clone().unwrap_or_else(|| "<none>".into()),
        "open_awaitables" => {
            if step.open_awaitables.is_empty() {
                "<none>".into()
            } else {
                step.open_awaitables
                    .iter()
                    .map(|a| {
                        format!(
                            "{} {} (opened at {})",
                            a.kind.as_str(),
                            a.name.as_deref().or(a.id.as_deref()).unwrap_or("<unnamed>"),
                            a.opened_at
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }
        "version_gates" => step
            .version_gates
            .iter()
            .map(|(id, v)| format!("{id}={v}"))
            .collect::<Vec<_>>()
            .join(", "),
        "patch_gates" => step
            .patch_gates
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", "),
        "side_effects" => step
            .side_effects
            .iter()
            .map(|e| {
                format!(
                    "{}:{}={}",
                    e.kind,
                    e.name.as_deref().unwrap_or("-"),
                    compact(&e.value)
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
        "markers" => step
            .markers
            .iter()
            .map(|m| format!("{}={}", m.name, compact(&m.details)))
            .collect::<Vec<_>>()
            .join(", "),
        // The whole recorded event with per-run identity (uuids, timestamps,
        // non-deterministic draws) normalized to `<per-run>`. This is the
        // backstop field, so it fires for any semantic difference no curated
        // projection names — printing it is what lets the operator SEE which
        // part of the event moved.
        "event_facts" => compact(&step.event_facts),
        // `history_fact_diff` owns the field vocabulary; a new field name added
        // there renders as "unavailable" rather than silently as "<none>",
        // which would read as a real (empty) value.
        _ => "<not rendered by this CLI version>".to_string(),
    };
    let value = if value.is_empty() {
        "<none>".to_string()
    } else {
        value
    };
    format!("      {field}: {value}\n")
}

/// Render one side of a divergence.
fn side(prefix: &str, label: &str, step: Option<&DebugStep>, field: Option<&str>) -> String {
    let Some(step) = step else {
        return format!("\n  {prefix} ({label}): <no step at this index>\n");
    };
    let mut out = format!(
        "\n  {prefix} ({label}) event {} [{}]\n",
        step.event_type,
        step.outcome.as_str()
    );
    if let Some(field) = field {
        out.push_str(&history_fact_line(step, field));
        // The named field is the whole story for a history-fact divergence;
        // a handler-free trace has no commands to add.
        return out;
    }
    if step.commands.is_empty() {
        out.push_str("      <no pending commands>\n");
    }
    for (i, command) in step.commands.iter().enumerate() {
        let _ = writeln!(out, "      #{i} {command}");
        if let Some(payload) = &command.payload {
            let _ = writeln!(out, "         payload: {}", compact(payload));
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry points
// ─────────────────────────────────────────────────────────────────────────────

/// `harvest debug replay <HISTORY>`.
///
/// # Errors
///
/// Fails when the history file cannot be read or is not a valid
/// `HistorySnapshot`, or when a requested breakpoint matched no step.
#[allow(clippy::too_many_arguments)]
pub fn run_replay(
    history: &Path,
    format: DebugFormat,
    step: Option<usize>,
    break_at_event_type: Option<&str>,
    break_at_index: Option<usize>,
    break_at_activity: Option<&str>,
    break_at_signal: Option<&str>,
    max_steps: Option<usize>,
    tui: bool,
) -> Result<(), CliError> {
    let snapshot = load_snapshot(history)?;
    let trace = ReplayTrace::from_history_capped(
        snapshot.workflow_name.clone(),
        snapshot.execution_id,
        &snapshot.events,
        max_steps.unwrap_or(usize::MAX),
    );

    let breakpoint = resolve_breakpoint(
        break_at_event_type,
        break_at_index,
        break_at_activity,
        break_at_signal,
    );
    let focus = resolve_focus(&trace, breakpoint.as_ref(), step);

    if tui {
        // `--step` / `--break-*` seed the opening cursor rather than being
        // silently discarded; a resolution that names no step opens at 0.
        let cursor = match focus {
            FocusResolution::BreakpointHit { index } | FocusResolution::Step { index } => index,
            _ => 0,
        };
        return crate::debug_tui::run(&trace, cursor);
    }

    if format == DebugFormat::Json {
        // JSON mode always emits the whole trace: a machine consumer does its
        // own focusing, and silently emitting a partial document would be a
        // trap for a `jq` pipeline. The breakpoint is still *evaluated*, so a
        // miss sets the exit code — otherwise `--format json --break-at-…` in a
        // pipeline is a false green whether or not the breakpoint exists.
        println!(
            "{}",
            serde_json::to_string_pretty(&trace).map_err(|e| CliError::InvalidJson {
                label: "replay trace",
                source: e,
            })?
        );
        return match focus {
            FocusResolution::BreakpointMissed => Err(CliError::DebugBreakpointMissed),
            FocusResolution::OutOfRange { index, len } => {
                Err(CliError::DebugStepOutOfRange { index, len })
            }
            _ => Ok(()),
        };
    }

    match focus {
        FocusResolution::Whole => print!("{}", render_trace_overview(&trace)),
        FocusResolution::BreakpointHit { index } => {
            print!("{}", render_trace_overview(&trace));
            println!("\nbreakpoint hit at step {index}\n");
            print!("{}", render_step_detail(&trace, index));
        }
        FocusResolution::BreakpointMissed => {
            print!("{}", render_trace_overview(&trace));
            println!("\nbreakpoint never hit across {} steps", trace.steps.len());
            return Err(CliError::DebugBreakpointMissed);
        }
        FocusResolution::Step { index } => print!("{}", render_step_detail(&trace, index)),
        FocusResolution::OutOfRange { index, len } => {
            return Err(CliError::DebugStepOutOfRange { index, len });
        }
    }
    Ok(())
}

/// `harvest debug diff <LEFT> <RIGHT>` — the two-fixture-histories arm of AC3.
///
/// Exits `1` when a divergence is found, mirroring `diff(1)`'s
/// "differences found" exit code so this is usable as a CI gate.
///
/// # Errors
///
/// Fails when either history file cannot be read or is not a valid
/// `HistorySnapshot`, or when the two traces diverge.
pub fn run_diff(left: &Path, right: &Path, format: DebugFormat) -> Result<(), CliError> {
    let left_snapshot = load_snapshot(left)?;
    let right_snapshot = load_snapshot(right)?;

    let left_trace = ReplayTrace::from_history(
        left_snapshot.workflow_name.clone(),
        left_snapshot.execution_id,
        &left_snapshot.events,
    );
    let right_trace = ReplayTrace::from_history(
        right_snapshot.workflow_name.clone(),
        right_snapshot.execution_id,
        &right_snapshot.events,
    );

    let diff = diff_traces(&left_trace, &right_trace);

    if format == DebugFormat::Json {
        println!(
            "{}",
            serde_json::to_string_pretty(&diff).map_err(|e| CliError::InvalidJson {
                label: "trace diff",
                source: e,
            })?
        );
    } else {
        print!(
            "{}",
            render_diff(
                &diff,
                &left.display().to_string(),
                &right.display().to_string()
            )
        );
    }

    diff.divergence.as_ref().map_or(Ok(()), |div| {
        Err(CliError::DebugDivergence {
            step_index: div.step_index,
        })
    })
}
