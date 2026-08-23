//! Interactive stepper for `harvest debug replay --tui` (issue #949).
//!
//! A thin ratatui view over an already-built
//! [`ReplayTrace`](autumn_harvest::debugger::ReplayTrace). All navigation state
//! lives in the pure [`StepperState`], which is unit-tested without a terminal;
//! the terminal layer only draws it and maps key presses onto its methods.
//!
//! Stepping "backward" is free: the trace is materialised up front, so moving
//! to a lower index is a cursor move, not a re-replay.

use std::io;
use std::time::Duration;

use autumn_harvest::debugger::{Breakpoint, ReplayTrace, StepOutcome};
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};

use crate::CliError;
use crate::debug::render_step_detail;

/// Keyboard help, rendered in the footer.
const HELP: &str = " n/→ next  ·  p/← prev  ·  g home  ·  G end  ·  d next divergence  ·  q quit ";

/// Pure navigation state for the stepper.
///
/// Extracted from the terminal layer so every navigation rule is testable
/// without a TTY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepperState {
    cursor: usize,
    len: usize,
}

impl StepperState {
    /// A stepper positioned at the first step of a `len`-step trace.
    #[must_use]
    pub const fn new(len: usize) -> Self {
        Self { cursor: 0, len }
    }

    /// The currently focused step index.
    #[must_use]
    pub const fn cursor(self) -> usize {
        self.cursor
    }

    /// Move forward one step, saturating at the last step.
    pub const fn next(&mut self) {
        if self.len > 0 && self.cursor + 1 < self.len {
            self.cursor += 1;
        }
    }

    /// Move back one step, saturating at the first step.
    pub const fn prev(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    /// Jump to the first step.
    pub const fn home(&mut self) {
        self.cursor = 0;
    }

    /// Jump to the last step.
    pub const fn end(&mut self) {
        self.cursor = self.len.saturating_sub(1);
    }

    /// Jump to `index`, clamped into range. Returns `true` when the cursor
    /// actually landed on the requested index.
    pub const fn jump(&mut self, index: usize) -> bool {
        if self.len == 0 {
            return false;
        }
        if index < self.len {
            self.cursor = index;
            true
        } else {
            self.cursor = self.len - 1;
            false
        }
    }

    /// Advance to the next step at or after the cursor that satisfies
    /// `breakpoint`. Returns `true` when one was found.
    ///
    /// The search starts one step **past** the cursor so repeatedly pressing
    /// the key walks through successive hits rather than sticking on the
    /// current one.
    pub fn run_to(&mut self, trace: &ReplayTrace, breakpoint: &Breakpoint) -> bool {
        let from = self.cursor.saturating_add(1);
        match trace.find_breakpoint(breakpoint, from) {
            Some(index) => {
                self.cursor = index;
                true
            }
            None => false,
        }
    }
}

/// Run the interactive stepper over `trace`.
///
/// # Errors
///
/// Returns [`CliError::DebugTerminal`] when terminal setup, teardown, or
/// drawing fails.
pub fn run(trace: &ReplayTrace, cursor: usize) -> Result<(), CliError> {
    if trace.steps.is_empty() {
        println!("history is empty — nothing to step through");
        return Ok(());
    }

    // Setup is rolled back step by step on failure. A `?` straight out of any
    // of these would return *past* the restore block at the bottom and leave
    // the user's shell in raw mode (and possibly on the alternate screen) —
    // on a partially-supported terminal, a debugger that wrecks the shell it
    // was launched from is worse than the bug being chased.
    enable_raw_mode().map_err(terminal_error("enable raw mode"))?;
    let mut stdout = io::stdout();
    if let Err(e) = execute!(stdout, EnterAlternateScreen) {
        disable_raw_mode().unwrap_or_default();
        return Err(terminal_error("enter alternate screen")(e));
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(e) => {
            disable_raw_mode().unwrap_or_default();
            execute!(io::stdout(), LeaveAlternateScreen).unwrap_or_default();
            return Err(terminal_error("create terminal")(e));
        }
    };

    // A panic inside the loop would unwind past the restore below and leave the
    // user's shell in raw mode — so restore from the panic hook as well.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        disable_raw_mode().unwrap_or_default();
        execute!(io::stdout(), LeaveAlternateScreen).unwrap_or_default();
        eprintln!("{info}");
    }));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        event_loop(&mut terminal, trace, cursor)
    }));
    std::panic::set_hook(previous_hook);

    // Restore the terminal even if the loop failed — a debugger that leaves the
    // user's shell in raw mode is worse than the bug they were chasing.
    //
    // **Every step is attempted, and a failure is reported rather than
    // swallowed.** Discarding these (the earlier `unwrap_or_default()`) meant a
    // shell genuinely left in raw mode — the failure the panic hook and the
    // setup rollback above exist to prevent — exited `0` with no diagnostic, so
    // the user is left in a broken shell with nothing pointing at why. Each
    // step still runs regardless of the previous one's outcome; only the
    // *first* failure is reported, since a later one is usually a consequence.
    let steps: [(&'static str, Result<(), io::Error>); 3] = [
        ("disable raw mode", disable_raw_mode()),
        (
            "leave alternate screen",
            execute!(terminal.backend_mut(), LeaveAlternateScreen),
        ),
        ("show cursor", terminal.show_cursor()),
    ];
    // The array is built eagerly, which is what makes "every step runs" true.
    let teardown = steps
        .into_iter()
        .find_map(|(op, res)| res.err().map(terminal_error(op)));

    let outcome = result.unwrap_or_else(|_| {
        Err(CliError::DebugTerminal {
            op: "run stepper",
            reason: "the interactive stepper panicked (terminal restored)".to_string(),
        })
    });
    // The loop's own error wins: it is what the user was chasing, and a
    // teardown failure downstream of it is usually the same root cause.
    match (outcome, teardown) {
        (Err(e), _) | (Ok(()), Some(e)) => Err(e),
        (Ok(()), None) => Ok(()),
    }
}

/// Map an `io::Error` from a terminal operation onto a `CliError`.
fn terminal_error(op: &'static str) -> impl Fn(io::Error) -> CliError {
    move |err| CliError::DebugTerminal {
        op,
        reason: err.to_string(),
    }
}

fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    trace: &ReplayTrace,
    cursor: usize,
) -> Result<(), CliError> {
    let mut state = StepperState::new(trace.steps.len());
    state.jump(cursor);
    let mut list_state = ListState::default();
    // Feedback for a `d` that found nothing — otherwise the key is silently
    // inert, which is indistinguishable from "the debugger is broken".
    let mut status = String::new();

    loop {
        list_state.select(Some(state.cursor()));
        terminal
            .draw(|frame| draw(frame, trace, state, &mut list_state, &status))
            .map_err(terminal_error("draw"))?;

        if !event::poll(Duration::from_millis(200)).map_err(terminal_error("poll input"))? {
            continue;
        }
        let Event::Key(key) = event::read().map_err(terminal_error("read input"))? else {
            continue;
        };
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
            KeyCode::Char('n') | KeyCode::Right | KeyCode::Down => state.next(),
            KeyCode::Char('p') | KeyCode::Left | KeyCode::Up => state.prev(),
            KeyCode::Char('g') | KeyCode::Home => state.home(),
            KeyCode::Char('G') | KeyCode::End => state.end(),
            KeyCode::Char('d') => {
                status = if state.run_to(trace, &Breakpoint::FirstDivergence) {
                    String::new()
                } else if trace
                    .steps
                    .iter()
                    .all(|s| s.outcome == StepOutcome::NotReplayed)
                {
                    " no divergences: this trace has no workflow code registered \
                     (the CLI is handler-free — see docs/replay-debugger.md) "
                        .to_string()
                } else {
                    " no further divergence ".to_string()
                };
            }
            _ => {}
        }
    }
}

fn draw(
    frame: &mut ratatui::Frame<'_>,
    trace: &ReplayTrace,
    state: StepperState,
    list_state: &mut ListState,
    status: &str,
) {
    let chunks = Layout::vertical([Constraint::Min(5), Constraint::Length(1)]).split(frame.area());
    let body = Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(chunks[0]);

    let items: Vec<ListItem<'_>> = trace
        .steps
        .iter()
        .map(|step| {
            let diverged = step.divergence.is_some();
            let style = if diverged {
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(
                format!("{:>4}  {}", step.index, step.event_type),
                style,
            )))
        })
        .collect();

    frame.render_stateful_widget(
        List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(if trace.truncated {
                        format!(
                            " {}  (capped at {} of {} events) ",
                            trace.workflow_name,
                            trace.steps.len(),
                            trace.total_events
                        )
                    } else {
                        format!(" {} ", trace.workflow_name)
                    }),
            )
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
        body[0],
        list_state,
    );

    frame.render_widget(
        Paragraph::new(render_step_detail(trace, state.cursor()))
            .block(Block::default().borders(Borders::ALL).title(" step "))
            .wrap(Wrap { trim: false }),
        body[1],
    );

    frame.render_widget(
        Paragraph::new(Span::styled(
            if status.is_empty() { HELP } else { status },
            Style::default().fg(Color::Black).bg(Color::Gray),
        )),
        chunks[1],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stepper_navigation_saturates_at_both_ends() {
        let mut s = StepperState::new(3);
        assert_eq!(s.cursor(), 0);
        s.prev();
        assert_eq!(s.cursor(), 0, "prev at the first step must saturate");
        s.next();
        s.next();
        assert_eq!(s.cursor(), 2);
        s.next();
        assert_eq!(s.cursor(), 2, "next at the last step must saturate");
        s.home();
        assert_eq!(s.cursor(), 0);
        s.end();
        assert_eq!(s.cursor(), 2);
    }

    #[test]
    fn stepper_on_an_empty_trace_never_moves() {
        let mut s = StepperState::new(0);
        s.next();
        s.end();
        assert_eq!(s.cursor(), 0);
        assert!(!s.jump(0), "an empty trace has no step to jump to");
    }

    /// A three-event trace whose middle step schedules `charge`.
    fn sample_trace() -> ReplayTrace {
        use autumn_harvest::event::WorkflowEvent;
        use autumn_harvest::types::{ActivityExecId, ExecutionId};

        let activity_id = ActivityExecId::new();
        ReplayTrace::from_history(
            "wf".to_string(),
            ExecutionId::new(),
            &[
                WorkflowEvent::workflow_started(serde_json::json!({}), chrono::Utc::now()),
                WorkflowEvent::ActivityScheduled {
                    activity_id,
                    name: "charge".to_string(),
                    input: serde_json::json!({}),
                    queue: "default".to_string(),
                },
                WorkflowEvent::ActivityCompleted {
                    activity_id,
                    output: serde_json::json!("ok"),
                },
            ],
        )
    }

    #[test]
    fn stepper_run_to_moves_to_the_hit_and_reports_success() {
        let trace = sample_trace();
        let mut s = StepperState::new(trace.steps.len());
        let bp = Breakpoint::ActivityName("charge".to_string());
        assert!(s.run_to(&trace, &bp), "the activity is in this trace");
        assert_eq!(s.cursor(), 1);
    }

    #[test]
    fn stepper_run_to_searches_past_the_cursor_so_repeats_advance() {
        // Searching from the cursor itself would stick on the current hit
        // forever; searching from cursor+1 walks successive hits.
        let trace = sample_trace();
        let mut s = StepperState::new(trace.steps.len());
        let bp = Breakpoint::ActivityName("charge".to_string());
        assert!(s.run_to(&trace, &bp));
        assert_eq!(s.cursor(), 1);
        assert!(
            !s.run_to(&trace, &bp),
            "there is no second hit, so the search must report failure"
        );
        assert_eq!(s.cursor(), 1, "a failed search must not move the cursor");
    }

    #[test]
    fn stepper_run_to_reports_failure_when_nothing_matches() {
        let trace = sample_trace();
        let mut s = StepperState::new(trace.steps.len());
        let bp = Breakpoint::ActivityName("nonexistent".to_string());
        assert!(!s.run_to(&trace, &bp));
        assert_eq!(s.cursor(), 0);
    }

    #[test]
    fn stepper_jump_clamps_out_of_range_and_reports_it() {
        let mut s = StepperState::new(3);
        assert!(s.jump(1));
        assert_eq!(s.cursor(), 1);
        assert!(!s.jump(99), "an out-of-range jump must report false");
        assert_eq!(s.cursor(), 2, "and clamp to the last step");
    }
}
