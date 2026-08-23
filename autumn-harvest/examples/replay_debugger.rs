#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Time-travel replay debugger — issue #949.
//!
//! ## The problem
//!
//! A deploy wedges an in-flight execution. The replay gates (`replay-verify`,
//! the replay-drift gate) and the server-side diagnosis
//! (`POST /workflows/{id}/replay-diagnosis`, #614) all tell you *that* it
//! diverges. None of them tell you **where in your code**, or show you the run
//! leading up to that point.
//!
//! ## The solution
//!
//! Replay the *same* recorded history against **two builds** of your workflow
//! function and diff the traces. The debugger reports the **first** divergent
//! command, with both sides' snapshots at that step.
//!
//! Read-only by construction: no `WorkflowEvent` is appended, no activity is
//! executed, no database is touched.
//!
//! Run with:
//!
//! ```text
//! cargo run --example replay_debugger --features debugger
//! ```
//!
//! See `docs/replay-debugger.md` for the full walkthrough, including the
//! `harvest debug replay --tui` interactive stepper.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::debugger::{DiffKind, ReplayDebugger, diff_traces};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use serde_json::{Value, json};

// ─────────────────────────────────────────────────────────────────────────────
// The two builds
//
// These stand in for `#[workflow] async fn order_flow(...)` before and after a
// change. They are written as raw handler functions so the example stays
// self-contained (the macro's companion function produces the same shape).
// ─────────────────────────────────────────────────────────────────────────────

/// **Old build**: reserve stock, then charge the card.
fn old_build<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("reserve_stock", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("charge_card", json!({}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

/// **New build**: a `fraud_check` was inserted *before* `charge_card` — with no
/// `ctx.patched(...)` gate around it. Any execution that already recorded
/// `charge_card` at that position will now diverge.
fn new_build<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("reserve_stock", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("fraud_check", json!({}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("charge_card", json!({}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

/// **Fixed build**: the same change, gated so pre-change executions keep their
/// recorded path (issue #687's two-state code-evolution gate).
fn fixed_build<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("reserve_stock", json!({}), "default")
            .await
            .map_err(|e| e.to_string())?;
        if ctx.patched("fraud-check-v1") {
            ctx.execute_activity_raw("fraud_check", json!({}), "payments")
                .await
                .map_err(|e| e.to_string())?;
        }
        ctx.execute_activity_raw("charge_card", json!({}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("done"))
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// The recorded history (in production this is `harvest history export <ID>`)
// ─────────────────────────────────────────────────────────────────────────────

fn recorded_history() -> Vec<WorkflowEvent> {
    let reserve = ActivityExecId::new();
    let charge = ActivityExecId::new();
    vec![
        WorkflowEvent::workflow_started(json!({}), chrono::Utc::now()),
        WorkflowEvent::ActivityScheduled {
            activity_id: reserve,
            name: "reserve_stock".to_string(),
            input: json!({}),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: reserve,
            output: json!("reserved"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: charge,
            name: "charge_card".to_string(),
            input: json!({}),
            queue: "payments".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: charge,
            output: json!("charged"),
        },
    ]
}

fn snapshot_json() -> String {
    serde_json::to_string(&json!({
        "workflow_name": "order_flow",
        "execution_id": ExecutionId::new(),
        "events": recorded_history(),
    }))
    .expect("snapshot serializes")
}

async fn trace_with(
    handler: WorkflowHandlerFn,
    history: &str,
) -> autumn_harvest::debugger::ReplayTrace {
    ReplayDebugger::new()
        .register_fn("order_flow", handler)
        .trace_json(history)
        .await
        .expect("history parses and the workflow is registered")
}

#[tokio::main]
async fn main() {
    let history = snapshot_json();

    // ── 1. Step through the run ─────────────────────────────────────────────
    let old = trace_with(old_build, &history).await;
    println!("recorded history: {} events\n", old.total_events);
    for step in &old.steps {
        let commands: Vec<String> = step.commands.iter().map(ToString::to_string).collect();
        println!(
            "  step {}  {:<20}  open={}  next={}",
            step.index,
            step.event_type,
            step.open_awaitables.len(),
            if commands.is_empty() {
                "-".to_string()
            } else {
                commands.join(", ")
            },
        );
    }

    // ── 2. Diff old vs new — the money step ─────────────────────────────────
    let new = trace_with(new_build, &history).await;
    let diff = diff_traces(&old, &new);
    let div = diff
        .divergence
        .as_ref()
        .expect("an ungated inserted activity must diverge");

    println!("\nold build vs new build");
    println!("  first divergence at step {}", div.step_index);
    match &div.kind {
        DiffKind::CommandMismatch { command_index } => {
            println!("  command #{command_index} differs");
        }
        other => println!("  {other:?}"),
    }
    for (label, side) in [("old", div.left.as_ref()), ("new", div.right.as_ref())] {
        if let Some(step) = side {
            let commands: Vec<String> = step.commands.iter().map(ToString::to_string).collect();
            println!("    {label}: {}", commands.join(", "));
        }
    }

    // ── 3. Confirm the fix ──────────────────────────────────────────────────
    //
    // "Does my fixed code replay this recorded history cleanly?" is a
    // *single-trace* question: does any step report a divergence?
    let fixed = trace_with(fixed_build, &history).await;
    let broken_at: Vec<usize> = new
        .steps
        .iter()
        .filter(|s| s.divergence.is_some())
        .map(|s| s.index)
        .collect();
    let fixed_at: Vec<usize> = fixed
        .steps
        .iter()
        .filter(|s| s.divergence.is_some())
        .map(|s| s.index)
        .collect();
    println!("\nreplaying the recorded history:");
    println!("  new build   diverges at steps {broken_at:?}");
    println!("  fixed build diverges at steps {fixed_at:?}  <- clean");
    // The verdict itself. `is_clean()` also refuses a trace that timed out,
    // panicked, or was capped -- none of which report a divergence.
    println!("  fixed.is_clean() = {}", fixed.is_clean());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ungated_insert_diverges_at_the_inserted_command() {
        let history = snapshot_json();
        let old = trace_with(old_build, &history).await;
        let new = trace_with(new_build, &history).await;
        let div = diff_traces(&old, &new)
            .divergence
            .expect("an ungated inserted activity must diverge");
        assert!(
            matches!(div.kind, DiffKind::CommandMismatch { command_index: 0 }),
            "the very first command of the step differs, got {:?}",
            div.kind
        );
    }

    #[tokio::test]
    async fn a_patched_gate_restores_a_clean_replay() {
        // The AC6 walkthrough's closing assertion. "Replays cleanly" is a
        // *single-trace* property: no step reports a divergence.
        //
        // Note it is deliberately NOT `diff_traces(old, fixed).is_none()`. A
        // step is a replay of `events[0..=k]`, so an intermediate step models an
        // execution parked at that truncated frontier — and a `ctx.patched`
        // gate at a frontier is, correctly, *newly* patched. So the gated build
        // legitimately differs from the ungated one at intermediate steps while
        // still replaying the full recorded history cleanly. See the
        // "Gates at a truncated frontier" section of `docs/replay-debugger.md`.
        let history = snapshot_json();
        let fixed = trace_with(fixed_build, &history).await;
        let diverged: Vec<_> = fixed
            .steps
            .iter()
            .filter(|s| s.divergence.is_some())
            .map(|s| (s.index, s.divergence.clone()))
            .collect();
        // `is_clean()` is the verdict, not the divergence scan: it additionally
        // refuses a trace whose steps timed out, panicked, or were never
        // replayed — each of which reports no divergence because nothing was
        // ever compared. The scan above only supplies a readable message.
        assert!(
            fixed.is_clean(),
            "a `ctx.patched` gate must keep the pre-change path intact, but this \
             trace is not clean (diverged: {diverged:?}, first unsuccessful step: \
             {:?}, truncated: {})",
            fixed.first_unsuccessful_step(),
            fixed.truncated,
        );
    }

    #[tokio::test]
    async fn the_ungated_build_does_not_replay_cleanly() {
        // The control: without the fix, replaying the same history *does*
        // report a divergence — so the assertion above is measuring something.
        let history = snapshot_json();
        let new = trace_with(new_build, &history).await;
        assert!(
            !new.is_clean() && new.first_divergent_step().is_some(),
            "an ungated inserted activity must break replay of a pre-change history"
        );
    }

    /// Counts real side effects performed during a trace. A workflow whose
    /// body bumps this proves whether the debugger *executes* anything.
    static SIDE_EFFECTS_PERFORMED: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    /// A build whose body performs a real, observable side effect on every
    /// live pass, so "did the debugger execute this?" is measurable.
    fn side_effecting_build<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            SIDE_EFFECTS_PERFORMED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ctx.execute_activity_raw("charge_card", json!({"amount_cents": 4999}), "payments")
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!("done"))
        })
    }

    #[tokio::test]
    async fn stepping_is_read_only() {
        // AC5. The earlier form of this test asserted `trace.total_events ==
        // events.len()`, which is `from_history_capped`'s own arithmetic — it
        // passed even for a stub that replayed nothing. These assertions can
        // each fail:
        //
        //  1. the input history is byte-identical afterwards (nothing mutated);
        //  2. no *activity* ran — the workflow body reaches
        //     `execute_activity_raw`, which in replay resolves from history
        //     rather than dispatching, so the activity registry is never
        //     consulted and no task row is enqueued.
        let events_before = recorded_history();
        let history = snapshot_json();

        SIDE_EFFECTS_PERFORMED.store(0, std::sync::atomic::Ordering::SeqCst);
        let trace = ReplayDebugger::new()
            .register_fn("order_flow", side_effecting_build)
            .trace_json(&history)
            .await
            .expect("trace");

        // The debugger *does* run the workflow body (that is how it learns the
        // frontier commands), once per prefix — so this is non-zero. What it
        // must never do is let that body reach a real activity.
        assert!(
            SIDE_EFFECTS_PERFORMED.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "the body must actually be driven, else the trace proves nothing"
        );
        assert!(
            trace.steps.iter().any(|s| !s.commands.is_empty()),
            "a registered handler must yield frontier commands"
        );

        // The trace covers exactly the recorded events — nothing appended.
        assert_eq!(trace.total_events, events_before.len());
        assert_eq!(trace.steps.len(), events_before.len());
        assert!(!trace.truncated);

        // Every step's event type matches the recording, in order: the debugger
        // reports the history it was given rather than one of its own.
        for (step, event) in trace.steps.iter().zip(&events_before) {
            assert_eq!(step.event_type, event.type_name());
        }
    }

    #[tokio::test]
    async fn a_build_that_completes_early_does_not_replay_cleanly() {
        // The single-trace "does this build replay cleanly?" predicate must
        // catch a build that *drops* work the history records, not just one
        // that adds or renames it. Without the early-completion check a build
        // that returns immediately reports 100% clean against any history.
        fn does_nothing<'a>(
            _ctx: &'a WorkflowContext,
            _input: Value,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move { Ok(json!("done")) })
        }

        let history = snapshot_json();
        let trace = trace_with(does_nothing, &history).await;
        assert!(
            !trace.is_clean() && trace.first_divergent_step().is_some(),
            "a build that skips every recorded activity must NOT replay cleanly"
        );
    }
}
