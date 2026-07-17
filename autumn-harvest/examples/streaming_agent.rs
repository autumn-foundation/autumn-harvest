#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Live workflow output streaming via `ctx.publish_progress` — issue #791.
//!
//! ## The problem
//!
//! You are building a durable AI agent (or a long import, or any interactive
//! flow) on Harvest. The run executes reliably, but the end user staring at a
//! spinner sees *nothing* until it finishes. Polling a query handler is a
//! snapshot pull; `set_current_details` (#473/#593) is one overwriting status
//! line; the engine-event SSE tail (#324) streams `ActivityScheduled` machinery
//! and is admin-only. None of them push the author's *content* — the tokens,
//! the reasoning steps, the per-item progress — to the user as it happens. The
//! usual escape hatch is to stand up an external message bus.
//!
//! ## The solution: `ctx.publish_progress(chunk)`
//!
//! One replay-safe call. It streams an ordered, author-defined chunk to a
//! subscriber on `GET /api/harvest/workflows/{id}/stream` over the existing
//! Postgres LISTEN/NOTIFY plumbing — no external broker, no event-log bloat.
//!
//! ```rust,ignore
//! ctx.publish_progress(serde_json::json!({
//!     "step": i, "status": "thinking", "token": tok,
//! }))?;
//! ```
//!
//! ## Consuming the stream (the other half of AC8)
//!
//! An example binary can't cleanly host a live HTTP server, so here is exactly
//! how a client subscribes with `curl`. Start your Autumn app (which mounts the
//! Harvest management API), trigger this workflow to get an `{exec_id}`, then:
//!
//! ```console
//! $ curl -N http://localhost:8080/api/harvest/workflows/{exec_id}/stream
//! event: progress
//! id: 16777217
//! data: {"status":"started","prompt":"summarize the doc"}
//!
//! event: progress
//! id: 16777218
//! data: {"step":0,"status":"thinking"}
//!
//! event: progress
//! id: 33554433
//! data: {"step":0,"status":"token","token":"tok0","emitted_after_ms":3}
//!
//! event: end
//! data: {"reason":"completed"}
//! ```
//!
//! `event: progress` frames carry the raw chunk in `data:` and the monotonic
//! sequence number in `id:` (use it to detect gaps after a reconnect); `event:
//! end` closes the stream when the run reaches a terminal state; `event: error`
//! closes it if the underlying LISTEN connection drops. The route is **not**
//! admin-gated (AC5) — it is an end-user read path; front it with your app's own
//! auth. See `docs/streaming-progress.md` for the full contract.
//!
//! ## Determinism contract (AC7)
//!
//! `publish_progress` is a **pure side effect**: a no-op during replay, and it
//! writes **nothing** to `harvest_events`. A workflow that publishes N chunks
//! has a byte-for-byte identical history to one that publishes none (asserted by
//! `tests::agent_run_leaves_zero_event_footprint` and
//! `tests::replay_self_check_succeeds` below). Crucially, **chunk content MAY be
//! non-deterministic** — precisely because it is never recorded or replayed.
//! This example deliberately publishes a wall-clock-derived field to demonstrate
//! that (see the heavily-commented block in `streaming_agent`).
//!
//! Run with:
//!   cargo run --example streaming_agent

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentRequest {
    prompt: String,
    steps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentResult {
    answer: String,
    tokens_emitted: u32,
}

/// An AI-agent-shaped workflow that streams a chunk at every reasoning step,
/// interleaved with a mock "LLM call" activity. Because each `execute_activity`
/// await suspends the workflow into a fresh decision cycle, successive
/// `publish_progress` calls genuinely span multiple cycles — this is live,
/// per-step streaming, not one batched flush at the end.
///
/// NOTE ON `allow_nondeterministic_apis`: this workflow reads the wall clock
/// (`std::time::SystemTime::now`) to build a "how long did this token take"
/// field. That call would normally be a hard determinism error (guardrail
/// HVG001) — and it is suppressed here **only** to demonstrate AC7. It is safe
/// in this exact spot because the wall-clock value flows *exclusively* into a
/// `publish_progress` chunk, which is never recorded and never replayed, so it
/// cannot affect the run's history or result. Do NOT copy this pattern for a
/// value that reaches an activity input, a workflow command, or the return
/// value — there, a fresh timestamp corrupts replay. For a deterministic clock
/// use `ctx.now()` / `ctx.system_now()` instead.
#[workflow(allow_nondeterministic_apis)]
async fn streaming_agent(ctx: &WorkflowContext, req: AgentRequest) -> Result<AgentResult, String> {
    // First chunk: the run has begun. Deterministic structural content.
    ctx.publish_progress(serde_json::json!({
        "status": "started",
        "prompt": req.prompt,
    }))
    .map_err(|e| e.to_string())?;

    let started = std::time::Instant::now();
    let mut answer = String::new();

    for step in 0..req.steps {
        // "Thinking" chunk BEFORE the activity — the user sees the agent move to
        // the next step immediately, before the (possibly slow) LLM call returns.
        ctx.publish_progress(serde_json::json!({
            "step": step,
            "status": "thinking",
        }))
        .map_err(|e| e.to_string())?;

        // The mock "LLM call". In a real agent this is where a token / reasoning
        // step is generated. It runs as a durable activity, so its RESULT is
        // recorded in history and replays deterministically.
        let token: String = ctx
            .execute_activity(&generate_token_info(), step)
            .await
            .map_err(|e| e.to_string())?;
        answer.push_str(&token);

        // Token chunk AFTER the activity. This carries `emitted_after_ms`, a
        // wall-clock delta that is genuinely NON-DETERMINISTIC — it differs on
        // every run and every replay. Publishing it is SAFE (AC7) precisely
        // because a chunk never enters `harvest_events` and is never replayed:
        // during a replay pass `publish_progress` is a no-op, so this value is
        // computed and then discarded, and the recorded history is unaffected.
        let emitted_after_ms = started.elapsed().as_millis() as u64;
        ctx.publish_progress(serde_json::json!({
            "step": step,
            "status": "token",
            "token": token,          // non-deterministic at generation; safe to stream
            "emitted_after_ms": emitted_after_ms, // wall-clock; safe only in a chunk
        }))
        .map_err(|e| e.to_string())?;
    }

    // Final chunk: done. A subscriber can also just wait for `event: end`.
    ctx.publish_progress(serde_json::json!({ "status": "done" }))
        .map_err(|e| e.to_string())?;

    Ok(AgentResult {
        answer,
        tokens_emitted: req.steps,
    })
}

/// Mock LLM call — returns a deterministic token for a given step. In a real
/// agent this would call an inference API; its output is recorded once and
/// replays identically.
#[activity(start_to_close = "30s")]
async fn generate_token(_ctx: &ActivityContext, step: u32) -> Result<String, String> {
    Ok(format!("tok{step}"))
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![streaming_agent];
    let _acts = activities![generate_token];
    println!("streaming_agent example compiled successfully");
    println!();
    println!("Publish from workflow code with ctx.publish_progress(chunk).");
    println!("Consume the stream from a client:");
    println!("  curl -N http://localhost:8080/api/harvest/workflows/{{exec_id}}/stream");
    println!("See docs/streaming-progress.md for the full contract.");
}

// Gated on the `testing` feature as well as `test`: the example itself must keep
// building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled. Run with:
//   cargo test -p autumn-harvest --features testing --example streaming_agent
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    fn run_env() -> WorkflowTestEnv {
        WorkflowTestEnv::new().mock_activity("generate_token", |input| {
            let step = input.as_u64().unwrap_or(0);
            Ok(json!(format!("tok{step}")))
        })
    }

    #[tokio::test]
    async fn agent_run_completes_and_streams_every_step() {
        let outcome = run_env()
            .run(
                streaming_agent_info().handler,
                json!({ "prompt": "summarize the doc", "steps": 3 }),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        assert_eq!(
            outcome.result.as_ref().unwrap(),
            &json!({ "answer": "tok0tok1tok2", "tokens_emitted": 3 })
        );
    }

    #[tokio::test]
    async fn agent_run_leaves_zero_event_footprint() {
        // The falsifiable AC2 bar: publishing N chunks must add 0 bytes to
        // `harvest_events`. The only events recorded are the three activity
        // dispatches/completions — the many publish_progress calls (one at
        // start, two per step, one at the end = 8 chunks) leave no trace.
        let outcome = run_env()
            .run(
                streaming_agent_info().handler,
                json!({ "prompt": "p", "steps": 3 }),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let events = outcome.events();
        let scheduled = events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            .count();
        let completed = events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
            .count();
        assert_eq!(
            scheduled, 3,
            "expected exactly three ActivityScheduled events"
        );
        assert_eq!(
            completed, 3,
            "expected exactly three ActivityCompleted events"
        );
        // Nothing but activity + lifecycle events — no progress footprint.
        for e in events {
            assert!(
                !format!("{e:?}").to_lowercase().contains("progress"),
                "publish_progress must leave zero footprint in harvest_events, found: {e:?}"
            );
        }
    }

    #[tokio::test]
    async fn replay_self_check_succeeds() {
        // AC2: a replay of a history whose workflow publishes chunks (including a
        // non-deterministic wall-clock chunk) must report ReplaySucceeded — the
        // chunks never enter history, so replay sees byte-identical events.
        let outcome = run_env()
            .run(
                streaming_agent_info().handler,
                json!({ "prompt": "p", "steps": 3 }),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let report = outcome.replay_check(streaming_agent_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "publish_progress must be replay-safe:\n{report}"
        );
    }
}
