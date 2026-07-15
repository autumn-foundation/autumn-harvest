//! FIX B / AUDIT (issue #1069 P2, Codex `runtime.rs:840`): cancellable durable
//! timers are a v0.1 non-goal for the single-writer `SQLite` backend, so a workflow
//! that reaches for `ctx.start_timer(...)` / `TimerHandle::await_fire·cancel·reset`
//! (which emit `WorkflowCommand::ArmTimer` / `CancelTimer`) fails LOUDLY with a
//! clear, actionable [`SqliteError::Unsupported`] that NAMES the command and points
//! at the supported fire-once `ctx.timer(...)` — never the opaque `Unsupported(
//! "Unknown")` these commands surfaced as before (they were absent from
//! `command_name` and fell through the generic catch-all).
//!
//! Only fire-once `ctx.timer(...)` durable timers are supported on this backend;
//! the full set of unsupported primitives (child workflows, external
//! signals/cancels, local activities, updates, search attributes, detached
//! children, cancellable timers) is documented as a v0.1 non-goal in the crate
//! docs and is likewise rejected BY NAME via `command_name`'s exhaustive match.
//!
//! No Docker: `SQLite` is embedded (`rusqlite` `bundled`).

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{SqliteError, SqliteRuntime};
use serde_json::json;

// Arms an author-controlled CANCELLABLE timer (issue #768) and awaits it. This
// emits `WorkflowCommand::ArmTimer`, which is outside the sqlite subset.
#[workflow]
async fn cancellable_timer_wf(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let handle = ctx.start_timer("gate", 1);
    handle.await_fire().await.map_err(|e| e.to_string())?;
    Ok(0)
}

// RED-in-spirit pre-fix: `ArmTimer` was not in `command_name` and fell through to
// the catch-all, so the run failed with `Unsupported("Unknown")` — a message that
// neither named the offending command nor guided the author to the supported
// primitive.
#[tokio::test]
async fn cancellable_timer_is_rejected_by_name_not_unknown() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&cancellable_timer_wf_info());

    let exec = rt.start_workflow("cancellable_timer_wf", json!(1)).unwrap();
    let err = rt
        .run_until_blocked(exec)
        .await
        .expect_err("a cancellable-timer workflow must be rejected, not driven");

    match err {
        SqliteError::Unsupported(msg) => {
            assert!(
                msg.contains("ArmTimer"),
                "the error must NAME the offending command; got: {msg}"
            );
            assert!(
                !msg.contains("Unknown"),
                "the error must NOT be the opaque `Unknown`; got: {msg}"
            );
            assert!(
                msg.contains("ctx.timer"),
                "the error must point at the supported fire-once `ctx.timer`; got: {msg}"
            );
            assert!(
                msg.contains("#768"),
                "the error should reference the cancellable-timer primitive (issue #768); got: {msg}"
            );
        }
        other => panic!("expected SqliteError::Unsupported, got {other:?}"),
    }
}
