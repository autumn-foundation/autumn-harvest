//! Corpus-local helpers that take a `&WorkflowContext`.
//!
//! These live in a **different module and file** from every `#[workflow]` fn in
//! this crate, which is exactly where `det_check::resolve_helper` stops: it
//! resolves one hop, same file *and* same module path. The `#[workflow]` proc
//! macro never leaves the annotated body at all.

use autumn_harvest::prelude::*;

/// Returns how many events the backing history currently holds.
///
/// Sanctioned API, replay-**varying** value: short during the live run, long
/// during a replay. Feeding it into a command argument is a determinism bug.
#[must_use]
pub fn replay_phase(ctx: &WorkflowContext) -> u64 {
    ctx.history_event_count()
}
