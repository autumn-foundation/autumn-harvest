#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Multi-signature quorum approval example using `await_condition` and `await_condition_timeout`.
//!
//! Demonstrates:
//! 1. Using `await_condition` to block a workflow until a complex local state predicate is met.
//! 2. Using `await_condition_timeout` to race a predicate against a duration deadline.
//! 3. Deterministic state rehydration through signal replay.
//!
//! Run with:
//!   cargo run --example collect_approvals

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApprovalInput {
    pub required_approvals: usize,
    pub deadline_secs: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApproveSignal {
    pub approver: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApprovalStatus {
    pub approvers: Vec<String>,
    pub quorum_met: bool,
    pub timed_out: bool,
}

// HVG010 opt-out with justification (issue #799): this workflow deliberately
// uses `futures::future::select` at line 92 to race a `wait_for_signal` against
// an `await_condition_timeout`. The guardrail flags the combinator as a
// HardBlocker because it cannot statically prove such a race is replay-safe, but
// this specific use IS deterministic: `futures::future::select` polls its
// branches in a FIXED order (signal first, then timer — unlike the randomized
// `tokio::select!`), the timer is borrowed by `&mut` so the losing branch is
// never dropped/leaked (it persists across loop iterations), and the signal
// future is freshly re-issued each iteration. The winner is therefore a pure
// function of recorded history order, so replay resolves it identically. This
// is exactly the AC4 "author has verified this select is safe" opt-out; a
// production workflow should prefer `ctx.race()` / `ctx.receive_signal_timeout()`.
#[workflow(allow_nondeterministic_apis)]
pub async fn collect_approvals(
    ctx: &WorkflowContext,
    input: ApprovalInput,
) -> Result<ApprovalStatus, String> {
    // Keep track of the approvers in workflow local state.
    // Because autumn-harvest is event-sourced, this state will be correctly
    // rehydrated by replaying signals when the workflow resumes.
    let approvers = Arc::new(Mutex::new(HashSet::<String>::new()));

    // Expose a read-only query to inspect approvals in-flight.
    let query_state = approvers.clone();
    ctx.register_query_handler("current_approvals", move |_: &()| {
        let set = query_state.lock().unwrap();
        let list: Vec<String> = set.iter().cloned().collect();
        Ok(json!({
            "approvers": list,
            "count": list.len(),
        }))
    });

    println!("[Workflow] Starting collect_approvals quorum gate.");
    println!(
        "[Workflow] Required approvals: {}, deadline: {}s",
        input.required_approvals, input.deadline_secs
    );

    // Define a predicate checking if our quorum is met AND we have at least one admin approval.
    let has_quorum_and_admin = |set: &HashSet<String>, required: usize| {
        set.len() >= required && (set.contains("admin") || set.contains("security_lead"))
    };

    // We race the predicate check against the deadline timeout.
    // If the predicate is met first, this resolves to Ok(true).
    // If the timer expires first, it resolves to Ok(false).
    let state_check = approvers.clone();
    let timer_res =
        ctx.await_condition_timeout("approval-deadline", input.deadline_secs, move || {
            let set = state_check.lock().unwrap();
            has_quorum_and_admin(&set, input.required_approvals)
        });
    tokio::pin!(timer_res);

    // In parallel, we process incoming signals, racing them with our deadline timer.
    let met;
    loop {
        // Check if our condition/timer already resolved early
        if let std::task::Poll::Ready(met_val) = futures::poll!(&mut timer_res) {
            met = met_val.map_err(|e| e.to_string())?;
            break;
        }

        // Wait for the next approval signal, or the timer deadline.
        let sig_fut = ctx.wait_for_signal("approve");
        tokio::pin!(sig_fut);

        // harvest-suppress: DET011 "fixed poll order + &mut timer keeps the loser alive; replay-safe, see the workflow-level opt-out above"
        match futures::future::select(sig_fut, &mut timer_res).await {
            futures::future::Either::Left((sig_res, _)) => {
                if let Ok(sig_val) = sig_res {
                    if let Ok(sig) = serde_json::from_value::<ApproveSignal>(sig_val) {
                        println!("[Workflow] Received approval signal from: {}", sig.approver);
                        approvers.lock().unwrap().insert(sig.approver);
                    }
                }
            }
            futures::future::Either::Right((timeout_res, _)) => {
                met = timeout_res.map_err(|e| e.to_string())?;
                break;
            }
        }
    }

    let mut final_approvers: Vec<String> = approvers.lock().unwrap().iter().cloned().collect();
    final_approvers.sort();

    if met {
        println!("[Workflow] Approval quorum SUCCESS: {:?}", final_approvers);
        Ok(ApprovalStatus {
            approvers: final_approvers,
            quorum_met: true,
            timed_out: false,
        })
    } else {
        println!(
            "[Workflow] Approval quorum TIMEOUT. Approvals collected: {:?}",
            final_approvers
        );
        Ok(ApprovalStatus {
            approvers: final_approvers,
            quorum_met: false,
            timed_out: true,
        })
    }
}

fn main() {
    println!("collect_approvals example loaded successfully!");
    println!(
        "Use `WorkflowReplayer` to verify this workflow in CI, or mount it on an Autumn worker."
    );
}
