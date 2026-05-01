//! Example: manager-approval workflow using external activity completion.
//!
//! This example shows how to model a human-in-the-loop approval step with
//! `execute_activity_external`.  The workflow suspends without blocking a worker
//! slot; an external system (a manager clicking "Approve" in a UI, or a webhook
//! from an approval service) calls the management API to deliver the decision.
//!
//! # Flow
//!
//! 1. Workflow calls `ctx.execute_activity_external(...)` and suspends.
//! 2. Harvest records an `ActivityAwaitingExternal` event and inserts a row in
//!    `harvest_external_tasks`, embedding the opaque `token`.
//! 3. The token is delivered to the approval system (e-mail, Slack, etc.) —
//!    this example just prints it.
//! 4. The approver POSTs to the management API:
//!    ```bash
//!    curl -X POST http://localhost:8080/harvest/activities/external/<TOKEN>/complete \
//!         -H 'Content-Type: application/json' \
//!         -d '{"output": {"approved": true}}'
//!    ```
//! 5. Harvest appends `ActivityCompletedExternally`, wakes the workflow, and
//!    the workflow resumes with the approval decision.
//!
//! # Running
//!
//! ```bash
//! cargo run --example approval_workflow
//! ```
//!
//! This requires a running Postgres instance and the `db` feature (the default).
//! Set `DATABASE_URL` before running.

use autumn_harvest::context::{ActivityContext, WorkflowContext};
use autumn_harvest::prelude::*;

/// Payload delivered by the approval activity.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ApprovalDecision {
    pub approved: bool,
    pub approver: String,
    pub comment: Option<String>,
}

/// An approval workflow for an expense report.
///
/// Suspends until a manager approves or rejects the expense via the
/// management API.  Automatically times out after 7 days.
#[workflow]
pub async fn expense_approval(
    ctx: &WorkflowContext,
    expense_id: String,
    amount_cents: u64,
) -> Result<ApprovalDecision, String> {
    // Send a notification to the approver so they know action is needed.
    ctx.execute_activity_raw(
        "notify_approver",
        serde_json::json!({
            "expense_id": expense_id,
            "amount_cents": amount_cents,
        }),
        "default",
    )
    .await
    .map_err(|e: autumn_harvest::error::HarvestError| e.to_string())?;

    // Suspend until the manager completes or fails the activity via the token.
    // The `schedule_to_close_secs` sets the hard deadline; after 7 days the
    // workflow automatically receives a Timeout error and can follow retry /
    // escalation logic.
    let schedule_to_close_secs: u64 = 7 * 24 * 60 * 60; // 7 days
    let result = ctx
        .execute_activity_external(
            "manager_approval",
            serde_json::json!({
                "expense_id": expense_id,
                "amount_cents": amount_cents,
            }),
            "approvals",
            schedule_to_close_secs,
        )
        .await
        .map_err(|e: autumn_harvest::error::HarvestError| e.to_string())?;

    let decision: ApprovalDecision =
        serde_json::from_value(result).map_err(|e| format!("bad approval payload: {e}"))?;

    Ok(decision)
}

/// A stub activity that pretends to notify an approver.
///
/// In a real app, this would send an e-mail, Slack message, or push
/// notification that includes the task token so the approver UI can call the
/// management API to complete the activity.
#[activity(start_to_close = "30s", queue = "default")]
pub async fn notify_approver(
    _ctx: &ActivityContext,
    expense_id: String,
    amount_cents: u64,
) -> Result<String, String> {
    // In production: send email / push notification with the token.
    // The token is retrieved from the workflow's event history via your
    // application layer; the worker embeds it in the notification payload.
    println!(
        "[notify_approver] Expense {expense_id} for {amount_cents} cents is awaiting approval."
    );
    println!("[notify_approver] Retrieve the task token from the management API:");
    println!("[notify_approver]   GET /harvest/workflows/<EXEC_ID>");
    println!("[notify_approver] Then call:");
    println!("[notify_approver]   POST /harvest/activities/external/<TOKEN>/complete");
    println!(
        "[notify_approver]        {{\"output\": {{\"approved\": true, \"approver\": \"alice\", \"comment\": null}}}}"
    );
    Ok("notification_sent".to_string())
}

fn main() {
    println!("approval_workflow example — see module-level doc comment for usage.");
    println!();
    println!("Key curl commands after starting the server:");
    println!();
    println!("# Start a workflow");
    println!(r#"curl -X POST http://localhost:8080/harvest/workflows/expense_approval/start \"#);
    println!(r#"     -H 'Content-Type: application/json' \"#);
    println!(r#"     -d '{{"input": ["exp-001", 49900]}}'"#);
    println!();
    println!("# (Retrieve EXEC_ID and TOKEN from the event history)");
    println!("# GET /harvest/workflows/<EXEC_ID>");
    println!();
    println!("# Approve the expense");
    println!("curl -X POST http://localhost:8080/harvest/activities/external/<TOKEN>/complete \\");
    println!("     -H 'Content-Type: application/json' \\");
    println!(
        r#"     -d '{{"output": {{"approved": true, "approver": "alice", "comment": null}}}}'"#
    );
    println!();
    println!("# Reject the expense");
    println!("curl -X POST http://localhost:8080/harvest/activities/external/<TOKEN>/fail \\");
    println!("     -H 'Content-Type: application/json' \\");
    println!(r#"     -d '{{"error": "expense exceeds budget", "retryable": false}}'"#);
    println!();
    println!("# Extend the deadline by 3 more days");
    println!("curl -X POST http://localhost:8080/harvest/activities/external/<TOKEN>/heartbeat \\");
    println!("     -H 'Content-Type: application/json' \\");
    println!(r#"     -d '{{"extend_by_secs": 259200}}'"#);
}
