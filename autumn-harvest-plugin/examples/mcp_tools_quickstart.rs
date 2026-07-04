//! Expose a durable workflow to AI agents as MCP tools (issue #597).
//!
//! Run with:
//! ```bash
//! export AUTUMN_DATABASE__URL=postgres://localhost/harvest_demo
//! cargo run -p autumn-harvest-plugin --example mcp_tools_quickstart --features mcp
//! ```
//!
//! One `#[workflow(mcp)]` opt-in gives an agent a correlated tool set at
//! `POST /mcp` (Streamable-HTTP JSON-RPC):
//!
//! - `start_document_review` — start durable work, returns the handle
//!   immediately: the agent's session can end, the daemon can restart, the
//!   work continues.
//! - `document_review_status` — read state/progress/output by handle.
//! - `signal_document_review` — deliver an async signal by handle (here: the
//!   human approval that unblocks `wait_for_signal`).
//! - `document_review_update_set_deadline` — a synchronous request/response
//!   update (from `#[update(workflow = …, mcp)]`).
//! - `document_review_watch` — subscribe to progress over streaming MCP
//!   (`notifications/progress`) instead of busy-polling status.
//!
//! An agent session looks like:
//!
//! ```json
//! --> {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
//!       "name":"start_document_review","arguments":{"body":{"doc_id":"doc-7"}}}}
//! <-- {"result":{"content":[{"type":"text","text":
//!       "{\"execution_id\":\"018f…\",\"workflow_name\":\"document_review\",…}"}]}}
//!
//! --> {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
//!       "name":"document_review_watch",
//!       "arguments":{"handle":"018f…"},"_meta":{"progressToken":"t1"}}}
//! <-- notifications/progress {"progressToken":"t1","progress":1,
//!       "message":"step 1/2: awaiting human approval"}
//! …
//!
//! --> {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
//!       "name":"signal_document_review",
//!       "arguments":{"handle":"018f…","signal_name":"approval",
//!                    "body":{"approved_by":"alice"}}}}
//! ```
//!
//! Safety posture: exposure is opt-in twice (the `mcp` attribute selects
//! workflows; `HarvestPlugin::mcp_tools()` mounts the routes), the mutating
//! tools are never part of autumn-web's read-only `expose_all_as_mcp` hatch,
//! and the whole `/mcp` endpoint should be gated with
//! `AppBuilder::secure_mcp(...)` in production — tool calls replay the
//! caller's credentials through the real handler pipeline.

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ReviewRequest {
    doc_id: String,
}

fn review_input_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "doc_id": { "type": "string", "description": "Document to review" }
        },
        "required": ["doc_id"]
    })
}

/// A human-in-the-loop review: the run parks (durably — for hours or days)
/// until the approval signal arrives, surviving restarts in between.
#[workflow(mcp, description = "Review a document with a human approval gate")]
async fn document_review(ctx: &WorkflowContext, request: ReviewRequest) -> Result<String, String> {
    ctx.set_current_details("step 1/2: awaiting human approval");
    let approval = ctx
        .wait_for_signal("approval")
        .await
        .map_err(|e| e.to_string())?;
    ctx.set_current_details("step 2/2: publishing review outcome");
    let approver = approval
        .get("approved_by")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Ok(format!("{} approved by {approver}", request.doc_id))
}

#[derive(Debug, Serialize, Deserialize)]
struct DeadlineRequest {
    deadline: String,
}

/// Synchronous steering: an agent calls this as its own MCP tool and gets the
/// result back in the same call (validated, durably admitted, executed).
#[update(workflow = "document_review", mcp)]
#[allow(clippy::unused_async)] // #[update] handlers must be async fn
async fn set_deadline(_ctx: &WorkflowContext, req: DeadlineRequest) -> Result<String, String> {
    Ok(format!("deadline set to {}", req.deadline))
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(vec![
                    __autumn_workflow_info_document_review()
                        .with_input_schema_fn(review_input_schema),
                ])
                .updates(updates![set_deadline])
                .worker(WorkerConfig::default())
                // api_with_auth (not plain `.api(...)`) is required, not
                // optional, in production: it protects both the management
                // API and every generated MCP tool route's own HTTP path.
                // `.secure_mcp(...)` below only gates the `/mcp` JSON-RPC
                // envelope -- without api_with_auth, a caller could bypass
                // it entirely by hitting e.g.
                // `POST /api/harvest/mcp/workflows/document_review/start`
                // directly. `RequireAuth` here is a stand-in for a real
                // session/token check -- swap in `RequireApiToken` or a
                // custom layer for production.
                .api_with_auth(
                    "/api/harvest",
                    autumn_web::auth::RequireAuth::new("user_id"),
                )
                // Generate the MCP tool routes for every #[workflow(mcp)].
                .mcp_tools(),
        )
        // Also secure the /mcp JSON-RPC envelope itself (initialize/
        // tools/list/tools/call dispatch) -- both layers are needed.
        .secure_mcp(autumn_web::auth::RequireAuth::new("user_id"))
        .mount_mcp("/mcp")
        .run()
        .await;
}
