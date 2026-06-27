#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Example: Fetch a workflow's terminal result over HTTP (issue #527).
//!
//! Shows how an external / non-Rust caller can:
//!   1. Start a workflow with `POST /api/harvest/workflows/{name}/start`.
//!   2. Block until it finishes with `GET /api/harvest/workflows/{id}/result?wait=30s`.
//!
//! The `?wait=` parameter turns the GET into a bounded long-poll — the server
//! holds the connection open (using LISTEN/NOTIFY, not busy-polling) and returns
//! `200 OK` as soon as the workflow reaches a terminal state, or `204 No Content`
//! + `Retry-After: 1` if the deadline expires first.
//!
//! Values of `?wait=` above the server ceiling (default **30 s**, configurable
//! via `HarvestApiState::set_workflow_result_max_wait`) are **silently clamped**,
//! not rejected.
//!
//! If the requested execution is in state `CONTINUED_AS_NEW`, the endpoint
//! follows the successor chain to the final run and returns its output — so
//! callers never have to handle the sentinel themselves.
//!
//! Run with:
//!   cargo run --example await_workflow_result_http

use autumn_harvest::prelude::*;

/// A minimal workflow for illustration purposes.
#[workflow]
async fn greeting(ctx: &WorkflowContext, name: String) -> Result<String, String> {
    let _ = ctx;
    Ok(format!("Hello, {name}!"))
}

fn main() {
    println!("await_workflow_result_http example");
    println!();
    println!("# Step 1 — start the workflow:");
    println!();
    println!(r#"  curl -X POST http://localhost:8080/api/harvest/workflows/greeting/start \"#);
    println!(r#"       -H 'Content-Type: application/json' \"#);
    println!(r#"       -d '{{"input": "World"}}'"#);
    println!();
    println!("Response:");
    println!(r#"  {{"#);
    println!(r#"    "execution_id": "01900000-0000-7000-8000-000000000001","#);
    println!(r#"    "workflow_id":  "greeting","#);
    println!(r#"    "state":        "RUNNING""#);
    println!(r#"  }}"#);
    println!();
    println!("# Step 2 — poll until terminal (long-poll, up to 30 s per request):");
    println!();
    println!(r#"  EXEC_ID="01900000-0000-7000-8000-000000000001""#);
    println!();
    println!(r#"  curl http://localhost:8080/api/harvest/workflows/$EXEC_ID/result?wait=30s"#);
    println!();
    println!("Terminal response (200 OK):");
    println!(r#"  {{"#);
    println!(r#"    "state":        "completed","#);
    println!(r#"    "output":       "Hello, World!","#);
    println!(r#"    "completed_at": "2026-06-27T00:00:00Z""#);
    println!(r#"  }}"#);
    println!();
    println!("Still-running response (204 No Content + Retry-After: 1):");
    println!("  <empty body — repeat the call to keep waiting>");
    println!();
    println!("# State values (snake_case):");
    println!("  completed | failed | cancelled | timed_out | terminated | continued_as_new");
    println!();
    println!("# Notes:");
    println!("  - 200 is returned for ALL terminal states including failures.");
    println!("  - For FAILED / CANCELLED / TIMED_OUT / TERMINATED, check the `error` field.");
    println!("  - ?wait= is clamped to the server ceiling (default 30 s).");
    println!(
        "  - CONTINUED_AS_NEW predecessors are followed to the final successor automatically."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greeting_workflow_info_is_registered() {
        let info = greeting_info();
        assert_eq!(info.name, "greeting");
    }
}
