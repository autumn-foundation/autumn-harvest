use autumn_harvest::prelude::*;

// Issue #811: `on_conflict` is a closed two-value set. A typo must be rejected
// at compile time — silently falling back to `defer` would give an author who
// believes they configured latest-wins the exact opposite semantics, with no
// signal until two runs collide in production.
#[workflow(concurrency(key = "input.doc_id", limit = 1, on_conflict = "cancel"))]
async fn typo_on_conflict(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok(String::new())
}

fn main() {}
