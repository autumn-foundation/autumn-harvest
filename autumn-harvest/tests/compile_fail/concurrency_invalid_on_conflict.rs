use autumn_harvest::prelude::*;

// Issue #811: `on_conflict` is a closed two-value set. A typo fails at compile
// time. A silent fallback to `defer` gives an author who configured latest-wins
// the exact opposite semantics. Nothing signals it until two runs collide in
// production.
#[workflow(concurrency(key = "input.doc_id", limit = 1, on_conflict = "cancel"))]
async fn typo_on_conflict(_ctx: &WorkflowContext, _input: String) -> Result<String, String> {
    Ok(String::new())
}

fn main() {}
