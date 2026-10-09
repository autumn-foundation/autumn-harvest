use autumn_harvest::prelude::*;

// A zero tenant window counts no spend, so the tenant LLM caps would never
// refuse a step. Must be rejected at compile time (issue #1997).
#[workflow(quota(key = "input.tenant", max_tenant_llm_tokens = 10, tenant_llm_window_secs = 0))]
async fn unbudgeted_workflow(_ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let _ = input;
    Ok(())
}

fn main() {}
