use autumn_harvest::prelude::*;

// A sub-unit rate (e.g. "0.5/s") with no explicit `burst` would default the
// bucket capacity to that same sub-one value, which can never successfully
// debit a token -- must be rejected at compile time (issue #607 code review),
// not deferred to a runtime `.expect(...)` panic in `ThrottlePolicy::from_rate_str`.
#[workflow(throttle(rate = "0.5/s"))]
async fn slow_throttled_workflow(_ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let _ = input;
    Ok(())
}

fn main() {}
