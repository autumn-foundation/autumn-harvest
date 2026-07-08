use autumn_harvest::prelude::*;

// An infinite explicit burst is inserted verbatim as the bucket's initial
// `tokens`, degrading the debit query's `LEAST(burst, tokens + refill)` to an
// unbounded, ever-growing allowance -- effectively disabling the throttle for
// that key. Must be rejected at compile time (issue #607 code review).
#[workflow(throttle(rate = "100/m", burst = 1e400))]
async fn unthrottleable_workflow(_ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let _ = input;
    Ok(())
}

fn main() {}
