use autumn_harvest::prelude::*;

// An infinite explicit burst becomes the initial `tokens` of the bucket. The
// debit query's `LEAST(burst, tokens + refill)` then gives an unbounded,
// ever-growing allowance, which disables the throttle for that key. The macro
// rejects it at compile time (issue #607 code review).
#[workflow(throttle(rate = "100/m", burst = 1e400))]
async fn unthrottleable_workflow(_ctx: &WorkflowContext, input: ()) -> Result<(), String> {
    let _ = input;
    Ok(())
}

fn main() {}
