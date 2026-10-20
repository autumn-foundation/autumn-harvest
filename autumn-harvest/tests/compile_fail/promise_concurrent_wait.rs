// Issue #1985: two waits on one promise handle at once do not compile. A
// promise has one settlement, and each timed wait keeps its own deadline.
use autumn_harvest::WorkflowContext;

async fn waits_twice_at_once(ctx: &WorkflowContext) {
    let mut promise = ctx.promise("approval").unwrap();
    let _ = tokio::join!(
        promise.wait::<u32>(),
        promise.wait_timeout::<u32>(std::time::Duration::from_secs(60)),
    );
}

fn main() {}
