use autumn_harvest::prelude::*;

struct MockBuilder;
impl MockBuilder {
    fn side_effect<F>(&self, _f: F) {}
}

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let builder = MockBuilder;
    // A `side_effect` call on a non-ctx receiver does not exempt its closure from analysis.
    // So the `rand::random()` call inside the closure triggers HVG002.
    builder.side_effect(|| {
        let _ = rand::random::<u64>();
    });
    Ok(())
}

fn make_closure<F>(_: F) -> impl FnOnce() {
    || {}
}

#[workflow]
async fn bad_workflow_factory(ctx: &WorkflowContext) -> Result<(), String> {
    // `rand::random()` here runs outside the closure, at construction time. That
    // code runs again on every replay, so it is a determinism violation.
    ctx.side_effect("k", make_closure(rand::random::<u64>()));
    Ok(())
}

fn main() {}
