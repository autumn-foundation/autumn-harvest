use autumn_harvest::prelude::*;

struct MockBuilder;
impl MockBuilder {
    fn side_effect<F>(&self, _f: F) {}
}

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let builder = MockBuilder;
    // Calling side_effect on a non-ctx receiver should not exempt the closure argument from analysis.
    // Therefore, the rand::random() call inside the closure should trigger HVG002.
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
    // Evaluating rand::random() before passing the closure to side_effect is a deterministic violation
    // because it runs outside the closure at construction time, which is re-evaluated on every replay.
    ctx.side_effect("k", make_closure(rand::random::<u64>()));
    Ok(())
}

fn main() {}
