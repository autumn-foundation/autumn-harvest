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

fn main() {}
