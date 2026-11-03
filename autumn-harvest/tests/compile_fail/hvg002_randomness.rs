use autumn_harvest::prelude::*;
use rand::RngExt as _;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    let _ = ctx.side_effect(&rand::random::<u64>().to_string(), || 42);

    // Current generator and method names.
    let mut rng = rand::rng();
    let _ = rng.random::<f64>();
    let _ = rng.random_range(0..10);
    let _ = rng.random_bool(0.5);
    let _ = rand::rngs::SysRng;

    // Free functions.
    let _ = rand::random_range(0..10);
    let _ = rand::random_bool(0.5);

    // Slices/buffers using .fill() (deterministic, allowed)
    let mut buf = vec![0u8; 10];
    buf.fill(42);

    Ok(())
}

fn main() {}
