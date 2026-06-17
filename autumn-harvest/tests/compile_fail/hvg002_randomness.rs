use autumn_harvest::prelude::*;
use rand::Rng as _;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    let _ = ctx.side_effect(&rand::random::<u64>().to_string(), || 42);
    
    // Rng gen/methods and OsRng check (using 2024 raw identifier spelling)
    let mut rng = rand::rngs::OsRng;
    let _ = rng.r#gen::<f64>();
    
    // Rand 0.9 top-level helpers (disallowed)
    let _ = rand::random_range(0..10);
    let _ = rand::random_bool(0.5);

    // Slices/buffers using .fill() (deterministic, allowed)
    let mut buf = vec![0u8; 10];
    buf.fill(42);
    
    Ok(())
}

fn main() {}


