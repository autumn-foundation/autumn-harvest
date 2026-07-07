use autumn_harvest::prelude::*;
use rand::Rng as _;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    let _ = ctx.side_effect(&rand::random::<u64>().to_string(), || 42);
    
    // Rng gen/methods and OsRng check (using 2024 raw identifier spelling)
    let mut rng = rand::rngs::OsRng;
    let _ = rng.r#gen::<f64>();
    
    // Rand 0.9 top-level helpers (disallowed). The workspace itself pins
    // rand 0.8 (no free-standing `random_range`/`random_bool` functions), so
    // these locally-shadowed same-named fns stand in for the real rand 0.9
    // API: the lint matches on the call's textual path alone (a syntactic
    // syn check that runs before type resolution), so it fires identically
    // whether `random_range`/`random_bool` resolves to rand or a same-named
    // local item.
    fn random_range(_r: std::ops::Range<i32>) -> i32 {
        0
    }
    fn random_bool(_p: f64) -> bool {
        false
    }
    let _ = random_range(0..10);
    let _ = random_bool(0.5);

    // Slices/buffers using .fill() (deterministic, allowed)
    let mut buf = vec![0u8; 10];
    buf.fill(42);
    
    Ok(())
}

fn main() {}


