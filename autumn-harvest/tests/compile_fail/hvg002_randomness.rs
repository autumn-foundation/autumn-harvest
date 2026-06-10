use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    let _ = ctx.side_effect(&rand::random::<u64>().to_string(), || 42);
    
    // Rng gen/methods and OsRng check
    let mut rng = rand::rngs::OsRng;
    let _ = rng.gen::<f64>();
    
    Ok(())
}

fn main() {}

