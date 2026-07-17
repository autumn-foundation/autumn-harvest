use autumn_harvest::prelude::*;

// The nested `rate_limit(...)` form (issue #699) requires both `key` and `rps`.
// Omitting `rps` must be a compile-time error, not a silent unbounded start.
#[activity(rate_limit(key = "input.tenant_id"))]
async fn missing_rps(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
