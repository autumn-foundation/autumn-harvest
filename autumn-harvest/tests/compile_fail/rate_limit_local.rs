use autumn_harvest::prelude::*;

// Per-key rate limiting (issue #699) is enforced on the task-dispatch path,
// which local activities bypass by running inline. The nested `rate_limit(...)`
// form must be rejected on `local = true` at compile time (mirroring how
// `circuit_breaker` is rejected).
#[activity(local = true, rate_limit(key = "input.tenant_id", rps = 50))]
async fn local_rate_limited(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
