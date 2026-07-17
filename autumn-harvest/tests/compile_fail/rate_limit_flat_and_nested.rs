use autumn_harvest::prelude::*;

// The flat `rate_limit_rps`/`rate_limit_burst`/`rate_limit_key` attributes and
// the nested per-key `rate_limit(...)` form (issue #699) configure the same
// token bucket in two incompatible ways. Combining them must be rejected at
// compile time.
#[activity(rate_limit_rps = 10, rate_limit(key = "input.tenant_id", rps = 50))]
async fn both_forms(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
