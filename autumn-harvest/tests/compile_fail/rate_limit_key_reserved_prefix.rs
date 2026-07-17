use autumn_harvest::prelude::*;

// The `dyn-rate:` prefix is reserved for per-key/dynamic rate-limit buckets
// (issue #699). A static `rate_limit_key` beginning with it could collide with a
// generated dynamic bucket, so it must be rejected at compile time.
#[activity(rate_limit_rps = 50, rate_limit_key = "dyn-rate:9:tenant_id:acme")]
async fn reserved_prefix_key(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
