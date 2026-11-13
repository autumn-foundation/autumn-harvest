use autumn_harvest::prelude::*;

// The `start-throttle:` prefix is reserved for workflow-start throttle buckets
// (issue #607). #699 reserved `dyn-rate:` and left this one as a follow-up,
// because a squat was then only a namespace nit. Issue #1127 made it a
// stranding bug. The idle-bucket GC collects exactly these namespaces. It
// assumes that each key in them re-registers with the work that needs it.
// That holds for a generated key, not for a static one.
#[activity(rate_limit_rps = 50, rate_limit_key = "start-throttle:onboarding:acme")]
async fn start_throttle_prefix_key(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
