use autumn_harvest::prelude::*;

// The `start-throttle:` prefix is reserved for workflow-start throttle buckets
// (issue #607). #699 reserved `dyn-rate:` and left this one as a follow-up,
// because a squat was then merely a namespace nit; issue #1127 made it a
// stranding bug, since the idle-bucket GC collects exactly these namespaces on
// the guarantee that everything in them re-registers with the work that needs
// it -- true for a generated key, false for a static one.
#[activity(rate_limit_rps = 50, rate_limit_key = "start-throttle:onboarding:acme")]
async fn start_throttle_prefix_key(_ctx: &ActivityContext, x: i64) -> Result<i64, String> {
    Ok(x)
}

fn main() {}
