use autumn_harvest::prelude::*;

// Issue #802: pins the `#[workflow]` "unsupported attribute" whitelist. The
// declared-dependency work extended it with `activities` and `children`. The
// list is hand-maintained. Without a snapshot, a new parser attribute can be
// silently omitted from the error an author sees.
#[workflow(bogus_attr = "nope")]
async fn bogus_attr_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
