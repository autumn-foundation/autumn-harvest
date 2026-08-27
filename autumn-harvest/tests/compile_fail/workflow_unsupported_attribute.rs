use autumn_harvest::prelude::*;

// Issue #802: pins the `#[workflow]` "unsupported attribute" whitelist, which
// the declared-dependency work extended with `activities` and `children`. The
// list is hand-maintained, so without a snapshot a future attribute can be
// added to the parser and silently omitted from the error an author sees.
#[workflow(bogus_attr = "nope")]
async fn bogus_attr_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
