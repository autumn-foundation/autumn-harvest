# Chapter 10 — Testing your workflow code

[← Operating the service](09-operations.md) · [Index](README.md)

---

Workflow code is deterministic, so it's testable without a database. Two
levels:

**1. Unit-test handlers in isolation.** Build a `WorkflowContext::new_test()`
or `ActivityContext::new_test()` (gated by the `testing` feature) and call
your function directly. Activities that read inputs and produce outputs are
trivial under this.

**2. Replay-test against recorded histories.** When you change a workflow
function, run it against histories captured from production with
`autumn_harvest::testing::WorkflowReplayer`:

```rust
use autumn_harvest::testing::{WorkflowReplayer, ReplayStatus};

#[tokio::test]
async fn checkout_replays() {
    let history = std::fs::read_to_string("fixtures/checkout_v3.json").unwrap();

    let report = WorkflowReplayer::new()
        .register_fn("checkout", checkout_handler)
        .replay_from_json(&history)
        .await
        .expect("fixture parses");

    assert!(matches!(report.status, ReplayStatus::ReplaySucceeded), "{report}");
}
```

The replayer never executes activities or touches Postgres — it runs the
workflow function in pure replay mode and compares the commands it emits
against the recorded history. A failure tells you exactly which event
diverged. Run this in CI on every workflow code change to catch
non-determinism *before* it produces DLQ entries.

See [`docs/runbooks/replay-fixture-export.md`](../runbooks/replay-fixture-export.md)
for capturing fixtures from a running service.

---

[← Operating the service](09-operations.md) · [Index](README.md)

You've reached the end of the guide. Head back to the [index](README.md) for
links to the reference example, runbooks, and architecture docs.
