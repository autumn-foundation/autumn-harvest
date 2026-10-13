## Fix — A dispatched task keeps the hint for its own row (issue #1429)

**Behaviour change.** A dispatch read claims a batch of references. The
worker starts each claimed task at once, and it acks the whole batch after
the last claim. A channel drops a publish for a reference that it has
leased out. A task can put its own row back to `PENDING` within
milliseconds:

- an activity that fails fast and retries
- an activity that an adaptive limit or an open circuit defers

When that hint reached the channel before the batch ack, the channel
dropped it. The row then waited for the reconcile sweep.

Now each task that a batch claims publishes through a gated channel. A
hint that names the task's own row goes out after the batch ack.
`DISPATCH_HINT_ACK_WAIT` bounds that wait at one second, so a stalled
channel cannot hold the hint for the whole batch timeout. The hint waits
off the task: a task can publish while it holds a pooled connection, and
the batch may need that connection to finish its claims. Every other hint,
and the start of the task, are unchanged.

**Why.** `dispatch_tests::a_saturated_reference_returns_when_a_slot_frees`
failed in 2 of 40 local Postgres 16 runs on `trunk-dev`, and in CI on
autumn-foundation/autumn-harvest#2154. Each failure waited for the
reconcile interval of the test, 10 s. The default interval is 1 s, so in
production the lost hint costs up to one second.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.** `a_row_put_back_before_the_ack_keeps_its_reference` installs a
channel whose acks take 500 ms, and an activity that fails once with a
100 ms retry. Before the fix, the retry waited for the 30 s reconcile
sweep of the test in every run. After the fix, the run completes in a few
seconds. `a_saturated_reference_returns_when_a_slot_frees` passed 100 of
100 local runs, against 2 of 40 failures on `trunk-dev`. A first version
held the publishing task until the ack. Under a full pool, that version
stalled the batch and failed the same test in about 1 of 160 runs.
