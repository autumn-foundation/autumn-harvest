## Fix — The dispatch reader waits for a permit, and a task keeps its own-row hint (issue #1429)

**Behaviour change 1: the reader waits for a full pool.** A worker gives a
reference straight back when the pool it needs has no free permit, so a
peer with a permit can read it. The read size counts the free permits of
both pools. So while the other pool had a free permit, the reader read the
same reference again at once and gave it back again. That loop made
thousands of channel calls per second until a permit freed. With an
in-memory channel no call yielded, so the loop starved the tasks that held
the permits. They then waited for the next reconcile sweep, which does
database I/O.

Now a single-shard reader waits after a batch that dispatched nothing and
gave a reference back for lack of a permit. A free permit of the refused
pool ends the wait. `DISPATCH_REFUSED_WAIT` caps it at 10 ms, or at the
poll interval when that is shorter. The cap is fixed because
`poll_interval_ms` can be seconds. When the refused pool still shows a free
permit, a claimed task holds it as a reservation, so the reader yields to
that task instead of waiting. The reference still goes back at once, so a
peer still reads it first. A multi-shard round did not spin, because it
waits when every shard is idle, so it is unchanged.

The cap is a trade-off. A reference of the other kind that arrives during
the wait waits up to 10 ms. A backlog of refused references ahead of a
claimable one costs up to 10 ms per read. In exchange, the reader makes at
most about 100 reads per second while a pool is full, against thousands
before.

**Behaviour change 2: a dispatched task keeps the hint for its own row.** A
dispatch read claims a batch of references. The worker starts each claimed
task at once, and it acks the whole batch after the last claim. A channel
drops a publish for a reference that it has leased out. A task can put its
own row back to `PENDING` within milliseconds:

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
reconcile interval of the test, 10 s. A channel log of a failing run shows
the cause. Four workflow tasks filled the workflow pool. The reader then
released the other six workflow references about 159,000 times in 10 s, and
the four tasks published nothing until the reconcile sweep ran. The
own-row hint fix alone left the test failing in 3 of 180 runs. With both
fixes it passed 200 of 200 runs.

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Tests.**

- `a_reference_for_a_full_pool_does_not_spin_the_reader` runs two slow
  workflows on one workflow permit. Before the fix, the reader released the
  waiting reference about 65,000 times in one 800 ms hold. The test allows
  1,000, against about 80 expected with a 10 ms wait.
- `a_row_put_back_before_the_ack_keeps_its_reference` installs a channel
  whose acks take 500 ms, and an activity that fails once with a 100 ms
  retry. Before the fix, the retry waited for the 30 s reconcile sweep of
  the test in every run. After the fix it passed 25 of 25 runs, each in a
  few seconds.
