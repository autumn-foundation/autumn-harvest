## Phase X.Y — A by-id cancel reports from a later fan-out than the one that cancelled (issue #1313)

`external_target_location`'s by-id fan-out reads every expected shard
sequentially, on separate connections to separate databases, with no shared
snapshot — Postgres has no cross-shard transaction and Harvest adds no
coordinator. It is therefore authoritative over the observations it made, not
over an instant. A run of `(workflow_name, workflow_id)` that starts on a shard
after that shard answered is invisible to the merge, and PR #1306's two
withholding checks cannot see it either: every shard answered, and the selected
run really was live, so nothing disagrees.

The stale view only becomes a wrong answer once the cancel makes the run it
found terminal, because that promotes the run which started during the fan-out
to current run for the key — while `ExternalCancelDelivered` claims that nothing
runs under it. So a by-id cancel that ends a live run now cancels it, withholds
the terminal, and leaves the claim to the next sweep, whose fan-out observes the
whole window the first one ran in. The missed run is an ordinary second live
copy to that later fan-out, cancelled one per sweep until none is left — the
same convergence the ambiguous-fan-out case already has.

This is option 2 of the three the issue lays out. It narrows the window rather
than making the assertion atomic; option 1 (cross-shard uniqueness for the
business key) remains the only thing that closes it outright, and remains an
architectural addition rather than a fix.

**Cost.** One extra scanner poll interval on a by-id cancel that finds a live
run, in a multi-shard deployment. A cancel whose target is already terminal
changes nothing and still reports on the sweep that resolves it, as does every
`ExecutionId`-addressed cancel and every single-shard deployment (which skips the
fan-out entirely). The signal path is untouched: a signal reports a delivery to
one run, never a claim about the whole key.

No schema change, no new `WorkflowEvent` variant, no new exception to the
`harvest_events` append-only invariant. `DeliveryRoute::Caller` and
`DeliveryRoute::CrossShard` each gained one field, `reverify_after_cancel`, and
`CancelDeliveryAccumulators` one, `cancelled_live_run`.

**Test evidence.** Two new cases in
`shard_placement_by_id_tests`:
`outbox_cancel_by_id_does_not_report_over_a_run_that_started_during_its_fanout`
builds the issue's interleaving from a real row lock rather than a sleep — the
cancel's own `FOR UPDATE` parks the sweep between its fan-out and its terminal
event, the racing start lands on a shard the fan-out already read and reported
empty, and the test then drives the three sweeps the convergence takes. Its
sibling `outbox_cancel_by_id_reports_from_a_later_fanout_than_the_one_that_cancelled`
pins the ordinary no-race case and its one-sweep cost. Both confirmed red
against the unfixed engine, the second failing exactly on the durable-wrong-answer
assertion. Seven existing cancel cases across `shard_placement_by_id_tests` and
`workflow_id_targeted_tests` gained the extra sweep and keep every assertion they
had.
