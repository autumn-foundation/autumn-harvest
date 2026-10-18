## Phase — Stage a NOTIFY note without a probe round trip (issue #2125)

**What changed.** A staged NOTIFY note needs the transaction id and the
database of its write. The stage used to read them in its own `SELECT
txid_current(), current_database(), pg_postmaster_start_time()`. Now the
event insert and the task insert return them in `RETURNING`, and the stage
uses them. The activity completion and failure transactions run in
`notify::coalesced`, so their wake uses the stamp of the event insert.

**Scope.** A write asks for the stamp only inside a diesel transaction, and
only when a sender is ready. There a failed stamp fails the transaction, as
a failed probe did. An autocommit write and a raw `BEGIN` block keep the
probe. The post-commit gate does not change: the sender still waits for
`txid_status` of the same transaction id. Finding 1 of the issue, the pool
recycle ping, shipped before this change.

**Evidence.** `autumn-harvest/scripts/notify_stage_probe_perf_repro.sh` on
the e2e throughput scenario, 240 workflows. Baseline in
`docs/perf-artifacts/notify-stage-probe/`: 3,600 probe calls, 15.000 per
workflow, 5.42% of all calls. After: see `after.txt` there. Tests:
`a_three_activity_workflow_sends_no_stage_probe`,
`an_append_in_a_transaction_sends_no_stage_probe`,
`an_enqueue_in_a_transaction_sends_no_stage_probe` and
`a_coalesced_scope_stages_with_the_stamp_of_its_append`. Design:
`DESIGN-2125.md`.

No new `WorkflowEvent` variant, no new migration, no replay impact.
