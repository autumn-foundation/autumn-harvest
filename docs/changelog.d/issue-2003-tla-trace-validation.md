## Testing — check chaos traces against the TLA+ specs (issue #2003)

**Trace validation.** The chaos suite now records the history of each task
row. Two test-only triggers copy each committed write to
`harvest_task_queue`, and each activity or terminal event, into
`harvest_tla_trace`. Each case exports one NDJSON trace per row to
`HARVEST_TLA_TRACE_DIR`. `scripts/check-formal-traces.sh` turns each trace
into a TLA+ constant and runs TLC on `formal/tla/trace/ActivityClaimTrace.tla`
or `WorkflowTaskClaimTrace.tla`. A trace passes when one behavior of the
spec matches every line. No production code changes.

**Writer attribution.** A test can open a connection that names a claim
through the `harvest.trace_actor` setting. A line that names a claim must
be a new claim or an owner action of that claim, so a stale owner write
fails the check.

**Red tests.** Each red trace holds an injected violation. The fixed spec
must reject a stale owner write, and the pre-fix spec must accept it, so
only the fence causes the rejection. A forged second terminal event must be
rejected by both. `formal/tla/trace/fixtures/` holds at least one clean
trace and one red trace for each spec. The `formal-models` CI job checks
them on every PR that changes code. `chaos_tests::trace_red` injects the #1789 and #1806 stale writes on
Postgres, and `chaos.yml` checks their traces after the suite.

**Model change.** The first chaos run found a gap in `WorkflowTaskClaim`.
The drain release of an unstarted claim (`release_unstarted_claim`, issue
#1813) gives back its `attempt`, and no action modelled it. The new
`UnstartedRelease` action closes the gap. Every model config gives the same
result as before.

**Guard.** `formal_trace_coverage.rs` checks the fixtures, the trace specs,
the CI wiring, the recorder calls in the chaos suite and the docs.
