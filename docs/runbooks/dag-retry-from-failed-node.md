# Runbook: A DAG Node Failed and You Fixed the Cause — Retry Without Re-running Upstream

A node in a long DAG run failed (for example, node `step_6` of a 10-node nightly
ETL because S3 returned 500s for ten minutes). You have fixed the upstream cause
and want to **re-run from the failed node**, not from node 1 — re-running the
successful upstream nodes is expensive and often non-idempotent (re-emitting
Kafka messages, double-writing staging tables, re-billing downstream APIs).

This is the harvest-native answer to Airflow's "Clear and rerun selected tasks",
Dagster's "Re-execute from failure", and Prefect's `retry_from_failed`. It
composes on top of workflow reset (#148) and the unified DAG execution model
(#256); it adds no new event variant and no migration.

**When to use:** a *unified* DAG run is in a terminal `FAILED`, `CANCELLED`, or
`TIMED_OUT` state and you want to resume from a specific failed node.

**When _not_ to use:**
- The run **succeeded** (`COMPLETED`) — use the schedule trigger-now / start
  endpoint for a fresh run.
- The run is **still running** — cancel it first (`POST /workflows/{id}/cancel`),
  then retry. This matches the #148 reset contract.
- It's a **classic** (non-unified) DAG — those are being retired (#256 step 5);
  this surface does not extend their lifetime.

---

## Always dry-run first

`dry_run: true` computes the plan **without writing anything**. It returns:

- `reset_to_event_id` — the event the source run will be forked at.
- `nodes_to_re_execute` — every node that will (re-)execute on the new run. This
  is computed from the *actual* fork point, so it includes the failed node, its
  declared downstream, and any same-level sibling that re-runs as a consequence.
  **No surprises.**
- `nodes_carried_over` — every node whose recorded result is preserved.

```bash
curl -sS -X POST \
  "$HARVEST_URL/dags/nightly_etl/runs/$RUN_EXEC_ID/retry" \
  -H 'content-type: application/json' \
  -d '{
        "from_nodes": ["transform_orders"],
        "reason": "S3 incident 2026-05-17, fixed at 02:14",
        "operator_id": "oncall@example.com",
        "dry_run": true
      }'
```

Inspect `nodes_to_re_execute` and `nodes_carried_over`. If they match your
intent, drop `dry_run` (or set it `false`) and re-issue to commit the retry. A
`201 Created` response carries the `new_run_exec_id` of the forked run.

### CLI

```bash
# Preview:
autumn-harvest dag retry nightly_etl "$RUN_EXEC_ID" \
  --from-node transform_orders \
  --reason "S3 incident 2026-05-17" \
  --dry-run

# Commit (multiple nodes allowed):
autumn-harvest dag retry nightly_etl "$RUN_EXEC_ID" \
  --from-node transform_orders --from-node load_warehouse \
  --reason "S3 incident 2026-05-17" \
  --operator-id oncall@example.com
```

`--operator-id` defaults to the global `--actor` if omitted.

---

## What gets re-executed (level-granular semantics)

The new run is forked at the **clean boundary before the failed node's
execution level**. Concretely:

- **Linear DAG** (`a → b → c → d`, `c` failed): retry from `c` carries `a` and
  `b`, then re-dispatches `c` (succeeds with the fixed cause) and `d`.
- **Fan-out DAG** (`a → {b, c, d} → e`): retrying *any* node in a parallel level
  re-executes the **whole level** plus its downstream join, because the durable
  history cannot fork mid-level without stranding a sibling. You only name the
  failed node — its same-level siblings are pulled in automatically (even if they
  already succeeded), so there is never a "name the succeeded sibling to widen"
  dead-end. Upstream (`a`) is always carried over.

The dry-run output lists every node that will re-run, so you can confirm the
blast radius before committing.

If the fork point falls inside an **unresolved upstream** side effect (an
upstream activity, timer, or child workflow that never settled — e.g. a run
cancelled mid-flight), the endpoint returns `409 Conflict` with the nearest
valid boundary and a hint to wait for it to settle or cancel the run first.

---

## A compensated run is not retryable (issue #780)

A DAG whose nodes declare compensators (`.compensate(undo_fn)`) **unwinds** on
terminal failure, undoing every node that succeeded. Retry-from-node
deliberately CARRIES OVER those same succeeded upstream nodes — so a retry
would resume as if their side effects still existed, double-spending the
compensation.

The endpoint therefore rejects such a run outright:

```
HTTP 409 Conflict
{"message":"this run already executed its compensation unwind, so its succeeded
 nodes' side effects were rolled back; retrying would resume on rolled-back
 state — start a fresh DAG run instead"}
```

**What to do instead:** start a **fresh DAG run**
(`POST /dags/{dag_name}/trigger`). The unwind already restored the pre-run
state, so a fresh run is the correct — and safe — recovery.

Detection uses two independent signals, **both read from the run's own recorded
history and never from the registered definition**:

1. a `saga_compensat*` marker **followed by a started activity dispatch**, or
2. a **started** activity dispatch whose **input is a compensation envelope**
   (`{"dag_compensate": …, "input": …, "output": …}`) whose `dag_compensate`
   names a node that was **dispatched in the same run**.

Signal 2 is load-bearing, not belt-and-braces. A run that received an
unsolicited signal unwinds **without** recording a marker (see
[a stray signal silences unwind observability](../saga.md#known-limitation--a-stray-signal-silences-unwind-observability)),
so a marker-only check would leave that fully rolled-back run retryable.

Both signals require the compensator to have **started**, not merely been
scheduled. Cancelling a run mid-unwind fails every open task without recording
an event, so a compensator dispatched-but-never-claimed appears in history
having rolled back nothing — and 409-ing that would push you to a fresh run
that re-runs an upstream node whose side effect is still live. The engine
appends `ActivityStarted` before it invokes a handler, so its absence proves
the compensator never ran. A compensator that started and then crashed
mid-body counts as compensated: whether the rollback landed is genuinely
unknown, and that ambiguity resolves against retrying.

The "followed by a dispatch" half of signal 1 matters just as much, in the
opposite direction. The unwind records its marker *before* running the first
compensation, so a compensator whose dispatch is rejected pre-dispatch — an
oversized `{dag_compensate, input, output}` envelope with no payload offloading
— leaves a marker in a run that rolled back **nothing**. Treating that as proof
would 409 a perfectly safe retry and send you to a fresh run, re-running an
upstream node whose side effect is still live. A marker with no dispatch after
it therefore stays retryable.

The dispatched-node condition on signal 2 is what keeps it from misfiring — a
forward node's input is arbitrary user data (a mapped cell, or a bound upstream
output) and may legitimately carry those three keys. Keeping the corroboration
*historical* is what makes it survive every way the currently registered
definition can drift from the run that produced the history (see the limitation
below): the compensator being **renamed**, **removed outright**, a later
definition **reusing** its name as a forward node, or an **older** definition's
forward node sharing a name with a compensator introduced since. Those last two
are why no name-keyed check against the current definition is used in either
direction — `CompensatorNameCollidesWithNode` only guarantees a compensator
does not shadow a node *within one definition version*, never across versions.

Because signal 2 reads a payload-bearing field, the endpoint loads the run's
history **inflated and codec-decoded**. Without that, an oversized compensation
envelope stored as a payload-offload reference (issue #524) — or an encrypted
codec envelope — would hide the three compensation keys, and a marker-less
rolled-back run would look retryable. Offloaded payloads cost one blob fetch
here; the endpoint is operator-invoked and low-frequency.

The inflate/decode pass runs **after the database connection is released**, so a
slow or unavailable payload store can never hold a pool slot and stall unrelated
API or worker work. It **fails closed**: any inflate or decode error fails the
request rather than falling back to the raw history, because a raw history hides
the compensation envelope and would let an already-rolled-back run be retried.

A DAG that failed **without** any compensator declared triggers neither signal
and remains fully retryable, exactly as before.

### A PII-erased run is refused outright

Both signals can be blinded at once by payload erasure (issue #495): the
tombstone replaces the compensation envelope (signal 2), and a run that unwound
at a drained signal frontier recorded no marker for signal 1 to anchor on.
Erasure is irreversible, so nothing downstream can recover the evidence.

The endpoint therefore rejects an erased source run before it resolves anything:

```
HTTP 409 Conflict
{"message":"DAG run's payloads were erased (issue #495), so whether it already
 compensated cannot be determined and its carried-over node outputs are
 tombstones; retrying would resume on unreadable state — start a fresh DAG run
 instead"}
```

This is the right answer even for a DAG with no compensators: the fork carries
over upstream node outputs, which are now tombstones, so a re-executing
downstream node bound with `.input_from(...)` would receive
`{"_harvest_erased": true}` as its input. **Start a fresh run.**

The check runs **twice**, and the second one is the guarantee. The first is a
pre-flight on an unlocked read — it gives you the error above without paying for
the history load. But the handler then releases its database connection to
inflate offloaded blobs, and only afterwards opens the fork's transaction, so an
`erase-payloads` call can land in that window. The fork therefore re-reads the
row **under the same `FOR UPDATE` lock it takes to seal the source**, before
copying a single event. Erasure either committed before that lock (the fork sees
it and refuses) or has to wait behind it (the fork completes on intact events);
there is no interleaving where a tombstoned run is forked. A retry refused at the
second check reports the same `409`, so you never need to care which one caught
it.

### An offloaded payload with no store configured is refused

Signal 2 reads a payload field, so the endpoint inflates offloaded payloads
(issue #524) before applying it. If the deployment that *wrote* the run had a
payload store and the current one does not — the store was unregistered, or
failed to construct at boot — there is nothing to inflate with, and the raw
`_harvest_offload_envelope` reference would otherwise flow through as if it were
the real payload, hiding the compensation envelope.

The endpoint fails closed with a `503`, naming the blob key and the store id the
history points at:

```
history still carries an offloaded payload reference (key '…', store 's3-main')
after the inflate pass; no payload store is configured for that store id, so the
real payload cannot be read — register the payload store this deployment wrote
with
```

**What to do:** re-register that payload store and retry. This is a
configuration problem, not a problem with the run.

## Limitation: build-id routing and topology changes

The retry resolves the fork point and the re-execute / carry-over node sets from
the **currently registered** DAG topology, but the forked run inherits the
**source run's pinned build** (`assigned_build_id`, per the #148 reset and the
safe-deploy contract). That is consistent with reset's "resume under current
code" model and is correct whenever the topology is unchanged.

If you use **worker build-id routing** (Phase 3.7 — opt-in, off by default) *and*
the DAG topology changed since the source run *and* the old and new builds are
not compatible, the worker that replays the fork can execute a different DAG
definition than the one used to pick the cut. The retry could then re-run an
already-successful node or fork at the wrong boundary.

In that situation: dry-run first and confirm the node sets, prefer retrying on a
build whose topology matches the source run, or start a fresh run. A
build-compatibility gate is tracked as follow-up work; v1 does not enforce it.

---

## Audit trail

`reason` and `operator_id` are **required**. They are recorded in the
`harvest_audit_log` (operation `dag.retry`) and embedded in the new run's
`WorkflowResetFork` event; the event's `reason` is augmented with
`dag_retry: nodes=[...]` so the history reads cleanly (see
[audit-trail.md](./audit-trail.md)).

---

## Error reference

| Status | Meaning |
|--------|---------|
| `400` | `from_nodes` empty; an unknown node (the response lists the declared nodes); a node that was never attempted; or a node that already succeeded. |
| `400` | The DAG is classic (non-unified). |
| `404` | The DAG name is not registered, or the run is not a run of that DAG. |
| `409` | The run succeeded (use a fresh run); the run is still running (cancel first); the run **already compensated** ([see above](#a-compensated-run-is-not-retryable-issue-780)); the run's payloads were **erased**, caught either pre-flight or under the fork's row lock ([see above](#a-pii-erased-run-is-refused-outright)); or the fork point lands inside an unresolved upstream side effect (remediation hint included). |
| `503` | The history carries an offloaded payload reference and no matching payload store is configured ([see above](#an-offloaded-payload-with-no-store-configured-is-refused)). Re-register the store and retry. |
| `201` | Retry committed; `new_run_exec_id` identifies the forked run. |
| `200` | Dry-run plan (no write performed). |
