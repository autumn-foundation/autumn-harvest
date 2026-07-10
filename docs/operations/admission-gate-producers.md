# Admission gate — start-producer contract (issue #618)

The admission gate (issue #377) lets an incident-response operator halt new
workflow starts fleet-wide or for a scoped subset (by workflow name, queue,
shard, or owner) while in-flight executions drain. This page documents the
**contract** every in-process workflow-start producer honours: a *new* start is
either gated or **explicitly, observably exempt** (an exempt producer counts
every start it relays on `harvest.admission.bypassed`). The gate governs new
admissions — it deliberately does not halt in-flight continuation (see
[Out of scope](#out-of-scope--in-flight-continuation-not-new-admission)).

The contract is discoverable at runtime: `GET /admin/gates` returns a
`producers` block (in addition to the active `gates`) enumerating each producer
and whether it is **gated**, **gated-at-admission**, or **exempt-by-design**
with a one-line rationale.

## Contract

| Producer | Status | How |
|----------|--------|-----|
| `api` | gated | The HTTP `POST /workflows/{name}/start` route checks the gate before any DB write. |
| `batch_start` | gated | Each item checks the gate with its resolved target shard. |
| `completion_trigger` | gated | Checks the gate inside the source workflow's terminal-commit transaction before starting the target. A transient gate-DB blip (fail-closed sentinel) **proceeds** rather than dropping in-flight continuation. |
| `webhook_delegate` | gated | Inbound webhooks (issue #344) delegate to the gated HTTP start / signal-with-start route. |
| `scheduler` | gated | Each due schedule slot checks the gate before firing. |
| `debounce` | gated-at-admission | Gated at HTTP admission; the deferred scanner fire relays an already-admitted start and is exempt-with-bypass-counter. |
| `throttle` | gated-at-admission | Gated at HTTP admission; the deferred scanner fire relays an already-admitted start and is exempt-with-bypass-counter. |
| `event_batch` | gated-at-admission | Gated at HTTP admission; the deferred scanner fire relays an already-admitted start and is exempt-with-bypass-counter. |
| `completion_trigger_outbox` | exempt-by-design | Cross-shard completion-trigger relay. The gate was checked at evaluate time before the outbox row was written; the scanner relays that already-accepted work and counts the bypass. |
| `outbox` | exempt-by-design | Relays workflow-start requests durably committed before the gate was raised. |

### `gated-at-admission` (deferred-fire producers)

`debounce`, `throttle`, and `event_batch` defer a start to a background scanner.
They are **gated at HTTP admission** — a start whose scope is gated is refused at
the `POST /workflows/{name}/start` (or batch) request and never deferred. Once a
start *is* admitted (no gate at admission time) and durably deferred, the later
scanner fire relays that already-accepted work even if a gate was raised in the
meantime, and counts it on `harvest.admission.bypassed{producer=…}`. So the gate
still stops *new* admissions immediately; only starts already accepted before the
gate was raised drain past it, and every one is observable.

There is a bounded TOCTOU window between the in-memory gate pre-check and the
deferred-row upsert (shared by all deferred-fire producers). A start that slips
that window is durably deferred and its scanner fire is counted as a bypass, so
it never produces an **un-counted** admission.

### Fail-closed sentinel (completion triggers)

`AdmissionGateCache::check()` returns a synthetic fail-closed sentinel when the
gate cache is uninitialized or after a transient gate-DB read error. For a
completion-trigger start — which is in-flight continuation of already-committed
work — permanently *dropping* the start on a transient infra blip (no operator
gate actually raised) is worse than a sub-second window where triggers aren't
gated. So on the sentinel the completion-trigger path **proceeds** (no block, no
`admission_blocked` fires row, no count), degrading gracefully to the same
behaviour as when no gate cache is installed. A **real** operator gate still
blocks, drops, and counts.

## Completion triggers — dropped, not deferred

A completion-trigger start refused by an active gate is **dropped**, exactly like
a direct API start being refused during an incident — it is *not* queued for
later. The refusal is:

- **counted** on `harvest.admission.blocked{scope,reason_hash}` (the same
  counter a direct API start increments), and
- recorded **exactly once** as a resolved-skip row in
  `harvest_completion_trigger_fires` with `outcome = 'admission_blocked'`, so a
  cascade re-evaluation of the same source terminal dedupes it and never retries
  the start.

The source workflow's terminal commit is **never** rolled back by a gate block —
the block is a clean skip. When no gate is active (or the plugin has not yet
published its gate cache at boot) the completion-trigger path behaves
byte-identically to before this change.

## Outbox — exempt, but observable

The transactional workflow-start outbox relay is **exempt-by-design**: it replays
workflow-start requests that were durably committed to the outbox *before* any
gate was raised. Gating them would drop already-accepted in-flight work, which
is the opposite of the gate contract ("halt **new** starts while in-flight work
drains"). An outbox row is already-accepted in-flight work, not a new admission
decision.

So the outbox is never blocked — but every relayed start increments
`harvest.admission.bypassed{producer="outbox"}` so the exemption is **observable**.
During an incident an operator can watch that counter to confirm exactly which,
and how much, already-committed work is still draining past the gate.

## Metrics

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `harvest.admission.blocked` | counter | `scope`, `reason_hash` | A new start was blocked by an active gate (any gated producer). |
| `harvest.admission.bypassed` | counter | `producer` | An exempt / deferred-fire producer relayed a start (`outbox`, `completion_trigger_outbox`, `debounce`, `throttle`, `event_batch`). |
| `harvest.admission.gates_active` | gauge | — | Current number of active gates. |

`execution.id` is never a metric label (ADR-0001 §7).

## Out of scope — in-flight continuation, not new admission

The gate governs *new* workflow starts. It intentionally does **not** block the
continuation of work that was already admitted. The following paths start
executions without a gate check because each extends an already-accepted run
rather than admitting a new one:

- **Workflow-level retry** (issue #523) — a failed run's own retry.
- **Continue-as-new** — the same logical run forking to a fresh execution.
- **Child / detached-child spawn** — a running parent's sub-orchestration.
- **Reset forks** — an operator re-running an existing execution from history.
- **The in-process typed-client handle** — a start already inside the process.

Gating these would halt in-flight work mid-flight, the opposite of the gate's
"halt **new** starts while in-flight work drains" contract.

Gating starts from *outside* the plugin process, auto-lifting/scheduling gates,
and rate-limited recovery replay are also out of scope (issue #618).
