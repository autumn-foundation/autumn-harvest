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
and whether it is **gated**, **gated-at-admission**, **gated-at-relay**, or
**exempt-by-design** with a one-line rationale.

## Contract

| Producer | Status | How |
|----------|--------|-----|
| `api` | gated | The HTTP `POST /workflows/{name}/start` route checks the gate before any DB write. |
| `batch_start` | gated | Each item checks the gate with its resolved target shard. |
| `completion_trigger` | gated | Checks the gate inside the source workflow's terminal-commit transaction before starting the target. A transient gate-DB blip (fail-closed sentinel) **proceeds** rather than dropping in-flight continuation. |
| `webhook_delegate` | gated | Inbound webhooks (issue #344) delegate to the gated HTTP start / signal-with-start route. |
| `scheduler` | gated | Each due schedule slot checks the gate before firing. |
| `debounce` | gated-at-admission | Gated at HTTP admission; the deferred scanner fire relays an already-admitted start and is exempt-with-bypass-counter. |
| `throttle` | gated-at-relay | Gated **authoritatively at fire time** (issue #1053): the scanner re-checks the gate on the workflow's real queue immediately before firing a deferred start; a matching gate blocks + counts `harvest.admission.blocked` and **re-defers** the row (nothing dropped — it fires once the gate opens or its `schedule_to_start` deadline passes); no gate → fires + counts `harvest.admission.bypassed`. |
| `event_batch` | gated-at-admission | Gated at HTTP admission; the deferred scanner fire relays an already-admitted start and is exempt-with-bypass-counter. |
| `completion_trigger_outbox` | gated-at-relay | Cross-shard completion-trigger relay, gated authoritatively at relay time on the target shard's real queue: a matching gate blocks + drops the row + counts `harvest.admission.blocked`; when no gate matches the relay starts + counts `harvest.admission.bypassed`. |
| `outbox` | exempt-by-design | Relays workflow-start requests durably committed before the gate was raised. |

### `gated-at-admission` (deferred-fire producers)

`debounce` and `event_batch` defer a start to a background scanner. They are
**gated at HTTP admission** — a start whose scope is gated is refused at the
`POST /workflows/{name}/start` (or batch) request and never deferred. Once a
start *is* admitted (no gate at admission time) and durably deferred, the later
scanner fire relays that already-accepted work even if a gate was raised in the
meantime, and counts it on `harvest.admission.bypassed{producer=…}`. So the gate
still stops *new* admissions immediately; only starts already accepted before the
gate was raised drain past it, and every one is observable.

There is a bounded TOCTOU window between the in-memory gate pre-check and the
deferred-row upsert (shared by these deferred-fire producers). A start that slips
that window is durably deferred and its scanner fire is counted as a bypass, so
it never produces an **un-counted** admission.

### `gated-at-relay` (throttle — fire-time gate, re-defer)

Since issue #1053 `throttle` is **gated authoritatively at fire time**, not at
HTTP admission. The HTTP admission check is only a best-effort fast path; the
throttle scanner (`fire_claimed_throttle_row`) re-checks the gate on the
workflow's **real** queue via `check_cached` immediately before firing each
deferred start — the same authoritative relay-time mechanism the
completion-trigger cross-shard outbox uses. This is the correct halt-beats-pace
semantics: an operator arming a gate mid-incident now stops throttled deferred
starts too, closing the last deferred-fire leak (the pre-existing looseness where
a start deferred *before* a gate was armed would otherwise fire gate-exempt once
tokens refill).

- **A gate matches the real queue → BLOCK + RE-DEFER:** the scanner counts
  `harvest.admission.blocked` and **leaves** the `harvest_start_throttle` row
  (refunding the token it reserved) so the held start fires the instant the gate
  opens (or is dropped only if its `schedule_to_start` deadline passes first).
  **Nothing is lost** — the throttle deferred-start `202` promise becomes "will
  run when tokens allow **and** the gate is open at fire time".
- **No gate matches → fire + count the bypass** on
  `harvest.admission.bypassed{producer="throttle"}`.

This shares the `check_cached` relay-time mechanism with the completion-trigger
outbox but **differs in disposition**: the completion-trigger outbox **drops** its
row on block (its source terminal decision has already fired, so a permanently
gated relay must not accumulate); the throttle scanner **re-defers** its row
(nothing lost). Block-vs-bypass never double-counts: a blocked fire returns
without starting, so it is never in the "fired" set that counts a bypass.

### Fail-closed handling (completion triggers)

When the gate cache is uninitialized or a background gate-DB refresh fails, it
goes **fail-closed**: `AdmissionGateCache::check()` (used by the API and other
synchronous producers) returns a synthetic block for *every* start, so a caller
can retry once the cache recovers. A completion-trigger start, however, is
in-flight continuation of already-committed work and **cannot retry** — blocking
it under fail-closed would permanently drop it.

So completion triggers consult the **last-known cached gates snapshot** instead,
via `AdmissionGateCache::check_cached()` (which ignores the fail-closed flag and
returns only a *real* matching gate, never the synthetic block). `set_fail_closed()`
retains the snapshot, so:

- **A real gate already in the snapshot matches this start → block + drop + count.**
  A known-active gate is honoured even under fail-closed — completion triggers do
  **not** bypass an active gate during exactly the incident the gate exists for.
- **No cached gate matches → proceed** (no block, no fires row, no count) — the
  transient-blip / boot-window case, degrading to pre-#618 behaviour rather than
  permanently dropping the start.

This is a deliberate divergence from the API path: when the cache is
initialized-and-healthy both see identical gates; only under fail-closed does the
API block everything (caller retries) while triggers honour only known-cached
gates. **Bounded caveat:** a gate *added* during a gate-DB outage (never loaded
into the snapshot) will not block completion triggers until the next successful
refresh — a narrow, documented degradation, strictly better than either a
permanent drop or an unconditional bypass.

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
the block is a clean skip. When no gate is active the completion-trigger path
behaves byte-identically to before this change.

### Startup ordering — persisted gates are loaded before workers spawn

The plugin populates the gate-cache snapshot with the **persisted active gates**
(the boot-time `load_active_gates` → `refresh`) *before* it publishes the cache
Arc to the process-global static and *before* `HarvestRunner::start` spawns the
worker poll loops and the timeout scanner. So a completion trigger (or a
deferred-fire scanner tick) firing in the boot window consults a snapshot that
already contains the persisted gates, rather than an empty one. The only residual
boot-window gap is a genuine gate-DB outage at startup: if the boot load itself
fails, the snapshot stays empty and completion triggers proceed (the same bounded
caveat as a mid-run fail-closed with no cached match) — the boot-load failure is
never converted into a permanent block-drop, and the direct HTTP start path still
fails closed via `check()`.

### Cross-shard relay — gated authoritatively at relay time

For a **cross-shard** completion trigger (the target workflow hashes to a shard
other than the source's), the authoritative gate check runs at **relay time on
the target shard**, at BOTH relay points — the immediate `DeferredTriggerStart::spawn()`
and the scanner `enforce_completion_triggers_outbox`. Each relay resolves the
**real** target queue on the target shard (including any schedule-level
`queue_name` override) and, immediately before starting, re-checks the gate on
that queue via `check_cached`:

- **A gate matches the real queue → BLOCK:** the relay drops the outbox row
  (deletes it) and records `harvest.admission.blocked` — the completion-trigger
  "block = drop" semantic, now authoritative on the real queue. It does not
  start.
- **No gate matches → start + count the bypass** on `harvest.admission.bypassed{producer="completion_trigger_outbox"}`.

Both are **gated on the outbox-row delete** for exactly-once: a row is processed
by exactly one relay path (the immediate spawn deletes on block/start; the
scanner only picks up rows the immediate spawn didn't delete), and a row is
counted **either** blocked **or** bypass, never both.

`check_cached` (the last-known snapshot, honoured even under a transient
fail-closed blip) is used at relay time — not the fail-closed `check()` — because
the relay materializes **pre-committed in-flight-continuation** work, so the same
no-permanent-drop rationale as the completion-trigger inline path applies.

The **source-side inline check** in `evaluate_triggers_for_execution`
(`resolve_cross_shard_target_queue`) remains as a **best-effort fast path** — it
resolves the target queue on the target shard's connection when reachable — but
it is no longer relied on for correctness: when the target pool is transiently
unavailable it falls back to the shard-independent default queue and could miss a
`Queue(real-q)` gate, which the authoritative relay-time check then catches.
Same-shard targets are gated inline at evaluate time (the source connection *is*
the target shard's connection) and never take the cross-shard relay path.

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
