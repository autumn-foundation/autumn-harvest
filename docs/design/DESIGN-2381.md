# DESIGN-2381: the adoption gate of a refine run

Issue #2381. Parent: #2369. Needs #2371 and #2380. Policy: #2394.

## Problem

`agent_refine` (#2380) ends with a chosen overlay. Nothing puts it live.
The auto-adopt path of #2394 adopts only the parts that an operator
lists. By default, every model-written overlay must wait for a person.
The wait must survive a crash, end at a deadline, and never put a stale
overlay live.

## Decision

`autumn-harvest-agent/src/refine/gate.rs` holds the gate. `agent_refine`
runs it after its loop, when the loop chose an overlay.

1. The activity `agent_refine_adopt` calls `refine::adopt`. A candidate
   that the policy allows goes live as `auto:<run>`. Any other candidate
   is put as a new, inactive version with the provenance `refine:<run>`.
2. The run drops each signal `refine_approval:<run>` that arrived before
   this point. It sets the search attributes `refine_kind` and
   `refine_status = waiting_approval`.
3. The run waits on `refine_approval:<run>` until
   `RefinePolicy::approval_timeout` passes. The wait reads the durable
   clock `system_now`. A refused decision does not reset the deadline.
4. The activity `agent_refine_decide` applies a decision to the store.

| Decision | Effect | `refine_status` |
|---|---|---|
| `approve` | `activate(kind, v, expected_active = the version active at refine start)` | `adopted` |
| `edit` with `overlay` | `overlay::apply` under the run ceilings, `put` as `refine:<run>`, then activate that version | `adopted` |
| `reject` | Nothing is activated. The version stays stored and inactive. | `rejected` |
| no decision before the deadline | As `reject`. | `timed_out` |
| `approve` or `edit` after a pointer move | The CAS fails. Nothing is activated. | `superseded` |
| a refused `edit`, or a payload that does not decode | Nothing changes. The run waits until the same deadline. | `waiting_approval` |

The payload uses the `verdict` tag of `Approval` (`approval.rs`). An
`edit` also accepts the key `arguments` of `Approval::Edit`. The run reuses
`approval::await_decision`, so a payload of the wrong shape cannot fail
the run.

The run sets `refine_kind` and `refine_status` at each end, also for
`unchanged`, `budget_exhausted` and `auto_adopted`. `harvest agents profile
pending <kind>` lists the runs with `refine_status = waiting_approval`.
`approve`, `reject` and `edit` send the signal.

`RefineInput::dry_run` ends the run after the loop, with `chosen`. It
stores nothing and opens no gate.

### The kernel

`gate_step(event)` is pure. Its events are `Deadline`, `Unreadable`,
`Reject`, `Activated`, `Refused` and `Conflict`. It returns `Wait` for
`Unreadable` and `Refused`, and one end for each other event.
`formal/verus/src/refine_gate.rs` proves:

- Only `Activated` gives `Adopted`.
- A fold over any run of events adopts only when an `Activated` event comes
  before every other end.
- A gate with no `Activated` event never adopts.
- An end is final. A later event changes nothing.

### Idempotent activities

- `agent_refine_adopt` finds its own `refine:<run>` version with the same
  overlay before it puts. A retry after a lost reply puts nothing.
- `agent_refine_decide` finds its own `refine:<run>` version with the
  edited overlay before it puts. A retry that sees the target active,
  with the last move by `refine:<run>`, reports `adopted`.
- The store refuses a stale pointer, so an operator move during the wait
  always wins.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where the gate runs | A new workflow. A child run. The end of `agent_refine`. | The end of `agent_refine`. The issue asks for it. One run id names the signal, the version and the attributes. |
| The payload | `Approval` as it is. A new enum. | A new enum `RefineDecision` with the same tag. `edit` needs an overlay, not tool arguments. It also accepts `arguments`. |
| The expected version | The pointer at the gate. The pointer at refine start. | The pointer at refine start. The candidate was built on it. |
| A refused `edit` | End the run. Keep the wait. | Keep the wait, as the issue asks. The deadline stays. |
| An unreadable payload | End as `rejected`. Keep the wait. | Keep the wait. A typo of a person must not lose the candidate. Nothing goes live. |
| The deadline clock | `ctx.now()`. `system_now`. | `system_now`. `ctx.now()` is the start time of the run. `system_now` records the wall clock once at each read. |
| An early signal | Read it. Drop it. | Drop it, as eval does. A decision needs a reviewer who saw the open wait. |
| The actor of the move | The run. A name in the payload. | The run, `refine:<run>`. History holds the signal. A name in a payload is caller text. |
| Old loop tests | Rewrite each one. A dry run. | A dry run. It also lets an operator see a candidate with no stored version. |

## Reverse brainstorm

How can the gate put the wrong overlay live? Each failure has a guard.

| Failure | Guard |
|---|---|
| A stale approval releases a later gate. | One gate per run. The name holds the run id. |
| An approval sent before the wait opens goes live. | The run drops each early signal. A test checks it. |
| A timeout activates. | `gate_step(Deadline)` is `TimedOut`. The proof shows that only `Activated` adopts. |
| An approval overwrites a newer operator version. | The CAS on the start pointer. The run ends as `superseded`. |
| An `edit` passes the ceilings. | `overlay::apply` under the run ceilings, then the store validator under the current policy. Both must pass. |
| An `edit` changes `auto_adopt` or another policy key. | `ProfileOverlay` is `deny_unknown_fields`. The payload does not decode. |
| A retry puts a second version. | Each activity finds its own version first. |
| A retry of `approve` reports `superseded`. | It reads the last move. A move by `refine:<run>` to the target is its own. |
| A refused `edit` restarts the deadline. | The deadline comes from the open time and the recorded policy. |
| A crash during the wait loses the deadline. | The open time is a recorded side effect. Each wait is a durable timer. |
| An operator cannot find a waiting run. | `refine_status = waiting_approval`, and `harvest agents profile pending`. |
| A finished run still shows as waiting. | The run sets `refine_status` at each end. |
| A policy change widens a waiting run. | The run reads the policy once. The timeout is the recorded one. |
| A bad payload fails the run. | `await_decision` gives `Unreadable`. The wait goes on. |
| A long reject reason fills history. | The report cuts each note to 500 characters. |

## Six hats

- **White.** `refine::adopt` stores a waiting candidate as `refine:<run>`.
  `gate_state` gives `Waiting` or `TimedOut`. `await_decision` decodes a
  payload. `activate` is a CAS. `system_now` is a recorded clock. The
  SQLite runtime has a test clock and a search attribute filter.
- **Red.** A person must trust that nothing goes live without a click. A
  silent activation is the worst outcome.
- **Black.** A run started before this release replays into the gate. Its
  start record has no active version, so it expects an unset pointer.
  With an active version, its approval ends as `superseded`. A dropped
  early signal can surprise a fast operator, so `pending` lists a run only
  after the wait opens.
- **Yellow.** An operator gets one list of waiting runs, one signal per
  run and an audit of each move.
- **Green.** Later: an approval route in the plugin with the operator id,
  and a notice when a gate opens.
- **Blue.** Spec and proof of the kernel first. Then red tests on SQLite.
  Then the activities, the wait, the CLI and the gates.

## Limits

- The move actor is `refine:<run>`, not the person. History holds the
  signal.
- An early signal is dropped with no reply. The report counts it.
- A run started before this release expects an unset pointer.
- The `filter` grammar of `GET /workflows` is Postgres only. SQLite lists
  the same attributes through `ExecutionFilter`.

## Tests

`autumn-harvest-agent/tests/refine_gate.rs` covers each criterion on
SQLite. `refine/gate.rs` unit tests and proptests cover the kernel and the
payload. `agent_profile.rs` tests cover the CLI routes.
