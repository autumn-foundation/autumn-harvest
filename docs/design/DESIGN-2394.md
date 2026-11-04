# DESIGN-2394: the operator refine policy

Issue #2394. Parent: #2369. Needs #2371 and #2374. Read by #2370, #2372,
#2378, #2379, #2380, #2381 and #2382. ADR:
[0014](../adr/0014-operator-refine-policy.md).

## Problem

The refine loop makes policy choices. Which change goes live without a
person? How many a day? Which tool effects may a probe run? How far may
an overlay move a bound? How long does an approval wait? How large is the
lesson library? An operator decides each one, at run time, with no
restart. The loop must never widen its own gate.

## Decision

A `RefinePolicy` for each agent kind. Each store of overlay versions also
holds the policy log of each kind. A missing policy means every default.

| Setting | Default | Read by | Enforced today |
|---|---|---|---|
| `auto_adopt.parts` | `[]`, so every overlay waits | `ProfileOverlayStore::auto_adopt`, through `refine::adopt` (#2381) | Yes, in the store |
| `auto_adopt.max_per_day` | `1` | `ProfileOverlayStore::auto_adopt` (#2381) | Yes, in the store |
| `approval_timeout` | `7d` | `RefinePolicy::gate_state` (#2381) | No. The gate of #2381 will call it. |
| `probe_effects` | `[read_only]` | `eval::live_probe` (#2378) | Yes, in the tool-call activity of the probe. `agent_refine` (#2380) will pass it. |
| `ceilings` | the profile values and model | the store validator and the op read (#2370, #2372) | Yes |
| `proposal_attempts` | `2` | `ProposeRequest::attempts` (#2379) | No. The workflow of #2380 will copy it. |
| `lessons.max_entries` | `64` | the lesson library (#2382) | No. #2382 will read it. |

### The type

`refine/policy.rs` holds `RefinePolicy`. Every struct is
`deny_unknown_fields`. A missing key takes its default. Each bounded value
checks its bound when it decodes, so a decoded policy is valid.

| Value | Bound |
|---|---|
| `max_per_day` | `0` to `1000`, or `"unlimited"` |
| `approval_timeout` | `1m` to `90d`, as `<n><s\|m\|h\|d>` |
| `ceilings.max_*` | `1` or more. `null` is the profile value. |
| `ceilings.models` | 1 to 64 names. `null` is the profile model. |
| `proposal_attempts` | `1` to `8` |
| `lessons.max_entries` | `1` to `4096` |

`ceilings.resolve(profile)` returns `Ceilings`. A set value replaces the
profile value. The policy can raise a ceiling above the profile. The
operator chooses that. `allow_clear_read_only` stays `false`.

### The adoption kernel

`changed_parts(active, candidate)` names each part that differs:
`system`, `guardrails`, `tool_guidance`, `skills`, `model`, `bounds` or
`read_only`. `bounds` covers the three `max_*` fields.

`decide_adoption(allowed, changed, used_today, cap)` is pure. It returns:

1. `Unchanged` when no part changed. A change that changes nothing never
   counts against the cap.
2. `NeedsApproval` when a changed part is not allowed.
3. `CapReached` when `used_today` is at the cap.
4. `Adopt` else.

The empty set is a subset of every set. Rule 1 stops an empty change from
going live under the default policy.

### The adoption step

`refine::adopt(store, kind, candidate, run, policy)`:

1. Read the active version. When it is `auto:<run>` with the same
   overlay, a retry lost its reply. Report `auto_adopted`.
2. Call `ProfileOverlayStore::auto_adopt` with the run policy and the
   active version as the expected one.
3. On a wait, put the version as `refine:<run>`. It waits for the gate of
   #2381.

The store enforces the gate itself:

1. It reads its current policy. A part is allowed only when the run policy
   and the current policy both allow it. The stricter daily cap counts.
   So a change during a run never widens that run. A narrowing reaches it
   at once.
2. It compares the candidate with the expected version, and runs the
   kernel. `Unchanged` and `NeedsApproval` change nothing.
3. One transaction puts the version as `auto:<run>`, checks the pointer,
   counts the auto moves of the UTC day of the store clock, and moves the
   pointer. `decide_auto_move` decides under the head lock. A full cap
   rolls the transaction back. So two runs at once never pass the cap.
4. The transaction first checks that the policy revision is the one that
   the gate read. On Postgres, a policy write and an adoption of one kind
   take the same advisory lock. On SQLite, the `IMMEDIATE` transaction
   holds the write lock. A changed policy refuses the move, and the store
   runs the gate again, at most four times. So a policy change that
   commits first always binds the adoption.
5. `put`, `activate` and `rollback` refuse an `auto` actor. So each auto
   move passes the count.
6. The embedder sets the store clock. No caller of a store method picks
   the day of the cap.

| Result | `refine_status` |
|---|---|
| Adopted | `auto_adopted` |
| Waits | `awaiting_approval`, with the reason |
| No change | `unchanged` |

A wait on the cap writes a `harvest.audit` tracing event that names the
cap. The activation log records each auto move with the actor
`auto:<run>`.

### The gate clock

`gate_state(requested_at, now, timeout)` is `Waiting { until }` or
`TimedOut`. `TimedOut` maps to `refine_status = timed_out`. The gate of
#2381 calls it with its durable clock.

### The probe rules

`RefinePolicy::probe_rules()` returns `ToolRules`. A listed effect runs. A
tool with another effect, or an unknown tool, is denied. The probe of
#2378 checks the same set in its tool-call activity instead, so a policy
cannot bypass it. See [DESIGN-2378](DESIGN-2378.md).

### Storage

Each store keeps an append-only policy log of each kind:

```sql
harvest_agent_refine_policies
    (kind, seq) PK, old_body, new_body, actor_kind, actor, changed_at
```

- The current policy is `new_body` of the last row. `NULL` means the
  defaults. No row also means the defaults.
- `set_policy` is a compare-and-swap on the last `seq`. A stale
  `expected_revision` is a `Conflict`.
- `reset_policy` appends a row with `new_body = NULL`. It needs no
  revision, because its result is the same from any state.
- The insert copies `old_body` from the last row in the same statement.
  Two writers that read the same last row take the same `seq`. The
  primary key refuses the second one. So `old_body` is always the body
  before.
- A trigger refuses each `UPDATE`. So an audit row never changes.
  `DELETE` is not guarded, as for the activation log. A manual delete of
  the last row can reuse its revision.
- Only `Provenance::Operator` may write. A refine run or an auto run gets
  `InvalidProvenance`.

`Provenance::Auto(run)` is new. Its stored author kind is `auto`. The
migration widens the `CHECK` of both overlay tables. `SQLite` has no
`CHECK` on the author kind.

### The validator and the op read

`OverlayValidator::validate` takes the policy. Each store reads its own
policy before a put, an activation, a rollback and an auto adoption. The
harness validator applies under `policy.ceilings.resolve(profile)`.

The op read of #2372 reads the policy in the same recorded activity. Both
checks apply under the resolved ceilings. A replay reads the record.

### The run read

The activity `agent_refine_policy(kind)` returns the policy. A refine run
(#2380) calls `read_refine_policy` once at its start, and passes the
result to each step. History records it, so a change during the run does
not reach that run. The next run reads the new policy. A worker with no
store fails the read as retryable.

### Routes and CLI

| Route | Class | CLI |
|---|---|---|
| `GET /agents/{kind}/profile/policy` | read | `policy <kind> show` |
| `PUT /agents/{kind}/profile/policy` | mutate | `policy <kind> set` |
| `POST /agents/{kind}/profile/policy/reset` | mutate | `policy <kind> reset` |
| `GET /agents/{kind}/profile/policy/changes` | read | `policy <kind> history` |

- `PUT` takes `{policy, expected_revision}`. Both keys are required.
- Each write adds one audit row, `agent_profile.policy_set` or
  `agent_profile.policy_reset`. `error_summary` names each key that
  changed, with its old and new value. The store log keeps both whole
  bodies.
- `harvest-operator` has the `mutate` scope. A viewer gets `403` from
  the role layer, before the handler runs.
- `set` with flags reads the policy, changes the named keys and sends the
  `PUT` over the revision that it read.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Where to store | A new trait. The overlay store. The audit table. | The overlay store. The issue says "next to the overlay versions". One install reaches the harness, the validator and the plugin. |
| Table shape | A current row and a log. A log only. | A log only. The last row is the current policy. The log is the audit of old and new values on every backend. |
| A concurrent `set` | Last write wins. A CAS. | A CAS on the revision, as `activate` needs `expected_active`. |
| How `reset` stores | Delete the row. A row with `NULL`. | A row with `NULL`. The log keeps who reset it. |
| The daily cap | Count before the put. Count in the move. | Count in the move, under the head lock. A count before races. |
| The cap clock | Database time. The caller time. A store clock. | A store clock that the embedder sets. A caller cannot pick the day, and a test can pin it. |
| Who enforces the gate | The caller. The store. | The store, with the stricter of the run policy and its current policy. A caller with a store cannot skip the parts or the cap. |
| Where the cap counts | All moves. Auto moves. | Auto moves, by the author kind `auto`. |
| An empty change | Adopt. Wait. Skip. | Skip, as `unchanged`. It must not spend the cap or go live. |
| The auto provenance | A flag. A new author kind. | A new author kind `auto`. The issue asks for `auto:<run>`. |
| Ceilings above the profile | Refuse. Allow. | Allow. The operator chooses, and the AC asks for it. |
| `null` ceiling | No limit. The profile value. | The profile value. A policy can then never drop a limit. |
| Duration form | Seconds. `<n><unit>`. | `<n><unit>`. `1h` and `7d` read well in a CLI and a log. |
| Probe gate | A new trait. `ToolRules`. | `ToolRules`. `Strictest` already combines it. |

## Reverse brainstorm

How can the policy fail its promise? Each attack has a guard.

| Attack | Guard |
|---|---|
| A proposer writes `auto_adopt` into an overlay. | `ProfileOverlay` is `deny_unknown_fields`. |
| An `AgentOp` or a `ProposeRequest` carries a policy. | An `AgentOp` refuses each policy key. A `ProposeRequest` refuses each unknown key. Its `ceilings` field holds the resolved ceilings of the recorded read. A test checks each policy key. |
| A refine run sets the policy. | The store accepts only an operator actor. |
| A caller of the store skips the parts or the cap. | `auto_adopt` reads the current policy and enforces the stricter one. `put`, `activate` and `rollback` refuse an `auto` actor. |
| A caller backdates its clock for a fresh cap. | The store clock counts the day. No caller passes a time. |
| A future `ToolEffect` slips past `probe_rules`. | An exhaustive `match` lists every effect, so a new one fails to compile. |
| A long audit summary hides a widening. | The gate settings come first. A list shows only what it adds and removes. A cut summary names its revision. |
| A viewer sets the policy. | The route is `Mutating`. The role layer refuses it with `403`. |
| Two refine runs pass a cap of 1 at once. | The count runs under the head lock, in the move. |
| An operator resets the policy between the gate and the move. | The move checks the policy revision under the policy lock, and runs the gate again. |
| An empty overlay goes live under the defaults. | The kernel returns `Unchanged` first. |
| A policy sets `max_per_day: 1000000`. | The decode refuses a value above 1000. |
| A policy sets `approval_timeout: "0s"`. | The decode refuses a value under 1 minute. |
| A `null` ceiling removes a limit. | `null` means the profile value. |
| A change mid-run widens that run. | The run reads the policy once, through a recorded activity. |
| A replay reads a new policy. | The read is an activity. Replay takes the record. |
| An old worker drops a new policy key. | The decode refuses an unknown key, so the read fails. |
| Two operators set at once, and one change is lost. | The CAS refuses the stale write with `409`. |
| An audit row is rewritten. | A trigger refuses each `UPDATE`. |
| A lowered ceiling leaves a wide version active. | The op read applies under the new ceilings and fails the op. |
| A probe runs an unknown tool. | `probe_rules` denies an unknown tool. |
| A cap wait leaves no trace. | A `harvest.audit` event names the cap. |

## Six hats

- **White.** The overlay store, its log and its routes exist (#2371,
  #2374). `Ceilings` has public fields (#2370). `ProposalAttempts` exists
  (#2379). The refine workflow (#2380), the gate (#2381), the probe
  (#2378) and the lessons (#2382) are open.
- **Red.** An operator fears a loop that edits itself into production.
  The defaults must never adopt. The operator must see each change.
- **Black.** Raised ceilings can let an overlay pass the profile. Only an
  operator can raise them, and each change is audited. A policy read and
  an overlay read are two statements, so an op can see a policy one change
  older than its overlay. Each one is a valid snapshot. A run keeps the
  gate that it read, as the AC asks. An operator who narrows the gate
  during a run can roll back an adoption of that run. The store still
  validates under the current ceilings, so a lowered ceiling refuses at
  once.
- **Yellow.** Each consumer gets one typed value and one pure kernel. The
  open issues only call them.
- **Green.** Later: a ceiling on the length of `system`, as #2370 notes.
  A per-tenant policy.
- **Blue.** Verus spec and proof of the kernel. Then red tests. Then the
  type, the stores, the migration, the activity, the routes and the CLI.
  Then the gates.

## Scope

This issue ships the policy, its storage, its routes and CLI, and each
kernel that a consumer calls. The ceilings, the auto-adopt parts and the
daily cap are enforced today. These consumers are open, and they will call
the kernels:

- #2380 will call `read_refine_policy` once per run, and copy
  `proposal_attempts` into each `ProposeRequest`.
- #2381 will call `refine::adopt`, and `RefinePolicy::gate_state` for
  `timed_out`.
- #2378 checks `probe_effects` in the tool-call activity of the probe.
- #2382 will read `lessons.max_entries`.

The AC names an `agent_refine` input. #2380 builds it. `ProposeRequest`
stands in for it today. It refuses each policy key except `ceilings`,
which holds the resolved ceilings of the recorded read.

## Proof

`formal/verus/src/refine_policy.rs` mirrors `parts_allowed`,
`decide_adoption` and `gate_timed_out`. It proves:

- An `Adopt` needs a change and every changed part allowed.
- With no allowed part, no overlay is adopted.
- An empty change is `Unchanged`.
- A larger allowed set never turns an `Adopt` into a wait.
- The gate times out exactly when `now` reaches `requested + timeout`, and
  a later `now` stays timed out.

The stores check the cap with `decide_auto_move` under the head lock.
`formal/verus/src/overlay_log.rs` mirrors it. It proves that a stale
pointer always conflicts, and that the auto moves of one UTC day never
pass the cap.

## Tests

| Acceptance test | Where |
|---|---|
| Defaults, and a `system` overlay waits | `policy.rs` `defaults_*`, `refine_policy.rs` `with_no_policy_a_system_overlay_waits` |
| `parts = [system]` adopts, `tool_guidance` waits | `refine_policy.rs` `a_system_policy_adopts_a_system_overlay_at_once` |
| `parts = [system, model]` adopts both | `refine_policy.rs` `a_system_and_model_policy_adopts_both` |
| `max_per_day = 1`, and `unlimited` | `refine_policy.rs` `the_daily_cap_makes_a_second_refine_wait`, the store suite, the Postgres race |
| `approval_timeout = 1h` | `policy.rs` `a_one_hour_gate_times_out_after_one_hour` |
| `probe_effects` | `policy.rs` `probe_effects_gate_each_tool` |
| `ceilings.max_steps = 40` | `policy.rs` `a_policy_ceiling_can_raise_the_profile_bound`, the store suite, `overlay_read_suite` |
| A second process | `refine_policy.rs` `a_set_reaches_a_running_second_process` |
| One read per run | `refine_policy.rs` `a_run_reads_the_policy_once` |
| `403` for a viewer | `agent_profile_integration.rs` `a_viewer_cannot_set_or_reset_the_policy` |
| Policy keys fail to decode | `policy.rs` `no_overlay_or_input_decodes_a_policy_key`, `a_propose_request_refuses_each_policy_key` |
| `reset`, and its audit | the store suite, `agent_profile_integration.rs` |
| `rollback` after an auto adoption | `overlay_read_suite` `a_rollback_after_an_auto_adoption_restores_v1` |
