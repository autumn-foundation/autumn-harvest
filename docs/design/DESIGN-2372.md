# DESIGN-2372: the recorded overlay read

Issue #2372. Parent: #2369. Needs: #2370 and #2371.

## Problem

A durable agent reads its profile once per run, through the recorded
activity `agent_profile`. So a deploy changes the next run only. An
overlay that an operator activates in a `ProfileOverlayStore` must reach
the next op of a running agent. It must never change an op that already
started, and it must never change a replay.

## Decision

Each op reads the active overlay through a recorded activity. The op runs
on that read to its end.

| Part | Where | What |
|---|---|---|
| `AgentHarness::overlay_store` | `harness.rs` | Installs a store. |
| `ProfileRead` | `agent/profile_read.rs` | A profile, its `overlay_version`, and the `overlay_store` flag. |
| `agent_profile` | `agent/mod.rs` | The run read. It returns the bare profile and the flag. |
| `OverlayRead` | `agent/profile_read.rs` | The input of the op read: the kind and the run profile. |
| `agent_profile_overlay` | `agent/mod.rs` | The op read. It returns the run profile with the overlay applied, and its version. |
| `op_profile` | `agent/profile_read.rs` | The kernel that picks the profile of an op. Verus proves it. |
| `AgentState::profile_version` | `agent/mod.rs` | The version of the last op that read a profile. |
| `agent_profile_version` | search attribute | That version, as text, for example `"v2"`. |

### The run read

`agent_profile` keeps its name and its input. Its output becomes
`Option<ProfileRead>`. `ProfileRead` flattens the profile, so a result that
an old worker recorded still decodes. Such a result has
`overlay_store: false`, so its run takes no op read. An old history
therefore replays with no new activity.

The run read never applies an overlay. A bad overlay then fails only the
op that reads it, and the next op reads the store again.

### The op read

When the run read reports `overlay_store: true`, each op that runs a
segment calls `agent_profile_overlay` first, with an `OverlayRead`: the
kind and the run profile. The activity reads the active version of the
kind. The version must still validate under the current harness profile,
as a put and an activation check it. Then it applies to the run profile
under the run ceilings. Both checks use `Ceilings::from_profile`, and the
harness is the registry. So each bound that the version sets is at most
the run bound and the deploy bound. A deploy that changes the profile
model refuses only a version that names a model.

| Store state | Result |
|---|---|
| No store installed on this worker | A retryable failure. The run read found a store, so another worker can take the retry. |
| No active version | The run profile, version `None`. |
| An active version that applies | The applied profile and its version. |
| An active version that `overlay::apply` refuses | A non-retryable failure that names the version. |
| A corrupt row | A non-retryable failure. |
| A backend error | A retryable failure. |

A failed read fails the op. `last_error` names the version, and no model
call runs. The agent never falls back to the bare profile.

### The op profile

The op takes the overlay fields of the read over the run profile:
`system`, `model`, `max_steps`, `max_total_tokens`, `max_output_tokens`,
`read_only`, `tool_guidance` and `skills`. With version `None`, the op runs
on the run profile as it is. So the mandate, the peers, the heartbeat and
the other run fields stay those of the run. A field that the overlay does
not set also keeps its run value, because the read applies to the run
profile. A deploy still changes the next run only. An activation changes
the next op only.

A heartbeat sets its own bounds and model for a tick. They take precedence
over the overlay, as they already do over the profile. An overlay can only
narrow the profile, so this widens nothing that an operator did not set.

### The op start

Each op checks the run and drains the stale interrupts before the op read.
Without a store, this keeps the order of the old code exactly. With a
store, an interrupt that arrives during the read stops the op.

### Deploy order

An older worker cannot replay a history that holds `agent_profile_overlay`.
Install the store only after every worker runs this version. Do not roll
the code back while runs with a store are in flight.

### The search attribute

The op stores the version in `AgentState::profile_version`. Each op
upserts `agent_profile_version` from it. With no version, the upsert
removes the key. The text form matches `OverlayVersion`'s `Display`, so
`filter=attrs.agent_profile_version = 'v2'` lists the agents whose last
op ran on version 2.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| When to read | Once per run. Once per op. Once per model turn. | Once per op. A run read misses the next op. A turn read changes an op in flight. |
| How to gate the read | Always. A `patched` marker. A flag in the run read. | The flag. An old history has no flag, and a harness with no store pays no activity. |
| Which activity | Change `agent_profile`. A new activity. | Both. `agent_profile` reports the flag. A new `agent_profile_overlay` reads per op, so the run read keeps its input. |
| What the op runs on | The whole read profile. The run profile with the overlay fields of the read. | The overlay fields only. The run fields keep the run guarantee. |
| The base of the apply | The current harness profile. The run profile. | The run profile, passed as the input. A deploy then never widens an op. |
| A worker with no store | The bare profile. A retryable failure. | A retryable failure. The run read found a store, so a bare op would skip an active version. |
| A bad overlay | Run bare. Fail the op. Fail the run. | Fail the op. The next op can read a fixed pointer. |
| The attribute value | A number. Text. | Text, `"v2"`. The filter of the issue quotes it, and errors print the same form. |

## Reverse brainstorm

How can the read break replay or the operator's intent? Each attack has a
guard.

| Attack | Guard |
|---|---|
| A replay reads the store again. | The read is an activity. Replay takes its recorded result. |
| An old history meets new code. | An old run result decodes with `overlay_store: false`, so no op read is scheduled. |
| An activation lands while an op waits on an approval. | The op read happens once, before the segment. Each turn of the op uses that read. |
| A model leaves the harness after a version is stored. | The op read applies the version again, with the harness as the registry. `UnknownModel` fails the op. |
| A deploy narrows the profile under an old version. | The op read applies under the current profile. `AboveCeiling` fails the op. |
| The op read fails, and the agent runs bare. | The kernel `op_profile` returns an error for an active version that does not apply. Verus proves that an active version never yields the bare profile. |
| A retry hides a bad version. | A refused overlay is a non-retryable failure. |
| An op read changes the mandate or the peers mid-run. | The op takes only the overlay fields from the read. |
| A deploy widens a field that the overlay does not set. | The read applies to the run profile, under the run ceilings. |
| A deploy narrows a bound under a stored version. | The version must still validate under the current harness profile. |
| A refused peer moves the version. | The op sets the version only after the model check. |
| A worker with no store runs the op bare. | That worker fails the read with a retryable error. |
| An interrupt during the read is dropped as stale. | The drain comes before the read. |
| The attribute names a version after the store goes. | A work op with no store clears the version. |
| A control op clears the attribute. | The attribute comes from the state, so an op that reads nothing keeps it. |

## Six hats

- **White.** `agent_profile` runs once per run today. `ProfileOverlayStore`
  and `overlay::apply` exist. `upsert_search_attrs` appends no event.
- **Red.** An operator expects an activation to reach the next message. An
  operator also expects a broken version to stop the agent, not to run it
  bare.
- **Black.** Each op with a store records one more activity, with the whole
  applied profile. A profile with many skills can be near 1 MiB, so the
  history grows faster. The input holds the run profile too. A worker
  without a store fails the read, so install the store on every worker.
- **Yellow.** Rollback is an activation of an older version, so it reaches
  the next op too. `harvest agents list` can filter by version (#2374).
- **Green.** #2394 can pass operator ceilings to the op read. The kernel
  stays the same.
- **Blue.** Verus spec and proof of `op_profile`. Then the red suite on
  SQLite and Postgres. Then the code. Then the gates.

## Proof

`formal/verus/src/agent_profile_read.rs` mirrors `op_profile`. It proves:

- With no active version, the op runs on the base profile, with no version.
- With an active version, an `Ok` result holds the applied profile and that
  version. It is never the base profile with no version.
- With an active version that does not apply, the result is an error that
  names the version.

## Tests

`autumn-harvest-agent/tests/overlay_read_suite/mod.rs` holds one suite of
six tests. `tests/overlay_read.rs` runs it on SQLite with the SQLite
store. `tests/overlay_read_postgres.rs` runs it on a Postgres worker with
the Postgres store.

1. Replay is exact.
2. The next op sees v2.
3. A mid-op activation is ignored.
4. The search attribute follows the op.
5. Bad stored data fails safe: a removed model.
6. Bad stored data fails safe: a lowered bound, where the bare profile
   could run.
