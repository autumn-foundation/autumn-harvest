# DESIGN-2658: reload the profile of a running agent

Issue #2658. Related: #2372 (the per-op overlay read).

## Problem

The `agent` workflow reads the profile of its kind once, at the start of
a run, through the recorded activity `agent_profile`. A long-lived agent
keeps one run. So a changed profile never reaches it.

A restart does not help. Replay reads the recorded activity result.
`max_ops_per_run` does not help either. The checkpoint starts the next
run at once. That run reads the profile before the restart, and then
waits for its next op.

## Facts (white hat)

- `agent()` builds an `AgentRun` from `lookup_profile` and `discover`.
  Each op reads that run.
- The wake hook reads the run profile for the heartbeat and the
  follow-ups.
- `Entity::max_ops_per_run` is fixed for the run.
- A control op applies at once, also while the agent is paused
  (`agent::inbox::admit`).
- An op that calls an activity is replay-safe. History records the result.
- `sqlite::register` again on a live runtime replaces each activity body.

## Brainstorm

1. A new `Control::ReloadProfile` op. The op reads the profile again
   through `agent_profile`, and the run keeps the new read.
2. Each op reads the base profile, as the overlay read does.
3. The next run reads the profile at its first op, not at its start.
4. A patch gate that re-reads the profile when a worker starts.
5. A helper that sends the reload to each running agent of a kind.
6. A search attribute that holds a profile digest, so an operator can
   find stale agents.
7. The op of idea 1 ends the run at a checkpoint. The next run reads the
   profile at its start.

## Reverse brainstorm: how would the fix fail?

| Failure | Answer |
|---|---|
| Idea 3: a heartbeat agent never wakes, because its wake hook has no profile until an op comes. | Reject idea 3. |
| Idea 2: each op adds one more activity. The mandate, the peers and the heartbeat change in the middle of a run. | Reject idea 2. |
| Idea 4: a worker start is not in history, so replay breaks. | Reject idea 4. |
| The reload reads before the new harness is registered. | The guide says: register first, then reload. |
| A failed reload drops a good profile, so each later op fails. | A failed reload keeps the current profile and records the error. |
| Idea 1 swaps the run in place. That needs shared mutable state next to the entity. harvest-verify reads it as a write through the handler future, so the strict gate reports `unknown`. | Reject idea 1. Take idea 7. |
| Idea 7: a failed read at the next run start fails each work op. | The op reads first. Only a good read asks for the checkpoint. A failed read keeps the run. |
| Idea 7: the new heartbeat waits for the next op. | The reload op books the heartbeat with the profile that it read. |
| Idea 7: a waiting op does not fit the checkpoint input, so the loop runs it first. | The loop keeps the request until the checkpoint happens. |
| Idea 7: a replay sees another request than the live run. | The loop takes the request before the snapshot. Replay runs the handler again, so it takes the same request. |
| An overlay no longer applies, or applies to the old base. | The op read takes the run profile. After a reload that is the new profile. |
| Replay reads the new harness. | The reload is a recorded activity. Replay reads its result. |
| An old worker gets the new op. | It does not decode. The entity counts a failed op and keeps the state. |
| The kind helper misses an agent whose run just started. | It filters on the workflow id prefix `<kind>/`. A new run has that id before it runs any op. |

## Six hats

- **White.** See the facts above.
- **Red.** An embedder expects a new harness to reach its agents. A reload
  op is one line for them. An automatic re-read with no op would surprise
  nobody, but it costs each op an activity.
- **Black.** A reload changes the mandate and the heartbeat. That is the
  purpose of the reload, and the operator asks for it. Each reload adds
  one run and one extra profile read. Reloads are rare, so the cost is
  small.
- **Yellow.** The op is explicit, cheap and replay-safe. It works on
  SQLite and on Postgres, because both run the same workflow. It needs no
  migration and no patch gate. No old history holds the op. A checkpoint
  starts a clean run, so every field changes, `max_ops_per_run` included.
- **Green.** A helper `sqlite::reload_profiles` sends the op to each
  running agent of a kind. An app with a config reload calls it once.
- **Blue.** Ship ideas 7 and 5. Prove the reload kernel in Verus. Test
  the repro of the issue on both backends. Keep the harvest-verify strict
  gate green.

## Decision

- `Control::ReloadProfile`. The op reads the profile through the recorded
  activity `agent_profile`, then runs the peer discovery again.
- The kernel `profile_read::reloaded` picks the profile of the rest of the
  op. A good read replaces it. A failed read keeps it, and the op records
  the error in `AgentState::last_error`.
  `formal/verus/src/agent_profile_read.rs` proves it.
- A good read sets the transient flag `AgentState::reload_requested`. It
  is never serialized. The op books the heartbeat and sets the search
  attributes with the profile that it read.
- The engine hook `Entity::run_with_wakes_and_checkpoints` takes the flag
  after the op, before the snapshot. It then ends the run at a checkpoint.
  The next run reads the profile at its start.
- `sqlite::reload_profiles(rt, kind)` sends the op to each running agent
  of the kind. On Postgres, send the op with `postgres::send`.
  `harvest agents reload <id>` sends it through the management API.

## Acceptance criteria

1. A running agent picks up a changed profile at the next op after a
   `Control::ReloadProfile`, on SQLite and on Postgres.
2. The repro of the issue passes: after a restart with a new harness and
   a reload, the model gets the new system prompt.
3. A change to `system`, `window` or the tool guidance reaches the agent.
4. History records the reload. Replay asks no model and reads no harness.
5. An active overlay applies to the reloaded profile.
6. A failed reload keeps the current profile and names the error.
7. `sqlite::reload_profiles` reloads each running agent of a kind, and no
   other agent. `harvest agents reload <id>` sends the op.
8. The guide, the changelog fragment and this record describe the
   reload.
9. Verus proves the reload kernel. A proptest checks the engine copy.
10. The harvest-verify strict gate proves the `agent` workflow.
11. The new heartbeat is booked at the reload. A paused agent runs its
    held ops on the new profile.
