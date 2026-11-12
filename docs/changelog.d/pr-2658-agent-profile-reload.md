## Reload the profile of a running agent (issue #2658)

A durable agent read its profile once, at the start of a run. A
long-lived agent keeps one run, so a new system prompt, window or tool
guidance never reached it. A restart did not help, because replay reads
the recorded profile. The design is in `docs/design/DESIGN-2658.md`.

- `Control::ReloadProfile` reads the profile of the kind again, through
  the recorded activity `agent_profile`, and discovers the peers again. A
  good read ends the run at a checkpoint after the op. The next run reads
  the profile at its start, so each later op runs on it. The reload op
  books the heartbeat and sets the search attributes with the new profile.
- `Entity::run_with_wakes_and_checkpoints` is a new engine hook. After each
  op, the loop calls `take` on the state, before the snapshot. A `true`
  ends the run at a checkpoint after that op. `run` and `run_with_wakes`
  pass a hook that always gives `false`, so their histories do not change.
- A failed read keeps the current profile. The op adds one to `failures`
  and names the error in `last_error`. The kernel `profile_read::reloaded`
  makes the choice, and `formal/verus/src/agent_profile_read.rs` proves
  it.
- `sqlite::reload_profiles(rt, kind)` sends the op to each running agent
  of a kind, by the workflow id prefix `<kind>/`. `postgres::reload_profiles`
  does the same by the search attribute `agent_kind`. Both send to the live
  run only, with one idempotency key for each call, so an ended agent stays
  ended. `harvest agents reload <id>` sends the op through the management
  API.
- No new event, no migration and no patch gate. No old history holds the
  op, and the request flag is never serialized.
- **Rolling deploy:** send the op only after every worker runs this
  release. An older worker cannot decode the op, and cannot replay a run
  that holds it. `docs/upgrading/0.10.0.md` lists it under "turn on new
  features after the roll".

Tests: `autumn-harvest-sqlite` `entity_checkpoint_request` checks the
engine hook and its replay. `tests/profile_reload.rs` runs the repro of the issue on SQLite,
with and without `max_ops_per_run`. It also covers the window, the tool
guidance, the heartbeat, a paused agent, a failed read, a removed kind and
`reload_profiles`. `tests/overlay_read_suite` checks the reload, its
replay and an active overlay on SQLite and Postgres. The proptest
`the_reload_kernel_never_drops_the_run` checks the engine kernel.
