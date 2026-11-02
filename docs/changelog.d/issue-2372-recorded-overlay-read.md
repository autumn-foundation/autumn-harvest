## Recorded overlay read: an activation changes the next op only (issue #2372)

A durable agent now reads the active overlay of its kind once per op.
Parent: #2369. It builds on the overlay kernel of #2370 and the store of
#2371.

- `AgentHarness::overlay_store` installs a `ProfileOverlayStore`.
- The run read `agent_profile` now returns a `ProfileRead`. It flattens the
  profile, so a result that an old worker recorded still decodes. With a
  store, it sets `overlay_store: true`.
- With that flag, each op that runs a segment calls the new activity
  `agent_profile_overlay` with an `OverlayRead`: the kind and the run
  profile. It returns the run profile with the overlay applied, and its
  `overlay_version`. History records both, so a replay never reads the
  store. An old history has no flag and schedules no new activity.
- The overlay applies under the meet of the run ceilings and the current
  harness ceilings. The op takes only the fields that an overlay can set.
  The mandate, the peers and the heartbeat stay those of the run.
- A worker with no store fails the op read with a retryable error.
- Each op drains its stale interrupts before the read, so an interrupt
  during the read stops the op.
- A version that `overlay::apply` refuses fails the op with a
  non-retryable error that names the version. The agent never runs on the
  bare profile in its place.
- `AgentState::profile_version` keeps the version of the last op that read
  one. Each op upserts it as the search attribute `agent_profile_version`,
  as text, for example `"v2"`.

**Proof.** `formal/verus/src/agent_profile_read.rs` mirrors the kernel
`op_profile`. It proves that an active version never yields the bare
profile, and that an op records the version that it applied.

**Deploy order.** An older worker cannot replay a history that holds an
op read. Install the store only after every worker runs this version.

**Tests.** `tests/overlay_read_suite` holds six tests: an exact replay,
the next op, a mid-op activation, the search attribute, and two cases of
bad stored data. `overlay_read.rs` runs them on SQLite, and `overlay_read_postgres.rs`
runs them on a Postgres worker. No `WorkflowEvent` variant and no
migration. Design: `docs/design/DESIGN-2372.md`.
