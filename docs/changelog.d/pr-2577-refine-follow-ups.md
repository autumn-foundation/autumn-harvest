## `agent_refine` follow-ups: agent sources, a keyed live fork, per-turn probe rows (issue #2577)

Four limits of the `agent_refine` workflow of issue #2380. The design is in
`docs/design/DESIGN-2577.md`.

- **A durable `agent` source.** `agent_refine_start` refuses an `agent` run
  before the end and kind checks. Each `agent` run now gets
  `RefineSourceNotReplayable`, of any kind and in any state. Before, a run
  of another kind got `RefineSourceOtherKind`, and a live one got
  `RefineSourceInFlight`.
- **A keyed live fork.** `agent_refine_probe` passes its activity
  idempotency key to the host in `LiveRequest::fork_key`. The new
  `eval::live_probe_keyed` forks under `eval::probe_fork_id(key)`. It finds
  a fork that holds the id first, and forks nothing. A concurrent fork that
  wins the id gives `EvalError::ForkIdInUse`, and the loser reads the
  winner. A run of another source that holds the id is an error.
  `LiveProbeHost` gets `find_fork` and `fork_live_as`. Their defaults
  return `Unsupported`, so SQLite needs no change. A worker crash during
  the wait now costs no second fork.
- **Per-turn probe rows.** `Evaluation::candidate_calls` holds the model
  id, the tokens and the cost of each candidate turn that called a model.
  A probe writes one cost ledger row for each, in place of one row under
  the unknown model id. The anonymised pool leaves the field out.
- **The id of a keyed fork.** A sharded router can send `agent-probe-<key>`
  to another shard. The Postgres host then adds the first suffix that the
  router sends to the source shard.
- **The call cap.** A probe with more than 256 turns writes one row for
  each model, so the rows still hold the whole spend.
- **A replaced attempt.** The design states why the refine ledger cannot
  hold its spend exactly. A retry of a regular activity writes no event.
  A retry reads only the error text of the attempt before it. A heartbeat
  can lose to the requeue. A crash gives no failure details.

**API notes.** `LiveRequest` gets the field `fork_key`, and `Evaluation`
gets `candidate_calls`. A struct literal of either type needs the new
field. `EvalError` gets `ForkIdInUse`. The new `LiveProbeHost` methods
have defaults.

No new `WorkflowEvent` variant, no migration, and no history change. An
`Evaluation` recorded before this change decodes with no turn spend.

Tests:

- `refine_run.rs`: three `agent` sources are refused (live of another
  kind, ended of another kind, live of the kind). Each probe row names its
  model. Each probe attempt passes the same key.
- `live_probe_postgres.rs`: a probe killed during the wait, then a retry,
  makes one fork.
- `eval.rs`: each turn names its model, and a routed turn names its route.
- Unit tests: `eval/probe.rs` (reuse, the race, a foreign fork),
  `refine/run.rs` (the call cap) and `postgres.rs` (a fork id on the
  source shard).
