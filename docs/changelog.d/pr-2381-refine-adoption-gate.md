## The adoption gate of a refine run (issue #2381)

A refine run that chose an overlay now waits for a person before the
overlay goes live. The design is in `docs/design/DESIGN-2381.md`.

- **Behavior change.** `agent_refine` no longer ends with `chosen`. The
  activity `agent_refine_adopt` calls `refine::adopt`. An allowed candidate
  goes live as `auto:<run>`. Any other candidate is put as an inactive
  version `refine:<run>`, and the run waits on the signal
  `refine_approval:<run>` for `approval_timeout` of its policy.
- `approve` activates the version with a compare-and-swap on the version
  that was active at refine start. `edit` checks the overlay under the run
  ceilings, puts it and activates it. `reject` and a timeout activate
  nothing. A pointer that moved ends the run as `superseded`. A refused
  edit and an unreadable payload keep the wait until the same deadline.
- A decision that arrives before the wait opens is dropped, as eval drops
  it.
- The run sets the search attributes `refine_kind` and `refine_status`:
  `waiting_approval` while it waits, then `adopted`, `rejected`,
  `timed_out`, `superseded`, `auto_adopted`, `unchanged`,
  `budget_exhausted` or `chosen`.
- `RefineStatus` gets `Adopted`, `Rejected` and `Superseded`.
  `AwaitingApproval` now writes `waiting_approval`, and still reads
  `awaiting_approval`. `RefineStatus::as_str` gives the attribute value.
- `RefineInput::dry_run` ends the run after the loop with `chosen`, stores
  nothing and opens no gate. `RefineReport::gate` holds the gate record.
  `RefineStart::active` holds the version active at the start.
- `refine::adopt` finds its own `refine:<run>` version on a retry, so it
  puts no second version.
- CLI: `harvest agents profile pending <kind>`, `approve <run>`,
  `reject <run>` and `edit <run> --file`. Rust: `sqlite::decide_refine` and
  `postgres::decide_refine`.
- **Upgrade.** Register the new activities `agent_refine_adopt` and
  `agent_refine_decide` with `workflow::activities()`. A run started on an
  older release replays into the gate and expects an unset pointer.
- `formal/verus/src/refine_gate.rs` proves that only an activation adopts
  and that an end is final. `autumn-harvest-agent/tests/refine_gate.rs`
  checks each criterion on SQLite, with a test clock, a restart and a
  strict replay.
