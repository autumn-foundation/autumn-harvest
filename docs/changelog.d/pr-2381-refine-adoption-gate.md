## The adoption gate of a refine run (issue #2381)

A refine run that chose an overlay now waits for a person before the
overlay goes live. The design is in `docs/design/DESIGN-2381.md`.

- **Behavior change.** A run that chose an overlay ends with `chosen`
  only as a dry run. The activity `agent_refine_adopt` calls
  `refine::adopt`. An allowed candidate goes live as `auto:<run>`. The
  step puts any other candidate as an inactive version `refine:<run>`, and
  the run waits on the signal `refine_approval:<run>` for
  `approval_timeout` of its policy. The gate needs an overlay store.
- `approve` activates the version with a compare-and-swap on the version
  that was active at refine start. `edit` checks the overlay under the run
  ceilings, puts it and activates it. `reject` and a timeout activate
  nothing. A move of the pointer after refine start, or after the wait
  opens, ends the run as `superseded`. A refused edit and an unreadable
  payload keep the wait until the same deadline.
- A retry of a decision never moves the pointer again. So it never undoes
  a rollback, and a retry after its own move reports it.
- `ProfileOverlayStore::activate_guarded` activates only when the log head
  is the one that the caller read. Each store of this crate checks it in
  the swap transaction, so a move away and back never passes. The kernel
  is `overlay_log::decide_guarded`, and `MoveRequest` gets the field
  `head`.
- The run drops a decision that arrives before the wait opens, as eval
  does. A key that the verdict does not read fails the decode.
- The run sets the search attributes `refine_kind` and `refine_status`:
  `waiting_approval` while it waits, then `adopted`, `rejected`,
  `timed_out`, `superseded`, `auto_adopted`, `unchanged`,
  `budget_exhausted`, `chosen` or `failed`.
- `RefineStatus` gets `Adopted`, `Rejected` and `Superseded`.
  `AwaitingApproval` now writes `waiting_approval`, and still reads
  `awaiting_approval`. `RefineStatus::as_str` gives the attribute value.
- `RefineInput::dry_run` ends the run after the loop with `chosen`, stores
  nothing and opens no gate. `RefineReport::gate` holds the gate record.
  `RefineStart::active` holds the version active at the start.
- `refine::adopt` finds its own `refine:<run>` version first on a retry. So
  it puts no second version, and a new day or a wider policy never changes
  the outcome of a retry. A lost race on the policy is now retryable.
- CLI: `harvest agents profile pending <kind>` (Postgres only),
  `approve <run>`, `reject <run>` and `edit <run> --file`. Rust:
  `sqlite::decide_refine` and `postgres::decide_refine`.
- **Upgrade.** Register the new activities `agent_refine_adopt` and
  `agent_refine_decide` with `workflow::activities()`. A run that started
  on an older release opens no gate and ends with `chosen`.
- `formal/verus/src/refine_gate.rs` proves that only an activation adopts
  and that an end is final. `autumn-harvest-agent/tests/refine_gate.rs`
  checks each criterion on SQLite, with a test clock, a restart and a
  strict replay. Unit tests in `refine/gate.rs` check each retry of a
  decision.
