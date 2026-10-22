## Testing — A fair drain phase for the owner-lease simulation (issue #2294)

The DST nightly failed the owner-lease sweep in all four shards. Each shard
failed one seed with "the run did not consume every signal within 400
steps". The engine was not at fault. The nemeses and the clock ended the
lease every few steps for all 400 steps. No lease makes progress then. The
check asked for progress while the faults went on, so about one seed in a
million failed.

A run of `dst::owner` now has two phases, as a world run has. The fault
phase is unchanged, so a run that ends in it keeps its trace. The drain
then injects no fault: no nemesis, no expiry, no release and no stall. It
is weakly fair, with `FAIR_STEPS` (8). A run must consume every signal
before the drain ends. The `OwnerConfig::drain_steps` doc derives the
bound of 400 steps. In 110,000 drains, no drain took more than 57 steps.

The world drain and the owner drain now share one fairness helper,
`dst::fair::Waits`. The world drain keeps its rule and its traces.

The replay command did not reproduce the failure. `replay_one_owner_seed`
failed on an invariant only, and the nightly seeds broke none. The sweep
and the replay now share one verdict, `OwnerReport::failure`.

`OwnerConfig::max_steps` is now `fault_steps`, with a new `drain_steps`.

The Stateright owner model adds `LiveRowHasAPinnedHolder`. The holder of a
live row is pinned at the epoch of the row, so the drain cannot stop with
work left. A mutant that unloads a worker on every fenced commit breaks it.
The same mutant stops the drain. The old 400-step sweep passed it, because
the lease churn hid the stall.

No migration. No `WorkflowEvent` change. No engine change.

Tests:

- `the_seeds_of_issue_2294_converge_in_the_drain`: the four nightly seeds
  converge, and each one needs the drain.
- `the_drain_converges_from_every_fault_step`: the drain starts at each
  step from 0 to 400, over 12 seeds.
- `the_drain_injects_no_fault`: no drain step ends the lease, releases the
  row or stalls a worker.
- `the_failure_of_a_run_that_does_not_converge_names_the_drain`: a store
  with no takeover fails the shared verdict.
- `dst::fair` unit tests: the fairness rules, and the bound
  `FAIR_STEPS + k - 1`.
- The 4,000,000 seeds of the failed nightly run pass.
