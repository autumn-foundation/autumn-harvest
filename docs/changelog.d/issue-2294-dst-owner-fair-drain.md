## Testing — A fair drain phase for the owner-lease simulation (issue #2294)

The DST nightly failed the owner-lease sweep in all four shards. Each shard
stopped at its first failing seed, with "the run did not consume every
signal within 400 steps". No invariant failed, and the engine was not at
fault. The faults ended the lease every few steps for all 400 steps. No
owner makes progress then. The check asked for progress while the faults
went on, so a few seeds per million failed.

A run of `dst::owner` now has two phases, as a world run has. The fault
phase is unchanged, so a run that ends in it has the same trace as before.
The drain then injects no fault: no nemesis, no expiry, no release and no
stall. It is weakly fair, with `owner::FAIR_STEPS` (8). A run must consume
every signal before the drain ends. `OwnerConfig::drain_bound` computes the
step bound of a drain. The default limit of 400 steps covers up to 103
decisions in flight at the drain start. The trace logs that number.

`OwnerConfig::max_steps` is now `fault_steps`, with a new `drain_steps`.
`OwnerStats` counts the drains, and the longest drain, wait and enabled
set.

The world drain and the owner drain now share one fairness helper,
`dst::fair::Waits`. The world drain keeps its rule and its traces.

The replay command did not reproduce the failure. `replay_one_owner_seed`
failed on an invariant only, and the nightly seeds broke none. The sweep
and the replay now share one verdict, `OwnerReport::failure`.

A new rule, R8 of ADR 0010, backs the drain. A fenced commit unloads its
worker only when the worker still pins the fenced epoch. Its invariant,
`LiveRowHasAPinnedHolder`, is now in `OwnerLease.tla`, `OwnerLease.cfg`,
the simulator and the Stateright model. The `unload-on-any-fence` mutant
breaks it in TLC, Stateright and the simulator. That mutant also stops a
drain with work left. The old sweep missed it, because the lease churn
freed the row. Stateright also checks `DrainCanProgress`: each state that
is not quiescent has a step that is not a fault.

No migration. No `WorkflowEvent` change. No engine change.

Tests:

- `the_seeds_of_issue_2294_converge_in_the_drain`: the four nightly seeds
  fail with no drain and pass with it.
- `the_drain_converges_fairly_within_its_bound`: the drain starts at every
  third step from 0 to 400, over 12 seeds. Each wait and each drain stays
  within its bound.
- `the_drain_leaves_the_fault_phase_unchanged`,
  `the_drain_injects_no_fault` and `the_drain_ends_every_stall`.
- `the_replay_fails_a_run_that_does_not_converge` and
  `the_failure_of_a_run_that_does_not_converge_names_the_drain`.
- `a_worker_that_unloads_on_any_fence_stops_the_drain`, in the `dst` and
  the `stateright` targets.
- `dst::fair` unit tests: the fairness rules, and the bound
  `fair_steps + k - 1` while actions come and go.
- All 20 rows of `formal/tla/models.txt` pass TLC.
- The 4,000,000 seeds of the failed nightly run pass.
