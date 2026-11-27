## DAG Monte Carlo criticality (Nova)

`DagCriticalityAnalyzer` gives each task a triangular duration range instead
of one fixed value. It samples every task, runs the critical path method, and
repeats the trial many times.

The report gives the criticality index of each task: the share of trials in
which the task has zero slack. A task with slack in the nominal plan can still
set the run time when it runs slow. The report also gives the makespan
distribution, its percentiles, and the chance to meet a deadline.

A fixed seed drives a `SplitMix64` generator, so a report is reproducible. A
`start_to_close` timeout caps the sampled duration of its task. The module is
pure and in-memory. It adds no dependency, no event, no migration and no query.

Eight unit tests in `dag_criticality.rs` cover range validation, agreement
with the slack report, shared criticality on overlapping branches, makespan
bounds, deadline probability, seed determinism, the timeout cap and an empty
DAG.
