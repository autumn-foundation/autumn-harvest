## Phase X.Y — DAG slot-limited plan

`dag_slot_plan` runs a DAG on a fixed number of worker slots. It answers how
long a run takes when it gets fewer slots than its peak demand.

- `plan` is a list scheduler. A free slot starts the ready task with the
  least slack. It reports the makespan, the delay that the limit adds, and
  the slot, start, finish and wait of each task.
- `fewest_slots` gives the fewest slots with which the plan meets the
  unconstrained makespan. It can sit below both peaks of `peak_bounds`.
- The plan is a heuristic, so its makespan is an upper bound on the best one.
- Zero-duration tasks take no slot. The module is pure and sends no SQL.
