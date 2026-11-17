## Phase X.Y — DAG deadline crash planner

`DagCrashPlanner` turns the slack and leverage analyses into a budget plan.
It answers the cheapest way to make a DAG run meet a deadline.

- `crashable` gives an activity a floor duration and a cost for each second
  it gives up. A task with no option keeps its duration.
- `plan` runs greedy project crashing. Each step cuts the task that buys a
  second of run time at the lowest price. Only the useful part of a cut is
  bought, so a cut never pays for extra slack.
- The plan lists each cut, its cost and the run time after it. It reports a
  missed deadline when no single cut helps, such as two tied parallel paths.
- The module is pure and sends no SQL.
