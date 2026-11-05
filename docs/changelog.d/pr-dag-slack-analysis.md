## Phase X.Y — DAG slack analysis (Nova)

Added `dag_slack` with `SlackAnalyzer`, `SlackReport` and `TaskSlack`. A forward and a backward pass over a `DagDefinition` give each task an earliest start, a latest start and a slack. A task with zero slack is critical. The zero-slack set matches `CriticalPathAnalyzer` on a diamond DAG. The module is pure and additive: it sends no SQL, adds no event variant and adds no migration. Six unit tests cover it.
