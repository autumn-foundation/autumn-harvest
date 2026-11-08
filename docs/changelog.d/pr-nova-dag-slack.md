## DAG slack analysis

`dag_slack::SlackAnalyzer` computes the slack of each task in a DAG. Slack is
the time that a task can slip before the whole DAG finishes later. A task with
zero slack is on the critical path. The report gives the earliest start, the
latest start and the slack of each task, plus the total duration.

- Durations come from `start_to_close`, then a name mock, then a default, as in
  `CriticalPathAnalyzer`.
- The analysis is pure. No migration, no `WorkflowEvent` variant, no replay
  impact.
