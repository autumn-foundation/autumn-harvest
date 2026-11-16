## Phase X.Y — DAG retry envelope

`DagRetryEnvelope` joins the DAG critical path method with the retry envelope
of each task. It answers how long a run can take when every task uses all of
its retries.

- Each task gets the worst case of `retry_envelope::analyze`: every attempt
  runs to its bound, and jitter picks the longest backoff.
- The report gives the nominal and the worst-case makespan and the critical
  path for each. `path_shifts` is `true` when retries move the critical path.
- `with_fallback_policy` models the registered default policy of tasks that
  set none. A retry list too long to bound makes the worst case `None`.
- The module is pure and sends no SQL. No new `WorkflowEvent` variant and no
  migration. Seven unit tests cover the chain, the path shift, jitter, the
  fallback policy and the unbounded case.
