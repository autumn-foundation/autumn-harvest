## Fix — autoresearch compiles again, and `harvest-verify` gates it in CI

PR #2187 added `start_provenance` to `testing::HistorySnapshot`. PR #2190
merged the autoresearch capstone in parallel, and its replay helper builds a
`HistorySnapshot` without the new field. The two PRs merged cleanly, but
`trunk-dev` no longer compiled `autoresearch`. The helper now sets
`start_provenance: None`. No autoresearch workflow reads its start
provenance, so the replayer default is correct.

The `harvest-verify` CI job gets a third gate step. It runs the semantic
determinism analyzer over `-p autoresearch --lib`. The examples gate covers
only the `examples/` targets of `autumn-harvest`, so it never saw the
capstone's two workflows. The step is non-strict, like the examples gate: a
`nondeterminism-found` fails the job and an `unknown` warns.
`ci_wiring::the_autoresearch_gate_is_wired_into_ci` keeps the step in place.

Result on Rust 1.99.0: `analyzed 2: proven 0, unknown 2, found 0, allowed 0`.
Each unknown is an `external-crate-body` boundary. It is not a finding.

No migration. No `WorkflowEvent` change.
