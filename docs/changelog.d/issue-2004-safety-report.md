## Docs — Jepsen-style safety report (issue #2004)

**What shipped.** `docs/safety-report.md` states four guarantees: leases,
fencing, exactly-once completion and signal ordering. For each one, it gives
the claim, the tests, the results and the known limits. Each result has a
command that reproduces it, and its cadence: PR or nightly. The report also
states its fault model, the bugs that the tests found, and its own limits.
The README, `docs/comparison.md`, `docs/architecture.md` and
`docs/testing/chaos.md` link it.

**Known limits stated.** The report states that #1871 is fixed only in part.
A TCP reset still loses a result write. No tool measures coverage (#1818). No
model covers shard-generation fencing. Two signals sent in one transaction
have no defined order. The report also cites the open signal bug #2079.

**Bug fix: reset keeps the signal order.** A reset with the `Buffer` policy
wrote the pending signals of the source run in one INSERT. Each new row took
a default `received_at`, so every row got the same `NOW()`. The random row id
then set the order in which the new run read them. `reapply_or_drop_signals`
now copies the source `received_at`, as continue-as-new already does. No
migration and no new `WorkflowEvent` variant.

**New tests.** `signal_tests::committed_sends_are_recorded_in_send_order`
sends eight signals on Postgres and ingests them in two wake cycles. History
must hold them in send order. No Postgres test checked signal order before.
`signal_tests::reset_buffer_keeps_the_order_of_pending_signals` failed before
the fix and passes after it.

**Gate.** `safety_report_docs` runs in the CI `lint` job, so a docs-only
change runs it too. It checks the sections of the report and that each
results row holds one command. A cargo filter that matches no test exits 0.
So the guard lists the tests that each command compiles, with its features
and `cfg` gates. Each filter must match a test that runs. A TLC row must
state the verdict of `formal/tla/models.txt`. The report must cite each bug
that `docs/testing/chaos.md` records. The guard pins the bullet count of the
limit sections in `formal-methods.md` and `simulation.md`. A new limit there
fails the guard until the report states it. No prose sentence may exceed 25
words.

**Refactor.** `docs_guard_support.rs` holds the prose and CI-step helpers
that `replay_positioning_docs.rs` and `safety_report_docs.rs` share.

**Evidence.** Every TLC row was re-run with TLC 1.7.4. The DB, simulator and
chaos rows were re-run on Postgres 16 and toxiproxy 2.9.0.
