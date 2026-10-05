## Testing — pin the nightly lifecycle counterexamples (issue #1923)

The first deep nightly run failed `lifecycle_matches_the_reference_model`
in all 8 shards. That run tested `e22a53bb` (PR #1909, issue #1824). #1824
made a new start sort 30 seconds later at claim. The #1829 model still
sorted by plain `scheduled_at`, so it expected the fresh start ahead of a
requeued orphan. The engine was correct. PR #1918 (`aff18f33`) already
moved the model and its claim check to `queue::CLAIM_ORDER_DUE_SQL`.

The 8 shards shrank to 3 distinct sequences. `PINNED` in
`tests/integration/lifecycle_model_props.rs` holds them. Each pin also
names the coverage labels it must reach, so a pin cannot pass without
reaching its branch. `pinned_counterexamples_replay` replays each pin
against the database in the `test-db-linux` job. A later claim-order change
that the model misses now fails that check on a pull request. The check is
not required yet, so it does not block a merge. The property test and the
replay share one setup helper, `on_own_database`.

`docs/testing/property-and-fuzz.md` and the #1829 fragment now state the
claim-order check, not `scheduled_at` order. The docs name `PINNED` as the
place for a new counterexample. A model comment that still said a requeued
orphan sorts behind a fresh start now states the #1824 order. When the
lifecycle job fails, the nightly alert issue tells the reader to pin each
shrunk sequence.

No engine code changes. There is no new `WorkflowEvent` variant and no
migration.

**Evidence.** On `e22a53bb`, the replay fails on all 3 sequences at the
nightly steps (6, 5 and 6, 0-based), with the nightly error: "the claim
took a task due at ..., but the earliest is ...". The gap is 5 seconds each
time. On `trunk-dev`, the replay and the 128-case property pass. A local
deep pass of 12000 cases, on 3 random seeds, also passes.
