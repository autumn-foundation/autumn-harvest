## Testing — pin the nightly lifecycle counterexamples (issue #1923)

The first deep nightly run failed `lifecycle_matches_the_reference_model`
in all 8 shards. That run tested `e22a53bb` (#1824). #1824 made a new start
sort 30 seconds later at claim. The #1829 model still sorted by plain
`scheduled_at`, so it expected the fresh start ahead of a requeued orphan.
The engine was correct. #1917 (`aff18f33`) already moved the model and its
claim check to `queue::CLAIM_ORDER_DUE_SQL`.

The 8 shards shrank to 3 distinct sequences. `PINNED` in
`tests/integration/lifecycle_model_props.rs` holds them, and
`pinned_counterexamples_replay` replays each one against the database on
every CI run. A later claim-order change that the model misses now fails a
pull request, not only the nightly. The property test and the replay share
one database setup.

`docs/testing/property-and-fuzz.md` and the #1829 fragment now state the
claim-order check, not `scheduled_at` order. The docs name `PINNED` as the
place for a new counterexample.

No engine code changes. There is no new `WorkflowEvent` variant and no
migration.
