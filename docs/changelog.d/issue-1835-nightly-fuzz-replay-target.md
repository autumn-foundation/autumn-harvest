## Testing — Nightly fuzzing and a structure-aware replay target (issue #1835)

Fuzzing now runs every night with a persisted corpus. A new target fuzzes
the replayer with structured histories. There is no new `WorkflowEvent`
variant, no migration and no change to engine behavior.

**Nightly job.** `.github/workflows/fuzz-nightly.yml` runs all five targets
each night for 600 seconds, one job per target. The actions cache keeps each
corpus. Each run restores the newest corpus and saves its own under a new
key, also after a crash. A failed scheduled run uploads the crash input and
opens an issue. A pull request that changes the harness runs each target for
60 seconds. The manual `fuzz-smoke` job in `ci.yml` is gone, because the new
workflow runs on demand too. The guard `fuzz_nightly_wiring.rs` checks the
cron, the corpus cycle, the alert job and the target lists.

**Replay target.** `fuzz_replay` feeds an arbitrary `Vec<WorkflowEvent>` to
the replayer. A new opt-in `arbitrary` feature derives
`arbitrary::Arbitrary` for `WorkflowEvent` and its field types, and adds
the `fuzzing` harness module. The JSON generator builds the reserved
`_harvest_*` envelope shapes on purpose. Each case runs twice through the
write path, the read path and the replayer. The oracles are: no panic, an
unchanged write-read round trip, and the same report on both runs.

**Seeds.** `fuzz/seeds/fuzz_replay/` holds JSON cases, with the #1253 and
#1758 reproducers. `replay_fuzz_seeds.rs` runs every seed in CI through a
new `allos` manifest row. With the #1253 fix reverted, the seeds fail with
`unknown payload codec key id 2026-q3`. With the #1758 fix reverted, they
fail with `references unknown store 's3-prod'`.
