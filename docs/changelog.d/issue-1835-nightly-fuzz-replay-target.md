## Testing — Nightly fuzzing and a structure-aware replay target (issue #1835)

Fuzzing now runs every night with a persisted corpus. A new target fuzzes
the replayer with structured histories. There is no new `WorkflowEvent`
variant, no migration and no change to engine behavior.

**Nightly job.** `.github/workflows/fuzz-nightly.yml` runs all five targets
each night for 600 seconds, one job per target. A run artifact keeps each
corpus for 30 days. Each run downloads the newest corpus of this repository
and uploads its own, also after a crash. The actions cache is not used,
because the CI build caches made GitHub evict a corpus within minutes. A failed scheduled run uploads the crash input and
opens an issue. A pull request that changes the harness or the
`autumn-harvest` source runs each target for 60 seconds. The manual `fuzz-smoke` job in `ci.yml` is gone, because the new
workflow runs on demand too. The guard `fuzz_nightly_wiring.rs` checks the
cron, the corpus cycle, the alert job and the target lists.

**Replay target.** `fuzz_replay` feeds an arbitrary `Vec<WorkflowEvent>` to
the replayer. A new opt-in `fuzzing` feature derives
`arbitrary::Arbitrary` for `WorkflowEvent` and its field types, and adds
the `fuzzing` harness module. The feature is not covered by semver. The
JSON generator builds the reserved `_harvest_*` envelope shapes on purpose.
Each case runs twice through the write path, the strict and lossy read
paths and the replayer. The oracles are: no panic, an unchanged write-read
round trip, and the same report on both runs.

**Seeds.** `fuzz/seeds/fuzz_replay/` holds JSON cases, with the #1253 and
#1758 reproducers. `replay_fuzz_seeds.rs` runs every seed in CI through a
new `allos` manifest row. Each reproducer fails with its fix reverted. The
#1253 seed fails with "the lossy read marked a stored value undecodable".
The #1758 seeds fail with "references unknown store 's3-prod'" and "no blob
under \"customer-42/invoice.pdf\"".

**Toolchain.** The 1.100 nightlies that were tested fail to build the `db`
feature, so the fuzz crate did not build on current nightly. The workflow
and `fuzz/smoke.sh` pin `nightly-2026-08-14`.

**Found by the fuzzer.** A history with two `WorkflowContinuedAsNew` events
of different target types tripped a `debug_assert!` after a correct match.
The check read the first continuation in the history, not the one just
matched. It now reads the last one before the cursor. Release builds were
not affected. The seed `fuzz-crash-continue-as-new-type-assert.json` pins
the fix.
