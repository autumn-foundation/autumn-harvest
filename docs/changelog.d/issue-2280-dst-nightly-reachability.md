## Fix — DST nightly: rare sweep targets and a cut image pull (issue #2280)

Run 15 of the DST nightly failed in two jobs.

**`autoresearch`.** `sometimes!("a paper or a failure taints a later
experiment")` was never true in seeds 75000 to 79999. The sweep sends a
hint, a policy and a stop. The session reads signals only between
experiments. Setup and the baseline take many tasks, so the stop nearly
always landed before the first experiment. The target was true on about
1 seed in 2000. Two more targets of that sweep were true on under 1 seed
in 100. The experiment sweep had three targets under 5 in 100. Seeds 0
to 63 reached them all by luck. With the session sweep fixed, a target of
another sweep still failed in 92 of 200 other 64-seed windows.

- The session sweep sends 13 copies of the policy before the stop. The
  copies change nothing and spread the stop over the night.
- The taint target is split by cause: a paper and a failure. The
  hand-off sweep requires both and reaches them on every seed. Its second
  hint is now a paper.
- The experiment mocks take the rare paths more often. Each target is
  now true on 10 or more seeds in 100.
- `a_session_reaches_its_targets_in_a_second_window` sweeps seeds 64 to
  127. It failed before the fix.
- After the fix, 0 of 300 64-seed windows fail, and 16 windows of 5000
  seeds pass, seeds 75000 to 79999 included.

**Replay command.** A failure of the whole sweep now sets
`HARVEST_SIM_SEED_BASE` and `HARVEST_SIM_SEEDS`. Before, its command ran
the default seeds, where the target was true, so it did not reproduce.
`a_sweep_wide_failure_names_its_seeds` failed before the fix.

**`world`.** testcontainers failed to pull `postgres:16` ("bytes
remaining on stream"). `.github/ci/pull-test-images.sh` now takes the
images to pull as arguments. The `differential` and `world` jobs call it
for `postgres:16` before their tests.

**Build.** `examples/autoresearch/src/runtime.rs` set `start_provenance`
twice, so the example did not compile on `trunk-dev`. It is set once.

**No migration. No new `WorkflowEvent` variant. No public API change.**
