## Fix — DST nightly: rare sweep targets and an interrupted image pull (issue #2280)

Run 15 of the DST nightly failed in two jobs.

**`autoresearch`.** `sometimes!("a paper or a failure taints a later
experiment")` was never true in seeds 75000 to 79999. The sweep sends a
hint, a policy and a stop. The session reads signals only between
experiments. Setup and the baseline take many tasks, so the stop nearly
always landed before the first experiment. The target was true on about
1 seed in 2000. Two more targets of that sweep were true on under 1 seed
in 100. The experiment sweep had three targets under 5 in 100. Seeds 0
to 63 reached them all by luck. With the session sweep fixed, a target of
the experiment sweep still failed in 92 of 200 other 64-seed windows.

- The session sweep sends the policy 13 times before the stop. Only the
  first signal changes the policy. The others spread the stop over later
  tasks.
- The taint target is split by cause: a paper and a failure. A cause
  counts only when it is the first taint of the night.
  `a_paper_or_a_failure_taints_a_clean_session` requires both, with no
  sibling hint to hide them. Before, the hand-off sweep reached the old
  target through its sibling hint. A session that dropped the label of a
  failed experiment then passed every test. Now it fails.
- The experiment mocks take the rare paths more often. Each target is
  now true on about 10 or more seeds in 100.
- `a_session_reaches_its_targets_in_a_second_window` sweeps the 64 seeds
  after the default window. It failed before the fix. It skips itself
  when a seed variable is set, as in the nightly job.
- After the fix, 0 of 300 64-seed windows fail, and 16 windows of 5000
  seeds pass, seeds 75000 to 79999 included.

**Replay command.** When a `sometimes!` label is never true, the command
now sets `HARVEST_SIM_SEED_BASE` and `HARVEST_SIM_SEEDS`. Before, it ran
the default seeds, where the target was true, so it did not reproduce.
`a_sweep_wide_failure_names_its_seeds` failed before the fix.

**`world`.** `testcontainers` failed to pull `postgres:16` ("bytes
remaining on stream"). It does not retry a pull.
`.github/ci/pull-test-images.sh` now takes the images to pull as
arguments. The `differential` and `world` jobs call it for `postgres:16`
before their tests, with a 10-minute step timeout.

**Build.** `examples/autoresearch/src/runtime.rs` set `start_provenance`
twice, so the example did not compile on `trunk-dev`. It is set once.

**No migration. No new `WorkflowEvent` variant. No public API change.**
