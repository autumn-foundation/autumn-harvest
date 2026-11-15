# 🚦 Semaphore CI health — `owner_decider_tests::a_saturated_owner_takes_a_high_priority_run_first` is red on 3 of the last 5 trunk-dev runs

**Status:** health report, no code change. The test fails in CI and does not
fail in any local harness. The hard gate for a fix PR is not met: there is no
reproduction, so there is no baseline and no after-measurement. A tolerance
change is not an option.

## 🎯 Verdict path

`ci.yml` `push` runs on `trunk-dev`, job `Test DB (linux, shard 0)`, step
"Run Linux Docker-backed manifest suites (shard)". The job takes about 11
minutes. A red here blocks the whole run.

## 🌡️ Symptom

Every `Test DB (linux, shard 0)` run on `trunk-dev` since `779a585`, by commit:

| Commit | Run | Verdict |
|---|---|---|
| `779a585` | 43500612307 | pass |
| `21cb8e3` | 43551122008 | cancelled, no verdict |
| `9e525fd` | 43554238029 | **fail**, this test |
| `5ee517e` | 43616125588 | **fail**, this test |
| `40643b1` | 43640092209 | **fail**, this test |
| `5fcdcc6` | 43645623177 | pass |

The test failed in 3 of 5 runs that gave a verdict. Each failure is the only
failure in a 272-test shard. Each lasts 3.0–3.3 s and has one signature:

```
assertion `left == right` failed: the high-priority run decides first
  left: 00000fa2-befe-4ac7-bf66-fba1ffd8b386   (first run to start)
 right: 0000e968-6bda-4562-860d-35bd7fe8044c   (the high-priority run)
```

The earlier `trunk-dev` failures in the sampled window (`1c692d2`, `05e0013`,
`cd6bfb4`, `515910b`) are other signatures: Kafka apt-dependency installs, an
SQLite plugin suite and a shard-1 failure. They are not this cluster.

`5fcdcc6` (#2682) changed comments only, and its message says the code token
stream is unchanged. Its pass is therefore not evidence of a fix. It is one
sample from the same distribution.

## 🔍 Diagnosis

**Product-versus-test verdict: undetermined.** I have a ranked hypothesis and
no proof.

1. **Test synchronization gap (leading).** The test waits for the wake of each
   run on its own `OwnerWakeListener`, then calls `SLOW_GATE.notify_one()` at
   once. A receipt on the test connection does not show that the owner's own
   listener has pushed the hint into `owner_hints`. If the owner's listener
   lags, the decider has only the normal runs ready when the permit frees. It
   takes the best of them and the high-priority run loses. This needs a loaded
   runner, which matches the CI-only failures.
2. **Product race (not excluded).** In `DeciderState` (`worker.rs`), `ranking()`
   blocks a take only for an unkeyed hint or a lookup in flight. A hint that
   reaches `owner_hints` after the permit frees cannot be ranked first. That
   may be inherent to wake-driven dispatch. It may also be a window the
   product could close. Only a reproduction separates the two.

Hypothesis 0, a `pass_granted` flag that outlives its take pass, is ruled out.
`end_pass()` runs on every loop iteration (`worker.rs`, the decider loop).

## 🔧 Treatment

None shipped. The correct next step is a deterministic reproduction, not a
change to the test.

- Add a test-only hook or counter that shows when the owner has consumed the
  hint for an execution. If hypothesis 1 holds, waiting on that signal instead
  of on the test's own listener is the deterministic fix. It awaits the real
  condition and loosens nothing.
- To tell the two hypotheses apart, delay the owner's listener by a fixed
  amount under a debug-only knob. The failure should then reproduce at 100%.
  If the product must not lose priority in that window, it is a product bug.

## 📊 Measurement

Local harness: the `integration` test binary built from `5fcdcc6`, the test run
`--exact`, a local PostgreSQL 16 through `HARVEST_TEST_DATABASE_URL`.

| Condition | Runs | Failures |
|---|---|---|
| isolated, serial, `fsync=off` | 20 | 0 |
| 4 concurrent copies plus 4 CPU burners, `fsync=off` | 32 | 0 |
| 12 concurrent copies plus 6 CPU burners, `fsync=on` | 36 | 0 |

That is 0 failures in 88 runs. CI shows 3 in 5, so the CI condition is not
reproduced here. CI uses a Docker-hosted PostgreSQL and a shared runner. The
revert check was not run: there is no fix to revert.

Ledger delta: none. The repository has no quarantine ledger. The test is
**not** quarantined by this report. Skipping a test that guards queue priority
without an owner and a diagnosis-by date would be burial.

## 🔬 Reproduce

```sh
# CI evidence: failure signature, run 43640092209, job 132592472706
# Local rerun protocol:
export HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres
cargo test -p autumn-harvest --test integration --no-run
B=$(ls -t target/debug/deps/integration-* | grep -v '\.d$' | head -1)
for i in $(seq 1 50); do
  $B owner_decider_tests::a_saturated_owner_takes_a_high_priority_run_first --exact \
    >/dev/null 2>&1 || echo "run $i FAILED"
done
```

## Ask

The owner of the decider (issue #2255, #2308) should decide between the two
hypotheses. Until then, treat a red on this test as a real signal and read the
panic before any rerun. Do not add a retry.
