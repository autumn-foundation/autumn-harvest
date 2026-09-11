# Four worker samplers issue SQL with no metrics-enabled guard

This note documents issue #1428: four of `Worker`'s ten periodic samplers
issue their SQL unconditionally, on every worker's poll-interval tick, for
the lifetime of the process, even when no metrics recorder is configured.
Their six siblings all check `telemetry.metrics.is_enabled()` first and
return before ever touching the pool.

## 🎯 Workload

The real-workload profile already exists and is not repeated here: assay
`docs/assays/0008-redis-dispatch-integrated-throughput.md` drove a
deployment-shaped 10,000-workflow backlog through four workers at a 25 ms
poll interval, with `NoOpMetrics` configured (the unconfigured-deployment
default). `pg_stat_activity` during that run was dominated by
`sample_history_oversized_counts` — a correlated subquery over every
`RUNNING` execution, issued 160 times a second across the worker pool for
no reason, since every row it returned was discarded.

## 📈 Profile

From that assay: with the four unguarded samplers quieted (a diagnostic,
not pre-registered, comparison), the Redis-dispatch arm's completed-task
rate rose from 173.04 to 279.83 tasks/s — **the samplers cost it 38% of its
own throughput** — and the Postgres-only control arm went from completing
zero task rows in 600 s to 26.09 tasks/s. That is a >20% wall-clock delta,
which this agent's evidence rules treat as admissible only when corroborated
by a same-direction deterministic-counter change; the counter evidence is
below.

The four unguarded samplers, named in the issue and confirmed by reading
`autumn-harvest/src/worker.rs`:

| sampler | guarded before this fix |
|:--|:--|
| `spawn_concurrency_sampler` | no |
| `spawn_rate_limit_sampler` | no |
| `spawn_dlq_depth_sampler` | no |
| `spawn_history_oversized_sampler` | no |

`spawn_queue_depth_sampler` already carries the guard and the comment
naming the rule the other four broke: "No recorder configured: never issue
the sampler SQL. The per-event `record_*` calls are zero-cost, but these
gauge-feeding queries are not."

This fix is at 100% of the profile's target: the samplers are not "a
fraction of the cost of a hot path," they are pure waste whenever no
metrics recorder is configured — there is no lower bound this analysis
needs to clear beyond "waste this to zero."

## 💡 Hypothesis

Each of the four samplers is missing the one-line guard its six siblings
already have. Adding it makes an unconfigured (`NoOpMetrics`) deployment
return before the first `pool.get()`, exactly like the guarded siblings —
eliminating the wasted round trips entirely, not merely reducing them.

## 🔧 Change

Add `if !telemetry.metrics.is_enabled() { return; }` at the top of each of
the four samplers' spawned task, before the `loop`, matching
`spawn_queue_depth_sampler`'s existing guard verbatim. No other behavior
changes: a metrics-enabled deployment (the only configuration under which
these samplers ever produced a used value) is byte-for-byte unaffected,
since `is_enabled()` returns `true` for it and the guard is a no-op.

## 📊 Measurement

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine.
The counter here is a direct proxy for "did a sampler ever reach
`pool.get()`": `autumn-harvest/src/worker.rs`'s
`zz_capture_metrics_sampler_guard_pool_touch_evidence` test spawns all four
samplers against `unreachable_pool` (a pool aimed at a closed port, so
`pool.get()` fails fast and the sampler's own failure branch logs "...could
not acquire DB connection") with `NoOpMetrics`, advances a paused clock by
20 sampler intervals, and counts how many times that log line fires — one
per pool touch.

A second, independent counter corroborates it: `strace -f -c -e
trace=connect,socket` against the compiled test binary running the same
test, counting real `connect(2)` syscalls.

| | tracing pool-touch count | `strace` `connect` calls |
|:--|--:|--:|
| Before (`docs/perf-artifacts/metrics-sampler-guard/before-counts.txt`, `before-strace.txt`) | 38 | 38 |
| After (`docs/perf-artifacts/metrics-sampler-guard/after-counts.txt`, `after-strace.txt`) | 0 | 0 |

The two independent counters agree exactly in both runs. The exact
before-count (38, not the naively-expected 4 samplers × 20 ticks = 80) is a
timing artifact of the paused-clock harness racing real (unvirtualized)
socket connect-refusal against virtual-clock advances — expected and
immaterial, since the only claim resting on it is "nonzero." The after-count
is not a measurement subject to timing at all: the guard makes the function
return before constructing a socket, so it is exactly, unconditionally zero
on every run, forever, for as long as metrics stay disabled — a stronger
guarantee than any measured percentage. This clears this agent's impact
floor ("a measurable reduction in syscall count") by eliminating the
syscalls outright, and the elimination is corroborated by the assay's 38%
wall-clock throughput delta in the same direction.

A permanent regression test,
`metrics_disabled_samplers_never_touch_the_pool`, pins the after-state:
it is not timing-sensitive (it asserts exactly zero, which the guard
makes true unconditionally) and runs on every `cargo test -p
autumn-harvest --lib`.

## 🔬 Reproduce

```sh
# Tracing pool-touch count (writes docs/perf-artifacts/metrics-sampler-guard/<PERF_LABEL>-counts.txt):
PERF_LABEL=before cargo test -p autumn-harvest --lib -- \
  worker::tests::zz_capture_metrics_sampler_guard_pool_touch_evidence \
  --exact --ignored --nocapture

# strace corroboration, against the compiled test binary directly:
BIN=$(cargo test -p autumn-harvest --lib --no-run 2>&1 | sed -n 's/.*(\(target\/debug\/deps\/autumn_harvest-[a-f0-9]*\))/\1/p' | tail -1)
strace -f -c -e trace=connect,socket "$BIN" \
  worker::tests::zz_capture_metrics_sampler_guard_pool_touch_evidence \
  --exact --ignored --nocapture

# Permanent regression assertion (always run):
cargo test -p autumn-harvest --lib -- \
  worker::tests::metrics_disabled_samplers_never_touch_the_pool --exact
```

## Verification

- `cargo fmt --all`: clean.
- `cargo clippy -p autumn-harvest --all-targets --all-features -- -D warnings`:
  no new findings on `worker.rs`.
- `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev`: no Tier
  A findings, no Tier B regressions.
- `cargo test -p autumn-harvest --lib`: full suite passes unchanged.

## Reference

- Issue [#1428](https://github.com/autumn-foundation/autumn-harvest/issues/1428).
- `docs/assays/0008-redis-dispatch-integrated-throughput.md` — the
  deployment-shaped profile that found this and named all four samplers.
