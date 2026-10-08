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

**Review amendment (Codex, PR #1468, P2):** `spawn_concurrency_sampler` is
not quite like its three siblings. It also emits a `DEBUG` trace
("concurrency cap saturated; pending tasks deferred until a slot frees")
that is a metrics-independent operator signal, not gated on the metrics
recorder before this fix. A blanket `is_enabled()`-only guard would have
silenced that trace for a deployment with no metrics recorder but with
DEBUG tracing enabled. Its guard is instead:

```rust
if !telemetry.metrics.is_enabled()
    && !tracing::enabled!(target: "autumn_harvest::worker", tracing::Level::DEBUG)
{
    return;
}
```

so it stays active whenever *either* signal could use its output. The
other three samplers have no such trace and keep the plain
`is_enabled()`-only guard.

## 📊 Measurement

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine.
The counter is a direct proxy for "did a sampler ever reach `pool.get()`":
`autumn-harvest/src/worker.rs`'s `zz_capture_metrics_sampler_guard_pool_
touch_evidence` test spawns all four samplers, with `NoOpMetrics` and no
tracing subscriber installed (DEBUG off, the realistic unconfigured-
deployment default), against an `AcceptCountingListener` — a loopback TCP
listener standing in for Postgres that counts every accepted connection
before dropping it (so the Postgres handshake then fails, same outward
effect as the `unreachable_pool` helper other tests in this file use, but
the accept is already counted by the time that happens). It advances a
paused clock by 20 sampler intervals and reads the accept count.

**This harness went through two review rounds.** The first version counted
pool touches via the `tracing::debug!` "could not acquire DB connection"
message each sampler's failure branch emits, observed through a
`tracing_subscriber::Layer`. To model "no DEBUG subscriber," that version
filtered the layer to `INFO`. Codex review on PR #1468 (P2) found this
vacuous: filtering the layer to `INFO` also suppresses the very `DEBUG`
event the counter relied on, so the count reads zero whether or not the
guard actually works — the test could not have failed even with the guard
removed entirely. `AcceptCountingListener` replaces that mechanism with a
real TCP accept count, a channel no tracing level can silence. Verified
by temporarily reverting the four guards and re-running
`metrics_disabled_samplers_never_touch_the_pool`: it fails (`left: 20,
right: 0`) as expected, then passes again once the guards are restored —
confirming the test is not vacuous.

A separate test, `concurrency_sampler_stays_active_for_its_saturation_trace_
under_debug_tracing`, pins the review amendment's other half: with metrics
disabled but DEBUG left enabled (via an attached no-op `Layer`, enough to
make `tracing::enabled!` read `true` without needing to observe anything),
`spawn_concurrency_sampler` alone still reaches `pool.get()` (count > 0),
so its saturation trace keeps firing.

A second, independent counter corroborates the aggregate before/after
numbers: `strace -f -c -e trace=connect,accept4` against the compiled test
binary running the same evidence test.

| | accept count | `strace` `connect` calls |
|:--|--:|--:|
| Before (guards removed; `docs/perf-artifacts/metrics-sampler-guard/before-counts.txt`, `before-strace.txt`) | 20 | 21 |
| After (this PR's code; `docs/perf-artifacts/metrics-sampler-guard/after-counts.txt`, `after-strace.txt`) | 0 | 0 |

`strace`'s 21 vs. the harness's 20 is expected: `strace` counts every
`connect(2)` syscall process-wide, including one the test's own listener
setup issues incidentally, where the harness counts only completed
accepts on that listener. Both independent measurements agree on the
claim that matters: nonzero before, exactly zero after. The after-count is
not a measurement subject to timing at all — the guard makes the function
return before constructing a socket, so it is exactly, unconditionally
zero on every run, forever, for as long as metrics stay disabled and DEBUG
tracing stays off — a stronger guarantee than any measured percentage.
This clears this agent's impact floor ("a measurable reduction in syscall
count") by eliminating the syscalls outright, and the elimination is
corroborated by the assay's 38% wall-clock throughput delta in the same
direction.

A permanent regression test,
`metrics_disabled_samplers_never_touch_the_pool`, pins the after-state on
every `cargo test -p autumn-harvest --lib`, using the same
non-vacuous `AcceptCountingListener` mechanism.

## 🔬 Reproduce

```sh
# Accept count (writes docs/perf-artifacts/metrics-sampler-guard/<PERF_LABEL>-counts.txt):
PERF_LABEL=after cargo test -p autumn-harvest --lib -- \
  worker::tests::zz_capture_metrics_sampler_guard_pool_touch_evidence \
  --exact --ignored --nocapture

# strace corroboration, against the compiled test binary directly:
BIN=$(cargo test -p autumn-harvest --lib --no-run 2>&1 | sed -n 's/.*(\(target\/debug\/deps\/autumn_harvest-[a-f0-9]*\))/\1/p' | tail -1)
strace -f -c -e trace=connect,accept4 "$BIN" \
  worker::tests::zz_capture_metrics_sampler_guard_pool_touch_evidence \
  --exact --ignored --nocapture

# Permanent regression assertions (always run):
cargo test -p autumn-harvest --lib -- \
  worker::tests::metrics_disabled_samplers_never_touch_the_pool \
  worker::tests::concurrency_sampler_stays_active_for_its_saturation_trace_under_debug_tracing \
  --exact
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
- PR [#1468](https://github.com/autumn-foundation/autumn-harvest/pull/1468)
  review (Codex, P2) — found the `spawn_concurrency_sampler` saturation-trace
  gap the "Change" section's amendment fixes.
