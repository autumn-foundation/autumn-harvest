### Added

- **Reproducible end-to-end benchmark suite with published results (#941).**
  Harvest had two performance artifacts — the replay CPU budget (#135) and the
  task-claim microbenchmark with its CI gate (#786) — and neither answered the
  first question an evaluating architect asks: *how many workflows per second,
  end to end, and at what latency?* This ships the answer, the harness that
  produces it, and a committed topology so a reader can check it themselves in
  one command.

  Four scenarios at 1, 2 and 4 shards: sustained **workflows completed/sec** for
  a canonical 3-activity workflow; **activity dispatch latency** p50/p99
  (`harvest_task_queue.created_at` → the handler's first line); **signal
  round-trip** p50/p99 (an HTTP signal request leaving the client → the workflow
  resuming past `wait_for_signal`); and **replay throughput** over the same
  10 001-event history #135 budgets. Results are published in
  `docs/benchmarks.md`, with each release's numbers kept in
  `docs/benchmarks/results-v<version>.md` rather than overwritten.

  `./benchmarks/run.sh` is the one documented command: it brings up
  `benchmarks/docker-compose.yml` (four **independent** Postgres servers, one per
  shard — a Harvest shard is a database with its own pool, so four databases on
  one server would make a shard sweep a measurement of one server contending
  with itself), runs all twelve cells, and tears it down.
  `HARVEST_BENCH_CHECK=1` additionally compares the run against the published
  baselines at ±15% and prints a per-number verdict; `HARVEST_BENCH_SCENARIOS`
  and `HARVEST_BENCH_SHARDS` narrow the matrix so reproducing one headline does
  not cost forty minutes.

  **Measurement discipline is the actual engineering here**, and every rule is
  enforced by the harness rather than promised in prose. Throughput is a
  **bounded closed loop**, not a pre-loaded drain: a first implementation that
  pre-loaded a backlog reported 473 workflows/s on the middle half of the drain
  and 55/s over the whole of it, because claim cost grows superlinearly with
  backlog depth — the drain accelerates as it empties, so the "rate" was an
  artifact of where you looked, and publishing it would have re-published #786's
  claim-depth curve under an end-to-end label. The closed loop keeps depth
  shallow and constant; `inflight_soundness` refuses to publish a run in which
  the harness, not the engine, was the limiter (it fires at 384 in flight on the
  reference box). The published default is **four times** the worker's workflow
  slots: a deeper population buys a higher number by measuring further into the
  claim-depth curve, which is #786's finding rather than this suite's. The two latency scenarios are paced at ~30% of the measured saturated
  rate, because a p99 taken under saturation measures the backlog rather than
  the dispatch path, and `pacing_verdict` marks a run that could not hold its
  pace. The signal round-trip is read from **one monotonic clock in one
  process**, so it carries no skew term; activity dispatch necessarily spans a
  database and a host clock, so the harness measures the offset and publishes it
  beside the number, and a negative sample — which only skew can produce — is
  counted and reported rather than clamped to zero. Any cell that is thin,
  truncated, left a shard idle, or failed a request reports `n/a` with a named
  reason.

  Replay is run at all three shard counts as the **noise control**: it is
  in-memory and cannot legitimately move with shard count, so drift across those
  three runs bounds how loaded the box was while the other nine cells were
  measured. Its history builder now lives in the shared harness and
  `benches/replay_bench.rs` calls it, so the throughput published here and the
  #135 budget can never drift into describing different workloads.

  **No duplication of #786 and no new CI gate.** The suite contains no claim or
  enqueue scenario, `docs/benchmarks.md` cross-references `docs/performance.md`
  as the component-level complement, and `benchmarks_docs.rs` fails the build if
  a CI manifest row ever runs an end-to-end scenario (issue #941 puts CI-gated
  end-to-end budgets out of scope until baselines stabilise). The same guard
  pins the doc to the harness: the published baselines are constants, the index
  page and the versioned results file must both carry them (the results file on
  a matching scenario/shard row, not merely somewhere in the text), every
  documented environment variable must be one the harness actually reads, the
  documented command must be the committed runner, the published worker and
  concurrency configuration must be the constants the run used, and the compose
  file must define a service per shard.

  **Zero engine impact.** No new `WorkflowEvent` variant, no migration, no
  behaviour or public-API change; nothing under `autumn-harvest/src/` is touched.
  Every number is taken from columns and events that already existed.

  Honest limits, all stated with the numbers: durability is off
  (`fsync=off`/`synchronous_commit=off`) so these are an upper bound for a
  durably configured Postgres; the signal endpoint is a minimal HTTP/1.1 handler
  calling the same `signal::send_signal` entry point the plugin route calls, not
  autumn-web's middleware stack; the workflow is deliberately trivial; the shard
  sweep holds hardware fixed, so it bounds *software* scale-out on one machine
  and says nothing about a four-machine deployment; and the published numbers
  come from native Postgres clusters rather than containers, which makes the
  compose path the more pessimistic of the two.
