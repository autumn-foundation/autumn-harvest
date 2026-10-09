## Engine — A worker runs more than one claim at once (assay #14, re-charter item 1)

**Behaviour change.** A worker now runs up to `max_concurrent_claims` claims
at once. The default is 2. Before, one serial loop ran one claim at a time.
Set `WorkerConfig::with_max_concurrent_claims(1)` to keep the old loop.

**The problem.** Assay #14 measured the claim-loop occupancy of one worker:
claims per second times the mean claim time. It read 0.91 to 0.99 in every
harvest run. So one claim was always in flight, and throughput followed
`1 / (claims per workflow × claim latency)`. The #1971 fix cut the claim
latency, but it did not add claim concurrency.

**The fix.**

- The poll loop stays as the **leader**. It keeps the listener, the timer
  and the capacity wake. After a successful Postgres claim it wakes one
  follower.
- A **follower** waits for that wake. It then claims until a claim returns
  nothing, and wakes one more follower after each success. It has no timer
  and no listener, so an idle worker polls at the single-loop rate.
- Each follower claims through `poll_once`, so it holds its own local
  permits. It skips a shard with a dispatch channel or with an unverified
  registration. The multi-shard loop gets followers too.
- All loops run in one join. The drain starts after every claim returns.

**No SQL change. No migration. No new `WorkflowEvent` variant.** The claim
statement, `FOR UPDATE SKIP LOCKED`, the advisory-lock recheck and the
completion fence are unchanged.

**Config.** `WorkerConfig::max_concurrent_claims`,
`WorkerRuntimeConfig::max_concurrent_claims` and the `max_concurrent_claims`
field of `GET /admin/config`. Validation rejects 0. A struct literal of
`WorkerRuntimeConfig` must now set the field. Use
`worker::DEFAULT_MAX_CONCURRENT_CLAIMS`.

**Metrics.** `harvest.worker.pollers` counts each claim loop, so the default
worker counts 2.

**Tests.** `claim_concurrency_tests` makes each claim of one queue sleep in a
test trigger and logs the claim intervals. Red: with 4 claim loops the
largest overlap was 1. Green: 3 or 4 on the single-pool and multi-shard
paths. Other tests: one loop stays serial; at most one activity row runs
under one activity permit; every task runs once across two workers; an
idle worker polls at the single-loop rate; no row stays `RUNNING` after the
worker stops.

**Measured.** See `docs/performance.md` § "Claim concurrency".
