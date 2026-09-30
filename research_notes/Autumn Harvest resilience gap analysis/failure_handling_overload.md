# Failure Handling and Overload Control: Best Practices and Autumn Harvest Gap Analysis

Scope: retries, timeouts, idempotency, circuit breaking, backpressure, load shedding, admission control, fairness, bulkheads, metastable failures. Research date: 2026-09-30. Codebase: `/home/user/autumn-harvest` at the checked-out `HEAD` (workspace version 0.6.0, `Cargo.toml:26`). Code citations use `path:line` relative to the repo root. Status labels used throughout: **ABSENT**, **PRESENT-UNTESTED**, **PRESENT-NOT-WIRED**, **PRESENT-TESTED** (a test file exists; I did not run it: the brief forbade `cargo test`).

---

## Q1. What do the AWS Builders' Library, the Google SRE book and the metastable-failure papers recommend?

### Takeaway
All four sources say the same thing. Retry at one layer only. Bound retries with a per-request cap and a per-client budget. Jitter all retries and all periodic work. Set timeouts from downstream latency percentiles. In asynchronous systems, treat the age of queued work as the main health signal: bound it, drop or sideline stale work, and send backpressure to producers. The metastable-failure papers show that retries are the most common feedback loop that keeps an outage going after its trigger is gone. They treat that loop, not the trigger, as the root cause.

### Cited Findings
**Timeouts, retries, jitter (Brooker, AWS Builders' Library; the PDF is undated, and the article has been online since about 2019)**
- Pick timeouts from downstream latency: "we choose an acceptable rate of false timeouts (such as 0.1%). Then, we look at the corresponding latency percentile on the downstream service (p99.9 in this example)." — [Brooker, Timeouts, retries, and backoff with jitter (PDF)](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)
- Retry amplification across layers: in a five-deep stack with three retries per layer, "the load on the database will increase 243x, making it unlikely to ever recover … our best practice is to retry at a single point in the stack." — [same](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)
- Brooker notes that circuit breakers "introduce modal behavior into systems that can be difficult to test, and can introduce significant addition time to recovery." AWS instead limits retries locally with a token bucket: calls retry while tokens remain, then "retry at a fixed rate when the tokens are exhausted". This has been in the AWS SDK since 2016. — [same](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)
- "APIs with side effects aren't safe to retry unless they provide idempotency." — [same](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)
- "If all the failed calls back off to the same time, they cause contention or overload again when they are retried. Our solution is jitter." The article also says to "add some jitter to all timers, periodic jobs, and other delayed work". For scheduled work, AWS picks jitter "consistent[ly] … the same number every time on the same host", not at random, so that problems repeat as a recognisable pattern. — [same](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)

**Queue backlogs (Yanacek, AWS Builders' Library; undated)**
- Queue-based systems are bimodal. When arrival rate exceeds processing rate, the system "flips into a more sinister operating mode … it can take a great deal of time to work through the backlog". — [Yanacek, Avoiding insurmountable queue backlogs (PDF)](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)
- Alarm on the age of messages in the queue, not only on depth or DLQ volume. DLQ information "would arrive too late for us to rely on it exclusively". — [same](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)
- "In asynchronous systems, each component of our systems needs to protect itself from overload … There will always be some workload that gets around the front-door admission control". A single shared queue "and multitenancy are at odds with each other". Real-time consumers "prefer LIFO-ish behavior". — [same](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)
- Other techniques in the same article: per-customer fairness throttles with bursting, shuffle-sharding, sidelining excess or old traffic to a separate queue (an approximation of LIFO), message TTL, and backpressure. For backpressure, the producer is throttled "(inversely) proportionally to backlog size". — [same](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)
- Heartbeating and leases: if a processor overruns its visibility timeout, "the same message can be delivered multiple times in parallel … a second processor will pick it up and similarly churn away past the timeout, and then a third … This potential for cascading brownouts is why we implement our message processing logic to stop work when a message expires, or to continue to heartbeat". — [same](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)
- Avoid too many in-flight messages: a consumer that dequeues work it cannot finish inflates the in-flight count. AWS favours "moving the excess messages to a different queue instead of letting them remain visible". — [same](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)

**Load shedding (Yanacek, AWS Builders' Library; undated)**
- Goodput is "the subset of the throughput that is handled without errors and with low enough latency for the client to make use of the response". Load shedding keeps goodput flat as offered load rises, until the cost of rejecting requests itself dominates (Amdahl). — [Yanacek, Using load shedding to avoid overload (PDF)](https://d1.awsstatic.com/builderslibrary/pdfs/using-load-shedding-to-avoid-overload.pdf)
- Prioritise finishing work over starting it: "the service should prioritize end() requests over start() requests. If it prioritized start(), clients wouldn't be able to complete the work they started, resulting in brownouts." — [same](https://d1.awsstatic.com/builderslibrary/pdfs/using-load-shedding-to-avoid-overload.pdf)
- "In addition to bounding the size of queues, we've found it's extremely important to place an upper bound on the amount of time that an incoming request sits on a queue, and we throw it out if it's too old … we look for ways to use a last in, first out (LIFO) queue". The article also covers client-supplied timeout hints and deadline propagation between hops. — [same](https://d1.awsstatic.com/builderslibrary/pdfs/using-load-shedding-to-avoid-overload.pdf)

**Google SRE book (2016; still the canonical reference)**
- Client-side adaptive throttling: track `requests` and `accepts` over two minutes, and reject locally with probability `(requests − K·accepts)/requests`, where K defaults to 2. Retries are capped at 3 attempts per request, and a per-client retry budget allows retries only while retries are under 10% of requests. Together these limit worst-case growth to about 1.1x. Requests carry criticality levels (CRITICAL_PLUS, CRITICAL, SHEDDABLE_PLUS, SHEDDABLE). When a large share of a backend is overloaded, it returns a "don't retry" error that is passed up to the caller. — [SRE book, Handling Overload](https://sre.google/sre-book/handling-overload/)
- Keep queues small, for example "50% or less" of the thread-pool size. Use LIFO or CoDel to drop stale requests. Always use "randomized exponential backoff". Use a server-wide retry budget. Never let retries multiply across layers. Propagate deadlines and cancellation. Load test "until they break". — [SRE book, Addressing Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/)

**Metastable failures**
- Bronson et al., HotOS 2021: metastable failures "occur in open systems with an uncontrolled source of load where a trigger causes the system to enter a bad state that persists even when the trigger is removed", with low goodput and "a sustaining effect—often involving work amplification". The authors "consider the root cause of a metastable failure to be the sustaining feedback loop, rather than the trigger". — [Bronson, Aghayev, Charapko, Zhu, HotOS'21 (PDF)](https://sigops.org/s/conferences/hotos/2021/papers/hotos21-s11-bronson.pdf)
- Bronson's suggested remedies include changing policy during overload: "disable failover and retries or set a retry budget, switch to LIFO scheduling …, reduce internal queue sizes, enforce priorities during overload, shed load". They also suggest giving retried work a lower priority than fresh work. They warn that "software structure encodes implicit priorities": a staged design that "deserializes as many requests as possible before processing them" favours intake over completion, and preserving goodput needs the opposite policy. One geo-distributed system that added retries and failover reached ">100×" worst-case amplification. — [same](https://sigops.org/s/conferences/hotos/2021/papers/hotos21-s11-bronson.pdf)
- Huang et al., OSDI 2022, studied 22 metastable failures across 11 organisations and found that "at least 4 out of 15 major outages in the last decade at Amazon Web Services were caused by metastable failures". "By far, the most common sustaining effect is due to the retry policy, affecting more than 50% of the studied incidents." Recovery used direct load shedding "in over 55% of the cases". "A policy with at most two retries will not amplify the work more than three times, while the policy with no cap effectively leaves the system with no stable region." — [Huang et al., Metastable Failures in the Wild (PDF)](https://www.usenix.org/system/files/osdi22-huang-lexiang.pdf)
  - Source conflict: a WebFetch summary of the same PDF reported "approximately 25%" for retries. The extracted full text says ">50%". The full-text figure is authoritative, so the 25% figure should be ignored.

### Inferences
- A durable workflow engine is an asynchronous queue-based system of exactly the kind Yanacek describes. The guidance that applies most directly is: bound the age of queued work, heartbeat or fence leases, protect every component and not only the front door, and prioritise completing in-flight runs over admitting new ones (the start()/end() rule).
- AWS schedules jitter deterministically per host. That supports Autumn Harvest's choice of seeding its retry jitter deterministically (see Q4), provided jitter is actually turned on.

### Gaps
- The AWS Builders' Library pages now redirect to `builder.aws.com`, which renders client-side and returned no text. I used the static PDFs instead. They carry no publication dates.

---

## Q2. Retry budgets, adaptive concurrency, CoDel/LIFO, deadline propagation, hedging, priority shedding: what are they, and when do they fit a workflow engine?

### Takeaway
Retry budgets and token-bucket retry throttling cap the extra load that retries add. Adaptive concurrency limits (Vegas/Gradient/AIMD) find the safe number of in-flight requests without manual tuning. CoDel with adaptive LIFO bounds how long work waits in a queue. Deadline propagation and priority shedding stop wasted work. Hedging lowers tail latency, but only for idempotent, cheap reads. For a workflow engine, the high-value set is: retry budgets per activity type or downstream, pull-based claiming bounded by local capacity, age-based expiry and priority of work, and prioritising continuations over new starts. Hedging applies mostly to engine-internal reads, if at all.

### Cited Findings
- Retry strategies compared by Brooker. N retries costs "(1+N) times the work at 100% failure". A token bucket behaves like N retries at low failure rates and like "0.1 retries" at high failure rates. A retry circuit breaker gives "no additional load at high failure rates" but switches in a binary way. With many small clients, per-client estimates diverge and neither approach is perfect without shared state. — [Brooker, "Fixing retries with token buckets and circuit breakers" (2022)](https://brooker.co.za/blog/2022/02/28/retries.html)
- Netflix concurrency-limits frames capacity as Little's law (`Limit = RPS × latency`). Vegas estimates queue depth as `L·(1 − minRTT/sampleRTT)` and adjusts the limit by ±1. Gradient2 compares long and short latency averages. AIMD is loss-based and suited to clients. Partitioned limiters reserve shares by traffic class (for example 90% live, 10% batch). Server-side use rejects with 429 or UNAVAILABLE. — [Netflix/concurrency-limits (GitHub)](https://github.com/Netflix/concurrency-limits)
- Facebook "Fail at Scale" (Maurer, ACM Queue, 2015) adapts CoDel to server queues, with M = 5 ms and N = 100 ms "tend[ing] to work well". Adaptive LIFO switches from FIFO to LIFO when a queue forms. The article also covers concurrency control. — [Fail at Scale, ACM Queue](https://queue.acm.org/detail.cfm?id=2839461) (the page returned 403 to my fetch, so the parameters come from the search snippet and the summary at [the morning paper](https://blog.acolyer.org/2015/11/19/fail-at-scale-controlling-queue-delay/))
- Hedged requests (Dean & Barroso, "The Tail at Scale", CACM 2013) send a second request after the p95 latency, which adds about 5% load. In Google's BigTable benchmark, a 10 ms hedge cut 99.9th-percentile latency from 1,800 ms to 74 ms with 2% more requests. — [The Tail at Scale (CACM)](https://cacm.acm.org/research/the-tail-at-scale/) (403 on fetch; figures from search snippets of the same article)
- Deadline propagation and criticality: see the SRE citations in Q1. — [SRE book, Addressing Cascading Failures](https://sre.google/sre-book/addressing-cascading-failures/)
- Temporal (the closest peer) defaults activity retry to initial interval 1 s, coefficient 2.0, max interval 100× initial, and unlimited attempts. Workflows do not retry by default. Temporal recommends bounding retries with Schedule-To-Close rather than maximum attempts. Its retry-policy page does not mention jitter. — [Temporal docs, Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)

### Inferences
- **Retry budgets and token buckets** fit Autumn Harvest's activity retries well. Retries are durable, re-enqueued rows. A per-activity-type or per-downstream budget (for example, retries at most X% of first attempts over a window) would give smooth degradation instead of the circuit breaker's on/off behaviour.
- **Adaptive concurrency** applies at the worker, which decides how many tasks to claim and run. It needs a latency or downstream-error signal, not only DB-pool pressure.
- **CoDel/LIFO:** strict LIFO does not suit a workflow engine, because it breaks FIFO fairness among a workflow's own tasks. Age-based priority ageing, schedule-to-start expiry, and priority for continuations capture most of the benefit.
- **Hedging** is mostly inappropriate for activities, which have side effects and are not guaranteed idempotent. It could help for idempotent engine reads such as queries or history fetches under tail latency, but that is low priority.
- **Deadline propagation** in a workflow engine means that when a caller's or parent's deadline has passed, the engine should not start or claim work whose result can no longer be used.

### Gaps
- I found no published guidance from Temporal or Restate on retry budgets or server-side load shedding for workers. Temporal's server-side rate limiting and "worker tuner" (slot suppliers) documentation was not reviewed.

---

## Q3. Idempotency: keys, exactly-once vs effectively-once, activity side-effect dedup (Temporal, Restate, Stripe)

### Takeaway
The industry standard is a caller-supplied idempotency token. The server records it in the same transaction as the side effect, returns a semantically equivalent response to retries, rejects a reused token that carries different parameters, and keeps tokens for the resource lifetime plus a margin. Durable-execution engines give exactly-once *recording* of step results and at-least-once *execution* of side effects. Effectively-once behaviour for external effects depends on activities passing a stable idempotency key downstream.

### Cited Findings
- AWS: use "a unique caller-provided client request identifier". Return semantically equivalent responses, even if the resource was later deleted. Return a validation error when a token is reused with different parameters. Record the token "atomic[ally]" with the resource creation. Retain it "for the lifetime of the resource, plus an interval". — [Featonby, Making retries safe with idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/)
- Stripe uses an `Idempotency-Key` header on mutating POSTs. Clients retry with exponential backoff plus jitter to avoid a thundering herd (Leach, 2017; older but canonical). — [Stripe blog, Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency)
- Temporal expects activities to "re-execute upon failure" and asks authors to make them idempotent. — [Temporal docs, Retry Policies](https://docs.temporal.io/encyclopedia/retry-policies)
- Restate journals each side-effecting step and its result, and replays completed steps on recovery. An idempotency key on an invocation deduplicates it and returns the original result. A failed `ctx.run` step is retried with exponential backoff. — [Restate docs, Durable Execution](https://docs.restate.dev/concepts/durable_execution)

### Inferences
- "Exactly-once" in workflow engines means exactly-once state transitions. External side effects stay at-least-once unless the downstream deduplicates on a key the engine keeps stable across retries and redeliveries. Autumn Harvest provides such a key (see Q4).

### Gaps
- I did not find Restate's current default retry limits for `ctx.run`. The fetched page did not say whether retries are unbounded.

---

## Q4. Codebase audit: does Autumn Harvest implement each practice, where, is it tested, is it wired, and are the defaults safe?

### Takeaway
Autumn Harvest has a broad toolbox of opt-in resilience features, most with integration tests: per-activity circuit breaker, throttles, quotas, concurrency caps, debounce, queue weights, an adaptive slot tuner, poison-pill quarantine, activity idempotency keys, and start idempotency. The **defaults** are weaker than the toolbox:
- Retry jitter is off by default.
- There is no retry budget.
- DB pool acquisition has no timeout on the single-shard path, and hot-path SQL has no `statement_timeout`.
- The default Postgres claim path **over-claims** past local concurrency while the start-to-close and heartbeat clocks run from claim time.
- There is no automatic, load-driven shedding or backpressure to producers. The admission gate is a manual operator switch.

### Cited Findings (by practice)

**Retries and backoff**
- `RetryPolicy { max_attempts, initial_interval, backoff_coefficient, max_interval, non_retryable_errors, jitter }` — `autumn-harvest/src/policy.rs:126-139`. The default is `exponential(3, 1s)` with a max interval of 300 s — `policy.rs:155-163, 248-251`. Attempts are capped by default, which is safer than Temporal's unlimited default ([Temporal](https://docs.temporal.io/encyclopedia/retry-policies)). **PRESENT-TESTED** (`tests/integration/retry_*_tests.rs`, `workflow_retry_tests.rs`).
- **Jitter defaults to `None`.** `JitterPolicy` has `#[default] None` (`policy.rs:35-42`), and both constructors set `jitter: JitterPolicy::None` (`policy.rs:162, 186`). Full, Equal and Decorrelated jitter are implemented (`policy.rs:62-110`) and seeded deterministically from the workflow and activity ids (`autumn-harvest/src/worker.rs:5024-5042`; workflow retry uses the execution id at `worker.rs:8234-8241`). So jitter is **PRESENT but opt-in**. Every activity that fails at the same moment under the default policy retries at the same instants: 1 s, 2 s, 4 s. This is the synchronised-retry problem Brooker describes ([PDF](https://d1.awsstatic.com/builderslibrary/pdfs/timeouts-retries-and-backoff-with-jitter.pdf)).
- If a task has no retry policy, the fallback is a **fixed 1 s retry** until `max_attempts` (the enqueue default is 3), with no backoff and no jitter — `worker.rs:5091-5100`, `autumn-harvest/src/queue.rs:407`.
- Engine-internal backoffs have no jitter either: `nd_block_backoff` (5 s to 300 s, `worker.rs:5612-5621`), `panic_retry_backoff` (1 s to 30 s, `worker.rs:7469-7476`), and the Redis-dispatch degraded cooldown (`worker.rs:26396-26403`). The comments do call the child-quota and session-acquire paths "bounded jittered backoff" (`worker.rs:16892-16896, 16954`).
- The `Retry-After` hint is honoured and clamped to a ceiling (default 15 min, `autumn-harvest/src/builder.rs:67`; logic at `policy.rs:280-291`, `worker.rs:5107-5112`). The code documents a footgun: a ceiling of zero means *immediate* retry, not "disabled" (`policy.rs:263-271`). **PRESENT-TESTED** (`worker.rs:40339-40472`, `tests/integration/retry_after_tests.rs`).
- Non-retryable classification by typed error class — `policy.rs:241-246`. **PRESENT-TESTED**.
- **Retry budget or token-bucket retry throttle: ABSENT.** Searches for `retry_budget`, `retry_ratio` and similar found nothing, and a GitHub issue search for "load shedding overload retry storm backpressure" in autumn-foundation/autumn-harvest returned 0 results. The only aggregate retry brake is the opt-in circuit breaker.

**Timeouts**
- Activity `start_to_close` has **no default**: `WorkerConfig.default_activity_start_to_close: None` (`builder.rs:3938`), and "`None` when unset (no timeout enforced)" (`policy.rs:1437-1440`). `ActivityInfo.default_heartbeat_timeout` defaults to `None` = disabled (`autumn-harvest/src/info.rs:1371-1372`). A hung activity on a *live* worker therefore runs forever unless its author sets a timeout. The stuck-running backstop covers only `workflow` tasks (`autumn-harvest/src/poison_pill.rs:125-145`). **Default unsafe.**
- Timeout enforcement scanner: heartbeat, start-to-close, schedule-to-start and schedule-to-close checks (`autumn-harvest/src/timeout.rs:86-105`). **PRESENT-TESTED** (`child_timeout_tests.rs`, `workflow_task_timeout_tests.rs`, `dag_execution_timeout_tests.rs`, and others).
- An activity start-to-close or heartbeat timeout appends `ActivityTimedOut` and calls `queue::fail_task`, which is **terminal for the task and not retried per `RetryPolicy`** (`timeout.rs:1452-1466`). The v0.1 design draft says instead "Start-to-Close … retried per policy" (`docs/autumn-workflow-architecture.md:1127`, dated March 2026). This is a doc/code discrepancy worth confirming. It differs from Temporal, where timeouts are retryable.
- **DB timeouts on the hot path: ABSENT.** The code says so directly: "Harvest configures no deadpool `Timeouts`, so every `pool.get().await` is an **unbounded** wait" (`worker.rs:5920-5921`). The single-shard path deliberately keeps "the original unbounded `pool.get().await`" (`worker.rs:5956-5960, 6010`). The multi-shard path bounds acquisition at max(poll_interval, 5 s) (`worker.rs:5915, 5962-5979`). `statement_timeout` and `lock_timeout` appear only in partition and replication maintenance (`autumn-harvest/src/partition.rs:1014, 4250`; `autumn-harvest/src/replication.rs:1113, 1794`), not in claim, persist or timeout paths. The timeout enforcer's own `FOR UPDATE` "can block for an UNBOUNDED period" (`timeout.rs:1390-1393`). Related closed issues: #1323 ("Bound the remaining unbounded peer-pool acquisitions…", closed 2026-09-18) and #1459 ("parent task can wedge RUNNING … when the workflow-task-timeout reset races DB-pool contention", closed 2026-09-14).
- HTTP clients: the completion-callback deliverer has a 10 s timeout (`autumn-harvest-plugin/src/callback_deliverer.rs:16, 47-48`), and so does the audit sink (`autumn-harvest-plugin/src/audit_sink.rs:70-71`). The CLI and TUI use `reqwest::Client::new()` with **no timeout** (`autumn-harvest-cli/src/lib.rs:7630, 7700`; `autumn-harvest-cli/src/tui.rs:49`), as does the plugin dev runtime (`autumn-harvest-plugin/src/dev/mod.rs:563`). These are operator tools, so the risk is low.
- **Deadline propagation: PARTIAL.** Activities receive a deadline through `ctx.info()` (`docs/architecture.md:1088-1133`). Local activities clamp to the enclosing workflow-task deadline (`worker.rs:4720-4730`). I found no mechanism that stops claiming or starting an activity whose parent workflow or run deadline has already passed. A search for `deadline_would_be_exceeded` found only a retry-vs-deadline check (`worker.rs:39799`).

**Circuit breaker**
- Per-activity closed/open/half-open breaker with a rolling window and a single half-open probe (`autumn-harvest/src/circuit_breaker.rs:1-36`). It is consulted before dispatch (`worker.rs:14881-14890`). It is **opt-in** (`info.rs:1457`) and **PRESENT-TESTED** (`tests/integration/circuit_breaker_wiring_tests.rs`, loom model in `tests/loom_models.rs`).
- Semantics: while open, dispatches fail fast with a **non-retryable** `"CircuitOpen"` failure (`circuit_breaker.rs:12-15`). An outage longer than the cooldown therefore becomes a *workflow-visible permanent failure* unless the workflow code handles it. State is in-process, per shard and per worker (`circuit_breaker.rs:30-36`), so N workers each need `failure_threshold` failures before tripping.
- The breaker is also fed by start-to-close and heartbeat **timeouts** on RUNNING tasks (`timeout.rs:1470-1494`). Only schedule-to-start and PENDING schedule-to-close are excluded. With over-claim (see Backpressure below), a task that waited behind a local semaphore and never ran the handler can still count as a downstream failure. That is my inference and is not tested.

**Backpressure and claiming**
- The worker bounds execution with two semaphores. The defaults are `max_concurrent_workflows = 20` and `max_concurrent_activities = 50` (`builder.rs:3929-3930`).
- **The default Postgres poll path claims without checking local capacity.** `poll_once` claims a row and calls `dispatch_task` (`worker.rs:30203-30220`). The code comment states that "the worker can over-claim past `max_concurrent_*` (the semaphore gates execution, not claiming)" (`worker.rs:30211-30214`). The spawned task only then waits on `semaphore.acquire()` (`worker.rs:30360-30364`). The poll loop immediately `continue`s after every successful claim (`worker.rs:29592-29625`). One worker can therefore move a large backlog from PENDING to RUNNING under its own id. Those rows are then invisible to idle peers.
- The claim stamps `started_at = NOW()` (`queue.rs:1113`). Start-to-close and heartbeat deadlines are measured from `started_at` (`timeout.rs:86-91`, `timeout.rs:100-105`). Over-claimed tasks therefore **use up their timeout budget while queued locally**.
- The Redis dispatch-channel path (opt-in, issue #1312) *does* gate claims on free permits, and its comment names the problem: "A workflow row claimed with no free workflow permit sits `RUNNING` under this worker while it waits on the local semaphore. A peer with capacity cannot claim it." (`worker.rs:26548-26556`; gating at `worker.rs:28877-28903, 28996-29023`). **PRESENT (channel path only); ABSENT (default Postgres path).**
- Channels: no `unbounded_channel` exists in `autumn-harvest/src`, `autumn-harvest-plugin/src` or `autumn-harvest-redis/src` (grep). The heartbeat channel is bounded at 64, and its `send().await` applies backpressure to the activity (`autumn-harvest/src/heartbeat.rs:30-39`; `autumn-harvest/src/context.rs:14954`). **Good.**
- **Backpressure to producers or start API: ABSENT.** Queue backlog is only *classified* for status output (`autumn-harvest-plugin/src/status_summary.rs:301-310`). The start API returns 503 only when the DB or token store is unavailable (`autumn-harvest-plugin/src/plugin.rs:2387-2416`), never because of backlog depth or age.

**Load shedding and admission control**
- The admission gate is a *manual* incident-response switch. Operators scope it by fleet, workflow, queue, shard or owner, and it persists in Postgres (`autumn-harvest/src/admission_gate.rs:1-7, 72-80`). It is enforced across start producers (`admission_gate.rs:9-32`). Continuations are deliberately not gated: workflow retries, continue-as-new, child spawns and resets (`admission_gate.rs:34-45`). This matches the start()/end() guidance ([Yanacek](https://d1.awsstatic.com/builderslibrary/pdfs/using-load-shedding-to-avoid-overload.pdf)). `start_or_load` is noted as still ungated (`admission_gate.rs:40-43`). **PRESENT-TESTED** (`admission_gate_tests.rs`, `admission_gate_authoritative_tests.rs`).
- **Automatic, load-driven shedding (by queue age or depth, DB latency, or pool saturation): ABSENT.** Searches for load shedding, overload, 429 or backlog limits found only status classification.
- Throttle: token-bucket or GCRA admission pacing per key that defers rather than drops (`docs/architecture.md:115`; `autumn-harvest/src/throttle.rs`). It is opt-in per workflow (`info.rs:512`) and **PRESENT-TESTED** (`throttle_tests.rs`). Buckets are per shard, and cross-shard global rate is documented as out of scope (`docs/architecture.md:115`).
- Quota: per-tenant caps on active executions, history bytes and DLQ count (`autumn-harvest/src/quota.rs:1-45`). Opt-in (`info.rs:526`). **PRESENT-TESTED** (`quota_enforcement_tests.rs` and others).
- Debounce (`autumn-harvest/src/debounce.rs`; opt-in, `info.rs:503`) is **PRESENT-TESTED** (`debounce_tests.rs`). The throttle, debounce and event-batch scanner fires bypass the admission gate, with a counter (`admission_gate.rs:26-28`).

**Fairness, priority and bulkheads**
- Weighted random queue ordering (Efraimidis-Spirakis) with a no-starvation guarantee (`autumn-harvest/src/queue_fairness.rs:1-28`). It is off by default: with an empty weight map the claim path is a single `ANY($2)` query (`worker.rs:30129-30133`). **PRESENT-TESTED** (`queue_fairness_tests.rs`).
- Priority with optional ageing: the claim orders by aged `priority DESC, scheduled_at ASC` (`queue.rs:1093-1099`). **PRESENT-TESTED** (`priority_tests.rs`). There is no automatic priority for continuations (workflow tasks of running executions, activity results) over first workflow tasks of new starts. This is my inference; I found no such rule.
- Multi-shard poll loops round-robin, so "a deep backlog on one shard cannot starve dispatch on another" (`docs/architecture.md:63`).
- Bulkheads:
  - Separate web and worker DB pools under a shared ceiling (`autumn-harvest/src/pool.rs:1-17, 89-117`). **PRESENT-TESTED** (unit tests `pool.rs:135-216`).
  - Per-activity-type cluster-wide `max_concurrent` with a shared `concurrency_key` (`info.rs:1377-1383`).
  - Per-activity rate limits (`info.rs:1414-1451`).
  - Per-key workflow concurrency (`autumn-harvest/src/concurrency.rs`).
  - Queue and activity pauses (`autumn-harvest/src/queue_pause.rs`, `autumn-harvest/src/activity_pause.rs`).
  - Worker sessions (`autumn-harvest/src/sessions.rs`).
  All are **PRESENT-TESTED** (`concurrency_key_tests.rs`, `rate_limit_key_tests.rs`, `queue_pause_tests.rs`, `activity_pause_tests.rs`, `worker_session_tests.rs`). The within-worker semaphores are *per kind* (workflow or activity), not per activity type. One slow activity type can occupy all 50 local slots unless `max_concurrent` is set.

**Adaptive concurrency**
- The slot tuner (`autumn-harvest/src/slot_tuner.rs:1-40`) is **opt-in**: `None` by default gives fixed semaphores (`docs/architecture.md:119`). **PRESENT-TESTED** (`slot_tuner_tests.rs`). Its decision rule shrinks by `shrink_step` when the *DB pool* is saturated and grows by `grow_step` when slots are saturated and permit wait exceeds a threshold (`slot_tuner.rs:181-197`). It has no downstream latency or error signal (Vegas or Gradient style, per [Netflix](https://github.com/Netflix/concurrency-limits)). Its grow signal, local permit wait, is inflated by over-claiming.

**Poison pills and DLQ**
- Crash-strike quarantine: the default threshold is 3, and 0 disables it (`docs/architecture.md:117`; `poison_pill.rs:43-50`). The worker-liveness orphan reclaim is at `poison_pill.rs:104-113`. The stuck-running backstop for workflow tasks "never touches `crash_strikes`" (`poison_pill.rs:352-356`). **PRESENT-TESTED** (`poison_pill_tests.rs`).
- DLQ bulk redrive is capped at 100 by default and 1000 at most per call (`autumn-harvest/src/dlq.rs:24-26`). Redriven tasks are not spread over time. That is an inference from the absence of pacing code in `dlq.rs`.

**Idempotency**
- Activity idempotency key: it is stable across retries, redelivery and replay, derived from `ActivityExecId` (`autumn-harvest/src/types.rs:1141-1149`), and exposed through `ctx.idempotency_key()` with `subkey()` and an optional per-attempt subkey (`context.rs:14520-14541, 14560-14563`). **PRESENT-TESTED** (`idempotency_tests.rs`).
- Start idempotency: keyed by `(workflow_name, idempotency_key)`, a 24 h default window, an atomic `INSERT … ON CONFLICT`, reserve-and-start rolled back together, and shard routing by key (`autumn-harvest/src/start_idempotency.rs:1-45`). This follows AWS's atomic-recording and retention guidance. Unlike AWS, the module doc does not describe rejecting a reused key that carries a *different* input, so that behaviour is unverified. **PRESENT-TESTED** (`start_idempotency_tests.rs`).
- Completion callbacks carry a `delivery_id` and an `X-Harvest-Delivery-Id` header, with at-least-once delivery (`autumn-harvest/src/completion_callback.rs:29, 948`). Their retry treats *every* non-2xx or transport error alike (`completion_callback.rs:1566-1588`). It does not distinguish permanent 4xx from 429/503, and it does not parse the receiver's `Retry-After`.
- Activity start fencing: `append_activity_started_if_pending` skips the handler unless the task row is still `RUNNING` (`worker.rs:5411-5434`). It checks `state` only, not `worker_id` or attempt. Terminal writes re-check the claim through `claim_still_held_for_update` (for example `worker.rs:8283`).

**Scheduling jitter and overlap**
- Schedule fire jitter exists but defaults to `Duration::ZERO` (`policy.rs:878-895, 1037`; cron cap at 1 h, `policy.rs:1341`). The default overlap policy is `Skip` (`policy.rs:558-560`), which is safe.

### Inferences
- Many defaults push the operator into manual tuning (jitter off, no activity timeout, no retry budget, fixed semaphores, unbounded pool waits). This is the pattern Netflix and AWS argue against. The toolbox is rich, but a default deployment gets little of its protection.
- The terminal (non-retried) treatment of activity timeouts limits retry amplification. It also means overload-induced timeouts become workflow failures, which is a correctness and availability trade-off.

### Gaps
- I did not run the test suites; the brief forbade `cargo test`. "PRESENT-TESTED" means a dedicated test file exists and targets the feature.
- I did not confirm whether `append_activity_started_if_pending` can pass for a stale worker after the same `task.id` has been re-claimed by a peer. In the timeout path the task is terminally failed and gets `ActivityTimedOut` (`timeout.rs:1452-1466`), so `pending_activity_id_for_task` should return `None` afterwards. The requeue paths (`queue::requeue_for_retry`, `queue.rs:2944`) were not traced end to end.
- I did not check whether start idempotency rejects a same-key start that carries a different input.

---

## Q5. Could any feedback loop in Autumn Harvest cause a metastable failure?

### Takeaway
Yes. Three loops are plausible, and each has code evidence. None has a test or a published analysis. None is tracked by an open GitHub issue: the 30 open issues on 2026-09-30 contain none on overload or metastability.
1. **Over-claim → timeout → failure/retry loop.** Workers claim past capacity, and timeouts run from claim time.
2. **Fleet-size-proportional background scan load.** Every worker runs timeout and sweep scans and samplers every 500 ms per shard, without jitter or leader election. Adding workers to relieve a backlog adds DB load.
3. **DB-pool contention → missed heartbeats and stalled writes → timeouts.** Pool waits and SQL statements are unbounded, and heartbeats share the worker pool.
Synchronised retries (jitter off) and per-enqueue `pg_notify` inside commit transactions both raise the load peak in the vulnerable state.

### Cited Findings
- Loop 1 evidence:
  - The worker over-claims past its concurrency (`worker.rs:30211-30214`), and the poll loop re-claims immediately after each success (`worker.rs:29592-29625`).
  - `started_at` is set at claim (`queue.rs:1113`), and timeouts measure from `started_at` (`timeout.rs:86-91, 100-105`).
  - A timeout terminally fails the task (`timeout.rs:1452-1466`) and feeds the circuit breaker (`timeout.rs:1487-1494`). The breaker then fails further dispatches non-retryably (`circuit_breaker.rs:12-15`).
  - Workflow-level retries are opt-in (`info.rs:566`) and are not gated by admission (`admission_gate.rs:34-39`). They re-enter the queue.
  - This matches AWS's description of "cascading brownouts" from work running past its lease ([Yanacek](https://d1.awsstatic.com/builderslibrary/pdfs/avoiding-insurmountable-queue-backlogs.pdf)) and Bronson's warning that structures favouring intake over processing encode the wrong priority under overload ([HotOS'21](https://sigops.org/s/conferences/hotos/2021/papers/hotos21-s11-bronson.pdf)).
- Loop 2 evidence:
  - The timeout checker runs per assigned shard in every worker process, with `self.config.poll_interval` as its interval (`worker.rs:28302-28308`). The default poll interval is 500 ms (`worker.rs:81`).
  - The loop sleeps a fixed `interval` with no jitter (`timeout.rs:5186-5189`).
  - The start-to-close and heartbeat scans are `SELECT *` without `LIMIT` (`timeout.rs:86-105, 488-491`).
  - Codec rotation, completion-callback firing and start-idempotency purge are folded into the same pass (`docs/architecture.md:103, 105`; `start_idempotency.rs:42-44`).
  - About ten metric samplers per worker are also created with `self.config.poll_interval` (`worker.rs:28162-28708`, for example `spawn_queue_depth_sampler`, `spawn_dlq_depth_sampler`, `spawn_stranded_work_sampler`). Whether each one issues DB queries was not verified.
  - I found no advisory-lock or leader election in `timeout.rs` (grep for `advisory|leader|elect`).
- Loop 3 evidence:
  - Pool acquisition is unbounded on the single-shard path (`worker.rs:5920-5921, 6010`).
  - Hot-path SQL has no `statement_timeout` (see Q4).
  - The heartbeat flusher competes for the same worker pool every 1 s (`heartbeat.rs:53, 73-86`) and only logs a warning on failure (`heartbeat.rs:87-93`).
  - A missed heartbeat flush lets `COALESCE(last_heartbeat_at, started_at) + heartbeat_timeout < NOW()` fire (`timeout.rs:86-91`), which terminally times out the activity (`timeout.rs:1452-1466`).
  - The slot tuner only *shrinks* on pool saturation if it is enabled; it is off by default (`slot_tuner.rs:183-185`).
- Amplifiers:
  - Default retries are unjittered (`policy.rs:35-42, 162`).
  - Each enqueue sends `pg_notify` (`autumn-harvest/src/notify.rs:181, 209`), and progress chunks run "serial `SELECT pg_notify(...)` in the persist transaction" (`worker.rs:9135-9136`). In Postgres, committing a transaction that issued NOTIFY takes a global lock that "effectively serialized every COMMIT" at high writer concurrency. That was still present in stable releases as of May 2026 per Recall.ai's update. — [Recall.ai, Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale); a counterpoint is [DBOS, Postgres LISTEN/NOTIFY Actually Scales](https://www.dbos.dev/blog/postgres-listen-notify-scalability) (not read in full).
  - Every notified worker sleeps a fixed 50 ms and then claims (`worker.rs:29633`), which synchronises claim bursts.
- Mitigations already present:
  - Default `max_attempts = 3` caps per-activity amplification at 3x ([Huang et al.](https://www.usenix.org/system/files/osdi22-huang-lexiang.pdf): capped retries keep a stable region).
  - The circuit breaker excludes schedule-to-start and PENDING timeouts (`timeout.rs:1476-1486`).
  - Multi-shard pool waits are bounded (`worker.rs:5962-5979`).
  - Continuations are not blocked by the admission gate (`admission_gate.rs:34-39`).

### Inferences
- The most dangerous loop is Loop 1. It turns a transient capacity dip (a DB brownout or a slow downstream) into terminal activity timeouts and non-retryable CircuitOpen failures. Recovery then depends on workflow code or operator redrive. The redrive bulk-re-enqueues up to 1000 tasks at once (`dlq.rs:24-26`), which can re-trigger the loop.
- Loop 2 works against the usual response to a backlog, which is to scale out workers. Each new worker adds roughly 2 full timeout scans per second per shard plus samplers. That is capacity-degradation amplification in Huang et al.'s terms.

### Gaps
- None of these loops has been reproduced. Confirming them needs a DB-backed load test, which the brief did not allow.
- I did not measure whether the timeout scan queries are index-backed. That belongs to the data-layer researcher.

---

## Q6. Gap ratings and remedies

### Takeaway
There are two Critical gaps: over-claim with claim-time timeout clocks, and unbounded DB waits (no pool acquire timeout or `statement_timeout`) on the default single-shard path. There are four High gaps: jitter off by default, no retry budget, no automatic load-driven shedding or backpressure, and O(workers) unjittered background scans. The rest are Medium or Low.

### Cited Findings (ratings with evidence)
| # | Gap | Rating | Evidence | Remedy |
|---|---|---|---|---|
| G1 | The Postgres poll path claims past local capacity, and `started_at` and timeouts run from claim | **Critical** | `worker.rs:30211-30220, 29592-29625`; `queue.rs:1113`; `timeout.rs:86-105` | Gate `poll_once` on `free_permits > 0` per kind, reusing the dispatch path's `dispatch_kind_admitted` and `DispatchReservation` (`worker.rs:26520-26568`). Alternatively, stamp `started_at` when the permit is acquired. Add a test where claims exceed `max_concurrent_*` under backlog. |
| G2 | No deadpool `Timeouts` on the single-shard path; no `statement_timeout` or `lock_timeout` on claim, persist or scan paths | **Critical** | `worker.rs:5920-5921, 5956-5960, 6010`; `timeout.rs:1390-1393` | Configure pool `wait` and `create` timeouts. Set `SET LOCAL statement_timeout` and `lock_timeout` per transaction class. Treat a timeout as retryable with jittered backoff. |
| G3 | Retry jitter defaults to `None`; engine-internal backoffs are unjittered; the no-policy fallback is a fixed 1 s | **High** | `policy.rs:35-42, 162, 186`; `worker.rs:5091-5100, 5612-5621, 7469-7476` | Default to `JitterPolicy::Full` or `Decorrelated`; the deterministic seed already exists (`worker.rs:5024-5042`). Apply the same jitter to ND-block, panic and cooldown backoffs. |
| G4 | No retry budget or token-bucket retry throttle | **High** | No matches for budget code; only the opt-in breaker (`circuit_breaker.rs`) | Add a per-activity-type or per-`rate_limit_key` retry token bucket: retries allowed only while they stay under X% of first attempts (SRE 10%). Once exhausted, retries pace at a fixed rate rather than failing. |
| G5 | No automatic load-driven shedding or backpressure to producers; the admission gate is manual | **High** | `admission_gate.rs:1-7`; `status_summary.rs:301-310` | Add an opt-in automatic gate driven by oldest-PENDING age or schedule-to-start p99 per queue. Return 429 with `Retry-After` on start. Keep continuations exempt (start()/end() rule). |
| G6 | Timeout checker and ~10 samplers run in every worker, per shard, every 500 ms, unjittered and without leader election; scans are unbounded | **High** | `worker.rs:81, 28162-28708, 28302-28308`; `timeout.rs:100-105, 488-491, 5186-5189` | Elect one scanner per shard (advisory lock or lease). Decouple the scan interval from `poll_interval`, with a 1–5 s default and jitter. Add `LIMIT`s and keyset batching. |
| G7 | Activity timeouts are terminal (no retry), unlike the docs and Temporal | **Medium** | `timeout.rs:1452-1466` vs `docs/autumn-workflow-architecture.md:1127` | Decide and document the semantics. If retried, route through `next_retry_delay`. Fix whichever of the doc or the code is wrong. |
| G8 | Circuit breaker trips on RUNNING timeouts that may be local queueing; it fails non-retryably; state is per process | **Medium** | `timeout.rs:1470-1494`; `circuit_breaker.rs:12-15, 30-36` | Feed the breaker only after the handler has actually started (`ActivityStarted`). Offer a "defer or reschedule after cooldown" mode instead of a non-retryable failure. Consider breaker state shared across the fleet. |
| G9 | No default activity `start_to_close` or heartbeat timeout | **Medium** | `builder.rs:3938`; `policy.rs:1437-1440`; `info.rs:1371-1372` | Ship a conservative builder default (for example 10 min), or require one at registration. Warn at startup when an activity has none. |
| G10 | Heartbeat flush shares the worker pool with an unbounded wait and fails silently | **Medium** | `heartbeat.rs:53, 73-93` | Reserve pool headroom for heartbeats, or give them a bounded acquire. Emit a metric on flush failure. |
| G11 | Per-enqueue `pg_notify` inside commit transactions (global commit lock); fixed 50 ms post-notify sleep | **Medium** | `notify.rs:181, 209`; `worker.rs:9135-9136, 29633` | Coalesce notifies, or send them after commit on a separate connection. Jitter the post-notify delay. Document a NOTIFY-off mode (polling or Redis dispatch). |
| G12 | No deadline propagation: work claimed or started after its parent run's deadline | **Medium** | No check found. `schedule_to_close` exists per activity (`docs/shipped-work.md:44`), but no run-deadline check at claim | At claim, skip or cancel tasks whose execution deadline has passed. |
| G13 | No continuation-over-new-start priority; queue weights and ageing are off by default | **Medium** | `queue.rs:1093-1099`; `worker.rs:30129-30133` | Give workflow tasks of existing runs and activity-result wakes a higher default priority than first tasks of new runs. |
| G14 | Slot tuner is opt-in, pool-pressure-only, and grows on local permit wait | **Low–Medium** | `slot_tuner.rs:181-197`; `docs/architecture.md:119` | Add a latency- or error-based limiter (Gradient2 or AIMD) keyed per activity type. Stop growing when the downstream error rate rises. |
| G15 | Completion-callback retries treat 4xx the same as 5xx and ignore the receiver's `Retry-After` | **Low** | `completion_callback.rs:1566-1588` | Dead-letter on non-429 4xx. Honour `Retry-After`, clamped. |
| G16 | DLQ bulk redrive re-enqueues up to 1000 tasks at once | **Low** | `dlq.rs:24-26` | Spread redriven tasks' `scheduled_at` over a window. |
| G17 | CLI and dev `reqwest` clients have no timeout | **Low** | `autumn-harvest-cli/src/lib.rs:7630, 7700`; `autumn-harvest-cli/src/tui.rs:49`; `autumn-harvest-plugin/src/dev/mod.rs:563` | Use `Client::builder().timeout(..)`. |
| G18 | Schedule fire jitter defaults to zero | **Low** | `policy.rs:878-895, 1037` | Default a small deterministic jitter for cron schedules. |

Strengths to credit in the report:
- Capped default attempts (`policy.rs:248-251`).
- Deterministic jitter infrastructure (`worker.rs:5024-5042`).
- `Retry-After` honouring with a ceiling (`builder.rs:67`).
- Stable activity idempotency keys (`types.rs:1141-1149`) and transactional start idempotency (`start_idempotency.rs:28-36`).
- Bounded channels throughout (no `unbounded_channel` found).
- A rich set of opt-in bulkheads: throttle, quota, concurrency, per-activity `max_concurrent`, rate limits, pauses, sessions, and separate web and worker pools.
- An admission gate that exempts continuations.
- Poison-pill quarantine that does not charge crash strikes for stuck-but-healthy tasks (`poison_pill.rs:352-356`).

### Inferences
- G1 and G2 compound. G1 fills local queues, G2 lets DB waits stretch without limit, and together they make timeouts fire on work that never ran. Fixing G1 alone removes most of the Loop 1 risk.
- G3, G4 and G5 are cheap relative to their value, because the building blocks already exist (seeded jitter, token-bucket tables in `harvest_rate_limit_buckets`, and the admission gate cache).

### Gaps
- The ratings come from static reading only. No load test or reproduction was run.
- Temporal's current worker-side slot tuner and server-side rate-limiter defaults were not reviewed as a comparison baseline.
