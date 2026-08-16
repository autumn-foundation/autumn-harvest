## Phase 3.x — Capability-miss release for a capable peer: stop failing tasks a worker simply hasn't been taught yet (issue #804)

**Implemented.** A worker that claimed a task whose handler it does not register used to **fail the execution terminally**. Mid-rolling-deploy that is a self-inflicted outage: the old pods are perfectly healthy, they simply have not been given the new handler yet, and `SKIP LOCKED` hands them the new build's work anyway. Now such a claim is **released back to `PENDING`** for a capable peer — with backoff, bounded by a redelivery budget, and observable — so a mixed fleet drains normally instead of burning executions.

**Why claim-time filtering is not the fix (and cannot be).** The obvious alternative — teach `claim_task` to skip tasks a worker can't run — is structurally impossible: a worker can enumerate the handlers it *has* registered, never the ones it has *not*. The existing `ineligible_activities` claim parameter is an **exclusion** list of registered-but-label-mismatched activities and cannot express "not registered". That confirmed release-then-escalate as the only in-scope design, rather than merely the convenient one.

**The mechanism, in one interception point.** Four sites previously raised an untyped `HarvestError::Config` on a handler-lookup miss (`process_workflow_task`, `process_activity_task`, the local-activity inline dispatch, and `persist_scheduled_activities`); two of them additionally called `fail_task_and_execution` on the spot. All four now raise the new typed `HarvestError::HandlerNotRegistered { kind, name }` and fail nothing themselves. `fail_execution_on_error` — the choke point that converts any persist-path `Err` into a terminal failure — passes that one variant through **un-failed**, precisely so it lands in a single new interception point in `process_task`, which owns the connection, the registry, the task and the worker id. `error.handler_not_registered()` is a deliberately narrow classifier (it matches exactly the one variant, pinned by a test that includes the pre-#804 `Config` variant to prove a broad match would be wrong). Only three child-workflow spawn sites were left untouched, because they resolve defaults with `map_or` and cannot fail — a missing child handler funnels through the workflow-task site when a worker claims the **child's own** task.

**Release semantics — the details that make it safe.** `queue::release_task_for_capability_miss` is an **ownership-guarded** `UPDATE` (`WHERE id = $1 AND state = 'RUNNING' AND worker_id = $2`) returning whether it matched, so a concurrent poison-pill reclaim or operator action that already took the row makes the release a counted-nothing no-op. Beyond the obvious claim-column reset it does four non-obvious things, each load-bearing:

- **`attempt = GREATEST(attempt - 1, 0)`** — `claim_task` increments `attempt` on every claim, so a release that did not undo it would silently drain the task's retry budget. This is the exact bug issue #369's `defer_rate_limited_task` had to fix, and the reason releases are not just "set state = PENDING".
- **`crash_strikes` never incremented; cleared only when the handler ran** — see AC4 below.
- **`sticky_worker_id`/`sticky_until`/`sticky_timeout` cleared** — sticky routing (#235) would otherwise give the *incapable* worker first refusal on the row it just released.
- **`error` is left entirely untouched.** A capability miss is not a task failure, so the column reserved for real failure reporting must not carry an infrastructure diagnostic. Two surfaces read it and would both be misled: `ActivityContext::previous_failure()`, which an author branches on, and — more sharply — the issue #773 `/stack` endpoint, which renders any non-null `error` on a pending activity as a `last_failure`. Because the release *also* restores `attempt` to `0`, a breadcrumb there would report a failure at an otherwise-unreachable `attempt: 0` for an activity that never executed, violating #773 AC3 ("a never-failed pending activity omits `last_failure`") and making a blameless deploy skew look like an application bug on the one surface whose runbook question is "why is this activity retrying?". The diagnostic reaches the operator through the release's `tracing::info!` and the new counter instead.
- **`activity_name` cleared on workflow rows only** (`CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END`) — a workflow row can carry the `mixed_signal_suspension` sentinel there; left stale, an unrelated wake would reset `scheduled_at` and bypass the backoff entirely. On an activity row `activity_name` is the identity of the work and must be preserved.

**Bounded by a durable counter, not an in-process one.** Each release increments a new `capability_misses INT NOT NULL DEFAULT 0` column on `harvest_task_queue` (migration `20260720000000_harvest_task_capability_misses`, additive). Storing it in-process was rejected because the task deliberately moves *between* workers, so the counter must travel with the row. Reusing `attempt` (the retry budget) or `crash_strikes` (the poison-pill counter) was rejected on AC4 grounds — see below.

The counter measures **consecutive** misses, so every path that *proves* the claiming worker was capable must reset it, or a long-lived row accumulates one miss per deploy and eventually escalates while a capable worker is demonstrably live — the exact outage #804 exists to prevent, inverted. An audit of every `RUNNING → PENDING` transition on `harvest_task_queue` settles which paths those are, and the discriminator is strict: **reset only where the path is proof the claiming worker was capable.** Resetting: `PendingRequeueChangeset` (activity retry — the handler ran), the new shared `CleanContinuationChangeset`, and **both** `park_workflow_task` queries (the dominant workflow suspension path — a park is reached after the handler was resolved and a decision cycle ran; ~14 call sites across activity, signal, child-workflow and mutex waits).

Four transitions deliberately do **not** reset, and two of those are non-obvious enough to be worth stating:

- `primary_repend_workflow_task_query` matches only rows already parked by `park_workflow_task`, so park is the single upstream choke point.
- `release_claim_if_queue_paused` checks the pause *before* the handler lookup and therefore proves nothing either way.
- The poison-pill orphan reclaim (#367) fires on a worker that *crashed*, which likewise proves nothing about whether it had the handler.
- **`reset_timed_out_workflow_task` (#494) does not reset**, despite the tempting intuition that a workflow-task timeout means the handler was found and ran long. The #494 budget is armed around the whole of `process_task`, and `pool.get()` plus the full history load both sit inside it — strictly *before* the registry lookup that defines a capability miss. Under pool starvation or a slow shard the timeout fires with the lookup never having run, so the path is not proof. Resetting on that false premise is the worse of the two errors: a genuinely unregistered type under load would have its streak zeroed indefinitely and **never escalate**, so the run never reaches the `no_capable_worker:` terminal AC3 promises and the operator sees only the ticket-severity sustained-release rule instead of the page. The accepted cost runs the other way — a capable-but-slow worker's timeout leaves a stale streak, so a later genuine miss can escalate before spending the full budget — and that direction is fail-safe, producing a loud actionable failure rather than a silent one.

One further asymmetry falls out of the same rule. `park_workflow_task` zeroes the counter in **shared** SQL, and one of its callers is *not* post-lookup: `requeue_parent_on_transient_ingest_conflict` (#779) parks from inside the wake-event ingest, which runs before the handler lookup, so an incapable worker reaches it too. That caller uses `park_workflow_task_preserving_capability_misses`, which is the same statement **minus** the two SET assignments. Preserving in place rather than zeroing-then-restoring is load-bearing: the park commits in autocommit mode (`process_workflow_task` opens its transaction much later), so between a zeroing park and any follow-up restore a peer can claim the freshly-parked row and record its own miss — or a capable peer can park and legitimately reset it — and no single-statement merge of two whole snapshots is monotone against both of those. Omitting the assignments closes the window by construction: the row is never observably zeroed at all. The two counters stay genuinely independent in both directions — the orphan reclaim bumps `crash_strikes` and leaves `capability_misses` alone; the release bumps `capability_misses` and never bumps `crash_strikes` (clearing it only for a post-handler miss — see review round 12).

**Escalation reuses the existing terminal path.** At the bound, `handle_capability_miss` calls the ordinary `fail_task_and_execution` — appending the ordinary `WorkflowFailed` and sealing the execution `FAILED` exactly as any other terminal failure does. That path writes **no dead-letter row** (it is `persist_workflow_failure`, not the DLQ), which the runbook states explicitly so nobody triaging a page goes looking in `GET /dead-letters` and reads the emptiness as evidence the alert was spurious. Nothing about the terminal contract changes; only its *reason string* and the delay before reaching it.

The reason carries the stable, greppable prefix `no_capable_worker:` on **all** escalations — the runbook's recovery sweep matches it over `GET /workflows?state=FAILED` and the recovery action is identical — but the parenthetical after it comes in **four** variants, because only one supports the unqualified fleet-wide conclusion:

| Cause | Reason says | Operator fix |
| --- | --- | --- |
| Distinct-worker budget exhausted | `escalated after R capability-miss redeliveries across D distinct worker(s); capability_miss_max_redeliveries = N; … no live worker on this queue has the handler` | Deploy the handler / finish the rollout |
| Absolute release ceiling | `escalated after R capability-miss redeliveries spread across only D distinct worker(s), hitting the absolute release ceiling of C releases (10x capability_miss_max_redeliveries (N)); … either those workers lack the handler or a capable peer lost every claim race` | Check those `D` workers first, then the fleet |
| Zero budget (`capability_miss_max_redeliveries = 0`) | `escalated immediately after 0 redeliveries: capability-miss redelivery is disabled …; a capable worker may exist on this queue` | Raise the knob off `0` |
| Session-pinned (#606) | `escalated immediately after 0 redeliveries: task is pinned to worker session {id} …` | Go to the pinned host, not the fleet |

In the last two the task was released **zero** times, so naming `max_redeliveries` would state a release count that never happened, and the fleet-wide clause may be outright false — a capable peer can exist and simply be ineligible (session pin) or never have been asked (zero budget). That is not cosmetic: the runbook's first triage step is `GET /admin/workflow-types/reachability`, which in both cases can report `in_use` and flatly contradict the reason, and the "False positives" advice to *raise the budget* is a guaranteed no-op for a pinned task. The ceiling variant is the same class of misreport in a subtler form: it would otherwise claim `N` redeliveries when the real count is up to `10 ×` higher, and assert a queue-wide sweep that provably did not happen. The cause is resolved once, from inputs already at the call site, through a named `EscalationCause` enum so the match is exhaustive and a future fifth branch cannot silently inherit another's wording — which is exactly how the ceiling branch came to claim the fleet conclusion when it was added, and exactly what the enum then caught. The `tracing::error!` message branches with it rather than merely carrying a `session_pinned` field beside a message that contradicts it, and carries `distinct_incapable_workers` alongside the release count.

**…and the metric splits where the conclusion splits.** A cause-specific reason string is not enough on its own, because the *paging signal* is the counter, not the error column. `EscalationCause::outcome_label()` maps the two *offered-around-the-queue* causes (distinct-worker exhaustion and the release ceiling) to `outcome="escalated"` and both zero-offer causes to a distinct `outcome="escalated_never_offered"`, and the escalate arm derives the reason, the metric label and the log line from **one** resolution so the three can never tell an operator different stories about the same escalation. The split is by operator *conclusion*, not by cause count: both of the first two actually bounced the task around the queue with backoff between releases; the other two are evidence about one config knob or one task's pin, where a capable worker may be live and idle the entire time. The ceiling pages despite its weaker evidence because it is the **only** bound a fleet smaller than the budget can trip — with one worker the distinct set can never exceed a budget of 5 — so ticketing it would mean a single-worker deployment never pages for a genuinely missing handler, under-paging the exact outage #804 exists to surface. Its reason string states both readings so triage is not misled either way. They share one value because they share that conclusion and that recovery — the reason string is what tells them apart during triage. Without the split, `harvest_no_capable_worker` (severity **page**, whose entire narrative is "no live worker on this queue registers the handler") fires for a task failed on its first claim, and its own `first_action` then sends on-call to `reachability`, which answers `in_use` and contradicts the page they are holding.

**Backoff is capped-exponential** (`policy::compute_retry_delay`: 1s, 2s, 4s, 8s, 16s, then a 30s cap) and exists for a specific reason: without it the same incapable worker re-claims its own just-released row in milliseconds and spends the whole budget before a peer ever sees the task. A budget of `N` grants exactly `N` releases and escalates on the `N + 1`th claim, so the default's five backoffs sum to ~31s of dwell on a single worker (less in wall-clock terms on a wide fleet, where incapable peers consume releases in parallel) — pinned by a test that DERIVES the release count from the shipped default rather than hard-coding it.

**The budget is consumed per DISTINCT worker, not per release.** The backoff makes a released task eligible to *every* worker again — including the one that just released it — and the claim query has no capability filter. Under plain total-miss accounting, `budget + 1` consecutive claim-race wins by a single incapable pod terminally failed the run while a capable peer sat live and idle: the exact rolling-deploy outage #804 exists to prevent, reproduced by the feature's own bound. The task therefore records the **set** of workers that have missed it (`capability_miss_workers TEXT[]`, appended idempotently in the release UPDATE), and a repeat miss by a worker already in that set backs off but consumes **no** budget. Escalation at `distinct > budget` now means `N + 1` *different* workers each failed to resolve the handler, which is a far stronger statement about the fleet than `N + 1` bounces ever was.

A secondary absolute ceiling of `10 ×` the budget on *total* releases covers the one case a distinct-worker budget structurally cannot: a fleet **smaller** than the budget, where the distinct set can never grow far enough — a lone incapable worker would otherwise bounce a task forever, violating AC3's "release is bounded". It is deliberately generous, because reaching it while a capable peer is live requires that peer to lose the claim race that many times consecutively; the sustained-release ticket alert fires at 15 minutes, long before it. It gets its **own** `EscalationCause` variant so it reports the counts it actually observed (`R` releases across `D` distinct workers) rather than inheriting the distinct-worker branch's wording, which would have claimed a queue-wide sweep that provably did not happen.

**Claim-time exclusion — filtering the releasing worker out of the claim query — was considered and rejected.** It would guarantee a capable peer sees the row, but in a single-worker fleet it makes the task permanently unclaimable, and a stall is strictly worse than a bounded failure. Distinct-worker *accounting* buys the same protection against the reported failure mode without that hazard: every worker stays eligible for every claim, and the set only changes what a miss **costs**. The residual is honest and documented — with a large heterogeneous fleet, `budget + 1` genuinely distinct incapable workers can still escalate while capable peers exist. Closing that last gap needs authoritative per-worker handler knowledge, which is issue #700's fleet-wide unregistered-type detection and is explicitly out of scope here.

**Session-pinned tasks (#606) escalate immediately, regardless of budget.** A session task is hard-pinned to its acquiring host by the claim gate `session_id IS NULL OR sticky_worker_id = $1`. Releasing it "for a capable peer" is false by construction — no other worker can ever claim it — and clearing its sticky pin would strand it forever. `capability_miss_releasable(session_id)` makes this a first-class, unit-tested carve-out in the pure decision function rather than an emergent property.

**AC4 holds by construction, not by convention.** A capability miss is distinguishable from a poison-pill crash (#367) and a hung body (#494) across four independent layers: (1) the release never increments `crash_strikes`, and clears it only when the miss was detected after the body ran to a conclusion (round 12); (2) `reclaim_orphaned_tasks` — the *only* writer of `crash_strikes` — scans `state = 'RUNNING' AND worker_id IS NOT NULL` with a stale heartbeat, and a released row is `PENDING` with `worker_id = NULL`, so it is invisible to that scanner; (3) the #494 workflow-task-timeout strike map is keyed on wall-clock timeout and a released task never ran a handler at all — **and the independence runs both ways**: `process_task` returns a `TaskDispatchOutcome` tri-state so the dispatch site can tell a genuine completion from a capability-miss release, and only the former clears that strike map. Collapsing them (a release returning a plain `Ok(())`) would let an incapable worker *erase* a genuinely hung execution's consecutive-timeout streak every time it happened to claim the row, so `poison_pill_threshold` would never be reached in a mixed fleet and #494's protection would be silently defeated by a blameless third party. That is the mirror image of the argument for why a miss must never *increment* `crash_strikes`, and the original audit only checked one direction; (4) escalation routes through the ordinary terminal-failure path (`WorkflowFailed` + a `FAILED` execution row) and writes **no** dead-letter row at all, so a `PoisonPill` quarantine entry can never be confused with one. The `harvest.task.quarantined` metric and the `harvest_no_capable_worker` alert are therefore mutually exclusive diagnoses.

**Observability (AC5).** New counter `harvest.task.capability_miss{queue, task_type, outcome}` via the standard three-touchpoint recipe (constant + no-op default trait method + `metrics_rs_adapter` bridge). The `outcome` dimension (`released` | `escalated` | `escalated_never_offered`) is a deliberate bounded **superset** of the `{queue, task_type}` set named in the issue: AC5 also requires operators to "alert on the escalation", which is only expressible if the benign release and the executions-are-failing escalation are separable on the same counter. This matches the existing repo idiom (`harvest.schedule.fire_attempts{outcome}`, `harvest.workflow.terminal{outcome}`). On the escalation branch the counter is recorded **before** the terminal write, deliberately: `fail_task_and_execution` can fail transiently, and that is precisely the case that most needs the page — a failed terminal write leaves the row `RUNNING` under a *live* worker, where the poison-pill orphan reclaimer (which requires a dead heartbeat) cannot see it. Ordering the metric after the `?` would silence the paging rule in exactly the stranding case. The counter therefore reports the escalation *decision*, which is final by the time it is emitted; a stranded row is re-claimed and escalates again, so it is at-least-once rather than exactly-once — the right trade for a paging signal. The workflow/activity **name** is deliberately *not* a label — the `queue` + `task_type` pair localizes a deploy skew, and per ADR-0001 §7 `execution.id` stays span-only.

The three outcomes ship as **three separate starter-pack rules**, because a pack rule carries a single `severity` and therefore fires every one of its expressions at that severity. `harvest_no_capable_worker` (severity `page`) selects `outcome="escalated"` **only** — genuine fleet exhaustion, executions being failed. `harvest_capability_miss_never_offered` (severity `ticket`) owns `escalated_never_offered`: executions *are* failing, so it must not be silent, but the cause is one config knob or one task's pin rather than a fleet-wide capability gap — and one of the two is a switch an operator deliberately flipped, so paging on its consequence would be self-inflicted noise. Both selectors are exact matchers and a pin test forbids `=~` on either, since an `outcome=~"escalated.*"` regex would silently re-conflate two causes that carry opposite conclusions.

`harvest_capability_miss_release_sustained` (severity `ticket`) owns the released half, and because the pack schema has no `for:` field its hold lives entirely in the expression — which needs **both** halves of `min_over_time((… > bool 0)[15m:1m]) == 1 and count_over_time((… > bool 0)[15m:1m]) >= 15`. The `min_over_time` half asserts the released rate was non-zero at every 1m step; the `count_over_time` half asserts the window is actually *full*, and is not redundant: Prometheus range functions **skip** subquery steps that have no sample rather than reading them as zero. On a series created by this very deploy — the normal case, since a new `(queue, task_type)` capability-miss series appears the first time a new handler rolls out — `min_over_time` would otherwise be taken over only the handful of samples that exist and could be satisfied within ~3 minutes (a 5m `rate` stays positive for 5m after the last release), firing on exactly the routine deploy this rule was split out to ignore. An `increase(…[15m]) > 0` form is worse still: it fires on a single release anywhere in the window. A textual pin test asserts every one of these halves so a future "fold these back together" or "simplify the subquery" pass cannot reintroduce a rule that fires on every deploy. Both rules ship with a runbook section (five subsections each), the shared dashboard panel, `docs/dashboards/README.md` alert↔panel mapping rows, and full `docs/telemetry.md` + ADR-0001 §7 catalogue rows — all required by the anti-drift `dashboard_pack_docs`/`alert_pack_docs` guards, which hard-fail until every ground truth exists.

**Storage-ceiling guard (defensive hardening, not a production bug).** `capability_miss_decision` escalates when the value about to be persisted is `i32::MAX`, because the release SQL's `capability_misses + 1` would then raise `integer out of range` in Postgres, abort the statement, and strand the task `RUNNING` under a live worker that no reclaimer can see. The invariant is stated on the **persisted value** rather than on the budget — "never let the column reach `i32::MAX`", so `+ 1` stays representable however the counter got there — which covers a hand-written column value, not just a nonsensical config. Its preconditions are explicitly non-organic: it needs both a budget `>= i32::MAX` (the default is 5, which already escalates correctly) **and** a counter already at the ceiling, against a column that resets to 0 on every successful claim; organically that is ~2,041 years at the 30s backoff cap.

**Configuration.** `WorkerConfig::capability_miss_max_redeliveries` (default **5**) with `with_capability_miss_max_redeliveries(n)`; `0` escalates on the first miss, i.e. opts back into the pre-#804 fail-fast behaviour. Threaded through `WorkerRuntimeConfig` and surfaced on `WorkerConfigView` — the #695 exhaustive-destructure guard made that a compile error until it was, which is exactly its job.

**AC7 — no event-schema cost. No new `WorkflowEvent` variant, no change to the adjacently-tagged event JSON contract, and a release appends *nothing* to `harvest_events`.** Release and redelivery are task-queue state, not event-log state; replay determinism is untouched. One additive migration, one additive column.

**Docs (AC6).** New "Worker-fleet handler contract" section in `docs/runbooks/safe-deploy.md` — the runbook the issue names explicitly — stating the contract ("all workers polling a queue should register the same handler set"), what a rolling deploy does to it, the released-not-failed behaviour, the bound, a signal→action table for the two outcomes, budget sizing guidance, and explicit contrasts with build-id routing (#171), the handler-coverage gate (#520/#700), queue coverage (#774), poison pill (#367) and workflow-task timeout (#494). Plus a full five-subsection `harvest_no_capable_worker` triage section in `docs/runbooks/harvest-alerts.md`.

**Tests, TDD red→green.** 40 pure unit tests (decision truth table incl. zero-budget and saturating counts, backoff growth/cap/saturation, the pod-flip dwell bound, bounded kind labels, session carve-out, reason-string shape, the narrow error classifier, the metric constants and adapter bridge with exact label order, the `WorkerConfig` threading) plus 11 no-DB SQL-shape tests asserting the release query is ownership-guarded, restores `attempt`, never touches `crash_strikes`, never writes `error`, increments the counter idempotently per worker, unpins sticky, clears the claim columns, and clears the sentinel on workflow rows only — and that the pre-lookup park variant never writes the miss columns at all. Then a 9-test DB suite (`tests/integration/capability_miss_tests.rs`, registered in `.github/ci/integration-suites.txt`) driving the **real worker poll loop** against a real Postgres 16: AC1 workflow release-then-capable-peer-completes (asserting the execution stays `RUNNING`, history is *exactly* `[WorkflowStarted]`, `crash_strikes == 0`, `attempt` restored to 0); AC2 the same for an activity task using a realistic split-queue fleet; AC3 bounded escalation with the `no_capable_worker:` reason and the ordinary `WorkflowFailed`; the session-pin immediate escalation; the ownership-guard no-op; the issue's **success metric measured** — a mixed fleet (one incapable worker, one capable) drives 8 executions to `COMPLETED` with **zero** escalations; the two consecutive-miss reset proofs, one per direction: a capable worker that *parks* on a signal wait clears a seeded streak of 3, while `reset_timed_out_workflow_task` **preserves** a seeded streak of 4 (the not-proof-of-capability case above) and leaves the execution `RUNNING`; and a two-part proof that the release never writes `error` — a fresh row stays never-failed at `attempt 0`, and a genuine prior failure survives untouched for `previous_failure()`. Both are driven against real entry points rather than SQL text, so they cannot drift from the statements they pin. Every test is *phased* (the incapable worker runs alone until the release is observed, then is shut down) and uses its **own queue name**, so neither the precondition nor the isolation is a race. **RED was confirmed** by neutering `handler_not_registered()` to return `None` and re-running: 5 of the 6 fail (the 6th targets the queue-layer SQL directly and is correctly unaffected).

---

## Review round 6

**P2 — validate handler capability *before* the terminal telemetry.** The three capability checks that run inside the persistence transaction (`persist_scheduled_activities`, `persist_all_started_child_workflows`, `create_detached_child_executions`) fire *after* `process_workflow_task` has already recorded `record_workflow_completed` and `harvest.workflow.terminal{outcome="completed"}` (#519). Returning the new typed error rolls the transaction back and releases the task, so the execution stays `RUNNING` — but the terminal counter had already been incremented, and because a capability miss is *repeatable* (every redelivery re-runs the same decision) one logical run was counted once per redelivery, corrupting the success-rate SLO the counter exists to serve.

The fix is a pre-pass, `first_persist_capability_miss`, run ahead of the telemetry block — and ahead of the history-cap check's DB round-trips, so an incapable worker releases without paying for work a capable peer will redo. The in-transaction checks stay as the authoritative all-or-nothing guard. The pre-pass matches over `WorkflowCommand` **exhaustively, with no wildcard arm**, so adding a variant is a compile error until someone decides whether persisting it resolves a handler — the same coverage-guard pattern as `WorkerConfigView::from_worker_config` (#695) and `ShardRouter::parts` (#697). `RunLocalActivity` is deliberately absent: it resolves during *execution* (already ahead of the telemetry) and that site is replay-aware, so flagging it here would release a task this worker could have replayed perfectly well.

**P1 — one incapable worker could exhaust the shared budget.** Fixed by the distinct-worker accounting described above. The migration gains one additive column; the release UPDATE appends the claiming worker idempotently (`WHEN $2 = ANY(...) THEN ... ELSE array_append(...)`), binding the *same* `$2` the ownership guard checks so the set can never record a worker that never held the claim. Every path that resets `capability_misses` also clears the set, so the two can never disagree about whether the streak was broken, and the #779 pre-lookup park preserves both in place (see round 7).

**Tests.** Four new pure tests for the pre-pass (each handler-bearing command is caught, a fully-registered batch is silent, local activities are skipped, the first miss in command order wins) and two for the distinct budget (a repeat miss by the same worker never consumes budget across 39 bounces; the total ceiling terminates a lone-worker bounce), with the pre-existing bound test rewritten so its budget is spent by `N` *distinct* workers and the `N + 1`th escalates. Three SQL-shape tests pin the append-once release, the park-time set clear, and the preserve-park variant. The DB suite gains `one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget` — the falsifiable P1 proof: one incapable worker, budget 2, bounced four times, asserting the distinct set holds exactly one entry and the execution is still `RUNNING` (under the old accounting it reaches `FAILED` on the third claim). The AC3 escalation test now seeds a budget's worth of prior distinct workers and runs a fresh worker as the `N + 1`th, which is what an all-incapable *fleet* actually looks like under the new accounting.

## Review round 7

**P2 — the #779 pre-lookup park undo was not race-free.** Round 6 kept the shared park SQL zeroing the capability-miss accounting and had `requeue_parent_on_transient_ingest_conflict` restore it in a *second* statement, guarded by `GREATEST` on the counter and a cardinality comparison on the worker set. Both statements run in **autocommit** — `process_workflow_task` opens its transaction much later — so a peer can claim the freshly-parked row in the gap between them. Neither guard is monotone against everything that can happen there: a stale four-worker set overwrites a fresher one-worker set and drops the new misser, and a capable peer's legitimate reset (to `0` / `{}`) is clobbered straight back up. Both directions corrupt the distinct/consecutive accounting and shift escalation.

The fix removes the second statement entirely. `park_workflow_task_preserving_capability_misses` is the same park statement **minus** the two SET assignments, so the row is never observably zeroed and there is no window to lose a race in. The two query `const fn`s take a `reset_capability_misses` flag and return one of two literals, which keeps both shapes unit-testable without a database; every other caller is unchanged and still resets, because every other caller *is* proof of capability. `queue::restore_capability_misses` and its query are deleted. (Deleting them also reattached a doc comment that had been orphaned onto the restore helper away from `release_task_for_capability_miss`.)

**P2 — the release ceiling claimed a conclusion it had not earned.** Round 6's absolute `10 ×` ceiling escalated but still resolved to `EscalationCause::BudgetExhausted`, so it emitted the page-triggering `outcome="escalated"` alongside the reason "escalated after N capability-miss redeliveries; no live worker on this queue has the handler" — two false statements at once. The real release count is up to `10 ×` higher than the `N` it names, and the ceiling fires precisely when *fewer* distinct workers than the budget missed the task, so the queue-wide sweep the sentence asserts provably did not happen; the alternative reading (a capable peer losing the claim races) is exactly what the operator is then told to stop looking for. This is the failure mode the exhaustive `EscalationCause` match was introduced in round 3 to prevent, reproduced by the branch round 6 added.

`EscalationCause` gains a fourth variant, `ReleaseCeilingExhausted { releases, distinct_workers }`, carrying the counts it actually observed; `resolve` now mirrors `capability_miss_decision`'s two bounds in the same order, over a shared `capability_miss_budget` clamp so the decision and the reason can never disagree about which bound tripped. The reason states the real release count, the distinct count, and **both** readings, and the `tracing::error!` gains `distinct_incapable_workers`.

It deliberately keeps `outcome="escalated"` (page). The ceiling is the *only* bound a fleet smaller than the budget can trip — with one worker the distinct set can never exceed a budget of 5 — so routing it to a ticket would mean a single-worker deployment never pages for a genuinely missing handler, under-paging the exact outage #804 exists to surface. The competing reading requires a capable peer to lose `10 × budget` consecutive claim races across ~25 minutes of backoff, which is not a realistic steady state; the reason string carries it anyway so triage is not misled. If the maintainers would rather have a fourth outcome value and a ticket rule for it, that is a one-line `outcome_label()` change plus an alert-pack rule — under-paging was judged the worse error of the two.

**Tests.** Two new pure tests, both RED-verified against the pre-fix code. `park_preserving_capability_misses_never_writes_the_miss_columns` fails when the preserve variant is made to write the columns (the zero-then-restore design); it replaces the now-deleted monotone-restore test. `release_ceiling_escalation_reports_its_real_counts_not_a_fleet_conclusion` asserts the ceiling shape resolves to the new variant, reports `after 51 …` rather than `after 5 …`, names the distinct count, offers the race reading, withholds the fleet conclusion, and still pages — it fails with `left: BudgetExhausted` when `resolve` is reverted to its three-cause form. The four `no_capable_worker_reason` call sites take counts through a `CapabilityMissCounts` struct so the two `i32`s cannot be swapped.

## Review round 8

**P1 — the budget could still fail a run while a capable worker was live.** Round 6 made the budget count *distinct* workers so one incapable pod could not exhaust it alone, but the count still had no relationship to the live fleet. A rolling deploy with `budget + 1` old pods plus one new capable pod can hand `budget + 1` genuinely distinct incapable worker ids to the decision — six old pods against the default budget of five is enough — and the task is terminally failed while the capable pod is live and polling the same queue. The execution's `error` then asserts the exact opposite of the truth ("no live worker on this queue has the handler"), which is the failure mode the whole feature exists to prevent and the half of the success metric that reads *"zero spurious FAILED executions ... as long as ≥ 1 capable worker is live"*.

The missing input was fleet evidence. The task's `capability_miss_workers` array is only ever evidence about the workers that happened to *win a claim race*; it says nothing about the fleet. New `workers::live_workers_on_queue` reads the workers with a fresh heartbeat advertising the task's queue — reusing the poison-pill reclaimer's liveness predicate verbatim (`2 × worker_heartbeat_interval`, now extracted as `worker::worker_stale_secs` so the reclaimer, the broken-session scanner and this cannot drift on the answer) — and the pure `fleet_capability_evidence` compares the two sets into three states:

| Evidence | Meaning | Effect on the budget |
| --- | --- | --- |
| `AllLiveWorkersMissed` | every live worker on the queue has now missed it | budget may escalate; the fleet conclusion is *earned* |
| `CapablePeerMayExist` | a live worker has never missed it | budget is **withheld** — only the ungated ceiling can fire |
| `Unavailable` | the registry does not even list the claiming worker | budget applies as before, and the reason says the fleet was not confirmed |

`Unavailable` is detected self-referentially: the worker holding the claim is by definition live and polling this queue, so a registry that cannot see it is not describing this fleet (heartbeats off, a different queue name advertised, a stale window shorter than the interval). Trusting such a set would let a misconfigured registry suppress escalation forever, so it falls back to the budget rather than to silence — but the reason string says so instead of claiming a conclusion it never established. A registry read that *errors* degrades to the same state with a `tracing::warn!`, rather than propagating and leaving the row `RUNNING` under a live worker where the orphan reclaimer cannot see it.

The absolute `10 ×` ceiling stays **ungated**, which is what keeps AC3's "bounded release" true when coverage is unreachable. Two consequences are documented rather than hidden: the effective bound becomes `max(budget, live fleet size)` redeliveries — you cannot prove "no worker here has the handler" in fewer redeliveries than there are workers to ask — and a live peer that never claims is now terminated by the ceiling, which gets its own wording pointing at that peer (saturated? draining? advertising a stale queue list?) rather than at the deploy.

**P2 — the escalation reason counted a redelivery that never happened.** The escalation branch never runs the release UPDATE, so `resolution.total_after` (and a freshly-appended distinct worker) describe a release that did not occur; the ceiling's reason therefore said "escalated after 51 capability-miss redeliveries" when 50 had been persisted. The reported counts now come from the row (`escalation_from_persisted`, extracted as its own function precisely so the one rule that is easy to get wrong at a call site is testable in isolation), while the *decision* keeps using the post-increment count — this claim genuinely is evidence that the claiming worker lacks the handler. The two bases are named and documented on `CapabilityMissEscalation` (which replaces round 7's `CapabilityMissCounts`) so they cannot be conflated again.

`EscalationCause`'s two page-severity variants gained the flag that changes their wording — `BudgetExhausted { fleet_confirmed }` and `ReleaseCeilingExhausted { .., capable_peer_may_exist }` — and the match stays exhaustive, so the reason string, the `tracing::error!` message and the metric label are still derived from one resolved cause.

**Tests.** Eight new pure tests and one new DB test, the two headline ones RED-verified against the pre-fix code. `budget_never_escalates_while_a_live_worker_has_never_missed` walks the entire six-old-pods-plus-one-capable rollout and asserts every claim releases, then asserts the scenario really did cross the raw distinct bound so the gate is what suppressed it rather than the arithmetic never firing; it fails with `left: Escalate, right: Release` on the sixth claim without the gate. `escalation_report_uses_persisted_counts_not_the_decision_inputs` builds the one shape where the two bases differ by exactly one and fails with `left: 51, right: 50`. Alongside them: the three-way evidence classification, budget-escalates-once-covered, unavailable-registry-falls-back-and-says-so, the ceiling's live-peer wording, the `worker_stale_secs` truth table, and a SQL-shape test pinning that the fleet query reuses the shared liveness predicate, scopes to the queue, and does *not* filter on `status` (a draining worker is still live enough to be a capable peer). The DB suite gains `budget_never_escalates_while_a_capable_worker_is_live`: a real worker, a budget of 1 already spent by one distinct pod, a capable peer registered in `harvest_workers`, driven two releases past the point the ungated budget would have failed the run, asserting the execution is still `RUNNING` with zero escalations.

**Refactor (green-phase).** Adding the fleet lookup pushed `handle_capability_miss` past two of the workspace's own clippy gates (8 arguments, 128 lines), so the function was split rather than exempted. Two argument bundles replace loose pairs that were only ever meaningful together: `MissingHandler { kind, name }` — the pair `HarvestError::handler_not_registered` returns, which every diagnostic on the miss path needs both halves of — and `CapabilityMissPolicy { max_redeliveries, worker_stale_secs }`, the two knobs that govern the path (one decides *how many* redeliveries are allowed, the other decides *whether the fleet may withhold that bound at all*). The escalation arm, which runs at most once per task, moved into `escalate_capability_miss` so the release arm that runs on every ordinary redelivery stays readable; the degrading registry read moved into `read_live_fleet_or_degrade`, where the reason it returns an empty set instead of propagating is documented once. Behaviour is unchanged — the same suite passes before and after.

**Doc-drift guard caught the new coupling.** `sqlite_feasibility_docs` failed on the new query: `workers` now genuinely uses `INTERVAL '…'` and Postgres-only syntax (`::bigint`/`::text` casts, JSONB `@>`), which its inventory row in `docs/rnd/sqlite-feasibility.md` did not record. The row and the two live mechanism counts (`interval-sql` 8 → 9, `raw-pg-sql` 18 → 19) are updated, and the fourth-round prose — which argued from that round's count of 18 — now says plainly that the counts move while the classifications do not, so the historical argument stays intact without going stale against the table above it. `workers` was already class (b) and remains so.

## Review round 9

**P1 — `increase(...) > 0` cannot see the first sample of a new series.** The two escalation alerts each read `sum by (queue, task_type) (increase(harvest_task_capability_miss_total{outcome=…}[5m])) > 0`. The adapter creates a counter *by incrementing it*, so a `(queue, task_type, outcome)` series that has never fired appears in the scrape already at `1`, with no preceding zero sample; `increase` reports last-minus-first over the window, which is `0` for a series whose every sample reads `1`. The first escalation on a new series is therefore invisible — and since a new series appears the first time a handler is rolled out, that is precisely the scenario these rules exist for. On a low-volume queue the first escalation may also be the only one, in which case the page never fires at all.

**Zero-initialising the label sets at worker startup was considered and rejected.** It is the usual remedy and it is not sufficient here: the zero has to be *scraped* before the increment, and both `escalated_never_offered` causes — a `capability_miss_max_redeliveries = 0` rollback switch, and a session-pinned task — escalate on the task's **first** claim, which can land inside the same scrape interval as worker startup. It would also fix one metric's instance of a property that belongs to the expression. The detection lives in the expression instead, matching round 5's precedent (that finding was the same family — Prometheus range-function semantics on sparse or new series — and was likewise fixed in the pack).

Each rule now carries a single `or`-joined expression whose second arm is a set difference: `M unless M offset 5m` matches only series present now and absent one window ago, i.e. exactly the first sample. It stops matching once the series is a window old, so the alert's resolve behaviour is unchanged. Both arms select the same outcome at the same severity, so this is not a repeat of round 2 (a second expression carrying a *different* severity's signal) — it is one condition written as one expression.

**`harvest_capability_miss_release_sustained` is deliberately unchanged.** It requires 15 consecutive minutes of non-zero release rate, which a single first sample cannot satisfy by construction; by the time the hold is met the counter has incremented many times and `rate` is well-defined. Codex flagged only the two `> 0` rules, correctly.

**The other two surfaces that quote the expression are corrected rather than left to mislead.** The `harvest_capability_miss_release_sustained` runbook's step 4 tells an operator to check that the escalation counter is "still flat" — copy-pasting the old form during triage reproduces the blind spot at the worst moment, so it now states that a zero there is not proof of zero escalations and points at both the set-difference arm and the scrape-independent `GET /workflows?state=FAILED` check. The dashboard panel keeps its `increase(...[1h])` trend form (the pack's convention is that counters live inside `rate`/`increase`, enforced by `dashboard_pack_docs`), with its description now naming the same limitation so a flat-looking panel is not read as contradicting a page.

**Tests.** `capability_miss_released_outcome_never_pages` gains a third half asserting both rules carry an `unless … offset 5m` set-difference arm selecting their own outcome. It fails RED against the pre-fix pack, naming the rule and printing the expression.

## Review round 10

**P2 — a post-handler capability miss preserved the issue #494 timeout strike it had no business preserving.** `first_persist_capability_miss` (added in round 6) releases the task when persisting a decision finds an unregistered activity or child type. By that point the workflow *body has already run to a conclusion inside its deadline* — only assembling its commands failed. The release was nevertheless collapsed into the same `TaskDispatchOutcome::Released` as a missing *workflow-type* lookup, whose entire justification is "the handler never ran, so this dispatch observed nothing". So a healthy workflow banked strikes for misses it had nothing to do with, and a single later transient timeout could tip it into poison-pill quarantine as if the timeouts had been consecutive.

The strike decision is a property of *where* the miss was detected, so that is what the error now carries. `HarvestError::HandlerNotRegistered` gains a `CapabilityMissPhase` (`BeforeHandler` / `DuringHandler` / `AfterHandler`), stamped at each of the eight raise sites, and `TaskDispatchOutcome::Released { clears_timeout_strike }` carries the resolved answer to the dispatch site. Encoding it at the raise site rather than inferring it later is what makes it correct: the error propagates out of `process_workflow_task` through `?` from several layers down, so the phase cannot be reconstructed at the interception point.

**`DuringHandler` deliberately does not clear.** An inline local-activity miss (#98) happens *mid*-body — the workflow suspended on it and has not returned. Treating that as evidence of health would let a workflow that hangs *after* its first local activity reset its own streak on every redelivery. The hazard is two-sided and that is why this is a three-value phase rather than a blanket rule: always preserving lets a healthy run bank strikes it did not earn, always clearing lets an incapable worker erase a genuinely hung run's streak so `poison_pill_threshold` is never reached in a mixed fleet.

**P2 — fleet evidence counted workers that could never claim the task.** Round 8's `live_workers_on_queue` selected on a fresh heartbeat plus the queue, but `claim_task` gates on two further task-specific predicates: `required_build_id` (#171, with the `harvest_build_compat` fallback) and `required_capabilities` (#522, Exact/In label matching). A worker failing either can never win the claim, so it can never enter `capability_miss_workers` — and left in the live set it held `fleet_capability_evidence` at `CapablePeerMayExist` *permanently*. The configured distinct-worker budget was therefore withheld forever on a task no live worker could run, leaving the far larger `10x` total ceiling as its only bound.

The query now returns `build_id` and `labels` too, and the pure `claim_eligible_workers` narrows the set using the same two helpers the #522 stranded-demand sampler uses for the same "could this worker claim it?" question (`BuildCompatibilitySet::is_eligible` and `eligibility::matches_requirements`), so the fleet-evidence filter and the demand-coverage check cannot drift. The gates are applied at the *caller*, not in SQL: the query answers "who is live on this queue" (a queue property), the caller narrows to "who could claim this task" (a task property).

**Unreadable `required_capabilities` keeps the worker.** Excluding on a value that cannot be parsed would shrink the live set toward empty and fabricate an `AllLiveWorkersMissed` conclusion, escalating a task a capable peer could still run. The failure direction is deliberately "release for longer", never "fail sooner" — the same no-false-positive rule the #522 sampler follows.

**Tests.** Both RED-verified. `capability_miss_phase_clears_the_strike_only_after_the_handler_ran` pins the truth table including the conservative `DuringHandler` case, and `missing_handler_phase_reaches_the_timeout_strike_decision` pins the wiring through to the outcome value so an inverted boolean or a hard-coded `false` (the pre-round-10 behaviour) is caught. `claim_ineligible_workers_are_excluded_from_the_live_fleet` covers wrong-build, wrong-labels, the unconstrained task, the legacy empty-build worker, and the unparseable-requirements fallback; with the build predicate neutered it fails `left: ["wrong-build", "eligible"], right: ["eligible"]`.

## Review round 11

**P2 — releasing a rate-limited activity kept the token its claim had spent.** `claim_task`'s `rate_limit_debit` CTE debits a token for any task carrying a `rate_limit_key` whose activity is not circuit-breaker tracked, and the claim only succeeds if that debit landed. A capability miss is necessarily that untracked case — the tracked set is derived from the claiming worker's *own* registered activities, so an activity it does not register cannot be in it. The release restored the task's `attempt` but never the token, so every incapable claim charged real capacity to a dispatch that ran nothing.

On a slow bucket that is not cosmetic. The capable peer waits out a refill interval it should not have to, and the incapable worker — whose release backoff starts at one second — is well placed to take the next minted token as well, so a low-refill activity can be starved by a worker that cannot run it.

`refund_capability_miss_rate_limit_token` runs unconditionally at the top of `handle_capability_miss`, *before* the release-vs-escalate branch, because the activity did not run in either case. A present `rate_limit_key` is the whole predicate — it mirrors `claim_task`'s own gate, so the two cannot drift, and the tracked-set invariant above means no separate circuit-breaker check is needed. A refund failure is logged rather than propagated: the release is the load-bearing action, and failing the dispatch over unreturned capacity would strand the row `RUNNING` under a live worker, which the poison-pill reclaimer (which requires a dead heartbeat) cannot recover — strictly worse than the leak.

**Test.** `releasing_a_rate_limited_activity_refunds_its_token` seeds a `burst 1 / refill 0` bucket — exactly one token, ever, so no refill can replace it and mask a missing refund — enqueues a rate-limited activity, and runs an incapable worker. It fails RED with `got 0`. The bucket key is unique per run: a shared key would let a `PENDING` task left by an earlier run race for the single token, so a later run could fail for losing that race rather than for the property under test.

## Review round 12

**P1 — a release laundered a poison task's crash history.** The release zeroed `crash_strikes` unconditionally, on the reasoning that "a successful claim+release proves the task crashed no worker". That reasoning is exactly the one round 4 and round 10 already rejected for the #494 timeout strike, applied to the wrong counter: a *capability miss ran nothing*, so it proves nothing about whether the task still kills the process that runs it. In a heterogeneous fleet an incapable claim landing between two capable crashes resets the streak to zero every time, so `poison_pill_threshold` (#367) is never reached and the poison task goes on crashing replacement workers indefinitely — the same blameless-third-party defeat, one counter over.

The fix reuses round 10's mechanism rather than inventing a second one. `CapabilityMissPhase` gains `clears_crash_strikes()`, and both it and `clears_workflow_timeout_strike()` now delegate to one private `handler_ran_to_conclusion()` — the single fact they are both derived from, stated once so they cannot drift, with a test that asserts they agree for every phase. `release_task_for_capability_miss_query(clear_crash_strikes: bool)` selects between two literal statements (mirroring `park_workflow_task_query`), and the preserving variant does not write the column at all.

**AC4 still holds, more conservatively than before.** AC4 requires that a clean miss must not *increment* a crash counter; preserving satisfies that strictly better than zeroing did, and the three-layer independence argument is unchanged — `reclaim_orphaned_tasks` is the only writer that increments the column, and it scans `RUNNING AND worker_id IS NOT NULL`, so a released row (`PENDING`, NULL worker) is invisible to it either way. Every shared clause in the release SQL is now pinned in **both** literal variants by a loop, since two strings are exactly the thing that drifts.

**P2 — modelling `claim_task`'s legacy `$6` activity gate was investigated and declined, because the available fix is provably a no-op for the bug.** Codex is right that `claim_task` carries a fourth task-specific gate round 10 did not model: for an *activity* row whose `required_capabilities` snapshot is NULL (a legacy or manual enqueue), a worker is rejected if the activity appears in its own `ineligible_activities` list — the activities it registers but whose declared `requires` its labels do not satisfy. Such a worker can never claim, never joins `capability_miss_workers`, and therefore holds the fleet evidence at `CapablePeerMayExist` indefinitely.

The suggested remedy is the #522 sampler's back-fill (`HandlerRegistry::activity_requirements_json`, via `queue::apply_activity_requirements`), which resolves an un-snapshotted row's requirements from the registry. That cannot work here: the registry it would be resolved from is the **claiming worker's**, and a claiming worker that just capability-missed an activity task by definition does not register that activity — `process_activity_task` raises the miss precisely because `registry.activities.get(activity_name)` returned `None`. So the back-fill always yields `None` on exactly the path that needs it. A faithful per-worker model is not reachable either: the gate is a function of each *peer's* own registry, and `harvest_workers` advertises queues, build and labels but not registered activities.

The residual is therefore left in place, because it fails in the safe direction. Withholding the configured distinct-worker budget delays escalation to the ungated `10 x` release ceiling — "release for longer", never "fail sooner", which is the rule round 10 set for exactly this class of imprecision — and AC3's bound still holds, since that ceiling is ungated precisely so coverage being unprovable cannot make the task unbounded. The ceiling's own escalation reason already points the operator at the live peer that never claimed; its wording now names label-ineligibility for the activity's registered requirements alongside saturated / draining / stale queue list, and the runbook says the same, so the one observable symptom is explained rather than left to be rediscovered.

## Review round 13

**P2 — a release cleared the issue #782 consecutive-panic strike it had no business clearing.** `fail_workflow_execution_clearing_strikes` cleared `workflow_panic_strikes` for *any* `Err`, then handed the error to `fail_execution_on_error`, which (since round 6) passes a capability miss through **un-failed** so the dispatch path can release the task. So a `BeforeHandler` or `DuringHandler` miss — a dispatch that ran nothing, or suspended mid-body on an unregistered local activity — wiped the streak on its way out. A worker that alternates between panicking on the body and missing an unregistered local activity therefore resets `workflow_panic_max_attempts` forever, and #782's containment never fires. This is the third instance of one bug: rounds 10 and 12 fixed the same laundering for #494's timeout strike and #367's `crash_strikes`.

The fix reuses the existing mechanism rather than adding a fourth ad-hoc rule. `CapabilityMissPhase` gains `clears_panic_strike()`, delegating — like its two siblings — to the private `handler_ran_to_conclusion()`, and the agreement test now covers all three predicates, so a future special-case has to be deliberate. `apply_panic_strike_clear_for_failure` extracts the decision *and* its effect on the map out of the async DB wrapper, so both are unit-testable without a connection.

**A non-capability-miss error still clears the entry, at every phase.** That path terminally fails the execution, so retaining the entry would leak one `u32` per such execution — the documented reason the wrapper exists. Only the non-terminal capability-miss releases are exempted, which is precisely the set that observed nothing about the body.

**Tests.** Both RED-verified against the pre-fix predicate (`failure_clears_panic_strike` stubbed to `true`), which fails `left: None, right: Some(2)` — the streak wiped. `capability_miss_phase_clears_panic_strike_only_after_the_handler_ran` pins the truth table plus cross-predicate agreement; `apply_panic_strike_clear_for_failure_mutates_the_map_per_phase` pins the mutation itself, including that `Ok` never touches the map and that an ordinary error still clears.

## Review round 14

**P2 — the new-series arm read a monitoring gap as a new escalation.** Both escalation rules pair `increase(...) > 0` with a set difference, because the adapter creates a counter by incrementing it: a `(queue, task_type, outcome)` series that has never fired appears in the scrape already at 1, so `increase` (last-minus-first) reports 0 and the *first* escalation — which on a low-volume queue may be the only one — is invisible. The set difference was written as `M unless M offset 5m`, and that arm cannot tell a brand-new series from a scrape or remote-write outage: if the target was unscrapeable across the whole `offset 5m` lookback, the right side is empty, so a series whose counter never moved is re-selected as "new" and pages. `increase` stays 0 throughout, so the `or` fires on nothing having happened.

The distinguishing fact is that a genuinely new series has **no** sample at any earlier point, whereas a gapped one has samples before the gap — so the comparison has to be a range, not an instant. Both rules now use `max_over_time(M[5m]) unless max_over_time(M[1h] offset 5m)`: "is there any sample for this series in the preceding hour?".

**Both sides are wrapped deliberately.** `max_over_time` and a bare instant selector do not agree on whether `__name__` survives; an asymmetric wrapper would make `unless` match nothing, silently degrading the arm to an unconditional `M > 0` — a far louder failure than the one being fixed. Wrapping both keeps the label sets symmetric whatever the Prometheus version does. Matching on the **full** label set rather than `on(queue, task_type)` is also deliberate: it preserves detection across a pod restart, where the replacement instance's series is genuinely new and its first escalation is exactly the invisible-to-`increase` case.

**Residual, stated rather than hidden.** A monitoring gap longer than the 1h memory still reads as new. Inhibiting that is deployment-specific — the standard remedy is an Alertmanager `inhibit_rule` keyed on the deployment's own scrape-health alert (`up == 0` / `TargetDown`) — and a deployment-agnostic starter pack cannot express it, so the rule notes and the runbook say so.

**Test.** `capability_miss_escalation_rules_detect_a_brand_new_series` gained two assertions, both RED-verified against the previous expression: the arm must look back over `[1h] offset 5m` rather than a bare instant offset, and `max_over_time(...)` must appear exactly twice per rule so the two sides cannot drift apart.

## Review round 15

**P1 — `capability_miss_max_redeliveries` was not a maximum for any fleet smaller than the budget.** The primary bound counts *distinct* incapable workers (round 6, so one pod repeatedly winning the claim race cannot exhaust a shared budget), and the absolute `10 ×` ceiling was the fallback for fleets too small to grow the distinct set. But "too small" is not exotic: with the default budget of 5, **any fleet of 1–5 workers** can never satisfy `distinct_after > 5`, so a single incapable worker fell straight through to the ceiling — 50 releases, escalating on the 51st claim, roughly **23 minutes** at the backoff cap instead of the documented five releases / ~31 s. The issue's own success metric ("with zero capable workers, escalate within ≤ N") was violated for the commonest deployment shape.

The missing bound is the configured budget applied to *total* releases, and the condition that licenses it is already computed: once `fleet_capability_evidence` reports `AllLiveWorkersMissed`, the distinct set cannot grow from the fleet as it stands, so there is nothing left to wait for except a *new* worker — and waiting for that is what the knob is sizing.

**Gated on `AllLiveWorkersMissed` specifically, not on `!CapablePeerMayExist`.** That asymmetry with the distinct bound is the point. The distinct bound may fire on `Unavailable` because `budget + 1` *different* workers missing is strong evidence on its own; this one may not, because its evidence comes entirely from the registry — on `Unavailable` it would fire after `budget` releases that may all have been won by the same pod, which is precisely the round-6 defect. A fleet the registry cannot describe therefore keeps the ceiling as its only bound: "release for longer, never fail sooner", the rule every prior round has applied to unprovable coverage.

**The ceiling changes meaning, and the docs follow.** It is no longer "the small-fleet bound"; it is now reachable *only* when coverage could not be concluded — a live worker that never missed the task, or an unreadable registry. `EscalationCause::resolve` mirrors the new bound so a small fleet reports the budget it exhausted (with the fleet-wide conclusion substantiated) instead of inheriting the ceiling's much weaker wording, and the `ReleaseCeilingExhausted` page rationale is restated: it pages because the executions are failing either way, not because a one-pod deployment has no other bound.

**Tests.** Three pure, all RED-verified by removing the bound: `single_worker_fleet_honors_the_configured_redelivery_maximum` fails `left: 50, right: 5` and pins the 31 s dwell; `configured_total_bound_requires_confirmed_fleet_coverage` pins that `Unavailable` and `CapablePeerMayExist` both still fall to the ceiling (so round 6 is not reintroduced) while confirmed coverage escalates at `budget + 1`; and `small_fleet_budget_exhaustion_reports_the_budget_not_the_ceiling` fails `left: ReleaseCeilingExhausted{..}, right: BudgetExhausted{fleet_confirmed: true}` and asserts the reason names the budget rather than the ceiling. `capability_miss_total_ceiling_bounds_a_single_worker_bounce` still passes unchanged — its empty `live_workers` makes the evidence `Unavailable`, which is now exactly the case the ceiling exists for.

Plus one DB test driving the real worker loop end to end, `single_worker_fleet_escalates_at_the_configured_budget`: a lone incapable pod on a dedicated queue (so the shared test database cannot lend it a peer) escalates after exactly `BUDGET` releases with a reason that names the configured budget, does **not** contain `absolute release ceiling`, and records `outcome="escalated"` once and `escalated_never_offered` zero times. It is the direct contrast to the round-6 `one_incapable_worker_cannot_exhaust_the_shared_redelivery_budget`, which now seeds a live never-claiming peer so its fleet evidence stays `CapablePeerMayExist` — the state that property is actually about. Both were confirmed RED against the pre-round-15 decision function.

## Review round 16

**P2 — an *escalated* capability miss leaked its issue #782 panic strike.** Round 13 stopped a pre-/mid-handler miss from clearing the strike, which is right for the **release** path: the execution stays `RUNNING`, so the streak must survive or a worker alternating between panicking on the body and missing an unregistered local activity would hold `workflow_panic_max_attempts` out of reach forever. Escalation is the other outcome and needs the opposite treatment — it routes through `fail_task_and_execution`, so the execution is terminally failed and there is no later cycle for it. The entry then sat in the map for the worker's lifetime, which is the same leak the ordinary-error arm of `apply_panic_strike_clear_for_failure` exists to prevent.

Reachable, not theoretical: a worker that registers workflow `W` but not activity `A` panics on `W`'s body (strike 1), is re-driven, reaches the `A` dispatch and raises a `DuringHandler` miss. If that miss escalates — budget exhausted, session-pinned, or `capability_miss_max_redeliveries = 0` — the run is terminally failed with the strike still held. It applies to activity tasks too: `fail_task_and_execution` fails the *owning workflow execution* whenever `workflow_exec_id` is present, whichever task type escalated.

**The discriminator is the re-raised typed error, and that choice is load-bearing.** `escalate_capability_miss` re-raises `HandlerNotRegistered` only *after* `fail_task_and_execution` returned `Ok`, and the release arm never returns that variant at all — so an `Err(HandlerNotRegistered)` out of `handle_capability_miss` means "escalated **and** the terminal write landed", and nothing else. That is strictly better than clearing on the escalate *decision*: a failed terminal write propagates as its own variant, leaves the row stranded `RUNNING` under a live worker, and keeps the strike — which is precisely the re-drive case round 13 protects. Round 13's rule is therefore untouched; this adds the terminal outcome it never covered.

New `clear_panic_strike_on_capability_miss_escalation` sits at the single `process_task` interception point (where the strike map is already in scope), so no signature threads a `Mutex` down through `handle_capability_miss`. A `None` `workflow_exec_id` (an orphan task) has no key to clear.

**Test.** `capability_miss_escalation_clears_the_panic_strike`, RED-verified against the pre-fix code (`cannot find function`, then `left: Some(2), right: None`): asserts the entry is cleared for all three phases on escalation, and preserved on a release, on a failed terminal write, and for an orphan task.

## Review round 17

**P2 — a post-handler release let `harvest.workflow.started` fire twice for one execution.** `record_workflow_started` is gated on `task.attempt == 1` **and** "no scheduling events in history" — two conditions that together mean "this dispatch is the execution's first". A post-handler capability miss satisfies both again on the next claim: the persist-time pre-pass runs *after* the metric has already fired, its persistence transaction rolls back (so no scheduling event lands), and the release rolled `attempt` back to `0`. The next capable claim therefore saw `attempt == 1` with clean history and counted the same execution as started a second time. Repeated incapable dispatches inflate workflow-start counts and every SLO derived from them.

It is a #804-introduced regression rather than a latent one: before this PR a post-handler miss failed the execution terminally, so there was never a second dispatch to double-count.

**Fixed by splitting the two jobs `attempt` was doing.** It is simultaneously the retry budget and the "is this the first dispatch?" signal, and those want opposite things once the handler has run. `CapabilityMissPhase` gains `restores_dispatch_attempt()`, and the release only decrements `attempt` when it is `true`.

The split is safe because the two populations do not overlap. Every **activity**-task miss is raised by the activity-handler lookup in `process_activity_task`, which is `BeforeHandler` — so the retry budget is still always restored wherever it *is* a budget, and round 1's fix is untouched. Every `DuringHandler`/`AfterHandler` miss is on a **workflow** task, where `attempt` is read only by this metric gate and the informational `attempts` field of a dead-letter row: `claim_task` never filters on it, and the issue #523 workflow-level retry loop counts `harvest_workflow_executions.workflow_attempt`, a different column. Letting a workflow row's `attempt` grow across post-handler releases therefore costs nothing and is exactly what suppresses the duplicate.

**A fourth predicate, deliberately not a fourth copy of the third.** The three strike predicates (#494, #367, #782) all delegate to `handler_ran_to_conclusion()` — "did the body *finish*?". This one delegates to a new `handler_was_reached()` — "did the body *begin*?" — because everything that fires at the *start* of a dispatch has already fired by then. The two facts come apart on exactly one phase: `DuringHandler` began but did not finish, so it answers `false` to both, which is why `restores_dispatch_attempt` cannot be expressed as the negation of a strike predicate. A test pins that, or a later "simplification" to `!clears_panic_strike()` would silently re-introduce the double-count for every mid-body local-activity miss.

**Two alternatives were rejected on evidence, not taste.** Deferring the metric until after the capability pre-pass would *under*-count: the pause re-park, the #494 workflow-task timeout, #782 panic containment and the #603 ND-block all early-return between the metric and the pre-pass, and none of them resets `attempt`, so such executions would never emit `started` at all. Gating on `capability_misses == 0` under-counts the common case in the other direction — a `BeforeHandler` rolling-deploy miss never emitted the metric on its first dispatch, so blocking the capable one loses it entirely. A new durable marker column was declined as disproportionate once the phase already carries the answer.

`release_task_for_capability_miss_query` now takes the `CapabilityMissPhase` itself rather than the booleans it implies, so the enum stays the single source of truth for both conditional clauses and the one incoherent combination — clear the crash strikes *and* restore the attempt — is unrepresentable.

**The shared release assertion narrowed with the invariant.** `assert_released_for_a_peer` hard-coded `attempt == 0` as a universal release property; it is now the phase-agnostic half (claim dropped, crash accounting untouched, `error` unwritten) with a new `assert_released_before_the_handler` wrapper carrying the AC4 retry-budget clause. That keeps the guarantee asserted exactly where it holds — the AC1 and AC2 end-to-end tests, both pre-handler — rather than quietly asserting the pre-fix behaviour everywhere. The new post-handler test asserts the opposite value explicitly. The narrowing was surfaced by the new test rather than anticipated: it failed on the shared helper first.

**Tests.** Three, all RED-verified. `capability_miss_phase_restores_the_attempt_only_before_the_handler_ran` pins the truth table and the `DuringHandler` divergence. `capability_miss_release_query_restores_the_attempt_only_before_the_handler` asserts the clause is present in exactly the `BeforeHandler` literal and that the other two do not rewrite `attempt` at all, with a companion loop pinning every clause the three variants share (three strings drift where two could). `a_post_handler_release_does_not_re_emit_workflow_started` drives the real worker loop: a worker registering the workflow but not the activity it schedules releases the workflow task post-handler, then a fully capable worker completes the run, and one shared recorder asserts `harvest.workflow.started` was emitted **exactly once** across the whole sequence.

## Review round 18

**P1 — an unreadable build-compatibility table terminally failed a run a capable peer could have finished.** `read_live_fleet_or_degrade` narrows the live fleet to workers that could actually win *this* task's claim (round-10 P2), and resolved a failed `load_compat_set` with `unwrap_or_default()`. But `BuildCompatibilitySet::is_eligible` answers `false` for any worker whose build differs from the requirement unless a declaration says otherwise — so degrading a failed read to the empty set does not merely *lose* the declarations, it **asserts their opposite**, dropping every cross-build peer that `claim_task` would still admit through `harvest_build_compat` (#171).

The narrowed fleet then held only the incapable claimer, `fleet_capability_evidence` fabricated `AllLiveWorkersMissed`, and the round-15 configured-total bound escalated at `budget + 1` — roughly 31 seconds — terminally failing an execution while a live, declared, capable peer was polling the queue. One transient `SELECT` failure was enough, because the evidence is recomputed on every miss and only the crossing one matters. It also reinstated the round-6 P1 the distinct bound exists to prevent: that bound counts *distinct* incapable workers precisely so a single worker cannot spend the budget by winning the claim race repeatedly, and the configured-total bound has no such protection — it is gated on the registry conclusion instead, which was exactly the conclusion being fabricated.

**Fixed by distinguishing "unknown" from "empty".** New pure `narrow_live_fleet` takes `Option<&BuildCompatibilitySet>`; `None` means the read failed. On failure it suppresses the **build** axis only, by asking `claim_eligible_workers` to evaluate no build requirement at all (`is_eligible(_, None)` is unconditionally `true`). The #522 label axis is read from the worker rows themselves, is therefore still knowable, and stays enforced — so the fix returns exactly the narrowing that became unknowable and no more.

This is the doctrine `claim_eligible_workers` already applied one field over: an unparseable `required_capabilities` "falls back to keeping the worker", for the stated reason that excluding the fleet "would fabricate an `AllLiveWorkersMissed` conclusion and escalate a task a capable peer could still run". The compat read was simply never held to the same rule. It is also the direction every prior round chose for unprovable coverage — release for longer, never fail sooner: retaining a worker that turns out to be genuinely ineligible costs at most a delay to the absolute ceiling, while dropping a capable one costs the execution.

**The neighbouring empty-fleet degrade is left as it is, and the asymmetry is now documented.** An empty *fleet* is inert — it cannot contain the claiming worker, so the evidence is `Unavailable` and concludes nothing about peers. An empty *compatibility set* is not inert; it makes a positive claim about eligibility. Only the first is safe to reach by degrading. The same doc comment also corrects a claim round 15 made stale: `Unavailable` does *not* "withhold no bound" — it withholds the registry-derived configured-total bound specifically, while the distinct bound and the absolute ceiling stay in force, which is what keeps AC3 true.

**The other two `load_compat_set` degrades were audited and deliberately left alone.** The #522 stranded-demand sampler (`worker.rs`) and the #171 `no_live_worker` shard-readiness gate (`shard_health.rs`) both `unwrap_or_default()` too, but in each the empty set pushes the answer toward *uncovered* / *not ready* — a false alarm and a fail-closed readiness verdict respectively, which is the safe direction for a monitoring signal and a gate. Only the #804 path had that same degrade pointed at a **terminal failure**, where the safe direction is the opposite one. Both predate this PR and neither is changed here.

**Test.** `unknown_compat_keeps_a_declared_peer_instead_of_fabricating_a_fleet_conclusion`, RED-verified (`cannot find function narrow_live_fleet`). It pins the retention both ways, then carries the finding through the full decision chain — `narrow_live_fleet` → `fleet_capability_evidence` → `capability_miss_decision` — asserting `Release` under the fix, and carries the pre-fix empty-set degrade alongside as a falsifying control asserting `Escalate`. A final case proves the build axis is suppressed without taking the label axis with it.

## Review round 19

**P1 — a fast-heartbeating worker could declare a healthy peer dead and escalate a task that peer could have run.** The fleet lookup that decides whether *"no live worker on this queue has the handler"* is actually true reads `harvest_workers.last_heartbeat_at` against a freshness window, and that window was `worker_stale_secs(...)` — `2 ×` the **claiming** worker's own configured `worker_heartbeat_interval`. Nothing in `harvest_workers` records the cadence each worker chose, so one window is applied to every row: a pod configured to heartbeat every second queries a two-second window and drops a peer running the **default** five-second cadence, which is healthy and roughly 60% of the time outside that window. With that peer gone the narrowed fleet held only the incapable claimant, `fleet_capability_evidence` reported `AllLiveWorkersMissed`, and round 15's configured-total bound terminally failed the execution while a capable peer was polling the queue. A default-configuration hazard, not an exotic one — the excluded peer is the one running the shipped default.

That window is correct for the two subsystems it was extracted for. The poison-pill reclaimer (#367) and the broken-session scanner (#606) each judge rows they are entitled to judge, with a window they chose. This lookup asks a different question — *"could some **other** worker still run this?"* — so judging peers by the claimant's cadence is a category error, and the round-19 divergence is deliberate rather than a drift.

**Fixed with a fleet-wide floor, applied with `max`.** New `capability_miss_fleet_stale_secs` widens the computed window to at least `CAPABILITY_MISS_MIN_FLEET_STALE_SECS` (120 s = `2 ×` the newly-named `MAX_SUPPORTED_HEARTBEAT_INTERVAL_FOR_FLEET_LIVENESS` of 60 s). Because it is a floor and not a replacement, the resulting window is **never narrower** than before — so this can only ever retain *more* peers, and no fleet that escalated correctly can begin escalating spuriously. `worker_stale_secs` itself is untouched: widening it would delay orphan recovery for the two subsystems that legitimately want the tighter window.

Retaining a worker that is genuinely gone is the safe direction and is bounded. A stale row that is not in `capability_miss_workers` holds the evidence at `CapablePeerMayExist`, which withholds only the configured-total bound — the distinct-worker bound and the absolute ceiling still fire, so AC3 holds. The cost is a delayed escalation; the cost of the opposite error is the execution. Same trade every prior round took for unprovable coverage.

**The cadence ceiling is surfaced, not assumed.** A constant floor is honest only if the contract it encodes is stated, so `try_build` now warns (never rejects — an already-deployed slow fleet must keep booting, matching the degenerate slot-tuner band and `queue_weights`) when `worker_heartbeat_interval` exceeds 60 s, naming the consequence. The runbook carries the same rule. A worker past the ceiling can still be escalated against early by a faster peer; it is now told so at startup instead of finding out through a failed execution.

**Tests.** Four pure unit tests plus a builder test, all RED-verified. `capability_miss_fleet_window_never_shrinks_the_liveness_window` sweeps twelve intervals and pins the retains-⊇ invariant that carries the whole safety argument. `capability_miss_fleet_window_covers_a_peer_on_the_default_cadence` is the finding itself, and carries a falsifying control asserting the *replaced* window did **not** cover that peer — without which it would prove nothing. `capability_miss_fleet_window_still_tracks_a_slower_claimant` pins that the floor is a floor and not a cap. `capability_miss_fleet_floor_matches_the_documented_supported_cadence` ties the constant to the ceiling the builder warns about, so the two halves of one contract cannot drift. `slow_heartbeat_warns_but_never_blocks_the_build` pins the boundary in both directions and that the build still succeeds.

**P2 — an incapable worker committed a local-activity batch's sibling commands before discovering it could not run the batch.** The inline local-activity arm commits five things ahead of `run_local_activity_inline`'s registry lookup: search-attribute patches, the `current_details` breadcrumb, durable workflow logs (#790), ephemeral progress frames (#791), and any early mutex release (#691). Once a miss became *releasable*, an incapable worker paid for all five and handed the task on, and the next worker redid them — up to once per redelivery.

**The reported harm is narrower than stated, and the correction matters for the fix's framing.** Durable logs are **not** duplicated: `RecordLog` carries a deterministic `seq` and `append_workflow_logs` inserts `ON CONFLICT DO NOTHING`, with an in-code comment naming re-drive as the case it exists for. Attribute and breadcrumb writes are last-write-wins; a repeated mutex release is fenced to zero rows. Only the ephemeral progress frames genuinely re-fire, and #791 already declares them best-effort and at-least-once with a `seq` clients dedup on. Nor is re-drive repetition new to #804 — the #494 workflow-task timeout, #782 panic containment and the #603 ND-block all re-run the same live frontier through the same commits. So this is not a correctness fix, and it is not claimed as one.

**Fixed anyway, via the reviewer's first suggested remedy, because it is strictly better.** New `missing_local_activity_handler` runs at the top of the arm, before any of the five, and raises the same typed error with the same `DuringHandler` phase (the body began and did not conclude — it suspended on this local activity). Behaviourally identical to the lookup a few frames later, except an incapable worker no longer does work a capable peer will redo, and the one genuinely observable repeat disappears. The original lookup stays as the backstop that still yields the `&ActivityInfo`; both consult `registry.activities`, so they cannot disagree about *whether* a handler exists, only about how much work happened first.

It checks **every** `RunLocalActivity` in the batch rather than the first, because `extract_run_local_activity` keeps the *last* — a first-only check would wave through exactly the command about to run.

**Tests.** `missing_local_activity_handler_names_the_activity_that_cannot_run` (RED-verified) pins the registered case as *not* flagged — otherwise every healthy local-activity cycle would release — the last-wins ordering, and that the sibling commands never flag on their own. `a_local_activity_miss_releases_before_committing_its_batch` drives the real worker loop and uses `current_details` as the cheapest durable witness for the whole commit block: phase 1 asserts the incapable worker left it `NULL`, and phase 2 asserts the capable worker *does* write it, so the phase-1 assertion cannot be passing merely because the workflow never set it.

## Review round 20

**P1 — misses recorded at one local-activity frontier were charged against the next one, failing runs the fleet could still finish.** `capability_misses` and `capability_miss_workers` describe **one frontier**: the position a workflow is stuck at, and therefore the single handler a claiming worker must register to move it. The park-time reset already encodes that — a park is proof a capable worker handled the row, so the next deploy starts with a full budget. But a workflow task can advance through *several* local-activity frontiers in ONE dispatch: `process_workflow_task` runs them inline in a loop and only parks on a *non*-local suspension. Each completion is appended durably, so the frontier moves permanently — yet nothing parked, so the counters kept describing a position now behind us.

Worker A missing local activity X and worker B later missing local activity Y therefore read as *"two distinct workers missed this task"*. The registry confirms both are live, `fleet_capability_evidence` reports `AllLiveWorkersMissed`, and the run is terminally failed — even though A may register Y and is polling the queue at that moment. That conclusion asserts the exact opposite of the truth, which is the failure mode #804 exists to prevent, and it is the same class as rounds 8, 15, 18 and 19.

**Fixed by scoping the accounting to the frontier.** `reset_capability_misses_after_inline_progress` zeroes both columns when a local activity resolves durably, which is precisely the moment every prior miss becomes stale by construction. It is the same reset the park path performs, at the other point a workflow task demonstrably advances — so this closes a gap in an already-established semantic rather than inventing one. The UPDATE is guarded on `state = 'RUNNING' AND worker_id = $2` so a row a concurrent poison-pill reclaim already took is never rewritten.

The release-vs-escalate **decision** had to move with it: `handle_capability_miss` read the two columns off the claim-time `TaskQueueItem` snapshot, which the reset may have invalidated from inside this very dispatch. It now re-reads them (`current_capability_miss_state`), falling back to the snapshot when the row is gone — a case the release's own `0 rows affected` path already handles. The backoff is scaled off the same fresh count, so a frontier the fleet has not been asked about starts at the base delay instead of inheriting a prior frontier's.

**This cannot be used to release forever.** A *completed* local activity resolves from history on the next replay (`HistoryMatch::Matched`) and emits no command, so a given frontier can make inline progress at most once; the number of local-activity call sites on a code path is bounded, and bounded again by the history hard cap. The budget becomes "per frontier" rather than "per task", which is what it always meant — and the existing escalation tests (`single_worker_fleet_escalates_at_the_configured_budget`, `capability_miss_escalates_after_the_budget_with_no_capable_worker`) still pass unchanged, so AC3's bound is intact.

The alternative remedy — keying the miss evidence by `(kind, name)` — was rejected: `capability_miss_workers` is a `TEXT[]` of bare worker ids, so it would need a migration (this PR adds none beyond its original one), rows written by an older build would carry an un-keyed meaning, and it would multiply the effective budget by the number of frontiers, undermining the very bound AC3 asks for.

**Tests.** Two pure SQL-shape unit tests pin the reset's two halves: `inline_progress_reset_clears_both_capability_miss_columns` (clearing the count alone would leave the DISTINCT set — the thing the primary bound escalates on — describing the old frontier) and `inline_progress_reset_is_guarded_on_this_workers_claim`. The money test `durable_inline_progress_starts_the_next_frontier_with_a_clean_budget` drives the real worker loop against a deliberately non-nested fleet: A registers `second_local` but not `first_local`, B the reverse. Neither can finish the run alone, but B can clear frontier 1 and A can then clear frontier 2 — so a correct engine completes it. RED-verified: pre-fix the execution is `FAILED` at a budget of 1 while A is live. It asserts its own premise (a `LocalActivityCompleted` really is in history before B's miss) so it cannot pass vacuously, and asserts the reset's shape — the set holds only `worker-has-first`, the count is 1 — rather than only the end state.

## Review round 21

**P1 — the frontier reset was an independent failure point *after* the progress it describes became durable.** Round 20's reset ran as its own autocommit `UPDATE` in the drive loop, immediately after `run_local_activity_inline` returned. A transient failure there `?`-returned out of `process_task` with the row still `RUNNING` and still owned by this worker — and nothing recovers that. The poll loop's `Ok(Err(error))` arm only logs; the #494 workflow-task timeout fires on `tokio::time::timeout` *elapsing*, not on a fast `Err` return; and #367's orphan reclaim requires a **dead** heartbeat, which a healthy worker does not have. The workflow strands until its worker dies.

**Honest scoping: that strand hazard is pre-existing and engine-wide, not something round 20 introduced.** Roughly six sibling `?` statements in the same arm — `persist_search_attrs_from_commands`, `persist_current_details_from_commands`, and the `store::append_events` calls inside `run_local_activity_inline` itself — carry it identically. A fix for the *class* belongs at the dispatch boundary (requeue-on-error), not in one statement, and is out of scope here. What round 20 *did* add was one more such statement, and that one is removable outright.

**Fixed by making the reset atomic with the append rather than by retrying it.** The reset is bookkeeping *about* those events — it says "the misses recorded so far were recorded against a handler this run has now moved past" — so it belongs in their transaction. New `append_frontier_resolution` commits both together at the two appends that actually retire a frontier: success (`LocalActivityCompleted`) and retry exhaustion (the `LocalActivityFailed` + `LocalActivityExhausted` pair). A non-terminal failure leaves the *same* local activity as the frontier and deliberately does not reset. The standalone `?` is gone, so the only failure mode left is the pre-existing "the append itself failed" — in which case no progress became durable and the counters still describe reality. Strictly fewer failure points than before, and no new recovery path to reason about.

**P2 — a failed re-read fell back to a snapshot that could terminally fail the run.** Round 20's re-read of the current counters fell back to the claim-time `TaskQueueItem` on both `Err` and `Ok(None)`. But that snapshot is exactly the stale-frontier value the re-read exists to replace: if it had already spent the budget, the first miss at a frontier **no peer has ever been offered** escalated immediately. The error path re-introduced the very bug round 20 fixed.

**Fixed by treating unreadable evidence as a clean frontier.** New `clean_frontier_state()` returns `(0, [])` for both failure modes, named and documented so the rule is legible where it is used. This is the same rule `FleetCapabilityEvidence::Unavailable` already encodes one level up: evidence we cannot prove may never justify a terminal failure. It is self-healing rather than lossy — the release statement's increment is relative (`+ 1`), so whatever the row really holds survives — and it cannot suppress a legitimate escalation, because the session-pin and zero-budget cases are both decided *before* any budget bound. It also fixes `Ok(None)` on its own merits: a vanished row previously escalated on a phantom budget.

**Tests.** `an_unreadable_frontier_state_never_escalates_where_a_stale_snapshot_would_have` carries its own falsifying control — it first asserts the stale snapshot *does* escalate, so the test cannot pass by accident if the rule stops mattering. `a_clean_frontier_state_still_escalates_a_session_pin_and_a_zero_budget` pins that the fallback is not a way to dodge the two escalations decided ahead of the budget. `exhausting_a_local_activity_also_starts_the_next_frontier_clean` drives the real worker loop through the **exhaustion** append — the second of the two sites, which the round-20 money test never exercises — against the same deliberately non-nested fleet, and asserts its own premise (`LocalActivityExhausted` really is in history) so it cannot silently degrade into a re-test of the success path.

## Review round 22

**P2 — round 21's own fallback bypassed the storage-ceiling guard and could strand a row it was meant to protect.** Round 21 replaced a stale-snapshot fallback with a fabricated clean frontier whenever the re-read of the capability-miss counters failed. But `capability_miss_decision` carries an explicitly documented invariant — *never let the column reach `i32::MAX`* — enforced by a guard on the value about to be persisted. Substituting `0` for an unreadable count made `misses_after_increment` read as `1`, so the guard never fired, the resolver said `Release`, and the release statement's `capability_misses + 1` was evaluated against the **real** row: `integer out of range`, statement aborted, `?` propagated, and the task left `RUNNING` under a live worker — precisely the strand the invariant exists to prevent. At lower values the same substitution granted releases past the configured budget.

**The two findings are only reconcilable if the current counters can be established without a read**, and after round 21's atomic fix they can. The reset now commits in the *same transaction* as the frontier-resolving append, so the dispatch holds an exact, infallible fact about what the row contains:

* reset committed → the row was written to `(0, [])`;
* reset not committed (including a rolled-back transaction) → nothing in this dispatch touched those columns, so the claim-time snapshot is exactly current.

Nothing else writes them between the claim and the miss — a concurrent poison-pill reclaim changes `worker_id`, which the release's own `worker_id = $2` guard turns into a `0 rows affected` no-op — so the two cases are exhaustive. New pure `frontier_miss_state(task, reset_committed)` encodes them; an `AtomicBool` set by `append_frontier_resolution` after its commit carries the fact from the drive loop to the interception point. This is the alternative the round-21 review offered alongside the re-read ("carry the known reset state through the dispatch"); it is strictly better than either horn — no DB round-trip, and no failure mode to fall back *from*. `current_capability_miss_state` is deleted rather than left unused.

**The release statement now saturates rather than raises**, independently of the above: `capability_misses = LEAST(capability_misses, 2147483646) + 1`. `resolve_capability_miss` already saturates in memory, so the bare SQL `+ 1` was a latent inconsistency — and this bug arose precisely because a caller bypassed an in-memory guard. Making the column's own write unable to raise converts the documented invariant from *callers must check* to *cannot happen*, which is the class of fix this round is about.

**Tests.** `no_frontier_reset_preserves_the_storage_ceiling_guard` is the finding itself, RED-verified against round 21's code (`left: 0, right: 2147483647`), and also pins that a spent-but-representable budget still escalates on its true count. `a_committed_frontier_reset_decides_on_the_clean_frontier_not_the_snapshot` carries a falsifying control asserting the snapshot *does* escalate, so it cannot pass by accident. `a_clean_frontier_still_escalates_a_session_pin_and_a_zero_budget` pins that the reset path is not a way to dodge the two escalations decided ahead of the budget. The two SQL-shape assertions move with the statement, so the saturating form cannot silently regress to a raising one.

## Review round 23

Two P2s, both in the terminal `no_capable_worker:` reason string — the artifact an operator reconstructs a capability-miss incident from, and (via the reason-string decision table in `harvest-alerts.md`) the thing that routes them to the right triage branch. Neither affects control flow; both make the durable record state a number that is not the number that acted.

**P2 — distinct-bound exhaustion reported the configured budget as the release count.** `BudgetExhausted` printed `max_redeliveries` where the sentence reads as a count of redeliveries that happened. The two coincide only in the canonical shape where each worker misses exactly once, which is why every existing test happened to pass: `budget_exhausted_counts(N)` builds precisely that shape. They come apart under `FleetCapabilityEvidence::Unavailable`, where a repeat miss grows `completed_releases` without moving `distinct_after` (by design — round 6's fix so one noisy pod cannot spend a shared budget) *and* the configured TOTAL bound is gated off (round 15 restricted it to `AllLiveWorkersMissed`). Releases then accumulate unbounded-by-the-budget until a fresh distinct worker finally trips the distinct bound: five workers can spend twenty releases and the string still says "escalated after 5". An operator sizing the knob off that concludes the task was offered around five times when it was offered twenty, and that raising the budget to 6 buys one more attempt.

**Fixed by carrying the real counts.** `BudgetExhausted` gains `completed_releases` and `released_workers`, populated from the same persisted basis `ReleaseCeilingExhausted` has used since round 8 — the escalating claim never runs the release UPDATE, so the durable record is one behind the decision input. The string now reads `escalated after R capability-miss redeliveries across D distinct worker(s); capability_miss_max_redeliveries = N`. Reporting all three separately is what makes the two sub-cases legible at a glance rather than by inference: `D > N` is a genuine distinct sweep, `D ≤ N` is a small fleet that exhausted on total releases, and `R > N` is the no-registry-evidence case above. The knob still appears — raising it is the fix — just in its own labelled position rather than in the one that reads as history.

**P2 — the ceiling wording printed a factor of the ceiling, formatted as an equation.** `{MULTIPLIER}x capability_miss_max_redeliveries = {max_redeliveries}` rendered, at the default budget, as `10x capability_miss_max_redeliveries = 5` — false as an equation, and off by the whole multiplier as a bound. The ceiling that actually terminated the task was 50. Same failure class as the finding above: the string names the knob where the bound belongs.

**Fixed by computing the product with the same expression the decision uses** — `capability_miss_budget(max_redeliveries).saturating_mul(CAPABILITY_MISS_TOTAL_BUDGET_MULTIPLIER)`, the clamped, saturating form `capability_miss_decision` compares against — so the printed bound cannot drift from the enforced one. It now reads `absolute release ceiling of 50 releases (10x capability_miss_max_redeliveries (5))`: the bound, the multiplier, and the knob, each in a position that is true.

**Tests.** Both findings were reproduced first and the panic text is the defect verbatim: `escalated after 5 capability-miss redeliveries` for a record of 20, and `10x capability_miss_max_redeliveries = 5` for a ceiling of 50. `distinct_bound_exhaustion_reports_real_releases_not_the_budget` builds the `Unavailable` + repeats shape and asserts its own premise first (that this really does reach the *distinct* bound, not the ceiling — 21 releases is well under 50), so it cannot degrade into a re-test of the ceiling. `release_ceiling_reason_prints_the_computed_ceiling_not_the_bare_budget` guards the arithmetic it asserts on and pins both directions: the product must appear, and the false equation must not. The two DB tests that read the reason end-to-end gain assertions on the new counts — the single-pod one pins `across 1 distinct worker(s)`, the case where the release count and the distinct count genuinely diverge and a single printed number cannot tell them apart. The runbooks' worked examples and the reason-string decision table are updated to the new wording, including which of `R`, `D`, `N` and `C` distinguishes each row.

## Review round 24 — investigated and declined, with the reason recorded and pinned

**The mechanism reported is accurate.** A task carrying a crash, timeout or panic strike that durably resolves one local-activity frontier and then misses at a later one is released as `DuringHandler`, so all three consecutive-failure counters are preserved, and a subsequent single crash/timeout/panic can reach its threshold. That is the round-17 phase gating working as designed, now viewed from the inline-progress angle rather than the miss angle.

**The literal remedy — clear the strikes when the frontier-resolution transaction commits — is self-defeating.** That commit is on the *inline* path, so it runs on every dispatch that completes a local activity, including the one that goes on to kill its worker. `poison_pill::reclaim_orphaned_tasks` increments from the row's **current** value, so the zero would land first and `crash_strikes` could never exceed `1`: with the default `poison_pill_threshold` of 3 the quarantine becomes unreachable for the entire class of poison that crashes *after* making some progress. #494 and #782 have the identical shape — a body that hangs, or panics, after its first local activity completes that activity on every single redelivery, so treating that as a streak-breaker makes the threshold unreachable for exactly the hang or panic the counter exists to contain. Verified against the code rather than reasoned about in the abstract: the reclaim's `task.crash_strikes.saturating_add(1)` reads the row, and the frontier reset is reached before it on every such dispatch.

**The offered alternative — carry the progress signal into the release outcome — is narrower but reintroduces the defeat this type exists to prevent.** Clearing only at release time avoids a crashing worker laundering itself, but a worker that registers local activity 1 and not local activity 2 — the ordinary rolling-deploy shape, and the exact fleet #804 exists for — would then zero a *capable* peer's crash/hang/panic streak on every claim it makes. That is the blameless-third-party defeat round 12 declined for the same reason, with frontier progress as the new laundering vector instead of the miss itself.

**Preserving is compliant and not lax.** AC4 requires only that a capability miss never *advances* a strike counter, and the release never does — `capability_miss_release_query_never_increments_crash_strikes` has pinned that since round 4. Genuine progress does still clear all three, by the intended route: the clean continuation (`CleanContinuationChangeset`), which fires when the body carries forward to a suspension instead of stalling at a later frontier. The declined behaviour is confined to the case where a dispatch made partial progress and then could not continue — where the counters' own question ("does this task still kill workers / hang / panic?") is genuinely unanswered.

**What did change is the missing signpost.** `append_frontier_resolution` documented why it resets the capability columns but said nothing about why it deliberately leaves the three strike states alone, which is what made this reachable as a finding at all. It now states the rule and the two failure modes above. Two pure tests pin it so the decision is enforced rather than only argued: `inline_progress_reset_never_launders_a_crash_strike` asserts the reset's `SET` clause is exactly the two capability columns (verified load-bearing — adding `crash_strikes = 0` fails it), and `inline_progress_at_an_earlier_frontier_does_not_clear_any_strike` pins the phase-level rule against all three counters under the scenario's own name.

## Review round 25 — the issue #494 budget must not cancel the capability-miss cleanup (Codex P1)

`process_task` was wrapped, at the dispatch site, in
`tokio::time::timeout(workflow_task_timeout, ...)` — the issue #494
consecutive-workflow-task-timeout budget. That wrapper spanned the whole
function, so it bounded not just the workflow decision cycle but also the
issue #804 capability-miss interception that runs *after* it.

That cleanup is DB I/O: a rate-limit refund, a fleet/compatibility read, and
the release-or-escalate statement. The post-handler preflight raises its miss
inside `persist_scheduled_activities` — i.e. once the workflow body has already
returned its commands and only persisting them found an unregistered activity
or child (`CapabilityMissPhase::AfterHandler`). A workflow that returns near
its deadline therefore reaches the cleanup with very little budget left, and
when the cleanup outlives it the whole future is cancelled mid-flight:

- the release never commits, so `capability_misses` does not advance and the
  bounded-escalation guarantee (AC3) makes no progress;
- an issue #494 timeout strike is banked against an execution whose body
  demonstrably completed;
- `harvest.workflow.task_timeout` is emitted instead of
  `harvest.task.capability_miss` (AC5), and the two paths stop being
  distinguishable (AC4);
- and once `poison_pill_threshold` strikes accumulate, the execution is
  quarantined and **terminally failed** — the exact outcome AC1 exists to
  prevent, with a workflow-task-timeout reason rather than
  `no_capable_worker:`.

The mechanism was traced end to end before changing anything: the dispatch-site
wrapper, the `AfterHandler` raise site inside the enqueue preflight, the three
awaits in `handle_capability_miss`, and the `Err(_elapsed)` recovery arm that
increments the strike map and calls `quarantine_decision`.

### The fix — scope the budget to the decision cycle

The budget moves *into* `process_task` as a new `workflow_body_timeout:
Option<Duration>` parameter and wraps the workflow decision cycle alone, via
the new generic helper `run_under_workflow_body_budget`. Everything sequenced
after that call — the capability-miss release/escalation — is outside the
budget and can no longer be cancelled by it. The dispatch site now hands the
budget down instead of wrapping the call, and routes the new
`TaskDispatchOutcome::BodyTimedOut` into the recovery arm that used to be
`Err(_elapsed)`; all four of its branches map one-for-one, so the strike
accounting, the timeout metric, the permit drop and the
requeue-or-quarantine decision are unchanged.

Two deliberate details:

- **The connection acquire stays inside the budget.** `pool.get()` moved into
  the budgeted block rather than above it, so a pool stall still counts against
  the decision cycle exactly as it did when the dispatch site owned the
  timeout. This change is about *where the budget ends*, not about widening
  what it covers.
- **A cut cycle never reuses its connection.** Dropping the budgeted future
  returns the connection to the pool, precisely as cancelling the whole of
  `process_task` used to; the `BodyTimedOut` arm returns without touching it,
  because a cancelled cycle may have left a transaction open on it.

The activity arm no longer acquires a connection up front only to drop it
before the handler runs — it acquires one only on the capability-miss path it
actually needs one for, so the activity happy path now pays for no checkout at
all rather than one wasted round-trip.

### Tests

`worker::tests::cleanup_sequenced_after_the_budget_is_not_cancelled_by_it` is
the falsifying pin: it runs the helper with a short budget and a cycle that
finishes inside it, then does work that outlives the remaining budget and
asserts that work completed — alongside a **control** that wraps both halves in
one timeout and demonstrates the cleanup being cancelled, which is the defect
this scoping removes. Three sibling tests pin the rest of the helper's contract
(`None` runs unbounded, an overrunning cycle is cut, a finishing cycle is
admitted).

`capability_miss_tests::an_overrunning_decision_cycle_is_still_cut_by_the_workflow_task_budget`
is the end-to-end regression guard for *moving* a timeout: an overrunning cycle
must still be cut, still emit `harvest.workflow.task_timeout`, still quarantine
to `FAILED`, and still land a `WorkflowTaskTimeout` dead letter — with **no**
`harvest.task.capability_miss` sample anywhere, since every handler in that
fixture is registered and a pure hang is not a capability miss (AC4).

Sizing that guard surfaced a real constraint worth recording:
`effective_workflow_task_timeout` floors the budget at
`max_local_activity_start_to_close`, because a local activity runs inline
inside the cycle and the budget may never sit below the local cap. A first
attempt drove the overrun with a slow local activity and could not fail — the
60s floor swallowed the 500ms budget. The guard now sets both to 1ms and uses
no local activity at all.

A deterministic *database-level* falsification of the P1 itself is not cleanly
reachable and was deliberately not faked: hitting it requires the budget to
expire during the cleanup rather than during the cycle, and since
`prepare_workflow_task` does DB work before the handler lookup, any budget
small enough to be reliable cuts the cycle instead. The pure control test
carries the falsification; the DB test carries the regression guard for the
move. Both doc comments say exactly that.

## Review round 26 — a same-id restart onto a capable build must not still read as incapable (Codex P1)

`capability_miss_workers` stores **bare worker IDs**, and `workers::register_worker`
upserts on `worker_id` — refreshing that row's `started_at` and `build_id`. A pod
restarting under the same configured `worker_id` onto a **new build that registers
the previously-missing handler** is therefore an explicitly supported operation
that the miss evidence could not see: by ID alone, the new capable instance was
indistinguishable from the old incapable one.

The harm lands at precisely the wrong moment. Once the persisted release budget
is spent, the next incapable claimant reads the live fleet, finds every ID in it
already covered by the stale evidence, derives `AllLiveWorkersMissed`, and
terminally fails the execution — **while the pod that can actually run it is up
and polling**. That is the exact failure issue #804 exists to prevent, arriving
one claim before the fix would have landed.

**Fix.** `Worker::register_in_fleet` now clears the re-registering ID's stale
evidence immediately after a successful `register_worker`, via the new
`queue::invalidate_capability_miss_evidence_for_worker`:

```sql
UPDATE harvest_task_queue
SET capability_miss_workers = array_remove(capability_miss_workers, $1)
WHERE queue_name = ANY($2)
  AND $1 = ANY(capability_miss_workers)
```

Registration is the first thing `Worker::run` does, so the invalidation lands
before this worker's first poll — and, more importantly, before any *other*
worker's next claim can weigh the fleet.

**Why invalidation rather than keying evidence by build/generation.** The finding
offered both. Keying by `build_id` degenerates exactly where it is needed most:
`build_id` is optional and empty by default (`#171`), so the common deployment
gets no discrimination at all. Keying by a registration generation would change
the persisted array's format and thread a new composite identity through
`resolve_capability_miss`, `release_task_for_capability_miss`, and every DB
fixture — a wide change whose failure mode is a *silently mismatched* key.
Invalidation is one new statement plus one call site, changes no formats, and
fails in the safe direction: it only ever **removes** evidence, and removing
evidence can only bias the decision toward releasing rather than terminally
failing (AC1).

**What is deliberately NOT touched.** `capability_misses` is a true historical
count of releases and backs the **ungated** absolute ceiling
(`CAPABILITY_MISS_TOTAL_BUDGET_MULTIPLIER`), the one bound that still applies when
no fleet evidence is available. Decrementing it here would remove that bound.
Pinned by `registration_invalidation_never_refunds_the_release_budget`, which
asserts the statement's `SET` clause assigns exactly one column.

**Accepted trade-off, stated plainly.** A worker crash-looping on the same
*incapable* build also clears its own evidence on every restart, so the
`AllLiveWorkersMissed` **total** bound stops applying to it. That case remains
bounded — by the absolute ceiling, which this change never touches — so escalation
still happens, just later. The degradation is deliberately in the safe direction:
prefer holding a task over terminally failing an execution that a deploy could
still rescue.

Scoped by `queue_name` so a worker only invalidates evidence on queues it actually
polls; an unscoped statement would hand a fresh reprieve to a task on a queue this
worker will never claim from. Failure to invalidate is logged and does not fail
startup, matching the existing registration error handling — a warning names the
consequence (a task this ID missed on a previous build may escalate early).

**No new `WorkflowEvent` variant, no migration, no change to the adjacently-tagged
event JSON** (AC7): a single `UPDATE` against one existing `TEXT[]` column.

**Tests.** Three pure `queue.rs` unit tests pin the statement's shape — that it
uses `array_remove` on exactly the re-registering ID (never clearing the whole
set, since other workers' evidence is still true), that it never touches
`capability_misses`, and that it is scoped by `queue_name`. Two DB integration
tests in `capability_miss_tests.rs` run as a **pair**:

- `restart_onto_a_capable_build_clears_its_stale_miss_evidence` — the headline.
  `pod-a` missed the frontier on its old build (budget spent), then restarts onto
  a build that registers the handler; the task is deferred out of claim range so
  it registers **without** claiming; `pod-b` (still incapable) then claims. The
  task must return to `PENDING` with the execution still `RUNNING`.
- `stale_miss_evidence_from_a_worker_that_never_restarted_still_escalates` — the
  control. Identical fixture, except `pod-a` is seeded directly into
  `harvest_workers` and never restarts, so no registration runs and its evidence
  is still true. The run **does** escalate. Without this the headline test could
  pass on a fixture that never escalates at all; with it, the release is
  attributable to the re-registration and nothing else.

Falsifiable — verified by disabling the invalidation call: the headline test fails
first on the evidence guard (`got [["pod-a"]]`) and, with that guard relaxed,
fails on the harm itself (`the execution went terminal instead of releasing`),
while the control passes throughout.

## Review round 27 — the decision must not run on a snapshot round 26 can invalidate (Codex P1)

Round 26's `invalidate_capability_miss_evidence_for_worker` is deliberately **not**
ownership-guarded: it has to reach a row another worker is mid-dispatch on,
because that is precisely the row whose decision the stale evidence poisons. That
made it the **one writer** that can change `capability_miss_workers` between a
claim and its miss — and `frontier_miss_state` had documented, and relied on, the
opposite:

> Nothing else writes them between the claim and the miss: a concurrent
> poison-pill reclaim changes `worker_id`, which the release's own
> `worker_id = $2` guard turns into a `0 rows affected` no-op.

So round 26 fixed nothing in its own headline scenario whenever the restart lands
mid-dispatch. `pod-b` claims while the row still names `pod-a`; `pod-a` restarts
onto a capable build and clears its entry; `pod-b`, still deciding on its
claim-time snapshot, reads `pod-a` as incapable *and* the registry reports it
live, derives `AllLiveWorkersMissed`, and terminally fails the run. Same terminal
failure, one layer down.

**Fix.** The decision re-reads the counters instead of trusting the snapshot. New
`queue::read_capability_miss_state` (ownership-guarded `SELECT`, so a row a
concurrent reclaim took is never decided about) behind the new async
`worker::current_frontier_miss_state`, which resolves three cases:

- `reset_committed` — an exact in-memory fact about *this* dispatch that no read
  can improve on, so it short-circuits with **no round-trip at all**. Round 20's
  frontier scoping and round 22's mechanism are untouched.
- a successful read — authoritative. Invalidation only ever *removes* entries, so
  a fresher view can only weaken the fleet evidence, and weaker evidence can only
  bias toward releasing (AC1).
- `None` (the claim is gone) or an error — fall back to the claim-time snapshot,
  which is exactly the pre-round-27 behaviour.

That fallback is the load-bearing detail. Round 21 P2 had *removed* a re-read
precisely because its failure fallback fabricated a clean `(0, [])` frontier,
bypassing the storage-ceiling guard: a row already at `i32::MAX` would be released
and the `capability_misses + 1` in the release statement would raise `integer out
of range`, stranding a `RUNNING` row under a live worker that orphan reclamation
skips. Falling back to the snapshot has no such mode — the count is real, so the
ceiling still fires, and a read failure degrades to the status quo rather than to
something new. `frontier_miss_state` survives unchanged as that fallback, with its
now-false invariant paragraph corrected to say what actually holds.

**Window.** The read sits as late as it can without a transaction — immediately
before the decision, with only in-memory work between it and the terminal write —
so a registration can still slip past in a few microseconds rather than across the
whole decision cycle, which for an `AfterHandler` miss is the entire workflow
body. Closing it completely would mean re-reading `FOR UPDATE` inside the
escalation's own transaction, and `fail_task_and_execution` is a sequence of
statements rather than one; that restructure is not worth the risk against a
window of pure in-memory work, and the residual fails in the same direction as
before rather than a new one.

**Truthful diagnostics preserved.** `escalate_capability_miss` reported
`task.capability_misses` and `task.capability_miss_workers.len()` — the snapshot,
which after round 26 may name a worker the decision no longer counted.
`CapabilityMissResolution` now carries `misses_before`/`distinct_before`, set by
`resolve_capability_miss` from the very values it decided on, and the escalation
reports those. Round 23's contract ("every number in an escalation reason is the
one that actually acted") therefore survives the change that made the snapshot
untrustworthy, and is now true *by construction* rather than by two call sites
agreeing to read the same row — the reported numbers and the decision cannot
diverge because they are the same values.

**No new `WorkflowEvent` variant, no migration** (AC7): one read-only `SELECT` on
the miss path, and no change to the release or escalation statements.

**Tests.** Two pure `queue.rs` unit tests pin the two halves of the invariant that
now replaces the old one: `miss_state_read_is_ownership_guarded` (the re-read is
scoped to this worker's own claim and refreshes both counters together) and
`registration_invalidation_must_reach_rows_under_an_active_claim`, which asserts
the invalidation has **no** ownership guard — the reflex fix, since every sibling
writer has one, and the one that would skip exactly the row the round-26 harm
lands on.

The DB test `peer_reregistering_mid_dispatch_is_seen_by_the_decision` hits the
race **deterministically rather than by timing**: the workflow body itself
performs the invalidation, and a body runs strictly between `claim_task` and the
`AfterHandler` miss raised by `persist_scheduled_activities`. That is the real
window, hit exactly, with no sleep and no second worker to race. Falsified by
reverting `current_frontier_miss_state` to the bare snapshot: the execution lands
in `FAILED` with a `no_capable_worker:` reason instead of the task returning to
`PENDING`.

## Review round 28 — a decision needs ownership, coherence, and evidence it can trust (Codex, 4× P1)

Round 27 re-read the miss counters before deciding. Round 28 is four findings on
what that re-read did **not** establish, and they compose into one rule: an
escalation may only run on evidence that is *owned*, *readable*, and *coherent
with the fleet read beside it*.

### 1. A lost claim is not a decision to make

`read_capability_miss_state` returning `None` means the row is no longer
`RUNNING` under this worker — a poison-pill reclaim or an operator took it.
Round 27 treated that as merely "no fresh data" and fell back to the snapshot.
But the two write paths are not symmetric: the **release** is ownership-guarded
(`worker_id = $2`, whose `0 rows affected` the caller already handles) while the
**escalation** is not — `queue::fail_task` accepts any `PENDING`/`RUNNING` row
regardless of `worker_id`, and `fail_task_and_execution` transitions the
execution. So a stale dispatcher deciding `Escalate` terminally failed a task
another worker owned, which a zero-budget, session-pinned, or already-exhausted
task reaches on its very first miss.

`None` is now `CurrentMissState::ClaimLost` — positive evidence, not absence of
it — and the caller returns `Released` without deciding anything.

### 2. An unreadable row may not license an evidence-derived escalation

Round 27's error fallback restored the claim-time array, which after round 26 is
no longer known to match storage: re-registration is an unguarded concurrent
writer, so the array can still name a peer that has since invalidated its own
entry. With the fleet read seeing that peer as live, the resolver derived
`AllLiveWorkersMissed` and failed the run at the budget.

The counters still fall back to the snapshot — the round-21 P2 hazard is
unchanged, and a fabricated clean frontier would bypass the storage ceiling and
strand a `RUNNING` row on `integer out of range` — but they now arrive as
`CurrentMissState::Unreadable`, which suppresses the evidence-derived bounds.

### 3. Neither read order is safe, so the fleet read is bracketed

The fleet read and the miss read are separate statements, so under `READ
COMMITTED` a registration can commit between them — and **both** orderings lose:

- fleet first: the peer is *absent from the fleet* but its evidence is *already
  cleared*, so the claimant is the only live worker and the only misser →
  `AllLiveWorkersMissed`.
- miss first: the peer is *present in the fleet* with its evidence *still
  stale*, so it reads as covered → `AllLiveWorkersMissed` again.

Reordering trades one race for the other. So the miss state is read on **both**
sides of the fleet read and the two must agree. Registration and invalidation
commit atomically (below) and snapshots are monotonic in time, so any commit the
fleet read could have seen is also visible to the second miss read — which makes
it differ from the first. Agreement therefore certifies that no registration
landed anywhere inside the bracket, and the three reads describe one coherent
world.

### 4. A published worker whose cleanup failed is evidence against itself

Round 26 logged an invalidation failure and continued into the poll loop. That
leaves the worst of both states: the worker is published as live on its new,
possibly capable build while every task it missed on the old one still names it,
so another claimant reads the whole live fleet as covered and fails a run.

`register_worker` and the invalidation are now one transaction
(`workers::register_worker_and_clear_stale_miss_evidence`). A failed cleanup
rolls the registration back, so the worker stays unpublished rather than
published-and-stale, and the heartbeat's existing `Ok(0)` self-heal retries the
pair within one interval — which is also why that self-heal had to move onto the
same atomic composite, or it would have republished without the cleanup and
reopened the hole one heartbeat later. Atomicity is additionally what makes
finding 3's bracket sound: split the pair and a claimant can observe the peer in
the fleet with its evidence not yet cleared, which agrees across both miss reads
and escalates.

### Suppression is exactly two bounds, and stays bounded

`MissEvidenceConfidence::PossiblyStale` forces the fleet answer to
`CapablePeerMayExist` — not a fudge but the literal truth, since the distinct set
may name a peer that has re-registered onto a capable build. That single
substitution withholds precisely the two **evidence-derived** bounds (the
distinct bound, gated on `!CapablePeerMayExist`, and the configured total bound,
gated on `AllLiveWorkersMissed`) and leaves every **ungated** one in force: the
absolute `10x` ceiling, the `i32::MAX` storage ceiling, the zero-budget
fail-fast, and the session-pinned carve-out. AC3's bounded release therefore
survives suppression — a permanently-unreadable row terminates at the ceiling
rather than releasing forever.

Suppressing the *distinct* bound as well as the total one is deliberate: its
input is `capability_miss_workers`, the very array a re-registration
invalidates, so its length is precisely what goes stale.

The 6-argument `resolve_capability_miss` is now a wrapper that means
`Current`, so all 21 existing call sites keep their meaning verbatim.

**No new `WorkflowEvent` variant, no migration** (AC7): one read-only `SELECT`
added on the miss path, and two existing writes moved into one transaction.

### Tests

Five pure `worker.rs` unit tests: a lost claim yields no decidable counters
while an unreadable row still yields the snapshot (so the storage ceiling keeps
its real count); confidence requires two *agreeing owned* reads; stale evidence
withholds both evidence-derived bounds; it keeps all four ungated ones; and the
un-confidenced form still means `Current`.

Two DB tests, each falsified by reverting its own fix:

- `a_claim_lost_mid_dispatch_makes_no_terminal_decision` — budget `0`, the
  sharpest case, so the ownership probe is the *only* thing between the stolen
  row and a terminal failure. The workflow body hands its own row to another
  worker mid-dispatch, which is deterministic rather than raced: a body runs
  strictly between `claim_task` and the `AfterHandler` miss. It then waits on
  that hand-off as a positive observable rather than sleeping blind — the test
  asserts that *nothing* happens, and a lost claim deliberately records no
  metric, so the body's own effect is the last event before the decision.
  Reverting the guard fails it with `left: "FAILED", right: "RUNNING"`.
- `a_failed_evidence_cleanup_rolls_back_the_registration` — the invalidation's
  target table is renamed away on a separate connection, so the failure is
  injected deterministically and reversibly (the suite runs `--test-threads=1`).
  Reverting to log-and-continue fails it with `left: 1, right: 0` — the worker
  published while its stale evidence survived.

## Review round 29 — an escalation is revalidated at the commit boundary (Codex, P1)

Round 27 characterised the gap between the final evidence read and the terminal
write as "a few microseconds of pure in-memory work". That was wrong, and this
finding says exactly why: `fail_task_and_execution` **awaits
`store::load_history`** before it opens the failure transaction, so the window
contained a database read whose cost grows with the run's history. A capable
peer registering during that read was neither seen nor blocked, and neither
`fail_task_and_execution` nor `queue::fail_task` rechecks anything.

### The expensive read is paid first, and the type system enforces it

`fail_task_and_execution` is split into `preload_failure_history` (the awaited
read, resolved into a `PreloadedFailureHistory`) and
`fail_task_and_execution_with_history` (the writes, which **consume** it). The
public wrapper preloads and delegates, so all six existing call sites are
behaviourally identical.

Because the committing call takes the preloaded value by value, an escalation
*cannot* reach the write without having already paid the read. The ordering is
therefore a compile-time property rather than a comment that drifts — which is
the whole reason round 27's comment was able to become false.

### Then the decision is re-run one last time

With the expensive await behind it, the escalation re-reads the miss evidence
(ownership-guarded) and the live fleet, and re-runs the *same pure resolver*.
`revalidate_escalation` turns that into one rule:

- the claim is gone → **withdraw** (`claim_lost`); the escalation write is not
  ownership-guarded, so a dispatcher that lost the row must not fail it;
- the fresh answer is no longer `Escalate` → **withdraw** (`evidence_changed`);
  a peer that re-registered appears here either as a cleared miss entry or as a
  newly-live capable worker, and both flip the answer;
- otherwise → **commit**.

A withdrawn escalation falls through to the ordinary release path rather than
returning early. That is load-bearing: leaving the row `RUNNING` under a **live**
worker would strand it, because the orphan reclaimer requires a dead heartbeat.
The release arm was extracted into `release_capability_miss` so the two callers
cannot drift on the backoff, the phase-gated counter handling, or the metric.

An `Unreadable` row is judged by its fresh action rather than being withdrawn on
sight: round 28 already handles its staleness upstream by forcing
`PossiblyStale`, which withholds the two evidence-derived bounds. An escalation
that survives *that* — the absolute ceiling, the storage ceiling, a zero budget,
a session pin — is a real one, so AC3 keeps terminating.

### What is left, stated plainly

A worker that registers strictly **after** the revalidation is indistinguishable
from one that registers after the commit: any implementation evaluates the fleet
at *some* instant, and "no capable worker is live" is a statement about a moving
set. What this round removes is the part that was not inherent — an unbounded
awaited read sitting inside the window. The residual is now genuinely the commit
itself, it fails in the same direction as before, and it is reachable only with
the budget already exhausted, where the outcome is a paged, redrivable
`no_capable_worker:` failure.

**No new `WorkflowEvent` variant, no migration** (AC7).

### Tests

Four pure `worker.rs` unit tests over `revalidate_escalation`: a lost claim
withdraws even when the decision said escalate; a fresh `Release` withdraws; an
unchanged decision commits; and an `Unreadable` row is judged by its fresh action
in both directions.

The ordering property is pinned by the compiler rather than by a test — the
committing function cannot be called without the preloaded history — and the
24-test DB suite covers the refactor for behavioural regression, since every
escalation test in it now runs through the split path.

## Review round 30 — the revalidation uses the same bracket the decision does (Codex, P1)

Round 29's commit-boundary revalidation re-derived its own read sequence: one
miss read, then the fleet read. That silently dropped the two-miss-read bracket
round 28 had put in place, and reintroduced exactly the race round 28 closed —
a peer re-registering between those two awaited reads is still named in
`capability_miss_workers` while already appearing in the live fleet, which
derives `AllLiveWorkersMissed` and terminally fails a run the newly-capable
worker could serve.

The finding is right, and its root cause is duplication: the bracket existed in
one place and was re-implemented by hand in another.

### One constructor, both callers

`observe_miss_and_fleet` now performs the miss → fleet → miss sequence and
returns a `BracketedMissObservation` carrying both reads, the fleet, and the
`MissEvidenceConfidence` derived from whether the two reads agree. The
decision path and its commit-boundary revalidation both take their
`(fleet, confidence)` pair from it, so neither can drift from the other again.
The only two `current_frontier_miss_state` calls left in the crate are the two
inside that helper.

The revalidation's own rule is unchanged: a lost claim withdraws
(`claim_lost`), a fresh answer that is no longer `Escalate` withdraws
(`evidence_changed`), and a withdrawal falls through to the ordinary release
path. What changed is that its `Escalate` can no longer be reached from
mismatched snapshots — a disagreeing bracket forces `PossiblyStale`, which
withholds both evidence-derived bounds, so the fresh answer becomes `Release`
and the escalation is withdrawn.

**No new `WorkflowEvent` variant, no migration** (AC7).

### Test

`a_peer_reregistering_inside_the_commit_bracket_withdraws_the_escalation`
composes exactly what the commit boundary now does: a first read naming
`pod-a` and `pod-b`, a second read in which `pod-a` has cleared its own entry,
and a fleet in which both are live. The disagreement forces `PossiblyStale`,
the resolver withholds the evidence-derived bounds, and the escalation is
withdrawn.

It carries its own control in the same test: feeding the *same* counts through
an **agreeing** bracket escalates. Without that, the assertion would pass for
the trivial reason that the budget was never reached, and would keep passing if
the bracket stopped doing anything.

## Review round 31

Two findings on `68ba2e1`.

### P1 — make the ownership check atomic with the terminal failure

Rounds 27, 28, 29 and 30 each *narrowed* the window between a capability-miss
escalation's last ownership check and its terminal write — by re-reading the row,
by keeping the three read outcomes distinct, by hoisting the history load out of
the way, by bracketing the fleet read. None of them **closed** it, because the
check and the write remained separate database operations and
`queue::fail_task` accepts any `PENDING`/`RUNNING` row *regardless of
`worker_id`*. A poison-pill reclaim or an operator action landing in between
therefore still let a stale dispatcher terminally fail the **new** owner's task
and append `WorkflowFailed` to a run a capable worker had just picked up — the
exact failure issue #804 exists to prevent.

The check and the write are now **one transaction**, with the task row's
`FOR UPDATE` lock held from the first to the last:

- `queue::read_capability_miss_state_for_update` is the locked sibling of the
  round-27 unlocked read. A no-DB shape test asserts it differs from the
  unlocked query **only** by ` FOR UPDATE`, so the two cannot drift about what
  "still ours" means.
- `worker::commit_terminal_failure_if_still_claimed` wraps guard + write in one
  transaction and returns `TerminalWriteOutcome::{Committed, ClaimLost}`. A
  concurrent transfer either commits before the lock is taken (and is seen, so
  nothing is written) or blocks until this transaction commits (and finds a row
  that is already terminal). There is no interleaving in which the guard passes
  and the write lands on someone else's claim.
- **Lock order is preserved.** `harvest_task_queue`'s documented order is
  **execution row → task row**, so the new `lock_workflow_execution_row_only`
  takes the execution lock first. Locking the task row first would invert
  against `enforce_activity_timeout` / `finalize_activity_completion` and risk
  an ABBA deadlock on exactly the terminal path this guard protects. The new
  helper deliberately does *not* re-load history — that would put the awaited
  read round 29 hoisted out straight back into the window.
- The page-severity counter is recorded **after the guard passes and before the
  write**, via a `before_write` callback, so it still fires when the write
  itself fails transiently (the case that most needs the page) but never fires
  for a withdrawn escalation.
- A lost claim at the write is a **withdrawal**, not an error: it falls through
  to the same ownership-guarded release path a commit-boundary withdrawal uses,
  which no-ops on a row we no longer own.

The round-29 `revalidate_escalation` check is kept as the cheap early half —
it can withdraw before paying for a transaction — but it is no longer the
guarantee.

### P2 — report the *revalidated* escalation cause

`revalidate_escalation` returned only `Stands`/`Withdrawn`, discarding the fresh
resolution, so an escalation that still stood was reported from the **obsolete**
one. The two can both say escalate for *different* causes: a task past the
absolute release ceiling escalates as `BudgetExhausted` while every live worker
has missed it, and as `ReleaseCeilingExhausted` the moment a never-tried capable
peer appears during the commit. Reporting the obsolete cause tells an operator
"no capable worker on this queue registers the handler" when one demonstrably
does — sending them to look for a missing deploy that is not missing.

`EscalationRevalidation::Stands` now carries the fresh `CapabilityMissResolution`,
and the reason string, the metric label and the log line are all derived from it.
The `resolution` parameter of `escalate_capability_miss` is gone: there is now
exactly one resolution in scope at the commit, so the obsolete one cannot be
reached by accident.

Honest scope note: the metric **label** is deliberately shared by the two
budget-ish causes (both were genuinely offered around the queue), and the two
inputs that could move it — the session pin and the configured budget — are
properties of the task and the config, not of the resolution. A revalidation can
therefore never change the label; what it changes is the reason string and the
log message, and the test asserts exactly that rather than pretending otherwise.

### Tests

- `queue::tests::miss_state_commit_read_takes_the_row_lock` — no-DB shape test
  pinning `FOR UPDATE`, the ownership guard, and only-differs-by-the-lock.
- `worker::tests::the_reported_cause_is_the_revalidated_one_not_the_obsolete_one`
  — the verdict carries the fresh resolution, the two causes genuinely differ,
  the obsolete reason makes the claim the fresh evidence contradicts, and both
  keep the AC5 `no_capable_worker:` prefix.
- `a_claim_transferred_inside_the_terminal_write_is_not_failed_by_the_stale_dispatcher`
  (DB) — the race, staged deterministically with the lock-holding pattern the
  pause / child-timeout suites use: a second connection holds the task row lock,
  the terminal write queues behind it, the claim is transferred to `thief`
  inside that window, and the lock is released. **Confirmed RED** against the
  pre-fix code (`left: Committed, right: ClaimLost` — the stale dispatcher
  failed the thief's task); GREEN with the guard. Also asserts no
  `WorkflowFailed` was appended, the execution stayed `RUNNING`, and the new
  owner's row kept its claim and carries no stale reason.
- `a_claim_still_held_through_the_terminal_write_still_fails_the_task` (DB) —
  the control: same staged window, same lock contention, nobody takes the claim,
  so the escalation must still commit (AC3 boundedness) and the counter must
  fire exactly once. It **passes under the RED patch too**, which is the point:
  a guard that simply always withdrew would satisfy the race test and silently
  break AC3.

## Review round 32

Four findings on the round-31 commit boundary itself: one from Codex, three from
a concurrency review agent run against the new transaction. Three are real
correctness bugs *introduced by* round 31; the fourth is a pre-existing hazard
that round 31 made reachable.

### The event id the terminal write appends at was stale (Codex P1)

Round 29 hoisted the history load OUT of the window between an escalation's last
evidence check and its terminal write. That made the `next_event_id` it produced
older, not fresher: an activity completing in the widened window consumes the
cached id, and appending `WorkflowFailed` at a consumed id aborts the whole
terminal transaction on `UNIQUE(workflow_exec_id, event_id)`. The row is then
left `RUNNING` under a **live** incapable worker — unclaimable, and invisible to
orphan reclamation, which requires a dead heartbeat. That is a strictly worse
outcome than the escalation this code exists to perform.

`commit_terminal_failure_if_still_claimed` now re-derives the id under the
execution row's lock, via the existing `store::next_event_id_for`. That function
already takes the `FOR UPDATE` the ordering wanted anyway, so the fix *replaces*
the explicit lock rather than adding a read — and round 29's hoist survives
intact, because only the indexed `MAX(event_id)` runs inside the window, never
the full history load.

### The guard's `FOR UPDATE` was an ABBA edge against poison-pill (F1)

`poison_pill::quarantine_orphan` locks the **task** row first and the
**execution** row second (via the dead-letter FK, then `fail_owning_workflow`) —
the exact inverse of the order this commit boundary takes, and the order
`harvest_task_queue`'s own convention documents. A blocking `FOR UPDATE` on the
task row therefore let an escalating dispatcher hold the execution row while
waiting for a task row a quarantine already held while waiting for the execution
row: a genuine deadlock cycle, on precisely the pair of paths that race (the
worker being quarantined is the one whose heartbeat went stale *while it was
escalating*).

The guard is now `FOR UPDATE SKIP LOCKED`. That removes the waiting edge
entirely — this transaction never blocks on the task row, so it cannot be part
of a lock cycle. A row someone else holds simply reads as "not ours right now",
which the caller treats exactly like a lost claim: withdraw, release, re-decide
on the next redelivery. Withdrawing is always the safe direction.

### The guard matched the worker, not the claim (F3)

`poison_pill::requeue_orphan` hands a reclaimed row back to the pool with
`state = PENDING`, `worker_id = NULL` and `crash_strikes` bumped — and nothing
stops the *original* worker from winning it again. A `(state, worker_id)`-only
guard passes for that **new** claim, so a stale escalation could terminally fail
work that had just been legitimately restarted. The guard now also matches
`crash_strikes`, which is the value the requeue moves; the dispatcher's snapshot
carries the pre-requeue strike count, so a re-claim reads as a lost claim.

### A best-effort diagnostic could silently downgrade COMMIT to ROLLBACK (F5)

`persist_workflow_failure` runs its deferred unfinished-handler checks with
`let _ = ...`, which swallows the Rust error but leaves the *connection's*
transaction aborted. Before round 31 that ran in autocommit and was harmless.
Inside the new outer transaction it is not: the later COMMIT becomes a ROLLBACK
and `commit_terminal_failure_if_still_claimed` reports `Committed` for a write
that never landed. Each check now runs in its own savepoint, so an error is
contained where it is swallowed.

### Tests

- `queue::tests::commit_boundary_claim_guard_locks_without_waiting_and_keys_on_the_claim`
  — no-DB shape test pinning `FOR UPDATE`, `SKIP LOCKED`, and all three of
  `state` / `worker_id` / `crash_strikes`.
- `a_terminal_write_appends_at_the_event_id_current_under_the_lock` (DB) — the
  window staged directly: preload, then append an unrelated event at the cached
  id, then commit. **Confirmed RED** against the pre-fix code with
  `duplicate key value violates unique constraint "harvest_events_workflow_exec_id_event_id_key"`;
  GREEN with the re-derivation, and the intervening event is not clobbered.
- `a_task_row_locked_by_a_concurrent_transaction_withdraws_without_waiting` (DB)
  — the ABBA-avoidance proof, asserted directly rather than by proxy: the write
  must complete **while the lock is still held**, which a blocking guard cannot
  do. **Confirmed RED** with `Elapsed(())` against a blocking `FOR UPDATE`.
- `a_same_worker_reclaim_after_a_requeue_is_not_failed_by_the_stale_escalation`
  (DB) — bumps the strike and re-claims with the same worker id. **Confirmed
  RED** with `left: Committed, right: ClaimLost` against a strike-blind guard.
- `a_claim_transferred_before_the_terminal_write_is_not_failed_by_the_stale_dispatcher`
  (DB) — round 31's race test, restaged as a committed transfer now that the
  guard no longer blocks.
- `a_claim_still_held_through_the_terminal_write_still_fails_the_task` (DB) —
  the control, unchanged in intent: a live claim must still commit (AC3
  boundedness) and fire the counter exactly once. It passes under every RED
  patch above, which is the point — a guard that always withdrew would satisfy
  the three withdrawal tests and silently break AC3.
- All four withdrawal tests also assert the page-severity counter fired **zero**
  times, pinning that `before_write` runs only for a write that actually lands.

## Review round 33

**The evidence re-decision has to happen under the terminal lock (Codex P1).**

Round 32 made the commit boundary validate the *claim* — `state`, `worker_id`
and `crash_strikes`, all under the task row's lock. That closes ownership races,
but it is blind to the one thing the escalation actually rests on: the evidence.
A peer re-registering onto a capable build clears itself from the task's
`capability_miss_workers` array (`array_remove`) and touches **neither**
`worker_id` **nor** `crash_strikes` — so ownership is unchanged, the guard
passes, and the stale escalation commits, terminally failing a run the
newly-capable peer could serve. That is the exact AC1/AC2 outcome this whole
issue exists to prevent.

Rounds 27, 30 and 31 all re-read the evidence, but all of them read it *before*
the transaction opened. The window between that read and the lock acquisition is
where a re-registration lands.

The observe-and-re-decide half of `prepare_escalation_commit` is now extracted
into `revalidate_escalation_evidence`, and the commit boundary runs it **inside**
its transaction, after the claim guard has pinned the row. That read is the
authoritative decision; the pre-transaction one is kept purely as a cheap early
exit that avoids opening a transaction and preloading history when the evidence
has already moved. Because both halves go through the same helper, they cannot
drift on how evidence is observed or how a verdict is derived from it. A
withdrawal at this point returns the new `TerminalWriteOutcome::EvidenceChanged`
and falls through to the ordinary release arm, exactly like a lost claim.

No new lock edge: the miss state is read from the row already held, and the
fleet read is an unlocked `SELECT` on `harvest_workers`.

Honest scope. Two things this deliberately does **not** claim:

- The *reason string* is still the one derived outside the transaction, so an
  escalation that still stands here but for a subtly different cause is reported
  with the second-most-recent cause. That is a diagnostic nuance, not a
  correctness gap — the metric label provably cannot move between the two (see
  round 31's note on `EscalationCause::outcome_label`), and the only thing this
  re-check changes is *whether* the escalation stands at all.
- A peer that re-registers *after* the lock is taken blocks on it (the clear is
  an `UPDATE` of the row we hold) and applies to an already-failed row. That is
  the irreducible "became capable one microsecond too late" case, and it is
  bounded and paged rather than silent.

### Tests

- `a_peer_that_re_registers_before_the_lock_withdraws_the_escalation` (DB) —
  two live workers both named incapable, then `worker-b`'s evidence is cleared
  the way registration clears it. **Confirmed RED** with
  `a peer that became capable before the lock must veto the escalation; got Committed`;
  GREEN with the in-transaction re-check. Asserts no `WorkflowFailed`, the
  execution still `RUNNING`, the task still claimable, and the page-severity
  counter at zero.
- `evidence_that_still_supports_escalation_under_the_lock_still_commits` (DB) —
  the control: identical setup and an attached re-check, nobody becomes capable,
  so the escalation must still commit and page. It **passes under the RED
  patch**, which is the point — a re-check that always withdrew would satisfy
  the test above and silently break AC3's boundedness.
- The four round-31/32 claim-guard tests pass `None` for the re-check, so they
  keep pinning exactly the ownership contract they were written for.

## Review round 34

Two P2s on round 33's boundary, both about telling an operator the truth.

### The persisted reason came from the wrong decision (P2)

Round 33 made the boundary re-decide under the lock, but only inspected the
`Withdrawn` arm — `Stands(fresh)` was discarded, so the terminal reason string,
the metric label and the log line were all still built from the *pre-transaction*
resolution. Round 33's own changelog flagged this as an accepted "diagnostic
nuance"; Codex is right that it isn't one.

The concrete case: a task above the absolute release ceiling resolves
`BudgetExhausted` before the transaction, then a never-tried capable peer
registers before the locked re-check and the fresh resolution becomes
`ReleaseCeilingExhausted`. Both escalate, so a `Stands`-only check sees nothing
change — but the two produce *opposite* reason strings. The older one states
"no live worker on this queue has the handler"; the fresh one states "a live
worker on this queue never missed this task, so it may well have the handler and
simply lost every claim race". Paging someone toward a missing deploy that does
not exist is exactly the failure mode round 23 fixed for the other causes.

The boundary now derives the reason from whichever resolution it decided on,
through a caller-supplied deriver, and `TerminalWriteOutcome::Committed` carries
that resolution so the caller's log and metric label follow it too. Both the
pre-transaction and in-transaction paths go through the same `cause_of` /
`reason_of` closures, so the three can never tell different stories about one
escalation. The `None`-recheck path (the claim-guard tests) keeps using the
supplied string, unchanged.

### The log overstated the durable distinct-worker count by one (P2)

`distinct_incapable_workers` logged `resolution.distinct_after`, the
post-increment count that includes the escalating claimant. But this branch never
runs the release `UPDATE`, so that claimant is never appended to
`capability_miss_workers` — the durable record is `distinct_before`. The field
therefore overstated the persisted evidence by one and disagreed with both its
own sibling `completed_releases` (already the persisted count, from round 8) and
the reason string (which correctly uses `distinct_before`). Now
`distinct_before`, so the log, the reason and the row agree.

### Tests

- `the_persisted_reason_comes_from_the_in_transaction_resolution` (DB) — a
  deliberately distinguishable stale reason is passed in and a different one is
  derived; the row must carry the derived one. **Confirmed RED** with
  `left: Some("no_capable_worker: decided-before-the-lock")` /
  `right: Some("no_capable_worker: derived-under-the-lock")`. Also asserts the
  counter callback receives the fresh resolution and that `Committed` carries it.
- The `distinct_before` fix is a consistency change to a tracing field; it is
  covered by the reason-string assertions that already pin the persisted counts,
  and by the field now matching the sibling it always should have.

## Review round 35

Three P2s, two of which are a correction to round 34.

### Round 34 fixed the wrong log line, and broke the right one

Round 34's `distinct_incapable_workers` change landed on the **release** branch,
not the escalation branch. The two have **opposite** correct answers, so getting
it backwards broke both directions at once:

- The release branch *does* run the release `UPDATE` that appends this claimant
  to `capability_miss_workers`, so the post-claim count **is** the durable set —
  and matches `capability_misses`, reported post-update on the same line. Round
  34 flipped it to the pre-claim count, so it began under-reporting by one.
- The escalation branch never runs that `UPDATE`, so the claimant is never
  appended and the persisted set is the pre-claim count. Round 34 left it on the
  post-claim count, so the original over-report by one was never fixed.

The misplaced comment was the tell: round 34's justification ("this branch never
runs the release UPDATE") ended up attached to the branch that does.

Both sites now go through `reported_distinct_workers(&resolution, released)`.
The two fields also have different Rust types (`i32` vs `usize`) and `tracing`
accepts either, which is a large part of why the swap compiled silently; the
helper normalises to one type, so the choice is a deliberate argument rather
than an incidental field name.

### Registration rewrote terminal history on the startup path

`invalidate_capability_miss_evidence_for_worker` matched every row on the queue
whose miss array named the re-registering worker — including completed and
failed ones. `complete_task` does not clear `capability_miss_workers`, so a queue
with real history accumulates terminal rows still naming past missers, and
rewriting them is pure cost: a terminal task is never redelivered and never
escalated, so its array cannot influence any decision.

The cost lands in the worst place. This runs in the same transaction as
`register_worker`, so the worker is not published to the fleet until it
finishes, and the `queue_name` indexes are partial to pending rows — so a busy
queue's startup pays an unindexed scan and bloats the historical table. The
update is now bounded to `PENDING`/`RUNNING`, the rows the evidence can actually
affect.

### Tests

- `worker::tests::the_two_branches_report_opposite_distinct_worker_bases` — the
  pin that would have caught round 34. A tracing field is invisible to every
  other test in the suite, so the choice is routed through one helper and
  asserted directly, including that the fixture keeps the two counts distinct so
  the test cannot pass vacuously. **Confirmed RED** with the round-34 mistake
  re-applied: `left: 2` / `right: 3`.
- `queue::tests::registration_invalidates_only_that_workers_miss_evidence` gains
  a `state IN ('PENDING', 'RUNNING')` assertion. **Confirmed RED** with the
  filter removed.

## Review round 36

**Codex P2 — the release logged a cardinality its own write never produced.**
Round 35 justified reporting `resolution.distinct_after` on the release branch
with: "the release `UPDATE` ran and appended this claimant, so the post-claim
count *is* the durable set." That justification only holds if nothing touched
`capability_miss_workers` between the read and the write — and something can.

`observe_miss_and_fleet` snapshots the array, then a peer restarting onto a
capable build runs `invalidate_capability_miss_evidence_for_worker`, which
`array_remove`s its own id from exactly these `RUNNING` rows (that is the
round-26 fix, correctly scoped by round 35 to live rows). The release `UPDATE`
then appends the claimant to the *post-invalidation* array, so the snapshot
over-reports: `{A,B}` + claimant `C` logs as 3 while the row actually holds
`{B,C}`.

The fix makes the number true by construction rather than by a timing
assumption: all three phase-selected release statements now carry
`RETURNING COALESCE(array_length(capability_miss_workers, 1), 0) AS
distinct_miss_workers`, and `release_task_for_capability_miss` returns
`Option<i32>` — `None` for a claim already taken (the previous `false`),
`Some(n)` for the cardinality *this statement committed*. The release log
reports that value.

`reported_distinct_workers` keeps its round-35 role as the single place the
choice is made, with the release arm now taking the durable value rather than a
snapshot field:

- `Some(durable)` — the release branch, straight from the `UPDATE`.
- `None` — the escalation branch, which never runs that `UPDATE`, so the
  claimant was never appended and the pre-claim `distinct_before` is correct
  (Codex round-8 P2, unchanged).

RED, on the no-DB SQL-shape pin:

```
the release must return its own post-update cardinality -- a value derived from
the caller's earlier snapshot silently over-reports when a peer's
re-registration invalidated an entry in between
```

RED, on the DB test, with a mutant reproducing the bug's exact symptom
(reporting one more than durable):

```
assertion `left == right` failed: the release must report the cardinality it
committed (["stale-b", "incapable"]), not the snapshot-derived 3 -- a peer
invalidated an entry in between
  left: Some(3)
 right: Some(2)
```

`the_release_reports_the_cardinality_it_actually_committed` reproduces the
interleaving in order — snapshot `{stale-a, stale-b}`, invalidate `stale-a`,
then release as `incapable` — and asserts the durable set is `{stale-b,
incapable}` and the reported count is 2, not the snapshot-derived 3. The
`the_two_branches_report_opposite_distinct_worker_bases` fixture was widened to
two invalidations so the durable count coincides with neither snapshot field;
with a single invalidation it equals `distinct_before` numerically and the
assertions could not discriminate.

## Review round 37

**Codex P1 — the release was guarded on the claim's *worker*, not its *claim*.**
`release_task_for_capability_miss` matched `WHERE id = $1 AND state = 'RUNNING'
AND worker_id = $2`. `poison_pill::requeue_orphan` hands an orphaned row back as
`PENDING` / `worker_id = NULL` with `crash_strikes + 1`, and nothing stops the
*same* worker from winning it again — so that guard also matches the **new**
claim. A stale dispatcher's release would then re-`PENDING` a row whose
replacement handler is already running, inviting a second concurrent claim of
the same task (duplicate side effects), and rolling back an `attempt` belonging
to the new dispatch.

This is the same hole round 32 closed on the terminal-write path, which is why
`claim_still_held_for_update_query` already keys on `crash_strikes`. The release
is the far more common path and did not get the same treatment. All three
phase-selected release statements now carry `AND crash_strikes = $4`, bound from
the dispatcher's claim-time snapshot (`task.crash_strikes`).

`crash_strikes` is the right discriminator because the requeue that creates the
race is precisely what bumps it. An audit of every writer confirms it is stable
within a claim: `poison_pill` is the only incrementer and only acts on orphans
(no live heartbeat), in which case the claim genuinely *is* gone and refusing
the release is the correct outcome; the only other writer is the `AfterHandler`
release's own `crash_strikes = 0`. A refused release is already handled by the
existing `None` arm — the worker logs, returns `Released` without counting the
metric, and the reclaimer re-pends the row — so the failure mode is self-healing
rather than a stuck task.

RED, on the no-DB SQL-shape pin:

```
a poison-pill requeue lets the SAME worker re-claim the row, so `worker_id`
alone does not identify the claim this dispatcher holds -- releasing a live
replacement claim risks a concurrent second dispatch
```

RED, on the DB test, with the guard neutralised:

```
the strike bump means this is a NEW claim -- matching worker_id alone would
re-PENDING a row whose replacement handler is already running
```

`a_same_worker_reclaim_after_a_requeue_is_not_released_by_the_stale_dispatcher`
is the release-path twin of the round-32 terminal-path test, and additionally
asserts the replacement claim's `attempt` and the redelivery budget are both
untouched.

**Note on `mixed_fleet_completes_every_execution_with_zero_spurious_failures`.**
One full-suite run failed this test while the machine was loaded (92.8 s, versus
35.8–63.7 s for three subsequent clean runs). It did not reproduce in isolation
or across those reruns. The guard's refusal path was audited rather than
assumed: a mid-claim `crash_strikes` bump can only come from a poison-pill
reclaim, which means the claim is genuinely gone, and the reclaimer re-pends the
row — so a refused release yields a correct, self-healing outcome, not a stuck
task.

## Review round 38

**Codex P1 — `Unavailable` evidence permitted the distinct bound even when the
registry was naming an untried live peer.**

`fleet_capability_evidence` detects `Unavailable` *self-referentially*: the
claimant is missing from the live set, so the registry "is not describing this
fleet". But that check says nothing about whether the **rest** of the set is
readable. A worker whose startup registration failed, or whose heartbeats went
stale while it kept polling, produces `Unavailable` from a registry that is
otherwise perfectly readable — and that registry may be listing a live peer
which has never missed this task. The distinct bound then fired after
`budget + 1` distinct missers and terminally failed the task while that peer was
live and polling: the round-8 P1 failure mode, reached through the `Unavailable`
door.

Round 15 deliberately allows the distinct bound on `Unavailable`, on the grounds
that `budget + 1` distinct missers is strong evidence *on its own*, independent
of the registry. That reasoning is sound when the registry tells us nothing, and
breaks precisely here, where the registry is actively naming an untried peer.

So rather than requiring `AllLiveWorkersMissed` outright — which would undo
round 15's choice for the genuinely-unreadable case — the untried-peer check now
runs **first**: any live worker that has never missed this task yields
`CapablePeerMayExist` regardless of whether the claimant appears in the set. A
registry naming nobody untried still resolves `Unavailable`, so round 15's bound
keeps exactly the case it was written for.

This is the narrower of the two remedies Codex offered ("or at least suppress
escalation whenever the returned fleet contains an unaccounted peer"), and it
also reconciles the code with the distinct bound's own doc comment, which
already claimed the bound is withheld "even when coverage is unreachable".

RED:

```
assertion `left == right` failed: a live peer that never missed this task may be
capable; the claimant's own absence from the registry does not make that peer
disappear
  left: Unavailable
 right: CapablePeerMayExist
```

`an_untried_live_peer_withholds_the_bound_even_if_the_claimant_is_missing`
asserts the evidence classification, the resulting `Release` decision, **and**
the round-15 control (an empty registry still resolves `Unavailable` and still
escalates on the distinct bound) — so an always-withhold mutant cannot satisfy
the fix while silently unbounding the small-fleet case.

## Review round 39

Fixed a Codex P2 — the startup capability-miss invalidation was a full scan of
the queue backlog, and it runs where that costs the most.

`invalidate_capability_miss_evidence_for_worker` executes in the **same
transaction as `register_worker`**, so a worker is not published to the fleet
until it finishes. Every pre-existing `harvest_task_queue` index narrows only by
queue/state, and `$1 = ANY(capability_miss_workers)` is not an indexable
predicate on its own, so Postgres had to inspect every `PENDING`/`RUNNING` row on
each advertised queue. On a queue with a large backlog a rolling restart
therefore serialised expensive scans and delayed the very workers meant to
resolve the capability misses — the #804 remediation got *slower* exactly when
it was needed most.

Measured on a 200k-row backlog with 20 matching rows:

| | plan | shared buffers | time |
|---|---|---|---|
| before | `Seq Scan` | 3847 | 84 ms |
| after | `Index Scan` | 9 | 1.7 ms |

**A partial B-tree, not the GIN index review suggested.** Capability misses are
exceptional, so the discriminating predicate is "has this row recorded a miss at
all?" — not the array's *contents*. Gating the index on a non-empty array
confines it to the miss population: **16 KB against a 45 MB, 300k-row table**,
and a normally enqueued row (empty array) never enters it, so the hottest write
path in the system pays nothing. A GIN index over `capability_miss_workers`
would instead index every `PENDING`/`RUNNING` row and tax every enqueue for no
additional selectivity. `queue_name` leads because the statement narrows by
`queue_name = ANY($2)`; the membership test is then a recheck over a handful of
rows.

The query gains `AND capability_miss_workers <> '{}'`, which is **load-bearing,
not redundant**: it is the predicate the index is partial on, and Postgres's
implication prover cannot derive it from the membership test. Dropping it
silently returns the statement to the full scan (`Seq Scan` / 3848 buffers /
83 ms on the same fixture). It is correctness-neutral — a row that recorded no
miss cannot name `$1` — so it never changes which rows are updated.

Migration `20260721000000_harvest_capability_miss_worker_index` is index-only:
no column, no table, no data change. It carries the `CREATE INDEX CONCURRENTLY`
form in a comment for live deployments, since that cannot run inside Diesel's
migration transaction. The upgrade guide gained a matching inventory row.

Tests (`enable_seqscan = off` makes both assertions independent of table size
and planner cost thresholds — Postgres falls back to a sequential scan only when
no index *can* serve a predicate, so "still a `Seq Scan`" means precisely "no
index is able to serve this query"):

* `the_startup_invalidation_is_index_servable_not_a_backlog_scan` — RED before
  the fix with the exact defect in the plan.
* `dropping_the_empty_array_guard_returns_the_invalidation_to_a_full_scan` —
  pins the conjunct as load-bearing so a future "simplify the WHERE clause"
  refactor fails loudly, and acts as the control: without it, an
  always-index-scan mutant (an unconditional index over the whole table) would
  satisfy the first assertion while hiding that the query shape is what makes a
  cheap partial index usable.

## Review round 40 — CI flake root-caused (test sizing, not a product defect)

`mixed_fleet_completes_every_execution_with_zero_spurious_failures` failed once
locally during round 37 and once on CI at round 39, both times as a bare
`reached no terminal state within 60s`. Two occurrences is a signal, so rather
than re-run until green I derived the failure rate from the feature's own
backoff curve.

The claim query has **no capability filter** — that is issue #804's premise — so
on every redelivery the incapable and the capable worker race 50/50 for the
task. A run of consecutive incapable claims is therefore a normal outcome, not a
fault. Each miss defers the task by `capability_miss_backoff`: 1s, 2s, 4s, 8s,
16s, then capped at 30s, so the cumulative deferral after `k` misses is
`31 + 30(k - 5)` seconds for `k >= 5`.

| misses | cumulative | P(a given task) | P(any of the 8) |
|--------|-----------|-----------------|-----------------|
| 6      | 61 s      | 1.56 %          | **11.8 %**      |
| 10     | 181 s     | 0.098 %         | 0.78 %          |
| 14     | 301 s     | 0.0061 %        | **0.049 %**     |

At the previous 60s bound the test crossed its own timeout at **six** misses —
roughly **one run in eight**, which matches the observed rate exactly (once in
~17 local runs, once on CI). CI latency on a contended runner stacks on top of
the backoff, which is why it surfaced there.

This is a **test-sizing defect, not a product defect**. The engine is doing
precisely what #804 specifies — release rather than fail, with dwell capped at
30s — and the property the test asserts (`COMPLETED`, zero spurious `FAILED`) is
untouched. The harness bound was simply mis-sized against the feature's own
backoff.

Two changes:

* `MIXED_FLEET_TERMINAL_WAIT` is now a named constant of **300s**, sized from
  the table above (14 tolerated misses, ~1-in-2000) with the arithmetic recorded
  on the constant so it is not "tidied" back down. This costs nothing in the
  common case: the wait returns the moment the row goes terminal, so a healthy
  run still finishes in seconds, and only a genuine stall spends the budget.
* `wait_for_terminal` now dumps the execution state and every task row —
  `state`, `worker_id`, `attempt`, `capability_misses`, `capability_miss_workers`,
  `crash_strikes`, `scheduled_at`, `error` — on timeout. A bare timeout cannot
  distinguish a backoff tail (`PENDING`, future `scheduled_at`, high
  `capability_misses`) from a stranded claim (`RUNNING`, live worker — a real
  defect), and telling those apart previously cost a round of log archaeology.
  The dump was verified by forcing the timeout path with a 1ms bound.

A 12-run local reproduction loop passed 12/12, which is consistent with (and
too small to distinguish) a ~12 % rate; the arithmetic above, not the loop, is
what establishes the cause.

## Review round 41

Two P2 findings, both documentation-vs-behavior mismatches rather than product
defects.

**1. The sustained-release alert's presence guard was a minute short of the hold
it promised.** `harvest_capability_miss_release_sustained` asserted the release
rate was continuously non-zero over `[15m:1m]` and required `>= 15` steps. But
Prometheus aligns a subquery's steps to absolute multiples of the resolution, so
`expr[Rm:1m]` yields `R` **or** `R + 1` points depending on whether the
evaluation instant lands on a minute boundary. Fifteen points one minute apart
span only **fourteen** minutes, so the rule could open its ticket a minute before
the hold it documents.

The literal repair — tightening to `>= 16` on the same 15m window — is strictly
worse than the bug: it demands the aligned case, so a rule group evaluating off a
minute boundary counts 15 and the alert can **never fire at all**. Widening the
window fixes both directions instead: `[16m:1m]` yields at least 16 points at
every alignment, and 16 points one minute apart span a full 15 minutes. The rule
now reads `[16m:1m]` in both halves with `>= 16`, and its `default_threshold`
states the guarantee in those terms.

The pin test that failed on this change turned out to hardcode `[15m:` in an
older, *structural* assertion ("the hold lives in the expression, as a
`min_over_time` over a subquery"). That assertion is now window-agnostic — it
pins the `:1m]` subquery step, which is what distinguishes a subquery from a
plain range vector — and the window **length** is owned solely by the new
alignment assertions, so the two no longer duplicate (or contradict) each other. The added prose
pushed the pin past the 100-line clippy bound, so it was split along the
`Half 1 / Half 2` seam its own doc comment already described — which outcomes
may page, versus whether the released-outcome rule holds — with the shared rule
accessor hoisted to a module-level helper. They are independent properties over
different rules, so a failure in one no longer masks the other.

**2. `capability_miss_max_redeliveries` misattributed the small-fleet bound.**
The rustdoc claimed the ungated `10 ×` absolute ceiling "terminates what the
gated bound cannot: a fleet smaller than the budget". That was true when written
but stopped being true at review round 15, which added the configured-total
bound precisely so a *registered* small fleet escalates at `N` rather than after
50 releases. The doc now describes all three bounds accurately: the distinct
count, the configured total (gated on fleet-covering evidence, and why it must
be gated on that specifically), and the ceiling — whose remaining job is the two
cases neither gated bound can reach, a fleet the registry cannot describe and a
live worker that never claims.

## Review round 42

One P2, **declined on the evidence** — with the invariant it questions now pinned
by a test rather than left to reasoning.

The finding read the unconditional rate-limit refund at the top of
`handle_capability_miss` as an over-credit: a stale dispatcher resuming after
`poison_pill::reclaim_orphaned_tasks` re-pended its task would "refund the
replacement claim's token", letting an extra activity through and violating the
configured rate limit. The suggested repair was to gate the refund on still
owning the claim.

Tokens are fungible, so there is no "replacement claim's token" to refund — the
only meaningful question is the **balance**: outstanding debits must equal the
activities that ran or are running. Traced against the code, with `A` the stale
dispatcher and `B` the replacement:

| step | bucket | running |
|------|--------|---------|
| `A` claims (`claim_task` debits) | `T-1` | 0 |
| orphan reclaim re-pends — no refund | `T-1` | 0 |
| `B` claims (debits) and runs | `T-2` | 1 |
| `A` resumes and refunds | `T-1` | 1 |

`T-1` for one running activity is correct. The refund does not take capacity
from `B`; it returns the debit `A` **stranded**, because `requeue_orphan`
rewrites state/`worker_id`/`started_at`/`crash_strikes` and never touches the
bucket. Gating the refund on ownership would stop at `T-2` — a permanent leak on
a `refill_rate = 0` bucket, and precisely the direction that starves the capable
peer the release exists to hand the task to.

Three facts were verified rather than assumed, since the conclusion depends on
all of them:

* **`requeue_orphan` does not refund.** Its `UPDATE` covers state, `worker_id`,
  `started_at` and `crash_strikes` only.
* **This is the only site that returns a claim-time debit.** The other worker
  refund is guarded on `circuit_token.is_some() && activity.circuit_breaker.is_some()`
  — a dispatch-time reservation for breaker activities, a disjoint token — and
  the remaining call sites are the `start-throttle:` bucket family (#607). No
  retry, timeout or cancel path refunds, so one debit cannot be credited twice.
* **A capability miss always had a claim-time debit.** The miss fires exactly
  when `registry.activities.get(name)` is `None`, and the breaker set that makes
  `claim_task` skip the debit is built from that *same* map — so a worker
  missing the handler is never breaker-tracked, never skips the debit, and
  returns before the dispatch-time reservation. The refund can therefore never
  fire without a matching debit.

New DB test `stale_dispatcher_refund_leaves_one_debit_for_the_live_claim` drives
the interleaving above deterministically through `queue::claim_task` (no worker
poll-loop races) against a `burst 2 / refill 0` bucket, and asserts **both**
failure directions: `2.0` would be the over-credit the finding describes, `0.0`
the leak its suggested fix would introduce. Confirmed falsifiable — applying the
ownership gate as a mutant fails the test at `0.0`. The reasoning is also
recorded on `refund_capability_miss_rate_limit_token`, which is now
`#[doc(hidden)] pub` so the test drives the real function rather than a copy.

**Also fixed, surfaced by adding that test:** the round-39 index probes were not
actually size-independent. `explain_plan` issued `SET LOCAL enable_seqscan = off`
as a bare statement on an autocommit connection, so it was scoped to its own
one-statement transaction and was already gone by the time `EXPLAIN` ran on the
next. The probes were therefore measuring the planner's ordinary cost choice,
which on a small table is a `Seq Scan` whether or not an index exists — they
passed only on incidental row counts left by whichever tests ran before them,
and the new test perturbed exactly that. Wrapped both statements in an explicit
`BEGIN`/`ROLLBACK` so the setting survives to the `EXPLAIN`; both probes now pass
against a pristine empty database, which the positive one could not have done
before.

## Review round 43

One P2, accepted in part — the *repair* chosen is the second of the two the
finding offered, and for a reason worth recording.

`EscalationCause::outcome_label` maps both the budget bounds and the ungated
absolute release ceiling to `outcome="escalated"`. That mapping is deliberate
(round 23): executions are being failed either way, and under-paging is the
worse error for the outage #804 exists to surface. But the two do not license
the same *conclusion*. The gated bounds fire only once the registry confirms the
recorded missers cover the live fleet; the ceiling fires precisely where that
coverage could **not** be established, so a live, never-tried peer may still be
capable. The paging rule's prose asserted the fleet reading for the whole
outcome — "the task was bounced around the queue … and still found no capable
worker", and a `first_action` that sent on-call straight to
`workflow-types/reachability` — so a ceiling escalation pointed triage at a
conclusion that does not follow.

The finding offered two repairs: split the ceiling into its own outcome, or make
the alert cause-neutral. **Cause-neutral was chosen.** Splitting would add a
fourth value to a bounded metric label and, unless the paging rule were widened
to select both, would stop the ceiling paging at all — reversing round 23's
deliberate "under-paging is worse" call for a case documented as requiring
`10 × budget` consecutive lost claim races across ~25 minutes of backoff. The
defect is in the prose asserting a conclusion the label never carried, not in
the label.

So the rule now names the discriminator instead of guessing: the description
directs the reader to the `no_capable_worker:` reason string **before** drawing
any fleet conclusion and states plainly that a ceiling escalation "does NOT mean
the queue was swept"; `default_threshold` justifies paging by *executions are
being failed* rather than by a fleet conclusion; and `first_action` branches —
reachability/workers for a coverage-confirmed escalation, the named workers
first for a ceiling trip. The runbook's escalation-cause table already gave the
two rows different fixes, so only its row 2a needed correcting.

**Same stale claim, third copy.** Row 2a and the alert description both still
credited the ceiling with "a fleet smaller than the budget" — the round-15
regression round 41 corrected in `builder.rs`. A *registered* small fleet
escalates at `N` via the configured-total bound (row 1a, `D ≤ N`); the ceiling
covers unprovable coverage. Both are corrected, and the new pin
`capability_miss_paging_rule_prose_is_cause_neutral` fails if the phrase returns
to any of the three prose fields — so the fourth copy cannot be written silently.
The pin also asserts the discriminator is named first and the ceiling's weaker
conclusion is stated; confirmed falsifiable against the pre-fix pack.

The changelog's earlier sections are left as written: they record what was true
at the round they describe, and rewriting them would falsify the history that
rounds 41 and 43 exist to correct.

## Review round 44

**Cause-neutral `escalated` metric-label docs.** Round 43 fixed the alert pack
and the runbook, but stopped at the surfaces harvest ships pre-written. The
`CAPABILITY_MISS_OUTCOME_ESCALATED` constant in `telemetry.rs` — the surface a
consumer reads when writing their *own* alert on `harvest.task.capability_miss`
— still called itself "the *fleet-exhaustion* signal, and the only escalation
cause that supports the conclusion 'no live worker on this queue registers the
handler'", and asserted "no capable worker ever claimed it".

Both claims are false for half of what the label records.
`EscalationCause::outcome_label` maps `BudgetExhausted` **and**
`ReleaseCeilingExhausted` to `escalated`, and since round 15 the ceiling is
reachable only on the two evidence states the configured-total bound withholds
itself from — `CapablePeerMayExist` (a live worker that has never been offered
the task, and may well be capable) and `Unavailable` (the registry could not be
read). A ceiling sample is precisely the case where coverage was *not*
established, so a consumer following these docs would page on it and then be
contradicted by `GET /admin/workflow-types/reachability`.

The docs now name both bounds, state that the ceiling case does not show the
queue was swept, explain why both are still recorded under one value
(under-paging is the worse error when executions are being failed either way),
and hand the reader the discriminator: the `no_capable_worker:` reason string
on the failed execution, which names the bound that actually tripped.

**Pinned, and paired.** `capability_miss_escalated_label_docs_are_cause_neutral`
reads the rustdoc block directly above the constant — walking backward from the
declaration, so it needs no line numbers and survives reformatting — and fails
if the label re-acquires the fleet-exhaustion assertion, drops the ceiling, stops
pointing at the reason string, or re-acquires the round-15 "fleet smaller than
the budget" claim. It sits beside round 43's
`capability_miss_paging_rule_prose_is_cause_neutral` deliberately: the two guard
the same false conclusion on two surfaces, so a future edit that repairs one and
leaves the other still fails. All four assertions were confirmed red against the
pre-fix text.

**Last copy of the imprecise clause.** `EscalationCause::outcome_label`'s own
rustdoc attributed the ceiling's `Unavailable` case to "a fleet smaller than the
budget". Under `Unavailable` the fleet's size is exactly what cannot be known —
the bound is on the distinct *observed* incapable workers — so the clause now
says that, with the reason it can say nothing stronger.

## Review round 45

**P1: a cross-type continue-as-new whose target handler is missing must
release, not seal the predecessor.** Issue #803's `ctx.continue_as_new_as`
resolves the **target** type's handler on the worker running the transition, and
#803 shipped an unregistered target as a terminal failure of the predecessor.
That is exactly the condition #804 exists for: during a rolling deploy the old
pod runs the source phase while only the new peer registers the target phase, so
a long-lived entity that happens to transition mid-deploy was killed by whichever
worker claimed it first — blameless, transient, and fixable by a peer.

**Why the existing pre-pass could not catch it.** `first_persist_capability_miss`
scans the cycle's commands, but `drive_workflow` `swap_remove`s the
`ContinueAsNew` command out of the batch and returns it as the *outcome*, so the
scan sees an empty list. `WorkflowCommand::ContinueAsNew` sat in the pre-pass's
"persists without resolving a handler" arm, which was true before #803 and
stopped being true when cross-type continuation landed.

New pure `continue_as_new_target_capability_miss` reads the outcome instead and
is `or_else`-chained onto the same gate, so it runs at the same point — ahead of
`record_workflow_completed`, the terminal telemetry, and the history-cap
round-trips — and returns `HandlerNotRegistered { Workflow, AfterHandler }`. The
phase is `AfterHandler` for the reason the phase exists: the body ran to a
conclusion and only persisting its decision found an unregistered type.

**Only the unregistered case.** `classify_continue_as_new_target` rejects a
target for five other reasons and none is about *this* worker, so none may
release: a blank target (a config error, and an empty name would reach the
`capability_miss` labels as a phantom workflow); a registered **DAG** target
(excluded by construction — a DAG's shadow `WorkflowInfo` lives in
`registry.workflows`, so `contains_key` already returns true and it falls
through to #803's terminal "trigger it with `POST /dags/{name}/trigger`"); and
cross-shard routing, an unrepresentable successor deadline, or a
`(target, workflow_id)` slot already held — all fleet-invariant, so releasing
could only burn the budget and then escalate with a `no_capable_worker:` reason
that misdescribes the fault. Each is pinned.

`check_continue_as_new_type`'s own unregistered arm is kept as the fail-closed
backstop for the narrow window where the target is *deregistered* between the
gate and the call, and its rustdoc now says so.

**Money test.** `cross_type_continue_as_new_missing_target_is_released_for_a_capable_peer`
drives a real two-phase deploy: a worker registering only the source phase
releases the task (`PENDING`, no owner, no crash strike), the predecessor stays
`RUNNING` with a null `error` and neither a `WorkflowFailed` nor a
`WorkflowContinuedAsNew` event, and a peer registering **both** phases then
completes the transition to `CONTINUED_AS_NEW`. Phase 2 is load-bearing twice:
it shows phase 1 was a release rather than a stall, and a check that released
unconditionally would pass phase 1 and hang here. Confirmed falsifiable — with
the gate wiring neutered the test fails, the predecessor sealed exactly as #803
shipped it.

Documentation follows the behavior: CLAUDE.md's #803 rollout-ordering note now
separates the releasable case from the three that stay terminal, while keeping
the ordering requirement (a target that never reaches any live worker still
escalates).

## Review round 46 — capability-miss evidence is keyed to the handler it is about

`capability_misses` / `capability_miss_workers` have always described **one
frontier** — the position a workflow is stuck at, and therefore the single
handler a claiming worker must register to move it. Two paths retired a frontier
explicitly (a park, round 20; inline local-activity progress, round 20/22), and
both reset the counters. A third retires one with neither:
`prepare_workflow_task_with_cache` ingests a due timer fire, a pending signal, or
an external delta **before** the capability gate, so the very next replay can be
stuck on a different handler with nothing having reset anything.

The row recorded counts and worker ids but never *whose* they were, so the new
frontier inherited the old one's spend. After a budget was exhausted looking for
`X`, the first worker to miss `Y` could produce `AllLiveWorkersMissed` and
terminally fail the execution — while a worker that missed `X` may well register
`Y` and was never asked. That is the exact inversion #804 exists to prevent, one
step removed: not "a capable peer lost the claim race" but "a capable peer was
never asked the question".

**The fix is a key, not another reset.** A new nullable
`harvest_task_queue.capability_miss_handler` records the `{kind}:{name}` the
counters are evidence about (`MissingHandler::frontier_key`; `kind` is included
because the two registries are separate namespaces, so a same-named activity and
workflow are different frontiers). The comparison lives in **SQL**, on both
sides:

- `read_capability_miss_state_query` returns `0` / `'{}'` when
  `capability_miss_handler IS DISTINCT FROM $3`, so the authoritative read is
  keyed **atomically** — the row cannot change frontier between deciding which
  counters apply and reading them.
- All three release-statement variants restart at `1` / `ARRAY[$2]` on a
  mismatch and stamp the new key, so a frontier's budget begins where it is
  first asked.

`IS DISTINCT FROM` rather than `<>`: a `NULL` key (nothing recorded yet, and
every row written before this change) must read as a mismatch — a full budget —
and a plain inequality yields `NULL`, which is neither arm of the `CASE`. That
direction is the safe one, and it is pinned.

Because a stale key with zeroed counters is inert (a match increments `0 → 1`, a
mismatch resets to `1` — the same row either way), the ~6 existing reset paths
needed no edit at all. `frontier_miss_state` applies the identical rule to the
claim-time snapshot it falls back to, so the fallback cannot disagree with the
authoritative read.

**AC3 (bounded) still holds per task.** Keying moves the absolute ceiling from
per-task to per-frontier, so the bound is worth restating: a frontier is a pure
function of recorded history, so two dispatches with no new events land on the
same handler and cannot oscillate. The frontier moves only when history grows,
and every such event is appended once and consumed once — so the number of
frontiers one task can present is bounded by the history hard cap (the same
argument `reset_capability_misses_after_inline_progress` already makes), and no
single frontier can release forever. Documented at `frontier_miss_state` and in
the runbook's "Sizing the budget" section.

**Money test.**
`evidence_recorded_against_another_handler_does_not_spend_this_frontiers_budget`
drives the real dispatch path: a task carrying a fully-spent two-worker budget
recorded against `workflow:a_frontier_we_moved_past` is claimed by a single
incapable worker configured with a budget of **1**. It releases rather than
escalating, re-keys the row to the frontier it actually missed, restarts the
count at `1` with a fresh worker set, leaves the execution `RUNNING` with no
`WorkflowFailed`, and touches no crash strike. Confirmed falsifiable at both
layers — with the Rust check and the read-query `CASE` neutered it escalates and
the test fails in 30 s.

Three pure tests pin the rule without a database (mismatch zeroes, a `NULL` key
zeroes, the two namespaces do not share a budget), and three SQL-shape tests pin
both arms of the release statement and all three properties of the read query,
so neither can be half-removed. The DB suite's `seed_prior_missing_workers` now
takes the workflow its misses are about, which is what keeps the eight
escalation tests it feeds honest: seeding evidence without its handler would
read as a fresh budget and every escalation they assert would silently become a
release.

## Review round 47 — two documentation-vs-behavior corrections

**(a) The escalation prose still made the unconditional fleet claim.** Round 43
corrected the alert rule and round 44 the metric-label constant, but the alerts
runbook's *overview* paragraph and the dashboard panel description both still
said `outcome="escalated"` means "no live worker on that queue registers the
handler at all". That is true only for the `BudgetExhausted` bound with
confirmed coverage. The absolute release ceiling fires under
`CapablePeerMayExist` / `Unavailable` — precisely when a live, untried worker
may well be capable — so the unconditional form sends on-call toward a
missing-deploy investigation when the actual next step is to check that worker.

Both now state the outcome (executions are being failed) and direct the reader
to the `no_capable_worker:` reason string before drawing any fleet conclusion,
pointing at the cause table that tells the two bounds apart. The `never_offered`
section's contrast sentence was corrected the same way — it asserted the budget
bound is "real evidence" unconditionally, where only the confirmed-coverage case
is.

**(b) The effective-config field named a DLQ destination that does not exist.**
`capability_miss_max_redeliveries`'s rustdoc described escalation as reaching the
"terminal-failure / DLQ path". It does not: `escalate_capability_miss` routes
through `fail_task_and_execution_with_history`, which fails the task and the
execution without inserting a `harvest_dead_letters` row — the property the DB
test `capability_miss_escalates_after_the_budget_with_no_capable_worker` has
asserted since the first round ("escalation must not dead-letter; the reason
lives on the execution row"). An operator consulting `GET /admin/config` while
diagnosing an exhausted budget would have been sent to an empty recovery
surface. The field now rules the DLQ out **positively** rather than merely
omitting it, because the neighbouring poison-pill knob really does quarantine
(#367) and silence would invite the reader to assume this one behaves the same.

**Anti-drift.** This is the third round in which the same cause-neutrality claim
survived in a surface the previous round did not touch, so both fixes are pinned
across their whole operator-reachable surface rather than at the site that
happened to be flagged: `capability_miss_escalation_prose_is_cause_neutral`
checks the runbook *and* the dashboard (an on-call engineer reads whichever one
their tooling put in front of them), and requires each to name the reason string
as the discriminator — silence would let a reader who never reaches the cause
table keep the old default conclusion.
`capability_miss_escalation_is_never_documented_as_dead_lettering` bans the
DLQ-as-*destination* constructions rather than the token, since the corrected doc
legitimately mentions the DLQ in order to rule it out and a bare substring check
cannot tell a negation from a claim. Both were confirmed falsifiable by
reverting each doc to its pre-fix wording and watching the corresponding pin
fail.

## Review round 48

**P2, accepted: the docs described a liveness window the fleet lookup stopped
using at round 19.** The capability-miss fleet query does not share the
poison-pill reclaimer's staleness window. Round 19 gave it its own —
`capability_miss_fleet_stale_secs`, which is `2 × worker_heartbeat_interval`
floored at `CAPABILITY_MISS_MIN_FLEET_STALE_SECS` (120 s) — precisely because
the two subsystems ask different questions: the reclaimer judges rows it owns
with a window it chose, while this query judges *peers*, whose heartbeat cadence
nothing in `harvest_workers` records, so a fast-heartbeating claimant applying
its own window would declare a healthy peer on the default cadence dead. Three
operator-reachable sites still described them as the same value: the
`capability_miss_max_redeliveries` field doc, the safe-deploy runbook, and the
alerts runbook. At the default 5 s cadence that is a 12× error — 10 s claimed
against 120 s actual — so an operator predicting when the fleet-covering bound
becomes available after a pod dies gets it wrong by nearly two minutes.

Corrected at all three, each stating the floor, the reason for the divergence,
and the timing consequence: for up to 120 s a dead pod's row still reads as *a
capable peer may exist*, and in that interval **only** the fleet-covering bound
is withheld — the distinct-worker bound and the absolute ceiling still fire, so
AC3 holds and the delay costs redeliveries rather than the run. Also noted that
tuning `worker_heartbeat_interval` below 60 s does not shorten it, since the
floor dominates.

A fourth site was added rather than corrected: the `worker_heartbeat_interval`
field doc was not *wrong* (it describes the management API's own classification,
which is the bare `2 ×` value) but it is the knob an operator turns, and it said
nothing about the subsystems that derive different windows from it. It now names
all three and which of them lowering the interval actually speeds up.

Codex additionally named `docs/alerts/starter-pack-v0.1.0.json`. Checked and
declined: its capability-miss rules never state a window at all. The two
"two heartbeat windows" phrases in that file belong to the preflight and
build-routing alerts, which are correct for their own subsystems.

Pinned by `capability_miss_fleet_window_is_never_documented_as_the_poison_pill_window`,
which reads all three surfaces, bans the *sameness* construction rather than the
window value (a correct doc may name `2 × worker_heartbeat_interval` in order to
say the capability lookup is not that), and requires each to state the floor.
Prose assertions run over whitespace-squeezed text so a reflow cannot silently
disarm them. Confirmed falsifiable at each surface independently — reverting any
one fails the pin and names that surface.

### Round 48, second finding: the correction was itself wrong

Verifying the three doc fixes against the decision code — rather than against
the rustdoc they were derived from — turned up a bug in the fix. The new prose
said `CapablePeerMayExist` "withholds only the fleet-covering bound — the
distinct-worker bound and the absolute ceiling still fire". That is false, and
it came from the round-19 rustdoc on `CAPABILITY_MISS_MIN_FLEET_STALE_SECS`,
which had carried the same wrong claim since it was written.

`capability_miss_decision` gates the distinct bound on `!CapablePeerMayExist`
and the configured-total bound on `AllLiveWorkersMissed` specifically, so a live
untried peer withholds **both** evidence-derived bounds. What remains is the
*ungated* set: the absolute `10 ×` release ceiling, the `i32::MAX` storage
ceiling, the zero-budget fail-fast, and the session-pinned carve-out. The
practical difference is an order of magnitude — an operator told the distinct
bound still fires expects escalation after a handful of distinct incapable
workers, when in fact nothing terminates the task until the ceiling.

Two rustdocs in `worker.rs` disagreed about this: the one on
`CAPABILITY_MISS_MIN_FLEET_STALE_SECS` was wrong, while the one on
`resolve_capability_miss_with_confidence` stated it correctly ("withholds
exactly the two evidence-derived bounds"). The inline comments inside
`capability_miss_decision` are also correct. Only the const's doc had drifted,
and it was the one this round happened to read.

Corrected in all four places — the root rustdoc plus the three docs this round
had just written it into — and pinned by
`capable_peer_may_exist_is_never_documented_as_withholding_only_one_bound`,
which bans the wrong construction across `worker.rs`, `builder.rs` and both
runbooks. A single-source rustdoc is evidently not a safe thing to propagate
from; the pin is what makes the four surfaces agree by construction.

## Review round 49

**A failed startup registration was retried only when the worker's row was
absent, so a reused `worker_id` never healed.**

`Worker::run` registers through the atomic register+invalidate pair, and a
failure rolls both back (round 28) — leaving the worker unpublished *and* its
stale capability-miss evidence intact. `do_heartbeat_tick` is meant to heal
that, and does: `heartbeat_worker` returns `Ok(0)` when the row is missing, and
that arm re-registers.

But `Ok(0)` only fires when the row is genuinely absent. A **reused**
`worker_id` — a configured stable id such as a pod name or hostname, not the
random UUID default — leaves the PREVIOUS instance's row alive. The heartbeat
updates that row, returns `Ok(1)`, and the re-registration arm is never reached.
The worker then polls indefinitely while the registry advertises the *old*
build's `build_id` and queues, and its id stays in `capability_miss_workers`.

That is affirmative fleet evidence against itself. A peer reading the live fleet
derives `AllLiveWorkersMissed` and terminally fails a task this worker can
actually run — the precise outcome issue #804 exists to prevent, arriving at the
moment the capable build did.

Fixed by tracking the failure rather than inferring it: `Worker` carries a
`registration_pending` flag set when startup's pair rolls back, and
`do_heartbeat_tick` retries the pair *before* the heartbeat can mask the
absence. `register_worker_and_clear_stale_miss_evidence` upserts, so the retry
is correct whether or not a row survives; the flag clears only on success, so a
still-failing database is retried next tick rather than given up on. A worker
that registered cleanly never enters the branch and its tick is unchanged.

`do_heartbeat_tick` is now `#[doc(hidden)] pub` so the regression test drives
the real tick: the defect is in this function's arm selection, so a test that
reimplemented the arms would prove nothing. The test's control leg runs the
identical tick with the flag clear — exactly the pre-fix path — and asserts the
stale state survives, so it fails against the unfixed function rather than
passing vacuously (verified: `left: "build-old-804r49"`, `right:
"build-new-804r49"`).

Making the function public also surfaced a pre-existing attribute misplacement:
`spawn_worker_heartbeat`'s doc comment, `#[must_use]` and
`#[allow(clippy::implicit_hasher)]` had drifted onto `do_heartbeat_tick`, which
was harmless while it was private but emitted an `unused_must_use` warning at
every new call site. Moved back to the function they describe.

## Review round 50

Three P1s, all against round 49's own fix. Each was verified in code before
being accepted.

**(a) A failed retry must not refresh the unverified row.** Round 49's `Err`
arm logged and fell through to `heartbeat_worker` — which, with a reused
`worker_id`, *succeeds*, because the previous instance's row is still there. So
a retry that failed again republished the old build's `build_id` and queues as
live while this id's stale capability-miss evidence was still uncleared. That is
affirmative false fleet evidence: exactly what produces `AllLiveWorkersMissed`
and a terminal failure of a task this worker can run. The tick now returns
without heartbeating, so the unverified row ages out of the liveness window
until the atomic pair succeeds — publishing nothing is more honest than
republishing something known-false.

The trade-off is real and is recorded at the site rather than glossed: a stale
row also makes this worker's in-flight rows look orphaned to the poison-pill
reclaimer (#367), which re-queues them, and remote-drain detection is skipped
for that tick. Both are recoverable — at-least-once is the documented activity
contract, and a worker that cannot register is already invisible to the fleet —
whereas the false `AllLiveWorkersMissed` is a terminal `WorkflowFailed` needing
operator action. Issue #804's own stated preference ("prefer holding a task over
terminally failing an execution") settles the ordering.

**(b) The pending flag was worker-wide, but registration is per shard.**
`run_multi_shard` registers against every shard pool and spawns one heartbeat
per shard, all sharing one `Arc<AtomicBool>`. A shard whose registration
succeeded would clear the flag before a *failed* shard's heartbeat ever read it,
leaving that shard advertising the old build forever.

Fixed structurally rather than by keying a map: `register_in_fleet` now
**returns** whether the pair is pending, and each `spawn_heartbeat_task` is
handed its own pool's answer. There is no shared cell left to share, so the bug
cannot recur by construction. The `Worker::registration_pending` field is gone.

**(c) A failed `pool.get()` never armed the retry at all.** That arm attempts
nothing, so under a reused `worker_id` the surviving row keeps advertising the
old build and keeps carrying the stale evidence — the same end state, reached
by a path round 49 did not cover. It now returns `true` like the transaction
failure does.

Tests: `a_failed_retry_does_not_refresh_the_unverified_row` (DB; injects the
failure by renaming `harvest_task_queue` out from under the invalidation, as
round 28's atomicity test does, and asserts `last_heartbeat_at` is byte-identical
— verified RED against the fall-through), plus
`registration_arms_the_retry_when_the_connection_cannot_be_acquired` and
`registration_pending_is_answered_per_pool`, which drive `register_in_fleet`
against pools pointed at a dead port.

## Review round 51

**Not heartbeating was necessary but not sufficient — the surviving row had to
be withdrawn, not merely left to expire.**

Round 50 stopped refreshing a row whose registration could not be verified, on
the reasoning that publishing nothing beats republishing something known-false.
True, but incomplete: the capability-miss fleet window is **floored at 120 s**
(round 19), and for that entire window the surviving row is still read as a
*live* worker — one that appears in `capability_miss_workers`, and therefore one
that has *already missed*. That is precisely the shape a peer needs to derive
`AllLiveWorkersMissed`, so the budget can still be exhausted and the run still
terminally failed while the replacement is up and capable.

The distinction that matters: a live row carrying stale evidence is
*affirmative false evidence*, while an **absent** row is neutral — the worker is
simply not part of the fleet read, which is the truth while its registration is
unverified. So the tick now withdraws the row outright rather than waiting for
it to age out.

The withdrawal is deliberately a single-table `DELETE` on `harvest_workers`, so
it can still succeed when the register+invalidate transaction cannot — that
transaction's `harvest_task_queue` half is the usual failure. It is best-effort:
if it fails too, round 50's ageing-out path remains the fallback and the next
tick retries both. `register_worker` re-inserts on the first successful retry.

**Gating the poll loop was the other suggestion, and was declined.** Tracing the
evidence logic shows it would not prevent the escalation: `AllLiveWorkersMissed`
is decided over the *other* live workers, and a worker absent from the fleet
read is neutral either way. Gating would only stop this — possibly capable —
worker from rescuing the task, which is the opposite of what issue #804 is for,
and would take an entire worker offline on a transient startup blip.

Pinned by the round-50 test, strengthened from "was not refreshed" to
"was withdrawn": it asserts the row is present before the tick and absent after,
which also proves the heartbeat never ran (`heartbeat_worker` refreshes a
surviving row, it never removes one). Verified RED against a neutered withdrawal
(`left: 1`, `right: 0`).
