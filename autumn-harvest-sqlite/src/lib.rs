//! Embedded, single-writer **`SQLite`** backend for `autumn-harvest` — an
//! edge / local-first workflow runtime.
//!
//! The valuable, hard part of `autumn-harvest` — the deterministic replay engine
//! (`run_workflow`, [`WorkflowEvent`](autumn_harvest::WorkflowEvent), the history
//! matcher, `WorkflowContext`) — is already backend-neutral: it consumes plain
//! values (an `ExecutionId`, a `Vec<WorkflowEvent>`, a handler `fn`, a JSON
//! input) and needs no database connection or trait object. This crate reuses
//! that core **wholesale** and reimplements only *persistence* — the event store
//! ([`store`]), the task queue + durable timers ([`queue`]), and the worker pass
//! ([`worker`]) — on embedded `SQLite` via `rusqlite` (`bundled`, so no system
//! `SQLite` and no Docker are required). It depends on `autumn-harvest` with
//! `default-features = false`, so no Diesel/Postgres is pulled in.
//!
//! A history produced by this backend is byte-identical, **per event**, to one
//! the Postgres backend would write (both serialize `WorkflowEvent` in the same
//! adjacently-tagged `#[serde(tag = "type", content = "data")]` form), so it
//! replays unchanged on the core `WorkflowReplayer` — the property the
//! cross-backend tests assert.
//!
//! # The single-writer / single-server contract
//!
//! This backend assumes **one writer process** against **one** `SQLite` file:
//!
//! - **`BEGIN IMMEDIATE` replaces `SELECT … FOR UPDATE SKIP LOCKED`.** A task
//!   claim takes `SQLite`'s database-level write lock up front, selects the oldest
//!   ready task, and flips it to `RUNNING`. Under the single-writer assumption
//!   this is exactly-once by construction — no two claimers race.
//! - **Polling replaces `LISTEN`/`NOTIFY`.** `SQLite` has no push notification, so
//!   the driver drains all ready work, re-runs the workflow, and repeats until a
//!   run is terminal or blocked ([`SqliteRuntime::run_until_blocked`] /
//!   [`SqliteRuntime::run_until_idle`]).
//! - **Restart = drop the runtime + re-open the file.** All state is in the
//!   database. [`SqliteRuntime::open`] reclaims any task stranded `RUNNING` by a
//!   crash (there is one server, so a `RUNNING` row at startup is an orphan) and
//!   the workflow resumes purely by deterministic replay. This makes activity
//!   execution **at-least-once**: a crash *after a body runs but before its result
//!   commits* re-runs the body on resume — write activity bodies to be idempotent.
//! - **Durable timers use the real wall clock, at millisecond precision.** A
//!   timer's absolute deadline is stored at arm time (`now + duration`) as an
//!   epoch-MILLISECOND (issue #1069 P2), so a fractional-second arm never fires
//!   before its true deadline (a `duration = 1s` timer armed at `…000.900` fires
//!   at `…001.900`, not the floored `…001.001`). On any pass, timers whose deadline
//!   has passed fire. There is no virtual clock to persist or reset, so timers are
//!   naturally monotonic across restarts. The public drivers read
//!   [`chrono::Utc::now`] **once per decision cycle** (issue #1069 P2), so a timer
//!   armed after a real-time activity anchors to its true arm instant rather than
//!   the start of the driver call; the `*_as_of` variants inject a single as-of
//!   time for deterministic, sleep-free tests.
//!   Timers that share the same `fire_at` (e.g. `tokio::join!(ctx.timer("a", 1),
//!   ctx.timer("b", 1))`) fire in `(fire_at, arm_seq)` order, so their `TimerFired`
//!   events land in `TimerStarted`-append order — the core matcher requires it to
//!   replay (issue #1069 P2).
//! - **Declared/per-call retry policies are honored IN FULL.** The typed
//!   `ctx.execute_activity(&info, ...)` / `execute_activity_with_opts` path carries
//!   the activity's resolved [`RetryPolicy`](autumn_harvest::policy::RetryPolicy) in
//!   the scheduling command's `retry_policy_override`; the backend persists the WHOLE
//!   policy (`retry_policy_json` on the task row) and the worker honors all of it
//!   (issue #1069 P2): `max_attempts` in preference to the registered
//!   [`ActivitySpec`] cap; **backoff timing** — a retryable failure is requeued at
//!   `failure_now + compute_retry_delay(initial_interval, backoff_coefficient,
//!   max_interval, attempt)` (the shared core helper), so the workflow BLOCKS on the
//!   backing-off activity until the driver advances the clock past the deadline
//!   rather than busy-retrying; and **non-retryable classification** — a typed
//!   `non_retryable` failure OR one matching the policy's `non_retryable_errors` list
//!   (incl. legacy `Err(String)`, via the core's `is_non_retryable` rule) fails
//!   terminally on the first attempt regardless of `max_attempts`, so a non-retryable
//!   error never repeats side effects. The backoff anchors to `failure_now` — the
//!   instant the body actually returned, re-read from the clock seam at *finalize*
//!   time — NOT the pre-body cycle-start `now` (Codex #1069 P2, `worker.rs:328`), so a
//!   body that ran for real time before failing schedules its next attempt `delay`
//!   after the failure, matching the Postgres path (which computes the delay AFTER
//!   handling the result). A **zero** computed delay is an IMMEDIATE retry requeued at
//!   the cycle-start `now`, so it re-claims in the same drain pass and the retry
//!   sequence converges in one call — this covers both the raw `execute_activity_raw`
//!   path (no override, no policy non-retryable list) AND a zero-interval policy like
//!   `RetryPolicy::fixed(n, 0ms)`; only a POSITIVE backoff anchors to `failure_now`.
//!   **Retry jitter is an accepted v0.1 simplification** — a `RetryPolicy` with
//!   `JitterPolicy::Full`/`Equal`/`Decorrelated` is retried at the deterministic base
//!   delay (`compute_retry_delay`), not the seeded jitter helper. Jitter exists to
//!   de-synchronize a FLEET of concurrent workers; this backend is single-writer /
//!   single-server (no fleet to de-synchronize), and retry timing (`run_at`) is not
//!   recorded in `harvest_events`, so the base delay is behaviorally equivalent with
//!   ZERO replay / byte-identity impact (Codex #1069 P2, `worker.rs:325`).
//! - **Activity `start_to_close` timeouts are honored.** The declared/per-call
//!   `start_to_close` (from `#[activity(start_to_close = "…")]` /
//!   `execute_activity_with_opts`) flows through the scheduling command's
//!   `start_to_close_override`; the backend persists it (milliseconds) on the task
//!   row and the worker enforces it as a **post-execution outcome** (issue #1069 P2):
//!   a synchronous body cannot be cancelled mid-flight in a single-writer runtime, so
//!   a body whose real wall-clock runtime exceeds its budget records a **terminal**
//!   `ActivityTimedOut { StartToClose }` (the workflow observes
//!   [`HarvestError::Timeout`](autumn_harvest::HarvestError::Timeout) and drives its
//!   own timeout branch) instead of `ActivityCompleted` — byte-equivalent to the
//!   durable event the Postgres timeout scanner records. The recorded history is
//!   byte-equivalent even though the wall-clock behavior (the body runs to completion
//!   before the timeout is recorded) differs, and it is replay-safe: the outcome is
//!   recorded once and replay never re-runs the body. `None` = no budget (unbounded,
//!   the prior behavior for every un-timed activity).
//! - **Typed workflow failures preserve their metadata.** A `#[workflow]` returning a
//!   typed [`WorkflowFailure`](autumn_harvest::failure::WorkflowFailure) (issue #767)
//!   is decoded from its `harvest_workflow_failure_v1` wire envelope before the
//!   terminal `WorkflowFailed` event is appended, so the event carries the typed
//!   `error_type`/`details` and the execution `error` column holds the human message
//!   — byte-equivalent to the Postgres terminal path (issue #1069 P2). A legacy
//!   `Err(String)` is unchanged (all typed fields `None`).
//! - **Typed ACTIVITY failures preserve their metadata too.** A registered activity
//!   returning an encoded [`ActivityFailure`](autumn_harvest::failure::ActivityFailure)
//!   (issue #227) is decoded (`failure::parse_error_payload_full`) before the terminal
//!   `ActivityFailed` event is appended, so the event carries the typed
//!   `error_type`/`details`/`non_retryable` and the human `message` (never the raw
//!   `harvest_activity_failure_v1` envelope) — byte-equivalent to the Postgres worker's
//!   `finalize_activity_failure` (issue #1069 P2). A legacy `Err(String)` decodes to
//!   `error_type = "Error"`, `non_retryable = false`, `details = None`, unchanged.
//! - **Payload-size caps are enforced at every boundary, matching the core** (issue
//!   #252). A caller-supplied payload that exceeds its byte cap is refused before it
//!   can enter `harvest_events`, so this backend never stores an unbounded history
//!   row or accepts a payload the Postgres path rejects. The **workflow START input**
//!   (`start_workflow`) and an **inbound signal payload** (`send_signal`) over their
//!   caps (`DEFAULT_MAX_WORKFLOW_INPUT_BYTES` = 2 MiB /
//!   `DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES` = 256 KiB) are rejected at ingress with
//!   [`SqliteError::PayloadTooLarge`] BEFORE anything is persisted (the signal cap is
//!   checked AFTER the not-found / terminal-target rejections). An oversized
//!   **activity RESULT** (over `DEFAULT_MAX_ACTIVITY_RESULT_BYTES` = 2 MiB) is
//!   normalized — exactly as the Postgres worker's `handle_activity_result` does —
//!   into a NON-RETRYABLE `PayloadTooLarge` `ActivityFailed` (the workflow observes
//!   the failure and drives its own error branch) rather than stored as an unbounded
//!   `ActivityCompleted`. The **activity INPUT** (schedule time), **side-effect
//!   value**, and **continue-as-new input** caps are enforced by the reused core
//!   [`WorkflowContext`] itself (the executor threads the same global caps), so the
//!   only boundaries this backend enforces directly are the two ingress rejections
//!   and the activity-result normalization. All caps use the GLOBAL default constants
//!   (this backend has no per-workflow / per-activity cap override); a `0` cap means
//!   uncapped, matching the core convention. Child-workflow input and external-signal
//!   payloads have no cap surface because those primitives are
//!   [`SqliteError::Unsupported`] here.
//!
//! # Crash-model bound (accepted non-goal)
//!
//! The only residual crash window is **mid activity body** — a crash while a body
//! is executing leaves the task `RUNNING` and is recovered by the orphan reclaim
//! on the next [`open`](SqliteRuntime::open) (re-running the body, at-least-once).
//! A single-process design has no heartbeat-timeout / poison-pill reclaimer for a
//! *live* peer, by design; multi-writer / multi-server recovery is out of scope.
//!
//! # Usage sketch
//!
//! ```no_run
//! use autumn_harvest::prelude::*;
//! use autumn_harvest_sqlite::SqliteRuntime;
//!
//! #[workflow]
//! async fn greet(ctx: &WorkflowContext, name: String) -> Result<String, String> {
//!     let greeting: String = ctx
//!         .execute_activity(&say_hello_info(), name)
//!         .await
//!         .map_err(|e| e.to_string())?;
//!     Ok(greeting)
//! }
//!
//! // The `#[activity]` supplies the macro-generated `say_hello_info()` (name +
//! // any declared `#[activity(...)]` defaults). This backend runs a caller-supplied
//! // SYNCHRONOUS body — the async fn body below is a placeholder for the shared
//! // `ActivityInfo`; the closure registered against it is what actually runs.
//! #[activity]
//! async fn say_hello(_ctx: &ActivityContext, name: String) -> Result<String, String> {
//!     Ok(format!("hello {name}"))
//! }
//!
//! # async fn run() -> Result<(), autumn_harvest_sqlite::SqliteError> {
//! let mut rt = SqliteRuntime::open("workflows.db")?;
//! rt.register_workflow(&greet_info());
//! // The audited, `ActivityInfo`-driven registration: any declared `#[activity(...)]`
//! // default is HONORED (e.g. `schedule_to_close`) or REJECTED LOUDLY at setup, never
//! // silently dropped. Use `register_activity_raw(name, ActivitySpec::new(..))` for a
//! // hand-made body that carries no declared defaults.
//! rt.register_activity(&say_hello_info(), |input| {
//!     let name: String = serde_json::from_value(input).unwrap_or_default();
//!     Ok(serde_json::json!(format!("hello {name}")))
//! });
//!
//! let exec = rt.start_workflow("greet", serde_json::json!("world"))?;
//! rt.run_until_blocked(exec).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Non-goals (see issue #1068 follow-ups)
//!
//! Out of scope for this backend: distributed / multi-writer workers, `LISTEN`/
//! `NOTIFY` push wake-ups, multi-server crash recovery, schedules, the management
//! API, retention, worker sessions, sharding, and DAGs.
//!
//! ## Unsupported workflow primitives — rejected LOUDLY, by name
//!
//! Only the *fire-once* durable timer `ctx.timer(...)` is a supported timer. Every
//! other out-of-subset primitive a workflow can reach surfaces as
//! [`SqliteError::Unsupported`] whose message NAMES the specific
//! `WorkflowCommand` (never an opaque `"Unknown"`, Codex #1069 P2) — a workflow
//! author gets an actionable failure, not a silent gap. The full v0.1 non-goal set,
//! and the command each emits:
//!
//! - **Child workflows** — `ctx.spawn_child_workflow(...)` (`StartChildWorkflow`),
//!   `ctx.spawn_child_workflow_detached(...)` (`SpawnDetachedChildWorkflow`).
//! - **External signals / cancels** — `ctx.signal_external_workflow(...)`
//!   (`SignalExternalWorkflow`), `ctx.request_cancel_external_workflow(...)`
//!   (`RequestCancelExternalWorkflow`).
//! - **Local activities** — `ctx.execute_local_activity(...)` (`RunLocalActivity`).
//! - **External activities** — task-token activities (`ScheduleExternalActivity`).
//! - **Updates** — admitted-update results (`RecordUpdateResult`).
//! - **Search attributes** — `ctx.upsert_search_attributes(...)`
//!   (`UpsertSearchAttributes`).
//! - **`continue_as_new`** (`ContinueAsNew`, surfaced from the terminal path).
//! - **Worker sessions** — `ctx.create_session(...)` (issue #606; a session dispatch
//!   carries the `session_id` / `schedule_to_start_override` `ScheduleActivity`
//!   fields this single-writer backend cannot honor).
//! - **Cancellable durable timers** — `ctx.start_timer(...)` and
//!   `TimerHandle::await_fire`/`cancel`/`reset` (issue #768;
//!   `ArmTimer`/`CancelTimer`). Use the fire-once `ctx.timer(...)` instead. These
//!   were previously the opaque `Unsupported("Unknown")` (absent from the
//!   command-name table); they are now named with a message pointing at the
//!   supported alternative (Codex #1069 P2, `runtime.rs:840`).
//!
//! The command-name table (`runtime.rs`) is EXHAUSTIVE — a future core
//! `WorkflowCommand` variant is a compile error in this backend, forcing an
//! explicit supported / unsupported decision rather than a silent `"Unknown"`.
//!
//! Two benign bookkeeping commands are instead silently **no-ops** (they append no
//! `WorkflowEvent` and gate no control flow): `ctx.set_current_details(...)` (the
//! operator status breadcrumb, issue #593) and a re-park `WaitForActivity`.
//!
//! **No `ScheduleActivity` field is silently dropped** (issue #1069 P2). Every
//! author-declared field on the core `ScheduleActivity` command is either HONORED
//! (`activity_id`/`name`/`input`; `queue` — persisted, though routing is a
//! single-process no-op; `retry_policy_override` → the WHOLE `RetryPolicy`
//! (`max_attempts` + backoff timing + non-retryable classification);
//! `start_to_close_override`, above) or REJECTED LOUDLY with
//! [`SqliteError::Unsupported`] (`session_id`/`session_worker_id` and the
//! session-acquire-only `schedule_to_start_override`, all outside the subset) —
//! never silently ignored, so a workflow's declared setting can never take
//! silently-wrong effect.
//!
//! ## Unsupported `WorkflowInfo`-level features — rejected at registration
//!
//! [`SqliteRuntime::register_workflow`](crate::SqliteRuntime::register_workflow)
//! retains ONLY the workflow's `name` + `handler`. Every OTHER `WorkflowInfo` field
//! (populated by `#[workflow(...)]` attributes or the fluent builder) would
//! otherwise be **silently dropped**. To close that class the SAME way the command
//! layer already closes its own (above), `register_workflow` audits the incoming
//! [`WorkflowInfo`](autumn_harvest::WorkflowInfo) and **PANICS at registration**
//! (setup-time — mirroring the Postgres `HarvestPlugin::build()` panic on
//! registration misconfiguration) if it declares an execution- or
//! admission-affecting feature this v0.1 backend cannot honor, naming the specific
//! feature and pointing at the supported alternative (Codex #1069 P2,
//! `runtime.rs:602` `execution_timeout`, `:686` `retry_policy`). The partition:
//!
//! - **REJECTED (would change lifecycle / admission / retry semantics if ignored):**
//!   - `execution_timeout` (#243) — no execution-timeout scanner exists here, so a
//!     declared hard deadline would never fire. Enforce a deadline inside the
//!     workflow body with `ctx.timer(...)` / `ctx.sleep_until(...)` instead.
//!   - workflow-level `retry_policy` — `#[workflow(retry(...))]` (#523) — no
//!     workflow-level auto-retry orchestration here. Use per-activity retry
//!     (`#[activity(retry = ...)]`, honored in full) or handle failure in the body.
//!   - `concurrency` (#247), `debounce` (#499), `batch` (#518), `throttle` (#607) —
//!     pre-start admission-gate features with no single-writer analog.
//!   - a raised `max_input_bytes` (#252) — a per-workflow raised input cap is not
//!     threaded into this backend's replay caps, so the global default would apply
//!     and silently REJECT an input the workflow declared acceptable; rejected so
//!     the divergence surfaces at setup, not as a surprising runtime `PayloadTooLarge`.
//! - **ACCEPTED, inert (pure metadata / observability — execution never consults
//!   them here, so ignoring them is harmless, NOT silently-wrong):** `sla` (#487 —
//!   no scanner, so no `harvest.workflow.sla_breached` metric; the run's lifecycle
//!   is unaffected either way), `owner` / `runbook_url` / `severity` (#372),
//!   `description` + `input_schema` / `output_schema` / `error_schema` (#373 —
//!   HTTP-edge discovery/validation, and this backend has no HTTP edge), `mcp` (#597
//!   — HTTP-edge MCP tool exposure), and `module` (diagnostics).
//! - **HONORED:** `name` (the registration key) and `handler` (the dispatch fn).
//!
//! Honoring the rejected features (execution-timeout enforcement, workflow-level
//! retry orchestration, the admission gates) is real, out-of-v0.1-scope work tracked
//! as issue #1068 follow-ups — hence rejection rather than a partial, silently-wrong
//! implementation.
//!
//! ## Unsupported `ActivityInfo`-level features — honored or rejected at registration
//!
//! This closes the **third** author-declared-feature ingress surface (Codex #1069 P2,
//! `runtime.rs:39`), after the `ScheduleActivity` command fields and the
//! `WorkflowInfo` fields above. A `#[activity(...)]` can declare *dispatch-time
//! defaults the Postgres worker enforces from `ActivityInfo` but which are NOT copied
//! into the `ScheduleActivity` command* — so a name-only body registration never saw
//! them, and they were neither honored nor rejected. The audited, `ActivityInfo`-driven
//! [`register_activity`](crate::SqliteRuntime::register_activity) (the analog of
//! `register_workflow`) closes that gap: it **PANICS at registration** for an
//! unsupported field, naming it and pointing at the alternative. (The escape hatch
//! [`register_activity_raw`](crate::SqliteRuntime::register_activity_raw) carries no
//! `ActivityInfo` and so runs no audit — the analog of `execute_activity_raw`.) The
//! partition:
//!
//! - **HONORED:**
//!   - `default_schedule_to_close` (#378) — the total, cross-retry wall-clock deadline.
//!     Resolved by name from the registry at dispatch (exactly as the Postgres worker
//!     resolves `ActivityInfo` from its `HandlerRegistry`), persisted as an ABSOLUTE
//!     deadline on the task row, and enforced by the worker: a task drained at/after
//!     it, or a retry whose next attempt would land at/after it, records a terminal
//!     `ActivityTimedOut { ScheduleToClose }` instead of running/retrying —
//!     byte-equivalent to the Postgres `schedule_to_close_deadline_exceeded` path.
//!   - `default_retry_policy` — its `max_attempts` becomes the registered fallback
//!     attempt cap; the WHOLE policy also reaches the worker per-dispatch via the
//!     command's `retry_policy_override` (backoff timing + non-retryable classification).
//!   - `default_start_to_close` — honored via the command's `start_to_close_override`
//!     (a per-attempt post-execution `ActivityTimedOut { StartToClose }`).
//!   - `default_queue` — recorded on the task row (single-writer routing is a no-op).
//! - **REJECTED (a dispatch-admission / cross-worker semantic with no single-writer
//!   analog, or a cap raiser that would otherwise silently apply the STRICTER global
//!   cap):** `default_heartbeat_timeout` (no heartbeating in the inline drain — use
//!   `start_to_close` / `schedule_to_close`), `default_schedule_to_start` (no queue-wait
//!   scanner), `circuit_breaker` (#369), `rate_limit_*` (#332), `max_concurrent` (#247),
//!   `requires` (capability routing), `is_local` (local-activity `RunLocalActivity`
//!   dispatch is a command-layer non-goal), and a raised `max_input_bytes` /
//!   `max_result_bytes` (#252).
//! - **ACCEPTED, inert:** `name` (registration key), `module` (diagnostics), and a lone
//!   `concurrency_key` (groups a cap that is not set). The core async
//!   `ActivityInfo::handler` is intentionally NOT used — this backend runs a
//!   caller-supplied SYNCHRONOUS body (no `ActivityContext`, no I/O framework),
//!   matching the crate's activity model, so `register_activity` takes both the
//!   `&ActivityInfo` (for the audit + declared defaults) and the body.
//!
//! **Signals are pull-only.** A staged signal is appended to history (as
//! `SignalReceived`) only when a workflow reaches a pull primitive that consumes
//! it. The supported pull surface is:
//! - the plain waits `ctx.wait_for_signal` / `ctx.receive_signal`; and
//! - the signal-or-deadline waits `ctx.wait_for_signal_timeout` /
//!   `ctx.receive_signal_timeout` (issue #476), which race the signal against a
//!   durable deadline timer — the signal wins if it arrives first, else the timer
//!   fires and the wait resolves to `None`. The winner is decided by **arrival
//!   time** (at millisecond precision, issue #1069 P2), not consumption order: the
//!   wake ingest interleaves a staged signal's `received_at` against the deadline
//!   timer's `fire_at` (mirroring the Postgres `merge_wake_events`), AND pending
//!   same-name signals are consumed in ARRIVAL order (`peek_pending_signal` orders
//!   by `received_at`, not insertion `signal_seq`, issue #1069 P2), so an on-time
//!   signal staged *after* a late one still wins the race. So a signal
//!   delivered *after* an expired deadline records its `TimerFired` FIRST and the
//!   timeout wins (the late signal stays recorded, observable to a later plain
//!   wait), while an on-time signal wins even when the run is only driven past the
//!   deadline. **Only the raced deadline timer of THIS wait** (the core's
//!   `__signal_timeout:{seq}:{name}` row) is reordered ahead of the signal (issue
//!   #1069 P2); an unrelated durable timer is never fired ahead of a consumed
//!   signal. When the **signal** wins, the
//!   core emits a `CancelRaceLosers` bookkeeping command to durably delete the
//!   losing deadline timer; this backend handles it (deleting the `harvest_timers`
//!   row in the same cycle transaction — whether it rides a suspending batch or the
//!   terminal drain), so the run completes cleanly and no orphaned timer is left
//!   behind.
//!
//! **Known limitation — a plain `wait_for_signal(name)` composed with a
//! `wait_for_signal_timeout(name, ...)` on the SAME signal name concurrently** (e.g.
//! `tokio::join!(ctx.wait_for_signal("go"), ctx.wait_for_signal_timeout("go", ...))`)
//! is subject to a pre-existing limitation of the *core* determinism engine, NOT
//! this backend: the sibling timeout wait's deadline (`__signal_timeout:{seq}:go`)
//! is a legitimate durable timer, so a signal arriving after that deadline records
//! `TimerFired` before `SignalReceived(go)`; the core `HistoryMatcher::match_signal`
//! path (the plain wait) does not skip an interleaved `TimerFired`, so replay cannot
//! consume the delivered signal and the run stays parked. This is NOT a backend
//! ingest/correlation gap — even exact per-wait timer correlation cannot fix it,
//! because the deadline `TimerFired` is in history regardless of ingest order, and
//! `match_signal` still stops at it (verified on the core `WorkflowReplayer`:
//! `[…, TimerFired(__signal_timeout:0:go), SignalReceived(go)]` for a plain-wait
//! handler → `NonDeterminismDetected`). Tracked as the same core-matcher class as
//! #1071 (the interleaved-`TimerFired` intolerance also behind the declined
//! activity-vs-timer reorder). The **supported** path for a signal-with-deadline is
//! `wait_for_signal_timeout` / `receive_signal_timeout` used ALONE (which arms
//! exactly the `__signal_timeout` deadline this backend reorders, and is covered by
//! the late-signal-vs-deadline tests) — do not also add a plain `wait_for_signal`
//! on the same name in the same race.
//!
//! Signal ingress is validated at the boundary:
//! [`SqliteRuntime::send_signal`](crate::SqliteRuntime::send_signal) rejects a signal
//! aimed at an unknown execution ([`SqliteError::ExecutionNotFound`]) or a terminal
//! one ([`SqliteError::WorkflowNotRunning`]) instead of staging a row no live pull
//! primitive could ever consume — mirroring the Postgres engine's rejection of a
//! signal to an unknown/terminal target. A `RUNNING` execution stages (and later
//! consumes) the signal exactly as before.
//!
//! The push-handler and non-blocking drain APIs — `register_signal_handler`
//! (#546), `drain_signals` / `try_receive_signal` (#775) — depend on the Postgres
//! task-preparation ingest that promotes *all* pending signals into history up
//! front, which this single-writer backend does not implement; a workflow that
//! relies on them will not see its signals here. Restrict to the pull primitives
//! on this backend; push-based signal *handlers* remain a follow-up.
//!
//! There is deliberately **no import/seed-a-foreign-history API**. The
//! cross-backend guarantee (a history written here replays on the core engine) is
//! export-shaped and needs none. A future edge → hub sync path that ingests an
//! in-flight history would have to rebuild the derived task/timer rows from any
//! open scheduled work (or reject such histories) so a run isn't wedged — that is
//! a follow-up.
//!
//! # Robustness — healthy-run-killer containment (Codex #1069 / round 8)
//!
//! Failure modes that would otherwise WEDGE (or wrongly seal) a healthy run are
//! contained:
//!
//! - **A workflow-handler panic is bounded-retried, not sealed on the first
//!   strike.** A `#[workflow]` body that `panic!()`s (distinct from an activity
//!   body panic, above) is caught by the core at the dispatch boundary and
//!   surfaced as a contained panic. Rather than sealing the run `FAILED` on the
//!   first strike — permanently closing a transiently-panicking or hotfix-able run
//!   — the backend discards the panicked decision cycle (appends nothing, persists
//!   no bookkeeping command, state stays `RUNNING`) and surfaces
//!   [`SqliteError::WorkflowPanicked`] under a bounded consecutive-panic budget, so
//!   a fixed/rolled-back build can re-drive the UNCHANGED history to completion.
//!   Only after the budget is exhausted is the run sealed `FAILED` with the typed
//!   `HandlerPanic` error (as a normal [`RunState::Failed`]). A genuine author
//!   `Err(...)` still fails terminally on the first strike, unchanged. Mirrors the
//!   Postgres worker's issue #782 panic gate.
//!
//! - **Engine replay non-determinism is NON-TERMINAL.** If a workflow's code
//!   drifts from its recorded history (a bad deploy), the determinism core
//!   returns a non-determinism divergence. Rather than sealing the run `FAILED`
//!   and poisoning history with a terminal event from the bad build, the backend
//!   **discards** the divergent decision cycle (appends nothing, persists no
//!   bookkeeping command), leaves the execution `RUNNING` / resumable, and
//!   surfaces [`SqliteError::NonDeterministic`]. A fixed or rolled-back build then
//!   re-drives the UNCHANGED history to completion. This mirrors the Postgres
//!   engine's issue #603 semantics ("a workflow-task failure must never fail the
//!   workflow"); the full automatic block-with-backoff is a documented follow-up.
//!   A genuine author `Err(...)` still fails terminally, unchanged.
//! - **An unregistered activity leaves its task re-claimable.** A workflow that
//!   schedules an activity whose body is not registered has the just-claimed task
//!   RELEASED back to `PENDING` (not stranded `RUNNING`) before the
//!   [`SqliteError::UnregisteredActivity`] error is surfaced, so a later drain
//!   re-claims it once the body is registered in the SAME runtime — no DB reopen.
//! - **A panicking activity body is contained.** A body that `panic!()`s (rather
//!   than returning `Err`) is caught at the dispatch boundary and routed through
//!   the NORMAL retryable-failure path (record the attempt, requeue if attempts
//!   remain, else terminal `ActivityFailed`), so it never unwinds past the
//!   finalize and strands the task `RUNNING`. Mirrors the Postgres worker's
//!   handler-panic containment (issue #782). Write bodies to be idempotent
//!   regardless — a caught panic is a retry, and (as with any failure) the body
//!   may have run partially.
//! - **Mixed timer + activity batch.** A single decision cycle emitting BOTH a
//!   timer and an activity (`tokio::join!`) records its schedule events in COMMAND
//!   order and drives to completion + replays cleanly on the core
//!   `WorkflowReplayer` — **when the activity branch is polled first**
//!   (`join!(activity, timer)`). The reverse order (`join!(timer, activity)`) is a
//!   pre-existing limitation of the *core* determinism engine, NOT this backend:
//!   `HistoryMatcher::match_timer_strict` breaks its forward `TimerFired` scan at
//!   an unconsumed `ActivityScheduled`, so the core `WorkflowReplayer` rejects even
//!   an ideal timer-first history (`NonDeterminismDetected(EarlyCompletion)`) —
//!   shared with the Postgres engine. Put the activity branch first in a mixed
//!   `join!`.
//!
//! # Deterministic side effects
//!
//! `ctx.system_now()`, `ctx.new_uuid()`, `ctx.random_*()`, and `ctx.side_effect()`
//! record a frozen value as a `SideEffectRecorded` event so replay never re-mints
//! it. These events are persisted **atomically with the rest of the cycle** in
//! every case — whether they ride a **suspending** batch (a side effect *before*
//! an activity/timer/signal, via [`apply_commands`](crate::SqliteRuntime)) or are
//! emitted in the cycle that **completes/fails** the workflow with no further
//! suspension (e.g. `ctx.new_uuid()` right before `Ok(..)`): the terminal cycle
//! drains those pending bookkeeping commands and appends each one BEFORE the
//! terminal `WorkflowCompleted`/`WorkflowFailed` event, in the same transaction,
//! mirroring the Postgres worker's `persist_terminal_outcome_commands`. So a
//! deterministic primitive is **never** silently dropped, and a history written
//! here always replays byte-identically on the core `WorkflowReplayer`.

mod error;
mod queue;
mod runtime;
mod schema;
mod store;
mod worker;

pub use autumn_harvest::{ExecutionId, WorkflowEvent};

pub use crate::error::{SqliteError, SqliteResult};
pub use crate::runtime::{ActivityBody, ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime};
pub use crate::store::ActivityAttempt;
