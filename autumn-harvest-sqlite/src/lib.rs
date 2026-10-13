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
//! - **The contract is enforced (issue #1834).** [`SqliteRuntime::open`] takes an
//!   exclusive OS lock on `<database>.lock`. A second runtime on the same file
//!   fails fast with [`SqliteError::DatabaseLocked`], before it changes the file.
//!   The lock dies with its process, so a crash leaves no stale lock. A read-only
//!   inspector connection still works. Do not delete the lock file while a
//!   runtime runs.
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
//! - **Activity `start_to_close` timeouts are honored.** The declared or per-call
//!   `start_to_close` comes from `#[activity(start_to_close = "…")]` or
//!   `execute_activity_with_opts`. It flows through the scheduling command's
//!   `start_to_close_override`. The backend persists it in milliseconds on the task
//!   row. The worker enforces it as a **post-execution outcome** (issue #1069 P2).
//!   The drive does not cancel a body mid-flight in a single-writer runtime. So a
//!   body that runs past its budget records a **terminal**
//!   `ActivityTimedOut { StartToClose }` instead of `ActivityCompleted`. The
//!   workflow observes [`HarvestError::Timeout`](autumn_harvest::HarvestError::Timeout)
//!   and drives its own timeout branch. The event is byte-equivalent to the one the
//!   Postgres timeout scanner records. The wall-clock behavior differs: the body
//!   runs to completion before the timeout is recorded. The history stays
//!   replay-safe, because the outcome is recorded once and replay never re-runs the
//!   body. `None` means no budget, so the activity is unbounded, as before.
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
//! // The `#[activity]` body runs with a real `ActivityContext` (issue #2024).
//! // The same fn runs unchanged on the Postgres backend.
//! #[activity]
//! async fn say_hello(ctx: &ActivityContext, name: String) -> Result<String, String> {
//!     Ok(format!("hello {name}, attempt {}", ctx.attempt()))
//! }
//!
//! # async fn run() -> Result<(), autumn_harvest_sqlite::SqliteError> {
//! let mut rt = SqliteRuntime::open("workflows.db")?;
//! rt.register_workflow(&greet_info());
//! // The audited, `ActivityInfo`-driven registration: any declared `#[activity(...)]`
//! // default is HONORED (e.g. `schedule_to_close`) or REJECTED LOUDLY at setup, never
//! // silently dropped. Use `register_activity_with_body` to run a synchronous stub
//! // in place of the body, or `register_activity_raw(name, ActivitySpec::new(..))`
//! // for a hand-made body that carries no declared defaults.
//! rt.register_activity(&say_hello_info());
//!
//! let exec = rt.start_workflow("greet", serde_json::json!("world"))?;
//! rt.run_until_blocked(exec).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Child workflows and commands between runs (issue #2026)
//!
//! A workflow can start a child, await another run, and signal or cancel
//! another run. Each command takes effect in the caller's decision
//! transaction. One database holds every run, so the backend needs no outbox.
//!
//! - **Attached child** — `ctx.spawn_child_workflow(...)`. The parent records
//!   `ChildWorkflowStarted`. The child gets its own row, with a parent link.
//!   Its `workflow_id` is its execution id. Its terminal writes
//!   `ChildWorkflowCompleted` or `ChildWorkflowFailed` to the parent in the
//!   same transaction. A typed failure keeps `error_type`, `details` and
//!   `non_retryable`. A cancelled child fails the parent's await with
//!   `child workflow cancelled: <reason>`, and a terminated child with
//!   `child workflow terminated: <reason>`. A child cannot continue as new.
//!   It fails with the Postgres text instead. A parent child-timeout deadline
//!   that is due fires first, so a late child loses to it, as on Postgres.
//! - **Detached child** — `ctx.spawn_child_workflow_detached(...)`. The parent
//!   records `ChildWorkflowSpawnedDetached` and does not wait. When the parent
//!   closes, `request_cancel` cancels a running child (reason `parent closed`),
//!   `terminate` fails it (`ParentClosed`), and `abandon` leaves it. The parent
//!   records `ChildWorkflowCascadeApplied` for each child it cascades to. The
//!   cascade reaches every level of a chain of detached children.
//! - **Await** — `ctx.await_external_workflow(...)` reads another run's result.
//!   A running target leaves the await open. Each later cycle checks it again.
//!   An await follows a target that continues as new to its last run.
//! - **Signal** — `ctx.signal_external_workflow(...)`, also by workflow id.
//!   A running target gets a staged signal. A terminal target fails the call
//!   with `target_terminal`, or `not_running` by workflow id. An idempotency
//!   key that the target already holds reports delivered and stages nothing.
//!   By workflow id, the key dedups across every run of that id. A keyed row
//!   stays when its run closes, so the dedup outlives a cancel.
//! - **Cancel** — `ctx.request_cancel_external_workflow(...)`, also by workflow
//!   id. A running target ends `CANCELLED` with reason `cancelled by external
//!   request`. Its body does not run again, unlike the cooperative
//!   [`SqliteRuntime::cancel`]. This matches Postgres. A terminal target is a no-op
//!   success. A run that its own cancel seals, through a cascade, applies no
//!   more of its batch.
//!
//! An unknown target fails at once with `target_unknown`. Postgres waits a
//! grace window first, because another shard can still create the run. One
//! file has no other shard, so its answer is final.
//!
//! A run that waits on a `RUNNING` child or await target parks as
//! [`RunState::WaitingWorkflow`]. [`SqliteRuntime::run_until_blocked`] then
//! drives each such run in the same call. When all of them block, the waiter
//! returns `WaitingWorkflow` naming the run it waits on directly. The fleet
//! drivers treat it as blocked. A drive that starts an unregistered child
//! workflow returns [`SqliteError::UnknownWorkflow`]. The cycle rolls back,
//! and the parent stays `RUNNING`.
//!
//! A child is a full run. `ctx.info().parent_execution_id` names its parent.
//! [`SqliteRuntime::parent_execution`] reads the same link, and
//! [`SqliteRuntime::outcome`] reads the child's result. A drive of a run that
//! another run cancelled returns [`RunState::Cancelled`] with the reason.
//!
//! # Non-goals (see issue #1068 follow-ups)
//!
//! Out of scope for this backend: distributed / multi-writer workers, `LISTEN`/
//! `NOTIFY` push wake-ups, multi-server crash recovery, schedules, the management
//! API, retention, worker sessions, sharding, and DAGs.
//!
//! ## Start-boundary reuse policy — enforced (issue #1068)
//!
//! This backend enforces the `(workflow_name, workflow_id)` start-boundary
//! reuse-policy matrix, mirroring the core Postgres PLAIN-start path.
//! [`start_workflow_with_id`](crate::SqliteRuntime::start_workflow_with_id) is
//! idempotent by default (the
//! [`AllowDuplicate`](autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate)
//! policy): a duplicate delivery for the same non-sealed `workflow_id` ATTACHES —
//! the same [`ExecutionId`](crate::ExecutionId) is returned and no second run is
//! started — so a retried webhook is idempotent instead of repeating its side
//! effects.
//! [`start_workflow_with_reuse_policy`](crate::SqliteRuntime::start_workflow_with_reuse_policy)
//! exposes the full four-policy matrix and returns a
//! [`StartOutcome`](crate::StartOutcome) (whether a fresh run was created);
//! [`start_workflow`](crate::SqliteRuntime::start_workflow) (no id) still creates a
//! distinct run every call, since its defaulted `workflow_id` is unique per run.
//!
//! The reuse decision is atomic in the single-writer start transaction (no
//! concurrent writers ⇒ a lookup-then-decide needs no database uniqueness index; a
//! unique index would also fail to build on reopen of a pre-#1068 file already
//! holding duplicate active ids). A replaced/superseded prior is SEALED to
//! `CONTINUED_AS_NEW` and thereby excluded from the reuse lookup, mirroring core's
//! uniqueness-index-excludes-terminal nuance.
//!
//! ### State-model restriction vs. the Postgres core
//!
//! This backend's execution state model is a strict subset of the core's. An
//! active run is only ever `RUNNING` (there is no `PAUSED`). The terminal states
//! it writes are `COMPLETED`, `FAILED`, `CANCELLED` and `TERMINATED` (issue
//! #2021), plus the `CONTINUED_AS_NEW` SEAL of the reuse "replace" cases. It never
//! writes `TIMED_OUT`. The reuse lookup skips `TERMINATED` and `CONTINUED_AS_NEW`,
//! so the reuse matrix is realized over
//! `{none, RUNNING, COMPLETED, FAILED, CANCELLED}` priors only:
//!
//! - `AllowDuplicate` — attach to / return any non-sealed prior; else fresh.
//! - `RejectDuplicate` — [`SqliteError::AlreadyExists`] for any non-sealed prior;
//!   else fresh (a SEALED-only prior is invisible, so it creates fresh).
//! - `AllowDuplicateFailedOnly` — replace (seal + fresh) a FAILED or `CANCELLED`
//!   prior, as core does; return a RUNNING/COMPLETED prior unchanged; else fresh.
//!   A `CANCELLED` prior comes from [`SqliteRuntime::cancel`] (issue #2021) or
//!   from another run's cancel (issue #2026).
//! - `TerminateIfRunning` — cancel (record `WorkflowCancelled` + clean up the
//!   prior's PENDING tasks and unfired timers) then seal a RUNNING prior; seal an
//!   already-terminal prior without a cancellation event; then start fresh. The
//!   single-writer runtime replaces the prior atomically and never drives it again,
//!   so this forcefully seals the prior (observably equivalent to core) rather than
//!   delivering cooperative cancellation the prior's body could observe.
//!
//! See the per-method rustdoc on `start_workflow_with_reuse_policy` for the full
//! matrix table.
//!
//! ## Cancel and terminate (issue #2021)
//!
//! [`SqliteRuntime::cancel`] is cooperative. It stores the request, and the next
//! drive replays the body with `ctx.cancellation_reason()` set. The body can run
//! cleanup activities. The run ends `CANCELLED` when the body returns, when it
//! blocks with no activity task left, or when the cycle diverges or deadlocks.
//! [`SqliteRuntime::terminate`] ends the run `TERMINATED` at once, and the body
//! does not run again.
//!
//! Both paths append the core `WorkflowCancelled` event as the last event, as
//! Postgres does. Cleanup events come before it. The history replays on the
//! core `WorkflowReplayer` when the body checks the request before its next new
//! command. Both paths delete pending tasks, unfired timers and staged signals.
//! The drives report [`RunState::Cancelled`] and [`RunState::Terminated`].
//!
//! ## Unsupported workflow primitives — rejected LOUDLY, by name
//!
//! Only the *fire-once* durable timer `ctx.timer(...)` is a supported timer. Every
//! other out-of-subset primitive a workflow can reach surfaces as
//! [`SqliteError::Unsupported`] whose message NAMES the specific
//! `WorkflowCommand` (never an opaque `"Unknown"`, Codex #1069 P2) — a workflow
//! author gets an actionable failure, not a silent gap.
//!
//! The run then ends `FAILED` (issue #1834). The drive rolls back the cycle and
//! seals the run with a typed `WorkflowFailed` event. Its `error_type` is
//! [`UNSUPPORTED_FEATURE_ERROR_TYPE`], `details.feature` names the command, and
//! `non_retryable` is `true`. A later drive returns [`RunState::Failed`] and does
//! not run the handler again. The full v0.1 non-goal set, and the command each
//! emits:
//!
//! - **Local activities** — `ctx.execute_local_activity(...)` (`RunLocalActivity`).
//! - **External activities** — task-token activities (`ScheduleExternalActivity`).
//! - **Search attributes** — `ctx.upsert_search_attributes(...)`
//!   (`UpsertSearchAttributes`).
//! - **A cross-type continue-as-new** — `ctx.continue_as_new_as_type(...)`
//!   (`ContinueAsNew.new_workflow_type`). A same-type `continue_as_new` is
//!   supported (issue #2023): see [Long-lived runs](#long-lived-runs-issue-2023).
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
//! ## Long-lived runs (issue #2023)
//!
//! `ctx.continue_as_new(input)` seals the run `CONTINUED_AS_NEW` and starts a
//! successor with the same `workflow_id`, in one transaction. The staged
//! signals that the hand-off decision replayed stay with the old run, as on
//! Postgres (issue #2020). A backlog past the history caps moves to the
//! successor. The drive returns
//! [`RunState::ContinuedAsNew`] with the successor id.
//!
//! The #1804 history caps apply, with the Postgres defaults: 50,000 events and
//! 50 MiB of stored event JSON per run. A run that reaches a cap fails with a
//! [`HistoryCapReason`] in its error JSON. A continuing run is exempt. See
//! [`SqliteRuntime::set_history_event_hard_cap`] and
//! [`SqliteRuntime::set_history_byte_hard_cap`].
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
//!   - `quota` (#946) — per-tenant resource quota enforcement needs the shared
//!     Postgres backend's admission-time advisory lock, absent here.
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
//!   — HTTP-edge MCP tool exposure), `declared_activities` / `declared_children`
//!   (#802 — an opt-in dependency declaration consumed only by the Postgres plugin's
//!   `catalog_consistency` deploy-time preflight check and its `/workflows/registered`
//!   discovery endpoint, neither of which this backend has; the core execution path
//!   never reads either field), and `module` (diagnostics).
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
//!     (a per-attempt post-execution `ActivityTimedOut { StartToClose }`). The
//!     timeout is terminal here. Postgres retries it per the retry policy (issue
//!     #1809, ADR 0005).
//!   - `default_queue` — recorded on the task row (single-writer routing is a no-op).
//! - **REJECTED:** each is a dispatch-admission or cross-worker semantic with no
//!   single-writer analog, or a cap raiser. A cap raiser would otherwise silently
//!   apply the STRICTER global cap.
//!   - `default_heartbeat_timeout`: no scanner times out a silent attempt. Use
//!     `start_to_close` or `schedule_to_close`. `ctx.heartbeat(..)` itself works and
//!     stores a checkpoint (issue #2024).
//!   - `default_schedule_to_start`: no queue-wait scanner.
//!   - `circuit_breaker` (#369), `rate_limit_*` (#332), `max_concurrent` (#247).
//!   - `requires`: capability routing.
//!   - `is_local`: local-activity `RunLocalActivity` dispatch is a command-layer
//!     non-goal.
//!   - A raised `max_input_bytes` / `max_result_bytes` (#252).
//! - **ACCEPTED, inert:** `name` (registration key), `module` (diagnostics), and a lone
//!   `concurrency_key` (groups a cap that is not set).
//! - **RUN:** `handler`. `register_activity` awaits the `#[activity]` body with a
//!   real `ActivityContext` (issue #2024). `register_activity_with_body` runs a
//!   synchronous closure in its place, after the same audit.
//!
//! **Signals.** Each decision cycle appends every staged signal to history
//! (as `SignalReceived`) before replay, in arrival order (issue #2020). This
//! mirrors the Postgres task-preparation ingest, but an unrelated due timer
//! fires after the signals of the cycle (issue #1071). So every signal API
//! works:
//! - the plain waits `ctx.wait_for_signal` / `ctx.receive_signal`;
//! - the non-blocking reads `ctx.drain_signals` / `ctx.try_receive_signal`
//!   (#775);
//! - the push handlers of `ctx.register_signal_handler` (#546), also with
//!   `ctx.await_condition`; and
//! - the signal-or-deadline waits `ctx.wait_for_signal_timeout` /
//!   `ctx.receive_signal_timeout` (issue #476).
//!
//! The ingest is planned before replay. The transaction that persists the
//! cycle outcome writes it first. So a discarded cycle (non-determinism, a
//! contained panic under its budget) consumes no signal. A panic seal and an
//! unsupported-feature seal write the signals that their replay saw.
//!
//! A signal that the workflow never reads also reaches history. The strict
//! core `WorkflowReplayer` then reports `NonDeterminismDetected(EarlyCompletion)`,
//! as for a Postgres history. A signal that lost a deadline race is exempt. A
//! file from an earlier release can hold signals staged for a name that the
//! run never awaited. The first drive after the upgrade appends them.
//!
//! A push handler runs at the next durable call (a timer, an activity or a
//! signal read) after it registers. Put such a call between the registration
//! and a read of its state. A run parked on `ctx.await_condition` emits no
//! command. So a drive before the signal arrives returns
//! [`SqliteError::Stuck`]. `poll_once` and `run_until_idle` report it as a
//! per-execution error and still drive the other runs. The run stays
//! `RUNNING`, and the next drive after `send_signal` resumes it.
//!
//! A signal-or-deadline wait races the signal against a durable deadline
//! timer. The signal wins if it arrives first. Otherwise the timer fires and
//! the wait resolves to `None`. **Arrival time** decides the winner, at
//! millisecond precision (issue #1069 P2), not consumption order:
//! - The ingest reads staged signals in arrival order (`received_at`), not
//!   insertion order (`signal_seq`). An on-time signal staged after a late one
//!   still wins.
//! - Before each staged signal, the ingest compares its `received_at` with
//!   the `fire_at` of each race deadline of that name. It uses the tie rule of
//!   the Postgres `merge_wake_events`: a tie goes to the timer. Each deadline
//!   fires at most once.
//! - A deadline at or before the signal arrival records its `TimerFired` first.
//!   The timeout wins, and the late signal stays in history for a later wait.
//! - An on-time signal wins, also when the drive comes after the deadline.
//! - **Only the race deadline timer** (the core `__signal_timeout:{seq}:{name}`
//!   row) can go ahead of a signal (issue #1069 P2). An unrelated durable
//!   timer never does.
//!
//! When the **signal** wins, the core emits a `CancelRaceLosers` bookkeeping
//! command to delete the losing deadline timer. This backend deletes the
//! `harvest_timers` row in the same cycle transaction, for a suspending batch
//! and for the terminal drain. So the run completes cleanly and leaves no
//! orphaned timer.
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
//! [`SqliteRuntime::send_signal`](crate::SqliteRuntime::send_signal) validates
//! the target at the boundary. An unknown execution is
//! [`SqliteError::ExecutionNotFound`]. A terminal one is
//! [`SqliteError::WorkflowNotRunning`]. No drive could consume a row staged for
//! either, so the call rejects it, as the Postgres engine does. A `RUNNING`
//! execution stages the signal, and the next drive consumes it.
//!
//! There is deliberately **no import/seed-a-foreign-history API**. The
//! cross-backend guarantee (a history written here replays on the core engine) is
//! export-shaped and needs none. A future edge → hub sync path that ingests an
//! in-flight history would have to rebuild the derived task/timer rows from any
//! open scheduled work (or reject such histories) so a run isn't wedged — that is
//! a follow-up.
//!
//! # Updates (issue #2025)
//!
//! [`SqliteRuntime::update`](crate::SqliteRuntime::update) runs an update and
//! returns its result. The workflow body registers the handler, or
//! [`SqliteRuntime::register_update`](crate::SqliteRuntime::register_update)
//! adds a declarative `#[update]`.
//!
//! The runtime replays the run to rebuild the handlers. The arg schema and the
//! validator run before any write, so a rejection writes no event. The handler
//! then runs. One transaction appends `UpdateAdmitted` and then
//! `UpdateCompleted` or `UpdateFailed`.
//!
//! The handler runs on that separate replay. State that the handler writes does
//! not reach the workflow body. Use an update to compute a result from the
//! workflow state, not to change that state. A `RecordUpdateResult` command from the workflow body records
//! the same events in the decision cycle, as on Postgres.
//!
//! The handler can await any future, within a 5 second limit. It cannot await
//! a durable command, such as an activity or a timer. Such a handler returns
//! [`SqliteError::UpdateTimedOut`] and writes no event.
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
//! # Queries (issue #2022)
//!
//! [`SqliteRuntime::query`] calls a handler that `ctx.register_query`
//! registered. It loads the history, replays the run to its frontier, then
//! calls the handler. It writes nothing. A running run answers with the state
//! at its frontier. A terminal run answers with the state at its recorded
//! end. One process holds the run, so a query needs no routing to a worker.
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

mod composition;
mod error;
mod history_cap;
mod lock;
mod query;
mod queue;
mod runtime;
mod schema;
mod store;
mod update;
mod worker;

pub use autumn_harvest::{ExecutionId, WorkflowEvent};

pub use crate::error::{SqliteError, SqliteResult};
pub use crate::history_cap::HistoryCapReason;
pub use crate::query::DEFAULT_QUERY_TIMEOUT;
pub use crate::runtime::{
    ActivityBody, ActivitySpec, ExecutionOutcome, RunState, SqliteRuntime, StartOutcome,
    UNSUPPORTED_FEATURE_ERROR_TYPE,
};
pub use crate::store::ActivityAttempt;
