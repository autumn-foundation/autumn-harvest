## Phase 3.57 — Live workflow output streaming via `ctx.publish_progress` (issue #791)

**Before:** a workflow author building an AI agent, a long-running import, or any
interactive flow had **no way to push incremental, author-defined business
output** to a waiting client while the run is in flight. The only surfaces were a
query handler (pull, snapshot-only), `set_current_details` (a single overwriting
status string, #473/#593), or — for operators only — the admin-gated engine-event
SSE tail (#324), which streams `ActivityScheduled`/`ActivityCompleted` *machinery*,
not the author's content. Streaming tokens / reasoning steps / per-item progress
to an end user meant standing up an external message bus.

**After:** one replay-safe method — `WorkflowContext::publish_progress(chunk: impl
serde::Serialize) -> HarvestResult<()>` — emits an ordered, author-defined chunk
from live workflow code, delivered to a subscriber on the new SSE route
`GET /api/harvest/workflows/{id}/stream` over the existing Postgres LISTEN/NOTIFY
plumbing. **Ephemeral, best-effort, product-UX channel** — not an audit record:
chunks are dropped when no subscriber is connected; the durable workflow *result*
remains authoritative.

**Determinism / append-only invariant — the load-bearing property.** The call is a
**pure side effect gated on `is_replaying()`**: on a replay pass it returns
`Ok(())` immediately with **no `WorkflowCommand` pushed and no seq consumed**
(mirrors `set_current_details`), so a workflow that publishes N chunks produces a
**byte-for-byte identical `harvest_events` history** to one that publishes none.
Chunk content **MAY be non-deterministic** (LLM tokens, wall-clock timestamps,
sampled values) *precisely because* it is never recorded or replayed. **No new
`WorkflowEvent` variant, no migration, no shard-routing change.**

**Core (`context.rs`).** Live path: serialize the chunk to `serde_json::Value`
(serialize error → `Err`), cap serialized bytes at `PROGRESS_CHUNK_MAX_BYTES = 7000`
(Postgres `NOTIFY` hard limit 8000, with envelope headroom); an oversize chunk is
**replaced** — never silently dropped — with the truncation marker
`{"_harvest_progress_truncated": true, "bytes": <original_len>}`, preserving the
ordered slot and signalling truncation to the client. Pushes a **bookkeeping**
`WorkflowCommand::PublishProgress { seq: u64, chunk: Value }` (no `result_tx`,
non-suspending; joins the `SetCurrentDetails` family at every exhaustive-match and
bookkeeping-classifier site in `worker.rs`, excluded from
`executor.rs::is_replay_significant_command`, `Ok(false)` no-op in
`testing.rs::apply_command`). **Monotonic seq (AC6):** `seq = (epoch << 24) |
(local_index & 0xFF_FFFF)` (`encode_progress_seq`), where `epoch` is the
loaded-history length at cycle start (read by locking the matcher directly, the
`info()` trick, to avoid the `pump_signal_handlers` side effect) — constant within a
cycle and strictly growing across cycles — and `local_index` is a per-cycle
`AtomicU64`. seq is therefore lifetime-monotonic (reconnect gap detection) and
**idempotent on a crash-retry of the same cycle** (best-effort duplicates are
dedupable by seq). Epoch shift uses `saturating_mul`; `local_index` saturates at
`2^24 - 1` (no overflow panic).

**Notify (`worker.rs` + `notify.rs`).** `notify_progress_from_commands(conn,
exec_id, &commands)` fires `notify::notify_workflow_progress(...)` on the worker's
**persist connection**, co-located right after every
`persist_current_details_from_commands` call site (5 sites: suspension, terminal,
local-activity pre-run, 2 panic-strike paths) — so progress flushes on both
suspension and terminal cycles, in the same transactional context as the
`current_details` write (pg NOTIFY delivers on commit → a rolled-back+retried cycle
re-fires live; the deterministic seq makes any autocommit-mode duplicate dedupable).
A notify failure is **logged and swallowed**, never fails the workflow — progress
is disposable. `notify.rs` adds the per-execution channel
`workflow_progress_channel(exec_id)` (`harvest_progress_{32-hex}`, < 63-char
`NAMEDATALEN`), `ProgressNotifyPayload { seq, chunk }`, `notify_workflow_progress`,
and `WorkflowProgressListener` (`connect` / `wait_for_progress` /
`wait_for_progress_timeout` → `ProgressWaitOutcome::{Chunk, TimedOut,
ChannelClosed}`), mirroring `WorkflowEventListener` (tokio-postgres driver task +
mpsc). All `pub` under `autumn_harvest::notify`.

**Plugin SSE route (`api.rs`).** `GET /api/harvest/workflows/{id}/stream`
(`text/event-stream`). Frames: `event: progress` (`id:` = seq, `data:` = the raw
chunk JSON, **not** re-wrapped in the adjacently-tagged envelope) in publish order;
`event: end` (`data: {"reason": <terminal-state>}`) on terminal; `event: error`
(`data: {"error": "listen_connection_closed"}`) on channel loss. **Auth (AC5):
deliberately NOT `require_admin`** — it is an end-user-facing read path (an app's
users stream their own workflows), distinct from the admin engine-event tail (#324).
It inherits the general `api_with_auth` middleware the other management routes use;
**open if no auth is configured** — the default posture is documented in
`docs/streaming-progress.md`, and embedders MUST front untrusted end-users.
Clean-close mechanics: `LISTEN` is established **before** the existence check (so a
chunk published in the check→listen window is not missed); the pooled DB connection
is **dropped** before streaming (SSE streams never hold pool connections — the
listener owns its own dedicated `tokio-postgres` connection, shard-resolved via
`sse_notification_url(shard)`); an already-terminal execution emits a single `end`
frame and closes immediately; on each keepalive `TimedOut` tick the producer polls
terminal state (bounding close latency to one keepalive) and detects an
idle-disconnected client via `tx.is_closed()`; `ChannelClosed` emits `error` and
ends rather than hanging. Read-path decode (#608) does **not** apply — progress
chunks are ephemeral author-supplied JSON, never codec-encoded.

**Contract / audit registration (AC9).** Route registered in `harvest_api_router`,
`management_api_routes()`, `management_api_response_fields()`, `docs/api-contract.json`,
and `autumn_harvest::audit`: `CLASSIFIED_ROUTES` (**ReadOnly**), `ALL_MUTATION_ROUTES`
(op **None**), `EXCLUDED_ROUTES`. Pinned classification tests in **both** `audit.rs`
and plugin `contract_regression.rs` (the audit exhaustiveness cross-check stays green
if a route is dropped from BOTH lists, so both are pinned independently).

**Out of scope (per issue):** durable persistence / replay of chunks (that is the
durable per-execution logs primitive, #790); at-least-once / exactly-once delivery,
cross-reconnect ordering guarantees, or buffering for late subscribers (best-effort
only); final-result delivery (#527) and snapshot reads (query handlers /
`set_current_details`); client→workflow streaming (that is signals/updates);
cross-shard fan-in of one logical stream (a stream is scoped to a single `exec_id`
on its owning shard).

**Docs / example.** `docs/streaming-progress.md` (API, determinism contract,
best-effort delivery model, SSE wire format + `seq` semantics, auth default posture,
7000-byte cap + truncation marker, out-of-scope list, cross-links to #473/#593/#324/#527).
`autumn-harvest/examples/streaming_agent.rs` (an AI-agent-shaped workflow publishing
per-step chunks interleaved with a mock activity across decision cycles, one
intentionally non-deterministic chunk with a SAFE-because-never-replayed comment, a
documented `curl -N` consumer, and embedded `WorkflowTestEnv` tests asserting
completion + zero-event-footprint + `ReplaySucceeded`).

**Tests, TDD red→green.** Core: 5 inline `context.rs` unit tests
(replay-suppressed, live-emits-bookkeeping-command, per-cycle local-index increase,
oversize→truncation-marker); 2 no-DB integration tests
(`publish_progress_leaves_zero_event_footprint` — identical event-type sequence to a
no-progress sibling; `publish_progress_replays_with_zero_divergence` — ReplaySucceeded
against an activity-only history, AC2); 2 new `notify::` unit tests (progress channel
+ payload round-trip). Plugin: DB integration
(`progress_stream_delivers_ordered_chunks_and_ends_on_terminal`,
`progress_stream_on_already_terminal_execution_closes_immediately`,
`progress_stream_unknown_execution_returns_404`) and no-DB registration/auth-posture
tests (`sse_stream_tests.rs`), plus the pinned classification tests. All core no-DB
surfaces run green in-sandbox; DB/HTTP suites (`progress_stream_integration`) run
under the Docker-backed CI step registered in `.github/ci/integration-suites.txt`.
