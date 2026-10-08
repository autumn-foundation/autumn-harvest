## Phase — Strict percent-decoding swept to every raw-pairs route (issue #1151)

Issue #774 fixed one route, `GET /admin/queue-coverage`: axum's built-in
`Query<Vec<(String, String)>>` extractor is backed by
`serde_urlencoded`/`form_urlencoded`, which *always* succeed by silently
substituting `U+FFFD` for a malformed percent-encoded byte sequence instead
of rejecting the request — so `?queue_name=%FF` silently decoded to a
*different*, legitimate-looking `queue_name` instead of the documented
`400`, letting a corrupted request produce a false-clean result on a
CI/CD deploy gate. Issue #1151 swept the same fix across the other 19
call sites in `autumn-harvest-plugin/src/api.rs` that shared the pattern.

**Behaviour change to note (narrows an existing success/error boundary, not
a documented shape):** every route listed below now returns `400` for a
query string containing a malformed percent-encoding — a syntactically
invalid `%` escape (`%`, `%2`, `%GG`) or a well-formed escape whose decoded
bytes are not valid UTF-8 (`%FF`) — where it previously returned `200`/`204`
with the offending byte sequence silently replaced by `U+FFFD` in whichever
param it appeared. No currently-*valid* request is affected; only a request
that was already malformed (and therefore already misbehaving) changes
outcome, from a wrong answer to the documented error.

**Shared decoder, not seventeen reimplementations.** The strict decoder
(`parse_raw_query_pairs_strict`, `InvalidQueryEncoding`) moved out of
`queue_coverage.rs` into a new module, `autumn-harvest-plugin/src/
strict_query.rs`, with three dispatch helpers so each route's malformed-query
`400` matches the JSON shape its *other* invalid-param `400`s already use,
rather than introducing a second, inconsistent shape on the same route:
`decode_or_autumn_error` for `Result<_, AutumnError>`-returning handlers (13
routes), `decode_or_autumn_error_response` for `axum::response::Response`-
returning handlers whose other `400`s are already `AutumnError`-shaped (4
routes), and `decode_or_queue_coverage_bad_request` kept separate for
`GET /admin/queue-coverage` alone, preserving its original, already-shipped
`{"error": "..."}` shape from issue #774 rather than silently changing that
route's contract. `queue_coverage::{parse_raw_query_pairs_strict,
InvalidQueryEncoding}` stays a `pub use` re-export of the new path —
`autumn-harvest-plugin` is a published library crate and both names were
already public there.

**Decode-first, consistently.** Every fixed handler decodes the raw query
string as its first statement, ahead of path/id validation or database
access, so a malformed query is rejected the same way regardless of whether
the rest of the request would have succeeded. Two routes needed a second
pass to hold that invariant: `GET /workflows/by-id/{workflow_name}/
{workflow_id}/result` and `.../children` resolve the business id to an
execution id (a database lookup) before forwarding the raw query string to
their delegate (`get_workflow_result` / `list_workflow_children`) — an
initial version left the malformed-query check inside the delegate only, so
a malformed query paired with an *unknown* workflow id or an unreachable
shard surfaced the resolution failure (404/503) instead of the documented
`400`, and paid for a lookup on a request that was always going to be
rejected. Both wrappers now re-validate the raw query before calling
`resolve_workflow_by_business_id`.

**Routes affected:** `GET /workflows`, `/workflows/summaries`,
`/workflows/count`, `/workflows/{id}/history`, `/workflows/{id}/history/
export`, `/workflows/{id}/result`, `/workflows/{id}/children`,
`/workflows/{id}/tree`, `/workflows/{id}/logs`, `/workflows/by-id/
{workflow_name}/{workflow_id}/result`, `/workflows/by-id/{workflow_name}/
{workflow_id}/children`, `/admin/usage`, `/admin/history/exports`,
`/admin/history/export-sample`, `/admin/external-handoffs`, `/admin/
schedules/{id}/runs`, `/dead-letters/aggregate`, `/workers`, `/workers/
drain-preview` — plus the already-shipped `/admin/queue-coverage`.
`docs/api-contract.json` gained a `400` `error_responses` entry and a
one-sentence note on each.

No new `WorkflowEvent` variant, no migration, no schema change — this is
entirely HTTP-boundary input validation on read routes.

Tests, red → green → refactor: a parameterized end-to-end suite,
`autumn-harvest-plugin/tests/malformed_query_percent_encoding_sweep_
integration.rs`, proves all 20 routes 400 on a malformed value, a malformed
key, and a syntactically invalid escape over the real `axum` router (a
router with no storage pool installed is enough, since the decode runs
before any database access) — plus unit tests for the decoder and its three
dispatch helpers in `strict_query.rs`, and the pre-existing
`queue_coverage_integration.rs` DB-backed suite for the one route that
predates this issue.
