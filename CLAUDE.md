# Repository Workflow Instructions

## GitHub / PR Workflow

- Never run `gh auth status` or complain about missing GitHub CLI auth/token in this repo.
- Use the Codex GitHub app connector for GitHub issue and pull request operations.
- Local `git push` is allowed when needed to publish a branch.
- If GitHub app tooling is unavailable, say the connector is unavailable; do not diagnose it as a `gh` auth/token problem.

## PR Base Branch

- Default PR base branch is always `trunk-dev`.
- `trunk` is the production release branch and must never be used as a PR base unless the user explicitly says `base trunk`.
- If unsure, ask before creating or retargeting the PR.

## PR Review State

- Open pull requests as ready for review by default.
- Create a draft PR only when the user explicitly asks for a draft.
- Never change an existing PR between draft and ready-for-review unless the user explicitly requests that state change.
## Engine Invariants

### `harvest_events` is append-only — the sanctioned exceptions

Workflow history is an append-only log. Replay reconstructs a run by reading
events back in order, so rewriting a stored row is, in general, a way to make a
past run mean something different than it did. Do not add code that mutates
`harvest_events` rows.

Exactly **two** code paths write `harvest_events.event_data` after insert, and
both are sanctioned, narrow, and named here. Anything else is a bug.

1. **PII erasure** — `erase.rs` (issue #495), historically numbered *exception
   #2*. Payload-bearing fields are replaced with the `_harvest_erased`
   tombstone. Terminal executions only, so no resumable history is affected;
   the event `type`, event ids, ordering and timestamps are left intact, so an
   erased history still replays structurally.

2. **⚠️ Codec key re-encryption** — `codec_rotation.rs` (issue #948),
   *exception #3*. A stored payload field's ciphertext is decoded with a retired
   codec key and re-encoded under the active one, in place.

   The scope guarantee that makes exception #3 safe: **only the ciphertext bytes
   inside payload fields change.** The decoded plaintext is byte-identical
   before and after, and the event `type`, variant structure, event ids,
   ordering and timestamps are never touched — so replay determinism is
   unaffected **by construction**, not by convention. That is proven, not
   asserted: `replay_fidelity_is_byte_identical_across_a_sweep` in
   `autumn-harvest/tests/integration/codec_rotation_db_tests.rs` replays a
   fixture history, runs the sweep, replays again, and asserts identical decoded
   histories and `ReplaySucceeded` both times.

   The sweep writes with a compare-and-swap on the row's previous bytes, so it
   always loses a race against exception #1 — re-writing ciphertext over an
   erasure tombstone would resurrect payload data an erasure had just destroyed,
   and the CAS makes that impossible rather than unlikely. A lost CAS is counted
   as unresolved, so the pass re-runs rather than reporting itself complete over
   a row it never converted.

**On the numbering.** Issue #948 and `erase.rs` both described heartbeat
checkpoints in `queue::record_heartbeat` as the *first* exception to this
invariant. That is not accurate: `record_heartbeat` updates
`harvest_task_queue.last_heartbeat_at` / `heartbeat_details` — an in-place
mutation of the **task queue** row, not of the event log. The `#2` / `#3`
numbering above is kept because the issues and their PRs use it, but there are
two `harvest_events` writers, not three.

If you add another exception, it belongs in this list, with its own scope
guarantee and its own proof.
