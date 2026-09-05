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
## Comment Hygiene

`docs/audits/comment-hygiene.py` gates every `*.rs` comment in CI's `lint`
job. Run it before you push: `python3 docs/audits/comment-hygiene.py`.

Write comments short, concise, and in ASD-STE100 style where the subject
allows it: one idea per sentence, 25 words or fewer, active voice, present
tense, no contractions, no first person.

**Never shorten a comment by deleting the reason it exists.** The long
rationale blocks in this tree are load-bearing — the ABBA lock-ordering
argument in `docs/architecture.md`, the `cohort`-key argument in
`partition.rs`, the codec-rotation scope guarantee above. STE asks for
short *sentences*, not short *explanations*. Split a dense paragraph into
several sentences; do not delete the argument to hit a word count. The
harness enforces exactly that distinction: it measures per sentence and
has no cap on block length.

Every comment is checked, leading or trailing, `//` or `/* */`. A defect
written as `let n = 1; // TODO: fix` is gated exactly like one on its own
line. Text inside a string is not a comment, so a fixture that embeds
Rust source is safe.

Four defects fail the build outright and are at zero — commented-out code,
a TODO/FIXME with no `#<issue>`, a narrative aside ("actually, let's ..."),
and a blank `//` line at a block edge. Three more are frozen at their
legacy counts in `docs/audits/comment-hygiene-baseline.json` and may only
fall: review-round archaeology ("Codex round 8" — cite the issue instead),
contractions, and sentences over 25 words. Fix a flagged line rather than
regenerating the baseline; see `docs/audits/README.md`.

## Database Migrations

### Name every migration with a second-precision UTC timestamp

Migration directories are `YYYYMMDDHHMMSS_snake_case_name`. Generate the prefix
from the real clock at the moment you create it:

```sh
date -u +%Y%m%d%H%M%S      # e.g. 20260901130054
```

**Never use a day-only prefix with a zeroed time** (`YYYYMMDD000000`). Diesel
takes the digits before the first underscore as the migration *version*, so a
day-granularity prefix gives every migration authored on the same day the same
version. Two branches in flight then collide, and the collision does not look
like one: git sees two differently-named directories and merges them cleanly, so
nothing conflicts until the duplicate version reaches a database.

Migrations up to and including `20260728000000` predate this rule and keep their
names — renaming a migration that has already been applied anywhere would orphan
its `__diesel_schema_migrations` row. The rule binds new migrations only.

When you renumber a migration that has not shipped, update every reference to its
version string in the same commit (`grep -rn '<old-version>' docs/ autumn-harvest/`
finds them): the upgrade guide's migration table, the changelog fragment and any
plan or design doc that cites it. Those are how an operator matches a migration
on disk to the note explaining what it does.

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

## Project Documentation

`CLAUDE.md` holds agent instructions and engine invariants only. Do not park
architecture notes, changelog entries, or a record of shipped work here — those
have homes:

- [`docs/architecture.md`](docs/architecture.md) — workspace layout, crate
  relationships, design decisions, module guide, macro-usage patterns,
  development commands, DB schema reference. Cross-references throughout
  `docs/` and the source comments point at its sections.
- [`docs/shipped-work.md`](docs/shipped-work.md) — the verbatim shipped-work
  record. Two guard suites read it as data, so treat it as load-bearing.
- [`docs/changelog.d/README.md`](docs/changelog.d/README.md) — where a feature
  PR's changelog entry goes (a fragment file, never the shared phase list).
