# Entity checkpoint hardening (issue #2049)

## Problem

Round 6 of the review of the keyed entity (issue #1975) left two findings.

1. `Entity::run` records the checkpoint byte budget once for each run. A later
   attempt in the same run reuses it. A deploy can lower the input cap while a
   handler waits on an activity. Replay then restores the old, larger budget.
   The loop builds an input above the new cap, and `continue_as_new` fails
   with `PayloadTooLarge`.
2. `record_failure` stores the handler error as is in `stats.last_error`.
   Every checkpoint input carries it. An error near the input cap makes each
   later checkpoint fail, even with `max_ops_per_run(1)`. A decode error can
   also quote part of a bad payload.

## Facts (white hat)

- `ctx.side_effect` runs its closure live and records the value. Replay
  returns the recorded value and ignores the closure.
- `claim_waiting_ops` is a pure function of the budget, the checkpoint and the
  history. Replay of a recorded budget gives the recorded claim.
- `continue_as_new` checks the live cap only when history holds no
  `WorkflowContinuedAsNew` event. It checks before it emits the command.
- `record_failure` is the only writer of `last_error`.
- The MSRV is 1.88. `str::floor_char_boundary` needs 1.91.
- Release 0.7.0 shipped on 2026-10-05. The entity merged after it.

## Brainstorm

Finding 1:

1. Record the budget at each checkpoint attempt. This is the issue proposal.
2. Read the live cap each time and record nothing. Replay then reads another
   cap and claims other ops than the run did.
3. Catch `PayloadTooLarge` from `continue_as_new` and retry. Nothing records
   the failure, so a replay under another cap diverges.
4. Put the budget in the decision side effect. This changes the recorded
   value of every decision and breaks stored histories.

Finding 2:

1. Cut the error in `record_failure`, at a `char` boundary, with a marker.
2. Cut it only when the loop writes the checkpoint. The live query and the
   replayed query then show different text.
3. Drop `last_error` from the checkpoint. This breaks the wire form.
4. Add a setting for the bound. The setting is part of replay and no user
   asked for it.

Option 1 wins in both lists. It keeps the wire form and the event types.

## Reverse brainstorm: how to make the fix fail

| Way to fail | Guard |
|---|---|
| The budget record sits far from the `continue_as_new` it sizes. | Record it just before `claim_waiting_ops`, with no `await` between them. |
| The new attempt reuses a recorded budget. | Test: cut a history after a failed attempt, lower the cap, drive the next cycle. The input fits the new cap. |
| The cut splits a multi-byte character and panics. | Test with a 3-byte character, so the cut falls inside it. |
| The marker pushes the result over the bound. | Test the lengths around the bound. |
| A short error changes. | Test an error of exactly the bound. It stays whole. |
| The decode-error path stays unbounded. | Test a bad `kind` far above the bound. |
| The kept buffer holds the memory of the huge error. | Build a new `String` for the cut text. |
| The `max_ops_per_run(1)` case still fails. | Test a 3 MiB error with `max_ops_per_run(1)`. |

## Six hats

- **White.** See the facts above.
- **Red.** An operator fears a deploy that wedges a live entity. A user does
  not like to see an error cut.
- **Black.** A history that retried a checkpoint under the old code does not
  replay under the new code. A history that carried an error above the bound
  does not replay either. No release holds the old code, so the cost is low.
  The cut also loses detail of a long error.
- **Yellow.** The change is small: one removed variable and one helper. It
  adds no event type and no migration. Each decision and its
  `continue_as_new` now use one config.
- **Green.** Export the bound as a constant, so a test can use it. Later, a
  handler may return a typed error with a code and a short message.
- **Blue.** Work in TDD order. Update ADR 0006, the changelog fragment, the
  security notes and the module docs. Review from several angles.

## Decisions

- No `ctx.patched` gate. Two code paths for a feature that no release holds
  cost more than they save.
- The bound is 1024 bytes in total, marker included. The marker is
  `... [truncated]`.
- No cut of a `last_error` that arrives in the start input. That input is
  trusted, and the next failed op replaces the field.

## Steps

1. RED: a test of the budget record for each attempt, a replay test under a
   lowered cap, tests of the bound and a 3 MiB error with
   `max_ops_per_run(1)`.
2. GREEN: remove the cached budget and add the bound.
3. REFACTOR: tidy the code and the docs.
4. Docs: ADR 0006, the changelog fragment, the security notes and the module
   docs.
5. Review from several angles. Fix each confirmed finding.
