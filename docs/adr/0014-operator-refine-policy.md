# ADR 0014: Operators decide the refine policy

## Status

Accepted (issue #2394, epic #2369).

## Context

- The refine loop of #2369 edits the setup of an agent. A model writes
  each edit.
- The loop makes policy choices: what goes live without a person, how
  many changes a day, which tool effects a probe may run, how far a bound
  may move, how long an approval waits and how large the lesson library
  grows.
- A hard-coded choice is wrong for some operator. A choice that the loop
  can change lets the loop widen its own gate.

## Decision

1. Operators decide. Each choice is a setting of `RefinePolicy`, one for
   each agent kind. No choice is hard-coded.
2. Each setting has a safe default. The defaults let no overlay go live
   without a person, let a probe only read, and let an overlay only
   narrow its profile.
3. The overlay store holds the policy, next to the overlay versions. A
   change takes effect at the next read, with no restart: the next op, or
   the next refine run of #2380.
4. Only an operator writes the policy. The store refuses any other actor.
   The routes need the `mutate` scope of the operator role.
5. No model output carries a policy. `ProfileOverlay` and `AgentOp`
   refuse each policy key when they decode. `ProposeRequest` refuses each
   one except `ceilings`, which holds the resolved ceilings of the
   recorded read.
6. A run reads the policy once, through a recorded activity. A change
   during a run never widens that run. The store enforces the stricter of
   the run policy and its current policy, so a narrowing reaches the
   adoption at once.
7. Each change is audited with its old and new value, in the store log
   and in the audit table.

## Consequences

- A new refine choice needs a new setting with a default, not a constant.
- An operator who raises a ceiling or adds an auto-adopt part accepts that
  risk. The audit names who did it.
- The open consumers (#2378, #2380, #2381, #2382) will read the policy.
  They must not define their own limits.
- The design is in [DESIGN-2394](../design/DESIGN-2394.md).
