# DESIGN-2375: an evaluation candidate takes a whole overlay

Issue #2375. Parent: #2369. Needs #2370 and #2373. Related: #2001, #2271.

## Problem

`eval::Candidate` takes a system prompt and a model. A refine loop probes
whole overlays. It needs a candidate that runs a recorded history under a
full `ProfileOverlay`. The model calls are live. No tool runs.

One criterion needs `tool_guidance`. #2373 adds it, with skills. This
change uses both parts of #2373 as they are.

## Decision

### The candidate

- `Candidate::overlay(ProfileOverlay)` sets the whole overlay. It replaces
  the parts that a shorthand set before it.
- `Candidate::system_prompt(s)` sets `overlay.system`. It is a one-part
  overlay.
- `Candidate::model(name)` sets `overlay.model`. The operator calls it, so
  the candidate ceilings admit `name`. It also asks `name` for every turn,
  as in #2271.
- `Candidate::ceilings(Ceilings)` sets the operator ceilings. The default
  is `Ceilings::from_profile` of the source profile.
- `Candidate::agent_model` stays a route for one agent. It is not an
  overlay part.

### The source profile

A recorded run holds an `AgentTask`, not an `AgentProfile`. The harness
reads the source profile from the task: the system prompt, the bounds,
`read_only`, the model, the mandate and the tool guidance. It applies the
overlay with `overlay::apply`. Then it writes the result back to the task.

`apply` runs before the engine starts, so a refused overlay costs no model
call. `evaluate` returns `EvalError::Overlay(OverlayError)`. `compare`
checks both overlays before it runs the first candidate.

The candidate report shows the recorded run id in place of the id of the
test engine. Only the root id and the ids of its lanes change. Two
evaluations of one candidate with the same replies are then equal.

### The digest

`ProfileOverlay::digest` is the SHA-256 of the JSON of the overlay, as
`sha256:<hex>`. The JSON leaves out each unset field and sorts the map
keys, so one overlay has one digest. `Evaluation::overlay_digest` records
it. An evaluation that decodes with no field gets the empty string. The
digest leaves out the ceilings and the agent routes.

### Tool guidance and skills

#2373 owns both parts. The candidate carries them like the other parts:

- The source profile reads `tool_guidance` and `skills` from the recorded
  task. `apply` merges the overlay parts over them and checks them.
- The result writes both back to the candidate task. The harness then
  rewrites the tool text and lists the skills, as on a worker.
- A recorded outcome answers a guided call by step, name and arguments,
  as before. A `load_skill` call is a tool call like any other.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Source profile | A new argument. Read it from the task. | Read it from the task. A history holds no profile. |
| Default ceilings | None. `from_profile`. Wide. | `from_profile`. A probe from a model must only narrow. |
| `Candidate::model` | A route only. An overlay part. Both. | Both. The digest names the model, and #2271 keeps every-turn routing. |
| Digest | The overlay. The applied profile. | The overlay. The issue asks for it, and #2371 stores overlays. |
| Tool guidance | Wait for #2373. Build it here. | Use #2373. It merged first, so the candidate carries its parts. |

## Reverse brainstorm

How can an overlay candidate give a wrong or unsafe report?

| Failure | Guard |
|---|---|
| A widening overlay spends model calls. | `apply` runs before the engine. A test counts 0 calls. |
| The report does not name the overlay. | `Evaluation::overlay_digest` holds the digest. |
| Two equal overlays give two digests. | The JSON is canonical. A unit test pins the empty digest. |
| Guidance changes the matching of recorded outcomes. | Guidance changes only the definition text. A test shows a replayed call. |
| Guidance names a tool that the harness does not hold. | `OverlayError::UnknownTool`. |
| A recorded task without the field replays differently. | Serde skips an empty map. |
| `compare` spends the calls of the first candidate, then refuses the second. | `compare` checks both overlays first. |
| A model-written overlay picks another model. | The default ceilings hold only the source model. |
| `overlay` after `model` keeps a stale route. | `overlay` clears the route and the admitted models. |
| A new overlay field never reaches the task. | `every_overlay_part_reaches_the_task` sets every field and reads the task. Its overlay literal names each field, so a new field fails to compile. |

## Six hats

- **White.** A recorded task holds every field that an overlay can set.
  `apply` is pure. The model mock already overrides the turn model.
- **Red.** A probe that silently ignores a part is worse than a refusal.
  Each part must reach the task, or fail.
- **Black.** `Candidate::model` now refuses an unknown model before any
  call, not at the first turn. It also refuses a model that the source
  mandate forbids, which used to run unchecked. Guidance replaces operator text, as
  `system` does, and no ceiling bounds which tools it names. The policy
  and the tool rules still decide each call. `UnknownTool` tells a
  proposer which tools the harness holds, as `UnknownModel` does for
  models. #2373 sets no byte cap on a guidance text. Each turn input
  records it, so a long text grows the history. #2394 can add a ceiling
  for each of these.
- **Yellow.** An operator can test a whole overlay against real history
  before it goes live. `compare` takes two overlays with no new code.
- **Green.** `Candidate::ceilings` lets #2394 pass the `RefinePolicy`
  ceilings.
- **Blue.** Red tests first. Then the candidate. Then the docs and the
  gates.

## Proof

This change adds no kernel rule. `apply` and its Verus model come from
#2370 and #2373 unchanged.

## Tests

- `autumn-harvest-agent/tests/eval.rs`: one test for each criterion, the
  refusals of `compare`, and the read-only and token refusals.
- `eval.rs`: every overlay part reaches the task. The rebase changes only
  the root id and its lanes.
- `overlay.rs`: the digest.
