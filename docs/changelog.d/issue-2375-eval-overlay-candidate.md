## Agents — An evaluation candidate takes a whole overlay (issue #2375, epic #2369)

`eval::Candidate::overlay` runs a recorded history under a whole
`ProfileOverlay`. The harness reads the source profile from the recorded
task and applies the overlay with `overlay::apply` before the engine
starts. A refused overlay returns `EvalError::Overlay` and costs no model
call. The default ceilings are `Ceilings::from_profile` of the source, so
an overlay can only narrow its bounds and model. `Candidate::ceilings`
sets operator ceilings. `Candidate::system_prompt` sets the overlay
prompt. `Candidate::model` sets the overlay model, admits it past the
ceilings and still asks it for every turn. `Candidate::agent_model` stays
a route. `compare` checks both overlays before the first model call.

`Evaluation::overlay_digest` holds `ProfileOverlay::digest`, a SHA-256 of
the canonical JSON. A candidate report now shows the recorded run id, so
two evaluations of one candidate compare equal. The tool guidance and the
skills of #2373 reach the candidate task, so the model sees the guided
text and the skill list.

Behaviour change: `Candidate::model` now sets the task model and checks
it before any call. A model that the harness does not hold used to fail at
the first turn. A model that the source mandate forbids used to run
unchecked. Both now fail with `EvalError::Overlay`. A delegated child
with no peer model now inherits the model.

No event, migration or SQL changes. A struct literal of `Evaluation`
needs the new field. The design is in `docs/design/DESIGN-2375.md`.

Tests: `autumn-harvest-agent/tests/eval.rs` covers each criterion of the
issue and the early refusal of `compare`. Unit tests in `eval.rs` show
that every overlay part reaches the task and that the rebase changes only
the root id and its lanes. `overlay.rs` pins the digest.
