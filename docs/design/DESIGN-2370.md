# DESIGN-2370: the profile overlay and its ceiling kernel

Issue #2370. Parent: #2369. Runtime ceilings: #2394.

## Problem

The refine loop (#2369) edits the setup of an agent. A model writes the
edit. The edit must never go past what the operator allows.

## Decision

`autumn-harvest-agent/src/agent/overlay.rs` holds the types below and the
kernel `apply`.

- `ProfileOverlay` holds the fields that a model may set: `system`,
  `guardrails`, `model`, `max_steps`, `max_total_tokens`,
  `max_output_tokens` and `read_only`. Each field is optional. It is
  `deny_unknown_fields`, so a JSON overlay that names `tools` fails to
  decode.
- `Ceilings` holds the highest value of each bound, the allowed models and
  `allow_clear_read_only`. Under `Ceilings::from_profile`, an overlay can
  only narrow the profile.
- `ModelRegistry` tells whether the harness has a model. `AgentHarness` and
  `BTreeSet<String>` implement it.
- `OverlayError` names each refusal.
- `apply(profile, overlay, ceilings, registry)` merges the overlay. It
  refuses a breach with a named `OverlayError` variant.

### The rules of `apply`

1. A bound is the overlay value, or else the profile value. No result
   bound may be above its ceiling. A `None` ceiling sets no limit. A
   `None` bound is above every set ceiling.
2. The result clears `read_only` only when the ceilings allow it.
3. The model is the overlay model, or else the profile model. `None` is
   `DEFAULT_MODEL`. The ceilings must hold the result model. The profile
   mandate and the registry must hold an overlay model.
4. `system` is the overlay prompt, or else the profile prompt. The
   guardrails go after it, under the heading `## Guardrails`.
5. Every other field is the profile field.

`apply` checks the result, not only the overlay. A profile bound above an
operator ceiling therefore fails, even when the overlay does not name it.

`apply` checks in the variant order of `OverlayError`. The ceilings come
before the registry. So a refused model cannot learn which models the
harness holds.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| Kernel shape | A method on `AgentProfile`. A free function. | A free function, as the issue names it. |
| Registry | `&AgentHarness`. A closure. A trait. | A trait. A test can use a plain set. |
| A profile value above a ceiling | Clamp it. Refuse it. Keep it. | Refuse it. A clamp changes operator data and hides the fault. |
| Guardrails | Append always. Strip and append. Append only when absent. | Append only when the prompt does not end with the section. This is idempotent. The append never edits the text before it. |
| Verus scope | The whole kernel. The integer kernels and a model. | Mirror `merged_bound` and `merged_read_only`. Model `apply` over them. Verus cannot check a `String`. |

## Reverse brainstorm

How can an overlay escape its ceilings? Each attack has a guard.

| Attack | Guard |
|---|---|
| Name `tools`, `mandate` or `tenant`. | `deny_unknown_fields`. |
| Leave a bound unset when the profile is above an operator ceiling. | `apply` checks the result bound. |
| Send `"max_total_tokens": null` to remove a bound. | `null` decodes to unset. Unset keeps the profile bound. |
| Name `default` when the ceilings hold only one other model. | `apply` resolves `None` and compares the names. |
| Name a model that the profile mandate forbids. | `apply` refuses it with `ModelNotAllowed`. |
| Probe the registry with refused models. | The ceiling check comes first. |
| Send a long model name to flood a log. | The error keeps a cut name. |
| Apply twice to append the guardrails twice. | The append skips a prompt that ends with the section. |
| Add an overlay field that `apply` ignores. | `apply` destructures the overlay, so a new field fails to compile. |

## Six hats

- **White.** An overlay can change six fields of `AgentProfile`.
  `AgentHeartbeat::narrowed` is the pattern. Verus cannot check a
  `String`, so the proof models the model names as integers.
- **Red.** A model that writes config is a risk. A refusal is safer than
  a clamp.
- **Black.** The Verus model of `apply` can drift from the engine. The
  proptest `apply_matches_the_oracle` compares the engine with the rules
  above. Three more proptests check the theorem properties on the engine.
- **Yellow.** The setup becomes data. The proof and the proptests show
  that an overlay cannot widen a profile.
- **Green.** The fields of `Ceilings` are public, so the `RefinePolicy`
  of #2394 can set them.
- **Blue.** Red tests, then the kernel, then the proof. Then the guards:
  `cargo fmt`, `cargo clippy`, the Verus coverage suite and
  `comment-hygiene.py`.

## Limits for #2369 and #2394

The ceilings bound the fields that an overlay can set. These limits stay
outside the kernel:

- **The heartbeat.** `AgentProfile::heartbeat` has its own bounds and
  model. A tick uses them. `apply` keeps them, because a cheap tick model
  differs from the profile model in a common setup. A custom heartbeat
  from `Control::SetHeartbeat` keeps the bounds of the profile at that
  time. #2394 must narrow both when it installs a refined profile.
- **Guardrails over many rounds.** Each round of a new text adds a
  section. The refine loop applies each overlay to the base profile of the
  operator, not to the last refined profile.
- **The system prompt.** An overlay `system` replaces the operator prompt.
  No ceiling caps the length of `system` or `guardrails`. #2394 can add
  these ceilings.

## Proof

`formal/verus/src/agent_overlay.rs` mirrors `merged_bound` and
`merged_read_only`. It models `apply` with those kernels. It proves the
four properties of the issue, the corollary for `Ceilings::from_profile`
and idempotency. `theorem_apply_refuses_exactly` states when `apply`
refuses and which value it picks.

## Tests

The unit tests in `overlay.rs` cover each criterion of the issue. The
proptests check idempotency, the ceiling properties and the oracle for
random inputs.
