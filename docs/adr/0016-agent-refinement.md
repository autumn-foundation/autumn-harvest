# ADR 0016: Agent refinement is offline and operator-governed

## Status

Accepted (issue #2383, epic #2369).

## Context

- The refine loop of #2369 edits the setup of an agent kind. A model
  writes each edit from the evidence of recorded runs.
- A model edit is untrusted. It can widen what the agent may do, or it
  can steer later edits.
- A live op must stay fast and predictable. A loop on its path adds
  model calls and changes behavior mid-op.
- Operators differ. A safe choice for one fleet is too strict or too
  loose for another.
- [ADR 0014](0014-operator-refine-policy.md) records the policy rule. This
  ADR records the shape of the whole loop.

## Decision

1. **Operators decide.** Each policy choice of the loop is a runtime
   operator setting of `RefinePolicy`, with a safe default. A change takes
   effect at the next read, with no restart. Only an operator writes the
   policy, and no model output carries it. So the loop cannot change it
   for itself. One floor is fixed: an overlay never clears `read_only`.
   ADR 0014 holds the detail.
2. **Offline, not inline.** The loop is the workflow `agent_refine`. It
   reads ended runs only, and it never runs on the path of an op. A
   durable op reads the active version at its start, never a waiting
   candidate. A replay probe costs no model call for the base rollout,
   and runs no tool.
3. **Overlays stay under operator ceilings.** Each stage, each probe, the
   store and the gate apply an overlay under the ceilings. The ceilings
   default to the profile values, so by default an overlay can only
   narrow. An overlay holds a prompt, guardrails, tool guidance, skills, a
   model, bounds and `read_only` only. Tools, tool rules, flow rules, a
   mandate, scripts and plugins are never overlay parts. An overlay
   refuses an unknown key when it decodes.
4. **A person adopts, and probes only read.** By default,
   `auto_adopt.parts` is empty, so each chosen overlay waits at the
   adoption gate for a person. No decision at the deadline counts as a
   reject. By default, `probe_effects` holds `read_only` only. So a live
   re-probe runs no tool that declares a write.

## Consequences

- A refinement shows up as a new overlay version, never as a change of an
  op in flight. A rollback restores the version before it.
- An operator who widens a ceiling, adds an auto-adopt part or adds a
  probe effect accepts that risk. The audit names who did it.
- A new kind of change, such as a tool, needs a review path outside the
  loop. It cannot become an overlay part without a new ADR.
- The loop needs recorded runs. A kind with no ended `agent_loop` run has
  nothing to refine.
- The guide is [section 25](../agent-adapter.md#25-refine-an-agent). The
  design is in [DESIGN-2383](../design/DESIGN-2383.md).
