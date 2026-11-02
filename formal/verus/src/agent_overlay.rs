//! A profile overlay never goes past its ceilings (issue #2370).
//!
//! `overlay::apply` merges an overlay into an agent profile. The copies of
//! `merged_bound` and `merged_read_only` below are the integer kernels of
//! `apply`. The spec `apply` models the rest of it. A model name is an
//! integer, and `default_model` stands for `DEFAULT_MODEL`. `allowed` is
//! the model allowlist of the profile mandate. `rest` stands for every other
//! field that an overlay cannot name. The model carries `rest` unchanged.
//! The proptests in `overlay.rs` check that the engine does the same.
//! The model leaves out the system prompt. The proptests check its
//! idempotency.
//!
//! Issue #2373 adds tool guidance and skills. A guidance key is an integer,
//! and `tools` is the set of registered tools. The model leaves out the
//! guidance text and the skills. The unit tests in `overlay.rs` check them.
//! `guided` models the tool definition that the harness sends.

use vstd::prelude::*;

verus! {

/// Whether the bound `value` is at most `ceiling`. A `None` ceiling sets no
/// limit. A `None` value is above every set ceiling.
pub open spec fn within(value: Option<u32>, ceiling: Option<u32>) -> bool {
    match ceiling {
        None => true,
        Some(cap) => value.is_some() && value.unwrap() <= cap,
    }
}

/// `merged_bound` as a spec.
pub open spec fn spec_merged_bound(current: Option<u32>, set: Option<u32>, ceiling: Option<u32>) -> Result<
    Option<u32>,
    (),
> {
    let next = if set.is_some() {
        set
    } else {
        current
    };
    if within(next, ceiling) {
        Ok(next)
    } else {
        Err(())
    }
}

// mirror: autumn-harvest-agent/src/agent/overlay.rs merged_bound
pub fn merged_bound(current: Option<u32>, set: Option<u32>, ceiling: Option<u32>) -> (r: Result<
    Option<u32>,
    (),
>)
    ensures
        r == spec_merged_bound(current, set, ceiling),
        r is Ok ==> within(r->Ok_0, ceiling),
        r is Ok && set.is_some() ==> r->Ok_0 == set,
        r is Ok && set.is_none() ==> r->Ok_0 == current,
{
    let next = match set {
        Some(value) => Some(value),
        None => current,
    };
    match ceiling {
        None => Ok(next),
        Some(cap) => match next {
            Some(value) if value <= cap => Ok(next),
            _ => Err(()),
        },
    }
}

/// `merged_read_only` as a spec.
pub open spec fn spec_merged_read_only(current: bool, set: Option<bool>, allow_clear: bool) -> Option<
    bool,
> {
    let next = if set.is_some() {
        set.unwrap()
    } else {
        current
    };
    if current && !next && !allow_clear {
        None
    } else {
        Some(next)
    }
}

// mirror: autumn-harvest-agent/src/agent/overlay.rs merged_read_only
pub fn merged_read_only(current: bool, set: Option<bool>, allow_clear: bool) -> (r: Option<bool>)
    ensures
        r == spec_merged_read_only(current, set, allow_clear),
        r.is_some() && current && !r.unwrap() ==> allow_clear,
        r.is_some() && set.is_none() ==> r.unwrap() == current,
{
    let next = match set {
        Some(value) => value,
        None => current,
    };
    if current && !next && !allow_clear {
        None
    } else {
        Some(next)
    }
}

/// The name of `DEFAULT_MODEL`.
pub open spec fn default_model() -> int {
    0
}

/// The name of a model field. `None` is the default model.
pub open spec fn name(model: Option<int>) -> int {
    if model.is_some() {
        model.unwrap()
    } else {
        default_model()
    }
}

/// The fields of a profile that the properties read.
pub struct Profile {
    pub max_steps: u32,
    pub max_total_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub read_only: bool,
    pub model: Option<int>,
    pub allowed: Option<Set<int>>,
    pub guidance: Set<int>,
    pub rest: int,
}

/// The fields of an overlay that the properties read.
pub struct Overlay {
    pub max_steps: Option<u32>,
    pub max_total_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub read_only: Option<bool>,
    pub model: Option<int>,
    pub guidance: Set<int>,
}

/// The ceilings of a result.
pub struct Ceilings {
    pub max_steps: Option<u32>,
    pub max_total_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub models: Set<int>,
    pub allow_clear_read_only: bool,
}

/// `Ceilings::from_profile` as a spec.
pub open spec fn from_profile(p: Profile) -> Ceilings {
    Ceilings {
        max_steps: Some(p.max_steps),
        max_total_tokens: p.max_total_tokens,
        max_output_tokens: p.max_output_tokens,
        models: set![name(p.model)],
        allow_clear_read_only: false,
    }
}

/// Whether the mandate allowlist `allowed` holds `model`. `None` holds
/// every model.
pub open spec fn allows(allowed: Option<Set<int>>, model: int) -> bool {
    allowed.is_none() || allowed.unwrap().contains(model)
}

/// `apply` as a spec. `None` is a refusal. `registry` holds the models of
/// the harness, and `tools` holds its registered tools.
pub open spec fn apply(p: Profile, o: Overlay, c: Ceilings, registry: Set<int>, tools: Set<int>) -> Option<
    Profile,
> {
    let steps = spec_merged_bound(Some(p.max_steps), o.max_steps, c.max_steps);
    let total = spec_merged_bound(p.max_total_tokens, o.max_total_tokens, c.max_total_tokens);
    let output = spec_merged_bound(p.max_output_tokens, o.max_output_tokens, c.max_output_tokens);
    let read_only = spec_merged_read_only(p.read_only, o.read_only, c.allow_clear_read_only);
    let model = if o.model.is_some() {
        o.model
    } else {
        p.model
    };
    let forbidden = o.model.is_some() && !allows(p.allowed, o.model.unwrap());
    if !(steps is Ok) || (steps->Ok_0).is_none() || !(total is Ok) || !(output is Ok)
        || read_only.is_none() {
        None
    } else if forbidden || !c.models.contains(name(model)) {
        None
    } else if o.model.is_some() && !registry.contains(o.model.unwrap()) {
        None
    } else if !p.guidance.union(o.guidance).subset_of(tools) {
        None
    } else {
        Some(
            Profile {
                max_steps: (steps->Ok_0).unwrap(),
                max_total_tokens: total->Ok_0,
                max_output_tokens: output->Ok_0,
                read_only: read_only.unwrap(),
                model,
                allowed: p.allowed,
                guidance: p.guidance.union(o.guidance),
                rest: p.rest,
            },
        )
    }
}

/// `apply` refuses exactly when a rule fails. Otherwise it returns the
/// overlay value of each field, or else the profile value.
pub proof fn theorem_apply_refuses_exactly(
    p: Profile,
    o: Overlay,
    c: Ceilings,
    registry: Set<int>,
    tools: Set<int>,
)
    ensures
        ({
            let steps = if o.max_steps.is_some() {
                o.max_steps.unwrap()
            } else {
                p.max_steps
            };
            let total = if o.max_total_tokens.is_some() {
                o.max_total_tokens
            } else {
                p.max_total_tokens
            };
            let output = if o.max_output_tokens.is_some() {
                o.max_output_tokens
            } else {
                p.max_output_tokens
            };
            let read_only = if o.read_only.is_some() {
                o.read_only.unwrap()
            } else {
                p.read_only
            };
            let model = if o.model.is_some() {
                o.model
            } else {
                p.model
            };
            let ok = within(Some(steps), c.max_steps) && within(total, c.max_total_tokens)
                && within(output, c.max_output_tokens) && !(p.read_only && !read_only
                && !c.allow_clear_read_only) && (o.model.is_some() ==> allows(
                p.allowed,
                o.model.unwrap(),
            ) && registry.contains(o.model.unwrap())) && c.models.contains(name(model))
                && p.guidance.union(o.guidance).subset_of(tools);
            &&& apply(p, o, c, registry, tools).is_some() == ok
            &&& ok ==> apply(p, o, c, registry, tools) == Some(
                Profile {
                    max_steps: steps,
                    max_total_tokens: total,
                    max_output_tokens: output,
                    read_only,
                    model,
                    allowed: p.allowed,
                    guidance: p.guidance.union(o.guidance),
                    rest: p.rest,
                },
            )
        }),
{
}

/// No result bound is above its ceiling. A result clears `read_only` only
/// when the ceilings allow it. Its model is in the ceilings. The registry
/// and the mandate hold an overlay model. Each field that an overlay cannot
/// name keeps its value. Each guidance key names a registered tool.
pub proof fn theorem_a_result_stays_under_the_ceilings(
    p: Profile,
    o: Overlay,
    c: Ceilings,
    registry: Set<int>,
    tools: Set<int>,
)
    ensures
        apply(p, o, c, registry, tools).is_some() ==> {
            let q = apply(p, o, c, registry, tools).unwrap();
            &&& within(Some(q.max_steps), c.max_steps)
            &&& within(q.max_total_tokens, c.max_total_tokens)
            &&& within(q.max_output_tokens, c.max_output_tokens)
            &&& (p.read_only && !q.read_only ==> c.allow_clear_read_only)
            &&& c.models.contains(name(q.model))
            &&& (o.model.is_some() ==> registry.contains(o.model.unwrap()))
            &&& (o.model.is_some() ==> allows(p.allowed, o.model.unwrap()))
            &&& q.allowed == p.allowed
            &&& q.rest == p.rest
            &&& q.guidance.subset_of(tools)
            &&& o.guidance.subset_of(q.guidance)
        },
{
}

/// Under the ceilings of the profile, no bound rises. An overlay never
/// clears `read_only`, and the model does not change.
pub proof fn theorem_default_ceilings_only_narrow(
    p: Profile,
    o: Overlay,
    registry: Set<int>,
    tools: Set<int>,
)
    ensures
        apply(p, o, from_profile(p), registry, tools).is_some() ==> {
            let q = apply(p, o, from_profile(p), registry, tools).unwrap();
            &&& q.max_steps <= p.max_steps
            &&& within(q.max_total_tokens, p.max_total_tokens)
            &&& within(q.max_output_tokens, p.max_output_tokens)
            &&& (p.read_only ==> q.read_only)
            &&& name(q.model) == name(p.model)
            &&& q.allowed == p.allowed
            &&& q.rest == p.rest
        },
{
    theorem_a_result_stays_under_the_ceilings(p, o, from_profile(p), registry, tools);
}

/// A second apply of the same overlay changes nothing.
pub proof fn theorem_apply_is_idempotent(
    p: Profile,
    o: Overlay,
    c: Ceilings,
    registry: Set<int>,
    tools: Set<int>,
)
    ensures
        apply(p, o, c, registry, tools).is_some() ==> apply(
            apply(p, o, c, registry, tools).unwrap(),
            o,
            c,
            registry,
            tools,
        ) == apply(p, o, c, registry, tools),
{
    let merged = p.guidance.union(o.guidance);
    assert(merged.union(o.guidance) =~= merged);
}

/// An empty overlay under the ceilings of the profile returns the profile,
/// when each guidance key of the profile names a registered tool.
pub proof fn theorem_an_empty_overlay_keeps_the_profile(p: Profile, registry: Set<int>, tools: Set<
    int,
>)
    requires
        p.guidance.subset_of(tools),
    ensures
        apply(
            p,
            Overlay {
                max_steps: None,
                max_total_tokens: None,
                max_output_tokens: None,
                read_only: None,
                model: None,
                guidance: Set::empty(),
            },
            from_profile(p),
            registry,
            tools,
        ) == Some(p),
{
    assert(p.guidance.union(Set::empty()) =~= p.guidance);
}

/// The fields of a tool definition that guidance can touch. `builtin` marks
/// a tool of the crate, such as `load_skill`.
pub struct ToolDef {
    pub name: int,
    pub description: int,
    pub effect: int,
    pub builtin: bool,
}

/// The definition that the harness sends for `t` under `guidance`. The
/// guidance text of an app tool replaces its description. A built-in keeps
/// its own text. This spec models `guided` in `harness.rs`. No mirror ties
/// them, so the tests in `tests/skills.rs` check the engine.
pub open spec fn guided(t: ToolDef, guidance: Map<int, int>) -> ToolDef {
    if !t.builtin && guidance.contains_key(t.name) {
        ToolDef { name: t.name, description: guidance[t.name], effect: t.effect, builtin: false }
    } else {
        t
    }
}

/// Guidance changes only the description of an app tool. The name and the
/// effect stay, so the policy decides as it did. A built-in, or a tool with
/// no guidance, keeps its whole definition.
pub proof fn theorem_guidance_changes_only_the_description(t: ToolDef, guidance: Map<int, int>)
    ensures
        guided(t, guidance).name == t.name,
        guided(t, guidance).effect == t.effect,
        guided(t, guidance).builtin == t.builtin,
        !t.builtin && guidance.contains_key(t.name) ==> guided(t, guidance).description
            == guidance[t.name],
        t.builtin || !guidance.contains_key(t.name) ==> guided(t, guidance) == t,
{
}

} // verus!
