//! A profile overlay never goes past its ceilings (issue #2370).
//!
//! `overlay::apply` merges an overlay into an agent profile. The copies of
//! `merged_bound` and `merged_read_only` below are the integer kernels of
//! `apply`. The spec `apply` models the rest of it. A model name is an
//! integer, and `DEFAULT` stands for `DEFAULT_MODEL`. `rest` stands for
//! every field that an overlay cannot name. The model leaves out the system
//! prompt, because no property reads it.

use vstd::prelude::*;

verus! {

/// Whether the bound `value` is at most `ceiling`. `None` is no bound.
pub open spec fn within(value: Option<u32>, ceiling: Option<u32>) -> bool {
    match ceiling {
        None => true,
        Some(cap) => value.is_some() && value.unwrap() <= cap,
    }
}

/// `merged_bound` as a spec.
pub open spec fn spec_merged_bound(current: Option<u32>, set: Option<u32>, ceiling: Option<u32>) -> Option<
    Option<u32>,
> {
    let next = if set.is_some() {
        set
    } else {
        current
    };
    if within(next, ceiling) {
        Some(next)
    } else {
        None
    }
}

// mirror: autumn-harvest-agent/src/agent/overlay.rs merged_bound
pub fn merged_bound(current: Option<u32>, set: Option<u32>, ceiling: Option<u32>) -> (r: Option<
    Option<u32>,
>)
    ensures
        r == spec_merged_bound(current, set, ceiling),
        r.is_some() ==> within(r.unwrap(), ceiling),
        r.is_some() && set.is_some() ==> r.unwrap() == set,
        r.is_some() && set.is_none() ==> r.unwrap() == current,
{
    let next = match set {
        Some(value) => Some(value),
        None => current,
    };
    match ceiling {
        None => Some(next),
        Some(cap) => match next {
            Some(value) if value <= cap => Some(next),
            _ => None,
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
    pub rest: int,
}

/// The fields of an overlay that the properties read.
pub struct Overlay {
    pub max_steps: Option<u32>,
    pub max_total_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub read_only: Option<bool>,
    pub model: Option<int>,
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

/// `apply` as a spec. `None` is a refusal.
pub open spec fn apply(p: Profile, o: Overlay, c: Ceilings, registry: Set<int>) -> Option<Profile> {
    let steps = spec_merged_bound(Some(p.max_steps), o.max_steps, c.max_steps);
    let total = spec_merged_bound(p.max_total_tokens, o.max_total_tokens, c.max_total_tokens);
    let output = spec_merged_bound(p.max_output_tokens, o.max_output_tokens, c.max_output_tokens);
    let read_only = spec_merged_read_only(p.read_only, o.read_only, c.allow_clear_read_only);
    let model = if o.model.is_some() {
        o.model
    } else {
        p.model
    };
    if steps.is_none() || steps.unwrap().is_none() || total.is_none() || output.is_none()
        || read_only.is_none() {
        None
    } else if o.model.is_some() && !registry.contains(o.model.unwrap()) {
        None
    } else if !c.models.contains(name(model)) {
        None
    } else {
        Some(
            Profile {
                max_steps: steps.unwrap().unwrap(),
                max_total_tokens: total.unwrap(),
                max_output_tokens: output.unwrap(),
                read_only: read_only.unwrap(),
                model,
                rest: p.rest,
            },
        )
    }
}

/// A result stays under each ceiling. It clears `read_only` only when the
/// ceilings allow it. Its model is in the ceilings, and an overlay model is
/// in the registry. Each field that an overlay cannot name keeps its value.
pub proof fn theorem_a_result_stays_under_the_ceilings(
    p: Profile,
    o: Overlay,
    c: Ceilings,
    registry: Set<int>,
)
    ensures
        apply(p, o, c, registry).is_some() ==> {
            let q = apply(p, o, c, registry).unwrap();
            &&& within(Some(q.max_steps), c.max_steps)
            &&& within(q.max_total_tokens, c.max_total_tokens)
            &&& within(q.max_output_tokens, c.max_output_tokens)
            &&& (p.read_only && !q.read_only ==> c.allow_clear_read_only)
            &&& c.models.contains(name(q.model))
            &&& (o.model.is_some() ==> registry.contains(o.model.unwrap()))
            &&& q.rest == p.rest
        },
{
}

/// Under the ceilings of the profile, no bound rises, `read_only` is never
/// cleared and the model does not change.
pub proof fn theorem_default_ceilings_only_narrow(p: Profile, o: Overlay, registry: Set<int>)
    ensures
        apply(p, o, from_profile(p), registry).is_some() ==> {
            let q = apply(p, o, from_profile(p), registry).unwrap();
            &&& q.max_steps <= p.max_steps
            &&& within(q.max_total_tokens, p.max_total_tokens)
            &&& within(q.max_output_tokens, p.max_output_tokens)
            &&& (p.read_only ==> q.read_only)
            &&& name(q.model) == name(p.model)
            &&& q.rest == p.rest
        },
{
    theorem_a_result_stays_under_the_ceilings(p, o, from_profile(p), registry);
}

/// A second apply of the same overlay changes nothing.
pub proof fn theorem_apply_is_idempotent(p: Profile, o: Overlay, c: Ceilings, registry: Set<int>)
    ensures
        apply(p, o, c, registry).is_some() ==> apply(apply(p, o, c, registry).unwrap(), o, c, registry)
            == apply(p, o, c, registry),
{
}

/// An empty overlay under the ceilings of the profile returns the profile.
pub proof fn theorem_an_empty_overlay_keeps_the_profile(p: Profile, registry: Set<int>)
    ensures
        apply(
            p,
            Overlay {
                max_steps: None,
                max_total_tokens: None,
                max_output_tokens: None,
                read_only: None,
                model: None,
            },
            from_profile(p),
            registry,
        ) == Some(p),
{
}

} // verus!
