//! An op never runs bare while an overlay is active (issue #2372).
//!
//! `op_profile` picks the profile of one op of a durable agent. `base` is
//! the harness profile. `active` is the active overlay version, with the
//! result of `overlay::apply` on it. The copy below proves three facts.
//! With no active version, the op runs on the base, with no version. With
//! an active version that applies, the op runs on the applied profile and
//! records that version. With an active version that does not apply, the
//! op fails and names the version. So no active version ever yields the
//! base with no version.

use vstd::prelude::*;

verus! {

/// The profile of an op, as a spec.
pub open spec fn spec_op_profile<P, V, E>(base: P, active: Option<(V, Result<P, E>)>) -> Result<
    (P, Option<V>),
    (V, E),
> {
    match active {
        None => Ok((base, None)),
        Some((version, Ok(applied))) => Ok((applied, Some(version))),
        Some((version, Err(err))) => Err((version, err)),
    }
}

// mirror: autumn-harvest-agent/src/agent/profile_read.rs op_profile
pub fn op_profile<P, V, E>(base: P, active: Option<(V, Result<P, E>)>) -> (r: Result<
    (P, Option<V>),
    (V, E),
>)
    ensures
        r == spec_op_profile(base, active),
        active is None ==> r == Ok::<(P, Option<V>), (V, E)>((base, None)),
        active is Some ==> (r is Ok <==> (active->0).1 is Ok),
        active is Some && r is Ok ==> (r->Ok_0).1 == Some((active->0).0),
        active is Some && r is Ok ==> (r->Ok_0).0 == (active->0).1->Ok_0,
        active is Some && r is Err ==> (r->Err_0).0 == (active->0).0,
{
    match active {
        None => Ok((base, None)),
        Some((version, Ok(applied))) => Ok((applied, Some(version))),
        Some((version, Err(err))) => Err((version, err)),
    }
}

/// An active version never yields an op with no version. So a refused
/// overlay never runs the base profile in its place.
pub proof fn theorem_an_active_version_never_runs_bare<P, V, E>(
    base: P,
    version: V,
    applied: Result<P, E>,
)
    ensures
        spec_op_profile(base, Some((version, applied))) is Ok ==> spec_op_profile(
            base,
            Some((version, applied)),
        )->Ok_0.1 is Some,
        applied is Err ==> spec_op_profile(base, Some((version, applied))) is Err,
{
}

/// The version that an op records is the version that it applied.
pub proof fn theorem_the_recorded_version_is_the_applied_one<P, V, E>(
    base: P,
    version: V,
    applied: P,
)
    ensures
        spec_op_profile(base, Some((version, Ok::<P, E>(applied)))) == Ok::<
            (P, Option<V>),
            (V, E),
        >((applied, Some(version))),
{
}

} // verus!
