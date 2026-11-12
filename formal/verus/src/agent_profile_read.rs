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
//!
//! `reloaded` picks the run of an agent after a profile reload. `current`
//! is the run, and `read` is the result of the reload read. A good read
//! replaces the run. A failed read keeps the run and returns the error. So
//! a failed reload never drops a run, and never hides its error.

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

/// The run after a reload, as a spec.
pub open spec fn spec_reloaded<R, E>(current: R, read: Result<R, E>) -> (R, Option<E>) {
    match read {
        Ok(next) => (next, None),
        Err(err) => (current, Some(err)),
    }
}

// mirror: autumn-harvest-agent/src/agent/profile_read.rs reloaded
pub fn reloaded<R, E>(current: R, read: Result<R, E>) -> (r: (R, Option<E>))
    ensures
        r == spec_reloaded(current, read),
        read is Ok ==> r == (read->Ok_0, None::<E>),
        read is Err ==> r == (current, Some(read->Err_0)),
{
    match read {
        Ok(next) => (next, None),
        Err(err) => (current, Some(err)),
    }
}

/// A failed reload keeps the run, and reports exactly its error.
pub proof fn theorem_a_failed_reload_keeps_the_run<R, E>(current: R, err: E)
    ensures
        spec_reloaded(current, Err::<R, E>(err)).0 == current,
        spec_reloaded(current, Err::<R, E>(err)).1 == Some(err),
{
}

/// A good reload always takes effect, and reports no error.
pub proof fn theorem_a_good_reload_replaces_the_run<R, E>(current: R, next: R)
    ensures
        spec_reloaded(current, Ok::<R, E>(next)) == (next, None::<E>),
{
}

/// The error slot is set exactly when the read failed.
pub proof fn theorem_the_error_is_reported_exactly_on_a_failure<R, E>(
    current: R,
    read: Result<R, E>,
)
    ensures
        spec_reloaded(current, read).1 is Some <==> read is Err,
{
}

} // verus!
