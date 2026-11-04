//! The worker and scheduler switches follow the process role (issue #2477).
//!
//! `HarvestRunner::start` resolves both switches from the Autumn role. The
//! copies below prove the table for every input. A `web` role with no
//! explicit value runs no worker and no scheduler. No role keeps the old
//! default, `true`. An explicit value always wins. The startup warning fires
//! exactly when the resolved value differs from the role default.

use vstd::prelude::*;

verus! {

/// `autumn_web::config::ProcessRole`.
pub enum ProcessRole {
    Combined,
    Web,
    Worker,
}

/// The switch value that a role gives when no value is set.
pub open spec fn spec_role_default(role: Option<ProcessRole>) -> bool {
    match role {
        Some(ProcessRole::Web) => false,
        _ => true,
    }
}

/// `switch_value` as a spec.
pub open spec fn spec_switch_value(explicit: Option<bool>, default: bool) -> bool {
    match explicit {
        Some(value) => value,
        None => default,
    }
}

/// `contradicts_role` as a spec. With no role, no value contradicts it.
pub open spec fn spec_contradicts(explicit: Option<bool>, role: Option<ProcessRole>) -> bool {
    match explicit {
        Some(value) => role.is_some() && value != spec_role_default(role),
        None => false,
    }
}

// mirror: autumn-harvest-plugin/src/process_role.rs role_default
pub fn role_default(role: Option<ProcessRole>) -> (r: bool)
    ensures
        r == spec_role_default(role),
{
    match role {
        Some(ProcessRole::Web) => false,
        Some(ProcessRole::Combined | ProcessRole::Worker) | None => true,
    }
}

// mirror: autumn-harvest-plugin/src/process_role.rs switch_value
pub fn switch_value(explicit: Option<bool>, default: bool) -> (r: bool)
    ensures
        r == spec_switch_value(explicit, default),
{
    match explicit {
        Some(value) => value,
        None => default,
    }
}

// mirror: autumn-harvest-plugin/src/process_role.rs contradicts_role
pub fn contradicts_role(explicit: Option<bool>, role: Option<ProcessRole>) -> (r: bool)
    ensures
        r == spec_contradicts(explicit, role),
{
    match (explicit, role) {
        (Some(value), Some(named)) => value != role_default(Some(named)),
        _ => false,
    }
}

/// A `web` role with no explicit value runs no worker and no scheduler.
pub proof fn theorem_a_web_role_runs_nothing_by_default()
    ensures
        !spec_switch_value(None, spec_role_default(Some(ProcessRole::Web))),
{
}

/// A `worker` or `combined` role with no explicit value runs both.
pub proof fn theorem_a_worker_or_combined_role_runs_both_by_default(role: ProcessRole)
    requires
        role != ProcessRole::Web,
    ensures
        spec_switch_value(None, spec_role_default(Some(role))),
{
}

/// No role keeps the default before issue #2477, so a standalone runner
/// keeps its worker and its scheduler.
pub proof fn theorem_no_role_keeps_the_old_default()
    ensures
        spec_switch_value(None, spec_role_default(None)),
{
}

/// An explicit value always wins over the role.
pub proof fn theorem_an_explicit_value_wins(value: bool, role: Option<ProcessRole>)
    ensures
        spec_switch_value(Some(value), spec_role_default(role)) == value,
{
}

/// With a role, the warning fires exactly when the resolved value is not
/// the role default. So a value that the role gives never warns, and each
/// override against the role warns. With no role, nothing warns.
pub proof fn theorem_a_warning_marks_each_override(
    explicit: Option<bool>,
    role: Option<ProcessRole>,
)
    ensures
        role.is_some() ==> (spec_contradicts(explicit, role) <==> spec_switch_value(
            explicit,
            spec_role_default(role),
        ) != spec_role_default(role)),
        role.is_none() ==> !spec_contradicts(explicit, role),
        explicit.is_none() ==> !spec_contradicts(explicit, role),
{
}

} // verus!
