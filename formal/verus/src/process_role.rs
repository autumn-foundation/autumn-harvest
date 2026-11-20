//! The placement of each Harvest runtime follows the role.
//!
//! A role gives each Harvest capability a default. `harvest.worker` follows
//! `job_workers`, `harvest.scheduler` follows `scheduler`, and
//! `harvest.maintenance` follows `maintenance`. A named role can set each
//! one, but it cannot turn one on above its base. An explicit switch in the
//! Harvest config always wins. With no role, each runtime is on.
//!
//! The batch executor takes the worker switch. The retention janitor takes
//! a switch from `retention_switch`. Both take the `harvest.maintenance`
//! default. A built-in role gives each capability the 0.10 default of its
//! base, as the unit test `each_built_in_role_keeps_its_placement` checks.
//! The theorems below prove that the janitor of a built-in role then keeps
//! its 0.10 placement.

use vstd::prelude::*;

verus! {

/// `autumn_web::config::ProcessRole`.
pub enum ProcessRole {
    Combined,
    Web,
    Worker,
}

// The engine type is `Copy`. The guard bans `#[derive]`, so these impls say so.
impl Clone for ProcessRole {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl Copy for ProcessRole {}

/// `crate::config::RetentionRunOn`.
pub enum RetentionRunOn {
    Scheduler,
    All,
}

impl Clone for RetentionRunOn {
    fn clone(&self) -> (r: Self)
        ensures
            r == *self,
    {
        *self
    }
}

impl Copy for RetentionRunOn {}

/// The default of a built-in role in 0.10: a `web` role runs nothing.
pub open spec fn spec_base_default(role: Option<ProcessRole>) -> bool {
    match role {
        Some(ProcessRole::Web) => false,
        _ => true,
    }
}

/// `Role::runs_plugin` of `autumn-web`. A preset value wins, but only up to
/// the base. With no value, the role runs a plugin capability when it runs
/// its parent.
pub open spec fn spec_runs_plugin(
    preset: Option<bool>,
    runs_parent: bool,
    base_runs_parent: bool,
) -> bool {
    match preset {
        Some(value) => value && base_runs_parent,
        None => runs_parent,
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
pub open spec fn spec_contradicts(explicit: Option<bool>, role_default: Option<bool>) -> bool {
    match (explicit, role_default) {
        (Some(value), Some(default)) => value != default,
        _ => false,
    }
}

/// The default of a switch. With no role it is on.
pub open spec fn spec_default(role_default: Option<bool>) -> bool {
    match role_default {
        Some(default) => default,
        None => true,
    }
}

/// `retention_switch` as a spec.
pub open spec fn spec_retention_switch(
    enabled: bool,
    run_on: RetentionRunOn,
    scheduler: Option<bool>,
) -> Option<bool> {
    if !enabled {
        Some(false)
    } else {
        match run_on {
            RetentionRunOn::All => Some(true),
            RetentionRunOn::Scheduler => scheduler,
        }
    }
}

/// The retention placement of 0.10: the janitor runs where the resolved
/// scheduler runs, unless `run_on = "all"`.
pub open spec fn spec_old_retention(
    enabled: bool,
    run_on: RetentionRunOn,
    scheduler: bool,
) -> bool {
    enabled && match run_on {
        RetentionRunOn::Scheduler => scheduler,
        RetentionRunOn::All => true,
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
pub fn contradicts_role(explicit: Option<bool>, role_default: Option<bool>) -> (r: bool)
    ensures
        r == spec_contradicts(explicit, role_default),
{
    match (explicit, role_default) {
        (Some(value), Some(default)) => value != default,
        _ => false,
    }
}

// mirror: autumn-harvest-plugin/src/process_role.rs resolve_switch
pub fn resolve_switch(explicit: Option<bool>, role_default: Option<bool>) -> (r: (bool, bool))
    ensures
        r.0 == spec_switch_value(explicit, spec_default(role_default)),
        r.1 == spec_contradicts(explicit, role_default),
{
    let default = match role_default {
        Some(default) => default,
        None => true,
    };
    let enabled = switch_value(explicit, default);
    (enabled, contradicts_role(explicit, role_default))
}

// mirror: autumn-harvest-plugin/src/process_role.rs retention_switch
pub fn retention_switch(enabled: bool, run_on: RetentionRunOn, scheduler: Option<bool>) -> (r:
    Option<bool>)
    ensures
        r == spec_retention_switch(enabled, run_on, scheduler),
{
    if !enabled {
        return Some(false);
    }
    match run_on {
        RetentionRunOn::All => Some(true),
        RetentionRunOn::Scheduler => scheduler,
    }
}

/// An explicit value always wins over the role.
pub proof fn theorem_an_explicit_value_wins(value: bool, default: bool)
    ensures
        spec_switch_value(Some(value), default) == value,
{
}

/// No role keeps the default before issue #2477, so a standalone runner
/// keeps each runtime.
pub proof fn theorem_no_role_keeps_the_old_default()
    ensures
        spec_switch_value(None, spec_default(None)),
{
}

/// With a role, the warning fires exactly when the resolved value is not
/// the role default. With no role, nothing warns.
pub proof fn theorem_a_warning_marks_each_override(
    explicit: Option<bool>,
    role_default: Option<bool>,
)
    ensures
        role_default.is_some() ==> (spec_contradicts(explicit, role_default)
            <==> spec_switch_value(explicit, spec_default(role_default)) != spec_default(
            role_default,
        )),
        role_default.is_none() ==> !spec_contradicts(explicit, role_default),
{
}

/// A named role cannot turn on a Harvest capability that its base does not
/// run. A `web` base runs no parent, so it never runs one.
pub proof fn theorem_a_preset_stays_within_its_base(preset: Option<bool>)
    ensures
        !spec_runs_plugin(preset, false, false),
{
}

/// A named role turns each capability off on its own, and keeps the rest.
pub proof fn theorem_a_preset_turns_one_capability_off(runs_parent: bool, base_runs_parent: bool)
    ensures
        !spec_runs_plugin(Some(false), runs_parent, base_runs_parent),
        spec_runs_plugin(None, runs_parent, base_runs_parent) == runs_parent,
{
}

/// The retention janitor of a built-in role keeps the 0.10 placement. The
/// `harvest.maintenance` default and the scheduler default are equal there.
pub proof fn theorem_a_built_in_role_keeps_its_janitor(
    enabled: bool,
    run_on: RetentionRunOn,
    scheduler: Option<bool>,
    role: Option<ProcessRole>,
)
    ensures
        spec_switch_value(
            spec_retention_switch(enabled, run_on, scheduler),
            spec_base_default(role),
        ) == spec_old_retention(
            enabled,
            run_on,
            spec_switch_value(scheduler, spec_base_default(role)),
        ),
{
}

/// An explicit scheduler value moves the janitor with it, on any role.
pub proof fn theorem_an_explicit_scheduler_moves_retention(
    enabled: bool,
    scheduler: bool,
    default: bool,
)
    ensures
        spec_switch_value(
            spec_retention_switch(enabled, RetentionRunOn::Scheduler, Some(scheduler)),
            default,
        ) == (enabled && scheduler),
{
}

/// With no scheduler switch, the `harvest.maintenance` default alone places
/// the janitor. So a named role with no scheduler, but with maintenance,
/// still runs it.
pub proof fn theorem_maintenance_places_retention_with_no_switch(enabled: bool, default: bool)
    ensures
        spec_switch_value(spec_retention_switch(enabled, RetentionRunOn::Scheduler, None), default)
            == (enabled && default),
{
}

/// A `web` role with no explicit value runs no janitor.
pub proof fn theorem_a_web_role_runs_no_janitor_by_default(enabled: bool)
    ensures
        !spec_switch_value(
            spec_retention_switch(enabled, RetentionRunOn::Scheduler, None),
            spec_base_default(Some(ProcessRole::Web)),
        ),
{
}

/// `run_on = "all"` runs enabled retention on each process, whatever the
/// role does.
pub proof fn theorem_run_on_all_runs_retention_everywhere(
    enabled: bool,
    scheduler: Option<bool>,
    default: bool,
)
    ensures
        spec_switch_value(spec_retention_switch(enabled, RetentionRunOn::All, scheduler), default)
            == enabled,
{
}

/// Retention that is off never runs, on any placement.
pub proof fn theorem_disabled_retention_never_runs(
    run_on: RetentionRunOn,
    scheduler: Option<bool>,
    default: bool,
)
    ensures
        !spec_switch_value(spec_retention_switch(false, run_on, scheduler), default),
{
}

} // verus!
