//! The worker and scheduler switches follow the process role (issue #2477).
//!
//! `HarvestRunner::start` resolves both switches from the Autumn role. The
//! copies below prove the table for every input. `resolve_switch` proves that
//! the runtime joins the kernels with the right arguments. A `web` role with no
//! explicit value runs no worker and no scheduler. No role keeps the old
//! default, `true`. An explicit value always wins. The startup warning fires
//! exactly when the resolved value differs from the role default.
//!
//! Issue #2478 adds the retention placement. With `run_on = "scheduler"`,
//! retention runs exactly where the resolved scheduler runs. So a `web` role
//! with no explicit value runs no retention. With `run_on = "all"`, retention
//! runs on each process, as it did before. Retention that is off never runs.

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

// mirror: autumn-harvest-plugin/src/process_role.rs resolve_switch
pub fn resolve_switch(explicit: Option<bool>, role: Option<ProcessRole>) -> (r: (bool, bool))
    ensures
        r.0 == spec_switch_value(explicit, spec_role_default(role)),
        r.1 == spec_contradicts(explicit, role),
{
    let enabled = switch_value(explicit, role_default(role));
    (enabled, contradicts_role(explicit, role))
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
{
}

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

/// `retention_placed` as a spec.
pub open spec fn spec_retention_placed(run_on: RetentionRunOn, scheduler: bool) -> bool {
    match run_on {
        RetentionRunOn::Scheduler => scheduler,
        RetentionRunOn::All => true,
    }
}

/// `runs_retention` as a spec.
pub open spec fn spec_runs_retention(
    enabled: bool,
    run_on: RetentionRunOn,
    scheduler: bool,
) -> bool {
    enabled && spec_retention_placed(run_on, scheduler)
}

// mirror: autumn-harvest-plugin/src/process_role.rs retention_placed
pub fn retention_placed(run_on: RetentionRunOn, scheduler: bool) -> (r: bool)
    ensures
        r == spec_retention_placed(run_on, scheduler),
{
    match run_on {
        RetentionRunOn::Scheduler => scheduler,
        RetentionRunOn::All => true,
    }
}

// mirror: autumn-harvest-plugin/src/process_role.rs runs_retention
pub fn runs_retention(enabled: bool, run_on: RetentionRunOn, scheduler: bool) -> (r: bool)
    ensures
        r == spec_runs_retention(enabled, run_on, scheduler),
{
    enabled && retention_placed(run_on, scheduler)
}

/// With `run_on = "scheduler"`, retention runs exactly where the scheduler
/// runs.
pub proof fn theorem_retention_follows_the_scheduler(enabled: bool, scheduler: bool)
    ensures
        spec_runs_retention(enabled, RetentionRunOn::Scheduler, scheduler) == (enabled
            && scheduler),
{
}

/// A `web` role with no explicit scheduler value and the default
/// placement runs no retention.
pub proof fn theorem_a_web_role_runs_no_retention_by_default(enabled: bool)
    ensures
        !spec_runs_retention(
            enabled,
            RetentionRunOn::Scheduler,
            spec_switch_value(None, spec_role_default(Some(ProcessRole::Web))),
        ),
{
}

/// A `worker` or `combined` role, or no role, runs enabled retention with
/// no explicit scheduler value and the default placement.
pub proof fn theorem_other_roles_run_retention_by_default(role: Option<ProcessRole>)
    requires
        role != Some(ProcessRole::Web),
    ensures
        spec_runs_retention(
            true,
            RetentionRunOn::Scheduler,
            spec_switch_value(None, spec_role_default(role)),
        ),
{
}

/// `run_on = "all"` runs enabled retention on each process, whatever the
/// scheduler does. That is the behaviour before issue #2478.
pub proof fn theorem_run_on_all_runs_retention_everywhere(enabled: bool, scheduler: bool)
    ensures
        spec_runs_retention(enabled, RetentionRunOn::All, scheduler) == enabled,
{
}

/// Retention that is off never runs, on any placement.
pub proof fn theorem_disabled_retention_never_runs(run_on: RetentionRunOn, scheduler: bool)
    ensures
        !spec_runs_retention(false, run_on, scheduler),
{
}

} // verus!
