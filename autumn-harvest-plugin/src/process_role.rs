//! Harvest capabilities and the placement of each Harvest runtime.
//!
//! Harvest declares three Autumn plugin capabilities. Each takes its default
//! from a built-in capability of the role:
//!
//! | Capability | Parent | Runtimes |
//! |---|---|---|
//! | `harvest.worker` | `job_workers` | the worker |
//! | `harvest.scheduler` | `scheduler` | the scheduler |
//! | `harvest.maintenance` | `maintenance` | the retention janitor, the batch executor |
//!
//! A `[roles.<name>]` preset can set each one. An explicit switch in the
//! Harvest config always wins:
//!
//! | Runtime | Explicit switch |
//! |---|---|
//! | worker | `worker_enabled` |
//! | scheduler | `scheduler_enabled` |
//! | batch executor | `worker_enabled` |
//! | retention janitor | off when retention is off, on for `run_on = "all"`, else `scheduler_enabled` |
//!
//! With no role, as in a standalone runner, each runtime is on. An explicit
//! value against the role default logs one `warn!` per switch.
//! `formal/verus/src/process_role.rs` proves that each built-in role keeps
//! the placement of 0.10.

use autumn_web::background::PluginCapability;
use autumn_web::config::{Capability, ProcessRole, Role};

use crate::config::RetentionRunOn;

/// The capability of the Harvest worker. Its default is `job_workers`.
pub const HARVEST_WORKER: PluginCapability =
    PluginCapability::new("harvest.worker", Capability::JobWorkers);

/// The capability of the Harvest scheduler. Its default is `scheduler`.
pub const HARVEST_SCHEDULER: PluginCapability =
    PluginCapability::new("harvest.scheduler", Capability::Scheduler);

/// The capability of the retention janitor and the batch executor. Its
/// default is `maintenance`.
pub const HARVEST_MAINTENANCE: PluginCapability =
    PluginCapability::new("harvest.maintenance", Capability::Maintenance);

/// Every Harvest capability.
pub const HARVEST_CAPABILITIES: [PluginCapability; 3] =
    [HARVEST_WORKER, HARVEST_SCHEDULER, HARVEST_MAINTENANCE];

/// A Harvest runtime that a role places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum HarvestLoop {
    /// The worker and its poll loops.
    Worker,
    /// The scheduler.
    Scheduler,
    /// The retention janitor.
    Retention,
    /// The batch executor.
    Batch,
}

impl HarvestLoop {
    /// Every runtime, in start order.
    pub const ALL: [Self; 4] = [Self::Worker, Self::Scheduler, Self::Retention, Self::Batch];

    /// The name of its Autumn background task.
    #[must_use]
    pub const fn task_name(self) -> &'static str {
        match self {
            Self::Worker => "harvest.worker",
            Self::Scheduler => "harvest.scheduler",
            Self::Retention => "harvest.retention",
            Self::Batch => "harvest.batch",
        }
    }

    /// The capability that places it.
    #[must_use]
    pub const fn capability(self) -> PluginCapability {
        match self {
            Self::Worker => HARVEST_WORKER,
            Self::Scheduler => HARVEST_SCHEDULER,
            Self::Retention | Self::Batch => HARVEST_MAINTENANCE,
        }
    }
}

/// The explicit switch of each Harvest runtime, from the Harvest config.
///
/// `None` lets the role decide. Each Autumn background task takes its value,
/// and so does the runner. So the two always agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopSwitches {
    worker: Option<bool>,
    scheduler: Option<bool>,
    retention: Option<bool>,
}

impl LoopSwitches {
    /// The switches of a config. `retention_enabled` is
    /// `RetentionConfig::enabled`.
    #[must_use]
    pub const fn new(
        worker: Option<bool>,
        scheduler: Option<bool>,
        retention_enabled: bool,
        run_on: RetentionRunOn,
    ) -> Self {
        Self {
            worker,
            scheduler,
            retention: retention_switch(retention_enabled, run_on, scheduler),
        }
    }

    /// The explicit switch of `runtime`.
    #[must_use]
    pub const fn explicit(self, runtime: HarvestLoop) -> Option<bool> {
        match runtime {
            HarvestLoop::Worker | HarvestLoop::Batch => self.worker,
            HarvestLoop::Scheduler => self.scheduler,
            HarvestLoop::Retention => self.retention,
        }
    }

    /// Whether `role` runs `runtime`. With no role, the default is on.
    #[must_use]
    pub fn runs(self, runtime: HarvestLoop, role: Option<&Role>) -> bool {
        switch_value(
            self.explicit(runtime),
            capability_default(role, runtime.capability()),
        )
    }
}

/// The default that `role` gives `capability`. With no role it is on.
#[must_use]
pub fn capability_default(role: Option<&Role>, capability: PluginCapability) -> bool {
    role.is_none_or(|role| role.runs_plugin(capability))
}

/// Where a resolved switch value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SwitchSource {
    /// The operator set the value in config or in the environment.
    Explicit,
    /// The capability of the process role gave the value. The field is the
    /// base of the role.
    Role(ProcessRole),
    /// No value and no role. The value is the old default, `true`.
    Default,
}

impl std::fmt::Display for SwitchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Explicit => "explicit",
            Self::Role(_) => "role",
            Self::Default => "default",
        })
    }
}

/// One resolved switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSwitch {
    /// The value that the runtime uses.
    pub enabled: bool,
    /// Where the value comes from.
    pub source: SwitchSource,
    /// True when an explicit value differs from the role default.
    pub contradicts_role: bool,
    /// The value that the role gives, or `None` with no role.
    pub role_default: Option<bool>,
}

impl ProcessSwitch {
    const fn resolve(
        explicit: Option<bool>,
        role_default: Option<bool>,
        base: Option<ProcessRole>,
    ) -> Self {
        let source = match (explicit, base) {
            (Some(_), _) => SwitchSource::Explicit,
            (None, Some(base)) => SwitchSource::Role(base),
            (None, None) => SwitchSource::Default,
        };
        let (enabled, contradicts_role) = resolve_switch(explicit, role_default);
        Self {
            enabled,
            source,
            contradicts_role,
            role_default,
        }
    }

    /// The explicit value, or `None` when the role or the default gave it.
    #[must_use]
    pub const fn explicit(&self) -> Option<bool> {
        match self.source {
            SwitchSource::Explicit => Some(self.enabled),
            SwitchSource::Role(_) | SwitchSource::Default => None,
        }
    }
}

/// The worker and scheduler switches of one process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSwitches {
    /// The base of the Autumn role, or `None` when the runtime has no
    /// `AppState`.
    pub role: Option<ProcessRole>,
    /// The name of the Autumn role: a preset name or a built-in name.
    pub role_name: Option<String>,
    /// `harvest.worker_enabled`.
    pub worker: ProcessSwitch,
    /// `harvest.scheduler_enabled`.
    pub scheduler: ProcessSwitch,
    /// The role default of `harvest.maintenance`. It is `true` with no role.
    pub maintenance: bool,
}

impl ProcessSwitches {
    /// Resolve both switches. `None` for a switch means "follow the role".
    #[must_use]
    pub fn resolve(worker: Option<bool>, scheduler: Option<bool>, role: Option<&Role>) -> Self {
        let base = role.map(Role::base);
        let default = |capability| role.map(|role| role.runs_plugin(capability));
        Self {
            role: base,
            role_name: role.map(|role| role.name().to_owned()),
            worker: ProcessSwitch::resolve(worker, default(HARVEST_WORKER), base),
            scheduler: ProcessSwitch::resolve(scheduler, default(HARVEST_SCHEDULER), base),
            maintenance: capability_default(role, HARVEST_MAINTENANCE),
        }
    }

    /// Whether this process runs the batch executor.
    #[must_use]
    pub const fn batch(&self) -> bool {
        switch_value(self.worker.explicit(), self.maintenance)
    }

    /// Whether this process runs the retention janitor.
    ///
    /// `enabled` is `RetentionConfig::enabled`. Retention that is off never
    /// runs.
    #[must_use]
    pub const fn retention(&self, enabled: bool, run_on: RetentionRunOn) -> bool {
        switch_value(
            retention_switch(enabled, run_on, self.scheduler.explicit()),
            self.maintenance,
        )
    }

    /// Log the resolved values once at startup.
    ///
    /// One `info!` names each value and its source. Each explicit value
    /// against the role adds one `warn!`, which names both values.
    pub fn log(&self) {
        let role = self.role_name.as_deref().unwrap_or("none");
        tracing::info!(
            role = %role,
            worker_enabled = self.worker.enabled,
            worker_source = %self.worker.source,
            scheduler_enabled = self.scheduler.enabled,
            scheduler_source = %self.scheduler.source,
            "harvest worker and scheduler switches resolved"
        );
        for (name, capability, switch) in [
            ("worker_enabled", HARVEST_WORKER.name(), self.worker),
            (
                "scheduler_enabled",
                HARVEST_SCHEDULER.name(),
                self.scheduler,
            ),
        ] {
            if switch.contradicts_role {
                tracing::warn!(
                    "harvest.{name} = {} contradicts the Autumn process role `{role}`, whose \
                     default for {capability} is {}. The explicit value wins. To remove this \
                     warning, set AUTUMN_ROLE to the role that gives {}, or unset harvest.{name}",
                    switch.enabled,
                    !switch.enabled,
                    switch.enabled,
                );
            }
        }
    }
}

/// The resolved value of one switch: the explicit value, else `default`.
///
/// `formal/verus/src/process_role.rs` proves the function for every input.
#[must_use]
pub(crate) const fn switch_value(explicit: Option<bool>, default: bool) -> bool {
    match explicit {
        Some(value) => value,
        None => default,
    }
}

/// True when an explicit value differs from the default of a known role.
///
/// `formal/verus/src/process_role.rs` proves the function for every input.
#[must_use]
pub(crate) const fn contradicts_role(explicit: Option<bool>, role_default: Option<bool>) -> bool {
    match (explicit, role_default) {
        (Some(value), Some(default)) => value != default,
        _ => false,
    }
}

/// The resolved value of one switch, and whether it contradicts the role.
///
/// With no role, the default is on. `formal/verus/src/process_role.rs`
/// proves the function for every input.
const fn resolve_switch(explicit: Option<bool>, role_default: Option<bool>) -> (bool, bool) {
    let default = match role_default {
        Some(default) => default,
        None => true,
    };
    let enabled = switch_value(explicit, default);
    (enabled, contradicts_role(explicit, role_default))
}

/// The explicit switch of the retention janitor.
///
/// Retention that is off never runs. `run_on = "all"` runs it on each
/// process. Otherwise it follows an explicit `scheduler_enabled`, and with
/// none the role decides. `formal/verus/src/process_role.rs` proves the
/// function for every input.
#[must_use]
pub(crate) const fn retention_switch(
    enabled: bool,
    run_on: RetentionRunOn,
    scheduler: Option<bool>,
) -> Option<bool> {
    if !enabled {
        return Some(false);
    }
    match run_on {
        RetentionRunOn::All => Some(true),
        RetentionRunOn::Scheduler => scheduler,
    }
}

/// Run `f` and return its log output at `level` and above.
#[cfg(test)]
pub(crate) fn capture_logs(level: tracing::Level, f: impl FnOnce()) -> String {
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    let buf = Buf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_max_level(level)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, f);
    let bytes = buf.0.lock().unwrap().clone();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
pub(crate) mod tests {
    use autumn_web::config::{ProcessRole, Role};

    use super::{
        HarvestLoop, LoopSwitches, ProcessSwitches, SwitchSource, capability_default,
        retention_switch,
    };
    use crate::config::{HarvestRuntimeConfig, RetentionRunOn};

    const NO_LINES: [&str; 0] = [];

    const BASES: [ProcessRole; 3] = [ProcessRole::Combined, ProcessRole::Web, ProcessRole::Worker];

    fn builtin(base: ProcessRole) -> Role {
        Role::from(base)
    }

    /// The warning lines of one `log` call.
    ///
    /// The INFO line must also be there. So an empty result means "no
    /// warning", never "the capture failed".
    fn warnings(switches: &ProcessSwitches) -> Vec<String> {
        let out = super::capture_logs(tracing::Level::INFO, || switches.log());
        assert!(
            out.contains(" INFO "),
            "the capture lost the INFO line: {out}"
        );
        out.lines()
            .filter(|line| line.contains(" WARN "))
            .map(str::to_owned)
            .collect()
    }

    /// Resolve the role that `toml` selects.
    pub(crate) fn resolve_role(toml: &str) -> Role {
        toml::from_str::<autumn_web::config::AutumnConfig>(toml)
            .expect("parses")
            .resolve_role()
            .expect("resolves")
    }

    #[test]
    fn each_runtime_has_its_task_and_capability() {
        let names: Vec<_> = HarvestLoop::ALL.iter().map(|l| l.task_name()).collect();
        assert_eq!(
            names,
            [
                "harvest.worker",
                "harvest.scheduler",
                "harvest.retention",
                "harvest.batch"
            ]
        );
        assert_eq!(HarvestLoop::Worker.capability().name(), "harvest.worker");
        assert_eq!(
            HarvestLoop::Scheduler.capability().name(),
            "harvest.scheduler"
        );
        for runtime in [HarvestLoop::Retention, HarvestLoop::Batch] {
            assert_eq!(runtime.capability().name(), "harvest.maintenance");
        }
        assert_eq!(
            super::HARVEST_WORKER.parent(),
            autumn_web::config::Capability::JobWorkers
        );
        assert_eq!(
            super::HARVEST_SCHEDULER.parent(),
            autumn_web::config::Capability::Scheduler
        );
        assert_eq!(
            super::HARVEST_MAINTENANCE.parent(),
            autumn_web::config::Capability::Maintenance
        );
    }

    /// A built-in role keeps the table of 0.10: `web` runs nothing, the
    /// others run each runtime.
    #[test]
    fn each_built_in_role_keeps_its_placement() {
        let switches = LoopSwitches::new(None, None, true, RetentionRunOn::Scheduler);
        for base in BASES {
            let role = builtin(base);
            for runtime in HarvestLoop::ALL {
                assert_eq!(
                    switches.runs(runtime, Some(&role)),
                    base != ProcessRole::Web,
                    "{base:?} {runtime:?}"
                );
            }
        }
        for runtime in HarvestLoop::ALL {
            assert!(switches.runs(runtime, None), "no role runs {runtime:?}");
        }
    }

    /// A named role sets one Harvest capability and keeps the others.
    #[test]
    fn a_named_role_sets_each_harvest_capability() {
        let cron = resolve_role(
            "role = \"cron\"\n[roles.cron]\nbase = \"worker\"\n\
             capabilities = { \"harvest.worker\" = false, \"harvest.scheduler\" = true }\n",
        );
        let switches = LoopSwitches::new(None, None, true, RetentionRunOn::Scheduler);
        assert!(!switches.runs(HarvestLoop::Worker, Some(&cron)));
        assert!(switches.runs(HarvestLoop::Scheduler, Some(&cron)));
        assert!(switches.runs(HarvestLoop::Retention, Some(&cron)));
        assert!(switches.runs(HarvestLoop::Batch, Some(&cron)));
        assert!(cron.capabilities().job_workers, "Autumn job workers stay");

        let quiet = resolve_role(
            "role = \"quiet\"\n[roles.quiet]\nbase = \"worker\"\n\
             capabilities = { \"harvest.maintenance\" = false }\n",
        );
        assert!(switches.runs(HarvestLoop::Worker, Some(&quiet)));
        assert!(!switches.runs(HarvestLoop::Retention, Some(&quiet)));
        assert!(!switches.runs(HarvestLoop::Batch, Some(&quiet)));
    }

    /// The reverse: a role with no Autumn job workers keeps the Harvest
    /// worker when it sets `harvest.worker`.
    #[test]
    fn a_named_role_keeps_the_harvest_worker_without_job_workers() {
        let harvest_only = resolve_role(
            "role = \"flows\"\n[roles.flows]\nbase = \"worker\"\njob_workers = false\n\
             capabilities = { \"harvest.worker\" = true }\n",
        );
        assert!(!harvest_only.capabilities().job_workers);
        let switches = LoopSwitches::new(None, None, true, RetentionRunOn::Scheduler);
        assert!(switches.runs(HarvestLoop::Worker, Some(&harvest_only)));

        // With no Harvest value, the worker follows `job_workers`.
        let no_jobs =
            resolve_role("role = \"api\"\n[roles.api]\nbase = \"combined\"\njob_workers = false\n");
        assert!(!switches.runs(HarvestLoop::Worker, Some(&no_jobs)));
        assert!(!capability_default(Some(&no_jobs), super::HARVEST_WORKER));
    }

    /// An explicit switch wins over each role, and the batch executor
    /// follows the worker switch.
    #[test]
    fn an_explicit_switch_wins_over_the_role() {
        let web = builtin(ProcessRole::Web);
        let worker = builtin(ProcessRole::Worker);
        let on = LoopSwitches::new(Some(true), Some(true), true, RetentionRunOn::Scheduler);
        let off = LoopSwitches::new(Some(false), Some(false), true, RetentionRunOn::Scheduler);
        for runtime in HarvestLoop::ALL {
            assert!(on.runs(runtime, Some(&web)), "{runtime:?}");
            assert!(!off.runs(runtime, Some(&worker)), "{runtime:?}");
        }
        assert_eq!(off.explicit(HarvestLoop::Batch), Some(false));
        assert_eq!(on.explicit(HarvestLoop::Retention), Some(true));
    }

    #[test]
    fn the_retention_switch_for_each_input() {
        for scheduler in [None, Some(true), Some(false)] {
            for run_on in [RetentionRunOn::Scheduler, RetentionRunOn::All] {
                assert_eq!(retention_switch(false, run_on, scheduler), Some(false));
            }
            assert_eq!(
                retention_switch(true, RetentionRunOn::All, scheduler),
                Some(true)
            );
            assert_eq!(
                retention_switch(true, RetentionRunOn::Scheduler, scheduler),
                scheduler
            );
        }
    }

    /// The runner plan and the task switches agree for each input.
    #[test]
    fn the_process_switches_agree_with_the_task_switches() {
        let roles = [
            None,
            Some(builtin(ProcessRole::Web)),
            Some(builtin(ProcessRole::Worker)),
            Some(builtin(ProcessRole::Combined)),
            Some(resolve_role(
                "role = \"cron\"\n[roles.cron]\nbase = \"worker\"\n\
                 capabilities = { \"harvest.worker\" = false }\n",
            )),
            Some(resolve_role(
                "role = \"quiet\"\n[roles.quiet]\nbase = \"combined\"\nmaintenance = false\n",
            )),
        ];
        let values = [None, Some(true), Some(false)];
        for role in &roles {
            for worker in values {
                for scheduler in values {
                    for enabled in [true, false] {
                        for run_on in [RetentionRunOn::Scheduler, RetentionRunOn::All] {
                            let process =
                                ProcessSwitches::resolve(worker, scheduler, role.as_ref());
                            let tasks = LoopSwitches::new(worker, scheduler, enabled, run_on);
                            let role = role.as_ref();
                            let case =
                                format!("{role:?} {worker:?} {scheduler:?} {enabled} {run_on:?}");
                            assert_eq!(
                                process.worker.enabled,
                                tasks.runs(HarvestLoop::Worker, role),
                                "{case}"
                            );
                            assert_eq!(
                                process.scheduler.enabled,
                                tasks.runs(HarvestLoop::Scheduler, role),
                                "{case}"
                            );
                            assert_eq!(
                                process.batch(),
                                tasks.runs(HarvestLoop::Batch, role),
                                "{case}"
                            );
                            assert_eq!(
                                process.retention(enabled, run_on),
                                tasks.runs(HarvestLoop::Retention, role),
                                "{case}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn a_web_role_with_no_value_turns_both_switches_off() {
        let web = builtin(ProcessRole::Web);
        let switches = ProcessSwitches::resolve(None, None, Some(&web));
        assert!(!switches.worker.enabled);
        assert!(!switches.scheduler.enabled);
        assert_eq!(switches.worker.source, SwitchSource::Role(ProcessRole::Web));
        assert_eq!(
            switches.scheduler.source,
            SwitchSource::Role(ProcessRole::Web)
        );
        assert_eq!(warnings(&switches), NO_LINES);
    }

    #[test]
    fn a_worker_or_combined_role_with_no_value_turns_both_switches_on() {
        for base in [ProcessRole::Worker, ProcessRole::Combined] {
            let role = builtin(base);
            let switches = ProcessSwitches::resolve(None, None, Some(&role));
            assert!(switches.worker.enabled, "{base:?}");
            assert!(switches.scheduler.enabled, "{base:?}");
            assert_eq!(switches.worker.source, SwitchSource::Role(base));
            assert_eq!(switches.scheduler.source, SwitchSource::Role(base));
            assert_eq!(warnings(&switches), NO_LINES, "{base:?}");
        }
    }

    /// A standalone runner has no `AppState`, so it has no role. Each
    /// switch keeps its old default.
    #[test]
    fn no_role_keeps_both_switches_on() {
        let switches = ProcessSwitches::resolve(None, None, None);
        assert!(switches.worker.enabled);
        assert!(switches.scheduler.enabled);
        assert!(switches.maintenance);
        assert_eq!(switches.worker.source, SwitchSource::Default);
        assert_eq!(switches.scheduler.source, SwitchSource::Default);
        assert_eq!(warnings(&switches), NO_LINES);
    }

    /// No role means nothing to contradict, so an explicit value never warns.
    #[test]
    fn an_explicit_value_with_no_role_wins_and_does_not_warn() {
        for value in [true, false] {
            let switches = ProcessSwitches::resolve(Some(value), Some(value), None);
            assert_eq!(switches.worker.enabled, value);
            assert_eq!(switches.scheduler.enabled, value);
            assert_eq!(switches.worker.source, SwitchSource::Explicit);
            assert_eq!(switches.scheduler.source, SwitchSource::Explicit);
            assert_eq!(warnings(&switches), NO_LINES, "{value}");
        }
    }

    /// An explicit value against the role gives one warning per switch. The
    /// warning names the switch, its value, the role and the capability. A
    /// value that the role also gives does not warn.
    #[test]
    fn an_override_against_the_role_warns_once_per_switch() {
        for base in BASES {
            let role = builtin(base);
            let role_default = base != ProcessRole::Web;
            for value in [true, false] {
                let switches = ProcessSwitches::resolve(Some(value), Some(value), Some(&role));
                let lines = warnings(&switches);
                if value == role_default {
                    assert_eq!(lines, NO_LINES, "{base:?} {value}");
                    continue;
                }
                assert_eq!(lines.len(), 2, "{base:?} {value}: {lines:?}");
                for (line, (switch, capability)) in lines.iter().zip([
                    ("worker_enabled", "harvest.worker"),
                    ("scheduler_enabled", "harvest.scheduler"),
                ]) {
                    assert!(line.contains(" WARN "), "{line}");
                    assert!(
                        line.contains(&format!("harvest.{switch} = {value}")),
                        "{line}"
                    );
                    assert!(
                        line.contains(&format!("role `{}`", base.as_str())),
                        "{line}"
                    );
                    assert!(
                        line.contains(&format!("default for {capability} is {role_default}")),
                        "{line}"
                    );
                    assert!(line.contains("set AUTUMN_ROLE"), "{line}");
                }
            }
        }
    }

    /// A named role that turns `harvest.worker` off makes an explicit
    /// `true` an override, and names the preset.
    #[test]
    fn an_override_against_a_named_role_names_the_preset() {
        let cron = resolve_role(
            "role = \"cron\"\n[roles.cron]\nbase = \"worker\"\n\
             capabilities = { \"harvest.worker\" = false }\n",
        );
        let switches = ProcessSwitches::resolve(Some(true), None, Some(&cron));
        let lines = warnings(&switches);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("role `cron`"), "{lines:?}");
        assert!(
            lines[0].contains("default for harvest.worker is false"),
            "{lines:?}"
        );
    }

    /// The issue's own example: `worker_enabled = true` on a `web` replica.
    /// The scheduler follows the role, so only the worker warns.
    #[test]
    fn an_explicit_worker_on_a_web_replica_warns_for_the_worker_only() {
        let web = builtin(ProcessRole::Web);
        let switches = ProcessSwitches::resolve(Some(true), None, Some(&web));
        assert!(switches.worker.enabled);
        assert!(!switches.scheduler.enabled);
        assert_eq!(switches.worker.source, SwitchSource::Explicit);
        assert_eq!(
            switches.scheduler.source,
            SwitchSource::Role(ProcessRole::Web)
        );
        let lines = warnings(&switches);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("harvest.worker_enabled = true"),
            "{lines:?}"
        );
        assert!(lines[0].contains("role `web`"), "{lines:?}");
    }

    /// An explicit `false` on a `worker` replica warns for the worker only.
    #[test]
    fn an_explicit_worker_off_on_a_worker_replica_warns_once() {
        let worker = builtin(ProcessRole::Worker);
        let switches = ProcessSwitches::resolve(Some(false), None, Some(&worker));
        assert!(!switches.worker.enabled);
        assert!(switches.scheduler.enabled);
        assert_eq!(
            switches.scheduler.source,
            SwitchSource::Role(ProcessRole::Worker)
        );
        let lines = warnings(&switches);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("harvest.worker_enabled = false"),
            "{lines:?}"
        );
    }

    /// The old API-only replica sets `false` with no `AUTUMN_ROLE`, so its
    /// role is `combined`. The warning must not tell it to unset the switch
    /// only, because that turns the worker back on.
    #[test]
    fn the_legacy_api_only_warning_names_the_role_fix() {
        let combined = builtin(ProcessRole::Combined);
        let switches = ProcessSwitches::resolve(Some(false), Some(false), Some(&combined));
        let lines = warnings(&switches);
        assert_eq!(lines.len(), 2, "{lines:?}");
        for line in &lines {
            assert!(
                line.contains("set AUTUMN_ROLE to the role that gives false"),
                "{line}"
            );
        }
    }

    /// The startup line names each value and its source.
    #[test]
    fn the_startup_line_names_each_value_and_source() {
        let web = builtin(ProcessRole::Web);
        let switches = ProcessSwitches::resolve(Some(true), None, Some(&web));
        let out = super::capture_logs(tracing::Level::INFO, || switches.log());
        let info = out
            .lines()
            .find(|line| line.contains(" INFO "))
            .unwrap_or_else(|| panic!("no INFO line: {out}"));
        for field in [
            "role=web",
            "worker_enabled=true",
            "worker_source=explicit",
            "scheduler_enabled=false",
            "scheduler_source=role",
        ] {
            assert!(info.contains(field), "{field} missing: {info}");
        }

        let standalone = super::capture_logs(tracing::Level::INFO, || {
            ProcessSwitches::resolve(None, None, None).log();
        });
        assert!(standalone.contains("role=none"), "{standalone}");
        assert!(standalone.contains("worker_source=default"), "{standalone}");
        assert!(
            standalone.contains("scheduler_source=default"),
            "{standalone}"
        );

        for base in [ProcessRole::Worker, ProcessRole::Combined] {
            let role = builtin(base);
            let out = super::capture_logs(tracing::Level::INFO, || {
                ProcessSwitches::resolve(None, Some(false), Some(&role)).log();
            });
            assert!(out.contains(&format!("role={}", base.as_str())), "{out}");
            assert!(out.contains("worker_source=role"), "{out}");
            assert!(out.contains("scheduler_source=explicit"), "{out}");
        }
    }

    #[test]
    fn the_switch_source_labels_are_stable() {
        assert_eq!(SwitchSource::Explicit.to_string(), "explicit");
        assert_eq!(SwitchSource::Role(ProcessRole::Web).to_string(), "role");
        assert_eq!(SwitchSource::Default.to_string(), "default");
    }

    /// The config methods read the loaded fields.
    #[test]
    fn the_config_resolves_its_own_fields() {
        let config = HarvestRuntimeConfig {
            worker_enabled: Some(true),
            scheduler_enabled: None,
            ..HarvestRuntimeConfig::default()
        };
        let web = builtin(ProcessRole::Web);
        assert_eq!(
            config.process_switches(Some(&web)),
            ProcessSwitches::resolve(Some(true), None, Some(&web))
        );
        assert_eq!(
            config.loop_switches(true),
            LoopSwitches::new(Some(true), None, true, RetentionRunOn::Scheduler)
        );
        let default = HarvestRuntimeConfig::default();
        assert_eq!(default.worker_enabled, None);
        assert_eq!(default.scheduler_enabled, None);
    }
}
