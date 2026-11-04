//! Worker and scheduler switches from the Autumn process role (issue #2477).
//!
//! `harvest.worker_enabled` and `harvest.scheduler_enabled` are optional.
//! An unset switch follows the role that `AppState::role()` gives:
//!
//! | Role | Worker | Scheduler |
//! |---|---|---|
//! | `web` | off | off |
//! | `worker`, `combined` | on | on |
//! | none (a standalone runner) | on | on |
//!
//! An explicit value always wins. A value against the role logs one `warn!`
//! per switch. `HarvestRunner::start` resolves the switches and logs them
//! once. `formal/verus/src/process_role.rs` proves the table.

use autumn_web::config::ProcessRole;

/// Where a resolved switch value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchSource {
    /// The operator set the value in config or in the environment.
    Explicit,
    /// The process role gave the value.
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
}

impl ProcessSwitch {
    const fn resolve(explicit: Option<bool>, role: Option<ProcessRole>) -> Self {
        let source = match (explicit, role) {
            (Some(_), _) => SwitchSource::Explicit,
            (None, Some(role)) => SwitchSource::Role(role),
            (None, None) => SwitchSource::Default,
        };
        Self {
            enabled: switch_value(explicit, role_default(role)),
            source,
            contradicts_role: contradicts_role(explicit, role),
        }
    }
}

/// The worker and scheduler switches of one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSwitches {
    /// The Autumn role, or `None` when the runtime has no `AppState`.
    pub role: Option<ProcessRole>,
    /// `harvest.worker_enabled`.
    pub worker: ProcessSwitch,
    /// `harvest.scheduler_enabled`.
    pub scheduler: ProcessSwitch,
}

impl ProcessSwitches {
    /// Resolve both switches. `None` for a switch means "follow the role".
    #[must_use]
    pub const fn resolve(
        worker: Option<bool>,
        scheduler: Option<bool>,
        role: Option<ProcessRole>,
    ) -> Self {
        Self {
            role,
            worker: ProcessSwitch::resolve(worker, role),
            scheduler: ProcessSwitch::resolve(scheduler, role),
        }
    }

    /// Log the resolved values once at startup.
    ///
    /// One `info!` names each value and its source. Each explicit value
    /// against the role adds one `warn!`, which names both values.
    pub fn log(&self) {
        let role = self.role.map_or("none", ProcessRole::as_str);
        tracing::info!(
            role = %role,
            worker_enabled = self.worker.enabled,
            worker_source = %self.worker.source,
            scheduler_enabled = self.scheduler.enabled,
            scheduler_source = %self.scheduler.source,
            "harvest worker and scheduler switches resolved"
        );
        for (name, switch) in [
            ("worker_enabled", self.worker),
            ("scheduler_enabled", self.scheduler),
        ] {
            if switch.contradicts_role {
                tracing::warn!(
                    "harvest.{name} = {} contradicts the Autumn process role `{role}`, whose \
                     default is {}. The explicit value wins. Unset harvest.{name} to follow \
                     the role",
                    switch.enabled,
                    role_default(self.role),
                );
            }
        }
    }
}

/// The switch value that `role` gives when no value is set.
///
/// `formal/verus/src/process_role.rs` proves the function for every input.
#[must_use]
pub const fn role_default(role: Option<ProcessRole>) -> bool {
    match role {
        Some(ProcessRole::Web) => false,
        Some(ProcessRole::Combined | ProcessRole::Worker) | None => true,
    }
}

/// The resolved value of one switch: the explicit value, else `default`.
///
/// `formal/verus/src/process_role.rs` proves the function for every input.
#[must_use]
pub const fn switch_value(explicit: Option<bool>, default: bool) -> bool {
    match explicit {
        Some(value) => value,
        None => default,
    }
}

/// True when an explicit value differs from the default of a known role.
///
/// `formal/verus/src/process_role.rs` proves the function for every input.
#[must_use]
pub const fn contradicts_role(explicit: Option<bool>, role: Option<ProcessRole>) -> bool {
    match (explicit, role) {
        (Some(value), Some(named)) => value != role_default(Some(named)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use autumn_web::config::ProcessRole;

    use super::{ProcessSwitches, SwitchSource};
    use crate::config::HarvestRuntimeConfig;

    const NO_LINES: [&str; 0] = [];

    const ROLES: [ProcessRole; 3] = [ProcessRole::Combined, ProcessRole::Web, ProcessRole::Worker];

    /// Run `f` and return its log output at `level` and above.
    fn capture(level: tracing::Level, f: impl FnOnce()) -> String {
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

    fn warnings(switches: ProcessSwitches) -> Vec<String> {
        capture(tracing::Level::WARN, || switches.log())
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn a_web_role_with_no_value_turns_both_switches_off() {
        let switches = ProcessSwitches::resolve(None, None, Some(ProcessRole::Web));
        assert!(!switches.worker.enabled);
        assert!(!switches.scheduler.enabled);
        assert_eq!(switches.worker.source, SwitchSource::Role(ProcessRole::Web));
        assert_eq!(
            switches.scheduler.source,
            SwitchSource::Role(ProcessRole::Web)
        );
        assert_eq!(warnings(switches), NO_LINES);
    }

    #[test]
    fn a_worker_or_combined_role_with_no_value_turns_both_switches_on() {
        for role in [ProcessRole::Worker, ProcessRole::Combined] {
            let switches = ProcessSwitches::resolve(None, None, Some(role));
            assert!(switches.worker.enabled, "{role:?}");
            assert!(switches.scheduler.enabled, "{role:?}");
            assert_eq!(switches.worker.source, SwitchSource::Role(role));
            assert_eq!(switches.scheduler.source, SwitchSource::Role(role));
            assert_eq!(warnings(switches), NO_LINES, "{role:?}");
        }
    }

    /// A standalone runner has no `AppState`, so it has no role. It keeps the
    /// default from before issue #2477.
    #[test]
    fn no_role_keeps_both_switches_on() {
        let switches = ProcessSwitches::resolve(None, None, None);
        assert!(switches.worker.enabled);
        assert!(switches.scheduler.enabled);
        assert_eq!(switches.worker.source, SwitchSource::Default);
        assert_eq!(switches.scheduler.source, SwitchSource::Default);
        assert_eq!(warnings(switches), NO_LINES);
    }

    /// No role means nothing to contradict, so an explicit value never warns.
    #[test]
    fn an_explicit_value_with_no_role_wins_and_does_not_warn() {
        for value in [true, false] {
            let switches = ProcessSwitches::resolve(Some(value), Some(value), None);
            assert_eq!(switches.worker.enabled, value);
            assert_eq!(switches.scheduler.enabled, value);
            assert_eq!(switches.worker.source, SwitchSource::Explicit);
            assert_eq!(warnings(switches), NO_LINES, "{value}");
        }
    }

    #[test]
    fn an_explicit_value_wins_on_each_role() {
        for role in ROLES {
            for value in [true, false] {
                let switches = ProcessSwitches::resolve(Some(value), Some(value), Some(role));
                assert_eq!(switches.worker.enabled, value, "{role:?} {value}");
                assert_eq!(switches.scheduler.enabled, value, "{role:?} {value}");
                assert_eq!(switches.worker.source, SwitchSource::Explicit);
                assert_eq!(switches.scheduler.source, SwitchSource::Explicit);
            }
        }
    }

    /// An explicit value against the role gives one warning per switch. The
    /// warning names the switch, its value and the role. A value that the
    /// role also gives does not warn.
    #[test]
    fn an_override_against_the_role_warns_once_per_switch() {
        for role in ROLES {
            let role_default = role != ProcessRole::Web;
            for value in [true, false] {
                let switches = ProcessSwitches::resolve(Some(value), Some(value), Some(role));
                let lines = warnings(switches);
                if value == role_default {
                    assert_eq!(lines, NO_LINES, "{role:?} {value}");
                    continue;
                }
                assert_eq!(lines.len(), 2, "{role:?} {value}: {lines:?}");
                for (line, switch) in lines.iter().zip(["worker_enabled", "scheduler_enabled"]) {
                    assert!(line.contains(" WARN "), "{line}");
                    assert!(
                        line.contains(&format!("harvest.{switch} = {value}")),
                        "{line}"
                    );
                    assert!(
                        line.contains(&format!("role `{}`", role.as_str())),
                        "{line}"
                    );
                    assert!(
                        line.contains(&format!("default is {role_default}")),
                        "{line}"
                    );
                }
            }
        }
    }

    /// The issue's own example: `worker_enabled = true` on a `web` replica.
    /// The scheduler follows the role, so only the worker warns.
    #[test]
    fn an_explicit_worker_on_a_web_replica_warns_for_the_worker_only() {
        let switches = ProcessSwitches::resolve(Some(true), None, Some(ProcessRole::Web));
        assert!(switches.worker.enabled);
        assert!(!switches.scheduler.enabled);
        assert_eq!(switches.worker.source, SwitchSource::Explicit);
        assert_eq!(
            switches.scheduler.source,
            SwitchSource::Role(ProcessRole::Web)
        );
        let lines = warnings(switches);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("harvest.worker_enabled = true"),
            "{lines:?}"
        );
        assert!(lines[0].contains("role `web`"), "{lines:?}");
    }

    /// The startup line names each value and its source.
    #[test]
    fn the_startup_line_names_each_value_and_source() {
        let switches = ProcessSwitches::resolve(Some(true), None, Some(ProcessRole::Web));
        let out = capture(tracing::Level::INFO, || switches.log());
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

        let standalone = capture(tracing::Level::INFO, || {
            ProcessSwitches::resolve(None, None, None).log();
        });
        assert!(standalone.contains("role=none"), "{standalone}");
        assert!(standalone.contains("worker_source=default"), "{standalone}");
    }

    #[test]
    fn the_switch_source_labels_are_stable() {
        assert_eq!(SwitchSource::Explicit.to_string(), "explicit");
        assert_eq!(SwitchSource::Role(ProcessRole::Web).to_string(), "role");
        assert_eq!(SwitchSource::Default.to_string(), "default");
    }

    /// The config method reads the two loaded fields.
    #[test]
    fn the_config_resolves_its_own_fields() {
        let config = HarvestRuntimeConfig {
            worker_enabled: Some(true),
            scheduler_enabled: None,
            ..HarvestRuntimeConfig::default()
        };
        assert_eq!(
            config.process_switches(Some(ProcessRole::Web)),
            ProcessSwitches::resolve(Some(true), None, Some(ProcessRole::Web))
        );
        let default = HarvestRuntimeConfig::default();
        assert_eq!(default.worker_enabled, None);
        assert_eq!(default.scheduler_enabled, None);
    }
}
