//! The queues that the Harvest worker of this process polls (issue #2479).
//!
//! Code declares the full set with `WorkerConfig::with_queues`. Two sources
//! can narrow it for one process:
//!
//! 1. `[harvest.worker] queues`, or `AUTUMN_HARVEST__WORKER__QUEUES`. Each
//!    name must be declared. An undeclared name stops the boot, so a typo
//!    cannot make a tier that polls nothing.
//! 2. The `pin` of the Autumn role. It applies only when no explicit list is
//!    set. Autumn job queues share the pin, so Harvest ignores a name that it
//!    does not declare. A pin that names no declared queue changes nothing.
//!    A process that runs the worker then logs one `warn!`.
//!
//! The worker polls the declared queues that the source selects, in the
//! declared order. The management API keeps the declared list, so a start
//! with no queue still goes to the first declared queue.
//! `formal/verus/src/worker_queues.rs` proves the source rule.

use std::collections::BTreeSet;

/// The config key, as an operator writes it.
pub const WORKER_QUEUES_KEY: &str = "harvest.worker.queues";

/// The environment variable that sets [`WORKER_QUEUES_KEY`].
pub const WORKER_QUEUES_ENV: &str = "AUTUMN_HARVEST__WORKER__QUEUES";

/// Where the polled queue list comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueueSource {
    /// No source narrows the list. The worker polls each declared queue.
    Code,
    /// `[harvest.worker] queues` or `AUTUMN_HARVEST__WORKER__QUEUES`.
    Explicit,
    /// The `pin` of the Autumn role.
    Role,
}

impl QueueSource {
    /// The stable name that logs and `GET /admin/config` show.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Code => "code",
            Self::Explicit => "explicit",
            Self::Role => "role",
        }
    }
}

impl std::fmt::Display for QueueSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An explicit list names a queue that code does not declare.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{WORKER_QUEUES_KEY} ({WORKER_QUEUES_ENV}) names queues that the code does not declare: \
     {}. Declared queues: {}. Fix the name, or declare the queue with \
     WorkerConfig::with_queues",
    .undeclared.join(", "),
    .declared.join(", ")
)]
pub struct UndeclaredQueues {
    /// Each undeclared name, in the order of the explicit list.
    pub undeclared: Vec<String>,
    /// The declared queues, in code order.
    pub declared: Vec<String>,
}

/// The resolved queues of one process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerQueues {
    declared: Vec<String>,
    polled: Vec<String>,
    source: QueueSource,
    pin: Option<Vec<String>>,
}

impl WorkerQueues {
    /// Resolve the polled queues.
    ///
    /// `declared` is the code list. `explicit` is the config list. An empty
    /// list means "not set", as for `jobs.pin`. `pin` is the queue pin of the
    /// Autumn role, or `None` with no pin.
    ///
    /// # Errors
    ///
    /// Returns [`UndeclaredQueues`] when `explicit` names a queue that
    /// `declared` does not hold.
    pub fn resolve(
        declared: &[String],
        explicit: &[String],
        pin: Option<&[String]>,
    ) -> Result<Self, UndeclaredQueues> {
        let undeclared: Vec<String> = dedupe(
            explicit
                .iter()
                .filter(|name| !declared.contains(name))
                .cloned(),
        );
        if !undeclared.is_empty() {
            return Err(UndeclaredQueues {
                undeclared,
                declared: declared.to_vec(),
            });
        }
        let pin_hits = pin.is_some_and(|pin| declared.iter().any(|name| pin.contains(name)));
        let source = queue_source(!explicit.is_empty(), pin_hits);
        let selected = match source {
            QueueSource::Explicit => explicit,
            QueueSource::Role => pin.unwrap_or_default(),
            QueueSource::Code => declared,
        };
        let polled = declared
            .iter()
            .filter(|name| selected.contains(name))
            .cloned()
            .collect();
        Ok(Self {
            declared: declared.to_vec(),
            polled,
            source,
            pin: pin.map(<[String]>::to_vec),
        })
    }

    /// The code list.
    #[must_use]
    pub fn declared(&self) -> &[String] {
        &self.declared
    }

    /// The queues that the worker polls, in code order.
    #[must_use]
    pub fn polled(&self) -> &[String] {
        &self.polled
    }

    /// Where the polled list comes from.
    #[must_use]
    pub const fn source(&self) -> QueueSource {
        self.source
    }

    /// The pin of the Autumn role, as read.
    #[must_use]
    pub fn pin(&self) -> Option<&[String]> {
        self.pin.as_deref()
    }

    /// The declared queues that this process does not poll.
    ///
    /// Another tier must poll each one. Queue coverage reports them.
    #[must_use]
    pub fn unpolled(&self) -> Vec<String> {
        self.declared
            .iter()
            .filter(|name| !self.polled.contains(name))
            .cloned()
            .collect()
    }

    /// True when the role pin names no declared queue.
    #[must_use]
    pub fn pin_misses(&self) -> bool {
        let pin_set = self.pin.as_ref().is_some_and(|pin| !pin.is_empty());
        pin_misses(
            self.source == QueueSource::Explicit,
            pin_set,
            self.source == QueueSource::Role,
        )
    }

    /// Log the resolved queues once at startup.
    ///
    /// One `info!` names the declared and the polled queues and the source.
    /// A role pin that names no declared queue adds one `warn!`, but only
    /// when the worker runs.
    pub fn log(&self, worker_enabled: bool) {
        let polled: &[String] = if worker_enabled { &self.polled } else { &[] };
        tracing::info!(
            worker_enabled,
            declared = ?self.declared,
            polled = ?polled,
            source = %self.source,
            "harvest worker queues resolved"
        );
        if worker_enabled && self.pin_misses() {
            tracing::warn!(
                "the Autumn role pin names no Harvest queue ({}), so the Harvest worker polls \
                 each declared queue ({}). To narrow it, set {WORKER_QUEUES_ENV} or \
                 [harvest.worker] queues, or add a Harvest queue to the role pin",
                self.pin.as_deref().unwrap_or_default().join(", "),
                self.declared.join(", "),
            );
        }
    }
}

/// Pick the source of the polled queues.
///
/// `explicit` is true when config names queues. `pin_hits` is true when the
/// role pin names a declared queue. `formal/verus/src/worker_queues.rs`
/// proves the function for every input.
#[must_use]
pub(crate) const fn queue_source(explicit: bool, pin_hits: bool) -> QueueSource {
    match (explicit, pin_hits) {
        (true, _) => QueueSource::Explicit,
        (false, true) => QueueSource::Role,
        (false, false) => QueueSource::Code,
    }
}

/// True when a role pin is set, no explicit list is set, and the pin names
/// no declared queue. The pin then changes nothing.
///
/// `formal/verus/src/worker_queues.rs` proves the function for every input.
#[must_use]
pub(crate) const fn pin_misses(explicit: bool, pin_set: bool, pin_hits: bool) -> bool {
    !explicit && pin_set && !pin_hits
}

/// Parse `AUTUMN_HARVEST__WORKER__QUEUES`: comma-separated names.
///
/// The parser trims each name, skips an empty item and keeps a repeated
/// name once. It skips an empty item as `AUTUMN_JOBS__PIN` does.
#[must_use]
pub fn parse_env_list(value: &str) -> Vec<String> {
    dedupe(
        value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned),
    )
}

/// The queue pin of an Autumn role, or `None` when it pins no queue.
///
/// A built-in role pins no queue. A `[roles.<name>]` preset can set `pin`.
#[must_use]
pub fn role_pin(role: &autumn_web::config::Role) -> Option<Vec<String>> {
    role.pin().map(<[String]>::to_vec)
}

/// Keep the first copy of each name, in order.
fn dedupe(names: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    names
        .into_iter()
        .filter(|name| seen.insert(name.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{QueueSource, UndeclaredQueues, WorkerQueues, parse_env_list};

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn no_source_polls_each_declared_queue() {
        let declared = names(&["default", "priority"]);
        let queues = WorkerQueues::resolve(&declared, &[], None).unwrap();
        assert_eq!(queues.polled(), declared.as_slice());
        assert_eq!(queues.declared(), declared.as_slice());
        assert_eq!(queues.source(), QueueSource::Code);
        assert_eq!(queues.unpolled(), Vec::<String>::new());
        assert!(!queues.pin_misses());
    }

    #[test]
    fn an_explicit_list_narrows_the_worker() {
        let declared = names(&["default", "priority", "bulk"]);
        let queues = WorkerQueues::resolve(&declared, &names(&["priority"]), None).unwrap();
        assert_eq!(queues.polled(), names(&["priority"]).as_slice());
        assert_eq!(queues.declared(), declared.as_slice());
        assert_eq!(queues.source(), QueueSource::Explicit);
        assert_eq!(queues.unpolled(), names(&["default", "bulk"]));
    }

    #[test]
    fn the_polled_list_keeps_the_code_order() {
        let declared = names(&["default", "priority", "bulk"]);
        let queues = WorkerQueues::resolve(&declared, &names(&["bulk", "default"]), None).unwrap();
        assert_eq!(queues.polled(), names(&["default", "bulk"]).as_slice());
    }

    #[test]
    fn an_undeclared_queue_is_refused_by_name() {
        let declared = names(&["default", "priority"]);
        let error =
            WorkerQueues::resolve(&declared, &names(&["priority", "priorty"]), None).unwrap_err();
        assert_eq!(
            error,
            UndeclaredQueues {
                undeclared: names(&["priorty"]),
                declared,
            }
        );
        let message = error.to_string();
        assert!(message.contains("priorty"), "{message}");
        assert!(
            message.contains("AUTUMN_HARVEST__WORKER__QUEUES"),
            "{message}"
        );
        assert!(message.contains("harvest.worker.queues"), "{message}");
        assert!(message.contains("default, priority"), "{message}");
    }

    #[test]
    fn a_queue_name_is_case_sensitive() {
        let declared = names(&["priority"]);
        let error = WorkerQueues::resolve(&declared, &names(&["Priority"]), None).unwrap_err();
        assert_eq!(error.undeclared, names(&["Priority"]));
    }

    #[test]
    fn a_role_pin_narrows_with_no_explicit_list() {
        let declared = names(&["default", "priority"]);
        let pin = names(&["priority", "emails"]);
        let queues = WorkerQueues::resolve(&declared, &[], Some(&pin)).unwrap();
        assert_eq!(queues.polled(), names(&["priority"]).as_slice());
        assert_eq!(queues.source(), QueueSource::Role);
        assert_eq!(queues.pin(), Some(pin.as_slice()));
        assert!(!queues.pin_misses());
    }

    #[test]
    fn an_explicit_list_wins_over_the_role_pin() {
        let declared = names(&["default", "priority"]);
        let pin = names(&["priority"]);
        let queues = WorkerQueues::resolve(&declared, &names(&["default"]), Some(&pin)).unwrap();
        assert_eq!(queues.polled(), names(&["default"]).as_slice());
        assert_eq!(queues.source(), QueueSource::Explicit);
        assert!(!queues.pin_misses());
    }

    #[test]
    fn a_pin_of_job_queues_only_keeps_the_code_set_and_warns() {
        let declared = names(&["default", "priority"]);
        let pin = names(&["emails"]);
        let queues = WorkerQueues::resolve(&declared, &[], Some(&pin)).unwrap();
        assert_eq!(queues.polled(), declared.as_slice());
        assert_eq!(queues.source(), QueueSource::Code);
        assert!(queues.pin_misses());
        let logs = crate::process_role::capture_logs(tracing::Level::WARN, || queues.log(true));
        assert!(logs.contains("emails"), "{logs}");
        assert!(logs.contains("default, priority"), "{logs}");
        assert!(logs.contains("AUTUMN_HARVEST__WORKER__QUEUES"), "{logs}");

        // A process with no worker polls nothing, so the warning is false.
        let logs = crate::process_role::capture_logs(tracing::Level::WARN, || queues.log(false));
        assert!(!logs.contains("WARN"), "{logs}");
    }

    #[test]
    fn an_empty_role_pin_changes_nothing() {
        let declared = names(&["default"]);
        let queues = WorkerQueues::resolve(&declared, &[], Some(&[])).unwrap();
        assert_eq!(queues.polled(), declared.as_slice());
        assert_eq!(queues.source(), QueueSource::Code);
        assert!(!queues.pin_misses());
    }

    #[test]
    fn the_startup_log_names_each_list_and_the_source() {
        let declared = names(&["default", "priority"]);
        let queues = WorkerQueues::resolve(&declared, &names(&["priority"]), None).unwrap();
        let logs = crate::process_role::capture_logs(tracing::Level::INFO, || queues.log(true));
        assert!(
            logs.contains(r#"declared=["default", "priority"]"#),
            "{logs}"
        );
        assert!(logs.contains(r#"polled=["priority"]"#), "{logs}");
        assert!(logs.contains("source=explicit"), "{logs}");
        assert!(!logs.contains("WARN"), "{logs}");
    }

    #[test]
    fn a_process_with_no_worker_logs_that_it_polls_nothing() {
        let declared = names(&["default", "priority"]);
        let queues = WorkerQueues::resolve(&declared, &names(&["priority"]), None).unwrap();
        let logs = crate::process_role::capture_logs(tracing::Level::INFO, || queues.log(false));
        assert!(logs.contains("worker_enabled=false"), "{logs}");
        assert!(logs.contains("polled=[]"), "{logs}");
    }

    #[test]
    fn the_env_list_is_trimmed_split_and_deduplicated() {
        assert_eq!(parse_env_list("priority"), names(&["priority"]));
        assert_eq!(
            parse_env_list(" priority , bulk,,priority ,"),
            names(&["priority", "bulk"])
        );
        assert_eq!(parse_env_list(""), Vec::<String>::new());
        assert_eq!(parse_env_list(" , "), Vec::<String>::new());
    }

    /// Resolve the role that `toml` selects.
    fn resolve_role(toml: &str) -> autumn_web::config::Role {
        toml::from_str::<autumn_web::config::AutumnConfig>(toml)
            .expect("parses")
            .resolve_role()
            .expect("resolves")
    }

    /// The pin of a `[roles.<name>]` preset narrows the worker. A built-in
    /// role and a preset with no pin change nothing.
    #[test]
    fn a_named_role_pin_reaches_the_worker() {
        let declared = names(&["default", "priority"]);
        let role = resolve_role(
            "role = \"priority\"\n[roles.priority]\nbase = \"worker\"\n\
             pin = [\"priority\", \"emails\"]\n",
        );
        let pin = super::role_pin(&role);
        assert_eq!(pin, Some(names(&["priority", "emails"])));
        let queues = WorkerQueues::resolve(&declared, &[], pin.as_deref()).unwrap();
        assert_eq!(queues.polled(), names(&["priority"]).as_slice());
        assert_eq!(queues.source(), QueueSource::Role);

        let builtin = resolve_role("role = \"worker\"\n");
        assert_eq!(super::role_pin(&builtin), None);
        let unpinned = resolve_role("role = \"cron\"\n[roles.cron]\nbase = \"worker\"\n");
        assert_eq!(super::role_pin(&unpinned), None);
    }

    #[test]
    fn the_source_kernel_matches_its_table() {
        use super::{pin_misses, queue_source};
        assert_eq!(queue_source(true, true), QueueSource::Explicit);
        assert_eq!(queue_source(true, false), QueueSource::Explicit);
        assert_eq!(queue_source(false, true), QueueSource::Role);
        assert_eq!(queue_source(false, false), QueueSource::Code);
        assert!(pin_misses(false, true, false));
        assert!(!pin_misses(true, true, false));
        assert!(!pin_misses(false, false, false));
        assert!(!pin_misses(false, true, true));
    }

    fn queue_name() -> impl Strategy<Value = String> {
        prop::sample::select(vec!["default", "priority", "bulk", "emails", "webhooks"])
            .prop_map(str::to_owned)
    }

    proptest! {
        /// Config and the role pin can only narrow the declared set. A
        /// resolved list is never empty, holds only declared queues, keeps
        /// the code order and holds no name twice.
        #[test]
        fn a_resolved_list_only_narrows_the_declared_set(
            declared in prop::collection::btree_set(queue_name(), 1..5),
            explicit in prop::collection::vec(queue_name(), 0..4),
            pin in prop::option::of(prop::collection::vec(queue_name(), 0..4)),
        ) {
            let declared: Vec<String> = declared.into_iter().collect();
            match WorkerQueues::resolve(&declared, &explicit, pin.as_deref()) {
                Ok(queues) => {
                    prop_assert!(!queues.polled().is_empty());
                    let mut cursor = 0;
                    for name in queues.polled() {
                        let at = declared[cursor..].iter().position(|d| d == name);
                        prop_assert!(at.is_some(), "{name} is out of code order");
                        cursor += at.unwrap_or_default() + 1;
                    }
                    let polled_and_unpolled = queues.polled().len() + queues.unpolled().len();
                    prop_assert_eq!(polled_and_unpolled, declared.len());
                    if !explicit.is_empty() {
                        prop_assert_eq!(queues.source(), QueueSource::Explicit);
                        for name in queues.polled() {
                            prop_assert!(explicit.contains(name));
                        }
                    }
                }
                Err(error) => {
                    prop_assert!(!error.undeclared.is_empty());
                    for name in &error.undeclared {
                        prop_assert!(explicit.contains(name));
                        prop_assert!(!declared.contains(name));
                    }
                }
            }
        }

        /// A list of declared names always resolves, and selects exactly
        /// those names.
        #[test]
        fn a_declared_selection_always_resolves(
            declared in prop::collection::btree_set(queue_name(), 1..5),
            mask in prop::collection::vec(any::<bool>(), 5),
        ) {
            let declared: Vec<String> = declared.into_iter().collect();
            let explicit: Vec<String> = declared
                .iter()
                .zip(&mask)
                .filter(|(_, keep)| **keep)
                .map(|(name, _)| name.clone())
                .collect();
            let queues = WorkerQueues::resolve(&declared, &explicit, None).unwrap();
            let expected = if explicit.is_empty() { declared } else { explicit };
            prop_assert_eq!(queues.polled(), expected.as_slice());
        }
    }
}
