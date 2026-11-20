//! Harvest runtimes as Autumn background tasks.
//!
//! The plugin declares the Harvest capabilities and registers one Autumn
//! background task per runtime. Autumn starts a task only on a role that
//! runs its capability. `/actuator/tasks` and `autumn graph` list each task
//! and the roles that run it.
//!
//! | Task | Capability | Runs |
//! |---|---|---|
//! | `harvest.worker` | `harvest.worker` | the worker |
//! | `harvest.scheduler` | `harvest.scheduler` | the scheduler |
//! | `harvest.retention` | `harvest.maintenance` | the retention janitor |
//! | `harvest.batch` | `harvest.maintenance` | the batch executor |
//! | `harvest.queues` | `harvest.worker` | nothing: it declares the queues |
//!
//! The `on_startup` hook builds the runtime. With a host, the runner spawns
//! no loop. It puts each loop that the role places in [`HostedLoops`]. Each
//! task takes its loop and runs it until Autumn cancels the task token. So
//! the Autumn drain stops each loop.
//!
//! `harvest.queues` carries the declared queues. Autumn then includes them in
//! the role `pin` check and in `autumn jobs manifest`. The worker task has no
//! queues. A task with queues runs only on a role with Autumn job workers,
//! and a named role can run the Harvest worker with none.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use autumn_web::AutumnError;
use autumn_web::app::AppBuilder;
use autumn_web::background::BackgroundTask;
use tokio_util::sync::CancellationToken;

use crate::process_role::{HARVEST_CAPABILITIES, HARVEST_WORKER, HarvestLoop, LoopSwitches};

/// The task that declares the Harvest queues.
pub const QUEUES_TASK: &str = "harvest.queues";

/// The future of one hosted loop.
type LoopFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One hosted loop: it runs until its token is cancelled.
type LoopRun = Box<dyn FnOnce(CancellationToken) -> LoopFuture + Send>;

/// The loops that the runner prepared for the Autumn background tasks.
///
/// The plugin creates one value and gives a clone to each task and to the
/// runner. The runner puts a loop. The task of that loop takes it.
#[derive(Clone, Default)]
pub(crate) struct HostedLoops {
    slots: Arc<Mutex<BTreeMap<HarvestLoop, LoopRun>>>,
}

impl std::fmt::Debug for HostedLoops {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("HostedLoops")
            .field("ready", &slots.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl HostedLoops {
    /// Put the loop of `runtime`. A later put replaces it.
    pub(crate) fn put<F, Fut>(&self, runtime: HarvestLoop, run: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let run: LoopRun = Box::new(move |stop| Box::pin(run(stop)));
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(runtime, run);
    }

    /// Take the loop of `runtime`, if the runner put one.
    fn take(&self, runtime: HarvestLoop) -> Option<LoopRun> {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&runtime)
    }

    /// Drop each loop that no task took.
    pub(crate) fn clear(&self) {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Declare the Harvest capabilities.
///
/// Each backend calls it. So `[roles.<name>] capabilities` can name a
/// Harvest capability on the SQLite backend too, which has no tasks.
pub(crate) fn declare_capabilities(app: AppBuilder) -> AppBuilder {
    HARVEST_CAPABILITIES
        .into_iter()
        .fold(app, AppBuilder::plugin_capability)
}

/// Register one background task per Harvest runtime, and `harvest.queues`.
///
/// `switches` gives the explicit switch of each task. `queues` are the
/// declared worker queues.
pub(crate) fn register(
    app: AppBuilder,
    switches: LoopSwitches,
    queues: Vec<String>,
    loops: &HostedLoops,
) -> AppBuilder {
    let app = HarvestLoop::ALL
        .into_iter()
        .fold(declare_capabilities(app), |app, runtime| {
            app.background(task_for(runtime, switches, loops))
        });
    // The worker switch also turns the queues off. A process with no worker
    // then drains no Harvest queue.
    app.background(
        BackgroundTask::new(QUEUES_TASK, HARVEST_WORKER, |_state, stop| async move {
            stop.cancelled().await;
            Ok(())
        })
        .queues(queues)
        .explicit(switches.explicit(HarvestLoop::Worker)),
    )
}

/// The background task of `runtime`.
fn task_for(runtime: HarvestLoop, switches: LoopSwitches, loops: &HostedLoops) -> BackgroundTask {
    let loops = loops.clone();
    BackgroundTask::new(
        runtime.task_name(),
        runtime.capability(),
        move |_state, stop| {
            let run = loops.take(runtime);
            async move {
                let Some(run) = run else {
                    return Err(AutumnError::service_unavailable_msg(format!(
                        "{} has no prepared loop: the Harvest runtime did not place it on \
                         this process",
                        runtime.task_name()
                    )));
                };
                run(stop).await;
                Ok(())
            }
        },
    )
    .explicit(switches.explicit(runtime))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use autumn_web::app::AppBuilder;
    use autumn_web::config::AutumnConfig;
    use autumn_web::plugin::Plugin;
    use autumn_web::test::{TestApp, TestClient};
    use serde_json::Value;

    use super::{HostedLoops, QUEUES_TASK};
    use crate::config::RetentionRunOn;
    use crate::process_role::{HarvestLoop, LoopSwitches};

    /// A plugin that registers only the Harvest tasks.
    struct Tasks {
        switches: LoopSwitches,
        loops: HostedLoops,
    }

    impl Plugin for Tasks {
        fn name(&self) -> std::borrow::Cow<'static, str> {
            "harvest-tasks".into()
        }

        fn build(self, app: AppBuilder) -> AppBuilder {
            super::register(
                app,
                self.switches,
                vec!["default".to_owned(), "priority".to_owned()],
                &self.loops,
            )
        }
    }

    /// A loop that waits for its token.
    fn idle(loops: &HostedLoops, runtime: HarvestLoop) {
        loops.put(runtime, |stop| async move { stop.cancelled().await });
    }

    /// The config of `toml`, as Autumn loads it.
    fn config(toml: &str) -> AutumnConfig {
        toml::from_str(&format!("{toml}\n[actuator]\nsensitive = true\n"))
            .expect("the config parses")
    }

    const PRESETS: &str = "\
[roles.cron]
base = \"worker\"
capabilities = { \"harvest.worker\" = false, \"harvest.scheduler\" = true }

[roles.flows]
base = \"worker\"
job_workers = false
capabilities = { \"harvest.worker\" = true }
";

    /// Build a test app with `role` and the presets, with a loop for each
    /// runtime that the role places.
    fn app(role: &str, switches: LoopSwitches) -> TestClient {
        let config = config(&format!("role = \"{role}\"\n{PRESETS}"));
        let resolved = config.resolve_role().expect("the role resolves");
        let loops = HostedLoops::default();
        for runtime in HarvestLoop::ALL {
            if switches.runs(runtime, Some(&resolved)) {
                idle(&loops, runtime);
            }
        }
        TestApp::new()
            .config(config)
            .plugin(Tasks { switches, loops })
            .build()
    }

    /// `background_tasks` of `/actuator/tasks`, by task name.
    async fn fetch_tasks(client: &TestClient) -> BTreeMap<String, Value> {
        let body: Value = client.get("/actuator/tasks").send().await.json();
        serde_json::from_value(body["background_tasks"].clone()).expect("a task map")
    }

    /// The Harvest tasks that run on this process.
    fn running(tasks: &BTreeMap<String, Value>) -> Vec<&str> {
        tasks
            .iter()
            .filter(|(name, task)| name.starts_with("harvest.") && task["runs_here"] == true)
            .map(|(name, _)| name.as_str())
            .collect()
    }

    fn default_switches() -> LoopSwitches {
        LoopSwitches::new(None, None, true, RetentionRunOn::Scheduler)
    }

    const ALL_TASKS: [&str; 5] = [
        "harvest.batch",
        QUEUES_TASK,
        "harvest.retention",
        "harvest.scheduler",
        "harvest.worker",
    ];

    /// With only the role set, a process runs exactly the Harvest tasks of
    /// its role.
    #[tokio::test]
    async fn each_built_in_role_runs_exactly_its_harvest_tasks() {
        for (role, expected) in [
            ("combined", ALL_TASKS.to_vec()),
            ("worker", ALL_TASKS.to_vec()),
            ("web", Vec::new()),
        ] {
            let client = app(role, default_switches());
            let tasks = fetch_tasks(&client).await;
            assert_eq!(running(&tasks), expected, "{role}");
            for name in &expected {
                assert_eq!(tasks[*name]["status"], "running", "{role} {name}");
            }
        }
    }

    /// A named role turns off the Harvest worker and keeps Autumn job
    /// workers.
    #[tokio::test]
    async fn a_named_role_turns_off_the_harvest_worker_only() {
        let client = app("cron", default_switches());
        let tasks = fetch_tasks(&client).await;
        assert_eq!(
            running(&tasks),
            ["harvest.batch", "harvest.retention", "harvest.scheduler"]
        );
        assert_eq!(tasks["harvest.worker"]["status"], "not_placed");
        let role = config(&format!("role = \"cron\"\n{PRESETS}"))
            .resolve_role()
            .expect("resolves");
        assert!(role.capabilities().job_workers, "Autumn job workers stay");
    }

    /// The reverse: a named role with no Autumn job workers runs the
    /// Harvest worker.
    #[tokio::test]
    async fn a_named_role_runs_the_harvest_worker_without_job_workers() {
        let client = app("flows", default_switches());
        let tasks = fetch_tasks(&client).await;
        let running = running(&tasks);
        assert!(running.contains(&"harvest.worker"), "{running:?}");
        assert_eq!(tasks["harvest.worker"]["status"], "running");
        let role = config(&format!("role = \"flows\"\n{PRESETS}"))
            .resolve_role()
            .expect("resolves");
        assert!(!role.capabilities().job_workers);
    }

    /// Each task names its capability, its parent and the roles that run it.
    #[tokio::test]
    async fn each_task_lists_the_roles_that_run_it() {
        let client = app("combined", default_switches());
        let tasks = fetch_tasks(&client).await;
        let names: Vec<&str> = tasks
            .keys()
            .map(String::as_str)
            .filter(|n| n.starts_with("harvest."))
            .collect();
        assert_eq!(names, ALL_TASKS);
        let worker = &tasks["harvest.worker"];
        assert_eq!(worker["capability"], "harvest.worker");
        assert_eq!(worker["parent"], "job_workers");
        assert_eq!(
            worker["roles"],
            serde_json::json!(["combined", "worker", "flows"])
        );
        let scheduler = &tasks["harvest.scheduler"];
        assert_eq!(scheduler["parent"], "scheduler");
        assert_eq!(
            scheduler["roles"],
            serde_json::json!(["combined", "worker", "cron", "flows"])
        );
        for name in ["harvest.retention", "harvest.batch"] {
            assert_eq!(tasks[name]["capability"], "harvest.maintenance", "{name}");
            assert_eq!(tasks[name]["parent"], "maintenance", "{name}");
        }
        let queues = &tasks[QUEUES_TASK];
        assert_eq!(queues["capability"], "harvest.worker");
        assert_eq!(queues["queues"], serde_json::json!(["default", "priority"]));
        // A task with queues needs Autumn job workers, so `flows` is absent.
        assert_eq!(queues["roles"], serde_json::json!(["combined", "worker"]));
        for name in ALL_TASKS {
            assert_eq!(tasks[name]["registered_by"], "harvest-tasks", "{name}");
        }
    }

    /// An explicit switch wins over the role.
    #[tokio::test]
    async fn an_explicit_switch_wins_over_the_role() {
        let on = LoopSwitches::new(Some(true), None, true, RetentionRunOn::Scheduler);
        let client = app("web", on);
        let tasks = fetch_tasks(&client).await;
        assert_eq!(
            running(&tasks),
            ["harvest.batch", QUEUES_TASK, "harvest.worker"]
        );

        let off = LoopSwitches::new(Some(false), None, false, RetentionRunOn::Scheduler);
        let client = app("worker", off);
        let tasks = fetch_tasks(&client).await;
        assert_eq!(running(&tasks), ["harvest.scheduler"]);
        for name in ["harvest.worker", "harvest.batch", "harvest.retention"] {
            assert_eq!(tasks[name]["status"], "disabled", "{name}");
        }
    }

    /// A task that finds no loop fails, so the actuator shows the gap.
    #[tokio::test]
    async fn a_task_with_no_loop_fails() {
        let client = TestApp::new()
            .config(config("role = \"worker\"\n"))
            .plugin(Tasks {
                switches: default_switches(),
                loops: HostedLoops::default(),
            })
            .build();
        let mut status = Value::Null;
        for _ in 0..200 {
            status = fetch_tasks(&client).await["harvest.worker"].clone();
            if status["status"] == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(status["status"], "failed", "{status}");
        let error = status["last_error"].as_str().unwrap_or_default();
        assert!(error.contains("no prepared loop"), "{error}");
    }

    /// The task token stops the hosted loop.
    #[tokio::test]
    async fn the_task_token_stops_the_loop() {
        let stopped = Arc::new(AtomicBool::new(false));
        let loops = HostedLoops::default();
        let seen = Arc::clone(&stopped);
        loops.put(HarvestLoop::Worker, move |stop| async move {
            stop.cancelled().await;
            seen.store(true, Ordering::SeqCst);
        });
        let client = TestApp::new()
            .config(config("role = \"worker\"\n"))
            .plugin(Tasks {
                switches: LoopSwitches::new(None, Some(false), false, RetentionRunOn::Scheduler),
                loops,
            })
            .build();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!stopped.load(Ordering::SeqCst), "the loop runs");
        drop(client);
        for _ in 0..200 {
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the task token did not stop the loop");
    }

    /// A role can name a Harvest capability on a backend with no tasks.
    #[tokio::test]
    async fn the_capabilities_are_declared_without_tasks() {
        struct Declare;
        impl Plugin for Declare {
            fn name(&self) -> std::borrow::Cow<'static, str> {
                "harvest-declare".into()
            }
            fn build(self, app: AppBuilder) -> AppBuilder {
                super::declare_capabilities(app)
            }
        }
        let _client = TestApp::new()
            .config(config(&format!("role = \"cron\"\n{PRESETS}")))
            .plugin(Declare)
            .build();
    }

    #[test]
    fn a_loop_is_taken_once_and_clear_drops_the_rest() {
        let loops = HostedLoops::default();
        idle(&loops, HarvestLoop::Worker);
        idle(&loops, HarvestLoop::Batch);
        assert!(loops.take(HarvestLoop::Worker).is_some());
        assert!(loops.take(HarvestLoop::Worker).is_none());
        loops.clear();
        assert!(loops.take(HarvestLoop::Batch).is_none());
        assert!(format!("{loops:?}").contains("HostedLoops"));
    }
}
