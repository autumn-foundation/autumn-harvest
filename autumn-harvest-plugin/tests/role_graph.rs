//! `autumn graph` and `autumn jobs manifest` list the Harvest runtimes.
//!
//! The `autumn` CLI runs the app binary with a dump variable set. The app
//! prints the dump and exits, and it opens no database. Each test here runs
//! this test binary again as that app. The child test builds an app with
//! `HarvestPlugin` and calls `run`. The parent reads the dump from stdout.
//!
//! No Docker, no database.

#![allow(clippy::unused_async)]

use std::path::Path;
use std::process::Command;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use serde_json::Value;

/// The child test reads this variable. With no value it does nothing.
const CHILD: &str = "HARVEST_ROLE_GRAPH_CHILD";

/// Two named roles: one turns the Harvest worker off, one keeps it with no
/// Autumn job workers.
const AUTUMN_TOML: &str = "\
[roles.cron]
base = \"worker\"
capabilities = { \"harvest.worker\" = false, \"harvest.scheduler\" = true }

[roles.flows]
base = \"worker\"
job_workers = false
capabilities = { \"harvest.worker\" = true }
";

#[workflow]
async fn rg_echo(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
    Ok(input)
}

/// The app that the parent runs. It runs only in a child process.
#[tokio::test]
async fn role_graph_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    Box::pin(
        autumn_web::app()
            .plugin(
                HarvestPlugin::new()
                    .workflows(vec![__autumn_workflow_info_rg_echo()])
                    .worker(WorkerConfig::default().with_queues(["default", "priority"])),
            )
            .run(),
    )
    .await;
}

/// Run the child app with `dump` set, in a directory with [`AUTUMN_TOML`].
fn dump(dump: &str, dir: &Path) -> String {
    std::fs::write(dir.join("autumn.toml"), AUTUMN_TOML).expect("write autumn.toml");
    let exe = std::env::current_exe().expect("the test binary");
    let out = Command::new(exe)
        .args([
            "role_graph_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .current_dir(dir)
        .env(CHILD, "1")
        .env(dump, "1")
        .env("AUTUMN_MANIFEST_DIR", dir)
        .env_remove("AUTUMN_ROLE")
        .output()
        .expect("the child runs");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "the child failed: {}\nstdout: {stdout}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

/// A fresh directory under the target dir of this test.
fn workdir(name: &str) -> std::path::PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the work dir");
    dir
}

#[test]
fn the_graph_lists_each_harvest_task_and_the_roles_that_run_it() {
    let stdout = dump("AUTUMN_DUMP_GRAPH", &workdir("role_graph"));
    let graph = autumn_web::graph::manifest::parse_manifest_dump(&stdout)
        .unwrap_or_else(|| panic!("no graph in the child output: {stdout}"));
    let graph = serde_json::to_value(&graph).expect("the graph serializes");
    let tasks: Vec<&Value> = graph["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .filter(|node| node["kind"] == "background_task")
        .collect();
    let task = |name: &str| -> &Value {
        tasks
            .iter()
            .find(|node| node["name"] == name)
            .unwrap_or_else(|| panic!("no {name} node: {tasks:?}"))
    };
    let roles = |name: &str| task(name)["background"]["roles"].clone();
    assert_eq!(
        roles("harvest.worker"),
        serde_json::json!(["combined", "worker", "flows"])
    );
    assert_eq!(
        roles("harvest.scheduler"),
        serde_json::json!(["combined", "worker", "cron", "flows"])
    );
    for name in ["harvest.retention", "harvest.batch"] {
        assert_eq!(
            roles(name),
            serde_json::json!(["combined", "worker", "cron", "flows"]),
            "{name}"
        );
        assert_eq!(
            task(name)["background"]["capability"],
            "harvest.maintenance"
        );
    }
    let queues = task("harvest.queues");
    // A feature can declare more queues, such as `webhooks`.
    let declared = queues["background"]["queues"].as_array().expect("queues");
    for queue in ["default", "priority"] {
        assert!(declared.contains(&Value::from(queue)), "{declared:?}");
    }
    assert_eq!(
        queues["background"]["roles"],
        serde_json::json!(["combined", "worker"])
    );
    assert_eq!(
        task("harvest.worker")["background"]["parent"],
        "job_workers"
    );
}

/// `autumn doctor` reads this manifest, so it covers the Harvest queues.
#[test]
fn the_jobs_manifest_lists_the_harvest_queues() {
    let stdout = dump("AUTUMN_DUMP_JOBS", &workdir("role_jobs"));
    let start = stdout
        .find("queues = ")
        .unwrap_or_else(|| panic!("no manifest in the child output: {stdout}"));
    let line = stdout[start..].lines().next().unwrap_or_default();
    let manifest: toml::Table = toml::from_str(line).expect("the manifest parses");
    let queues: Vec<&str> = manifest["queues"]
        .as_array()
        .expect("a queue list")
        .iter()
        .filter_map(toml::Value::as_str)
        .collect();
    for queue in ["default", "priority"] {
        assert!(queues.contains(&queue), "{queue} missing: {queues:?}");
    }
}
