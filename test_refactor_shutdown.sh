sed -i.bak -e '/async fn shutdown_and_cleanup(/i \
async fn await_monitor_task(\
    worker_id: &str,\
    task: tokio::task::JoinHandle<()>,\
    name: &str,\
) {\
    if let Err(error) = task.await {\
        tracing::warn!(\
            worker_id = %worker_id,\
            error = %error,\
            "{} task failed during shutdown", name\
        );\
    }\
}\
' autumn-harvest/src/worker.rs
