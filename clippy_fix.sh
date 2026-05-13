sed -i.bak -e '/pub async fn run_with_listener(/i \
    #[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/worker.rs
sed -i.bak -e '/async fn tick_one_workflow_schedule(/i \
#[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/scheduler.rs
sed -i.bak -e '/async fn process_job(/i \
    #[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/batch.rs
sed -i.bak -e '/async fn process_workflow_task(/i \
#[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/worker.rs
sed -i.bak -e '/async fn shutdown_and_cleanup(/i \
    #[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/worker.rs
sed -i.bak -e '/async fn register_in_fleet(/i \
    #[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/worker.rs
sed -i.bak -e '/async fn poll_once(/i \
    #[allow(clippy::cognitive_complexity)]\
' autumn-harvest/src/worker.rs
