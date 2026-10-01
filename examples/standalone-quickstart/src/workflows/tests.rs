use autumn_harvest::WorkflowSimulator;
use serde_json::json;

use super::{__autumn_workflow_info_onboarding, harvest_builder};

#[test]
fn the_builder_registers_the_chapter_2_workflow_and_activity() {
    let built = harvest_builder()
        .try_build()
        .expect("the chapter 2 registrations build");

    assert_eq!(built.workflow_count(), 1);
    assert_eq!(built.activity_count(), 1);
}

#[tokio::test]
async fn onboarding_returns_the_status_of_the_welcome_email() {
    let result = WorkflowSimulator::new(__autumn_workflow_info_onboarding().handler)
        .mock_activity("send_welcome_email", |_| Ok(json!({ "status": "sent" })))
        .run(json!(42))
        .await;

    assert_eq!(
        result.final_output.expect("onboarding completes"),
        json!("sent")
    );
}
