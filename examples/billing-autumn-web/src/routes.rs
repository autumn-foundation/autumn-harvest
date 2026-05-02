use autumn_harvest_plugin::prelude::enqueue_workflow_start_outbox;
use autumn_web::prelude::*;
use autumn_web::reexports::axum::extract::State;

use crate::domain::{
    CheckoutRequest, CheckoutStartResponse, PlanQuote, checkout_start_request, plan_quotes,
};

pub fn routes() -> Vec<autumn_web::Route> {
    routes![list_plans, start_checkout]
}

#[get("/billing/plans")]
pub async fn list_plans() -> Json<Vec<PlanQuote>> {
    Json(plan_quotes())
}

#[post("/billing/checkout")]
pub async fn start_checkout(
    State(state): State<autumn_web::AppState>,
    Json(request): Json<CheckoutRequest>,
) -> AutumnResult<Json<CheckoutStartResponse>> {
    let start = checkout_start_request(request);
    let pool = state.pool().cloned().ok_or_else(|| {
        AutumnError::service_unavailable_msg("billing checkout requires an application database")
    })?;
    let mut conn = pool
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    enqueue_workflow_start_outbox(&mut conn, &start)
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    Ok(Json(CheckoutStartResponse {
        workflow_id: start.workflow_id,
        outbox: "queued",
    }))
}
