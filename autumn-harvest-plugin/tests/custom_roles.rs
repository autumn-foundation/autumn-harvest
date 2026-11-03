//! Custom roles on a standalone mount (issue #1978).
//!
//! These tests drive `StandaloneAdminAuth::with_roles` over the real API and
//! Vantage routers. No database is needed. An allowed request reaches its
//! handler, which may answer `503` without a store. A denied request gets
//! `403` from the role layer before any handler runs.
//!
//! The checks live in `support/role_contract.rs`. The SQLite suite runs the
//! same checks on a SQLite app (issue #2469).

use std::collections::HashMap;

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_harvest_plugin::roles::{RoleGrant, SESSION_ROLES_KEY};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::extract::Request;
use axum::middleware::Next;

#[path = "support/role_contract.rs"]
mod role_contract;

use role_contract::{MOUNT, RoleBackend, Source};

/// The API and Vantage routers behind the role layer, in the `prod` profile
/// with no auth boundary. Host middleware outside the stack sets the roles.
struct Standalone;

impl RoleBackend for Standalone {
    fn app(&self, source: Source) -> axum::Router {
        let api_state = HarvestApiState::new();
        let router =
            harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
        let auth = StandaloneAdminAuth::new()
            .with_deployment_profile("prod")
            .with_roles(role_contract::roles());
        let mounted = auth
            .mount(router, &api_state)
            .layer(axum::middleware::from_fn(
                move |mut request: Request, next: Next| {
                    let source = source.clone();
                    async move {
                        give_roles(&mut request, source);
                        next.run(request).await
                    }
                },
            ));
        axum::Router::new().nest(MOUNT, mounted)
    }

    fn bare(&self) -> axum::Router {
        let api_state = HarvestApiState::new();
        let router = harvest_api_router(api_state.clone());
        let mounted = StandaloneAdminAuth::new()
            .with_deployment_profile("prod")
            .mount(router, &api_state);
        axum::Router::new().nest(MOUNT, mounted)
    }
}

/// Put the roles of `source` on `request`, as host auth does.
fn give_roles(request: &mut Request, source: Source) {
    let (session, grant) = match source {
        Source::None => (None, None),
        Source::Session(s) => (Some(s), None),
        Source::Grant(g) => (None, Some(g)),
        Source::Both { session, grant } => (Some(session), Some(grant)),
    };
    if let Some(value) = session {
        let mut data = HashMap::new();
        data.insert(SESSION_ROLES_KEY.to_string(), value.to_string());
        request
            .extensions_mut()
            .insert(Session::new_for_test("roles".to_string(), data));
    }
    if let Some(grant) = grant {
        request.extensions_mut().insert(RoleGrant::new(grant));
    }
}

#[tokio::test]
async fn viewer_reads_the_api_and_vantage() {
    role_contract::viewer_reads_the_api_and_vantage(&Standalone).await;
}

#[tokio::test]
async fn viewer_is_denied_mutations_with_a_role_403() {
    role_contract::viewer_is_denied_mutations_with_a_role_403(&Standalone).await;
}

#[tokio::test]
async fn a_custom_role_reaches_only_its_extra_route() {
    role_contract::a_custom_role_reaches_only_its_extra_route(&Standalone).await;
}

#[tokio::test]
async fn a_role_admits_an_admin_gated_read_without_a_boundary() {
    role_contract::a_role_admits_an_admin_gated_read(&Standalone).await;
}

#[tokio::test]
async fn an_operator_passes_the_mutation_gate_without_a_boundary() {
    role_contract::an_operator_passes_the_mutation_gate(&Standalone).await;
}

#[tokio::test]
async fn no_roles_and_unknown_roles_are_denied() {
    role_contract::no_roles_and_unknown_roles_are_denied(&Standalone).await;
}

#[tokio::test]
async fn public_routes_need_no_role() {
    role_contract::public_routes_need_no_role(&Standalone).await;
}

#[tokio::test]
async fn a_header_never_grants_a_role() {
    role_contract::a_header_never_grants_a_role(&Standalone).await;
}

#[tokio::test]
async fn a_role_grant_extension_admits_and_wins_over_the_session() {
    role_contract::a_role_grant_extension_admits_and_wins_over_the_session(&Standalone).await;
}

#[tokio::test]
async fn options_preflight_passes_without_a_role() {
    role_contract::options_preflight_passes_without_a_role(&Standalone).await;
}

#[tokio::test]
async fn a_mount_without_roles_is_unchanged() {
    role_contract::a_mount_without_roles_is_unchanged(&Standalone).await;
}
