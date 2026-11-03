//! Integration tests for scoped API tokens + rotation (issue #942).
//!
//! Tests run against a real Postgres (set `HARVEST_TEST_DATABASE_URL` to a
//! migrated database to run directly with `--test-threads=1`, otherwise a fresh
//! testcontainers Postgres is booted with the full migration set).
//!
//! End-to-end coverage of the management token layer:
//!   - create returns the secret exactly once; only the hash is stored (AC2)
//!   - `GET /admin/tokens` returns metadata only, never the hash/secret (AC2)
//!   - a minted `hvst_` bearer authenticates a read route (AC1)
//!   - revocation and expiry are effective on the next request (AC5)
//!   - a `read` token gets 403 on 100% of mutating routes and reaches read-only
//!     routes (AC3/AC4, the 100%/0% success-metric sweep)
//!   - a `mutate` token reaches an admin-gated mutation without an embedder
//!     boundary (AC7/D3, the `require_admin` composition)
//!   - a token-authed mutation writes an audit row whose actor is `token:{id}`,
//!     never the secret/hash, and a client `x-harvest-actor` cannot override it
//!     (AC6)
//!   - an unknown `hvst_` bearer is rejected 401 (AC5/AC7)
//!
//! The checks live in `support/token_contract.rs`. The SQLite suite runs the
//! same checks on a SQLite app (issue #2469).

// Local `#[derive(QueryableByName)]` row structs sit next to their query.
#![allow(clippy::items_after_statements)]
// A `TokenBackend` method is async for the SQLite harness. A Postgres app
// builds at once.
#![allow(clippy::unused_async_trait_impl)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, StandaloneAdminAuth,
    harvest_api_router,
};
use autumn_web::reexports::axum;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[path = "support/token_contract.rs"]
mod token_contract;

use token_contract::TokenBackend;

type HarvestApiApp = axum::Router;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn api_state(pool: &DbPool) -> HarvestApiState {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("token-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    api_state
}

/// App with the embedder admin boundary declared (mints/verifies under
/// boundary) AND the token scope layer installed. This is the `api_with_auth`
/// plus tokens composition.
///
/// Assembled through the exported [`StandaloneAdminAuth`] mount (issue #1608),
/// not a hand-written layer stack, so this suite exercises the composition an
/// embedder actually writes.
fn build_app_boundary(pool: &DbPool) -> HarvestApiApp {
    let state = api_state(pool);
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .mount(harvest_api_router(state.clone()), &state)
}

/// App with NO embedder boundary and the token scope layer installed. This is
/// the standalone-token mode: a verified `mutate` token must satisfy
/// `require_admin` via the `TokenPrincipal` extension.
fn build_app_standalone(pool: &DbPool) -> HarvestApiApp {
    let state = api_state(pool);
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .mount(harvest_api_router(state.clone()), &state)
}

async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_api_tokens",
        "DELETE FROM harvest_audit_log",
    ] {
        let _ = diesel::sql_query(stmt).execute(conn).await;
    }
}

/// One Postgres database, scrubbed. Both apps share its pool.
struct Pg {
    pool: DbPool,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Pg {
    async fn new() -> Self {
        let (url, container) = setup_database().await;
        let pool = build_pool(&url);
        let mut conn = pool.get().await.unwrap();
        scrub(&mut conn).await;
        Self {
            pool,
            _container: container,
        }
    }
}

impl TokenBackend for Pg {
    fn prefix(&self) -> &'static str {
        ""
    }

    async fn boundary(&mut self) -> axum::Router {
        build_app_boundary(&self.pool)
    }

    async fn standalone(&mut self) -> axum::Router {
        build_app_standalone(&self.pool)
    }

    async fn stored_hashes(&self) -> Vec<(String, String)> {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = diesel::sql_types::Text)]
            name: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            token_hash: String,
        }
        let mut conn = self.pool.get().await.unwrap();
        let rows: Vec<Row> = diesel::sql_query("SELECT name, token_hash FROM harvest_api_tokens")
            .load(&mut conn)
            .await
            .unwrap();
        rows.into_iter().map(|r| (r.name, r.token_hash)).collect()
    }

    async fn seed_admin_token(&self, name: &str, token_hash: &str) -> String {
        #[derive(diesel::QueryableByName)]
        struct IdRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            id: String,
        }
        let mut conn = self.pool.get().await.unwrap();
        // The SQL that `harvest token bootstrap` prints.
        let row: IdRow = diesel::sql_query(
            "INSERT INTO harvest_api_tokens (name, token_hash, scope, created_by) \
             VALUES ($1, $2, 'admin', 'bootstrap') RETURNING id::text AS id",
        )
        .bind::<diesel::sql_types::Text, _>(name)
        .bind::<diesel::sql_types::Text, _>(token_hash)
        .get_result(&mut conn)
        .await
        .unwrap();
        row.id
    }
}

#[tokio::test]
async fn create_returns_secret_once_and_stores_only_hash() {
    token_contract::create_returns_secret_once_and_stores_only_hash(&mut Pg::new().await).await;
}

#[tokio::test]
async fn list_returns_metadata_only() {
    token_contract::list_returns_metadata_only(&mut Pg::new().await).await;
}

#[tokio::test]
async fn verify_with_secret_authenticates_a_read_route() {
    token_contract::verify_with_secret_authenticates_a_read_route(&mut Pg::new().await).await;
}

#[tokio::test]
async fn mixed_case_bearer_scheme_authenticates_identically() {
    token_contract::mixed_case_bearer_scheme_authenticates_identically(&mut Pg::new().await).await;
}

#[tokio::test]
async fn unknown_hvst_bearer_is_401() {
    token_contract::unknown_hvst_bearer_is_401(&mut Pg::new().await).await;
}

#[tokio::test]
async fn revoke_denies_next_request() {
    token_contract::revoke_denies_next_request(&mut Pg::new().await).await;
}

#[tokio::test]
async fn expired_token_is_401() {
    token_contract::expired_token_is_401(&mut Pg::new().await).await;
}

#[tokio::test]
async fn read_token_403_on_all_mutations_and_reaches_reads() {
    token_contract::read_token_403_on_all_mutations_and_reaches_reads(&mut Pg::new().await).await;
}

#[tokio::test]
async fn mutate_token_reaches_admin_route_standalone() {
    token_contract::mutate_token_reaches_admin_route_standalone(&mut Pg::new().await).await;
}

#[tokio::test]
async fn token_authed_mutation_audit_actor_is_token_id() {
    token_contract::token_authed_mutation_audit_actor_is_token_id(&mut Pg::new().await).await;
}

#[tokio::test]
async fn passthrough_cannot_forge_token_namespaced_actor() {
    token_contract::passthrough_cannot_forge_token_namespaced_actor(&mut Pg::new().await).await;
}

#[tokio::test]
async fn create_and_revoke_are_audited_with_actor() {
    token_contract::create_and_revoke_are_audited_with_actor(&mut Pg::new().await).await;
}

#[tokio::test]
async fn bootstrap_seeded_token_authenticates_identically_to_route_minted() {
    token_contract::bootstrap_seeded_token_authenticates_identically_to_route_minted(
        &mut Pg::new().await,
    )
    .await;
}

#[tokio::test]
async fn mutate_token_is_denied_token_admin_and_audited() {
    token_contract::mutate_token_is_denied_token_admin_and_audited(&mut Pg::new().await).await;
}
