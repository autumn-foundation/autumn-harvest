//! Guards for the issue #1837 tenant-isolation decision and its issue #1977
//! amendment.
//!
//! ADR 0004 records whether Harvest supports multi-tenant deployment. The
//! amendment records that hostile multi-tenancy is in scope at the
//! management API. The security posture page states the same answer to
//! operators. These guards check that both pages exist, agree, and cite APIs
//! and tests that are real.
//!
//! The guards do not freeze the ADR prose. They check facts only: the status,
//! the decision line, the cited API names and the cited test names.

use std::path::{Path, PathBuf};

const ADR: &str = "docs/adr/0004-tenant-isolation-cells.md";
const POSTURE: &str = "docs/security-posture.md";
const SHARDING: &str = "docs/sharding.md";
const FLOOD_TEST: &str = "autumn-harvest/tests/integration/tenant_cell_isolation_tests.rs";
const BINDING_TEST: &str = "autumn-harvest-plugin/tests/tenant_binding_integration.rs";
const RETENTION_TEST: &str = "autumn-harvest/tests/integration/retention_overrides_tests.rs";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a repo file with CRLF folded to LF, so a Windows checkout matches.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("issue #1837: cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

#[test]
fn adr_records_an_accepted_decision() {
    let adr = read(ADR);
    assert!(
        adr.starts_with("# ADR 0004: "),
        "{ADR} must open with its ADR number"
    );
    assert!(
        adr.contains("## Status\nAccepted"),
        "{ADR} must record an accepted decision"
    );
    assert!(adr.contains("issue #1837"), "{ADR} must cite issue #1837");
    for heading in ["## Decision", "## Options considered", "## Consequences"] {
        assert!(adr.contains(heading), "{ADR} lacks `{heading}`");
    }
    for answer in [
        "Harvest supports cooperative multi-tenant deployment",
        "Harvest adds no first-class namespaces",
        "## Amendment: tenant binding (issue #1977)",
        "Hostile multi-tenancy is in scope at the management",
    ] {
        assert!(adr.contains(answer), "{ADR} must state: {answer}");
    }
}

/// Every API the ADR tells an operator to call must exist in the source.
#[test]
fn adr_cites_only_real_apis() {
    let adr = read(ADR);
    let shard = read("autumn-harvest/src/shard.rs");
    let builder = read("autumn-harvest/src/builder.rs");
    for (api, source, file) in [
        ("with_reserved_shards", &shard, "shard.rs"),
        ("with_residency_map", &shard, "shard.rs"),
        ("with_shard_assignments", &builder, "builder.rs"),
        ("with_queues", &builder, "builder.rs"),
    ] {
        assert!(adr.contains(api), "{ADR} must cite `{api}`");
        let declared = [format!("pub fn {api}("), format!("pub fn {api}<")]
            .iter()
            .any(|sig| source.contains(sig.as_str()));
        assert!(
            declared,
            "{ADR} cites `{api}`, but {file} has no such public fn"
        );
    }
}

/// The ADR names its proof. The named tests must exist in the flood suite.
#[test]
fn adr_cites_the_flood_test_that_proves_the_bound() {
    let adr = read(ADR);
    let suite = read(FLOOD_TEST);
    for test in [
        "a_flood_in_one_cell_does_not_raise_the_other_tenants_schedule_to_start",
        "the_same_flood_on_a_shared_shard_breaks_the_bound",
    ] {
        assert!(adr.contains(test), "{ADR} must cite `{test}`");
        assert!(
            suite.contains(&format!("async fn {test}(")),
            "{FLOOD_TEST} lacks `{test}`"
        );
    }
}

/// The issue #1977 amendment names the APIs and the tests that prove it.
#[test]
fn adr_amendment_cites_real_apis_and_tests() {
    let adr = read(ADR);
    let retention = read("autumn-harvest/src/retention.rs");
    let tenant = read("autumn-harvest-plugin/src/tenant.rs");
    for (api, source, sig) in [
        (
            "with_tenant_override",
            &retention,
            "pub fn with_tenant_override(",
        ),
        ("VerifiedTenant", &tenant, "pub struct VerifiedTenant("),
        (
            "TENANT_SCOPED_ROUTES",
            &tenant,
            "pub const TENANT_SCOPED_ROUTES:",
        ),
    ] {
        assert!(adr.contains(api), "{ADR} must cite `{api}`");
        assert!(
            source.contains(sig),
            "{ADR} cites `{api}`, which is not real"
        );
    }
    let suite = read(BINDING_TEST);
    for test in [
        "tenant_a_credential_cannot_reach_tenant_b_by_setting_the_header",
        "tenant_token_reaches_only_its_own_runs_without_an_authorizer",
        "tenant_token_is_refused_off_the_tenant_scoped_routes",
    ] {
        assert!(adr.contains(test), "{ADR} must cite `{test}`");
        assert!(
            suite.contains(&format!("async fn {test}(")),
            "{BINDING_TEST} lacks `{test}`"
        );
    }
    assert!(adr.contains("retention_overrides_tests.rs"));
    assert!(
        read(RETENTION_TEST).contains("async fn tenant_override_deletes_only_that_tenants_runs("),
        "{RETENTION_TEST} lacks the per-tenant override test"
    );
}

#[test]
fn security_posture_states_the_tenancy_model() {
    let posture = read(POSTURE);
    let section = "## Multi-tenant deployment (issue #1837)";
    assert!(posture.contains(section), "{POSTURE} lacks `{section}`");
    assert!(
        posture.contains("adr/0004-tenant-isolation-cells.md"),
        "{POSTURE} must link ADR 0004"
    );
    assert!(
        posture.contains("not a security boundary between tenants"),
        "{POSTURE} must say that cells do not isolate a hostile tenant"
    );
    assert!(
        posture.contains("## Tenant binding (issue #1977)"),
        "{POSTURE} must document tenant binding"
    );
}

/// Issue #1837 makes per-shard worker assignment a supported cell tool.
/// The sharding guide must not still list it as out of scope.
#[test]
fn sharding_guide_documents_cells() {
    let guide = read(SHARDING);
    assert!(
        guide.contains("## Tenant cells (issue #1837)"),
        "{SHARDING} lacks the tenant cells section"
    );
    let out_of_scope = guide
        .lines()
        .filter(|line| line.contains("**Out of scope**"))
        .find(|line| line.contains("per-shard worker assignment,"));
    assert!(
        out_of_scope.is_none(),
        "{SHARDING} still lists per-shard worker assignment as out of scope"
    );
}
