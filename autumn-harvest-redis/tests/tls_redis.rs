//! `rediss://` against a TLS-only Redis container (issue #1834).
//!
//! Each case mints a throwaway CA and server certificate with `rcgen`. The
//! container listens on TLS only (`--port 0`), so a passing case proves the
//! transport is TLS. Nothing secret is committed.
//!
//! Without Docker each case skips. `HARVEST_TEST_REQUIRE_REDIS=1` turns the
//! skip into a failure, as in the other Redis suites.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use autumn_harvest::dispatch::{DispatchHint, TaskDispatch};
use autumn_harvest_redis::{
    EnqueueParams, RedisDispatch, RedisDispatchConfig, RedisTaskQueue, RedisTaskQueueConfig,
    RedisTlsOptions, TaskQueueAdapter, TaskType,
};
use chrono::Utc;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use uuid::Uuid;

const REQUIRE_VAR: &str = "HARVEST_TEST_REQUIRE_REDIS";
/// Redis 6 added TLS. This tag matches the CI pre-pull list.
const IMAGE_TAG: &str = "7.4-alpine";
const TLS_PORT: u16 = 6379;

/// The child half of the platform-store case reads these.
const PROBE_URL_VAR: &str = "HARVEST_TLS_PROBE_URL";
const PROBE_OK: &str = "PROBE:CONNECTED";

/// A throwaway CA, a server certificate for `localhost`, and a client
/// certificate. The CA signs both.
///
/// Every field holds PEM text.
struct Pki {
    ca: String,
    server_cert: String,
    server_key: String,
    client_cert: String,
    client_key: String,
}

fn mint_pki() -> Pki {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "harvest test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "harvest test client");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

    Pki {
        ca: ca_cert.pem(),
        server_cert: server_cert.pem(),
        server_key: server_key.serialize_pem(),
        client_cert: client_cert.pem(),
        client_key: client_key.serialize_pem(),
    }
}

/// A TLS-only Redis, and the `rediss://` URL that reaches it.
struct TlsRedis {
    _container: ContainerAsync<GenericImage>,
    url: String,
}

fn fixture_is_required() -> bool {
    std::env::var(REQUIRE_VAR).is_ok_and(|value| value == "1")
}

/// Start a TLS-only Redis, or return `None` so the case skips.
async fn start_tls_redis(pki: &Pki, require_client_cert: bool) -> Option<TlsRedis> {
    let auth_clients = if require_client_cert { "yes" } else { "no" };
    let started = GenericImage::new("redis", IMAGE_TAG)
        .with_exposed_port(TLS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .with_copy_to("/tls/ca.crt", pki.ca.clone().into_bytes())
        .with_copy_to("/tls/server.crt", pki.server_cert.clone().into_bytes())
        .with_copy_to("/tls/server.key", pki.server_key.clone().into_bytes())
        .with_cmd([
            "redis-server",
            "--port",
            "0",
            "--tls-port",
            "6379",
            "--tls-cert-file",
            "/tls/server.crt",
            "--tls-key-file",
            "/tls/server.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            auth_clients,
        ])
        .start()
        .await;
    let container = match started {
        Ok(container) => container,
        Err(err) => {
            assert!(
                !fixture_is_required(),
                "{REQUIRE_VAR}=1 demands a TLS Redis container: {err}"
            );
            eprintln!("skipping: docker unavailable: {err}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(TLS_PORT).await.ok()?;
    // `localhost` matches the certificate's DNS name.
    let url = format!("rediss://localhost:{port}");
    Some(TlsRedis {
        _container: container,
        url,
    })
}

fn private_ca(pki: &Pki) -> RedisTlsOptions {
    RedisTlsOptions {
        ca_cert_pem: Some(pki.ca.clone().into_bytes()),
        ..RedisTlsOptions::default()
    }
}

fn config() -> RedisDispatchConfig {
    RedisDispatchConfig {
        key_prefix: format!("tls_{}", Uuid::new_v4().simple()),
        ..RedisDispatchConfig::default()
    }
}

fn hint(task_id: Uuid) -> DispatchHint {
    DispatchHint {
        task_id,
        queue_name: "default".to_string(),
        scheduled_at: Utc::now(),
        priority: 0,
        shard: None,
        kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
    }
}

/// Publish one hint and read it back over the channel.
async fn assert_round_trip(dispatch: &RedisDispatch) {
    let task_id = Uuid::new_v4();
    dispatch.publish(&[hint(task_id)]).await.expect("publish");
    let leases = dispatch
        .next(
            &["default".to_string()],
            "consumer-1",
            10,
            Duration::from_millis(200),
        )
        .await
        .expect("next");
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].task_id, task_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn dispatch_round_trips_over_tls_with_a_private_ca() {
    let pki = mint_pki();
    let Some(redis) = start_tls_redis(&pki, false).await else {
        return;
    };

    let dispatch = RedisDispatch::connect_with_tls(&redis.url, config(), private_ca(&pki))
        .await
        .expect("a rediss:// URL with a trusted CA must connect");
    assert_round_trip(&dispatch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_server_certificate_is_refused() {
    let pki = mint_pki();
    let Some(redis) = start_tls_redis(&pki, false).await else {
        return;
    };
    let stranger = mint_pki();

    let err = RedisDispatch::connect_with_tls(&redis.url, config(), private_ca(&stranger))
        .await
        .expect_err("a server certificate from an unknown CA must fail verification");
    assert!(
        err.to_string().contains("invalid peer certificate"),
        "the error must name the certificate failure, not a timeout: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_sends_the_client_certificate() {
    let pki = mint_pki();
    let Some(redis) = start_tls_redis(&pki, true).await else {
        return;
    };

    let without_identity =
        RedisDispatch::connect_with_tls(&redis.url, config(), private_ca(&pki)).await;
    assert!(
        without_identity.is_err(),
        "a server that demands a client certificate must refuse a client without one"
    );

    let mutual = RedisTlsOptions {
        client_cert_pem: Some(pki.client_cert.clone().into_bytes()),
        client_key_pem: Some(pki.client_key.clone().into_bytes()),
        ..private_ca(&pki)
    };
    let dispatch = RedisDispatch::connect_with_tls(&redis.url, config(), mutual)
        .await
        .expect("a client certificate signed by the server's CA must connect");
    assert_round_trip(&dispatch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_standalone_queue_connects_over_tls() {
    let pki = mint_pki();
    let Some(redis) = start_tls_redis(&pki, false).await else {
        return;
    };

    let queue_config = RedisTaskQueueConfig {
        key_prefix: format!("tlsq_{}", Uuid::new_v4().simple()),
        ..RedisTaskQueueConfig::default()
    };
    let queue = RedisTaskQueue::connect_with_tls(&redis.url, queue_config, private_ca(&pki))
        .await
        .expect("the standalone queue must connect over TLS");
    let task_id = queue
        .enqueue(EnqueueParams::new(
            "default",
            TaskType::Activity,
            serde_json::json!({"n": 1}),
        ))
        .await
        .expect("enqueue");
    let claim = queue
        .claim(&["default".to_string()], "worker-1")
        .await
        .expect("claim")
        .expect("one task");
    assert_eq!(claim.envelope.task_id, task_id);
}

/// The child half of `the_platform_store_honours_ssl_cert_file`. It is a
/// no-op in a normal run.
#[tokio::test(flavor = "multi_thread")]
async fn platform_store_probe_child() {
    let Ok(url) = std::env::var(PROBE_URL_VAR) else {
        return;
    };
    match RedisDispatch::connect(&url, config()).await {
        Ok(dispatch) => {
            assert_round_trip(&dispatch).await;
            println!("{PROBE_OK}");
        }
        Err(err) => println!("PROBE:ERROR:{err}"),
    }
}

/// The plain `connect` trusts the platform store, which reads
/// `SSL_CERT_FILE`. That is how an operator trusts a private CA without code.
/// The variable is process-wide, so a child process sets it.
#[tokio::test(flavor = "multi_thread")]
async fn the_platform_store_honours_ssl_cert_file() {
    let pki = mint_pki();
    let Some(redis) = start_tls_redis(&pki, false).await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    std::fs::write(&ca_file, &pki.ca).unwrap();

    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "platform_store_probe_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PROBE_URL_VAR, &redis.url)
        .env("SSL_CERT_FILE", &ca_file)
        .env_remove("SSL_CERT_DIR")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .unwrap();
    let probe = BufReader::new(child.stdout.as_slice())
        .lines()
        .map_while(Result::ok)
        .find_map(|line| {
            line.find("PROBE:")
                .map(|at| line[at..].trim_end().to_string())
        })
        .expect("the probe child must print a PROBE token");
    assert_eq!(probe, PROBE_OK);
}
