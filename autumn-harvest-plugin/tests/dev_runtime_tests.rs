#![cfg(feature = "dev-runtime")]
//! No-database, no-process unit coverage for the zero-setup dev runtime
//! (issue #525).
//!
//! Everything here is a pure function over explicit inputs: DSN classification,
//! Postgres binary discovery, the stale-session reap decision, banner rendering
//! and DSN construction. The parts that actually spawn a postmaster live in
//! `dev_runtime_lifecycle.rs`, which skips cleanly where no Postgres binaries
//! exist.
//!
//! | AC | Test |
//! |----|------|
//! | AC3 banner names the UI and a copy-pasteable trigger | `banner_*` |
//! | AC4 refuses a non-ephemeral / production database | `classify_*` |
//! | AC5 teardown reclaims all ephemeral state | `reap_*` |
//! | AC6 no new migration, no new event variant | `dev_runtime_adds_no_migration_and_no_event_variant` |

use std::path::{Path, PathBuf};

use autumn_harvest_plugin::dev::MAX_UNIX_SOCKET_PATH_LEN;
use autumn_harvest_plugin::dev::{
    BannerInputs, DatabaseSafety, DevRuntimeConfig, DiscoveryEnv, Platform, ReapDecision,
    RefusalReason, SessionRecord, SkipReason, StorageDescription, SuspicionReason,
    candidate_bin_dirs, classify_database_url, decide_reap, effective_postmaster_pid,
    ephemeral_dsn, http_authority, parse_postmaster_pid, postgres_conf_lines, proc_stat_is_live,
    proc_stat_start_time, record_is_self_consistent, redact_dsn, render_banner, resolve_bin_dir,
    unix_socket_path_len, write_private_atomic,
};

// ---------------------------------------------------------------------------
// AC4 — the dev-only safety gate
// ---------------------------------------------------------------------------

#[test]
fn classify_accepts_a_loopback_url_dsn() {
    for dsn in [
        "postgres://harvest:pw@127.0.0.1:5432/harvest_dev",
        "postgres://harvest:pw@localhost:5432/harvest_dev",
        "postgresql://harvest@[::1]:5432/harvest_dev",
    ] {
        assert!(
            matches!(classify_database_url(dsn), DatabaseSafety::Allowed),
            "expected {dsn} to be allowed"
        );
    }
}

/// The socket DSN `EphemeralPostgres` produces when it listens on a socket
/// directory rather than a TCP port.
const UNIX_SOCKET_DSN: &str = "postgres:///harvest_dev?host=/tmp/harvest-dev-1234-ab/socket";

#[cfg(unix)]
#[test]
fn classify_accepts_a_unix_socket_dsn() {
    assert!(
        matches!(
            classify_database_url(UNIX_SOCKET_DSN),
            DatabaseSafety::Allowed
        ),
        "unix-socket DSN must be allowed"
    );
}

#[cfg(not(unix))]
#[test]
fn classify_refuses_a_unix_socket_dsn_where_there_are_no_unix_sockets() {
    // `tokio_postgres::config::Host::Unix` is `#[cfg(unix)]`, so off Unix the
    // client parses `host=/tmp/...` as an ordinary TCP *hostname* — and the gate
    // classifies whatever the client will actually dial, by design. A name that
    // is not loopback is refused, which is the right answer on a platform with
    // no Unix sockets to reach: the DSN could not have worked anyway, and
    // failing closed beats inventing a meaning the client does not share.
    assert!(
        matches!(
            classify_database_url(UNIX_SOCKET_DSN),
            DatabaseSafety::Refused(_)
        ),
        "a socket path is not reachable off Unix, so it must not be allowed"
    );
}

#[test]
fn classify_accepts_a_dsn_with_no_host_at_all() {
    // `libpq`'s default with no host is the local Unix socket — as local as
    // loopback. Both spellings must reach the same verdict.
    for dsn in ["postgres:///harvest_dev", "dbname=harvest_dev user=me"] {
        assert!(
            matches!(classify_database_url(dsn), DatabaseSafety::Allowed),
            "an omitted host means the local socket, not a remote server: {dsn}"
        );
    }
}

#[test]
fn classify_accepts_a_keyword_value_dsn() {
    let dsn = "host=127.0.0.1 port=5432 dbname=harvest_dev user=harvest";
    assert!(
        matches!(classify_database_url(dsn), DatabaseSafety::Allowed),
        "keyword/value DSNs are valid libpq input and must be classified, not refused"
    );
}

#[test]
fn classify_refuses_a_remote_host() {
    let DatabaseSafety::Refused(reason) =
        classify_database_url("postgres://harvest:pw@db.internal.example.com:5432/harvest")
    else {
        panic!("a non-loopback host must be refused");
    };
    assert!(
        matches!(reason, RefusalReason::RemoteHost { .. }),
        "{reason:?}"
    );
}

#[test]
fn classify_refuses_a_tls_requiring_dsn() {
    for mode in ["require", "verify-ca", "verify-full"] {
        let dsn = format!("postgres://u:p@localhost:5432/app?sslmode={mode}");
        let DatabaseSafety::Refused(reason) = classify_database_url(&dsn) else {
            panic!("sslmode={mode} must be refused even on loopback");
        };
        assert!(
            matches!(reason, RefusalReason::TlsRequired { .. }),
            "{reason:?}"
        );
    }
}

#[test]
fn classify_allows_disabled_and_prefer_sslmode() {
    for mode in ["disable", "prefer"] {
        let dsn = format!("postgres://u:p@localhost:5432/harvest_dev?sslmode={mode}");
        assert!(
            matches!(classify_database_url(&dsn), DatabaseSafety::Allowed),
            "sslmode={mode} on loopback is ordinary local development"
        );
    }
}

#[test]
fn classify_refuses_an_sslmode_the_client_itself_cannot_use() {
    // `allow` is valid `libpq` but the Rust client does not implement it, so the
    // connection would fail anyway. Refusing it here says so up front instead of
    // letting it surface later as an opaque connect error.
    let safety = classify_database_url("postgres://u:p@localhost:5432/harvest_dev?sslmode=allow");
    assert!(
        matches!(
            safety,
            DatabaseSafety::Refused(RefusalReason::Unusable { .. })
        ),
        "{safety:?}"
    );
}

// ---------------------------------------------------------------------------
// AC4 — the `sslmode` scan is syntax-aware, not positional (issue #1286)
// ---------------------------------------------------------------------------

#[test]
fn classify_allows_a_keyword_value_dsn_whose_password_contains_sslmode_text() {
    // A bare `dsn.find("sslmode=")` cannot tell a key from the inside of a
    // value, so a legal loopback database with this password was refused as
    // TLS-requiring — a message describing a state the DSN is not in. The
    // closing `'` is what made the extracted value read as exactly `require`.
    let dsn = "host=localhost password='sslmode=require' dbname=harvest_dev";
    let safety = classify_database_url(dsn);
    assert!(
        matches!(safety, DatabaseSafety::Allowed),
        "a password that merely contains the text `sslmode=require` is not an \
         sslmode key: {dsn} -> {safety:?}"
    );
}

#[test]
fn classify_allows_a_uri_whose_parameter_value_contains_sslmode_text() {
    // Same root cause in the other syntax: `application_name`'s VALUE was
    // scanned as if it were the query string's own key.
    let dsn = "postgres://u:pw@localhost/app?application_name=sslmode=require";
    let safety = classify_database_url(dsn);
    assert!(
        matches!(safety, DatabaseSafety::Allowed),
        "another parameter's value is not the query string's own key: {dsn} -> {safety:?}"
    );
}

#[test]
fn classify_allows_other_keyword_values_that_contain_sslmode_text() {
    // Not just `password`: any value at all. `options` is the realistic one —
    // it carries arbitrary server settings — and an unquoted value is scanned
    // by the same code path as a quoted one.
    for dsn in [
        "host=127.0.0.1 dbname=harvest_dev options='-c sslmode=require'",
        "host=127.0.0.1 dbname=harvest_dev application_name=sslmode=require",
        "host=127.0.0.1 dbname=harvest_dev password=sslmode=verify-full",
    ] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(safety, DatabaseSafety::Allowed),
            "{dsn} carries no sslmode key at all -> {safety:?}"
        );
    }
}

#[test]
fn classify_still_refuses_a_real_sslmode_key_in_keyword_value_syntax() {
    // The reason the textual scan exists at all: `tokio_postgres` models only
    // `disable`/`prefer`/`require`, so `verify-ca` and `verify-full` — the two
    // strongest signals of a remote managed database — would otherwise arrive
    // as an opaque parse error rather than as the specific thing they are.
    for (dsn, expected) in [
        ("host=localhost sslmode=verify-ca dbname=harvest_dev", "verify-ca"),
        (
            "host=localhost sslmode=verify-full dbname=harvest_dev",
            "verify-full",
        ),
        ("host=localhost sslmode=require dbname=harvest_dev", "require"),
        // libpq permits whitespace around `=` and single-quoted values, and
        // both spellings are a real `sslmode` key.
        ("host=localhost sslmode = verify-full", "verify-full"),
        ("host=localhost sslmode='verify-ca'", "verify-ca"),
        // A quoted value with an escaped quote inside still terminates where
        // libpq says it does, so the key after it is still a key.
        (
            r"host=localhost password='a\'b' sslmode=verify-full",
            "verify-full",
        ),
    ] {
        let safety = classify_database_url(dsn);
        let DatabaseSafety::Refused(RefusalReason::TlsRequired { sslmode }) = &safety else {
            panic!("{dsn} demands TLS and must be refused as such, got {safety:?}");
        };
        assert_eq!(sslmode, expected, "{dsn}");
    }
}

#[test]
fn classify_still_refuses_a_real_sslmode_key_in_uri_syntax() {
    for (dsn, expected) in [
        (
            "postgres://u:pw@db.example.com/app?sslmode=verify-full",
            "verify-full",
        ),
        (
            "postgres://u:pw@localhost/app?application_name=x&sslmode=verify-ca",
            "verify-ca",
        ),
        (
            "postgresql://u:pw@localhost/app?sslmode=require&application_name=x",
            "require",
        ),
    ] {
        let safety = classify_database_url(dsn);
        let DatabaseSafety::Refused(RefusalReason::TlsRequired { sslmode }) = &safety else {
            panic!("{dsn} demands TLS and must be refused as such, got {safety:?}");
        };
        assert_eq!(sslmode, expected, "{dsn}");
    }
}

#[test]
fn classify_reads_a_percent_encoded_sslmode_key_the_way_the_client_does() {
    // `tokio_postgres` percent-decodes URI query keys and values before it
    // matches them, so `%73slmode=verify-full` IS the sslmode parameter as far
    // as the code that dials is concerned. Comparing the raw bytes would miss
    // it, and `get_ssl_mode()` cannot name it because the client refuses to
    // parse `verify-full` at all.
    for dsn in [
        "postgres://u:pw@db.example.com/app?%73slmode=verify-full",
        "postgres://u:pw@db.example.com/app?sslmode=verify%2Dfull",
    ] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(
                safety,
                DatabaseSafety::Refused(RefusalReason::TlsRequired { .. })
            ),
            "{dsn} -> {safety:?}"
        );
    }
}

#[test]
fn classify_does_not_read_a_uri_path_or_userinfo_as_a_query_parameter() {
    // Everything before the `?` is authority and path, never parameters. A
    // database whose name happens to contain the text is still just a name.
    let dsn = "postgres://u:pw@localhost/sslmode=require";
    let safety = classify_database_url(dsn);
    assert!(
        !matches!(
            safety,
            DatabaseSafety::Refused(RefusalReason::TlsRequired { .. })
        ),
        "a path segment is not a query parameter: {dsn} -> {safety:?}"
    );
}

#[test]
fn classify_terminates_and_never_panics_on_adversarial_dsn_syntax() {
    // The scan walks quotes and `\` escapes by hand. Unterminated quotes,
    // trailing escapes, empty values and multi-byte characters after an escape
    // are all inputs a developer can type, and none of them may hang the dev
    // runtime or slice a `String` off a character boundary.
    for dsn in [
        "host=localhost password='unterminated",
        r"host=localhost password=trailing\",
        r"host=localhost password='\",
        "host=localhost sslmode=",
        "host=localhost sslmode=''",
        "host=localhost =novalue sslmode=verify-ca",
        "bareword host=localhost",
        r"host=localhost password='\é' sslmode=verify-full",
        "host=localhost password=é sslmode=verify-full",
        "sslmode",
        "=",
        "'",
        "postgres://u@localhost/app?",
        "postgres://u@localhost/app?&&=&",
        "postgres://u@localhost/app?%",
        "postgres://u@localhost/app?%zz=1",
    ] {
        // The verdict is not the point; not diverging is.
        let _ = classify_database_url(dsn);
        let _ = redact_dsn(dsn);
    }
}

#[test]
fn redaction_still_covers_a_password_next_to_a_multibyte_escape() {
    // The keyword scanner is shared with the safety gate after #1286, so the
    // banner's whole reason for existing — never printing a credential — is
    // asserted against the shared implementation too.
    let dsn = r"host=localhost password='hunter2\é x' dbname=harvest_dev";
    let redacted = redact_dsn(dsn);
    assert!(!redacted.contains("hunter2"), "{redacted}");
    assert!(redacted.contains("host=localhost"), "{redacted}");
    assert!(redacted.contains("dbname=harvest_dev"), "{redacted}");
}

#[test]
fn classify_refuses_a_hostaddr_that_points_somewhere_else() {
    // `hostaddr` is NOT a synonym for `host`: when both are present it is the
    // address actually dialled, and `host` is only the TLS/SNI name. A gate that
    // folds them together reads this as "localhost".
    for dsn in [
        "postgres://harvest:pw@localhost/app?hostaddr=203.0.113.5",
        "hostaddr=203.0.113.5 host=localhost dbname=app user=harvest",
        "host=localhost hostaddr=203.0.113.5 dbname=app user=harvest",
    ] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(safety, DatabaseSafety::Refused(_)),
            "a remote hostaddr must be refused however it is spelled: {dsn} -> {safety:?}"
        );
    }
}

#[test]
fn classify_allows_a_hostaddr_that_is_loopback() {
    assert!(matches!(
        classify_database_url("postgres://harvest:pw@localhost/harvest_dev?hostaddr=127.0.0.1"),
        DatabaseSafety::Allowed
    ));
}

#[test]
fn classify_refuses_a_dsn_that_smuggles_a_remote_host_alongside_a_local_one() {
    // A `host=` query parameter APPENDS a host, it does not replace the
    // authority host — and the remote one is tried first. Every host in the
    // list has to be local, not just one of them.
    for dsn in [
        "postgres://u:p@db.prod.example.com:5432/app?host=127.0.0.1",
        "postgres://u:p@127.0.0.1:5432/app?host=db.prod.example.com",
        "host=127.0.0.1,db.prod.example.com dbname=app user=u",
        "host=db.prod.example.com host=127.0.0.1 dbname=app user=u",
    ] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(safety, DatabaseSafety::Refused(_)),
            "every host must be local, not merely one of them: {dsn} -> {safety:?}"
        );
    }
}

#[test]
fn classify_refuses_known_managed_postgres_providers() {
    for host in [
        "mydb.abcdef123456.us-east-1.rds.amazonaws.com",
        "ep-cool-block-123456.us-east-2.aws.neon.tech",
        "db.abcdefghijklmnop.supabase.co",
        "myserver.postgres.database.azure.com",
        "app-db-do-user-1-0.b.db.ondigitalocean.com",
        "dpg-abc123-a.oregon-postgres.render.com",
        "pg-123abc-myorg.aivencloud.com",
        "ec2-1-2-3-4.compute-1.amazonaws.com",
    ] {
        let dsn = format!("postgres://u:p@{host}:5432/app");
        let DatabaseSafety::Refused(reason) = classify_database_url(&dsn) else {
            panic!("{host} is a managed Postgres endpoint and must be refused");
        };
        assert!(
            matches!(
                reason,
                RefusalReason::ManagedProvider { .. } | RefusalReason::RemoteHost { .. }
            ),
            "{reason:?}"
        );
    }
}

#[test]
fn classify_flags_a_production_shaped_local_database_as_suspicious_not_refused() {
    // A loopback database that merely *looks* production-shaped is a real and
    // harmless situation, so it is an opt-in, not a hard refusal.
    for dsn in [
        "postgres://u:p@localhost:5432/myapp_production",
        "postgres://u:p@127.0.0.1:5432/prod",
        "postgres://u:p@localhost:5432/billing-live",
        "postgres://production_user:p@localhost:5432/app",
    ] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(
                safety,
                DatabaseSafety::Suspicious(SuspicionReason::ProductionShapedName { .. })
            ),
            "expected {dsn} to be suspicious, got {safety:?}"
        );
    }
}

#[test]
fn classify_does_not_flag_ordinary_dev_names_as_suspicious() {
    for dsn in [
        "postgres://u:p@localhost:5432/harvest_dev",
        "postgres://u:p@localhost:5432/quickstart",
        "postgres://u:p@localhost:5432/reproduction_notes",
        "postgres://u:p@localhost:5432/aliveness",
    ] {
        assert!(
            matches!(classify_database_url(dsn), DatabaseSafety::Allowed),
            "{dsn} is an ordinary local name and must not trip the production check"
        );
    }
}

#[test]
fn classify_fails_closed_on_an_unparseable_dsn() {
    for dsn in ["", "   ", "not a dsn at all", "mysql://u:p@localhost/app"] {
        let safety = classify_database_url(dsn);
        assert!(
            matches!(
                safety,
                DatabaseSafety::Refused(RefusalReason::Unusable { .. })
            ),
            "a DSN we cannot understand must fail closed, got {safety:?} for {dsn:?}"
        );
    }
}

#[test]
fn classify_refuses_a_userinfo_that_hides_a_remote_host() {
    // `url` splits userinfo at the LAST `@`, the Postgres client at the FIRST —
    // so a hand-rolled gate and the connection disagree about where the host
    // even begins. Classifying with the client's own parser removes the
    // disagreement.
    let safety = classify_database_url("postgres://user@evil.example.com@127.0.0.1/app");
    assert!(matches!(safety, DatabaseSafety::Refused(_)), "{safety:?}");
}

#[test]
fn classify_refuses_a_host_that_merely_starts_with_a_loopback_prefix() {
    // `localhost.attacker.example` and `127.0.0.1.nip.io` both *contain* a
    // loopback spelling; neither IS loopback.
    for host in ["localhost.example.com", "127.0.0.1.nip.io", "notlocalhost"] {
        let dsn = format!("postgres://u:p@{host}:5432/app");
        assert!(
            matches!(classify_database_url(&dsn), DatabaseSafety::Refused(_)),
            "{host} is not loopback and must be refused"
        );
    }
}

// ---------------------------------------------------------------------------
// Postgres binary discovery
// ---------------------------------------------------------------------------

fn env_with(pairs: &[(&str, &str)]) -> DiscoveryEnv {
    let mut env = DiscoveryEnv::empty();
    for (key, value) in pairs {
        env.set(key, value);
    }
    env
}

#[test]
fn discovery_puts_the_explicit_override_first() {
    let env = env_with(&[("HARVEST_DEV_PG_BIN", "/opt/mypg/bin")]);
    let candidates = candidate_bin_dirs(Platform::Linux, &env);
    assert_eq!(
        candidates.first().map(PathBuf::as_path),
        Some(Path::new("/opt/mypg/bin")),
        "HARVEST_DEV_PG_BIN must win over every discovered location"
    );
}

/// Whether any candidate's textual path starts with `prefix`.
///
/// Deliberately not `Path::starts_with`, which matches whole path *components*:
/// `Path::new("/usr/pgsql-16/bin").starts_with("/usr/pgsql-")` is `false`, so
/// using it here would silently assert nothing about the versioned layouts.
fn any_path_starting_with(candidates: &[PathBuf], prefix: &str) -> bool {
    candidates
        .iter()
        .any(|dir| dir.to_string_lossy().starts_with(prefix))
}

#[test]
fn discovery_includes_path_entries_and_well_known_linux_locations() {
    let env = env_with(&[("PATH", "/home/dev/.local/bin:/usr/bin")]);
    let candidates = candidate_bin_dirs(Platform::Linux, &env);

    assert!(candidates.contains(&PathBuf::from("/home/dev/.local/bin")));
    assert!(candidates.contains(&PathBuf::from("/usr/bin")));
    assert!(
        any_path_starting_with(&candidates, "/usr/lib/postgresql/"),
        "the Debian layout must be probed: {candidates:?}"
    );
    assert!(
        any_path_starting_with(&candidates, "/usr/pgsql-"),
        "the RedHat layout must be probed: {candidates:?}"
    );
}

#[test]
fn discovery_includes_homebrew_and_postgres_app_on_macos() {
    let env = env_with(&[("PATH", "/usr/bin")]);
    let candidates = candidate_bin_dirs(Platform::MacOs, &env);
    assert!(
        any_path_starting_with(&candidates, "/opt/homebrew/opt/postgresql@"),
        "{candidates:?}"
    );
    assert!(
        any_path_starting_with(&candidates, "/usr/local/opt/postgresql@"),
        "the Intel-Homebrew prefix must be probed too: {candidates:?}"
    );
    assert!(
        any_path_starting_with(&candidates, "/Applications/Postgres.app/"),
        "{candidates:?}"
    );
}

#[test]
fn discovery_includes_the_enterprisedb_layout_on_windows() {
    let env = env_with(&[("PATH", r"C:\Windows\System32")]);
    let candidates = candidate_bin_dirs(Platform::Windows, &env);
    assert!(
        candidates
            .iter()
            .any(|dir| dir.to_string_lossy().contains(r"Program Files\PostgreSQL")),
        "{candidates:?}"
    );
}

#[test]
fn discovery_prefers_newer_postgres_versions() {
    let env = env_with(&[]);
    let candidates = candidate_bin_dirs(Platform::Linux, &env);
    let debian: Vec<_> = candidates
        .iter()
        .filter(|dir| dir.to_string_lossy().starts_with("/usr/lib/postgresql/"))
        .collect();
    assert!(debian.len() >= 2, "{candidates:?}");
    let first = debian[0].to_string_lossy().to_string();
    let second = debian[1].to_string_lossy().to_string();
    let version_of = |s: &str| -> u32 {
        s.trim_start_matches("/usr/lib/postgresql/")
            .trim_end_matches("/bin")
            .parse()
            .unwrap()
    };
    assert!(
        version_of(&first) > version_of(&second),
        "newest first: {first} then {second}"
    );
}

#[test]
fn resolve_picks_the_first_candidate_holding_the_whole_toolset() {
    let candidates = vec![
        PathBuf::from("/empty"),
        PathBuf::from("/partial"),
        PathBuf::from("/complete"),
        PathBuf::from("/also-complete"),
    ];
    let resolved = resolve_bin_dir(&candidates, Platform::Linux, &|dir, tool| match dir
        .to_string_lossy()
        .as_ref()
    {
        "/partial" => tool == "initdb",
        "/complete" | "/also-complete" => true,
        _ => false,
    });
    assert_eq!(resolved.as_deref(), Some(Path::new("/complete")));
}

#[test]
fn resolve_requires_initdb_pg_ctl_and_postgres_together() {
    let candidates = vec![PathBuf::from("/only-psql")];
    let resolved = resolve_bin_dir(&candidates, Platform::Linux, &|_, tool| tool == "psql");
    assert!(
        resolved.is_none(),
        "a client-only install cannot provision a server"
    );
}

#[test]
fn resolve_uses_exe_suffixed_names_on_windows() {
    let seen = std::cell::RefCell::new(Vec::new());
    let candidates = vec![PathBuf::from(r"C:\pg\bin")];
    let _ = resolve_bin_dir(&candidates, Platform::Windows, &|_, tool| {
        seen.borrow_mut().push(tool.to_owned());
        false
    });
    let seen = seen.into_inner();
    assert!(
        seen.iter()
            .all(|tool| Path::new(tool).extension().is_some_and(|ext| ext == "exe")),
        "Windows probes must use .exe names: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// AC5 — teardown / stale-session reaping
// ---------------------------------------------------------------------------

fn record(owner_pid: u32, postmaster_pid: Option<u32>) -> SessionRecord {
    SessionRecord {
        owner_pid,
        postmaster_pid,
        owner_start_token: Some("11002".to_owned()),
        postmaster_start_token: Some("87231".to_owned()),
        bin_dir: Some(PathBuf::from("/usr/lib/postgresql/16/bin")),
        data_dir: PathBuf::from("/tmp/harvest-dev-0/session-1-aa/data"),
        created_at: chrono::Utc::now(),
    }
}

#[test]
fn reap_leaves_a_live_session_alone() {
    let decision = decide_reap(&record(4242, Some(4243)), true, true, 99);
    assert!(
        matches!(decision, ReapDecision::Skip { .. }),
        "{decision:?}"
    );
}

#[test]
fn reap_never_touches_our_own_session() {
    // The reaper runs at startup, after our own record could already exist from
    // a same-pid predecessor; identity beats liveness.
    let decision = decide_reap(&record(4242, Some(4243)), true, true, 4242);
    assert!(
        matches!(decision, ReapDecision::Skip { .. }),
        "{decision:?}"
    );
}

#[test]
fn reap_stops_an_orphaned_postmaster_then_removes_the_directory() {
    let decision = decide_reap(&record(4242, Some(4243)), false, true, 99);
    assert!(
        matches!(
            decision,
            ReapDecision::StopThenRemove {
                postmaster_pid: 4243
            }
        ),
        "{decision:?}"
    );
}

#[test]
fn reap_removes_the_directory_when_the_postmaster_is_already_gone() {
    let decision = decide_reap(&record(4242, Some(4243)), false, false, 99);
    assert!(matches!(decision, ReapDecision::Remove), "{decision:?}");
}

#[test]
fn reap_removes_a_session_that_died_before_recording_a_postmaster() {
    let decision = decide_reap(&record(4242, None), false, false, 99);
    assert!(matches!(decision, ReapDecision::Remove), "{decision:?}");
}

#[test]
fn a_record_whose_data_dir_is_not_its_own_is_never_acted_on() {
    // The reaper stops processes and deletes trees. A record naming a data
    // directory outside its own session directory is corrupt or planted; either
    // way it is not an instruction we execute.
    let session_dir = Path::new("/tmp/harvest-dev-0/session-1-aa");
    assert!(record_is_self_consistent(&record(1, Some(2)), session_dir));

    let mut hostile = record(1, Some(2));
    hostile.data_dir = PathBuf::from("/etc");
    assert!(!record_is_self_consistent(&hostile, session_dir));

    let mut sneaky = record(1, Some(2));
    sneaky.data_dir = PathBuf::from("/tmp/harvest-dev-0/session-1-aa/data/../../other/data");
    assert!(!record_is_self_consistent(&sneaky, session_dir));
}

#[test]
fn the_start_token_distinguishes_a_reused_pid_from_the_recorded_process() {
    // A pid is not an identity: the gap between a SIGKILLed run and the next
    // `cargo dev` is exactly where pid reuse happens, and the reaper would
    // otherwise `SIGKILL` whoever inherited the number.
    let stat = "4243 (postgres) S 1 4243 4243 0 -1 4194560 100 0 0 0 1 2 3 4 20 0 1 0 87231                 148424 1234 18446744073709551615";
    assert_eq!(proc_stat_start_time(stat).as_deref(), Some("87231"));

    let reused = "4243 (some-other-process) S 1 4243 4243 0 -1 4194560 100 0 0 0 1 2 3 4 20 0 1                   0 99999 148424 1234 18446744073709551615";
    assert_ne!(proc_stat_start_time(reused), proc_stat_start_time(stat));
}

#[test]
fn the_start_token_parser_survives_a_process_name_with_spaces_and_parens() {
    let stat = "4243 (postgres: (walwriter) ) S 1 4243 4243 0 -1 4194560 100 0 0 0 1 2 3 4 20 0                 1 0 87231 148424 1234";
    assert_eq!(proc_stat_start_time(stat).as_deref(), Some("87231"));
}

#[test]
fn a_malformed_stat_line_yields_no_start_token() {
    for stat in ["", "garbage", "4243 (postgres", "4243 (postgres) S 1"] {
        assert_eq!(proc_stat_start_time(stat), None, "{stat:?}");
    }
}

#[test]
fn a_session_record_remembers_the_binaries_that_started_its_cluster() {
    // Codex round 1 (P2). A default `cargo dev` on a machine with no
    // PostgreSQL downloads one into a per-user cache that discovery does not
    // search. Without this field the reaper had no `pg_ctl` for such a
    // cluster — and on Windows no fallback either, since `process_start_token`
    // is `None` there, so a force-killed run would have leaked its postmaster
    // and data directory permanently.
    let record = record(4242, Some(4243));
    let decoded =
        SessionRecord::from_json(&record.to_json().expect("serialize")).expect("deserialize");
    assert_eq!(decoded.bin_dir, record.bin_dir);
}

#[test]
fn an_ipv6_host_produces_a_parseable_url() {
    // Codex round 2 (P2). `format!("http://{host}:{port}")` turns the valid
    // loopback host `::1` into `http://::1:3000`, which no URL parser accepts —
    // so the readiness poll could never succeed and the runtime would report
    // `ServerNotReady` after its full budget, with the real cause absent from
    // the message.
    assert_eq!(http_authority("::1", 3000), "[::1]:3000");
    assert_eq!(http_authority("fe80::1", 3000), "[fe80::1]:3000");
    // Already bracketed, and the non-IPv6 cases, are left alone.
    assert_eq!(http_authority("[::1]", 3000), "[::1]:3000");
    assert_eq!(http_authority("127.0.0.1", 3000), "127.0.0.1:3000");
    assert_eq!(http_authority("localhost", 3000), "localhost:3000");

    // And the result is actually a URL.
    for host in ["::1", "127.0.0.1", "localhost"] {
        let url = format!("http://{}/api/harvest/health", http_authority(host, 3000));
        assert!(
            url.parse::<reqwest::Url>().is_ok(),
            "{host} produced an unparseable URL: {url}"
        );
    }
}

#[test]
fn a_record_written_before_bin_dir_existed_still_parses() {
    // `#[serde(default)]`, so an older leftover is reclaimed rather than
    // skipped as unreadable and left on disk forever.
    let older = r#"{
        "owner_pid": 4242,
        "postmaster_pid": 4243,
        "postmaster_start_token": "87231",
        "data_dir": "/tmp/harvest-dev-0/session-1-aa/data",
        "created_at": "2026-09-02T00:00:00Z"
    }"#;
    let decoded = SessionRecord::from_json(older).expect("an older record must still parse");
    assert_eq!(decoded.owner_pid, 4242);
    assert_eq!(decoded.bin_dir, None);
    assert_eq!(decoded.owner_start_token, None);
}

#[test]
fn a_session_record_remembers_the_owner_that_created_it() {
    // Codex round 2 (P2). Liveness alone lets a REUSED owner pid make an
    // abandoned session look permanently active, so its cluster and data
    // directory would survive every later start. The start time tells the
    // recorded run apart from whoever inherited its pid.
    let record = record(4242, Some(4243));
    let decoded =
        SessionRecord::from_json(&record.to_json().expect("serialize")).expect("deserialize");
    assert_eq!(decoded.owner_start_token, record.owner_start_token);
    assert_ne!(
        decoded.owner_start_token, decoded.postmaster_start_token,
        "the two pids in a record carry their own tokens"
    );
}

#[test]
fn session_record_round_trips_through_its_on_disk_form() {
    let original = record(4242, Some(4243));
    let encoded = original.to_json().expect("serialize");
    let decoded = SessionRecord::from_json(&encoded).expect("deserialize");
    assert_eq!(decoded.owner_pid, original.owner_pid);
    assert_eq!(decoded.postmaster_pid, original.postmaster_pid);
    assert_eq!(decoded.data_dir, original.data_dir);
}

#[test]
fn a_corrupt_session_record_is_reported_not_panicked_on() {
    assert!(SessionRecord::from_json("{ not json").is_err());
    assert!(SessionRecord::from_json("{}").is_err());
}

#[test]
fn the_effective_postmaster_pid_prefers_the_record() {
    let record = record(4242, Some(4243));
    assert_eq!(
        effective_postmaster_pid(&record, Some("9999\n/tmp\n")),
        Some(4243),
        "the record is authoritative once it has a pid"
    );
}

#[test]
fn the_effective_postmaster_pid_falls_back_to_the_pid_file() {
    // The record is written before `pg_ctl start`, so during the whole start
    // window it carries no pid while a postmaster may already be running.
    // Without this fallback the reaper would delete a live cluster's directory.
    let record = record(4242, None);
    assert_eq!(
        effective_postmaster_pid(&record, Some("4243\n/tmp/data\n")),
        Some(4243)
    );
}

#[test]
fn the_effective_postmaster_pid_is_none_when_neither_source_has_one() {
    let record = record(4242, None);
    assert_eq!(effective_postmaster_pid(&record, None), None);
    assert_eq!(effective_postmaster_pid(&record, Some("")), None);
    assert_eq!(effective_postmaster_pid(&record, Some("garbage\n")), None);
}

#[test]
fn a_zombie_process_does_not_count_as_running() {
    // The distinction that makes teardown assertable. `pg_ctl` daemonises the
    // postmaster, so once it exits it is an orphan whose reaping belongs to
    // init — which, in a container, can take arbitrarily long. Both
    // `/proc/<pid>` and `kill -0` stay true for that whole window, so counting
    // a zombie as running reports correct teardown as a leak.
    assert!(!proc_stat_is_live(
        "4243 (postgres) Z 1 4243 4243 0 -1 4194560"
    ));
    assert!(!proc_stat_is_live(
        "4243 (postgres) X 1 4243 4243 0 -1 4194560"
    ));
    for state in ["R", "S", "D", "T", "I"] {
        assert!(
            proc_stat_is_live(&format!("4243 (postgres) {state} 1 4243 4243 0 -1 4194560")),
            "state {state} is a running process"
        );
    }
}

#[test]
fn the_liveness_parser_survives_a_process_name_containing_spaces_and_parens() {
    // `/proc/<pid>/stat`'s second field is the raw executable name: it can
    // contain both spaces and parentheses, which is why the state is the first
    // token after the LAST `)` rather than the third whitespace field. Postgres
    // backends rename themselves to things like `postgres: walwriter `.
    assert!(proc_stat_is_live(
        "4243 (postgres: (walwriter) ) S 1 4243 4243 0 -1 4194560"
    ));
    assert!(!proc_stat_is_live(
        "4243 (postgres: (walwriter) ) Z 1 4243 4243 0 -1 4194560"
    ));
}

#[test]
fn a_malformed_stat_line_reads_as_not_running() {
    for stat in ["", "garbage", "4243 (postgres"] {
        assert!(!proc_stat_is_live(stat), "{stat:?}");
    }
}

#[test]
fn postmaster_pid_is_read_from_the_first_line_of_the_pid_file() {
    let contents = "4243\n/tmp/harvest-dev-1-aa/data\n1717171717\n5432\n/tmp\n";
    assert_eq!(parse_postmaster_pid(contents), Some(4243));
}

#[test]
fn a_truncated_or_empty_pid_file_yields_no_pid() {
    for contents in ["", "\n", "not-a-pid\n", "  \n/tmp/data\n"] {
        assert_eq!(parse_postmaster_pid(contents), None, "{contents:?}");
    }
}

// ---------------------------------------------------------------------------
// DSN construction and server configuration
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_dsn_percent_encodes_the_generated_password() {
    let dsn = ephemeral_dsn("harvest", "p@ss/word:with#specials", 54321, "harvest_dev");
    assert!(
        !dsn.contains("p@ss/word"),
        "the raw password must not appear unencoded in {dsn}"
    );
    assert!(dsn.starts_with("postgres://harvest:"), "{dsn}");
    assert!(dsn.contains("@127.0.0.1:54321/harvest_dev"), "{dsn}");
    // A generated DSN must survive its own safety gate.
    assert!(matches!(
        classify_database_url(&dsn),
        DatabaseSafety::Allowed
    ));
}

#[test]
fn the_server_never_listens_beyond_loopback() {
    let conf = postgres_conf_lines(5432, Path::new("/tmp/harvest-dev-1-aa/socket")).join("\n");
    assert!(conf.contains("listen_addresses = '127.0.0.1'"), "{conf}");
    assert!(!conf.contains("0.0.0.0"), "{conf}");
    assert!(!conf.contains("'*'"), "{conf}");
}

#[test]
fn the_unix_socket_lives_inside_the_session_directory() {
    // Not cosmetic: Debian/Ubuntu's packaged Postgres defaults the socket to
    // `/var/run/postgresql`, which an ordinary developer cannot write to — the
    // postmaster then starts, fails `could not create lock file`, and shuts
    // down. Confining it here also means it is reclaimed with the session.
    let session = Path::new("/tmp/harvest-dev-1-aa");
    let socket_dir = session.join("socket");
    let conf = postgres_conf_lines(5432, &socket_dir).join("\n");
    // Built from the same path rather than spelled out: `Path::join` uses the
    // platform separator, so a hard-coded POSIX string asserts the separator
    // instead of the containment this test is about.
    assert!(
        conf.contains(&format!(
            "unix_socket_directories = '{}'",
            socket_dir.display()
        )),
        "{conf}"
    );
    assert!(
        !conf.contains("/var/run"),
        "the socket must never land in a system directory: {conf}"
    );
}

#[test]
fn a_quote_in_the_session_path_cannot_break_the_generated_config() {
    let conf = postgres_conf_lines(5432, Path::new("/tmp/o'brien/socket")).join("\n");
    assert!(
        conf.contains("unix_socket_directories = '/tmp/o''brien/socket'"),
        "a literal quote must be doubled, per Postgres's own escaping rule: {conf}"
    );
}

#[test]
fn the_default_session_layout_leaves_room_for_the_unix_socket() {
    // A Unix socket address is capped at 107 bytes, and the socket lives inside
    // the session directory — so the directory NAMES are load-bearing. A 32-hex
    // UUID in the session name pushed a perfectly ordinary `/tmp` layout over
    // the limit, and Postgres reports that as `could not create any Unix-domain
    // sockets`, which names neither the path nor the cap.
    let socket_dir = Path::new("/tmp/harvest-dev-4294967295/session-4294967295-0123abcd/socket");
    let len = unix_socket_path_len(socket_dir, u16::MAX);
    assert!(
        len <= MAX_UNIX_SOCKET_PATH_LEN,
        "the default layout must fit with room to spare: {len} > {MAX_UNIX_SOCKET_PATH_LEN}"
    );
}

#[test]
fn the_socket_path_length_accounts_for_the_socket_file_itself() {
    let dir = Path::new("/tmp/x");
    assert_eq!(
        unix_socket_path_len(dir, 5432),
        "/tmp/x/.s.PGSQL.5432".len()
    );
}

#[test]
fn the_server_is_tuned_for_a_throwaway_instance() {
    let conf = postgres_conf_lines(0, Path::new("/tmp/harvest-dev-1-aa/socket")).join("\n");
    assert!(conf.contains("fsync = off"), "{conf}");
    assert!(conf.contains("synchronous_commit = off"), "{conf}");
}

// ---------------------------------------------------------------------------
// AC3 — the banner
// ---------------------------------------------------------------------------

fn banner_inputs() -> BannerInputs {
    BannerInputs {
        ui_url: "http://127.0.0.1:3000/api/harvest/ui".to_owned(),
        api_url: "http://127.0.0.1:3000/api/harvest".to_owned(),
        sample_workflow: "dev_greeting".to_owned(),
        storage: StorageDescription::Provisioned {
            version: "16.4".to_owned(),
            data_dir: PathBuf::from("/tmp/harvest-dev-1234-ab/data"),
        },
    }
}

#[test]
fn banner_leads_with_the_ui_url() {
    let banner = render_banner(&banner_inputs());
    assert!(
        banner.contains("http://127.0.0.1:3000/api/harvest/ui"),
        "{banner}"
    );
}

#[test]
fn banner_carries_a_copy_pasteable_trigger_command() {
    let banner = render_banner(&banner_inputs());
    assert!(banner.contains("curl"), "{banner}");
    assert!(
        banner.contains("http://127.0.0.1:3000/api/harvest/workflows/dev_greeting/start"),
        "the trigger command must name the real start route: {banner}"
    );
}

#[test]
fn banner_states_it_is_not_for_production() {
    let banner = render_banner(&banner_inputs());
    let upper = banner.to_uppercase();
    assert!(upper.contains("NOT FOR PRODUCTION"), "{banner}");
}

#[test]
fn banner_says_where_the_ephemeral_state_lives_and_that_it_is_reclaimed() {
    let banner = render_banner(&banner_inputs());
    assert!(banner.contains("/tmp/harvest-dev-1234-ab/data"), "{banner}");
    assert!(
        banner.to_lowercase().contains("removed on exit"),
        "the teardown promise belongs in the banner: {banner}"
    );
}

#[test]
fn banner_names_a_byo_database_rather_than_claiming_to_own_it() {
    let mut inputs = banner_inputs();
    inputs.storage = StorageDescription::BringYourOwn {
        redacted_dsn: "postgres://u:***@localhost:5432/harvest_dev".to_owned(),
    };
    let banner = render_banner(&inputs);
    assert!(
        banner.contains("postgres://u:***@localhost:5432/harvest_dev"),
        "{banner}"
    );
    assert!(
        !banner.to_lowercase().contains("removed on exit"),
        "we must not promise to delete a database we did not create: {banner}"
    );
}

#[test]
fn redaction_covers_every_spelling_of_a_password() {
    // The banner is the one thing developers paste into issues and chat, so a
    // password may not survive in ANY accepted form — not just `user:pw@`.
    for dsn in [
        "postgres://u:hunter2@localhost:5432/app",
        "postgres://u@localhost:5432/app?password=hunter2",
        "postgres://localhost:5432/app?password=hunter2",
        "postgres://u:hunter2@localhost:5432/app?password=hunter2",
        "host=localhost dbname=app user=u password=hunter2",
        "postgres://u:p%40ss@localhost:5432/app?sslmode=disable&password=hunter2",
    ] {
        let redacted = redact_dsn(dsn);
        assert!(
            !redacted.contains("hunter2"),
            "the password survived redaction: {dsn} -> {redacted}"
        );
    }
}

#[test]
fn redaction_does_not_mangle_a_dsn_that_has_no_password() {
    let dsn = "postgres://u@localhost:5432/harvest_dev?sslmode=disable";
    assert_eq!(redact_dsn(dsn), dsn);
}

#[test]
fn redaction_is_not_confused_by_an_at_sign_after_the_authority() {
    // An `@` in the path or query is not userinfo, and must not be treated as
    // the end of one.
    let dsn = "postgres://u:hunter2@localhost:5432/app?options=-c%20search_path%3Da@b";
    let redacted = redact_dsn(dsn);
    assert!(!redacted.contains("hunter2"), "{redacted}");
    assert!(redacted.contains("@localhost:5432/app"), "{redacted}");
}

#[test]
fn banner_does_not_promise_a_restart_demonstration_provisioned_storage_cannot_give() {
    // Codex round 1 (P1). The banner used to say "kill this process mid-timer
    // and start it again to watch the run resume from history". With
    // provisioned storage that is impossible — `shutdown` deletes the cluster
    // and the reaper reclaims a killed run's directory — so a new user
    // following it would see an empty run and conclude the engine had lost
    // their workflow.
    let banner = render_banner(&banner_inputs());
    let lowered = banner.to_lowercase();
    assert!(
        !lowered.contains("resume from history"),
        "the provisioned banner must not promise restart-resume: {banner}"
    );
    assert!(
        !lowered.contains("start it\n  again") && !lowered.contains("start it again"),
        "the provisioned banner must not tell people to restart into a fresh cluster: {banner}"
    );
    // It should still point at where the demonstration *does* work.
    assert!(
        banner.contains("HARVEST_DEV_DATABASE_URL"),
        "offer the configuration that can actually survive a restart: {banner}"
    );
}

#[test]
fn banner_offers_the_restart_demonstration_only_for_a_database_we_did_not_create() {
    let mut inputs = banner_inputs();
    inputs.storage = StorageDescription::BringYourOwn {
        redacted_dsn: "postgres://u:***@localhost:5432/harvest_dev".to_owned(),
    };
    let banner = render_banner(&inputs);
    assert!(
        banner.to_lowercase().contains("resumes from history"),
        "a database that outlives the process CAN demonstrate durability: {banner}"
    );
}

#[test]
fn redaction_survives_a_quoted_password_containing_whitespace() {
    // Codex round 1 (P2). `split_whitespace()` redacted `password='foo` and
    // left `hunter2'` in a string whose whole purpose is to be safe to paste.
    for dsn in [
        "host=localhost password='foo hunter2' dbname=app",
        "host=localhost password = 'foo hunter2' dbname=app",
        r"host=localhost password='it\'s hunter2' dbname=app",
        "PASSWORD='foo hunter2' dbname=app",
        r"host=localhost password=hunter2\ still dbname=app",
    ] {
        let redacted = redact_dsn(dsn);
        assert!(
            !redacted.contains("hunter2"),
            "the password survived redaction: {dsn} -> {redacted}"
        );
        assert!(
            redacted.contains("dbname=app"),
            "redaction must not eat the rest of the DSN: {dsn} -> {redacted}"
        );
    }
}

#[test]
fn banner_never_leaks_a_password() {
    let mut inputs = banner_inputs();
    inputs.storage = StorageDescription::BringYourOwn {
        redacted_dsn: redact_dsn("postgres://u:hunter2@localhost:5432/harvest_dev"),
    };
    let banner = render_banner(&inputs);
    assert!(!banner.contains("hunter2"), "{banner}");
}

// ---------------------------------------------------------------------------
// AC4 — the gate is actually WIRED, not merely implemented
// ---------------------------------------------------------------------------
//
// The classifier above is pure and heavily tested, but a pure function nothing
// calls refuses nothing. These drive the real `DevRuntime::start`, which
// rejects before it provisions anything — so they need no Postgres.

#[tokio::test]
async fn starting_against_a_remote_database_is_refused() {
    let error = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
        database_url: Some("postgres://u:p@db.prod.example.com:5432/app".to_owned()),
        // Kernel-chosen. These tests are about DSN classification, but the port
        // is settled FIRST (deliberately — see `DevRuntime::start`), so a fixed
        // one makes them fight each other for 127.0.0.1:3000 when the harness
        // runs them in parallel, and the loser fails with a bind error instead
        // of the refusal it was asserting.
        http_port: 0,
        ..DevRuntimeConfig::default()
    })
    .await
    .expect_err("a remote database must be refused");
    assert!(
        matches!(
            error,
            autumn_harvest_plugin::dev::DevError::RefusedDatabase { .. }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_remote_database_is_refused_even_with_the_suspicious_name_opt_in() {
    // There is deliberately no override for a remote host, so the opt-in for a
    // production-shaped *name* must not double as one.
    let error = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
        database_url: Some("postgres://u:p@db.prod.example.com:5432/app".to_owned()),
        allow_suspicious_database_name: true,
        http_port: 0,
        ..DevRuntimeConfig::default()
    })
    .await
    .expect_err("a remote database must be refused regardless of the name opt-in");
    assert!(
        matches!(
            error,
            autumn_harvest_plugin::dev::DevError::RefusedDatabase { .. }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_production_shaped_local_name_needs_the_explicit_opt_in() {
    let error = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
        database_url: Some("postgres://u:p@127.0.0.1:5432/myapp_production".to_owned()),
        http_port: 0,
        ..DevRuntimeConfig::default()
    })
    .await
    .expect_err("a production-shaped name must need the opt-in");
    assert!(
        matches!(
            error,
            autumn_harvest_plugin::dev::DevError::SuspiciousDatabase { .. }
        ),
        "{error}"
    );
}

#[test]
fn no_test_here_reserves_the_fixed_default_http_port() {
    // A test that starts the runtime with `DevRuntimeConfig::default()` inherits
    // the default port 3000 and really does bind it, because `DevRuntime::start`
    // settles the port before anything else. Several such tests run in parallel
    // under the harness, so they fight over one global port and the loser fails
    // with `Address already in use` instead of whatever it was asserting.
    //
    // It cost a red macOS CI leg, and it is invisible on a developer machine
    // where 3000 happens to be free, so it is pinned here rather than trusted to
    // review. Every inline `DevRuntime::start(DevRuntimeConfig { .. })` must name
    // an `http_port`; pass 0 unless the test is specifically about a port.
    let source = std::fs::read_to_string(
        workspace_root().join("autumn-harvest-plugin/tests/dev_runtime_tests.rs"),
    )
    .expect("this test file");

    // Assembled at run time, never written out whole: a literal of the pattern
    // would appear in this very file and the scan would flag itself.
    let call = format!("{}{}", "DevRuntime::start(DevRuntimeConfig ", '{');
    let end = format!("{}{}", "..DevRuntimeConfig::", "default()");
    let (call, end) = (call.as_str(), end.as_str());

    let mut searched_from = 0;
    let mut checked = 0;
    while let Some(found) = source[searched_from..].find(call) {
        let start = searched_from + found + call.len();
        let stop = start
            + source[start..]
                .find(end)
                .expect("a config literal must end with the struct-update default");
        assert!(
            source[start..stop].contains("http_port:"),
            "a `DevRuntime::start` call at byte {start} leaves `http_port` at its default, \
             which binds the fixed port 3000 and races every other test that does the same; \
             pass `http_port: 0`"
        );
        checked += 1;
        searched_from = stop;
    }
    assert!(
        checked >= 3,
        "the guard found only {checked} call sites, so it is no longer scanning what it thinks"
    );
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[test]
fn config_defaults_to_provisioning_its_own_storage() {
    let config = DevRuntimeConfig::default();
    assert!(config.database_url.is_none());
    assert!(!config.allow_suspicious_database_name);
}

// ---------------------------------------------------------------------------
// AC6 — invariants
// ---------------------------------------------------------------------------

#[test]
fn dev_runtime_adds_no_migration_and_no_event_variant() {
    // AC6. The dev runtime provisions storage and then uses the engine exactly
    // as an embedder does: it must not grow the schema, the event contract, or
    // the set of `harvest_events` writers.
    //
    // Asserted as a property of the module's own sources rather than as a
    // migration count, deliberately: a hard-coded count would collide with every
    // unrelated migration landing in parallel, which is precisely what the
    // changelog-fragment convention exists to avoid.
    let dev_dir = workspace_root().join("autumn-harvest-plugin/src/dev");
    let sources = read_sources(&dev_dir);
    assert!(!sources.is_empty(), "the dev module should have sources");

    for (path, body) in &sources {
        // Comment lines are stripped first: this guard is about what the code
        // *does*, and a doc comment explaining that the dev runtime never
        // writes `harvest_events` must not fail the test that says so.
        let body = &strip_comment_lines(body);
        for forbidden in [
            "embed_migrations!",
            "WorkflowEvent",
            "harvest_events",
            "diesel::update",
        ] {
            assert!(
                !body.contains(forbidden),
                "{}: the dev runtime must not reference `{forbidden}` — it adds no migration, \
                 no event variant, and no writer of the append-only event log",
                path.display()
            );
        }
    }

    assert!(
        !dev_dir.join("migrations").exists(),
        "the dev runtime ships no migrations of its own"
    );
}

/// Drop whole-line comments, so the invariant guard reads code, not prose.
fn strip_comment_lines(source: &str) -> String {
    strip_comment_lines_with(source, "//")
}

/// [`strip_comment_lines`] for a file whose comment marker is not `//`.
fn strip_comment_lines_with(source: &str, marker: &str) -> String {
    source
        .lines()
        .filter(|line| !line.trim_start().starts_with(marker))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every `.rs` file under `dir`, recursively, as `(path, contents)`.
fn read_sources(dir: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            out.extend(read_sources(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && let Ok(body) = std::fs::read_to_string(&path)
        {
            out.push((path, body));
        }
    }
    out
}

fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("Cargo.lock").exists() {
        assert!(dir.pop(), "workspace root not found");
    }
    dir
}

// ---------------------------------------------------------------------------
// Codex round 3
// ---------------------------------------------------------------------------

#[test]
fn redaction_survives_a_quoted_password_containing_a_question_mark() {
    // Codex round 3 (P2). `?` starts a query string in a URI and is an ordinary
    // character in a keyword/value string. Splitting on it before knowing which
    // syntax the DSN was in cut `password='abc?hunter2'` in half: the scanner
    // redacted the head and the tail was appended back verbatim.
    for dsn in [
        "host=localhost password='abc?hunter2 ghi' dbname=harvest_dev",
        "host=localhost password=abc?hunter2 dbname=harvest_dev",
        "password='??hunter2' host=localhost",
    ] {
        let redacted = redact_dsn(dsn);
        assert!(
            !redacted.contains("hunter2"),
            "the password survived redaction: {dsn} -> {redacted}"
        );
        assert!(
            redacted.contains("host=localhost"),
            "redaction must not eat the rest of the DSN: {dsn} -> {redacted}"
        );
    }
}

#[test]
fn redaction_still_treats_a_real_uri_query_as_a_query() {
    // The other half of the same fix: deciding syntax by prefix must not stop
    // `?password=` in an actual URI from being redacted.
    let dsn = "postgres://harvest@localhost:5432/harvest_dev?password=hunter2&sslmode=disable";
    let redacted = redact_dsn(dsn);
    assert!(!redacted.contains("hunter2"), "{redacted}");
    assert!(redacted.contains("password=***"), "{redacted}");
    assert!(redacted.contains("sslmode=disable"), "{redacted}");
}

#[test]
fn a_session_record_is_replaced_rather_than_truncated_in_place() {
    // Codex round 3 (P2). The record is rewritten once the postmaster is up. A
    // truncating writer killed mid-rewrite leaves empty or partial JSON, and
    // `reap_stale_sessions` deliberately skips a record it cannot parse — so
    // the very mechanism meant to reclaim a killed run's postmaster would leak
    // it permanently instead.
    //
    // A finished file cannot tell the two writers apart, so this asks the
    // filesystem: a hard link pinned to the original inode still holds the old
    // bytes after a rename, and would have been truncated along with the
    // target by an in-place rewrite.
    let dir = tempfile::tempdir().expect("temp dir");
    let record = dir.path().join("session.json");
    write_private_atomic(&record, "{\"first\":true}").expect("first write");

    let witness = dir.path().join("witness.json");
    std::fs::hard_link(&record, &witness).expect("hard link");

    write_private_atomic(&record, "{\"second\":true}").expect("second write");

    assert_eq!(
        std::fs::read_to_string(&witness).expect("witness"),
        "{\"first\":true}",
        "the record was rewritten in place, so a kill mid-write could leave partial JSON"
    );
    assert_eq!(
        std::fs::read_to_string(&record).expect("record"),
        "{\"second\":true}"
    );

    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("tmp-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "the staging file must not survive the rename: {leftovers:?}"
    );
}

#[cfg(unix)]
#[test]
fn a_session_record_is_written_owner_only() {
    // The staging file is created before the rename, so it — not just the
    // final name — has to carry the private mode.
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().expect("temp dir");
    let record = dir.path().join("session.json");
    write_private_atomic(&record, "{}").expect("write");
    let mode = std::fs::metadata(&record)
        .expect("metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "mode was {:o}", mode & 0o777);
}

#[test]
fn root_is_refused_before_any_session_state_is_touched() {
    // Codex round 3 (P1). As `root`, `provision_storage` used to create and
    // harden the session root and run the reaper — which executes a recorded
    // `bin_dir`'s `pg_ctl` — before Postgres's own "cannot run as root" refusal
    // was reached. Any unprivileged local user can pre-create `harvest-dev-0`,
    // so that ordering handed them a root-executed binary.
    //
    // Asserted on the sources with comments stripped, the same way AC6 is: the
    // bug was an *ordering*, and the ordering is what has to stay true. A
    // behavioural test could only ever observe it on a machine running as root.
    let mod_rs = workspace_root().join("autumn-harvest-plugin/src/dev/mod.rs");
    let body = strip_comment_lines(&std::fs::read_to_string(&mod_rs).expect("mod.rs"));
    let refusal = body
        .find("refuse_to_run_as_root()")
        .expect("provision_storage must refuse root");
    let root_use = body
        .find("reaper::session_root(")
        .expect("provision_storage must create the session root");
    assert!(
        refusal < root_use,
        "the root refusal must come before the session root is created or reaped"
    );

    let postgres_rs = workspace_root().join("autumn-harvest-plugin/src/dev/postgres.rs");
    let body = strip_comment_lines(&std::fs::read_to_string(&postgres_rs).expect("postgres.rs"));
    let refusal = body
        .find("refuse_to_run_as_root()")
        .expect("EphemeralPostgres::start must refuse root");
    let root_use = body
        .find("session_root(")
        .expect("EphemeralPostgres::start must create the session root");
    assert!(
        refusal < root_use,
        "the root refusal must come before the session root is created"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn provisioning_as_root_creates_no_session_root() {
    // The behavioural half of the same finding, which only says anything on a
    // machine actually running as root — which plenty of container-based dev
    // environments, and this repo's own CI image, are.
    if !autumn_harvest_plugin::dev::running_as_root() {
        return;
    }
    let base = tempfile::tempdir().expect("temp dir");
    let config = DevRuntimeConfig {
        session_root: Some(base.path().to_path_buf()),
        // Kernel-chosen, so a CI runner that happens to have something on the
        // default 3000 fails this test for the wrong reason.
        http_port: 0,
        ..DevRuntimeConfig::default()
    };
    let error = autumn_harvest_plugin::dev::DevRuntime::start(config)
        .await
        .expect_err("running as root must be refused");
    assert!(
        matches!(error, autumn_harvest_plugin::dev::DevError::RunningAsRoot),
        "expected a root refusal, got: {error}"
    );
    let entries: Vec<_> = std::fs::read_dir(base.path())
        .expect("read dir")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        entries.is_empty(),
        "root must be refused before any session state exists, found: {entries:?}"
    );
}

// ---------------------------------------------------------------------------
// Codex round 4
// ---------------------------------------------------------------------------

#[test]
fn redaction_decodes_a_percent_encoded_password_key() {
    // Codex round 4 (P2). `tokio_postgres`'s URI parser percent-decodes query
    // keys before matching them (`config.rs`, `parse_params`), so
    // `?%70assword=` IS the password parameter to the code that dials — while
    // a comparison against the raw key printed the whole credential.
    for dsn in [
        "postgres://u@localhost/app?%70assword=hunter2",
        "postgres://u@localhost/app?%50ASSWORD=hunter2",
        "postgres://u@localhost/app?sslmode=disable&pass%77ord=hunter2",
    ] {
        let redacted = redact_dsn(dsn);
        assert!(
            !redacted.contains("hunter2"),
            "the password survived redaction: {dsn} -> {redacted}"
        );
    }
}

#[test]
fn redaction_leaves_other_query_parameters_byte_for_byte() {
    // Decoding is for the comparison only: nothing else in the DSN is
    // re-encoded or reshaped, so what a developer pastes still round-trips.
    let dsn = "postgres://u@localhost/app?application%5Fname=my%20app&connect_timeout=5";
    assert_eq!(redact_dsn(dsn), dsn);
}

#[test]
fn a_reused_owner_pid_does_not_strand_a_session_forever() {
    // Codex round 4 (P2). `owner_alive` is an identity answer — the caller
    // computes it from the recorded owner start token. Matching our own pid
    // used to short-circuit ahead of it, so a run that received a dead
    // predecessor's pid (ordinary under supervisors and pid namespaces) would
    // skip that session, and so would every run after it.
    let record = SessionRecord {
        owner_pid: 4242,
        owner_start_token: Some("111".to_owned()),
        postmaster_pid: Some(4243),
        postmaster_start_token: Some("222".to_owned()),
        bin_dir: None,
        data_dir: PathBuf::from("/tmp/harvest-dev-1/session-4242-0000000a/data"),
        created_at: chrono::Utc::now(),
    };

    // Same pid as ours, but the owner is *not* the recorded one: reap it.
    assert_eq!(
        decide_reap(&record, false, true, 4242),
        ReapDecision::StopThenRemove {
            postmaster_pid: 4243
        },
        "a stale record must be reaped even when its owner pid matches ours"
    );

    // Genuinely ours: still skipped, and still says so.
    assert_eq!(
        decide_reap(&record, true, true, 4242),
        ReapDecision::Skip(SkipReason::OwnedByThisProcess)
    );

    // Someone else's live session: skipped for the other reason.
    assert_eq!(
        decide_reap(&record, true, true, 99),
        ReapDecision::Skip(SkipReason::OwnerAlive)
    );
}

#[tokio::test]
async fn a_non_loopback_http_host_is_refused() {
    // Codex round 4 (P2). `http_host` is public and documented as
    // loopback-only, but a doc comment is not an enforcement — and the
    // management router is mounted with `.api(...)`, not `api_with_auth`,
    // precisely because it is supposed to be unreachable.
    for host in ["0.0.0.0", "::", "192.0.2.1"] {
        let error = autumn_harvest_plugin::dev::DevRuntime::start(DevRuntimeConfig {
            http_host: (*host).to_owned(),
            http_port: 0,
            ..DevRuntimeConfig::default()
        })
        .await
        .expect_err("a non-loopback bind address must be refused");
        assert!(
            matches!(
                error,
                autumn_harvest_plugin::dev::DevError::NonLoopbackHttpHost { .. }
            ),
            "{host}: {error}"
        );
    }
}

// ---------------------------------------------------------------------------
// Codex round 5
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[test]
fn the_uid_parser_reads_the_effective_id_not_the_real_one() {
    // Codex round 5 (P2). The `Uid:` line is `real effective saved filesystem`,
    // and `id -u` reports the *effective* id (`id -ru` is the real one). Reading
    // the first column made the two sources disagree for exactly the process the
    // root refusal must not fail open on: a setuid-root launcher, real uid 1000,
    // effective uid 0 — which would have read as "not root" and then reaped a
    // real user's session root, executing its recorded `pg_ctl` as root.
    use autumn_harvest_plugin::dev::parse_proc_status_uid;

    let setuid_root = "Name:\tharvest-dev\nUid:\t1000\t0\t0\t0\nGid:\t1000\t1000\t1000\t1000\n";
    assert_eq!(parse_proc_status_uid(setuid_root), Some(0));

    let ordinary = "Name:\tharvest-dev\nUid:\t1000\t1000\t1000\t1000\n";
    assert_eq!(parse_proc_status_uid(ordinary), Some(1000));

    assert_eq!(parse_proc_status_uid("Name:\tx\n"), None);
    assert_eq!(parse_proc_status_uid("Uid:\t1000\n"), None);
}

#[test]
fn an_unreadable_postmaster_pid_file_leaves_the_session_alone() {
    // Codex round 5 (P2). During the start window the record carries no
    // postmaster pid, so `postmaster.pid` is the only evidence a cluster is
    // running. A truncated file — exactly what a crash mid-write leaves —
    // parsed to `None`, which reads as "no server": `decide_reap` answered
    // `Remove`, deleting the data directory out from under a postmaster that
    // may well still be running, along with the only record that could stop it.
    //
    // Absence of a pid is evidence only when it is *confirmed* absence.
    let base = tempfile::tempdir().expect("temp dir");
    let root = autumn_harvest_plugin::dev::session_root(base.path()).expect("session root");

    let session_dir = root.join("session-4242-0000000a");
    let data_dir = session_dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let mut stale = record(u32::MAX - 1, None);
    stale.owner_start_token = None;
    stale.data_dir = data_dir.clone();
    std::fs::write(
        session_dir.join("session.json"),
        stale.to_json().expect("json"),
    )
    .expect("write record");

    // Half-written: the file exists, but there is no pid on its first line.
    std::fs::write(data_dir.join("postmaster.pid"), "\n/tmp/data\n").expect("pid file");
    assert_eq!(
        autumn_harvest_plugin::dev::reap_stale_sessions(&root).expect("reap"),
        0,
        "an unreadable postmaster.pid is uncertainty, not proof that nothing is running"
    );
    assert!(session_dir.exists(), "{}", session_dir.display());

    // Confirmed absent — `pg_ctl` removes it on a clean stop — so the session
    // really is a corpse and is reclaimed.
    std::fs::remove_file(data_dir.join("postmaster.pid")).expect("remove pid file");
    assert_eq!(
        autumn_harvest_plugin::dev::reap_stale_sessions(&root).expect("reap"),
        1,
        "with no pid file at all the session is a corpse and must be reclaimed"
    );
    assert!(!session_dir.exists(), "{}", session_dir.display());
}

// ---------------------------------------------------------------------------
// AC7 — the docs present the zero-setup path as the default
// ---------------------------------------------------------------------------

/// The getting-started chapter must lead with `cargo dev` and still keep the
/// Docker route, and the command it prints must be the one the alias defines.
///
/// A doc guard rather than prose, following the repo's existing
/// `*_docs.rs` convention: AC7 is the one criterion whose whole content is a
/// document, so an untested claim about it is no claim at all.
#[test]
fn the_getting_started_chapter_leads_with_the_zero_setup_path() {
    let root = workspace_root();
    let chapter = std::fs::read_to_string(root.join("docs/getting-started/01-project-skeleton.md"))
        .expect("chapter 1 should exist");

    let zero_setup = chapter
        .find("## The fastest path: `cargo dev`")
        .expect("the zero-setup path must have its own section");
    let bring_your_own = chapter
        .find("## Bring your own Postgres")
        .expect("the bring-your-own-Postgres path must be retained");
    assert!(
        zero_setup < bring_your_own,
        "the zero-setup path must come first — it is the default"
    );

    // The Docker/Compose route is retained, not replaced.
    assert!(chapter.contains("docker compose up -d"), "{chapter}");
    assert!(chapter.contains("compose.yaml"), "{chapter}");

    // And the alias the chapter tells people to run is the one that exists.
    // Comment lines are stripped: the file *explains* why it is not
    // `--release`, so reading the raw text matches the prose, not the alias.
    let alias = strip_comment_lines_with(
        &std::fs::read_to_string(root.join(".cargo/config.toml")).expect("cargo config"),
        "#",
    );
    assert!(
        alias.contains("\ndev = ["),
        "the `cargo dev` alias must exist: {alias}"
    );
    assert!(
        alias.contains("dev-runtime-managed"),
        "the alias must enable the tier that needs no installed Postgres: {alias}"
    );
    assert!(
        !alias.contains("--release"),
        "deliberately not --release: on a fresh clone the compile dominates the metric"
    );
}
