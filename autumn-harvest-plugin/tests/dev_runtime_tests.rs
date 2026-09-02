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
    RefusalReason, SessionRecord, StorageDescription, SuspicionReason, candidate_bin_dirs,
    classify_database_url, decide_reap, effective_postmaster_pid, ephemeral_dsn, http_authority,
    parse_postmaster_pid, postgres_conf_lines, proc_stat_is_live, proc_stat_start_time,
    record_is_self_consistent, redact_dsn, render_banner, resolve_bin_dir, unix_socket_path_len,
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

#[test]
fn classify_accepts_a_unix_socket_dsn() {
    // The shape `EphemeralPostgres` itself produces when it listens on a socket
    // directory rather than a TCP port.
    let dsn = "postgres:///harvest_dev?host=/tmp/harvest-dev-1234-ab/socket";
    assert!(
        matches!(classify_database_url(dsn), DatabaseSafety::Allowed),
        "unix-socket DSN must be allowed"
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
    let conf = postgres_conf_lines(5432, &session.join("socket")).join("\n");
    assert!(
        conf.contains("unix_socket_directories = '/tmp/harvest-dev-1-aa/socket'"),
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
