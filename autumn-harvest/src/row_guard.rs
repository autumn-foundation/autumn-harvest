//! Keep a by-id task row statement on the primary key.
//!
//! Many statements find one `harvest_task_queue` row by `id`, then guard
//! it on `state` and `worker_id`. A partial index matches each such guard.
//! Both paths estimate one row, so the planner can pick the small partial
//! index over the primary key. That path reads each index entry that
//! matches the guard, then filters on `id`. A cached generic plan keeps that
//! choice.
//!
//! `row_guard_sql!` and `row_guard` hide such a guard from the planner.
//! [`guarded_statements`] lists each raw statement in scope and a sample of
//! each Diesel guard shape. A DB test checks that the generic and the custom
//! plan of each one read the table through its primary key. See
//! `docs/performance-claim-release-guard.md`.

#[cfg(feature = "db")]
use diesel::QueryDsl;
#[cfg(feature = "db")]
use diesel::expression::Expression;
#[cfg(feature = "db")]
use diesel::pg::Pg;
#[cfg(feature = "db")]
use diesel::sql_types::{Bool, BoolOrNullableBool};

/// Wrap the guard of a by-id statement so that no index can match it.
///
/// `CASE WHEN g THEN true END` is true when `g` is true and NULL in all
/// other cases. `WHERE` drops a NULL row as it drops a false row, so the
/// statement keeps the same rows. The planner cannot use a `CASE` as an
/// index condition. It also cannot prove a partial index predicate from
/// it. So it finds the row through `id = $1` and the primary key.
///
/// Use it only beside a primary key test, such as `id = $1`. A scan without
/// `id` needs its index. Use it only as a top-level `AND` term. `NOT` or `OR`
/// on a NULL result changes the rows.
macro_rules! row_guard_sql {
    ($guard:literal) => {
        concat!("(CASE WHEN ", $guard, " THEN true END)")
    };
}
pub(crate) use row_guard_sql;

/// The Diesel form of `row_guard_sql!`.
///
/// The `true` goes as a bind, so the statement keeps a static query id and
/// stays in the statement cache. The rules of `row_guard_sql!` apply.
#[cfg(feature = "db")]
pub(crate) fn row_guard<G>(guard: G) -> diesel::dsl::case_when<G, bool, Bool>
where
    G: Expression,
    G::SqlType: BoolOrNullableBool,
{
    diesel::dsl::case_when::<_, _, Bool>(guard, true)
}

/// One by-id statement and the types of its parameters.
#[cfg(feature = "db")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedStatement {
    /// A name for test output.
    pub name: &'static str,
    /// The SQL text, with `$n` parameters.
    pub sql: String,
    /// The SQL type of each parameter, in order.
    pub param_types: Vec<&'static str>,
    /// True when `sql` is the production text. A Diesel entry is false: it
    /// has the production guard, but its other clauses are a sample.
    pub raw: bool,
}

#[cfg(feature = "db")]
impl GuardedStatement {
    fn raw(name: &'static str, sql: &str, param_types: &[&'static str]) -> Self {
        Self {
            name,
            sql: sql.to_owned(),
            param_types: param_types.to_vec(),
            raw: true,
        }
    }

    fn diesel<Q>(name: &'static str, query: &Q, param_types: &[&'static str]) -> Self
    where
        Q: diesel::query_builder::QueryFragment<Pg>,
    {
        let text = diesel::debug_query::<Pg, _>(query).to_string();
        let sql = text
            .split_once(" -- binds: ")
            .map_or(text.as_str(), |(sql, _)| sql);
        Self {
            raw: false,
            ..Self::raw(name, sql, param_types)
        }
    }
}

/// Each by-id statement that the plan test checks.
///
/// The raw statements are the production text. The Diesel statements are
/// samples of each production guard shape. `queue::fence` and
/// `queue::claim_held` are the production helpers. A source walk in the
/// tests below checks each Diesel call site.
#[cfg(feature = "db")]
#[doc(hidden)]
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn guarded_statements() -> Vec<GuardedStatement> {
    use crate::error::CapabilityMissPhase;
    use crate::queue;
    use crate::schema::harvest_task_queue::dsl;
    use diesel::{BoolExpressionMethods, ExpressionMethods};
    use jiff_diesel::ToDiesel;

    const ID_WORKER: &[&str] = &["uuid", "text"];
    const CAPABILITY_MISS: &[&str] = &[
        "uuid",
        "text",
        "double precision",
        "integer",
        "text",
        "integer",
    ];

    let mut statements = vec![
        GuardedStatement::raw(
            "release_claim_if_queue_paused",
            crate::queue_pause::release_claim_if_queue_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim",
            crate::queue_pause::release_claim_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_activity_paused",
            crate::activity_pause::release_claim_if_activity_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_workflow_paused",
            crate::execution::release_claim_if_workflow_paused_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_claim_if_over_cap",
            queue::release_claim_if_over_cap_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_after_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_before_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "release_for_capability_miss_during_handler",
            queue::release_task_for_capability_miss_query(CapabilityMissPhase::DuringHandler),
            CAPABILITY_MISS,
        ),
        GuardedStatement::raw(
            "reset_capability_misses_after_inline_progress",
            queue::reset_capability_misses_after_inline_progress_query(),
            ID_WORKER,
        ),
        GuardedStatement::raw(
            "read_capability_miss_state",
            queue::read_capability_miss_state_query(),
            &["uuid", "text", "text"],
        ),
        GuardedStatement::raw(
            "release_suspended_workflow_claim",
            queue::release_suspended_workflow_claim_query(),
            &["uuid", "text", "integer", "integer"],
        ),
        GuardedStatement::raw(
            "pending_hint_rows",
            queue::pending_hint_rows_query(),
            &["uuid[]"],
        ),
        GuardedStatement::raw(
            "requeue_orphan",
            crate::poison_pill::requeue_orphan_stmt(),
            &[
                "uuid",
                "text",
                "integer",
                "integer",
                "bigint",
                "timestamptz",
            ],
        ),
    ];
    for reset in [true, false] {
        statements.push(GuardedStatement::raw(
            if reset {
                "park_workflow_task_sticky_reset"
            } else {
                "park_workflow_task_sticky_keep"
            },
            queue::park_workflow_task_sticky_query(reset),
            &["uuid", "text", "interval"],
        ));
        statements.push(GuardedStatement::raw(
            if reset {
                "park_workflow_task_reset"
            } else {
                "park_workflow_task_keep"
            },
            queue::park_workflow_task_query(reset),
            &["uuid"],
        ));
    }

    let id = uuid::Uuid::nil();
    let claim = queue::TaskClaim::new(id, "worker", 1);

    let fenced = queue::fence(
        diesel::update(dsl::harvest_task_queue.find(id))
            .set(dsl::state.eq("COMPLETED"))
            .into_boxed(),
        Some(&claim),
    );
    statements.push(GuardedStatement::diesel(
        "claim_fence",
        &fenced,
        &["text", "uuid", "text", "text", "integer", "boolean"],
    ));

    let running = diesel::update(
        dsl::harvest_task_queue
            .find(id)
            .filter(row_guard(dsl::state.eq("RUNNING"))),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "running_guard",
        &running,
        &["text", "uuid", "text", "boolean"],
    ));

    let abandoned = diesel::update(
        dsl::harvest_task_queue.find(id).filter(row_guard(
            dsl::state
                .eq("RUNNING")
                .and(dsl::started_at.eq(jiff::Timestamp::UNIX_EPOCH.to_diesel())),
        )),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "started_guard",
        &abandoned,
        &["text", "uuid", "text", "timestamptz", "boolean"],
    ));

    let workflow = diesel::update(dsl::harvest_task_queue.find(id).filter(row_guard(
        dsl::state.eq("RUNNING").and(dsl::task_type.eq("workflow")),
    )))
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "workflow_guard",
        &workflow,
        &["text", "uuid", "text", "text", "boolean"],
    ));

    let worker = dsl::harvest_task_queue
        .find(id)
        .filter(row_guard(
            dsl::state
                .eq("RUNNING")
                .and(dsl::worker_id.eq("worker"))
                .and(dsl::crash_strikes.eq(0))
                .and(dsl::attempt.ne(1)),
        ))
        .select(dsl::id);
    statements.push(GuardedStatement::diesel(
        "worker_guard",
        &worker,
        &["uuid", "text", "text", "integer", "integer", "boolean"],
    ));

    let lock = dsl::harvest_task_queue
        .find(id)
        .filter(row_guard(
            queue::claim_held("worker", 1).and(dsl::crash_strikes.eq(0)),
        ))
        .select(dsl::id)
        .for_update();
    statements.push(GuardedStatement::diesel(
        "claim_lock",
        &lock,
        &["uuid", "text", "text", "integer", "integer", "boolean"],
    ));

    let live = diesel::update(
        dsl::harvest_task_queue
            .find(id)
            .filter(row_guard(dsl::state.eq_any(["PENDING", "RUNNING"]))),
    )
    .set(dsl::state.eq("FAILED"));
    statements.push(GuardedStatement::diesel(
        "live_guard",
        &live,
        &["text", "uuid", "text[]", "boolean"],
    ));

    let pending = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::id.eq(id))
            .filter(row_guard(
                dsl::workflow_exec_id
                    .eq(Some(id))
                    .and(dsl::state.eq("PENDING")),
            )),
    )
    .set(dsl::state.eq("PENDING"));
    statements.push(GuardedStatement::diesel(
        "pending_guard",
        &pending,
        &["text", "uuid", "uuid", "text", "boolean"],
    ));

    statements
}

#[cfg(all(test, feature = "db"))]
mod tests {
    use super::guarded_statements;

    /// The engine files that hold by-id task queue statements.
    const SOURCES: [(&str, &str); 6] = [
        ("queue.rs", include_str!("queue.rs")),
        ("worker.rs", include_str!("worker.rs")),
        ("poison_pill.rs", include_str!("poison_pill.rs")),
        ("queue_pause.rs", include_str!("queue_pause.rs")),
        ("activity_pause.rs", include_str!("activity_pause.rs")),
        ("execution.rs", include_str!("execution.rs")),
    ];

    /// Each Diesel chain that finds one task queue row by id hides its guard.
    ///
    /// The plan test checks samples of the Diesel guards. This walk checks
    /// each call site, so a site that drops `row_guard` fails here.
    #[test]
    fn every_by_id_diesel_chain_hides_its_guard() {
        let mut chains = 0;
        for (file, source) in SOURCES {
            let lines: Vec<&str> = source.lines().collect();
            for (at, line) in lines.iter().enumerate() {
                let line = line.trim();
                let by_id = line.starts_with(".find(") || line.starts_with(".filter(dsl::id.eq(");
                let on_queue = lines[at.saturating_sub(2)..at]
                    .iter()
                    .any(|l| l.trim_end().ends_with("harvest_task_queue"));
                if !by_id || !on_queue {
                    continue;
                }
                chains += 1;
                for next in lines[at + 1..]
                    .iter()
                    .map(|l| l.trim())
                    .take_while(|l| l.starts_with('.') || l.starts_with("//"))
                {
                    for plain in [
                        ".filter(dsl::state",
                        ".filter(dsl::worker_id",
                        ".filter(claim_held",
                    ] {
                        assert!(
                            !next.starts_with(plain),
                            "{file}:{}: a by-id chain uses a plain guard: {next}",
                            at + 1
                        );
                    }
                }
            }
        }
        assert!(chains >= 10, "the walk found only {chains} by-id chains");
    }

    /// No raw statement tests `state` or `worker_id` plainly beside its id.
    #[test]
    fn every_by_id_raw_statement_hides_its_guard() {
        for (file, source) in SOURCES {
            // A string continuation drops the newline and the indent, as rustc does.
            let mut text = String::with_capacity(source.len());
            let mut rest = source;
            while let Some(at) = rest.find("\\\n") {
                text.push_str(&rest[..at]);
                rest = rest[at + 2..].trim_start();
            }
            text.push_str(rest);
            for plain in [
                " id = $1 AND state",
                " id = $1 AND worker_id",
                " id = $1 AND task_type = 'workflow' AND state",
                " id = ANY($1) AND state",
            ] {
                assert!(!text.contains(plain), "{file} has a plain guard: {plain}");
            }
        }
    }

    #[test]
    fn every_guarded_statement_hides_its_guard() {
        for statement in guarded_statements() {
            assert!(
                statement.sql.contains("CASE WHEN"),
                "{} must wrap its row guard in a CASE: {}",
                statement.name,
                statement.sql
            );
        }
    }

    #[test]
    fn statement_names_are_unique() {
        let statements = guarded_statements();
        let mut names: Vec<_> = statements.iter().map(|s| s.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), statements.len());
    }
}
