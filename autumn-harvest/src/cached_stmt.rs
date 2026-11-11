//! Cached prepared statements for constant SQL text (issue #2651).
//!
//! Diesel caches a prepared statement only when the statement reports that
//! caching is safe. Three common forms report that it is not:
//!
//! - every `diesel::sql_query` statement,
//! - a DSL statement that holds a `sql::<T>()` fragment, and
//! - an insert with an `Option` value, which renders `DEFAULT` for `None`.
//!
//! Diesel cannot know that such text comes from a bounded set, so it sends
//! the statement unnamed. The cost of that is high on a hot path. The driver
//! prepares the statement in one round trip, runs it in a second one, then
//! closes it. Postgres parses, analyses and plans the text on every call.
//! The issue #2651 census measured more planning time than execution time on
//! the canonical workflow.
//!
//! [`CachePreparedDsl::cache_prepared`] marks one statement as safe to cache.
//! The connection then keeps one prepared statement per distinct text and
//! bind type list. A later call sends Bind and Execute only, and Postgres can
//! keep a generic plan.
//!
//! # When to use it
//!
//! Use it on a statement that meets all three rules:
//!
//! 1. **The text comes from a small fixed set.** A constant qualifies. So
//!    does a DSL statement with a fixed shape. A statement built with
//!    `format!` from a runtime value does not: each distinct text adds one
//!    prepared statement to every connection, and nothing evicts it.
//! 2. **The result columns are named.** A cached statement that returns `*`
//!    fails with `cached plan must not change result type` after a migration
//!    adds a column. A rolling upgrade would then break each old worker. A
//!    debug build refuses such text. See [`names_its_result_columns`].
//! 3. **The generic plan is acceptable.** Postgres plans the first five runs
//!    of a prepared statement with the bind values. After that it keeps the
//!    generic plan when its estimate is not higher. A lookup by primary key
//!    has one plan either way. Check a statement that filters a skewed column
//!    through a bind before you cache it.
//!
//! [`queue::CachedClaimQuery`](crate::queue) is the older, hand-written form
//! of the same idea (issue #1971).
//!
//! # Constant fragments
//!
//! A DSL statement is often unsafe to cache because of one `sql::<T>()`
//! fragment with constant text. [`ConstSql`] is that fragment with a type of
//! its own. It is safe to cache and it has a static query id. The statement
//! around it then stays cacheable with no mark at the call site.
//! [`const_sql!`](crate::const_sql) declares one.

use diesel::QueryResult;
use diesel::expression::{
    AppearsOnTable, Expression, SelectableExpression, TypedExpressionType, ValidGrouping,
    is_aggregate,
};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use std::marker::PhantomData;

/// A statement that the connection may cache as a prepared statement.
///
/// Build one with [`CachePreparedDsl::cache_prepared`]. It renders the SQL
/// and the binds of the statement that it wraps, with no change.
#[derive(Debug, Clone, Copy)]
#[must_use = "a statement does nothing until it runs"]
pub struct CachePrepared<Q> {
    statement: Q,
    /// False sends the statement unnamed, as diesel does for unsafe text.
    cache: bool,
}

/// The most rows of one insert that [`cache_small_batch`] caches.
///
/// A batch insert has one text per row count. A decision appends one event
/// in most cases, and a fan-out appends a few. This bound keeps the texts of
/// one insert shape to a small set.
///
/// [`cache_small_batch`]: CachePreparedDsl::cache_small_batch
pub const CACHED_BATCH_ROWS: usize = 8;

/// Adds [`cache_prepared`](CachePreparedDsl::cache_prepared) to a statement.
pub trait CachePreparedDsl: Sized {
    /// Let the connection cache this statement as a prepared statement.
    ///
    /// Read the [module rules](self) first. The text must come from a small
    /// fixed set, and it must name its result columns.
    fn cache_prepared(self) -> CachePrepared<Self> {
        self.cache_prepared_if(true)
    }

    /// [`cache_prepared`](Self::cache_prepared) when `cache` is true.
    ///
    /// With `false` the statement is sent unnamed. Use it when one call site
    /// renders a bounded text in the common case and an unbounded text in a
    /// rare case.
    fn cache_prepared_if(self, cache: bool) -> CachePrepared<Self> {
        CachePrepared {
            statement: self,
            cache,
        }
    }

    /// Cache a batch insert of `rows` rows when the batch is small.
    ///
    /// The row count is part of the text of a batch insert. A batch of at
    /// most [`CACHED_BATCH_ROWS`] rows is cached. A larger batch is sent
    /// unnamed, so the cache holds a bounded set of texts.
    fn cache_small_batch(self, rows: usize) -> CachePrepared<Self> {
        self.cache_prepared_if(rows <= CACHED_BATCH_ROWS)
    }
}

impl<Q: QueryFragment<Pg>> CachePreparedDsl for Q {}

impl<Q> QueryId for CachePrepared<Q> {
    type QueryId = ();

    /// No static id, so the cache key is the SQL text and the bind types.
    ///
    /// The wrapped statement can have a static id and still render more than
    /// one text. An insert with an `Option` value is the example. A key by
    /// type would then run the first text for every later value.
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Q: Query> Query for CachePrepared<Q> {
    type SqlType = Q::SqlType;
}

impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for CachePrepared<Q> {
    fn walk_ast<'b>(&'b self, pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        self.statement.walk_ast(pass)
    }

    fn is_safe_to_cache_prepared(&self, backend: &Pg) -> QueryResult<bool> {
        #[cfg(debug_assertions)]
        if self.cache {
            use diesel::query_builder::QueryBuilder;
            let mut builder = diesel::pg::PgQueryBuilder::default();
            self.statement.to_sql(&mut builder, backend)?;
            let sql = builder.finish();
            debug_assert!(
                names_its_result_columns(&sql),
                "a cached statement must name its result columns: {sql}"
            );
        }
        let _ = backend;
        Ok(self.cache)
    }
}

/// The text and the SQL type of one constant fragment.
///
/// A marker type implements it. [`const_sql!`](crate::const_sql) writes the
/// marker and this impl.
pub trait ConstSqlFragment: 'static {
    /// The SQL type of the fragment.
    type SqlType: TypedExpressionType;
    /// The text of the fragment. It holds no bind.
    const SQL: &'static str;
}

/// A constant SQL fragment that a cached statement can hold.
///
/// `diesel::dsl::sql` marks its statement as unsafe to cache, because its
/// text can come from anywhere. This type takes its text from the constant
/// of a marker type. Two statements with different fragments then have
/// different types, so the static query id of each is exact.
///
/// The fragment is valid in every `FROM` clause and in every grouping, as a
/// `sql::<T>()` fragment is. The caller checks that the text fits.
pub struct ConstSql<F>(PhantomData<fn() -> F>);

impl<F> ConstSql<F> {
    /// The fragment of the marker type `F`.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<F> Default for ConstSql<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> Clone for ConstSql<F> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<F> Copy for ConstSql<F> {}

impl<F: ConstSqlFragment> std::fmt::Debug for ConstSql<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ConstSql").field(&F::SQL).finish()
    }
}

impl<F: ConstSqlFragment> Expression for ConstSql<F> {
    type SqlType = F::SqlType;
}

impl<F: ConstSqlFragment> QueryFragment<Pg> for ConstSql<F> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql(F::SQL);
        Ok(())
    }
}

impl<F: ConstSqlFragment> QueryId for ConstSql<F> {
    type QueryId = Self;

    const HAS_STATIC_QUERY_ID: bool = true;
}

impl<QS, F: ConstSqlFragment> SelectableExpression<QS> for ConstSql<F> {}

impl<QS, F: ConstSqlFragment> AppearsOnTable<QS> for ConstSql<F> {}

impl<F, GB> ValidGrouping<GB> for ConstSql<F> {
    type IsAggregate = is_aggregate::Never;
}

/// Declare the marker type of one [`ConstSql`] fragment.
///
/// ```
/// autumn_harvest::const_sql! {
///     /// The clock of the statement, not of the transaction.
///     pub ClockTimestamp: diesel::sql_types::Timestamptz = "clock_timestamp()"
/// }
/// let now = autumn_harvest::cached_stmt::ConstSql::<ClockTimestamp>::new();
/// # let _ = now;
/// ```
#[macro_export]
macro_rules! const_sql {
    ($(#[$meta:meta])* $vis:vis $name:ident: $sql_type:ty = $sql:expr) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy)]
        $vis struct $name;

        impl $crate::cached_stmt::ConstSqlFragment for $name {
            type SqlType = $sql_type;
            const SQL: &'static str = $sql;
        }
    };
}

const_sql! {
    /// `NOW()`: the start time of the transaction, on the database clock.
    pub PgNow: diesel::sql_types::Timestamptz = "NOW()"
}

/// `NOW()` as a fragment that keeps its statement cached.
///
/// `diesel::dsl::now` renders `CURRENT_TIMESTAMP`. This one keeps the text
/// of a statement that was written with `sql::<Timestamptz>("NOW()")`.
#[must_use]
pub const fn pg_now() -> ConstSql<PgNow> {
    ConstSql::new()
}

/// Whether `sql` names each result column, so its result type is stable.
///
/// It refuses a `*` in a result list: `SELECT *`, `SELECT t.*`,
/// `RETURNING *` and `DISTINCT *`. It accepts `COUNT(*)` and a
/// multiplication, because neither changes with the table.
///
/// The test reads the token in front of each `*`. A `*` in a string literal
/// or a comment can give a false refusal. It cannot give a false pass.
#[must_use]
pub fn names_its_result_columns(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    bytes
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'*')
        .all(|(star, _)| !star_is_a_result_list(&bytes[..star]))
}

/// Whether the text in front of a `*` makes that `*` a result list.
fn star_is_a_result_list(before: &[u8]) -> bool {
    let before = before.trim_ascii_end();
    let Some(last) = before.last() else {
        return true;
    };
    if matches!(last, b'.' | b',') {
        return true;
    }
    let word_start = before
        .iter()
        .rposition(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'_'))
        .map_or(0, |at| at + 1);
    let word = &before[word_start..];
    [&b"SELECT"[..], b"RETURNING", b"DISTINCT", b"ALL"]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::query_builder::QueryBuilder;
    use diesel::sql_types::{Integer, Text};

    /// Whether the connection may cache `query`. The bound picks the
    /// default dialect impl of a select statement.
    fn safe_to_cache<Q: QueryFragment<Pg>>(query: &Q) -> bool {
        query.is_safe_to_cache_prepared(&Pg).expect("walk")
    }

    fn sql_of<Q: QueryFragment<Pg>>(query: &Q) -> String {
        let mut builder = diesel::pg::PgQueryBuilder::default();
        query
            .to_sql(&mut builder, &Pg)
            .expect("render the statement");
        builder.finish()
    }

    #[test]
    fn a_raw_statement_is_not_cached_until_it_is_marked() {
        let raw = diesel::sql_query("SELECT id FROM t WHERE id = $1").bind::<Integer, _>(1);
        assert!(!safe_to_cache(&raw));
        let cached = raw.cache_prepared();
        assert!(safe_to_cache(&cached));
    }

    #[test]
    fn a_false_condition_sends_the_statement_unnamed() {
        let raw = || diesel::sql_query("SELECT id FROM t");
        assert!(!safe_to_cache(&raw().cache_prepared_if(false)));
        assert!(safe_to_cache(&raw().cache_prepared_if(true)));
    }

    #[test]
    fn a_small_batch_is_cached_and_a_large_one_is_not() {
        let raw = || diesel::sql_query("INSERT INTO t (a) VALUES ($1)");
        for (rows, cached) in [
            (0, true),
            (1, true),
            (CACHED_BATCH_ROWS, true),
            (CACHED_BATCH_ROWS + 1, false),
            (10_000, false),
        ] {
            assert_eq!(
                safe_to_cache(&raw().cache_small_batch(rows)),
                cached,
                "{rows} rows"
            );
        }
    }

    #[test]
    fn the_wrapper_renders_the_same_text_and_binds() {
        let raw = || {
            diesel::sql_query("SELECT a FROM t WHERE a = $1 AND b = $2")
                .bind::<Integer, _>(7)
                .bind::<Text, _>("seven")
        };
        assert_eq!(sql_of(&raw()), sql_of(&raw().cache_prepared()));
        let debug_raw = diesel::debug_query::<Pg, _>(&raw()).to_string();
        let debug_cached = diesel::debug_query::<Pg, _>(&raw().cache_prepared()).to_string();
        assert_eq!(debug_raw, debug_cached);
        assert!(debug_cached.contains("seven"), "{debug_cached}");
    }

    #[test]
    fn the_cache_key_is_the_text_and_not_the_type() {
        type Marked = CachePrepared<diesel::query_builder::SqlQuery>;
        const { assert!(!<Marked as QueryId>::HAS_STATIC_QUERY_ID) };
        assert!(<Marked as QueryId>::query_id().is_none());
    }

    #[test]
    fn a_dsl_statement_with_a_sql_fragment_is_cached_once_marked() {
        use diesel::prelude::*;
        diesel::table! {
            probe (id) {
                id -> Integer,
            }
        }
        let query = || probe::table.select(diesel::dsl::sql::<Integer>("id + 1"));
        assert!(!safe_to_cache(&query()));
        assert!(safe_to_cache(&query().cache_prepared()));
        assert_eq!(sql_of(&query()), sql_of(&query().cache_prepared()));
    }

    crate::const_sql! {
        /// A fragment for the tests.
        ProbePlusOne: Integer = "id + 1"
    }

    crate::const_sql! {
        /// A second fragment with another text.
        ProbePlusTwo: Integer = "id + 2"
    }

    #[test]
    fn a_const_fragment_keeps_its_statement_cacheable() {
        use diesel::prelude::*;
        diesel::table! {
            const_probe (id) {
                id -> Integer,
            }
        }
        let literal = const_probe::table.select(diesel::dsl::sql::<Integer>("id + 1"));
        let constant = const_probe::table.select(ConstSql::<ProbePlusOne>::new());
        assert_eq!(sql_of(&literal), sql_of(&constant));
        assert!(!safe_to_cache(&literal));
        assert!(safe_to_cache(&constant));
    }

    #[test]
    fn each_const_fragment_has_its_own_static_query_id() {
        type One = ConstSql<ProbePlusOne>;
        type Two = ConstSql<ProbePlusTwo>;
        const { assert!(<One as QueryId>::HAS_STATIC_QUERY_ID) };
        assert_ne!(<One as QueryId>::query_id(), <Two as QueryId>::query_id());
        assert_eq!(sql_of(&One::new()), "id + 1");
        assert_eq!(sql_of(&Two::new()), "id + 2");
    }

    #[test]
    fn named_result_columns_pass() {
        for sql in [
            "SELECT id, state FROM harvest_task_queue WHERE id = $1",
            "SELECT COUNT(*) AS n FROM harvest_events",
            "SELECT count( * ) FROM t",
            "SELECT a * 2 AS doubled FROM t",
            "SELECT (a + b) * $1 AS scaled FROM t",
            "UPDATE t SET a = a * $1 WHERE id = $2 RETURNING id, a",
            "SELECT f(t.c, t.p) FROM unnest($1, $2) AS t(c, p)",
        ] {
            assert!(names_its_result_columns(sql), "{sql}");
        }
    }

    #[test]
    fn a_star_result_list_fails() {
        for sql in [
            "SELECT * FROM t",
            "select * from t",
            "SELECT e.* FROM harvest_events e",
            "SELECT id, * FROM t",
            "SELECT DISTINCT * FROM t",
            "INSERT INTO t (a) VALUES ($1) RETURNING *",
            "UPDATE t SET a = 1 RETURNING\n  *",
            "WITH x AS (SELECT t.* FROM t) SELECT id FROM x",
            "*",
        ] {
            assert!(!names_its_result_columns(sql), "{sql}");
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a cached statement must name its result columns")]
    fn a_debug_build_refuses_a_star_statement() {
        let _ = diesel::sql_query("SELECT * FROM t")
            .cache_prepared()
            .is_safe_to_cache_prepared(&Pg);
    }
}
