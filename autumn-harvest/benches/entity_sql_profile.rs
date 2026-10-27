//! Deterministic (non-criterion) instruction-count profiling harness for
//! `entity_sql::run`, the read-only SQL over one entity state (issue #2260).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! Each query opens a fresh in-memory SQLite database, as
//! `Worker::query_entity_sql` does. `ENTITY_SQL_PROFILE_SHAPE` picks the
//! state and the SQL:
//!
//! - `point` (default): one field of a small state with two items. The
//!   result is one row of one value. `N` does not apply.
//! - `scan`: an aggregate over `json_each` of an array of `N` items, with a
//!   filter on each item. The result is one row of two values.
//! - `rows`: one result row of two values for each of the `N` items. So the
//!   result conversion runs `2N` times. `N` must not pass `MAX_ROWS`.
//!
//! The harness builds the record once, outside the measured
//! `#[inline(never)]` wrapper `measure_run`. So `callgrind_annotate
//! --inclusive=yes` separates the query from the setup. Then `settle_heap`
//! makes glibc merge the small chunks that the setup freed.
//! `ENTITY_SQL_PROFILE_REPS` (default 1) repeats the query. The first query
//! of a process also runs the one-time SQLite initialization.
//!
//! The record has fixed ids. The progress handler reads the clock, but the
//! number of its calls depends on the step count only.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --features owner-disk-cache \
//!   --bench owner_disk_cache_profile --bench entity_sql_profile --no-run \
//!   --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="entity_sql_profile") | .executable')
//! ENTITY_SQL_PROFILE_SHAPE=scan ENTITY_SQL_PROFILE_N=1000 valgrind --tool=callgrind \
//!   --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use autumn_harvest::entity::{EntitySnapshot, EntityStats};
use autumn_harvest::entity_sql::{EntityRecord, EntitySqlRows, EntitySqlSource, MAX_ROWS, run};
use autumn_harvest::types::ExecutionId;
use serde_json::{Value, json};

/// The SQL of the `point` shape.
const POINT_SQL: &str = "SELECT json_extract(state, '$.owner') FROM entity";

/// The SQL of the `scan` shape. `?1` is the lowest quantity that counts.
const SCAN_SQL: &str = "SELECT count(*), sum(json_extract(value, '$.price_cents')) \
     FROM entity, json_each(entity.state, '$.items') \
     WHERE json_extract(value, '$.qty') >= ?1";

/// The SQL of the `rows` shape.
const ROWS_SQL: &str = "SELECT json_extract(value, '$.sku'), json_extract(value, '$.qty') \
     FROM entity, json_each(entity.state, '$.items')";

/// The lowest quantity that the `scan` filter keeps.
const SCAN_MIN_QTY: u64 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Point,
    Scan,
    Rows,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("ENTITY_SQL_PROFILE_SHAPE") else {
            return Self::Point;
        };
        match value.as_str() {
            "point" => Self::Point,
            "scan" => Self::Scan,
            "rows" => Self::Rows,
            other => panic!("ENTITY_SQL_PROFILE_SHAPE must be point, scan or rows, got {other:?}"),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Scan => "scan",
            Self::Rows => "rows",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

/// The quantity of item `i`, from 1 to 5.
const fn qty(i: usize) -> u64 {
    1 + (i % 5) as u64
}

/// The price of item `i`, in cents.
const fn price_cents(i: usize) -> u64 {
    (i as u64 * 137) % 100_000
}

fn sku(i: usize) -> String {
    format!("sku-{i:05}")
}

fn item(i: usize) -> Value {
    json!({
        "sku": sku(i),
        "qty": qty(i),
        "price_cents": price_cents(i),
    })
}

/// The state of each shape. The `point` state is the one of the
/// `entity_sql` unit tests.
fn state(shape: Shape, n: usize) -> Value {
    match shape {
        Shape::Point => json!({
            "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 5}],
            "owner": "ada",
        }),
        Shape::Scan | Shape::Rows => json!({
            "items": (0..n).map(item).collect::<Vec<_>>(),
            "owner": "ada",
        }),
    }
}

fn record(state: Value) -> EntityRecord {
    let mut counters = EntityStats::default();
    counters.applied = 3;
    EntityRecord::new(
        "cart",
        "cart-7",
        ExecutionId::from_uuid(uuid::Uuid::from_u128(7)),
        9,
        false,
        EntitySnapshot {
            state,
            stats: counters,
        },
    )
}

/// Ask the allocator for one 4 KiB block, then free it.
///
/// A request of this size makes glibc merge its small free chunks. The setup
/// frees many small chunks. Without this call, the first query pays for that
/// merge. The harness calls it once, after the setup.
#[inline(never)]
fn settle_heap() {
    drop(std::hint::black_box(Vec::<u8>::with_capacity(4096)));
}

#[inline(never)]
fn measure_run(record: &EntityRecord, sql: &str, params: &[Value]) -> EntitySqlRows {
    run(record, sql, params, EntitySqlSource::File)
        .unwrap_or_else(|e| panic!("the query must succeed: {e}"))
}

/// The one result row that the `scan` SQL must return over `n` items.
fn scan_expected(n: usize) -> Vec<Vec<Value>> {
    let kept = (0..n).filter(|i| qty(*i) >= SCAN_MIN_QTY);
    let count = kept.clone().count();
    let sum: u64 = kept.map(price_cents).sum();
    // SQLite `sum` of no rows is NULL.
    let sum = if count == 0 { Value::Null } else { json!(sum) };
    vec![vec![json!(count), sum]]
}

/// Assert the result of one query. It returns the number of result rows.
fn check(shape: Shape, n: usize, result: &EntitySqlRows) -> usize {
    match shape {
        Shape::Point => {
            assert_eq!(result.rows, vec![vec![json!("ada")]], "the point query");
        }
        Shape::Scan => {
            assert_eq!(result.rows, scan_expected(n), "the scan aggregate");
        }
        Shape::Rows => {
            assert_eq!(
                result.rows.len(),
                n,
                "the rows query returns one row per item"
            );
            if let Some(last) = n.checked_sub(1) {
                assert_eq!(
                    result.rows[last],
                    vec![json!(sku(last)), json!(qty(last))],
                    "the last row"
                );
            }
        }
    }
    result.rows.len()
}

fn main() {
    let shape = Shape::from_env();
    let n = env_usize("ENTITY_SQL_PROFILE_N", 1_000);
    let reps = env_usize("ENTITY_SQL_PROFILE_REPS", 1);
    assert!(
        shape != Shape::Rows || n <= MAX_ROWS,
        "ENTITY_SQL_PROFILE_N must not pass MAX_ROWS ({MAX_ROWS}) for the rows shape"
    );
    let record = record(state(shape, n));
    let (sql, params) = match shape {
        Shape::Point => (POINT_SQL, Vec::new()),
        Shape::Scan => (SCAN_SQL, vec![json!(SCAN_MIN_QTY)]),
        Shape::Rows => (ROWS_SQL, Vec::new()),
    };
    settle_heap();
    let mut rows = 0_usize;
    for _ in 0..reps {
        let result = measure_run(&record, sql, &params);
        rows += check(shape, n, &result);
    }
    println!(
        "entity_sql_profile: shape={} n={n} reps={reps} rows={rows}",
        shape.name()
    );
}
