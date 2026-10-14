//! The durable workflow mutex (issue #2029, the Postgres feature of issue
//! #691).
//!
//! A run that acquires a key joins the FIFO queue of that key. It gets the
//! lock when the key is free and it is first in the queue. The grant mints a
//! new `lock_seq`, the fencing token, and removes the run from the queue.
//!
//! The single writer makes each step atomic. Postgres needs an advisory lock
//! per key and a lease for that. This backend needs neither. A holder that
//! crashes resumes on reopen and still holds its lock. A run that ends
//! releases every lock it holds, through [`release_all`].
//!
//! No step wakes a waiter. The fleet poll drives every run, so a waiter
//! tries again on the next pass. This matches Postgres, which appends no
//! event to the waiter either.

use autumn_harvest::ExecutionId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::SqliteResult;

/// Queue `exec` for `key`, then grant the lock if it is free and `exec` is
/// first in the queue. Returns the new `lock_seq` on a grant.
///
/// A repeat call keeps the place of `exec` in the queue.
pub fn acquire(conn: &Connection, exec: ExecutionId, key: &str) -> SqliteResult<Option<i64>> {
    let exec = exec.to_string();
    conn.execute(
        "INSERT OR IGNORE INTO harvest_mutex_waiters (lock_key, waiter_exec_id) VALUES (?1, ?2)",
        params![key, exec],
    )?;
    let taken: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM harvest_mutex_locks WHERE lock_key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    if taken.is_some() {
        return Ok(None);
    }
    let head: Option<String> = conn
        .query_row(
            "SELECT waiter_exec_id FROM harvest_mutex_waiters WHERE lock_key = ?1 \
             ORDER BY waiter_seq LIMIT 1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    if head.as_deref() != Some(exec.as_str()) {
        return Ok(None);
    }
    let lock_seq: i64 = conn.query_row(
        "INSERT INTO harvest_mutex_seq (id, last) VALUES (1, 1) \
         ON CONFLICT (id) DO UPDATE SET last = last + 1 RETURNING last",
        [],
        |row| row.get(0),
    )?;
    conn.execute(
        "INSERT INTO harvest_mutex_locks (lock_key, holder_exec_id, lock_seq) VALUES (?1, ?2, ?3)",
        params![key, exec, lock_seq],
    )?;
    conn.execute(
        "DELETE FROM harvest_mutex_waiters WHERE lock_key = ?1 AND waiter_exec_id = ?2",
        params![key, exec],
    )?;
    Ok(Some(lock_seq))
}

/// Release the lock on `key` that `exec` holds under `lock_seq`. Returns
/// `true` if a lock was released.
///
/// The guard of a released grant pushes its release again on each replay.
/// The fence makes that repeat a no-op, also after a later grant of the key.
pub fn release(
    conn: &Connection,
    exec: ExecutionId,
    key: &str,
    lock_seq: i64,
) -> SqliteResult<bool> {
    let n = conn.execute(
        "DELETE FROM harvest_mutex_locks \
         WHERE lock_key = ?1 AND holder_exec_id = ?2 AND lock_seq = ?3",
        params![key, exec.to_string(), lock_seq],
    )?;
    Ok(n > 0)
}

/// Release every lock that `exec` holds, and remove it from every queue.
///
/// Each terminal state setter in [`store`](crate::store) calls it in the
/// terminal transaction. A run that ends can then never block a key.
pub fn release_all(conn: &Connection, exec: ExecutionId) -> SqliteResult<()> {
    let exec = exec.to_string();
    conn.execute(
        "DELETE FROM harvest_mutex_locks WHERE holder_exec_id = ?1",
        params![exec],
    )?;
    conn.execute(
        "DELETE FROM harvest_mutex_waiters WHERE waiter_exec_id = ?1",
        params![exec],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use autumn_harvest::ExecutionId;
    use rusqlite::Connection;

    use super::{acquire, release, release_all};
    use crate::schema;

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    #[test]
    fn a_free_key_is_granted_with_a_new_token_each_time() {
        let conn = open();
        let (a, b) = (ExecutionId::new(), ExecutionId::new());
        let first = acquire(&conn, a, "k").unwrap().unwrap();
        assert!(release(&conn, a, "k", first).unwrap());
        let second = acquire(&conn, b, "k").unwrap().unwrap();
        assert!(second > first, "a token is never reused");
    }

    #[test]
    fn waiters_are_granted_in_arrival_order() {
        let conn = open();
        let (h, a, b) = (ExecutionId::new(), ExecutionId::new(), ExecutionId::new());
        let seq = acquire(&conn, h, "k").unwrap().unwrap();
        assert_eq!(acquire(&conn, a, "k").unwrap(), None);
        assert_eq!(acquire(&conn, b, "k").unwrap(), None);
        release(&conn, h, "k", seq).unwrap();
        // `b` asks first after the release, but `a` is ahead in the queue.
        assert_eq!(acquire(&conn, b, "k").unwrap(), None);
        assert!(acquire(&conn, a, "k").unwrap().is_some());
    }

    // H3: a replayed release with an old token frees nothing.
    #[test]
    fn a_stale_release_is_a_no_op() {
        let conn = open();
        let a = ExecutionId::new();
        let old = acquire(&conn, a, "k").unwrap().unwrap();
        release(&conn, a, "k", old).unwrap();
        let new = acquire(&conn, a, "k").unwrap().unwrap();
        assert!(!release(&conn, a, "k", old).unwrap());
        assert!(release(&conn, a, "k", new).unwrap());
    }

    // H4: a run that ends frees its locks and leaves every queue.
    #[test]
    fn release_all_frees_locks_and_queue_places() {
        let conn = open();
        let (a, b, c) = (ExecutionId::new(), ExecutionId::new(), ExecutionId::new());
        acquire(&conn, a, "k").unwrap().unwrap();
        assert_eq!(acquire(&conn, b, "k").unwrap(), None);
        assert_eq!(acquire(&conn, c, "k").unwrap(), None);
        release_all(&conn, b).unwrap();
        release_all(&conn, a).unwrap();
        assert!(acquire(&conn, c, "k").unwrap().is_some(), "c is now first");
    }
}
