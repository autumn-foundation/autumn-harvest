//! The fleet epoch of a Harvest file.
//!
//! A fleet tenant opens its Harvest file under an ownership epoch. The file
//! records the newest such epoch in a one-row table. A host that gets a range
//! back reads it before the open. When the replica holds a newer epoch of
//! another owner, the local file is stale, and the host replaces it.
//!
//! The table is not part of the Harvest schema. A file that never opened
//! under an epoch has no table, and reads as epoch 0.

use std::path::Path;

use rusqlite::{Connection, OpenFlags, OptionalExtension as _};

use crate::error::SqliteResult;

/// The table that holds the epoch row.
pub(crate) const TABLE: &str = "harvest_fleet_epoch";

/// The newest epoch that the Harvest file at `path` opened under, or 0.
///
/// It opens the file read only, so it never creates the file.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`](crate::SqliteError::Sqlite) when the file
/// cannot be opened or read.
pub fn read(path: &Path) -> SqliteResult<u64> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    read_on(&conn)
}

/// As [`read`], on an open connection.
pub(crate) fn read_on(conn: &Connection) -> SqliteResult<u64> {
    let present: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [TABLE],
            |row| row.get(0),
        )
        .optional()?;
    if present.is_none() {
        return Ok(0);
    }
    let epoch: Option<i64> = conn
        .query_row(
            &format!("SELECT epoch FROM {TABLE} WHERE id = 1"),
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(epoch.map_or(0, |epoch| u64::try_from(epoch).unwrap_or(0)))
}

/// Record that the file behind `conn` opens under `epoch`. It never lowers
/// the stored epoch.
pub(crate) fn raise_on(conn: &Connection, epoch: u64) -> SqliteResult<()> {
    let epoch = i64::try_from(epoch).unwrap_or(i64::MAX);
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {TABLE} \
         (id INTEGER PRIMARY KEY CHECK (id = 1), epoch INTEGER NOT NULL)"
    ))?;
    conn.execute(
        &format!(
            "INSERT INTO {TABLE} (id, epoch) VALUES (1, ?1) \
             ON CONFLICT (id) DO UPDATE SET epoch = max(epoch, excluded.epoch)"
        ),
        [epoch],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(dir: &tempfile::TempDir) -> std::path::PathBuf {
        let path = dir.path().join("acme.harvest.db");
        drop(Connection::open(&path).unwrap());
        path
    }

    #[test]
    fn a_file_with_no_table_reads_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(&file(&dir)).unwrap(), 0);
    }

    #[test]
    fn a_raise_never_lowers_the_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let path = file(&dir);
        let conn = Connection::open(&path).unwrap();
        raise_on(&conn, 5).unwrap();
        assert_eq!(read(&path).unwrap(), 5);
        raise_on(&conn, 3).unwrap();
        assert_eq!(read(&path).unwrap(), 5, "an older epoch");
        raise_on(&conn, 9).unwrap();
        assert_eq!(read_on(&conn).unwrap(), 9);
    }

    #[test]
    fn a_read_never_creates_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.harvest.db");
        assert!(read(&path).is_err());
        assert!(!path.exists());
    }
}
