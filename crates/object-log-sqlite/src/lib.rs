//! `SQLite` state-machine demonstration for `object-log`.

#![deny(missing_docs)]
#![deny(unsafe_code)]

mod connection;
mod database;
mod format;
mod policy;
#[allow(unsafe_code)]
mod wal;

pub use database::{Database, SqliteCheckpointStatus, StageStatus, StagedWrite};

/// Fixed `SQLite` database page size.
pub const PAGE_SIZE: u32 = 4_096;

/// An invalid `SQLite` cache, WAL, durable record, or object-log operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqliteError {
    /// `SQLite` rejected an operation.
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// `SQLite` did not retain a required connection setting.
    #[error("invalid SQLite configuration: {0}")]
    Configuration(String),
    /// The committed WAL boundary is invalid or unreadable.
    #[error("invalid SQLite WAL: {0}")]
    InvalidWal(String),
    /// Durable adapter bytes do not use the current canonical record format.
    #[error("invalid SQLite record: {0}")]
    InvalidRecord(String),
    /// A database snapshot has an invalid file header, size, or page size.
    #[error("invalid SQLite database snapshot")]
    InvalidSnapshot,
    /// A payload cannot fit the fixed object-log byte and reference limits.
    #[error("SQLite payload exceeds the object-log limits")]
    PayloadLimit,
    /// The disposable cache is not safe to use until recovery completes.
    #[error("the SQLite cache requires recovery")]
    DirtyCache,
    /// An active garbage collection prevents a protected cache rebuild.
    #[error("garbage collection is active for this SQLite database")]
    CollectionActive,
    /// Another live database instance owns the same local cache path.
    #[error("the SQLite cache path is already open")]
    CacheInUse,
    /// The process-local cache-path registry is not usable.
    #[error("the SQLite cache-path registry is unavailable")]
    CacheRegistry,
    /// The generic object-log operation failed.
    #[error("object log: {0}")]
    Log(#[from] object_log::Error),
    /// A local disposable-cache operation failed.
    #[error("SQLite cache file: {0}")]
    Io(#[from] std::io::Error),
    /// A numeric value cannot be represented by `SQLite` or this platform.
    #[error("SQLite numeric limit exceeded")]
    Numeric(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};

    use rusqlite::TransactionBehavior;

    use super::{PAGE_SIZE, connection, wal};

    type TestResult = Result<(), Box<dyn StdError>>;

    #[test]
    fn journal_pointer_reads_only_the_committed_wal_prefix() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.sqlite3");
        let mut conn = connection::open(&path)?;
        conn.execute_batch("CREATE TABLE values_table (value INTEGER NOT NULL);")?;
        let first = wal::committed(&conn, PAGE_SIZE as usize)?;
        assert!(first.frames > 0);
        assert_eq!(first.bytes, fs::read(wal_path(&path))?);

        let before_rollback = first.clone();
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for value in 0..1_000 {
                tx.execute("INSERT INTO values_table VALUES (?1)", [value])?;
            }
        }
        assert_eq!(wal::committed(&conn, PAGE_SIZE as usize)?, before_rollback);

        {
            let mut tx = conn.transaction()?;
            tx.execute("INSERT INTO values_table VALUES (1)", [])?;
            {
                let savepoint = tx.savepoint()?;
                savepoint.execute("INSERT INTO values_table VALUES (2)", [])?;
            }
            tx.commit()?;
        }
        assert_eq!(
            conn.query_row("SELECT group_concat(value) FROM values_table", [], |row| {
                row.get::<_, String>(0)
            })?,
            "1"
        );
        let saved = wal::committed(&conn, PAGE_SIZE as usize)?;
        assert!(saved.frames > first.frames);
        assert_eq!(saved.bytes, fs::read(wal_path(&path))?);
        Ok(())
    }

    #[test]
    fn reset_ignores_an_old_physical_suffix_and_changes_salts() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("reset.sqlite3");
        let conn = connection::open(&path)?;
        conn.execute_batch(
            "CREATE TABLE values_table (value BLOB NOT NULL);
             INSERT INTO values_table VALUES (zeroblob(819200));",
        )?;
        let long = wal::committed(&conn, PAGE_SIZE as usize)?;
        let old_salts = &long.bytes[16..24];
        let checkpoint: (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(RESTART)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        assert_eq!(checkpoint.0, 0);
        conn.execute("INSERT INTO values_table VALUES (x'01')", [])?;

        let reset = wal::committed(&conn, PAGE_SIZE as usize)?;
        let physical = fs::read(wal_path(&path))?;
        assert_eq!(reset.frames, 1);
        assert_ne!(&reset.bytes[16..24], old_salts);
        assert_eq!(reset.bytes.as_ref(), &physical[..reset.bytes.len()]);
        assert!(physical.len() > reset.bytes.len());

        let truncate: (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        assert_eq!(truncate.0, 0);
        assert_eq!(
            wal::committed(&conn, PAGE_SIZE as usize)?,
            wal::WalImage {
                frames: 0,
                bytes: bytes::Bytes::new(),
            }
        );
        assert_eq!(fs::metadata(wal_path(&path))?.len(), 0);
        Ok(())
    }

    fn wal_path(database: &Path) -> PathBuf {
        let mut path: OsString = database.as_os_str().to_owned();
        path.push("-wal");
        PathBuf::from(path)
    }
}
