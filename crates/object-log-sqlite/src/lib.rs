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

const PAGE_SIZE: u32 = 4_096;

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
    /// Another live database instance owns the same local cache path.
    #[error("the SQLite cache path is already open")]
    CacheInUse,
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

    use super::{PAGE_SIZE, SqliteError, connection, wal};

    type TestResult = Result<(), Box<dyn StdError>>;

    #[test]
    fn journal_pointer_reads_only_the_committed_wal_prefix() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("capture.sqlite3");
        let mut conn = connection::open(&path)?;
        conn.execute_batch("CREATE TABLE values_table (value INTEGER NOT NULL);")?;
        let first = committed(&conn, &wal::WalPosition::default())?;
        let header = first.position.header.ok_or("capture has no WAL header")?;
        let physical = fs::read(wal_path(&path))?;
        assert!(first.position.frames > 0);
        assert_eq!(header.as_slice(), &physical[..wal::WAL_HEADER_BYTES]);
        assert_eq!(
            first.bytes.as_ref(),
            &physical[wal::WAL_HEADER_BYTES..wal::WAL_HEADER_BYTES + first.bytes.len()]
        );
        assert_eq!(
            wal::validate_complete(&header, &first.bytes)?,
            first.position
        );

        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for value in 0..1_000 {
                tx.execute("INSERT INTO values_table VALUES (?1)", [value])?;
            }
        }
        let rollback = committed(&conn, &first.position)?;
        assert!(rollback.bytes.is_empty());
        assert_eq!(rollback.position, first.position);

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
        let saved = committed(&conn, &first.position)?;
        assert!(saved.position.frames > first.position.frames);
        let physical = fs::read(wal_path(&path))?;
        let frame_size = PAGE_SIZE as usize + wal::WAL_FRAME_HEADER_BYTES;
        let start = wal::WAL_HEADER_BYTES + usize::try_from(first.position.frames)? * frame_size;
        assert_eq!(
            saved.bytes.as_ref(),
            &physical[start..start + saved.bytes.len()]
        );
        assert_eq!(
            wal::validate_record(&header, &saved.bytes, first.position)?,
            saved.position
        );
        let mut complete = first.bytes.to_vec();
        complete.extend_from_slice(&saved.bytes);
        assert_eq!(wal::validate_complete(&header, &complete)?, saved.position);
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
        let long = committed(&conn, &wal::WalPosition::default())?;
        let long_header = long.position.header.ok_or("capture has no WAL header")?;
        let old_salts = long_header[16..24].to_owned();
        let checkpoint: (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(RESTART)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        assert_eq!(checkpoint.0, 0);
        conn.execute("INSERT INTO values_table VALUES (x'01')", [])?;

        assert!(committed(&conn, &long.position).is_err());
        let reset = committed(&conn, &wal::WalPosition::default())?;
        let reset_header = reset.position.header.ok_or("capture has no WAL header")?;
        let physical = fs::read(wal_path(&path))?;
        assert_eq!(reset.position.frames, 1);
        assert_ne!(reset_header[16..24], old_salts);
        assert_eq!(reset_header.as_slice(), &physical[..wal::WAL_HEADER_BYTES]);
        assert_eq!(
            reset.bytes.as_ref(),
            &physical[wal::WAL_HEADER_BYTES..wal::WAL_HEADER_BYTES + reset.bytes.len()]
        );
        assert!(physical.len() > wal::WAL_HEADER_BYTES + reset.bytes.len());

        let truncate: (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        assert_eq!(truncate.0, 0);
        assert!(committed(&conn, &reset.position).is_err());
        let empty = committed(&conn, &wal::WalPosition::default())?;
        assert_eq!(empty.position, wal::WalPosition::default());
        assert!(empty.bytes.is_empty());
        assert_eq!(fs::metadata(wal_path(&path))?.len(), 0);
        Ok(())
    }

    #[test]
    fn complete_wal_validation_rejects_corruption() -> TestResult {
        #[derive(Clone, Copy, Debug)]
        enum Corruption {
            Magic,
            Format,
            PageSize,
            HeaderData,
            HeaderChecksum,
            FrameData,
            FrameChecksum,
            Salt,
            PageNumber,
            OutOfRangePageNumber,
            CommitMarker,
            Alignment,
        }

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt.sqlite3");
        let conn = connection::open(&path)?;
        conn.execute_batch(
            "CREATE TABLE values_table (value BLOB NOT NULL);
             INSERT INTO values_table VALUES (zeroblob(8192));",
        )?;
        let capture = committed(&conn, &wal::WalPosition::default())?;
        let valid_header = capture.position.header.ok_or("capture has no WAL header")?;
        wal::validate_complete(&valid_header, &capture.bytes)?;
        let frame_size = PAGE_SIZE as usize + wal::WAL_FRAME_HEADER_BYTES;
        let last = capture.bytes.len() - frame_size;

        for corruption in [
            Corruption::Magic,
            Corruption::Format,
            Corruption::PageSize,
            Corruption::HeaderData,
            Corruption::HeaderChecksum,
            Corruption::FrameData,
            Corruption::FrameChecksum,
            Corruption::Salt,
            Corruption::PageNumber,
            Corruption::OutOfRangePageNumber,
            Corruption::CommitMarker,
            Corruption::Alignment,
        ] {
            let mut header = valid_header;
            let mut frames = capture.bytes.to_vec();
            match corruption {
                Corruption::Magic => header[..4].fill(0),
                Corruption::Format => header[4..8].fill(0),
                Corruption::PageSize => header[8..12].copy_from_slice(&1_024_u32.to_be_bytes()),
                Corruption::HeaderData => header[12] ^= 1,
                Corruption::HeaderChecksum => header[24] ^= 1,
                Corruption::FrameData => frames[wal::WAL_FRAME_HEADER_BYTES] ^= 1,
                Corruption::FrameChecksum => frames[16] ^= 1,
                Corruption::Salt => frames[8] ^= 1,
                Corruption::PageNumber => frames[..4].fill(0),
                Corruption::OutOfRangePageNumber => frames[..4].fill(0xff),
                Corruption::CommitMarker => frames[last + 4..last + 8].fill(0),
                Corruption::Alignment => {
                    frames.pop();
                }
            }
            let expected = match corruption {
                Corruption::Magic | Corruption::Format | Corruption::PageSize => {
                    "invalid WAL header"
                }
                Corruption::HeaderData | Corruption::HeaderChecksum => {
                    "invalid WAL header checksum"
                }
                Corruption::FrameData | Corruption::FrameChecksum => "invalid WAL frame checksum",
                Corruption::Salt => "WAL frame salt does not match its header",
                Corruption::PageNumber | Corruption::OutOfRangePageNumber => {
                    "invalid WAL page number"
                }
                Corruption::CommitMarker => "WAL range has no final commit marker",
                Corruption::Alignment => "invalid WAL frame alignment",
            };
            expect_invalid_wal(wal::validate_complete(&header, &frames), expected)?;
        }

        conn.execute("INSERT INTO values_table VALUES (zeroblob(32768))", [])?;
        let transaction = committed(&conn, &capture.position)?;
        assert_eq!(
            wal::validate_record(&valid_header, &transaction.bytes, capture.position)?,
            transaction.position
        );
        if transaction.bytes.len() < frame_size * 2 {
            return Err("test transaction has fewer than two frames".into());
        }
        let mut early_commit = transaction.bytes.to_vec();
        early_commit[4..8].copy_from_slice(&1_u32.to_be_bytes());
        expect_invalid_wal(
            wal::validate_record(&valid_header, &early_commit, capture.position),
            "WAL record has an early commit marker",
        )?;
        Ok(())
    }

    fn expect_invalid_wal(
        result: Result<wal::WalPosition, SqliteError>,
        expected: &str,
    ) -> TestResult {
        match result {
            Err(SqliteError::InvalidWal(message)) if message == expected => Ok(()),
            other => Err(format!("expected {expected:?}, got {other:?}").into()),
        }
    }

    fn committed(
        connection: &rusqlite::Connection,
        prior: &wal::WalPosition,
    ) -> Result<wal::WalCapture, SqliteError> {
        wal::committed(connection, prior, usize::MAX)
    }

    fn wal_path(database: &Path) -> PathBuf {
        let mut path: OsString = database.as_os_str().to_owned();
        path.push("-wal");
        PathBuf::from(path)
    }
}
