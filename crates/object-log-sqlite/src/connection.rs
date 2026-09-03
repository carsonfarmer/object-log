use std::path::Path;

use rusqlite::{Connection, OpenFlags, config::DbConfig};

use crate::{PAGE_SIZE, SqliteError};

const MIN_SQLITE_VERSION: i32 = 3_053_000;

pub(crate) fn open(path: &Path) -> Result<Connection, SqliteError> {
    let version = rusqlite::version_number();
    if version < MIN_SQLITE_VERSION {
        return Err(SqliteError::Configuration(format!(
            "SQLite {version} is older than required version {MIN_SQLITE_VERSION}"
        )));
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.pragma_update(None, "page_size", PAGE_SIZE)?;
    require_i64(&conn, "page_size", i64::from(PAGE_SIZE))?;
    let locking: String =
        conn.pragma_update_and_check(None, "locking_mode", "exclusive", |row| row.get(0))?;
    if !locking.eq_ignore_ascii_case("exclusive") {
        return Err(SqliteError::Configuration(
            "SQLite did not enter exclusive locking mode".into(),
        ));
    }
    let journal: String =
        conn.pragma_update_and_check(None, "journal_mode", "wal", |row| row.get(0))?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err(SqliteError::Configuration(
            "SQLite did not enter WAL mode".into(),
        ));
    }
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    require_i64(&conn, "wal_autocheckpoint", 0)?;
    conn.pragma_update(None, "synchronous", "normal")?;
    require_i64(&conn, "synchronous", 1)?;
    require_config(&conn, DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    require_config(&conn, DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    require_config(&conn, DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)?;
    Ok(conn)
}

fn require_i64(conn: &Connection, name: &str, expected: i64) -> Result<(), SqliteError> {
    let actual: i64 = conn.pragma_query_value(None, name, |row| row.get(0))?;
    if actual != expected {
        return Err(SqliteError::Configuration(format!(
            "SQLite {name} is {actual}, expected {expected}"
        )));
    }
    Ok(())
}

fn require_config(conn: &Connection, config: DbConfig, value: bool) -> Result<(), SqliteError> {
    if conn.set_db_config(config, value)? != value {
        return Err(SqliteError::Configuration(format!(
            "SQLite did not retain {config:?}={value}"
        )));
    }
    Ok(())
}
