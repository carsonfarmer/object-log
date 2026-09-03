use std::error::Error as StdError;
use std::ffi::OsString;
use std::fs::{self, OpenOptions, TryLockError};
use std::path::{Path as FsPath, PathBuf};
use std::process::Command;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{CommitStatus, Log, LogId, ObjectKind, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, SqliteError, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;
use rusqlite::ErrorCode;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn snapshot_then_wal_recovers_without_the_local_cache() -> TestResult {
    let log = open_log("sqlite-recovery").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;

    let StageStatus::Staged(first) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE values_table (value TEXT NOT NULL);
                 INSERT INTO values_table VALUES ('first');",
            )?;
            Ok(Bytes::from_static(b"created"))
        })
        .await?
    else {
        return Err("the first write was not staged".into());
    };
    assert_eq!(first.result(), &Bytes::from_static(b"created"));
    assert!(!first.recovery_token().is_empty());
    assert!(matches!(first.publish().await?, CommitStatus::Committed(_)));

    let StageStatus::Staged(second) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute("INSERT INTO values_table VALUES ('second')", [])?;
            Ok(Bytes::from_static(b"inserted"))
        })
        .await?
    else {
        return Err("the second write was not staged".into());
    };
    assert!(matches!(
        second.publish().await?,
        CommitStatus::Committed(_)
    ));
    drop(database);

    let mut recovered =
        Database::open(log.clone(), directory.path().join("recovered.sqlite3")).await?;
    let values = recovered
        .read(|connection| {
            let mut statement =
                connection.prepare("SELECT value FROM values_table ORDER BY rowid")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .await?;
    assert_eq!(values, ["first", "second"]);
    assert_eq!(log.load().await?.tail().len(), 2);
    Ok(())
}

#[tokio::test]
async fn read_only_transactions_do_not_publish() -> TestResult {
    let log = open_log("sqlite-read-only").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("cache.sqlite3")).await?;
    let generation = log.load().await?.cursor().generation();

    let StageStatus::ReadOnly(result) = database
        .stage_write(TransactionId::new(), |transaction| {
            let value = transaction.query_row("SELECT 42", [], |row| row.get::<_, i64>(0))?;
            Ok(Bytes::copy_from_slice(&value.to_be_bytes()))
        })
        .await?
    else {
        return Err("a read-only transaction produced WAL data".into());
    };
    assert_eq!(result.as_ref(), 42_i64.to_be_bytes());
    assert_eq!(log.load().await?.cursor().generation(), generation);
    Ok(())
}

#[tokio::test]
async fn callback_error_rolls_back_and_keeps_the_cache_usable() -> TestResult {
    let log = open_log("sqlite-callback-rollback").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;

    let error = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute("INSERT INTO values_table VALUES (2)", [])?;
            Err(rusqlite::Error::InvalidQuery)
        })
        .await;
    assert!(matches!(error, Err(SqliteError::Sqlite(_))));
    assert_eq!(
        database
            .read(|connection| {
                connection.query_row("SELECT sum(value) FROM values_table", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_publication_rebuilds_before_the_next_callback() -> TestResult {
    let (store, log) = open_fault_log("sqlite-cancelled-publication").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the write was not staged".into());
    };

    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let mut publication = Box::pin(staged.publish());
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        result = &mut publication => return Err(format!("publication finished early: {result:?}").into()),
    };
    assert!(entered);
    drop(publication);
    assert!(!pause.release());

    assert_eq!(
        database
            .read(|connection| connection.query_row::<i64, _, _>(
                "SELECT count(*) FROM sqlite_schema WHERE name = 'values_table'",
                [],
                |row| row.get(0),
            ))
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn dropping_a_stage_forces_a_rebuild_before_later_work() -> TestResult {
    let log = open_log("sqlite-drop-stage").await?;
    let directory = tempfile::tempdir()?;
    let first_path = directory.path().join("first.sqlite3");
    let second_path = directory.path().join("second.sqlite3");
    let mut first = Database::open(log.clone(), first_path).await?;
    commit_sql(
        &mut first,
        "CREATE TABLE values_table (value TEXT); INSERT INTO values_table VALUES ('base');",
    )
    .await?;

    let StageStatus::Staged(abandoned) = first
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute("INSERT INTO values_table VALUES ('abandoned')", [])?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the write was not staged".into());
    };
    drop(abandoned);

    let mut second = Database::open(log, second_path).await?;
    commit_sql(
        &mut second,
        "INSERT INTO values_table VALUES ('published');",
    )
    .await?;
    let values = first
        .read(|connection| {
            let mut statement =
                connection.prepare("SELECT value FROM values_table ORDER BY rowid")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .await?;
    assert_eq!(values, ["base", "published"]);
    Ok(())
}

#[tokio::test]
async fn checkpoint_replaces_the_tail_and_later_wal_recovers() -> TestResult {
    let log = open_log("sqlite-checkpoint").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;

    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER NOT NULL); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    commit_sql(&mut database, "INSERT INTO values_table VALUES (2);").await?;
    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ));
    let checkpointed = log.load().await?;
    assert!(checkpointed.checkpoint().is_some());
    assert!(checkpointed.tail().is_empty());

    commit_sql(&mut database, "INSERT INTO values_table VALUES (3);").await?;
    drop(database);
    let mut recovered =
        Database::open(log.clone(), directory.path().join("recovered.sqlite3")).await?;
    assert_eq!(
        recovered
            .read(|connection| connection.query_row(
                "SELECT sum(value) FROM values_table",
                [],
                |row| row.get::<_, i64>(0)
            ))
            .await?,
        6
    );
    let current = log.load().await?;
    assert!(current.checkpoint().is_some());
    assert_eq!(current.tail().len(), 1);
    Ok(())
}

#[tokio::test]
async fn conflict_does_not_rerun_the_sqlite_callback() -> TestResult {
    let log = open_log("sqlite-conflict").await?;
    let directory = tempfile::tempdir()?;
    let mut first = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;
    let mut second = Database::open(log, directory.path().join("second.sqlite3")).await?;
    let mut calls = 0;

    let StageStatus::Staged(first_write) = first
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE values_table (value TEXT NOT NULL);
                 INSERT INTO values_table VALUES ('winner');",
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the first write was not staged".into());
    };
    let StageStatus::Staged(second_write) = second
        .stage_write(TransactionId::new(), |transaction| {
            calls += 1;
            transaction.execute_batch(
                "CREATE TABLE values_table (value TEXT NOT NULL);
                 INSERT INTO values_table VALUES ('loser');",
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the second write was not staged".into());
    };
    assert!(matches!(
        first_write.publish().await?,
        CommitStatus::Committed(_)
    ));
    assert!(matches!(
        second_write.publish().await?,
        CommitStatus::Conflict(_)
    ));
    assert_eq!(calls, 1);
    assert_eq!(
        second
            .read(|connection| connection
                .query_row("SELECT value FROM values_table", [], |row| row
                    .get::<_, String>(0)))
            .await?,
        "winner"
    );
    assert_eq!(calls, 1);
    Ok(())
}

#[tokio::test]
async fn cached_write_cannot_bypass_the_read_policy() -> TestResult {
    let log = open_log("sqlite-cached-policy").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;

    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction
                .prepare_cached("UPDATE values_table SET value = value + 1")?
                .execute([])?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the cached write was not staged".into());
    };
    assert!(matches!(
        staged.publish().await?,
        CommitStatus::Committed(_)
    ));

    let attempted = database
        .read(|connection| {
            connection
                .prepare_cached("UPDATE values_table SET value = value + 1")?
                .execute([])
        })
        .await;
    assert!(matches!(
        attempted,
        Err(SqliteError::Sqlite(rusqlite::Error::SqliteFailure(error, _)))
            if error.code == ErrorCode::AuthorizationForStatementDenied
    ));
    assert_eq!(
        database
            .read(|connection| {
                connection.query_row("SELECT value FROM values_table", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn cache_lock_rejects_same_path_and_cleans_stale_journal() -> TestResult {
    let log = open_log("sqlite-same-path").await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.sqlite3");
    fs::write(sidecar(&path, "-journal"), b"stale")?;
    let database = Database::open(log.clone(), &path).await?;
    assert!(!sidecar(&path, "-journal").exists());
    assert!(Database::open(log.clone(), &path).await.is_err());

    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "advisory_lock_probe", "--nocapture"])
        .env("OBJECT_LOG_SQLITE_LOCK_PROBE", sidecar(&path, "-lock"))
        .status()?;
    assert!(status.success());

    drop(database);
    assert!(sidecar(&path, "-lock").exists());
    let reopened = Database::open(log, path).await?;
    drop(reopened);
    Ok(())
}

#[tokio::test]
async fn large_snapshot_and_wal_use_multiple_chunks() -> TestResult {
    let options = Options {
        max_inline_operation_bytes: 1_024,
        max_object_bytes: 8_240,
        ..Options::default()
    };
    let log = open_log_with("sqlite-chunks", options).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;

    commit_sql(
        &mut database,
        "CREATE TABLE blobs (value BLOB NOT NULL); INSERT INTO blobs VALUES (zeroblob(200000));",
    )
    .await?;
    commit_sql(&mut database, "UPDATE blobs SET value = zeroblob(300000);").await?;
    let records = log.read_tail(&log.load().await?).await?;
    assert_eq!(records.len(), 2);
    assert!(records[0].objects().len() > 1);
    assert!(records[1].objects().len() > 1);

    drop(database);
    let mut recovered = Database::open(log, directory.path().join("recovered.sqlite3")).await?;
    assert_eq!(
        recovered
            .read(|connection| {
                connection.query_row("SELECT length(value) FROM blobs", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .await?,
        300_000
    );
    Ok(())
}

#[tokio::test]
async fn lost_commit_success_resumes_without_callback_replay() -> TestResult {
    let (store, log) = open_fault_log("sqlite-lost-commit").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("cache.sqlite3")).await?;
    let mut calls = 0;
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            calls += 1;
            transaction.execute_batch("CREATE TABLE values_table (value INTEGER)")?;
            Ok(Bytes::from_static(b"done"))
        })
        .await?
    else {
        return Err("the write was not staged".into());
    };
    let token = staged.recovery_token().clone();
    store.reset();
    store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(staged.publish().await?, CommitStatus::Pending(_)));
    let mut later = Database::open(log, directory.path().join("later.sqlite3")).await?;
    commit_sql(&mut later, "INSERT INTO values_table VALUES (7);").await?;
    assert!(matches!(
        database.resume(&token).await?,
        object_log::Resolution::Committed(_)
    ));
    assert_eq!(calls, 1);
    assert_eq!(
        database
            .read(|connection| connection.query_row(
                "SELECT sum(value) FROM values_table",
                [],
                |row| row.get::<_, i64>(0)
            ))
            .await?,
        7
    );
    Ok(())
}

#[tokio::test]
async fn repeated_checkpoint_resolves_a_lost_success() -> TestResult {
    let (store, log) = open_fault_log("sqlite-lost-checkpoint").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("cache.sqlite3")).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;

    store.reset();
    store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Pending
    ));
    let mut later = Database::open(log.clone(), directory.path().join("later.sqlite3")).await?;
    commit_sql(&mut later, "INSERT INTO values_table VALUES (2);").await?;
    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_some());
    assert_eq!(current.tail().len(), 1);
    assert_eq!(
        database
            .read(|connection| connection.query_row(
                "SELECT sum(value) FROM values_table",
                [],
                |row| row.get::<_, i64>(0)
            ))
            .await?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn open_rejects_options_that_cannot_hold_one_wal_frame() -> TestResult {
    let options = Options {
        max_object_bytes: 4_119,
        ..Options::default()
    };
    let log = open_log_with("sqlite-small-object", options).await?;
    let directory = tempfile::tempdir()?;
    assert!(matches!(
        Database::open(log, directory.path().join("cache.sqlite3")).await,
        Err(SqliteError::PayloadLimit)
    ));

    let options = Options {
        max_inline_operation_bytes: 1,
        ..Options::default()
    };
    let log = open_log_with("sqlite-small-descriptor", options).await?;
    assert!(matches!(
        Database::open(log, directory.path().join("other.sqlite3")).await,
        Err(SqliteError::PayloadLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn declared_payload_length_is_checked_before_allocation() -> TestResult {
    let log = open_log("sqlite-malicious-length").await?;
    let object = log.put_object(Bytes::from(vec![0; 4_096])).await?;
    assert_eq!(object.kind(), ObjectKind::Blob);
    let view = log.load().await?;
    let operation = malicious_snapshot_record(u64::MAX - 4_095)?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        operation,
        Bytes::new(),
        vec![object],
    )?;
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Committed(_)
    ));

    let directory = tempfile::tempdir()?;
    match Database::open(log, directory.path().join("cache.sqlite3")).await {
        Err(SqliteError::InvalidRecord(message)) => {
            assert_eq!(message, "record chunks do not match the declared length");
            Ok(())
        }
        Err(SqliteError::Numeric(_)) if usize::BITS < 64 => Ok(()),
        _ => Err("malicious payload length was not rejected before allocation".into()),
    }
}

#[test]
fn advisory_lock_probe() -> TestResult {
    let Some(path) = std::env::var_os("OBJECT_LOG_SQLITE_LOCK_PROBE") else {
        return Ok(());
    };
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    match file.try_lock() {
        Err(TryLockError::WouldBlock) => Ok(()),
        Err(TryLockError::Error(error)) => Err(error.into()),
        Ok(()) => Err("another process acquired the live cache lock".into()),
    }
}

async fn open_log(id: &str) -> Result<Log, Box<dyn StdError>> {
    open_log_with(id, Options::default()).await
}

async fn open_log_with(id: &str, options: Options) -> Result<Log, Box<dyn StdError>> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("sqlite-tests")).await?;
    Ok(Log::open(backend.scope(&LogId::new(id)?), options).await?)
}

async fn open_fault_log(id: &str) -> Result<(FaultStore, Log), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("sqlite-fault-tests")).await?;
    let log = Log::open(backend.scope(&LogId::new(id)?), Options::default()).await?;
    Ok((store, log))
}

async fn commit_sql(database: &mut Database, sql: &str) -> TestResult {
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(sql)?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("SQL did not produce a staged write".into());
    };
    if !matches!(staged.publish().await?, CommitStatus::Committed(_)) {
        return Err("SQL write did not commit".into());
    }
    Ok(())
}

fn malicious_snapshot_record(payload_len: u64) -> Result<Bytes, Box<dyn StdError>> {
    let mut encoder = minicbor::Encoder::new(Vec::new());
    encoder
        .map(5)?
        .u8(0)?
        .u32(1)?
        .u8(1)?
        .u8(0)?
        .u8(2)?
        .u32(4_096)?
        .u8(3)?
        .u64(payload_len)?
        .u8(5)?
        .u8(1)?;
    Ok(Bytes::from(encoder.into_writer()))
}

fn sidecar(path: &FsPath, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}
