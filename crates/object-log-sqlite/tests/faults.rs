use std::error::Error as StdError;
use std::ffi::OsString;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{CommitStatus, Log, LogId, Options, Resolution, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn cancelled_external_staging_discards_the_unpublished_local_write() -> TestResult {
    let options = Options {
        max_inline_operation_bytes: 1_024,
        max_object_bytes: 8_240,
        ..Options::default()
    };
    let (store, log) = open_fault_log("cancel-external-stage", options).await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.sqlite3");
    let mut database = Database::open(log.clone(), &path).await?;
    let mut calls = 0;

    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let mut staging = Box::pin(database.stage_write(TransactionId::new(), |transaction| {
        calls += 1;
        transaction.execute_batch(
            "CREATE TABLE blobs (value BLOB NOT NULL);
             INSERT INTO blobs VALUES (zeroblob(200000));",
        )?;
        Ok(Bytes::new())
    }));
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut staging => return Err("staging finished before the pause".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("staging did not reach an external put".into()),
    };
    assert!(entered);
    assert!(fs::metadata(wal_path(&path))?.len() > 0);
    drop(staging);
    assert!(!pause.release());
    assert_eq!(calls, 1);
    assert!(log.load().await?.tail().is_empty());
    assert_eq!(table_count(&mut database, "blobs").await?, 0);
    Ok(())
}

#[tokio::test]
async fn resume_not_committed_rebuilds_to_the_winning_write() -> TestResult {
    let (_, log) = open_fault_log("resume-not-committed", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut loser = Database::open(log.clone(), directory.path().join("loser.sqlite3")).await?;
    let mut winner = Database::open(log, directory.path().join("winner.sqlite3")).await?;
    let token = abandon_sql(
        &mut loser,
        "CREATE TABLE values_table (value TEXT); INSERT INTO values_table VALUES ('loser');",
    )
    .await?;

    commit_sql(
        &mut winner,
        "CREATE TABLE values_table (value TEXT); INSERT INTO values_table VALUES ('winner');",
    )
    .await?;
    assert!(matches!(
        loser.resume(&token).await?,
        Resolution::NotCommitted(_)
    ));
    assert_eq!(value(&mut loser).await?, "winner");
    Ok(())
}

#[tokio::test]
async fn resume_read_failure_stays_pending_then_resolves() -> TestResult {
    let (store, log) = open_fault_log("resume-still-pending", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
    let token = abandon_sql(
        &mut database,
        "CREATE TABLE values_table (value TEXT); INSERT INTO values_table VALUES ('candidate');",
    )
    .await?;

    store.fail_next(Operation::Get, FailurePhase::Before);
    assert!(matches!(
        database.resume(&token).await?,
        Resolution::StillPending(_)
    ));
    assert_eq!(table_count(&mut database, "values_table").await?, 0);
    assert!(matches!(
        database.resume(&token).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(value(&mut database).await?, "candidate");
    Ok(())
}

#[tokio::test]
async fn resume_expires_after_its_outcome_leaves_the_window() -> TestResult {
    let options = Options {
        resolution_window: 1,
        ..Options::default()
    };
    let (_, log) = open_fault_log("resume-expired", options).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;
    let mut winner = Database::open(log, directory.path().join("winner.sqlite3")).await?;
    let token = abandon_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (99);",
    )
    .await?;

    commit_sql(
        &mut winner,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    expect_checkpoint(&mut winner, ExpectedCheckpoint::Published).await?;
    commit_sql(&mut winner, "INSERT INTO values_table VALUES (2);").await?;
    expect_checkpoint(&mut winner, ExpectedCheckpoint::Published).await?;

    assert!(matches!(
        database.resume(&token).await?,
        Resolution::Expired(_)
    ));
    assert_eq!(sum(&mut database).await?, 3);
    Ok(())
}

#[tokio::test]
async fn direct_checkpoint_conflict_rebuilds_to_the_winner() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-conflict", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut first = Database::open(log.clone(), directory.path().join("first.sqlite3")).await?;
    commit_sql(
        &mut first,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    let mut second = Database::open(log, directory.path().join("second.sqlite3")).await?;

    store.reset();
    let mut pause = store.pause_put_at(2, FailurePhase::Before);
    let mut first_attempt = Box::pin(first.checkpoint());
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut first_attempt => return Err("checkpoint finished before the pause".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("checkpoint did not reach its head update".into()),
    };
    assert!(entered);
    expect_checkpoint(&mut second, ExpectedCheckpoint::Published).await?;
    assert!(pause.release());
    assert!(matches!(
        first_attempt.await?,
        SqliteCheckpointStatus::Conflict(_)
    ));
    assert_eq!(sum(&mut first).await?, 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_checkpoint_before_cas_keeps_the_local_wal() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-cancel-before", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.sqlite3");
    let mut database = Database::open(log.clone(), &path).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    commit_sql(&mut database, "INSERT INTO values_table VALUES (2);").await?;

    store.reset();
    let mut pause = store.pause_put_at(2, FailurePhase::Before);
    let mut attempt = Box::pin(database.checkpoint());
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut attempt => return Err("checkpoint finished before the pause".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("checkpoint did not reach its head update".into()),
    };
    assert!(entered);
    assert!(fs::metadata(wal_path(&path))?.len() > 0);
    assert_eq!(log.load().await?.tail().len(), 2);
    drop(attempt);
    assert!(!pause.release());

    assert_eq!(sum(&mut database).await?, 3);
    assert_eq!(log.load().await?.tail().len(), 2);
    Ok(())
}

#[tokio::test]
async fn cancelled_checkpoint_after_cas_waits_to_truncate_the_local_wal() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-cancel-after", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.sqlite3");
    let mut database = Database::open(log.clone(), &path).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    commit_sql(&mut database, "INSERT INTO values_table VALUES (2);").await?;

    store.reset();
    let mut pause = store.pause_put_at(2, FailurePhase::After);
    let mut attempt = Box::pin(database.checkpoint());
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut attempt => return Err("checkpoint finished before the pause".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("checkpoint did not reach its visible head update".into()),
    };
    assert!(entered);
    let visible = log.load().await?;
    assert!(visible.checkpoint().is_some());
    assert!(visible.tail().is_empty());
    assert!(fs::metadata(wal_path(&path))?.len() > 0);
    drop(attempt);
    assert!(!pause.release());

    expect_checkpoint(&mut database, ExpectedCheckpoint::Published).await?;
    assert_eq!(fs::metadata(wal_path(&path))?.len(), 0);
    assert_eq!(sum(&mut database).await?, 3);
    Ok(())
}

#[tokio::test]
async fn pending_checkpoint_resolves_not_published() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-not-published", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut first = seeded_database(&log, directory.path().join("first.sqlite3")).await?;
    let mut second = Database::open(log, directory.path().join("second.sqlite3")).await?;

    leave_pending_checkpoint(&store, &mut first).await?;
    expect_checkpoint(&mut second, ExpectedCheckpoint::Published).await?;
    expect_checkpoint(&mut first, ExpectedCheckpoint::Conflict).await?;
    assert_eq!(sum(&mut first).await?, 1);
    Ok(())
}

#[tokio::test]
async fn pending_checkpoint_read_failure_stays_pending_then_resolves() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-still-pending", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = seeded_database(&log, directory.path().join("cache.sqlite3")).await?;

    leave_pending_checkpoint(&store, &mut database).await?;
    store.fail_next(Operation::Get, FailurePhase::Before);
    expect_checkpoint(&mut database, ExpectedCheckpoint::Pending).await?;
    expect_checkpoint(&mut database, ExpectedCheckpoint::Published).await?;
    assert_eq!(sum(&mut database).await?, 1);
    Ok(())
}

#[tokio::test]
async fn pending_checkpoint_expires_after_two_later_head_updates() -> TestResult {
    let (store, log) = open_fault_log("checkpoint-expired", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut first = seeded_database(&log, directory.path().join("first.sqlite3")).await?;
    let mut second = Database::open(log, directory.path().join("second.sqlite3")).await?;

    leave_pending_checkpoint(&store, &mut first).await?;
    commit_sql(&mut second, "INSERT INTO values_table VALUES (2);").await?;
    commit_sql(&mut second, "INSERT INTO values_table VALUES (3);").await?;
    expect_checkpoint(&mut first, ExpectedCheckpoint::Expired).await?;
    assert_eq!(sum(&mut first).await?, 6);
    Ok(())
}

#[tokio::test]
async fn refresh_failure_happens_before_the_write_callback() -> TestResult {
    let (store, log) = open_fault_log("refresh-before-callback", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
    let mut calls = 0;

    store.fail_next(Operation::Get, FailurePhase::Before);
    let result = database
        .stage_write(TransactionId::new(), |transaction| {
            calls += 1;
            transaction.execute_batch("CREATE TABLE unreachable (value INTEGER)")?;
            Ok(Bytes::new())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(calls, 0);
    assert_eq!(table_count(&mut database, "unreachable").await?, 0);
    Ok(())
}

#[derive(Clone, Copy)]
enum ExpectedCheckpoint {
    Published,
    Conflict,
    Pending,
    Expired,
}

async fn open_fault_log(
    id: &str,
    options: Options,
) -> Result<(FaultStore, Log), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("sqlite-fault-cases")).await?;
    let log = Log::open(backend.scope(&LogId::new(id)?), options).await?;
    Ok((store, log))
}

async fn seeded_database(log: &Log, path: PathBuf) -> Result<Database, Box<dyn StdError>> {
    let mut database = Database::open(log.clone(), path).await?;
    commit_sql(
        &mut database,
        "CREATE TABLE values_table (value INTEGER); INSERT INTO values_table VALUES (1);",
    )
    .await?;
    Ok(database)
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

async fn abandon_sql(database: &mut Database, sql: &str) -> Result<Bytes, Box<dyn StdError>> {
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(sql)?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("SQL did not produce a staged write".into());
    };
    Ok(staged.recovery_token().clone())
}

async fn leave_pending_checkpoint(store: &FaultStore, database: &mut Database) -> TestResult {
    store.reset();
    store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    expect_checkpoint(database, ExpectedCheckpoint::Pending).await
}

async fn expect_checkpoint(database: &mut Database, expected: ExpectedCheckpoint) -> TestResult {
    let status = database.checkpoint().await?;
    let matches = matches!(
        (expected, status),
        (
            ExpectedCheckpoint::Published,
            SqliteCheckpointStatus::Published(_)
        ) | (
            ExpectedCheckpoint::Conflict,
            SqliteCheckpointStatus::Conflict(_)
        ) | (ExpectedCheckpoint::Pending, SqliteCheckpointStatus::Pending)
            | (
                ExpectedCheckpoint::Expired,
                SqliteCheckpointStatus::Expired(_)
            )
    );
    if !matches {
        return Err("checkpoint returned an unexpected status".into());
    }
    Ok(())
}

async fn table_count(database: &mut Database, table: &str) -> Result<i64, Box<dyn StdError>> {
    Ok(database
        .read(|connection| {
            connection.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
        })
        .await?)
}

async fn value(database: &mut Database) -> Result<String, Box<dyn StdError>> {
    Ok(database
        .read(|connection| {
            connection.query_row("SELECT value FROM values_table", [], |row| row.get(0))
        })
        .await?)
}

async fn sum(database: &mut Database) -> Result<i64, Box<dyn StdError>> {
    Ok(database
        .read(|connection| {
            connection.query_row("SELECT sum(value) FROM values_table", [], |row| row.get(0))
        })
        .await?)
}

fn wal_path(path: &FsPath) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push("-wal");
    PathBuf::from(value)
}
