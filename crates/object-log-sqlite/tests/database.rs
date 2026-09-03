use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;

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
    assert!(matches!(
        database.publish(first).await?,
        CommitStatus::Committed(_)
    ));

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
        database.publish(second).await?,
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
        first.publish(first_write).await?,
        CommitStatus::Committed(_)
    ));
    assert!(matches!(
        second.publish(second_write).await?,
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
async fn callback_policy_allows_main_savepoints_and_rejects_other_mutation() -> TestResult {
    let log = open_log("sqlite-policy").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;

    assert!(
        database
            .read(|connection| connection.execute_batch("CREATE TABLE denied (value INTEGER)"))
            .await
            .is_err()
    );
    assert!(
        database
            .stage_write(TransactionId::new(), |transaction| {
                transaction.execute_batch("CREATE TEMP TABLE denied (value INTEGER)")?;
                Ok(Bytes::new())
            })
            .await
            .is_err()
    );

    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE allowed (value INTEGER);
                 SAVEPOINT nested;
                 INSERT INTO allowed VALUES (1);
                 ROLLBACK TO nested;
                 RELEASE nested;
                 INSERT INTO allowed VALUES (2);",
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the allowed write was not staged".into());
    };
    assert!(matches!(
        database.publish(staged).await?,
        CommitStatus::Committed(_)
    ));
    assert_eq!(
        database
            .read(|connection| connection
                .query_row("SELECT value FROM allowed", [], |row| row.get::<_, i64>(0)))
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn one_process_rejects_a_second_owner_of_the_same_cache_path() -> TestResult {
    let log = open_log("sqlite-same-path").await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.sqlite3");
    let database = Database::open(log.clone(), &path).await?;
    assert!(Database::open(log.clone(), &path).await.is_err());
    drop(database);
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
    let mut database = Database::open(log, directory.path().join("cache.sqlite3")).await?;
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
    assert!(matches!(
        database.publish(staged).await?,
        CommitStatus::Pending(_)
    ));
    assert!(matches!(
        database.resume(&token).await?,
        object_log::Resolution::Committed(_)
    ));
    assert_eq!(calls, 1);
    database
        .read(|connection| {
            connection.query_row("SELECT count(*) FROM values_table", [], |row| {
                row.get::<_, i64>(0)
            })
        })
        .await?;
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
    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_some());
    assert!(current.tail().is_empty());
    Ok(())
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
    if !matches!(database.publish(staged).await?, CommitStatus::Committed(_)) {
        return Err("SQL write did not commit".into());
    }
    Ok(())
}
