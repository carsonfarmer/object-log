use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::{CommitStatus, Error, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, SqliteError, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;

const FRAME_BYTES: usize = 4_120;
const TINY_PAYLOAD_BYTES: usize = 8 * FRAME_BYTES;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn deterministic_commit_rejections_do_not_run_the_callback() -> TestResult {
    let log = open_log(
        "sqlite-full-tail",
        Options {
            max_tail_entries: 1,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("tail.sqlite3")).await?;
    commit_sql(
        &mut database,
        TransactionId::new(),
        "CREATE TABLE values_table (value INTEGER)",
    )
    .await?;

    let mut calls = 0;
    let result = database
        .stage_write(TransactionId::new(), |transaction| {
            calls += 1;
            transaction.execute("INSERT INTO values_table VALUES (1)", [])?;
            Ok(Bytes::new())
        })
        .await;
    assert!(matches!(
        result,
        Err(SqliteError::Log(Error::LimitExceeded(
            "active tail entries"
        )))
    ));
    assert_eq!(calls, 0);

    let log = open_log("sqlite-duplicate-id", Options::default()).await?;
    let mut database = Database::open(log, directory.path().join("duplicate.sqlite3")).await?;
    let transaction_id = TransactionId::new();
    commit_sql(
        &mut database,
        transaction_id,
        "CREATE TABLE values_table (value INTEGER)",
    )
    .await?;
    for checkpoint in [false, true] {
        if checkpoint {
            assert!(matches!(
                database.checkpoint().await?,
                SqliteCheckpointStatus::Published(_)
            ));
        }
        let mut calls = 0;
        let result = database
            .stage_write(transaction_id, |transaction| {
                calls += 1;
                transaction.execute("INSERT INTO values_table VALUES (1)", [])?;
                Ok(Bytes::new())
            })
            .await;
        assert!(matches!(
            result,
            Err(SqliteError::Log(Error::InvalidFormat(message)))
                if message == "the transaction ID is already committed"
        ));
        assert_eq!(calls, 0);
    }
    Ok(())
}

#[tokio::test]
async fn oversized_wal_is_rejected_before_capture_allocation() -> TestResult {
    let log = open_log("sqlite-wal-bound", tiny_options()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("wal.sqlite3")).await?;
    commit_sql(
        &mut database,
        TransactionId::new(),
        "CREATE TABLE blobs (value BLOB NOT NULL)",
    )
    .await?;

    assert!(matches!(
        database
            .stage_write(TransactionId::new(), |transaction| {
                transaction.execute("INSERT INTO blobs VALUES (zeroblob(100000))", [])?;
                Ok(Bytes::new())
            })
            .await,
        Err(SqliteError::PayloadLimit)
    ));
    assert_eq!(
        database
            .read(|connection| {
                connection.query_row("SELECT count(*) FROM blobs", [], |row| row.get::<_, i64>(0))
            })
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn first_snapshot_uses_snapshot_capacity_not_wal_capacity() -> TestResult {
    let log = open_log(
        "sqlite-asymmetric-bound",
        Options {
            max_inline_operation_bytes: 128,
            max_object_refs: 1,
            max_object_bytes: 8_192,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("asymmetric.sqlite3")).await?;
    commit_sql(
        &mut database,
        TransactionId::new(),
        "CREATE TABLE values_table (value INTEGER)",
    )
    .await?;
    assert_eq!(
        database
            .read(|connection| connection.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name = 'values_table'",
                [],
                |row| row.get::<_, i64>(0),
            ))
            .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn oversized_snapshot_is_rejected_before_loading_into_memory() -> TestResult {
    let log = open_log("sqlite-snapshot-bound", tiny_options()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("snapshot.sqlite3")).await?;
    commit_sql(
        &mut database,
        TransactionId::new(),
        "CREATE TABLE blobs (value BLOB NOT NULL)",
    )
    .await?;

    for _ in 0..100 {
        commit_sql(
            &mut database,
            TransactionId::new(),
            "INSERT INTO blobs VALUES (zeroblob(1024))",
        )
        .await?;
    }
    assert!(matches!(
        database.checkpoint().await,
        Err(SqliteError::PayloadLimit)
    ));
    Ok(())
}

fn tiny_options() -> Options {
    Options {
        max_inline_operation_bytes: 128,
        max_object_refs: 1,
        max_object_bytes: TINY_PAYLOAD_BYTES,
        ..Options::default()
    }
}

async fn open_log(id: &str, options: Options) -> Result<Log, Box<dyn StdError>> {
    let backend =
        ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("sqlite-bound-tests")).await?;
    Ok(Log::open(backend.scope(&LogId::new(id)?), options).await?)
}

async fn commit_sql(
    database: &mut Database,
    transaction_id: TransactionId,
    sql: &str,
) -> TestResult {
    let StageStatus::Staged(staged) = database
        .stage_write(transaction_id, |transaction| {
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
