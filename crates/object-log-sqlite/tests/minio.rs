#![cfg(feature = "aws")]

use std::env;
use std::error::Error as StdError;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options, Resolution,
    TransactionId, ValidatedBackend,
};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

#[tokio::test]
#[ignore = "requires OBJECT_LOG_MINIO_* and the pinned local MinIO from scripts/test-minio.sh"]
async fn minio_sqlite_recovers_before_and_after_collection() -> TestResult {
    let faults = FaultStore::new(build_minio()?);
    let root = Path::from("object-log-sqlite-local-tests");
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), root).await?;
    let log_id = LogId::new(format!("sqlite-minio-{}", Uuid::new_v4().simple()))?;
    let options = Options {
        max_inline_operation_bytes: 1_024,
        max_object_bytes: 8_240,
        ..Options::default()
    };
    let log = Log::open(backend.scope(&log_id), options).await?;
    let directory = tempfile::tempdir()?;
    let first = directory.path().join("first/cache.sqlite3");
    let second = directory.path().join("second/cache.sqlite3");
    let third = directory.path().join("third/cache.sqlite3");
    let mut database = Database::open(log.clone(), &first).await?;

    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE state (
                    id INTEGER PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    marker TEXT NOT NULL,
                    payload BLOB NOT NULL
                );
                INSERT INTO state VALUES (1, 1, 'state-1', randomblob(65536));",
            )?;
            Ok(Bytes::from_static(b"created"))
        })
        .await?
    else {
        return Err("initial SQLite write did not stage".into());
    };
    let token = staged.recovery_token().clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(staged.publish().await?, CommitStatus::Pending(_)));
    let Resolution::Committed(first_view) = database.resume(&token).await? else {
        return Err("lost SQLite publication did not resolve as committed".into());
    };
    let first_tail = log.read_tail(&first_view).await?;
    assert_eq!(first_tail.len(), 1);
    assert!(first_tail[0].objects().len() > 1);

    write_generation(&mut database, 2, 98_304).await?;
    let wal_view = log.load().await?;
    assert!(
        log.read_tail(&wal_view)
            .await?
            .last()
            .is_some_and(|commit| commit.objects().len() > 1)
    );

    let SqliteCheckpointStatus::Published(checkpoint_view) = database.checkpoint().await? else {
        return Err("SQLite checkpoint did not publish".into());
    };
    assert!(
        log.read_checkpoint(&checkpoint_view)
            .await?
            .ok_or("SQLite checkpoint is missing")?
            .objects()
            .len()
            > 1
    );
    write_generation(&mut database, 3, 131_072).await?;
    let current = log.load().await?;
    assert_eq!(current.tail().len(), 1);
    assert!(log.read_tail(&current).await?[0].objects().len() > 1);
    assert_state(&mut database, 3, 131_072).await?;
    drop(database);
    assert_integrity(&first)?;

    fs::remove_dir_all(first.parent().ok_or("first cache has no parent")?)?;
    drop(log);
    let log = Log::open(backend.scope(&log_id), options).await?;
    let mut recovered = Database::open(log.clone(), &second).await?;
    assert_state(&mut recovered, 3, 131_072).await?;
    drop(recovered);
    assert_integrity(&second)?;

    let before_collection = log.load().await?;
    let CollectionStart::Installed(fenced, start) =
        log.start_collection(&before_collection).await?
    else {
        return Err("SQLite collection did not install a deletion plan".into());
    };
    assert!(start.candidate_count() > 0);
    let CollectionFinish::Complete(_, finish) = log.resume_collection(&fenced).await? else {
        return Err("SQLite collection did not complete".into());
    };
    assert_eq!(finish.candidate_count(), start.candidate_count());
    assert_eq!(finish.delete_attempts(), start.candidate_count());

    fs::remove_dir_all(second.parent().ok_or("second cache has no parent")?)?;
    drop(log);
    let log = Log::open(backend.scope(&log_id), options).await?;
    let mut collected = Database::open(log, &third).await?;
    assert_state(&mut collected, 3, 131_072).await?;
    drop(collected);
    assert_integrity(&third)?;
    Ok(())
}

async fn write_generation(
    database: &mut Database,
    generation: i64,
    payload_bytes: i64,
) -> TestResult {
    let marker = format!("state-{generation}");
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), move |transaction| {
            transaction.execute(
                "UPDATE state
                 SET generation = ?1, marker = ?2, payload = randomblob(?3)
                 WHERE id = 1",
                rusqlite::params![generation, marker, payload_bytes],
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("SQLite update did not stage".into());
    };
    if !matches!(staged.publish().await?, CommitStatus::Committed(_)) {
        return Err("SQLite update did not publish".into());
    }
    Ok(())
}

async fn assert_state(database: &mut Database, generation: i64, payload_bytes: i64) -> TestResult {
    let actual = database
        .read(|connection| {
            connection.query_row(
                "SELECT count(*), generation, marker, length(payload) FROM state",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
        })
        .await?;
    assert_eq!(
        actual,
        (1, generation, format!("state-{generation}"), payload_bytes)
    );
    Ok(())
}

fn assert_integrity(path: &std::path::Path) -> TestResult {
    let connection = rusqlite::Connection::open(path)?;
    let result =
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?;
    assert_eq!(result, "ok");
    Ok(())
}

fn build_minio() -> TestResult<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(required_env("OBJECT_LOG_MINIO_ENDPOINT")?)
        .with_access_key_id(required_env("OBJECT_LOG_MINIO_ACCESS_KEY")?)
        .with_secret_access_key(required_env("OBJECT_LOG_MINIO_SECRET_KEY")?)
        .with_bucket_name(required_env("OBJECT_LOG_MINIO_BUCKET")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_disable_bulk_delete(false)
        .build()?)
}

fn required_env(name: &'static str) -> TestResult<String> {
    env::var(name).map_err(|_| format!("{name} is not set").into())
}
