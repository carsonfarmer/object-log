use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{FaultStore, Metrics, Operation};
use object_log::{CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;

const MIB: usize = 1_024 * 1_024;

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

#[tokio::test]
#[ignore = "allocates a 100 MiB SQLite database"]
async fn staged_object_request_accounting() -> TestResult {
    let mib = u64::try_from(MIB)?;
    let update = measure_update().await?;
    assert_eq!(update.operation(Operation::Get).requests, 1);
    assert_eq!(update.operation(Operation::Put).requests, 3);
    assert_eq!(update.total_requests(), 4);
    assert_eq!(blob_gets(&update), 0);
    assert_eq!(update.downloaded_bytes(), 0);
    assert!(update.uploaded_bytes() > mib);
    report("1 MiB SQLite update", &update);

    let checkpoint = measure_checkpoint().await?;
    assert_eq!(checkpoint.operation(Operation::Get).requests, 2);
    assert_eq!(checkpoint.operation(Operation::Put).requests, 4);
    assert_eq!(checkpoint.total_requests(), 6);
    assert_eq!(blob_gets(&checkpoint), 0);
    assert!(checkpoint.downloaded_bytes() < mib);
    assert!(checkpoint.uploaded_bytes() > 100 * mib);
    report("100 MiB SQLite checkpoint", &checkpoint);
    Ok(())
}

async fn measure_update() -> TestResult<Metrics> {
    let (store, log) = open_log("one-mib-update").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("database.sqlite3")).await?;
    create_database(&mut database, MIB).await?;
    publish_checkpoint(&mut database).await?;

    store.reset();
    let payload_bytes = i64::try_from(MIB)?;
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute(
                "UPDATE state SET generation = 1, payload = randomblob(?1)",
                [payload_bytes],
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("the update did not produce a staged write".into());
    };
    assert!(matches!(
        staged.publish().await?,
        CommitStatus::Committed(_)
    ));
    Ok(store.metrics())
}

async fn measure_checkpoint() -> TestResult<Metrics> {
    let (store, log) = open_log("hundred-mib-checkpoint").await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("database.sqlite3")).await?;
    create_database(&mut database, 100 * MIB).await?;

    store.reset();
    publish_checkpoint(&mut database).await?;
    Ok(store.metrics())
}

async fn open_log(id: &str) -> TestResult<(FaultStore, Log)> {
    let store = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(store.clone()), Path::from("accounting")).await?;
    let log = Log::open(backend.scope(&LogId::new(id)?), Options::default()).await?;
    Ok((store, log))
}

async fn create_database(database: &mut Database, payload_bytes: usize) -> TestResult {
    let payload_bytes = i64::try_from(payload_bytes)?;
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), move |transaction| {
            transaction.execute_batch(
                "CREATE TABLE state (generation INTEGER NOT NULL, payload BLOB NOT NULL)",
            )?;
            transaction.execute(
                "INSERT INTO state VALUES (0, zeroblob(?1))",
                [payload_bytes],
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("database creation did not produce a staged write".into());
    };
    assert!(matches!(
        staged.publish().await?,
        CommitStatus::Committed(_)
    ));
    Ok(())
}

async fn publish_checkpoint(database: &mut Database) -> TestResult {
    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ));
    Ok(())
}

fn blob_gets(metrics: &Metrics) -> usize {
    metrics
        .events
        .iter()
        .filter(|event| event.operation == Operation::Get && event.path.contains("/blobs/"))
        .count()
}

fn report(label: &str, metrics: &Metrics) {
    println!(
        "{label}: requests={}, GET={}, PUT={}, uploaded={} bytes, downloaded={} bytes",
        metrics.total_requests(),
        metrics.operation(Operation::Get).requests,
        metrics.operation(Operation::Put).requests,
        metrics.uploaded_bytes(),
        metrics.downloaded_bytes(),
    );
}
