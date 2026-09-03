use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_log::sim::{FailurePhase, FaultStore, Operation, RequestOutcome};
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Error, Log, LogId, Options, TransactionId,
    ValidatedBackend,
};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn cold_open_retries_when_collection_deletes_its_old_snapshot() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("sqlite-gc-race-tests")).await?;
    let log = Log::open(
        backend.scope(&LogId::new("materialization")?),
        Options {
            max_inline_operation_bytes: 1_024,
            max_object_bytes: 8_240,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let mut writer = Database::open(log.clone(), directory.path().join("writer.sqlite3")).await?;
    commit(
        &mut writer,
        "CREATE TABLE state (generation INTEGER, payload BLOB);
         INSERT INTO state VALUES (0, randomblob(32768));",
    )
    .await?;
    checkpoint(&mut writer).await?;
    let old_view = log.load().await?;
    let old_blob = log
        .read_checkpoint(&old_view)
        .await?
        .ok_or("the old view has no checkpoint")?
        .objects()
        .first()
        .ok_or("the old checkpoint has no external blob")?
        .clone();

    store.reset();
    let mut pause = store.pause_get_at(3, FailurePhase::Before);
    let cache = directory.path().join("cold.sqlite3");
    let mut opening = Box::pin(Database::open(log.clone(), cache.clone()));
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut opening => return Err("cold open finished before the target GET".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("cold open did not reach the target GET".into()),
    };
    assert!(entered);
    assert!(store.metrics().operation(Operation::Get).requests >= 3);

    commit(
        &mut writer,
        "UPDATE state SET generation = 1, payload = randomblob(32768)",
    )
    .await?;
    checkpoint(&mut writer).await?;
    let current = log.load().await?;
    let CollectionStart::Installed(fenced, _) = log.start_collection(&current).await? else {
        return Err("collection did not install its deletion plan".into());
    };
    if !matches!(
        log.resume_collection(&fenced).await?,
        CollectionFinish::Complete(_, _)
    ) {
        return Err("collection did not complete".into());
    }
    assert!(matches!(
        log.read_object(&old_view, &old_blob).await,
        Err(Error::ViewExpired)
    ));

    assert!(pause.release());
    let mut recovered = opening.await?;
    assert_eq!(
        recovered
            .read(|connection| connection.query_row(
                "SELECT generation, length(payload) FROM state",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            ))
            .await?,
        (1, 32_768)
    );
    drop(recovered);
    let connection = rusqlite::Connection::open(cache)?;
    assert_eq!(
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    let target = store
        .metrics()
        .events
        .into_iter()
        .find(|event| event.operation == Operation::Get && event.occurrence == 3)
        .ok_or("the target GET was not recorded")?;
    assert!(target.path.contains("/blobs/"));
    assert_eq!(target.outcome, RequestOutcome::BackendError);
    Ok(())
}

async fn commit(database: &mut Database, sql: &str) -> TestResult {
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

async fn checkpoint(database: &mut Database) -> TestResult {
    if !matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ) {
        return Err("SQLite checkpoint did not publish".into());
    }
    Ok(())
}
