use std::error::Error as StdError;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options, TransactionId,
    ValidatedBackend, View,
};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

const CHECKPOINTS: i64 = 8;
const UPDATES_PER_CHECKPOINT: i64 = 3;
const PAYLOAD_BYTES: i64 = 32 * 1_024;
const GC_DEADLINE: Duration = Duration::from_secs(10);

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn collection_removes_sqlite_history_and_preserves_cold_recovery() -> TestResult {
    let raw = Arc::new(InMemory::new());
    let root = Path::from("sqlite-gc-tests");
    let id = LogId::new("checkpoint-history")?;
    let backend = ValidatedBackend::new(raw.clone(), root.clone()).await?;
    let log = Log::open(
        backend.scope(&id),
        Options {
            max_inline_operation_bytes: 1_024,
            max_object_bytes: 8_240,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let cache = directory.path().join("cache").join("database.sqlite3");
    let mut database = Database::open(log.clone(), &cache).await?;

    create_database(&mut database).await?;
    checkpoint(&mut database).await?;
    let mut generation = 0;
    for _ in 0..CHECKPOINTS {
        for _ in 0..UPDATES_PER_CHECKPOINT {
            generation += 1;
            write_generation(&mut database, generation).await?;
        }
        checkpoint(&mut database).await?;
    }
    generation += 1;
    write_generation(&mut database, generation).await?;
    assert_database(&mut database, generation).await?;
    drop(database);
    assert_integrity(&cache)?;

    let view = log.load().await?;
    let live = live_object_count(&log, &view).await?;
    let scope = root.join("v1").join("logs").join(id.as_str());
    let before = list(&raw, &scope).await?;
    assert!(segment_count(&before, "checkpoints") > 1);
    assert!(segment_count(&before, "commits") > usize::try_from(UPDATES_PER_CHECKPOINT)?);
    assert!(segment_count(&before, "blobs") > 100);
    assert!(before.len() > live + 100);

    let started = Instant::now();
    let (current, candidates) = tokio::time::timeout(GC_DEADLINE, collect(&log, &view))
        .await
        .map_err(|_| format!("SQLite GC exceeded {GC_DEADLINE:?}"))??;
    let elapsed = started.elapsed();
    assert!(candidates > 100);
    assert_eq!(candidates, before.len() - live);

    let after = list(&raw, &scope).await?;
    assert_eq!(after.len(), live);
    assert_eq!(segment_count(&after, "checkpoints"), 1);
    assert_eq!(segment_count(&after, "commits"), current.tail().len());
    assert_eq!(segment_count(&after, "collection-plans"), 0);

    fs::remove_dir_all(cache.parent().ok_or("cache path has no parent")?)?;
    assert!(!cache.exists());
    let mut recovered = Database::open(log.clone(), &cache).await?;
    assert_database(&mut recovered, generation).await?;
    drop(recovered);
    assert_integrity(&cache)?;

    let generation_before = current.cursor().generation();
    let epoch_before = current.collection_epoch();
    let CollectionStart::Empty(report) = log.start_collection(&current).await? else {
        return Err("the second collection was not empty".into());
    };
    assert_eq!(report.candidate_count(), 0);
    let unchanged = log.load().await?;
    assert_eq!(unchanged.cursor().generation(), generation_before);
    assert_eq!(unchanged.collection_epoch(), epoch_before);
    assert_eq!(list(&raw, &scope).await?.len(), after.len());

    eprintln!(
        "SQLite GC: {candidates} objects removed in {elapsed:?}; {} live objects remain",
        after.len()
    );
    Ok(())
}

async fn create_database(database: &mut Database) -> TestResult {
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), |transaction| {
            transaction.execute_batch(
                "CREATE TABLE state (
                    id INTEGER PRIMARY KEY,
                    generation INTEGER NOT NULL,
                    marker TEXT NOT NULL,
                    payload BLOB NOT NULL
                );
                INSERT INTO state VALUES (1, 0, 'state-0', randomblob(32768));",
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("database creation did not produce a staged write".into());
    };
    if !matches!(staged.publish().await?, CommitStatus::Committed(_)) {
        return Err("database creation did not commit".into());
    }
    Ok(())
}

async fn write_generation(database: &mut Database, generation: i64) -> TestResult {
    let marker = format!("state-{generation}");
    let StageStatus::Staged(staged) = database
        .stage_write(TransactionId::new(), move |transaction| {
            transaction.execute(
                "UPDATE state
                 SET generation = ?1, marker = ?2, payload = randomblob(32768)
                 WHERE id = 1",
                rusqlite::params![generation, marker],
            )?;
            Ok(Bytes::new())
        })
        .await?
    else {
        return Err("database update did not produce a staged write".into());
    };
    if !matches!(staged.publish().await?, CommitStatus::Committed(_)) {
        return Err("database update did not commit".into());
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

async fn assert_database(database: &mut Database, generation: i64) -> TestResult {
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
        (1, generation, format!("state-{generation}"), PAYLOAD_BYTES)
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

async fn live_object_count(log: &Log, view: &View) -> TestResult<usize> {
    let checkpoint = log
        .read_checkpoint(view)
        .await?
        .ok_or("SQLite history has no checkpoint")?;
    let tail = log.read_tail(view).await?;
    Ok(1 + 1
        + checkpoint.objects().len()
        + tail
            .iter()
            .map(|commit| 1 + commit.objects().len())
            .sum::<usize>())
}

async fn collect(log: &Log, view: &View) -> TestResult<(View, usize)> {
    let CollectionStart::Installed(fenced, start) = log.start_collection(view).await? else {
        return Err("collection did not install a deletion plan".into());
    };
    let CollectionFinish::Complete(current, finish) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish its deletion plan".into());
    };
    assert_eq!(finish.candidate_count(), start.candidate_count());
    assert_eq!(finish.delete_attempts(), start.candidate_count());
    Ok((current, start.candidate_count()))
}

async fn list(store: &InMemory, scope: &Path) -> TestResult<Vec<ObjectMeta>> {
    Ok(store.list(Some(scope)).try_collect().await?)
}

fn segment_count(objects: &[ObjectMeta], segment: &str) -> usize {
    let marker = format!("/{segment}/");
    objects
        .iter()
        .filter(|object| object.location.as_ref().contains(&marker))
        .count()
}
