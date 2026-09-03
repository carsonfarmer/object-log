use std::error::Error as StdError;
use std::fs;
use std::path::Path as FsPath;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::{CommitStatus, Digest, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, SqliteError, StageStatus};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rusqlite::ErrorCode;

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn ten_wal_transactions_recover_without_the_cache() -> TestResult {
    let (_, log) = open_log("ten-wal", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let cache = directory.path().join("cache/database.sqlite3");
    let mut database = Database::open(log.clone(), &cache).await?;

    commit_sql(
        &mut database,
        "CREATE TABLE events (sequence INTEGER PRIMARY KEY, marker TEXT NOT NULL);
         INSERT INTO events VALUES (0, 'event-0');",
    )
    .await?;
    for sequence in 1..=10 {
        let StageStatus::Staged(staged) = database
            .stage_write(TransactionId::new(), move |transaction| {
                transaction.execute(
                    "INSERT INTO events VALUES (?1, ?2)",
                    rusqlite::params![sequence, format!("event-{sequence}")],
                )?;
                Ok(Bytes::new())
            })
            .await?
        else {
            return Err("event insert did not produce a staged write".into());
        };
        assert!(matches!(
            staged.publish().await?,
            CommitStatus::Committed(_)
        ));
    }
    assert_eq!(log.load().await?.tail().len(), 11);

    drop(database);
    remove_cache(&cache)?;
    let mut recovered = Database::open(log, &cache).await?;
    let events = recovered
        .read(|connection| {
            let mut statement =
                connection.prepare("SELECT sequence, marker FROM events ORDER BY sequence")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .await?;
    assert_eq!(
        events,
        (0..=10)
            .map(|sequence| (sequence, format!("event-{sequence}")))
            .collect::<Vec<_>>()
    );
    drop(recovered);
    assert_integrity(&cache)?;
    Ok(())
}

#[tokio::test]
async fn chunked_checkpoint_and_later_wal_recover_cold() -> TestResult {
    let options = Options {
        max_inline_operation_bytes: 1_024,
        max_object_bytes: 8_240,
        ..Options::default()
    };
    let (_, log) = open_log("chunked-checkpoint", options).await?;
    let directory = tempfile::tempdir()?;
    let cache = directory.path().join("cache/database.sqlite3");
    let mut database = Database::open(log.clone(), &cache).await?;

    commit_sql(
        &mut database,
        "CREATE TABLE state (generation INTEGER NOT NULL, payload BLOB NOT NULL);
         INSERT INTO state VALUES (0, zeroblob(65536));",
    )
    .await?;
    commit_sql(
        &mut database,
        "UPDATE state SET generation = 1, payload = randomblob(98304);",
    )
    .await?;
    let before_checkpoint = log.read_tail(&log.load().await?).await?;
    assert_eq!(before_checkpoint.len(), 2);
    assert!(
        before_checkpoint
            .iter()
            .all(|record| record.objects().len() > 1)
    );

    assert!(matches!(
        database.checkpoint().await?,
        SqliteCheckpointStatus::Published(_)
    ));
    let checkpointed = log.load().await?;
    assert!(checkpointed.tail().is_empty());
    assert!(
        log.read_checkpoint(&checkpointed)
            .await?
            .ok_or("checkpoint record is missing")?
            .objects()
            .len()
            > 1
    );

    commit_sql(
        &mut database,
        "UPDATE state SET generation = 2, payload = randomblob(81920);",
    )
    .await?;
    let current = log.load().await?;
    let tail = log.read_tail(&current).await?;
    assert_eq!(tail.len(), 1);
    assert!(tail[0].objects().len() > 1);

    drop(database);
    remove_cache(&cache)?;
    let mut recovered = Database::open(log, &cache).await?;
    assert_eq!(
        recovered
            .read(|connection| connection.query_row(
                "SELECT count(*), generation, length(payload) FROM state",
                [],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?
                ))
            ))
            .await?,
        (1, 2, 81_920)
    );
    drop(recovered);
    assert_integrity(&cache)?;
    Ok(())
}

#[tokio::test]
async fn corrupt_descriptors_and_referenced_blobs_fail_closed() -> TestResult {
    let (_, descriptor_log) = open_log("bad-descriptor", Options::default()).await?;
    commit_record(
        &descriptor_log,
        Bytes::from_static(b"not a SQLite record"),
        Vec::new(),
    )
    .await?;
    let directory = tempfile::tempdir()?;
    assert!(matches!(
        Database::open(descriptor_log, directory.path().join("descriptor.sqlite3")).await,
        Err(SqliteError::InvalidRecord(_))
    ));

    for (id, corrupt) in [("missing-blob", false), ("corrupt-blob", true)] {
        let (raw, log) = open_log(id, Options::default()).await?;
        let object = log.put_object(Bytes::from(vec![0; 4_096])).await?;
        let path = object_path(&raw, id, object.digest()).await?;
        commit_record(&log, snapshot_record()?, vec![object]).await?;
        if corrupt {
            raw.put(&path, Bytes::from(vec![1; 4_096]).into()).await?;
        } else {
            raw.delete(&path).await?;
        }
        assert!(matches!(
            Database::open(log, directory.path().join(format!("{id}.sqlite3"))).await,
            Err(SqliteError::Log(object_log::Error::CorruptObject))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn callback_policy_allows_main_sql_and_rejects_escape_paths() -> TestResult {
    let (_, log) = open_log("policy", Options::default()).await?;
    let directory = tempfile::tempdir()?;
    let mut database = Database::open(log, directory.path().join("database.sqlite3")).await?;

    commit_sql(
        &mut database,
        "CREATE TABLE values_table (id INTEGER PRIMARY KEY, value INTEGER);
         CREATE TABLE audit (value INTEGER);
         CREATE INDEX values_index ON values_table(value);
         CREATE TRIGGER values_insert AFTER INSERT ON values_table
           BEGIN INSERT INTO audit VALUES (NEW.value); END;
         INSERT INTO values_table VALUES (1, 10);
         UPDATE values_table SET value = 11 WHERE id = 1;
         SAVEPOINT nested;
         INSERT INTO values_table VALUES (2, 20);
         ROLLBACK TO nested;
         RELEASE nested;
         CREATE VIEW values_view AS SELECT value FROM values_table;
         ALTER TABLE values_table ADD COLUMN note TEXT;
         ANALYZE;
         REINDEX values_index;
         INSERT INTO audit VALUES (99);
         DELETE FROM audit WHERE value = 99;",
    )
    .await?;
    assert_eq!(
        database
            .read(|connection| connection.query_row(
                "SELECT
                    (SELECT count(*) FROM values_table),
                    (SELECT value FROM values_view),
                    (SELECT group_concat(value) FROM audit),
                    (SELECT count(*) FROM sqlite_schema
                     WHERE name IN ('values_index', 'values_insert', 'values_view'))",
                [],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?
                ))
            ))
            .await?,
        (1, 11, "10".to_owned(), 3)
    );

    assert_authorization_error(
        "read callback mutation",
        database
            .read(|connection| connection.execute_batch("INSERT INTO audit VALUES (99)"))
            .await,
    );
    for sql in [
        "CREATE TEMP TABLE denied (value INTEGER)",
        "ATTACH DATABASE ':memory:' AS denied",
        "DETACH DATABASE main",
        "PRAGMA user_version = 1",
        "COMMIT",
    ] {
        assert_authorization_error(
            sql,
            database
                .stage_write(TransactionId::new(), |transaction| {
                    transaction.execute_batch(sql)?;
                    Ok(Bytes::new())
                })
                .await,
        );
    }
    // Denied PRAGMA access keeps writable_schema off, so SQLite rejects this before authorization.
    assert!(
        database
            .stage_write(TransactionId::new(), |transaction| {
                transaction.execute_batch("DELETE FROM sqlite_schema")?;
                Ok(Bytes::new())
            })
            .await
            .is_err()
    );
    Ok(())
}

async fn open_log(id: &str, options: Options) -> TestResult<(Arc<InMemory>, Log)> {
    let raw = Arc::new(InMemory::new());
    let backend = ValidatedBackend::new(raw.clone(), Path::from("sqlite-recovery-tests")).await?;
    let log = Log::open(backend.scope(&LogId::new(id)?), options).await?;
    Ok((raw, log))
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

async fn commit_record(
    log: &Log,
    operation: Bytes,
    objects: Vec<object_log::ObjectRef>,
) -> TestResult {
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        operation,
        Bytes::new(),
        objects,
    )?;
    if !matches!(log.commit(prepared).await?, CommitStatus::Committed(_)) {
        return Err("test record did not commit".into());
    }
    Ok(())
}

fn snapshot_record() -> TestResult<Bytes> {
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
        .u64(4_096)?
        .u8(5)?
        .u32(1)?;
    Ok(Bytes::from(encoder.into_writer()))
}

async fn object_path(raw: &Arc<InMemory>, id: &str, digest: Digest) -> TestResult<Path> {
    let prefix = Path::from(format!("sqlite-recovery-tests/v1/logs/{id}/data"));
    let digest = digest.to_string();
    raw.list(Some(&prefix))
        .try_filter(|object| {
            let path = object.location.to_string();
            std::future::ready(path.contains("/blobs/") && path.ends_with(&digest))
        })
        .map_ok(|object| object.location)
        .try_next()
        .await?
        .ok_or_else(|| "test blob is missing".into())
}

fn remove_cache(path: &FsPath) -> TestResult {
    fs::remove_dir_all(path.parent().ok_or("cache path has no parent")?)?;
    assert!(!path.exists());
    Ok(())
}

fn assert_integrity(path: &FsPath) -> TestResult {
    let connection = rusqlite::Connection::open(path)?;
    let result =
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?;
    assert_eq!(result, "ok");
    Ok(())
}

fn assert_authorization_error<T>(label: &str, result: Result<T, SqliteError>) {
    let error = result.err();
    assert!(
        matches!(
            &error,
            Some(SqliteError::Sqlite(rusqlite::Error::SqliteFailure(error, _)))
                if error.code == ErrorCode::AuthorizationForStatementDenied
        ),
        "{label}: {error:?}"
    );
}
