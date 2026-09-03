use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Options, ScopedStore, TransactionId, View,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn checkpoint_replaces_one_prefix_and_preserves_its_suffix() -> TestResult {
    let log = open(
        Arc::new(InMemory::new()),
        "checkpoint-prefix",
        Options::default(),
    )
    .await?;
    let empty = log.load().await?;
    let first = append(&log, &empty, b"first").await?;
    let second = append(&log, &first, b"second").await?;
    let third = append(&log, &second, b"third").await?;
    let through = third.tail()[0].clone();

    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(&third, &through, Bytes::from_static(b"state after first"))
        .await?
    else {
        return Err("checkpoint publication returned a conflict".into());
    };

    let reference = compacted
        .checkpoint()
        .ok_or("published view has no checkpoint")?;
    assert_eq!(reference.through_sequence, through.sequence);
    assert_eq!(reference.through_commit, through.digest);
    assert_eq!(compacted.tail().len(), 2);
    assert_eq!(compacted.tail()[0].sequence, 1);
    assert_eq!(compacted.tail()[1].sequence, 2);
    assert_eq!(
        log.read_checkpoint(&compacted).await?,
        Some(Bytes::from_static(b"state after first"))
    );
    let tail = log.read_tail(&compacted).await?;
    assert_eq!(tail[0].expected_tip(), Some(through.digest));
    assert_eq!(tail[0].operation(), &Bytes::from_static(b"second"));
    assert_eq!(tail[1].operation(), &Bytes::from_static(b"third"));
    Ok(())
}

#[tokio::test]
async fn checkpoint_and_append_race_without_losing_history() -> TestResult {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(Arc::clone(&backend), "checkpoint-race", Options::default()).await?;
    let second = open(backend, "checkpoint-race", Options::default()).await?;
    let empty = first.load().await?;
    let one = append(&first, &empty, b"one").await?;
    let through = one.tail()[0].clone();
    let append_candidate = second.prepare(
        one.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"two"),
        Bytes::new(),
        Vec::new(),
    )?;

    let (checkpoint, append) = tokio::join!(
        first.publish_checkpoint(&one, &through, Bytes::from_static(b"one-state")),
        second.commit(append_candidate)
    );
    let checkpoint = checkpoint?;
    let append = append?;
    match (checkpoint, append) {
        (CheckpointStatus::Published(view), CommitStatus::Conflict(current)) => {
            assert!(view.tail().is_empty());
            assert!(view.checkpoint().is_some());
            assert_eq!(current.cursor().generation(), view.cursor().generation());
        }
        (CheckpointStatus::Conflict(current), CommitStatus::Committed(view)) => {
            assert!(current.checkpoint().is_none());
            assert_eq!(view.tail().len(), 2);
            assert_eq!(current.cursor().tip(), view.cursor().tip());
        }
        _ => return Err("checkpoint race did not produce one CAS winner".into()),
    }
    Ok(())
}

#[tokio::test]
async fn stale_checkpoint_returns_the_current_view() -> TestResult {
    let log = open(
        Arc::new(InMemory::new()),
        "stale-checkpoint",
        Options::default(),
    )
    .await?;
    let empty = log.load().await?;
    let one = append(&log, &empty, b"one").await?;
    let through = one.tail()[0].clone();

    let CheckpointStatus::Published(published) = log
        .publish_checkpoint(&one, &through, Bytes::from_static(b"first base"))
        .await?
    else {
        return Err("first checkpoint did not publish".into());
    };
    let CheckpointStatus::Conflict(current) = log
        .publish_checkpoint(&one, &through, Bytes::from_static(b"stale base"))
        .await?
    else {
        return Err("stale checkpoint did not conflict".into());
    };
    assert_eq!(
        current.cursor().generation(),
        published.cursor().generation()
    );
    assert_eq!(
        current.checkpoint().map(|base| base.object.digest),
        published.checkpoint().map(|base| base.object.digest)
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_limit_fails_before_index_publication() -> TestResult {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let options = Options {
        max_checkpoint_bytes: 1,
        ..Options::default()
    };
    let log = open(backend, "checkpoint-limit", options).await?;
    let empty = log.load().await?;
    let one = append(&log, &empty, b"one").await?;
    let through = one.tail()[0].clone();

    assert!(matches!(
        log.publish_checkpoint(&one, &through, Bytes::from_static(b"too large"))
            .await,
        Err(object_log::Error::LimitExceeded("encoded checkpoint bytes"))
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_none());
    assert_eq!(current.tail(), one.tail());
    Ok(())
}

async fn open(
    store: Arc<dyn ObjectStore>,
    id: &str,
    options: Options,
) -> Result<Log, object_log::Error> {
    let log_id = LogId::new(id)?;
    let scoped = ScopedStore::new(store, Path::from("checkpoint-tests"), &log_id);
    Log::open(scoped, options).await
}

async fn append(
    log: &Log,
    view: &View,
    operation: &'static [u8],
) -> Result<View, Box<dyn StdError>> {
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(operation),
        Bytes::new(),
        Vec::new(),
    )?;
    match log.commit(prepared).await? {
        CommitStatus::Committed(next) => Ok(next),
        CommitStatus::Conflict(_) => Err("append returned a conflict".into()),
        CommitStatus::Pending(_) => Err("append returned pending".into()),
    }
}
