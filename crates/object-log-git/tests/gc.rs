mod support;

use std::{
    collections::BTreeSet,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::TryStreamExt;
use object_log::sim::{FailurePhase, FaultStore, Operation, RequestOutcome};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, Error as LogError, Log, LogId, Options,
    Resolution, TransactionId, ValidatedBackend, View,
};
use object_log_git::{Error as GitError, ObjectFormat, Repository};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as StorePath};
use support::{Fixture, TestResult, fixture, publish};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DEAD_BYTES: usize = 2 * 1024 * 1024;
const GC_DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test]
async fn cold_open_retries_from_an_empty_cache_after_its_view_expires() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let raw = Arc::new(InMemory::new());
    let store = FaultStore::from_arc(raw.clone());
    let backend = ValidatedBackend::new(
        Arc::new(store.clone()),
        StorePath::from("git-open-gc-race-tests"),
    )
    .await?;
    let log = Log::open(
        &backend,
        &LogId::new("repository")?,
        Options {
            max_object_bytes: 8_240,
            max_collection_objects: 10_000,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let old = fixture("old", 256 * 1_024, 1)?;
    let new = fixture("new", 64 * 1_024, 2)?;

    let initial = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(
        initial,
        "refs/heads/old",
        None,
        Some(old.target),
        Some(&old.pack),
    )
    .await?;
    let checkpoint = Repository::open(&log, ObjectFormat::Sha1).await?;
    assert!(matches!(
        checkpoint.checkpoint().await?,
        CheckpointStatus::Published(_)
    ));

    // Precompute the other process's valid publication, retaining the old head.
    // Replaying those heads below models separate hosts without bypassing the
    // process-wide shared-engine admission limit.
    let head = head_path(&raw).await?;
    let old_head = raw.get(&head).await?.bytes().await?;
    let writer = Repository::open(&log, ObjectFormat::Sha1).await?;
    let prepared = writer
        .prepare_receive(
            TransactionId::new(),
            support::receive(
                &[
                    ("refs/heads/old", Some(old.target), None),
                    ("refs/heads/main", None, Some(new.target)),
                ],
                Some(&new.pack),
            )?,
        )
        .await?;
    let (resolution, response) = prepared.publish_receive().await?;
    assert!(matches!(resolution, Resolution::Committed(_)));
    drop(response);
    let checkpoint = Repository::open(&log, ObjectFormat::Sha1).await?;
    let CheckpointStatus::Published(_) = checkpoint.checkpoint().await? else {
        return Err("new checkpoint did not publish".into());
    };
    let new_head = raw.get(&head).await?.bytes().await?;
    raw.put(&head, old_head.into()).await?;
    store.reset();
    // Sparse open reads the checkpoint, not every pack. GC expires that exact
    // checkpoint while its GET is paused, forcing open's bounded retry.
    let mut pause = store.pause_get_at(2, FailurePhase::Before);
    let mut opening = Box::pin(Repository::open(&log, ObjectFormat::Sha1));
    let entered = tokio::select! {
        entered = pause.wait_until_entered() => entered,
        _ = &mut opening => return Err("cold open finished before the target GET".into()),
        () = tokio::time::sleep(Duration::from_secs(5)) => return Err("cold open did not reach the target GET".into()),
    };
    assert!(entered);
    raw.put(&head, new_head.into()).await?;
    let current = log.load().await?;
    let CollectionStart::Installed(fenced, _) = log.start_collection(&current).await? else {
        return Err("collection did not install its deletion plan".into());
    };
    assert!(matches!(
        log.resume_collection(&fenced).await?,
        CollectionFinish::Complete(_, _)
    ));

    assert!(pause.release());
    let recovered = opening.await?;
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&new.target)
    );
    assert!(!recovered.refs().contains_key(&b"refs/heads/old"[..]));
    support::recover(recovered, &directory.path().join("cold"), &new).await?;

    let target = store
        .metrics()
        .events
        .into_iter()
        .find(|event| event.operation == Operation::Get && event.occurrence == 2)
        .ok_or("the target GET was not recorded")?;
    assert!(target.path.contains("/checkpoints/"));
    assert_eq!(target.outcome, RequestOutcome::BackendError);
    Ok(())
}

#[tokio::test]
async fn cold_open_does_not_retry_current_epoch_corruption() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let raw = Arc::new(InMemory::new());
    let root = StorePath::from("git-open-corruption-tests");
    let store = FaultStore::from_arc(raw.clone());
    let backend = ValidatedBackend::new(Arc::new(store.clone()), root.clone()).await?;
    let log = Log::open(
        &backend,
        &LogId::new("repository")?,
        Options {
            max_object_bytes: 8_240,
            ..Options::default()
        },
    )
    .await?;
    let fixture = fixture("current", 64 * 1_024, 3)?;
    let initial = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(
        initial,
        "refs/heads/main",
        None,
        Some(fixture.target),
        Some(&fixture.pack),
    )
    .await?;
    let checkpoint = Repository::open(&log, ObjectFormat::Sha1).await?;
    assert!(matches!(
        checkpoint.checkpoint().await?,
        CheckpointStatus::Published(_)
    ));

    let blob = raw
        .list(Some(&root))
        .try_filter(|object| futures::future::ready(object.location.as_ref().contains("/blobs/")))
        .try_next()
        .await?
        .ok_or("the durable pack has no blob")?;
    raw.delete(&blob.location).await?;
    store.reset();

    let repository = Repository::open(&log, ObjectFormat::Sha1).await?;
    let result = support::fetch(repository, fixture.target).await;
    assert!(matches!(
        result
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<GitError>()),
        Some(GitError::ObjectLog(LogError::CorruptObject))
    ));
    let head_reads = store
        .metrics()
        .events
        .iter()
        .filter(|event| event.operation == Operation::Get && event.path.ends_with("/index.cbor"))
        .count();
    assert_eq!(head_reads, 2);
    Ok(())
}

#[tokio::test]
async fn checkpoint_makes_dead_pack_collectable_and_keeps_live_pack() -> TestResult {
    let _guard = TEST_LOCK.lock().await;
    let raw = Arc::new(InMemory::new());
    let root = StorePath::from("git-gc-tests");
    let backend = ValidatedBackend::new(raw.clone(), root.clone()).await?;
    let log = Log::open(
        &backend,
        &LogId::new("repository")?,
        Options {
            max_object_bytes: 16 * 1024,
            max_collection_objects: 10_000,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;

    let empty = Repository::open(&log, ObjectFormat::Sha1).await?;
    let CheckpointStatus::Published(empty_view) = empty.checkpoint().await? else {
        return Err("empty checkpoint did not return its current view".into());
    };
    assert!(empty_view.tail().is_empty());

    let live = fixture("live", 4_096, 1)?;
    let dead = fixture("dead", DEAD_BYTES, 2)?;
    assert!(dead.pack_bytes > live.pack_bytes);
    let first = Repository::open(&log, ObjectFormat::Sha1).await?;
    let first_view = publish(
        first,
        "refs/heads/main",
        None,
        Some(live.target),
        Some(&live.pack),
    )
    .await?;
    assert_eq!(first_view.tail().len(), 1);
    let live_pack = pack_keys(&stored_keys(&raw, &root).await?);

    let second = Repository::open(&log, ObjectFormat::Sha1).await?;
    let second_view = publish(
        second,
        "refs/heads/dead",
        None,
        Some(dead.target),
        Some(&dead.pack),
    )
    .await?;
    assert_eq!(second_view.tail().len(), 2);
    let both_packs = pack_keys(&stored_keys(&raw, &root).await?);
    let dead_pack = both_packs
        .difference(&live_pack)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert!(dead_pack.len() >= 100);

    let third = Repository::open(&log, ObjectFormat::Sha1).await?;
    publish(third, "refs/heads/dead", Some(dead.target), None, None).await?;

    let checkpoint = Repository::open(&log, ObjectFormat::Sha1).await?;
    let CheckpointStatus::Published(checkpoint_view) = checkpoint.checkpoint().await? else {
        return Err("Git checkpoint did not publish".into());
    };
    assert!(checkpoint_view.tail().is_empty());

    let tail = Repository::open(&log, ObjectFormat::Sha1).await?;
    let current = publish(
        tail,
        "refs/tags/after-checkpoint",
        None,
        Some(live.target),
        None,
    )
    .await?;
    assert_eq!(current.tail().len(), 1);

    let before = stored_keys(&raw, &root).await?;
    let started = Instant::now();
    let (current, candidates) = tokio::time::timeout(GC_DEADLINE, collect(&log, &current))
        .await
        .map_err(|_| format!("Git GC exceeded {GC_DEADLINE:?}"))??;
    let elapsed = started.elapsed();
    assert!(candidates >= dead_pack.len());
    assert!(candidates >= 100);

    let after = stored_keys(&raw, &root).await?;
    assert_eq!(after.len(), before.len() - candidates);
    assert!(live_pack.is_subset(&after));
    assert!(dead_pack.is_disjoint(&after));

    assert_recovery(&log, directory.path(), &live).await?;

    let generation = current.generation();
    let epoch = current.collection_epoch();
    let CollectionStart::Empty(report) = log.start_collection(&current).await? else {
        return Err("the second collection was not empty".into());
    };
    assert_eq!(report.candidate_count(), 0);
    let unchanged = log.load().await?;
    assert_eq!(unchanged.generation(), generation);
    assert_eq!(unchanged.collection_epoch(), epoch);
    eprintln!(
        "Git GC: {candidates} objects removed in {elapsed:?}; {} pack objects remain",
        live_pack.len()
    );
    Ok(())
}

async fn assert_recovery(log: &Log, root: &Path, live: &Fixture) -> TestResult {
    let path = root.join("recovered");
    let recovered = Repository::open(log, ObjectFormat::Sha1).await?;
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&live.target)
    );
    assert_eq!(
        recovered.refs().get(&b"refs/tags/after-checkpoint"[..]),
        Some(&live.target)
    );
    support::recover(recovered, &path, live).await
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

async fn stored_keys(store: &InMemory, root: &StorePath) -> TestResult<BTreeSet<String>> {
    Ok(store
        .list(Some(root))
        .map_ok(|object| object.location.to_string())
        .try_collect()
        .await?)
}

fn pack_keys(keys: &BTreeSet<String>) -> BTreeSet<String> {
    keys.iter()
        .filter(|key| key.contains("/blobs/") || key.contains("/nodes/"))
        .cloned()
        .collect()
}

async fn head_path(raw: &InMemory) -> TestResult<StorePath> {
    Ok(raw
        .list(None)
        .try_filter(|object| {
            futures::future::ready(object.location.as_ref().ends_with("/index.cbor"))
        })
        .try_next()
        .await?
        .ok_or("missing head")?
        .location)
}
