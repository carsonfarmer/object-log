mod support;

use std::{
    collections::BTreeSet,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::TryStreamExt;
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, Log, LogId, Options, ValidatedBackend,
    View,
};
use object_log_git::{ObjectFormat, Repository};
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use support::{Fixture, TestResult, assert_repository, fixture, publish};

const DEAD_BYTES: usize = 2 * 1024 * 1024;
const GC_DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test]
async fn checkpoint_makes_dead_pack_collectable_and_keeps_live_pack() -> TestResult {
    let raw = Arc::new(InMemory::new());
    let root = StorePath::from("git-gc-tests");
    let backend = ValidatedBackend::new(raw.clone(), root.clone()).await?;
    let log = Log::open(
        backend.scope(&LogId::new("repository")?),
        Options {
            max_object_bytes: 16 * 1024,
            max_collection_objects: 10_000,
            ..Options::default()
        },
    )
    .await?;
    let directory = tempfile::tempdir()?;

    let empty = repository(&log, directory.path(), "empty").await?;
    let CheckpointStatus::Published(empty_view) = empty.checkpoint().await? else {
        return Err("empty checkpoint did not return its current view".into());
    };
    assert!(empty_view.tail().is_empty());

    let live = fixture("live", 4_096, 1)?;
    let dead = fixture("dead", DEAD_BYTES, 2)?;
    assert!(dead.pack_bytes > live.pack_bytes);
    let first = repository(&log, directory.path(), "first").await?;
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

    let second = repository(&log, directory.path(), "second").await?;
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

    let third = repository(&log, directory.path(), "third").await?;
    publish(third, "refs/heads/dead", Some(dead.target), None, None).await?;

    let checkpoint = repository(&log, directory.path(), "checkpoint").await?;
    let CheckpointStatus::Published(checkpoint_view) = checkpoint.checkpoint().await? else {
        return Err("Git checkpoint did not publish".into());
    };
    assert!(checkpoint_view.tail().is_empty());

    let tail = repository(&log, directory.path(), "tail").await?;
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

    let generation = current.cursor().generation();
    let epoch = current.collection_epoch();
    let CollectionStart::Empty(report) = log.start_collection(&current).await? else {
        return Err("the second collection was not empty".into());
    };
    assert_eq!(report.candidate_count(), 0);
    let unchanged = log.load().await?;
    assert_eq!(unchanged.cursor().generation(), generation);
    assert_eq!(unchanged.collection_epoch(), epoch);
    eprintln!(
        "Git GC: {candidates} objects removed in {elapsed:?}; {} pack objects remain",
        live_pack.len()
    );
    Ok(())
}

async fn assert_recovery(log: &Log, root: &Path, live: &Fixture) -> TestResult {
    let path = root.join("recovered");
    let recovered = Repository::open(log, &path, ObjectFormat::Sha1).await?;
    assert_eq!(
        recovered.refs().get(&b"refs/heads/main"[..]),
        Some(&live.target)
    );
    assert_eq!(
        recovered.refs().get(&b"refs/tags/after-checkpoint"[..]),
        Some(&live.target)
    );
    assert_repository(&path, live)
}

async fn repository(log: &Log, root: &Path, name: &str) -> TestResult<Repository> {
    Ok(Repository::open(log, root.join(name), ObjectFormat::Sha1).await?)
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
