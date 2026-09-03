#![cfg(feature = "test-util")]

use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Error, Log, LogId, Options,
    Resolution, RetentionId, RetentionStatus, TransactionId, ValidatedBackend, View,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

type TestResult = Result<(), Box<dyn StdError>>;

struct Fixture {
    log: Log,
    store: FaultStore,
    raw: Arc<dyn ObjectStore>,
    scope: Path,
}

impl Fixture {
    async fn new(id: &str, options: Options) -> Result<Self, Error> {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = FaultStore::from_arc(Arc::clone(&raw));
        let root = Path::from("gc-tests");
        let id = LogId::new(id)?;
        let backend = ValidatedBackend::new(Arc::new(store.clone()), root.clone()).await?;
        let log = Log::open(backend.scope(&id), options).await?;
        let scope = root.join("v1").join("logs").join(id.as_str());
        Ok(Self {
            log,
            store,
            raw,
            scope,
        })
    }

    fn scope(&self) -> Path {
        self.scope.clone()
    }

    async fn object_path(&self, digest: object_log::Digest) -> Result<Path, Box<dyn StdError>> {
        self.raw
            .list(Some(&self.scope().join("data")))
            .try_filter(|metadata| {
                std::future::ready(metadata.location.as_ref().ends_with(&digest.to_string()))
            })
            .map_ok(|metadata| metadata.location)
            .try_next()
            .await?
            .ok_or_else(|| "object path was not found".into())
    }

    async fn segment_path(&self, segment: &str) -> Result<Path, Box<dyn StdError>> {
        let marker = format!("/{segment}/");
        self.raw
            .list(Some(&self.scope().join("data")))
            .try_filter(|metadata| std::future::ready(metadata.location.as_ref().contains(&marker)))
            .map_ok(|metadata| metadata.location)
            .try_next()
            .await?
            .ok_or_else(|| format!("{segment} path was not found").into())
    }
}

async fn append(log: &Log, view: &View, operation: &'static [u8]) -> Result<View, Error> {
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(operation),
        Bytes::new(),
        Vec::new(),
    )?;
    match log.commit(prepared).await? {
        CommitStatus::Committed(view) => Ok(view),
        _ => Err(Error::InvalidFormat("test append did not commit".into())),
    }
}

async fn install_collection(log: &Log, view: &View) -> Result<View, Error> {
    match log.start_collection(view).await? {
        CollectionStart::Installed(view, _) => Ok(view),
        _ => Err(Error::InvalidFormat(
            "test collection did not install a plan".into(),
        )),
    }
}

fn assert_no_collection_mutation(fixture: &Fixture) {
    let metrics = fixture.store.metrics();
    assert_eq!(metrics.operation(Operation::Delete).requests, 0);
    assert_eq!(metrics.operation(Operation::Put).requests, 0);
}

fn assert_collection_not_started(fixture: &Fixture) {
    assert_eq!(
        fixture.store.metrics().operation(Operation::List).requests,
        0
    );
    assert_no_collection_mutation(fixture);
}

#[tokio::test]
async fn collection_deletes_only_unreachable_data_and_classifies_missing_reads() -> TestResult {
    let fixture = Fixture::new("exact", Options::default()).await?;
    let old = fixture.log.load().await?;
    let orphan = fixture
        .log
        .put_object(old.cursor(), Bytes::from_static(b"unreachable"))
        .await?;

    let CollectionStart::Installed(fenced, start_report) =
        fixture.log.start_collection(&old).await?
    else {
        return Err("collection did not install".into());
    };
    assert_eq!(fenced.collection_epoch(), 1);
    assert_eq!(start_report.candidate_count(), 1);
    assert_eq!(start_report.candidate_bytes(), 11);
    assert_eq!(start_report.delete_attempts(), 0);

    let CollectionFinish::Complete(current, finish_report) =
        fixture.log.resume_collection(&fenced).await?
    else {
        return Err("collection did not finish".into());
    };
    assert_eq!(finish_report.candidate_count(), 1);
    assert_eq!(finish_report.candidate_bytes(), 11);
    assert_eq!(finish_report.delete_attempts(), 1);
    assert!(matches!(
        fixture.log.read_object(&old, orphan.reference()).await,
        Err(Error::ViewExpired)
    ));
    assert!(matches!(
        fixture.log.read_object(&current, orphan.reference()).await,
        Err(Error::CorruptObject)
    ));
    assert!(matches!(
        fixture.log.start_collection(&current).await?,
        CollectionStart::Empty(_)
    ));
    assert_eq!(fixture.log.load().await?.collection_epoch(), 1);
    Ok(())
}

#[tokio::test]
async fn empty_collection_does_not_write_or_advance_the_head() -> TestResult {
    let fixture = Fixture::new("empty", Options::default()).await?;
    let view = fixture.log.load().await?;
    fixture.store.reset();

    let CollectionStart::Empty(report) = fixture.log.start_collection(&view).await? else {
        return Err("empty collection created a fence".into());
    };
    assert_eq!(report.candidate_count(), 0);
    assert_eq!(
        fixture.store.metrics().operation(Operation::Put).requests,
        0
    );
    let current = fixture.log.load().await?;
    assert_eq!(current.cursor().generation(), view.cursor().generation());
    assert_eq!(current.collection_epoch(), view.collection_epoch());
    Ok(())
}

#[tokio::test]
async fn append_and_collection_have_one_cas_winner_and_preserve_the_fence() -> TestResult {
    let first = Fixture::new("append-start-first", Options::default()).await?;
    let source = first.log.load().await?;
    first
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let prepared = first.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"append"),
        Bytes::new(),
        Vec::new(),
    )?;
    let fenced = install_collection(&first.log, &source).await?;
    assert!(matches!(
        first.log.commit(prepared).await?,
        CommitStatus::Conflict(_)
    ));

    let appended = append(&first.log, &fenced, b"after-fence").await?;
    let CollectionFinish::Complete(current, report) = first.log.resume_collection(&fenced).await?
    else {
        return Err("collector did not follow the preserved fence".into());
    };
    assert_eq!(current.cursor().tip(), appended.cursor().tip());
    assert_eq!(report.delete_attempts(), 1);
    assert!(matches!(
        first.log.resume_collection(&current).await?,
        CollectionFinish::Complete(_, report) if report.candidate_count() == 0
    ));

    let second = Fixture::new("append-start-second", Options::default()).await?;
    let stale = second.log.load().await?;
    second
        .log
        .put_object(stale.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let _ = append(&second.log, &stale, b"winner").await?;
    assert!(matches!(
        second.log.start_collection(&stale).await?,
        CollectionStart::Conflict(_)
    ));
    Ok(())
}

#[tokio::test]
async fn checkpoint_and_collection_have_one_cas_winner() -> TestResult {
    let first = Fixture::new("checkpoint-start-first", Options::default()).await?;
    let one = append(&first.log, &first.log.load().await?, b"one").await?;
    first
        .log
        .put_object(one.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let through = one.tail()[0].clone();
    let fenced = install_collection(&first.log, &one).await?;
    assert!(matches!(
        first
            .log
            .publish_checkpoint(&one, &through, Bytes::from_static(b"state"), Vec::new())
            .await?,
        CheckpointStatus::Conflict(_)
    ));
    let CheckpointStatus::Published(checkpointed) = first
        .log
        .publish_checkpoint(
            &fenced,
            &through,
            Bytes::from_static(b"safe state"),
            Vec::new(),
        )
        .await?
    else {
        return Err("safe checkpoint did not publish through the fence".into());
    };
    assert!(matches!(
        first.log.start_collection(&checkpointed).await?,
        CollectionStart::Active(_)
    ));
    assert!(matches!(
        first.log.resume_collection(&checkpointed).await?,
        CollectionFinish::Complete(_, _)
    ));

    let second = Fixture::new("checkpoint-start-second", Options::default()).await?;
    let one = append(&second.log, &second.log.load().await?, b"one").await?;
    second
        .log
        .put_object(one.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let through = one.tail()[0].clone();
    assert!(matches!(
        second
            .log
            .publish_checkpoint(&one, &through, Bytes::from_static(b"state"), Vec::new())
            .await?,
        CheckpointStatus::Published(_)
    ));
    assert!(matches!(
        second.log.start_collection(&one).await?,
        CollectionStart::Conflict(_)
    ));
    Ok(())
}

#[tokio::test]
async fn collection_preserves_the_checkpoint_tail_and_nested_live_graph() -> TestResult {
    let fixture = Fixture::new("live-graph", Options::default()).await?;
    let source = fixture.log.load().await?;
    let leaf = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"leaf"))
        .await?;
    let node = fixture
        .log
        .put_node(
            source.cursor(),
            Bytes::from_static(b"node"),
            vec![leaf.clone()],
        )
        .await?;
    let prepared = fixture.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"base"),
        Bytes::new(),
        vec![node.clone()],
    )?;
    let CommitStatus::Committed(base) = fixture.log.commit(prepared).await? else {
        return Err("base commit failed".into());
    };
    let CheckpointStatus::Published(checkpointed) = fixture
        .log
        .publish_checkpoint(
            &base,
            &base.tail()[0],
            Bytes::from_static(b"checkpoint"),
            vec![node.clone()],
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let tail_object = fixture
        .log
        .put_object(checkpointed.cursor(), Bytes::from_static(b"tail object"))
        .await?;
    let prepared = fixture.log.prepare(
        checkpointed.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"tail"),
        Bytes::new(),
        vec![tail_object.clone()],
    )?;
    let CommitStatus::Committed(live_view) = fixture.log.commit(prepared).await? else {
        return Err("tail commit failed".into());
    };
    let orphan = fixture
        .log
        .put_object(live_view.cursor(), Bytes::from_static(b"orphan"))
        .await?;

    let CollectionStart::Installed(fenced, report) =
        fixture.log.start_collection(&live_view).await?
    else {
        return Err("collection did not install".into());
    };
    assert_eq!(report.candidate_count(), 2);
    let CollectionFinish::Complete(current, report) =
        fixture.log.resume_collection(&fenced).await?
    else {
        return Err("collection did not complete".into());
    };
    assert_eq!(report.delete_attempts(), 2);
    assert_eq!(fixture.log.read_tail(&current).await?.len(), 1);
    assert_eq!(
        fixture
            .log
            .read_checkpoint(&current)
            .await?
            .ok_or("checkpoint is missing")?
            .snapshot(),
        &Bytes::from_static(b"checkpoint")
    );
    assert_eq!(
        fixture
            .log
            .read_node(&current, node.reference())
            .await?
            .children(),
        std::slice::from_ref(leaf.reference())
    );
    assert_eq!(
        fixture.log.read_object(&current, leaf.reference()).await?,
        Bytes::from_static(b"leaf")
    );
    assert_eq!(
        fixture
            .log
            .read_object(&current, tail_object.reference())
            .await?,
        Bytes::from_static(b"tail object")
    );
    assert!(matches!(
        fixture.log.read_object(&current, orphan.reference()).await,
        Err(Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
async fn compacted_commit_resolves_after_collection_removes_its_body() -> TestResult {
    let fixture = Fixture::new("compacted-resolution", Options::default()).await?;
    let source = fixture.log.load().await?;
    let prepared = fixture.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"commit"),
        Bytes::new(),
        Vec::new(),
    )?;
    fixture.store.reset();
    fixture.store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let CommitStatus::Pending(pending) = fixture.log.commit(prepared).await? else {
        return Err("lost commit response was not pending".into());
    };
    let committed = fixture.log.load().await?;
    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(compacted) = fixture
        .log
        .publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"checkpoint"),
            Vec::new(),
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let fenced = install_collection(&fixture.log, &compacted).await?;
    let CollectionFinish::Complete(_, report) = fixture.log.resume_collection(&fenced).await?
    else {
        return Err("collection did not complete".into());
    };
    assert_eq!(report.delete_attempts(), 1);
    assert!(matches!(
        fixture.log.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn collection_fence_rejects_stale_and_planned_dependencies() -> TestResult {
    let fixture = Fixture::new("publication-fence", Options::default()).await?;
    let source = append(&fixture.log, &fixture.log.load().await?, b"base").await?;
    let planned = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"planned"))
        .await?;
    let fenced = install_collection(&fixture.log, &source).await?;

    assert!(matches!(
        fixture.log.prepare(
            fenced.cursor(),
            TransactionId::new(),
            Bytes::from_static(b"stale proof"),
            Bytes::new(),
            vec![planned.clone()],
        ),
        Err(Error::InvalidStagedObject)
    ));
    assert!(matches!(
        fixture
            .log
            .stage_objects(fenced.cursor(), vec![planned.reference().clone()])
            .await,
        Err(Error::CollectionFence)
    ));
    assert!(matches!(
        fixture
            .log
            .publish_checkpoint(
                &fenced,
                &source.tail()[0],
                Bytes::from_static(b"stale checkpoint proof"),
                vec![planned.clone()],
            )
            .await,
        Err(Error::InvalidStagedObject)
    ));
    assert!(matches!(
        fixture
            .log
            .start_collection(&fixture.log.load().await?)
            .await?,
        CollectionStart::Active(_)
    ));
    let CollectionFinish::Complete(cleared, _) = fixture.log.resume_collection(&fenced).await?
    else {
        return Err("collection did not clear".into());
    };
    assert!(matches!(
        fixture
            .log
            .stage_objects(cleared.cursor(), vec![planned.reference().clone()])
            .await,
        Err(Error::InvalidFormat(_))
    ));
    Ok(())
}

#[tokio::test]
async fn retention_and_collection_have_one_winner_and_release_is_idempotent() -> TestResult {
    let first = Fixture::new("retain-first", Options::default()).await?;
    let source = first.log.load().await?;
    let id = RetentionId::new();
    let RetentionStatus::Applied(retained) = first.log.retain(&source, id).await? else {
        return Err("retention was not applied".into());
    };
    assert!(matches!(
        first.log.start_collection(&retained).await?,
        CollectionStart::Retained(_)
    ));
    let RetentionStatus::Applied(released) = first.log.release_retention(&retained, id).await?
    else {
        return Err("retention was not released".into());
    };
    let generation = released.cursor().generation();
    let RetentionStatus::Applied(repeated) = first.log.release_retention(&released, id).await?
    else {
        return Err("repeat release was not idempotent".into());
    };
    assert_eq!(repeated.cursor().generation(), generation);

    let second = Fixture::new("collect-first", Options::default()).await?;
    let source = second.log.load().await?;
    second
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&second.log, &source).await?;
    assert!(matches!(
        second.log.retain(&fenced, RetentionId::new()).await?,
        RetentionStatus::ActiveCollection(_)
    ));
    Ok(())
}

#[tokio::test]
async fn stable_retention_id_resolves_a_lost_success_response() -> TestResult {
    let fixture = Fixture::new("retention-ack", Options::default()).await?;
    let source = fixture.log.load().await?;
    let id = RetentionId::new();
    fixture.store.reset();
    fixture.store.fail_next(Operation::Put, FailurePhase::After);

    assert!(matches!(
        fixture.log.retain(&source, id).await?,
        RetentionStatus::Pending
    ));
    let current = fixture.log.load().await?;
    assert!(matches!(
        fixture.log.retain(&current, id).await?,
        RetentionStatus::Applied(_)
    ));
    assert_eq!(
        fixture.store.metrics().operation(Operation::Put).requests,
        1
    );

    fixture.store.reset();
    fixture.store.fail_next(Operation::Put, FailurePhase::After);
    assert!(matches!(
        fixture.log.release_retention(&current, id).await?,
        RetentionStatus::Pending
    ));
    let released = fixture.log.load().await?;
    assert!(matches!(
        fixture.log.release_retention(&released, id).await?,
        RetentionStatus::Applied(_)
    ));
    assert_eq!(
        fixture.store.metrics().operation(Operation::Put).requests,
        1
    );
    Ok(())
}

#[tokio::test]
async fn collection_recovers_lost_fence_and_clear_responses() -> TestResult {
    let fixture = Fixture::new("collection-acks", Options::default()).await?;
    let source = fixture.log.load().await?;
    fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    fixture.store.reset();
    fixture.store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });

    assert!(matches!(
        fixture.log.start_collection(&source).await?,
        CollectionStart::Pending
    ));
    let fenced = fixture.log.load().await?;
    assert_eq!(fenced.collection_epoch(), 1);

    fixture.store.reset();
    fixture.store.fail_next(Operation::Put, FailurePhase::After);
    let CollectionFinish::Pending(report) = fixture.log.resume_collection(&fenced).await? else {
        return Err("lost clear response was not pending".into());
    };
    assert_eq!(report.delete_attempts(), 1);
    assert!(fixture.log.load().await?.cursor().generation() > fenced.cursor().generation());
    let CollectionFinish::Complete(cleared, report) =
        fixture.log.resume_collection(&fenced).await?
    else {
        return Err("the old fence did not resolve the lost clear".into());
    };
    assert_eq!(report.candidate_count(), 0);
    assert!(matches!(
        fixture.log.start_collection(&cleared).await?,
        CollectionStart::Empty(_)
    ));
    Ok(())
}

#[tokio::test]
async fn failed_plan_cleanup_is_collected_by_the_next_run() -> TestResult {
    let fixture = Fixture::new("plan-cleanup-failure", Options::default()).await?;
    let source = fixture.log.load().await?;
    fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&fixture.log, &source).await?;
    fixture.store.reset();
    fixture.store.schedule(Failure {
        operation: Operation::Delete,
        occurrence: 2,
        phase: FailurePhase::Before,
    });

    let CollectionFinish::Complete(cleared, report) =
        fixture.log.resume_collection(&fenced).await?
    else {
        return Err("plan cleanup changed the completed collection status".into());
    };
    assert_eq!(report.delete_attempts(), 1);
    let CollectionStart::Installed(fenced, report) = fixture.log.start_collection(&cleared).await?
    else {
        return Err("the failed cleanup did not leave one collectible plan".into());
    };
    assert_eq!(report.candidate_count(), 1);
    let CollectionFinish::Complete(cleared, _) = fixture.log.resume_collection(&fenced).await?
    else {
        return Err("the cleanup collection did not complete".into());
    };
    assert!(matches!(
        fixture.log.start_collection(&cleared).await?,
        CollectionStart::Empty(_)
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_live_data_and_graph_bounds_fail_before_listing_or_deletion() -> TestResult {
    let missing = Fixture::new("missing-live", Options::default()).await?;
    let source = missing.log.load().await?;
    let object = missing
        .log
        .put_object(source.cursor(), Bytes::from_static(b"required"))
        .await?;
    let prepared = missing.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![object.clone()],
    )?;
    let CommitStatus::Committed(live_view) = missing.log.commit(prepared).await? else {
        return Err("live reference did not commit".into());
    };
    let path = missing.object_path(object.reference().digest()).await?;
    missing.raw.delete(&path).await?;
    missing.store.reset();
    assert!(matches!(
        missing.log.start_collection(&live_view).await,
        Err(Error::InvalidFormat(_))
    ));
    assert_collection_not_started(&missing);

    let bounded = Fixture::new(
        "bounded-live",
        Options {
            max_collection_objects: 1,
            ..Options::default()
        },
    )
    .await?;
    let source = bounded.log.load().await?;
    let object = bounded
        .log
        .put_object(source.cursor(), Bytes::from_static(b"required"))
        .await?;
    let prepared = bounded.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![object],
    )?;
    let CommitStatus::Committed(view) = bounded.log.commit(prepared).await? else {
        return Err("bounded reference did not commit".into());
    };
    bounded.store.reset();
    assert!(matches!(
        bounded.log.start_collection(&view).await,
        Err(Error::LimitExceeded("collection live objects"))
    ));
    assert_collection_not_started(&bounded);
    Ok(())
}

#[tokio::test]
async fn corrupt_live_blob_fails_before_collection_io() -> TestResult {
    let corrupt = Fixture::new("corrupt-live", Options::default()).await?;
    let source = corrupt.log.load().await?;
    let object = corrupt
        .log
        .put_object(source.cursor(), Bytes::from_static(b"required"))
        .await?;
    let prepared = corrupt.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![object.clone()],
    )?;
    let CommitStatus::Committed(live_view) = corrupt.log.commit(prepared).await? else {
        return Err("corrupt reference did not commit".into());
    };
    corrupt
        .raw
        .put(
            &corrupt.object_path(object.reference().digest()).await?,
            Bytes::from_static(b"requirEd").into(),
        )
        .await?;
    corrupt.store.reset();
    assert!(matches!(
        corrupt.log.start_collection(&live_view).await,
        Err(Error::CorruptObject)
    ));
    assert_collection_not_started(&corrupt);
    Ok(())
}

#[tokio::test]
async fn corrupt_live_node_fails_before_collection_io() -> TestResult {
    let corrupt_node = Fixture::new("corrupt-live-node", Options::default()).await?;
    let source = corrupt_node.log.load().await?;
    let child = corrupt_node
        .log
        .put_object(source.cursor(), Bytes::from_static(b"child"))
        .await?;
    let node = corrupt_node
        .log
        .put_node(source.cursor(), Bytes::from_static(b"node"), vec![child])
        .await?;
    let prepared = corrupt_node.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![node.clone()],
    )?;
    let CommitStatus::Committed(live_view) = corrupt_node.log.commit(prepared).await? else {
        return Err("corrupt node reference did not commit".into());
    };
    corrupt_node
        .raw
        .put(
            &corrupt_node.object_path(node.reference().digest()).await?,
            Bytes::from(vec![0; usize::try_from(node.reference().len())?]).into(),
        )
        .await?;
    corrupt_node.store.reset();
    assert!(matches!(
        corrupt_node.log.start_collection(&live_view).await,
        Err(Error::CorruptObject)
    ));
    assert_collection_not_started(&corrupt_node);
    Ok(())
}

#[tokio::test]
async fn invalid_collection_plans_fail_before_delete_or_clear() -> TestResult {
    let corrupt = Fixture::new("corrupt-plan", Options::default()).await?;
    let source = corrupt.log.load().await?;
    corrupt
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&corrupt.log, &source).await?;
    let path = corrupt.segment_path("collection-plans").await?;
    let len = corrupt.raw.head(&path).await?.size;
    corrupt
        .raw
        .put(&path, Bytes::from(vec![0; usize::try_from(len)?]).into())
        .await?;
    corrupt.store.reset();
    assert!(matches!(
        corrupt.log.resume_collection(&fenced).await,
        Err(Error::CorruptObject)
    ));
    assert_no_collection_mutation(&corrupt);

    let oversized = Fixture::new("oversized-plan", Options::default()).await?;
    let source = oversized.log.load().await?;
    oversized
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&oversized.log, &source).await?;
    let path = oversized.segment_path("collection-plans").await?;
    let len = oversized.raw.head(&path).await?.size;
    oversized
        .raw
        .put(
            &path,
            Bytes::from(vec![
                0;
                usize::try_from(len.checked_add(1).ok_or("overflow")?)?
            ])
            .into(),
        )
        .await?;
    oversized.store.reset();
    assert!(matches!(
        oversized.log.resume_collection(&fenced).await,
        Err(Error::LimitExceeded("read bytes"))
    ));
    assert_no_collection_mutation(&oversized);
    Ok(())
}

#[tokio::test]
async fn resume_propagates_a_read_failure_before_any_delete() -> TestResult {
    let fixture = Fixture::new("resume-read-failure", Options::default()).await?;
    let source = fixture.log.load().await?;
    fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&fixture.log, &source).await?;
    fixture.store.reset();
    fixture
        .store
        .fail_next(Operation::Get, FailurePhase::Before);

    let error = fixture
        .log
        .resume_collection(&fenced)
        .await
        .err()
        .ok_or("head read failure returned a collection status")?;
    assert!(matches!(&error, Error::Store(error) if FaultStore::is_injected(error)));
    assert_no_collection_mutation(&fixture);
    Ok(())
}

#[tokio::test]
async fn unknown_entries_count_toward_the_scan_bound_and_survive() -> TestResult {
    let fixture = Fixture::new(
        "unknown-scan",
        Options {
            max_collection_objects: 1,
            ..Options::default()
        },
    )
    .await?;
    let view = fixture.log.load().await?;
    let unknown = fixture.scope().join("unknown");
    fixture
        .raw
        .put(&unknown, Bytes::from_static(b"keep").into())
        .await?;
    fixture.store.reset();

    assert!(matches!(
        fixture.log.start_collection(&view).await,
        Err(Error::LimitExceeded("collection scan objects"))
    ));
    assert_eq!(
        fixture.store.metrics().operation(Operation::List).requests,
        1
    );
    assert_no_collection_mutation(&fixture);
    assert_eq!(
        fixture.raw.get(&unknown).await?.bytes().await?,
        b"keep".as_slice()
    );
    Ok(())
}

#[tokio::test]
async fn partial_delete_failures_leave_the_plan_and_repeat_the_complete_set() -> TestResult {
    for (name, phase) in [
        ("delete-before", FailurePhase::Before),
        ("delete-after", FailurePhase::After),
    ] {
        let fixture = Fixture::new(name, Options::default()).await?;
        let source = fixture.log.load().await?;
        fixture
            .log
            .put_object(source.cursor(), Bytes::from_static(b"orphan-one"))
            .await?;
        fixture
            .log
            .put_object(source.cursor(), Bytes::from_static(b"orphan-two"))
            .await?;
        let fenced = install_collection(&fixture.log, &source).await?;
        fixture.store.reset();
        fixture.store.schedule(Failure {
            operation: Operation::Delete,
            occurrence: 1,
            phase,
        });

        let CollectionFinish::Pending(report) = fixture.log.resume_collection(&fenced).await?
        else {
            return Err(format!("{name}: delete fault was not pending").into());
        };
        assert_eq!(report.delete_attempts(), 2);
        assert_eq!(fixture.log.load().await?.collection_epoch(), 1);

        fixture.store.reset();
        let CollectionFinish::Complete(_, report) = fixture.log.resume_collection(&fenced).await?
        else {
            return Err(format!("{name}: retry did not complete").into());
        };
        assert_eq!(report.delete_attempts(), 2);
        assert_eq!(
            fixture
                .store
                .metrics()
                .operation(Operation::Delete)
                .requests,
            3
        );
    }
    Ok(())
}

#[tokio::test]
async fn append_after_resume_loads_the_fence_wins_the_clear_cas() -> TestResult {
    let fixture = Fixture::new("append-during-resume", Options::default()).await?;
    let source = fixture.log.load().await?;
    fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let fenced = install_collection(&fixture.log, &source).await?;
    fixture.store.reset();
    let mut pause = fixture.store.pause_next_delete(FailurePhase::Before);
    let collector = tokio::spawn({
        let log = fixture.log.clone();
        let fenced = fenced.clone();
        async move { log.resume_collection(&fenced).await }
    });
    assert!(pause.wait_until_entered().await);
    let appended = append(&fixture.log, &fenced, b"append").await?;
    assert!(pause.release());
    let CollectionFinish::Conflict(current, report) = collector.await?? else {
        return Err("stale collector cleared a newer head".into());
    };
    assert_eq!(current.cursor().tip(), appended.cursor().tip());
    assert_eq!(report.delete_attempts(), 1);
    assert!(matches!(
        fixture.log.resume_collection(&current).await?,
        CollectionFinish::Complete(_, _)
    ));
    Ok(())
}

#[tokio::test]
async fn stale_retention_no_ops_are_resolved_from_the_current_head() -> TestResult {
    let fixture = Fixture::new("stale-retention", Options::default()).await?;
    let source = fixture.log.load().await?;
    let id = RetentionId::new();
    let RetentionStatus::Applied(retained) = fixture.log.retain(&source, id).await? else {
        return Err("retention did not apply".into());
    };
    let RetentionStatus::Applied(released) = fixture.log.release_retention(&retained, id).await?
    else {
        return Err("retention did not release".into());
    };

    assert!(matches!(
        fixture.log.retain(&retained, id).await?,
        RetentionStatus::Conflict(current)
            if current.cursor().generation() == released.cursor().generation()
    ));
    assert!(matches!(
        fixture.log.release_retention(&source, id).await?,
        RetentionStatus::Applied(current)
            if current.cursor().generation() == released.cursor().generation()
    ));
    Ok(())
}

#[tokio::test]
async fn cancellation_before_and_after_fence_installation_is_restart_safe() -> TestResult {
    let before = Fixture::new("cancel-before-fence", Options::default()).await?;
    let source = before.log.load().await?;
    before
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    before.store.reset();
    let mut pause = before.store.pause_next_put(FailurePhase::Before);
    let task = tokio::spawn({
        let log = before.log.clone();
        let source = source.clone();
        async move { log.start_collection(&source).await }
    });
    assert!(pause.wait_until_entered().await);
    task.abort();
    assert!(task.await.is_err());
    assert!(!pause.release());
    assert_eq!(before.log.load().await?.collection_epoch(), 0);

    let after = Fixture::new("cancel-after-fence", Options::default()).await?;
    let source = after.log.load().await?;
    after
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    after.store.reset();
    let mut head_pause = after.store.pause_put_at(2, FailurePhase::After);
    let task = tokio::spawn({
        let log = after.log.clone();
        let source = source.clone();
        async move { log.start_collection(&source).await }
    });
    assert!(head_pause.wait_until_entered().await);
    task.abort();
    assert!(task.await.is_err());
    assert!(!head_pause.release());
    let fenced = after.log.load().await?;
    assert_eq!(fenced.collection_epoch(), 1);
    assert!(matches!(
        after.log.resume_collection(&fenced).await?,
        CollectionFinish::Complete(_, _)
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_deletes_repeat_safely_before_and_after_visibility() -> TestResult {
    for (name, phase) in [
        ("cancel-delete-before", FailurePhase::Before),
        ("cancel-delete-after", FailurePhase::After),
    ] {
        let fixture = Fixture::new(name, Options::default()).await?;
        let source = fixture.log.load().await?;
        fixture
            .log
            .put_object(source.cursor(), Bytes::from_static(b"orphan"))
            .await?;
        let fenced = install_collection(&fixture.log, &source).await?;
        fixture.store.reset();
        let mut pause = fixture.store.pause_next_delete(phase);
        let task = tokio::spawn({
            let log = fixture.log.clone();
            let fenced = fenced.clone();
            async move { log.resume_collection(&fenced).await }
        });
        assert!(pause.wait_until_entered().await);
        task.abort();
        assert!(task.await.is_err());
        assert!(!pause.release());
        let current = fixture.log.load().await?;
        assert_eq!(current.collection_epoch(), 1);
        assert!(matches!(
            fixture.log.resume_collection(&current).await?,
            CollectionFinish::Complete(_, _)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn two_collectors_clear_only_the_exact_plan_and_delayed_delete_is_isolated() -> TestResult {
    let fixture = Fixture::new("delayed-delete", Options::default()).await?;
    let source = fixture.log.load().await?;
    let old = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"same content"))
        .await?;
    let fenced = install_collection(&fixture.log, &source).await?;
    fixture.store.reset();
    let mut pause = fixture.store.pause_next_delete(FailurePhase::Before);
    let delayed = tokio::spawn({
        let log = fixture.log.clone();
        let fenced = fenced.clone();
        async move { log.resume_collection(&fenced).await }
    });
    assert!(pause.wait_until_entered().await);

    let CollectionFinish::Complete(cleared, _) = fixture.log.resume_collection(&fenced).await?
    else {
        return Err("second collector did not clear the fence".into());
    };
    let new = fixture
        .log
        .put_object(cleared.cursor(), Bytes::from_static(b"same content"))
        .await?;
    assert_ne!(old.reference(), new.reference());
    assert_eq!(old.reference().digest(), new.reference().digest());
    assert!(pause.release());
    assert!(matches!(delayed.await??, CollectionFinish::Complete(_, _)));
    assert_eq!(
        fixture.log.read_object(&cleared, new.reference()).await?,
        Bytes::from_static(b"same content")
    );
    Ok(())
}

#[tokio::test]
async fn released_then_reacquired_id_does_not_make_an_old_view_retained() -> TestResult {
    let fixture = Fixture::new("retention-reacquire", Options::default()).await?;
    let source = fixture.log.load().await?;
    let object = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"orphan"))
        .await?;
    let id = RetentionId::new();
    let RetentionStatus::Applied(retained) = fixture.log.retain(&source, id).await? else {
        return Err("retention did not apply".into());
    };
    let RetentionStatus::Applied(released) = fixture.log.release_retention(&retained, id).await?
    else {
        return Err("retention did not release".into());
    };
    let fenced = install_collection(&fixture.log, &released).await?;
    let CollectionFinish::Complete(cleared, _) = fixture.log.resume_collection(&fenced).await?
    else {
        return Err("collection did not complete".into());
    };
    let RetentionStatus::Applied(_reacquired) = fixture.log.retain(&cleared, id).await? else {
        return Err("retention ID was not reacquired".into());
    };

    assert!(matches!(
        fixture.log.read_object(&retained, object.reference()).await,
        Err(Error::ViewExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn a_missing_object_in_a_retained_view_is_corruption() -> TestResult {
    let fixture = Fixture::new("retained-corruption", Options::default()).await?;
    let source = fixture.log.load().await?;
    let object = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"retained"))
        .await?;
    let RetentionStatus::Applied(retained) =
        fixture.log.retain(&source, RetentionId::new()).await?
    else {
        return Err("retention did not apply".into());
    };
    fixture
        .raw
        .delete(&fixture.object_path(object.reference().digest()).await?)
        .await?;

    assert!(matches!(
        fixture.log.read_object(&retained, object.reference()).await,
        Err(Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
async fn compacted_commit_and_checkpoint_reads_expire_with_their_views() -> TestResult {
    let fixture = Fixture::new("expired-history", Options::default()).await?;
    let one = append(&fixture.log, &fixture.log.load().await?, b"one").await?;
    let through_one = one.tail()[0].clone();
    let CheckpointStatus::Published(first_checkpoint) = fixture
        .log
        .publish_checkpoint(&one, &through_one, Bytes::from_static(b"first"), Vec::new())
        .await?
    else {
        return Err("first checkpoint did not publish".into());
    };
    let two = append(&fixture.log, &first_checkpoint, b"two").await?;
    let through_two = two.tail()[0].clone();
    let CheckpointStatus::Published(second_checkpoint) = fixture
        .log
        .publish_checkpoint(
            &two,
            &through_two,
            Bytes::from_static(b"second"),
            Vec::new(),
        )
        .await?
    else {
        return Err("second checkpoint did not publish".into());
    };
    let fenced = install_collection(&fixture.log, &second_checkpoint).await?;
    let CollectionFinish::Complete(_, _) = fixture.log.resume_collection(&fenced).await? else {
        return Err("history collection did not finish".into());
    };

    assert!(matches!(
        fixture.log.read_tail(&one).await,
        Err(Error::ViewExpired)
    ));
    assert!(matches!(
        fixture.log.read_checkpoint(&first_checkpoint).await,
        Err(Error::ViewExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn per_record_reference_limit_is_not_a_whole_graph_limit() -> TestResult {
    let fixture = Fixture::new(
        "per-record-roots",
        Options {
            max_object_refs: 1,
            ..Options::default()
        },
    )
    .await?;
    let source = fixture.log.load().await?;
    let first_object = fixture
        .log
        .put_object(source.cursor(), Bytes::from_static(b"first"))
        .await?;
    let prepared = fixture.log.prepare(
        source.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"first"),
        Bytes::new(),
        vec![first_object],
    )?;
    let CommitStatus::Committed(first) = fixture.log.commit(prepared).await? else {
        return Err("first commit failed".into());
    };
    let second_object = fixture
        .log
        .put_object(first.cursor(), Bytes::from_static(b"second"))
        .await?;
    let prepared = fixture.log.prepare(
        first.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"second"),
        Bytes::new(),
        vec![second_object],
    )?;
    let CommitStatus::Committed(second) = fixture.log.commit(prepared).await? else {
        return Err("second commit failed".into());
    };

    assert!(matches!(
        fixture.log.start_collection(&second).await?,
        CollectionStart::Empty(_)
    ));
    Ok(())
}
