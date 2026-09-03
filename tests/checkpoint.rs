use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
#[cfg(feature = "test-util")]
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
#[cfg(feature = "test-util")]
use object_log::{CheckpointResolution, PendingCommit};
use object_log::{
    CheckpointStatus, CommitStatus, Digest, Log, LogId, Options, Resolution, TransactionId,
    ValidatedBackend, View,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

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
        .publish_checkpoint(
            &third,
            &through,
            Bytes::from_static(b"state after first"),
            Vec::new(),
        )
        .await?
    else {
        return Err("checkpoint publication returned a conflict".into());
    };

    let reference = compacted
        .checkpoint()
        .ok_or("published view has no checkpoint")?;
    assert_eq!(reference.through_sequence(), through.sequence());
    assert_eq!(reference.through_commit(), through.digest());
    assert_eq!(compacted.tail().len(), 2);
    assert_eq!(compacted.tail()[0].sequence(), 1);
    assert_eq!(compacted.tail()[1].sequence(), 2);
    let checkpoint = log
        .read_checkpoint(&compacted)
        .await?
        .ok_or("published checkpoint is missing")?;
    assert_eq!(checkpoint.snapshot(), b"state after first".as_slice());
    assert!(checkpoint.objects().is_empty());
    let tail = log.read_tail(&compacted).await?;
    assert_eq!(tail[0].expected_tip(), Some(through.digest()));
    assert_eq!(tail[0].operation(), &Bytes::from_static(b"second"));
    assert_eq!(tail[1].operation(), &Bytes::from_static(b"third"));
    Ok(())
}

#[tokio::test]
async fn append_before_checkpoint_preserves_both_entries() -> TestResult {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(
        Arc::clone(&backend),
        "append-before-checkpoint",
        Options::default(),
    )
    .await?;
    let second = open(backend, "append-before-checkpoint", Options::default()).await?;
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

    let CommitStatus::Committed(appended) = second.commit(append_candidate).await? else {
        return Err("append did not publish".into());
    };
    let CheckpointStatus::Conflict(current) = first
        .publish_checkpoint(&one, &through, Bytes::from_static(b"one-state"), Vec::new())
        .await?
    else {
        return Err("stale checkpoint did not conflict".into());
    };
    assert_eq!(current.tail(), appended.tail());
    assert_eq!(first.read_tail(&current).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn checkpoint_before_append_preserves_the_base() -> TestResult {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = open(
        Arc::clone(&backend),
        "checkpoint-before-append",
        Options::default(),
    )
    .await?;
    let second = open(backend, "checkpoint-before-append", Options::default()).await?;
    let one = append(&first, &first.load().await?, b"one").await?;
    let through = one.tail()[0].clone();
    let append_candidate = second.prepare(
        one.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"two"),
        Bytes::new(),
        Vec::new(),
    )?;

    let CheckpointStatus::Published(checkpointed) = first
        .publish_checkpoint(&one, &through, Bytes::from_static(b"one-state"), Vec::new())
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let CommitStatus::Conflict(current) = second.commit(append_candidate).await? else {
        return Err("stale append did not conflict".into());
    };
    assert_eq!(current.checkpoint(), checkpointed.checkpoint());
    assert!(current.tail().is_empty());
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
        .publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"first base"),
            Vec::new(),
        )
        .await?
    else {
        return Err("first checkpoint did not publish".into());
    };
    let CheckpointStatus::Conflict(current) = log
        .publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"stale base"),
            Vec::new(),
        )
        .await?
    else {
        return Err("stale checkpoint did not conflict".into());
    };
    assert_eq!(
        current.cursor().generation(),
        published.cursor().generation()
    );
    assert_eq!(
        current.checkpoint().map(|base| base.object().digest()),
        published.checkpoint().map(|base| base.object().digest())
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
        log.publish_checkpoint(&one, &through, Bytes::from_static(b"too large"), Vec::new(),)
            .await,
        Err(object_log::Error::LimitExceeded("encoded checkpoint bytes"))
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_none());
    assert_eq!(current.tail(), one.tail());
    Ok(())
}

#[tokio::test]
async fn checkpoint_root_limit_fails_before_index_publication() -> TestResult {
    let log = open(
        Arc::new(InMemory::new()),
        "checkpoint-root-limit",
        Options {
            max_object_refs: 0,
            ..Options::default()
        },
    )
    .await?;
    let initial = log.load().await?;
    let object = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    let one = append(&log, &initial, b"one").await?;
    let through = one.tail()[0].clone();

    assert!(matches!(
        log.publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"page map"),
            vec![object],
        )
        .await,
        Err(object_log::Error::LimitExceeded("object references"))
    ));
    assert!(log.load().await?.checkpoint().is_none());
    Ok(())
}

#[tokio::test]
async fn checkpoint_declares_live_objects_for_lazy_restore() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = backend.clone();
    let log = open(store, "checkpoint-objects", Options::default()).await?;
    let initial = log.load().await?;
    let object = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    let one = append(&log, &initial, b"one").await?;
    let through = one.tail()[0].clone();
    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"page map"),
            vec![object.clone()],
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    backend
        .delete(
            &immutable_location(
                &backend,
                "checkpoint-objects",
                "blobs",
                object.reference().digest(),
            )
            .await?,
        )
        .await?;
    let checkpoint = log
        .read_checkpoint(&compacted)
        .await?
        .ok_or("checkpoint is missing")?;
    assert_eq!(
        checkpoint.objects(),
        std::slice::from_ref(object.reference())
    );
    assert!(matches!(
        log.read_object(&compacted, object.reference()).await,
        Err(object_log::Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
async fn checkpoint_can_root_a_traversable_object_tree() -> TestResult {
    let log = open(
        Arc::new(InMemory::new()),
        "checkpoint-tree",
        Options::default(),
    )
    .await?;
    let initial = log.load().await?;
    let page = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    let node = log
        .put_node(
            initial.cursor(),
            Bytes::from_static(b"page map"),
            vec![page.clone()],
        )
        .await?;
    let same = log
        .put_node(
            initial.cursor(),
            Bytes::from_static(b"page map"),
            vec![page.clone()],
        )
        .await?;
    assert_ne!(same.reference(), node.reference());
    assert_eq!(same.reference().digest(), node.reference().digest());
    let one = append(&log, &initial, b"one").await?;
    let through = one.tail()[0].clone();
    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"root"),
            vec![node.clone()],
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    let checkpoint = log
        .read_checkpoint(&compacted)
        .await?
        .ok_or("checkpoint is missing")?;
    assert_eq!(checkpoint.objects(), std::slice::from_ref(node.reference()));
    let restored = log.read_node(&compacted, node.reference()).await?;
    assert_eq!(restored.payload(), b"page map".as_slice());
    assert_eq!(restored.children(), std::slice::from_ref(page.reference()));
    Ok(())
}

#[tokio::test]
async fn staging_rejects_missing_and_corrupt_existing_objects() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = backend.clone();
    let log = open(store, "invalid-node-child", Options::default()).await?;
    let view = log.load().await?;
    let missing = log
        .put_object(view.cursor(), Bytes::from_static(b"missing"))
        .await?;
    backend
        .delete(
            &immutable_location(
                &backend,
                "invalid-node-child",
                "blobs",
                missing.reference().digest(),
            )
            .await?,
        )
        .await?;
    assert!(matches!(
        log.stage_objects(view.cursor(), vec![missing.reference().clone()])
            .await,
        Err(object_log::Error::InvalidFormat(_))
    ));

    let corrupt = log
        .put_object(view.cursor(), Bytes::from_static(b"correct"))
        .await?;
    backend
        .put(
            &immutable_location(
                &backend,
                "invalid-node-child",
                "blobs",
                corrupt.reference().digest(),
            )
            .await?,
            Bytes::from_static(b"changed").into(),
        )
        .await?;
    assert!(matches!(
        log.stage_objects(view.cursor(), vec![corrupt.reference().clone()])
            .await,
        Err(object_log::Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
async fn checkpoint_staging_rejects_missing_declared_object() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = backend.clone();
    let log = open(store, "checkpoint-missing-object", Options::default()).await?;
    let initial = log.load().await?;
    let object = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    backend
        .delete(
            &immutable_location(
                &backend,
                "checkpoint-missing-object",
                "blobs",
                object.reference().digest(),
            )
            .await?,
        )
        .await?;

    assert!(matches!(
        log.stage_objects(initial.cursor(), vec![object.reference().clone()])
            .await,
        Err(object_log::Error::InvalidFormat(_))
    ));
    assert!(log.load().await?.checkpoint().is_none());
    Ok(())
}

#[tokio::test]
async fn checkpoint_staging_rejects_corrupt_declared_object() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = backend.clone();
    let log = open(store, "checkpoint-corrupt-object", Options::default()).await?;
    let initial = log.load().await?;
    let object = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    backend
        .put(
            &immutable_location(
                &backend,
                "checkpoint-corrupt-object",
                "blobs",
                object.reference().digest(),
            )
            .await?,
            Bytes::from_static(b"bad!").into(),
        )
        .await?;

    assert!(matches!(
        log.stage_objects(initial.cursor(), vec![object.reference().clone()])
            .await,
        Err(object_log::Error::CorruptObject)
    ));
    assert!(log.load().await?.checkpoint().is_none());
    Ok(())
}

#[tokio::test]
async fn checkpoint_read_rejects_missing_and_corrupt_roots() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = backend.clone();
    let log = open(store, "missing-checkpoint", Options::default()).await?;
    let one = append(&log, &log.load().await?, b"one").await?;
    let through = one.tail()[0].clone();
    let CheckpointStatus::Published(compacted) = log
        .publish_checkpoint(&one, &through, Bytes::from_static(b"snapshot"), Vec::new())
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let reference = compacted.checkpoint().ok_or("checkpoint is missing")?;
    let location = immutable_location(
        &backend,
        "missing-checkpoint",
        "checkpoints",
        reference.object().digest(),
    )
    .await?;
    backend.delete(&location).await?;
    assert!(matches!(
        log.read_checkpoint(&compacted).await,
        Err(object_log::Error::CorruptObject)
    ));

    let checkpoint_len = usize::try_from(reference.object().len())?;
    backend
        .put(&location, Bytes::from(vec![0_u8; checkpoint_len]).into())
        .await?;
    assert!(matches!(
        log.read_checkpoint(&compacted).await,
        Err(object_log::Error::CorruptObject)
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn lost_checkpoint_success_resolves_as_published() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-pending-after",
        Options::default(),
    )
    .await?;
    let one = append(&log, &log.load().await?, b"one").await?;
    let through = one.tail()[0].clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });

    let pending = match log
        .publish_checkpoint(&one, &through, Bytes::from_static(b"snapshot"), Vec::new())
        .await?
    {
        CheckpointStatus::Pending(pending) => pending,
        CheckpointStatus::Published(_) | CheckpointStatus::Conflict(_) => {
            return Err("lost checkpoint response did not remain pending".into());
        }
    };
    assert!(matches!(
        log.resolve_checkpoint(pending).await?,
        CheckpointResolution::Published(_)
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn reopened_checkpoint_resolution_rejects_a_lost_root() -> TestResult {
    let backend = Arc::new(InMemory::new());
    let faults = FaultStore::new(backend.clone());
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-pending-lost-root",
        Options::default(),
    )
    .await?;
    let initial = log.load().await?;
    let object = log
        .put_object(initial.cursor(), Bytes::from_static(b"page"))
        .await?;
    let one = append(&log, &initial, b"one").await?;
    let through = one.tail()[0].clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let pending = match log
        .publish_checkpoint(
            &one,
            &through,
            Bytes::from_static(b"page map"),
            vec![object.clone()],
        )
        .await?
    {
        CheckpointStatus::Pending(pending) => pending,
        CheckpointStatus::Published(_) | CheckpointStatus::Conflict(_) => {
            return Err("lost checkpoint response did not remain pending".into());
        }
    };
    faults.reset();
    assert!(matches!(
        log.resolve_checkpoint(pending.clone()).await?,
        CheckpointResolution::Published(_)
    ));
    assert_eq!(segment_gets(&faults, "blobs"), 0);
    assert_eq!(segment_gets(&faults, "checkpoints"), 0);
    backend
        .delete(
            &immutable_location(
                &backend,
                "checkpoint-pending-lost-root",
                "blobs",
                object.reference().digest(),
            )
            .await?,
        )
        .await?;

    faults.reset();
    let reopened = open(
        Arc::new(faults),
        "checkpoint-pending-lost-root",
        Options::default(),
    )
    .await?;
    assert!(matches!(
        reopened.resolve_checkpoint(pending).await,
        Err(object_log::Error::InvalidFormat(_))
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn failed_checkpoint_update_retries_the_exact_prefix() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-pending-before",
        Options::default(),
    )
    .await?;
    let one = append(&log, &log.load().await?, b"one").await?;
    let through = one.tail()[0].clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });

    let pending = match log
        .publish_checkpoint(&one, &through, Bytes::from_static(b"snapshot"), Vec::new())
        .await?
    {
        CheckpointStatus::Pending(pending) => pending,
        CheckpointStatus::Published(_) | CheckpointStatus::Conflict(_) => {
            return Err("failed checkpoint update did not remain pending".into());
        }
    };
    assert!(matches!(
        log.resolve_checkpoint(pending).await?,
        CheckpointResolution::Published(_)
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn superseded_pending_checkpoint_reports_expired_not_conflict() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-superseded",
        Options::default(),
    )
    .await?;
    let one = append(&log, &log.load().await?, b"one").await?;
    let through_one = one.tail()[0].clone();
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let pending = match log
        .publish_checkpoint(
            &one,
            &through_one,
            Bytes::from_static(b"state one"),
            Vec::new(),
        )
        .await?
    {
        CheckpointStatus::Pending(pending) => pending,
        CheckpointStatus::Published(_) | CheckpointStatus::Conflict(_) => {
            return Err("lost checkpoint response did not remain pending".into());
        }
    };

    let checkpointed_one = log.load().await?;
    let two = append(&log, &checkpointed_one, b"two").await?;
    let through_two = two.tail()[0].clone();
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(
            &two,
            &through_two,
            Bytes::from_static(b"state two"),
            Vec::new(),
        )
        .await?
    else {
        return Err("replacement checkpoint did not publish".into());
    };

    assert!(matches!(
        log.resolve_checkpoint(pending).await?,
        CheckpointResolution::Expired(_)
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn checkpoint_retains_a_pending_commit_outcome() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-retains-outcome",
        Options::default(),
    )
    .await?;
    let pending = append_with_lost_response(&log, &faults).await?;
    let committed = log.load().await?;
    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"snapshot"),
            Vec::new(),
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    assert!(matches!(
        log.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
#[cfg(feature = "test-util")]
async fn checkpoint_reports_expired_when_the_durable_window_is_zero() -> TestResult {
    let faults = FaultStore::new(InMemory::new());
    let options = Options {
        resolution_window: 0,
        ..Options::default()
    };
    let log = open(
        Arc::new(faults.clone()),
        "checkpoint-expires-outcome",
        options,
    )
    .await?;
    let pending = append_with_lost_response(&log, &faults).await?;
    let committed = log.load().await?;
    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"snapshot"),
            Vec::new(),
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    assert!(matches!(
        log.resolve(pending).await?,
        Resolution::Expired(_)
    ));
    Ok(())
}

#[tokio::test]
async fn exact_recovery_does_not_report_conflict_after_evidence_expires() -> TestResult {
    let options = Options {
        resolution_window: 0,
        ..Options::default()
    };
    let log = open(Arc::new(InMemory::new()), "expired-duplicate", options).await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        Vec::new(),
    )?;
    let recovery_token = prepared.recovery_token()?;
    let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
        return Err("first candidate did not publish".into());
    };
    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"snapshot"),
            Vec::new(),
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    assert!(matches!(
        log.resume(&recovery_token).await?,
        Resolution::Expired(_)
    ));
    Ok(())
}

#[tokio::test]
async fn checkpoint_rejects_missing_source_history_before_publication() -> TestResult {
    let backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let log = open(
        Arc::clone(&backend),
        "checkpoint-missing-history",
        Options::default(),
    )
    .await?;
    let committed = append(&log, &log.load().await?, b"one").await?;
    let through = committed.tail()[0].clone();
    let location = immutable_location(
        &backend,
        "checkpoint-missing-history",
        "commits",
        through.digest(),
    )
    .await?;
    backend.delete(&location).await?;

    assert!(matches!(
        log.publish_checkpoint(
            &committed,
            &through,
            Bytes::from_static(b"snapshot"),
            Vec::new(),
        )
        .await,
        Err(object_log::Error::CorruptObject)
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_none());
    assert_eq!(current.tail(), committed.tail());
    Ok(())
}

async fn open(
    store: Arc<dyn ObjectStore>,
    id: &str,
    options: Options,
) -> Result<Log, object_log::Error> {
    let log_id = LogId::new(id)?;
    let backend = ValidatedBackend::new(store, Path::from("checkpoint-tests")).await?;
    let scoped = backend.scope(&log_id);
    Log::open(scoped, options).await
}

async fn immutable_location<S: ObjectStore + ?Sized>(
    store: &Arc<S>,
    log_id: &str,
    kind: &str,
    digest: Digest,
) -> Result<Path, Box<dyn StdError>> {
    let prefix = Path::from(format!("checkpoint-tests/v1/logs/{log_id}/data"));
    let kind = format!("/{kind}/");
    let digest = digest.to_string();
    store
        .list(Some(&prefix))
        .try_filter(|object| {
            let path = object.location.to_string();
            std::future::ready(path.contains(&kind) && path.ends_with(&digest))
        })
        .map_ok(|object| object.location)
        .try_next()
        .await?
        .ok_or_else(|| "immutable test object is missing".into())
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

#[cfg(feature = "test-util")]
async fn append_with_lost_response(
    log: &Log,
    faults: &FaultStore,
) -> Result<PendingCommit, Box<dyn StdError>> {
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"pending operation"),
        Bytes::new(),
        Vec::new(),
    )?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    match log.commit(prepared).await? {
        CommitStatus::Pending(pending) => Ok(pending),
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            Err("lost response did not leave pending evidence".into())
        }
    }
}

#[cfg(feature = "test-util")]
fn segment_gets(store: &FaultStore, segment: &str) -> usize {
    let marker = format!("/{segment}/");
    store
        .metrics()
        .events
        .iter()
        .filter(|event| event.operation == Operation::Get && event.path.contains(&marker))
        .count()
}
