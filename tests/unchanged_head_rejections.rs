#![cfg(feature = "test-util")]

use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_log::sim::{FailurePhase, FaultStore, Operation, PutRejection};
use object_log::{
    CheckpointResolution, CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log,
    LogId, Options, Resolution, RetentionId, RetentionStatus, TransactionId, ValidatedBackend,
};
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

async fn open(id: &str) -> Result<(FaultStore, Log), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("unchanged-head")).await?;
    let log = Log::open(&backend, &LogId::new(id)?, Options::default()).await?;
    Ok((store, log))
}

fn rejections() -> [PutRejection; 2] {
    [PutRejection::Precondition, PutRejection::AlreadyExists]
}

#[tokio::test]
async fn rejected_commit_head_is_pending_and_recoverable() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("commit-{rejection:?}")).await?;
        let source = log.load().await?;
        let prepared = log.prepare(
            &source,
            TransactionId::new(),
            Bytes::from_static(b"append"),
            Bytes::new(),
            Vec::new(),
        )?;
        store.reset();
        store.reject_put_before(2, rejection); // Immutable commit, then head.
        let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
            return Err(format!("{rejection:?} did not leave commit pending").into());
        };
        assert_eq!(log.load().await?.generation(), source.generation());
        let put = store.metrics().operation(Operation::Put);
        assert_eq!(put.injected_before, 1);
        assert_eq!(put.visible_mutations, 1);
        let Resolution::Committed(committed) = log.resolve(pending).await? else {
            return Err("pending commit did not recover".into());
        };
        assert_eq!(committed.tail().len(), 1);
        assert_eq!(log.load().await?.tail(), committed.tail());
    }
    Ok(())
}

#[tokio::test]
async fn rejected_checkpoint_head_is_pending_and_recoverable() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("checkpoint-{rejection:?}")).await?;
        let source = log.load().await?;
        let prepared = log.prepare(
            &source,
            TransactionId::new(),
            Bytes::from_static(b"append"),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err("setup commit failed".into());
        };
        store.reset();
        store.reject_put_before(2, rejection); // Immutable checkpoint, then head.
        let CheckpointStatus::Pending(pending) = log
            .publish_checkpoint(
                &committed,
                &committed.tail()[0],
                Bytes::from_static(b"snapshot"),
                Vec::new(),
            )
            .await?
        else {
            return Err(format!("{rejection:?} did not leave checkpoint pending").into());
        };
        assert!(log.load().await?.checkpoint().is_none());
        assert_eq!(store.metrics().operation(Operation::Put).injected_before, 1);
        let CheckpointResolution::Published(published) = log.resolve_checkpoint(pending).await?
        else {
            return Err("pending checkpoint did not recover".into());
        };
        assert_eq!(published.checkpoint(), log.load().await?.checkpoint());
    }
    Ok(())
}

#[tokio::test]
async fn rejected_retention_changes_are_pending_and_retriable() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("retention-{rejection:?}")).await?;
        let source = log.load().await?;
        let id = RetentionId::new();
        store.reset();
        store.reject_put_before(1, rejection);
        assert!(matches!(
            log.retain(&source, id).await?,
            RetentionStatus::Pending
        ));
        assert_eq!(log.load().await?.generation(), source.generation());
        let RetentionStatus::Applied(retained) = log.retain(&source, id).await? else {
            return Err("retention retry did not apply".into());
        };

        store.reset();
        store.reject_put_before(1, rejection);
        assert!(matches!(
            log.release_retention(&retained, id).await?,
            RetentionStatus::Pending
        ));
        assert_eq!(log.load().await?.generation(), retained.generation());
        let RetentionStatus::Applied(released) = log.release_retention(&retained, id).await? else {
            return Err("release retry did not apply".into());
        };

        let RetentionStatus::Applied(retained) = log.retain(&released, id).await? else {
            return Err("second retention did not apply".into());
        };
        store.reset();
        store.reject_put_before(1, rejection);
        assert!(matches!(
            log.clear_retentions_after_drain(&retained).await?,
            RetentionStatus::Pending
        ));
        assert_eq!(log.load().await?.generation(), retained.generation());
        assert!(matches!(
            log.clear_retentions_after_drain(&retained).await?,
            RetentionStatus::Applied(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn commit_reports_conflict_after_sixteen_compatible_retention_races() -> TestResult {
    let (store, log) = open("retention-exhausts-commit-retry").await?;
    let source = log.load().await?;
    let prepared = log.prepare(
        &source,
        TransactionId::new(),
        Bytes::from_static(b"candidate"),
        Bytes::new(),
        Vec::new(),
    )?;
    store.reset();
    let mut pause = Some(store.pause_put_at(2, FailurePhase::Before));
    let writer = tokio::spawn({
        let log = log.clone();
        async move { log.commit(prepared).await }
    });
    for attempt in 0..16 {
        let mut stopped = pause.take().ok_or("a writer pause was not scheduled")?;
        assert!(tokio::time::timeout(Duration::from_secs(5), stopped.wait_until_entered()).await?);
        let current = log.load().await?;
        assert!(matches!(
            log.retain(&current, RetentionId::new()).await?,
            RetentionStatus::Applied(_)
        ));
        let next = (attempt < 15).then(|| {
            let occurrence = store.metrics().operation(Operation::Put).requests + 1;
            store.pause_put_at(occurrence, FailurePhase::Before)
        });
        assert!(stopped.release());
        pause = next;
    }
    let CommitStatus::Conflict(current) = writer.await?? else {
        return Err("exhausted compatible retention races did not return conflict".into());
    };
    assert_eq!(current.generation(), source.generation() + 16);
    assert!(current.tail().is_empty());
    assert_eq!(log.load().await?.generation(), current.generation());
    Ok(())
}

#[tokio::test]
async fn rejected_collection_install_keeps_head_unfenced() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("collection-install-{rejection:?}")).await?;
        let source = log.load().await?;
        log.put_object(&source, Bytes::from_static(b"orphan"))
            .await?;
        store.reset();
        store.reject_put_before(2, rejection); // Plan object, then head.
        assert!(matches!(
            log.start_collection(&source).await?,
            CollectionStart::Pending
        ));
        let current = log.load().await?;
        assert_eq!(current.generation(), source.generation());
        assert_eq!(current.collection_epoch(), source.collection_epoch());
        assert_eq!(store.metrics().operation(Operation::Put).injected_before, 1);
        let CollectionStart::Installed(fenced, _) = log.start_collection(&current).await? else {
            return Err("collection retry did not install a fence".into());
        };
        assert_eq!(fenced.collection_epoch(), source.collection_epoch() + 1);
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            CollectionFinish::Complete(_, _)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn rejected_collection_clear_keeps_plan_active_until_retry() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("collection-clear-{rejection:?}")).await?;
        let source = log.load().await?;
        log.put_object(&source, Bytes::from_static(b"orphan"))
            .await?;
        let CollectionStart::Installed(fenced, _) = log.start_collection(&source).await? else {
            return Err("setup collection failed".into());
        };
        store.reset();
        store.reject_put_before(1, rejection);
        let CollectionFinish::Pending(report) = log.resume_collection(&fenced).await? else {
            return Err(format!("{rejection:?} did not leave collection pending").into());
        };
        assert_eq!(report.delete_attempts(), 1);
        let current = log.load().await?;
        assert_eq!(current.generation(), fenced.generation());
        assert_eq!(current.collection_epoch(), fenced.collection_epoch());
        let CollectionFinish::Complete(cleared, _) = log.resume_collection(&fenced).await? else {
            return Err("collection retry did not clear the fence".into());
        };
        assert_eq!(cleared.generation(), log.load().await?.generation());
    }
    Ok(())
}
