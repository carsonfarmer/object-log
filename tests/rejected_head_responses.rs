#![cfg(feature = "test-util")]

use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{FaultStore, Operation, PutRejection, RequestOutcome};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options,
    RetentionId, RetentionStatus, TransactionId, ValidatedBackend,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};

type TestResult = Result<(), Box<dyn StdError>>;

async fn open(id: &str) -> Result<(FaultStore, Log), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(
        Arc::new(store.clone()),
        Path::from("rejected-head-responses"),
    )
    .await?;
    let log = Log::open(&backend, &LogId::new(id)?, Options::default()).await?;
    Ok((store, log))
}

fn rejections() -> [PutRejection; 2] {
    [PutRejection::Precondition, PutRejection::AlreadyExists]
}

#[tokio::test]
async fn commit_recognizes_applied_then_rejected_head_update() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("commit-{rejection:?}")).await?;
        let source = log.load().await?;
        let prepared = log.prepare(
            &source,
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        store.reset();
        store.reject_put_after(2, rejection); // Commit object, then head.
        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err(format!("commit lost applied {rejection:?} update").into());
        };
        assert_eq!(committed.tail(), log.load().await?.tail());
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_recognizes_applied_then_rejected_head_update() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("checkpoint-{rejection:?}")).await?;
        let source = log.load().await?;
        let prepared = log.prepare(
            &source,
            TransactionId::new(),
            Bytes::from_static(b"operation"),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
            return Err("setup commit failed".into());
        };
        let through = committed.tail()[0].clone();
        store.reset();
        store.reject_put_after(2, rejection); // Checkpoint object, then head.
        let CheckpointStatus::Published(published) = log
            .publish_checkpoint(
                &committed,
                &through,
                Bytes::from_static(b"snapshot"),
                Vec::new(),
            )
            .await?
        else {
            return Err(format!("checkpoint lost applied {rejection:?} update").into());
        };
        assert_eq!(published.checkpoint(), log.load().await?.checkpoint());
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);
    }
    Ok(())
}

#[tokio::test]
async fn retention_recognizes_applied_then_rejected_head_update() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("retention-{rejection:?}")).await?;
        let source = log.load().await?;
        let id = RetentionId::new();
        store.reset();
        store.reject_put_after(1, rejection);
        let RetentionStatus::Applied(retained) = log.retain(&source, id).await? else {
            return Err(format!("retention lost applied {rejection:?} update").into());
        };
        assert_eq!(retained.generation(), log.load().await?.generation());
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);

        store.reset();
        store.reject_put_after(1, rejection);
        let RetentionStatus::Applied(released) = log.release_retention(&retained, id).await? else {
            return Err(format!("release lost applied {rejection:?} update").into());
        };
        assert_eq!(released.generation(), log.load().await?.generation());
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);
    }
    Ok(())
}

#[tokio::test]
async fn collection_install_preserves_applied_plan_after_rejected_response() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("collection-install-{rejection:?}")).await?;
        let source = log.load().await?;
        log.put_object(&source, Bytes::from_static(b"orphan"))
            .await?;
        store.reset();
        store.reject_put_after(2, rejection); // Plan object, then head.
        let CollectionStart::Installed(fenced, report) = log.start_collection(&source).await?
        else {
            return Err(format!("collection lost applied {rejection:?} fence").into());
        };
        assert_eq!(report.candidate_count(), 1);
        assert_eq!(fenced.collection_epoch(), 1);
        assert_eq!(fenced.generation(), log.load().await?.generation());
        assert_eq!(store.metrics().operation(Operation::Delete).requests, 0);
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);

        // The durable plan must still be readable by every fenced operation.
        log.put_object(&fenced, Bytes::from_static(b"new blob"))
            .await?;
        assert!(matches!(
            log.resume_collection(&fenced).await?,
            CollectionFinish::Complete(_, _)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn collection_clear_recognizes_applied_then_rejected_head_update() -> TestResult {
    for rejection in rejections() {
        let (store, log) = open(&format!("collection-clear-{rejection:?}")).await?;
        let source = log.load().await?;
        log.put_object(&source, Bytes::from_static(b"orphan"))
            .await?;
        let CollectionStart::Installed(fenced, _) = log.start_collection(&source).await? else {
            return Err("setup collection failed".into());
        };
        store.reset();
        store.reject_put_after(1, rejection);
        let CollectionFinish::Complete(cleared, report) = log.resume_collection(&fenced).await?
        else {
            return Err(format!("collection clear lost applied {rejection:?} update").into());
        };
        assert_eq!(report.delete_attempts(), 1);
        assert_eq!(cleared.generation(), log.load().await?.generation());
        assert_eq!(store.metrics().operation(Operation::Put).injected_after, 1);
        assert!(matches!(
            log.start_collection(&cleared).await?,
            CollectionStart::Empty(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn rejected_put_fault_records_visible_mutation() -> TestResult {
    for rejection in rejections() {
        let store = FaultStore::new(InMemory::new());
        let path = Path::from("head");
        let initial = store.put(&path, Bytes::from_static(b"old").into()).await?;
        store.reset();
        store.reject_put_after(1, rejection);
        let error = store
            .put_opts(
                &path,
                Bytes::from_static(b"new").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion::from(initial)),
                    ..PutOptions::default()
                },
            )
            .await
            .err()
            .ok_or("rejected PUT unexpectedly succeeded")?;
        assert!(matches!(
            (&error, rejection),
            (
                object_store::Error::Precondition { .. },
                PutRejection::Precondition
            ) | (
                object_store::Error::AlreadyExists { .. },
                PutRejection::AlreadyExists
            )
        ));
        assert!(FaultStore::is_injected(&error));
        assert_eq!(
            store.get(&path).await?.bytes().await?,
            Bytes::from_static(b"new")
        );
        let put = store.metrics().operation(Operation::Put);
        assert_eq!(put.visible_mutations, 1);
        assert_eq!(put.injected_after, 1);
        assert!(store.metrics().events.iter().any(|event| {
            event.operation == Operation::Put && event.outcome == RequestOutcome::InjectedAfter
        }));
    }
    Ok(())
}
