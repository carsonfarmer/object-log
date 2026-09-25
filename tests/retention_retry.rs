#![cfg(feature = "test-util")]

use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
use object_log::sim::{FailurePhase, FaultStore, Operation};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Error, Log, LogId, Options,
    RetentionId, RetentionStatus, TransactionId, ValidatedBackend, View,
};
use object_store::memory::InMemory;
use object_store::path::Path;

type TestResult = Result<(), Box<dyn StdError>>;

async fn open(name: &str) -> Result<(FaultStore, Log), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("retention-retry")).await?;
    let log = Log::open(&backend, &LogId::new(name)?, Options::default()).await?;
    Ok((store, log))
}

async fn append(log: &Log, view: &View) -> Result<View, Error> {
    let prepared = log.prepare(
        view,
        TransactionId::new(),
        Bytes::from_static(b"append"),
        Bytes::new(),
        Vec::new(),
    )?;
    match log.commit(prepared).await? {
        CommitStatus::Committed(next) => Ok(next),
        _ => Err(Error::InvalidFormat("test append did not commit".into())),
    }
}

#[tokio::test]
async fn acquisition_retries_commits_and_protects_original_view() -> TestResult {
    let (store, log) = open("commit-races").await?;
    let initial = log.load().await?;
    let old = log
        .put_object(&initial, Bytes::from_static(b"old view object"))
        .await?;
    let old_ref = old.reference().clone();
    let prepared = log.prepare(
        &initial,
        TransactionId::new(),
        Bytes::from_static(b"first"),
        Bytes::new(),
        vec![old],
    )?;
    let CommitStatus::Committed(source) = log.commit(prepared).await? else {
        return Err("initial commit did not publish".into());
    };
    store.reset();
    let mut pause = Some(store.pause_next_put(FailurePhase::Before));
    let id = RetentionId::new();
    let acquiring = tokio::spawn({
        let log = log.clone();
        let source = source.clone();
        async move { log.retain(&source, id).await }
    });

    let mut current = source.clone();
    for race in 0..2 {
        let mut stop = pause.take().ok_or("acquisition pause was not scheduled")?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), stop.wait_until_entered())
                .await?
        );
        current = append(&log, &current).await?;
        pause = (race == 0).then(|| store.pause_next_put(FailurePhase::Before));
        assert!(stop.release());
    }

    let RetentionStatus::Applied(retained) = acquiring.await?? else {
        return Err("retention did not survive commit races".into());
    };
    assert_eq!(retained.tail(), current.tail());
    assert_eq!(retained.collection_epoch(), source.collection_epoch());
    let CheckpointStatus::Published(checkpointed) = log
        .publish_checkpoint(&retained, &source.tail()[0], Bytes::new(), Vec::new())
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    assert!(matches!(
        log.start_collection(&checkpointed).await?,
        CollectionStart::Retained(_)
    ));
    assert_eq!(
        log.read_object(&source, &old_ref).await?,
        Bytes::from_static(b"old view object")
    );
    Ok(())
}

#[tokio::test]
async fn acquisition_does_not_cross_a_collection_fence() -> TestResult {
    let (store, log) = open("collection-race").await?;
    let source = log.load().await?;
    let old = log
        .put_object(&source, Bytes::from_static(b"collection candidate"))
        .await?;
    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let id = RetentionId::new();
    let acquiring = tokio::spawn({
        let log = log.clone();
        let source = source.clone();
        async move { log.retain(&source, id).await }
    });

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pause.wait_until_entered()
        )
        .await?
    );
    let CollectionStart::Installed(fenced, _) = log.start_collection(&source).await? else {
        return Err("collection plan was not installed".into());
    };
    assert!(pause.release());
    assert!(matches!(
        acquiring.await??,
        RetentionStatus::Conflict(current) | RetentionStatus::ActiveCollection(current)
            if current.collection_epoch() == fenced.collection_epoch()
    ));
    assert!(matches!(
        log.retain(&fenced, id).await?,
        RetentionStatus::ActiveCollection(_)
    ));
    let CollectionFinish::Complete(cleared, _) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish".into());
    };
    assert!(cleared.collection_epoch() > source.collection_epoch());
    assert!(matches!(
        log.read_object(&source, old.reference()).await,
        Err(Error::ViewExpired)
    ));
    Ok(())
}

#[tokio::test]
async fn acquisition_rejects_an_installed_and_cleared_collection_epoch() -> TestResult {
    let (store, log) = open("cleared-collection-race").await?;
    let source = log.load().await?;
    log.put_object(&source, Bytes::from_static(b"orphan"))
        .await?;
    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let acquiring = tokio::spawn({
        let log = log.clone();
        let source = source.clone();
        async move { log.retain(&source, RetentionId::new()).await }
    });

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pause.wait_until_entered()
        )
        .await?
    );
    let CollectionStart::Installed(fenced, _) = log.start_collection(&source).await? else {
        return Err("collection plan was not installed".into());
    };
    let CollectionFinish::Complete(cleared, _) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish".into());
    };
    assert!(pause.release());
    assert!(matches!(
        acquiring.await??,
        RetentionStatus::Conflict(current)
            if current.collection_epoch() == cleared.collection_epoch()
    ));
    assert!(cleared.collection_epoch() > source.collection_epoch());
    Ok(())
}

#[tokio::test]
async fn acquisition_contended_through_retry_limit_returns_conflict() -> TestResult {
    let (store, log) = open("bounded-contention").await?;
    let source = log.load().await?;
    store.reset();
    let mut pause = Some(store.pause_next_put(FailurePhase::Before));
    let acquiring = tokio::spawn({
        let log = log.clone();
        let source = source.clone();
        async move { log.retain(&source, RetentionId::new()).await }
    });

    let mut current = source;
    for race in 0..16 {
        let mut stop = pause.take().ok_or("acquisition pause was not scheduled")?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), stop.wait_until_entered())
                .await?
        );
        current = append(&log, &current).await?;
        pause = (race < 15).then(|| store.pause_next_put(FailurePhase::Before));
        assert!(stop.release());
    }
    assert!(matches!(
        acquiring.await??,
        RetentionStatus::Conflict(view) if view.generation() == current.generation()
    ));
    // One immutable commit and one head update per writer, plus 16 rejected
    // acquisition attempts. The acquire path does not retry indefinitely.
    assert_eq!(store.metrics().operation(Operation::Put).requests, 48);
    Ok(())
}

#[tokio::test]
async fn hidden_success_after_a_retry_remains_pending_until_resolved() -> TestResult {
    let (store, log) = open("retry-hidden-success").await?;
    let source = log.load().await?;
    store.reset();
    let mut pause = store.pause_next_put(FailurePhase::Before);
    let id = RetentionId::new();
    let acquiring = tokio::spawn({
        let log = log.clone();
        let source = source.clone();
        async move { log.retain(&source, id).await }
    });

    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pause.wait_until_entered()
        )
        .await?
    );
    let appended = append(&log, &source).await?;
    store.fail_next(Operation::Put, FailurePhase::After);
    assert!(pause.release());
    assert!(matches!(acquiring.await??, RetentionStatus::Pending));

    let current = log.load().await?;
    assert!(current.generation() > appended.generation());
    assert!(matches!(
        log.retain(&current, id).await?,
        RetentionStatus::Applied(view) if view.generation() == current.generation()
    ));
    assert!(matches!(
        log.start_collection(&current).await?,
        CollectionStart::Retained(_)
    ));
    Ok(())
}

#[tokio::test]
async fn reused_id_cannot_protect_a_view_from_an_earlier_epoch() -> TestResult {
    let (_store, log) = open("reused-id").await?;
    let source = log.load().await?;
    let old = log.put_object(&source, Bytes::from_static(b"old")).await?;
    let id = RetentionId::new();
    let RetentionStatus::Applied(retained) = log.retain(&source, id).await? else {
        return Err("initial retention failed".into());
    };
    let RetentionStatus::Applied(released) = log.release_retention(&retained, id).await? else {
        return Err("release failed".into());
    };
    let CollectionStart::Installed(fenced, _) = log.start_collection(&released).await? else {
        return Err("collection did not install".into());
    };
    let CollectionFinish::Complete(cleared, _) = log.resume_collection(&fenced).await? else {
        return Err("collection did not finish".into());
    };
    let RetentionStatus::Applied(new_retention) = log.retain(&cleared, id).await? else {
        return Err("ID reuse failed".into());
    };

    assert!(matches!(
        log.retain(&retained, id).await?,
        RetentionStatus::Conflict(current)
            if current.generation() == new_retention.generation()
    ));
    assert!(matches!(
        log.read_object(&retained, old.reference()).await,
        Err(Error::ViewExpired)
    ));
    Ok(())
}
