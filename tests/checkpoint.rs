use std::error::Error as StdError;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(feature = "test-util")]
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
#[cfg(feature = "test-util")]
use object_log::{CheckpointResolution, PendingCommit};
use object_log::{
    CheckpointStatus, CommitStatus, Log, LogId, Options, Resolution, ScopedStore, TransactionId,
    View,
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
        .publish_checkpoint(&third, &through, Bytes::from_static(b"state after first"))
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
    assert_eq!(
        log.read_checkpoint(&compacted).await?,
        Some(Bytes::from_static(b"state after first"))
    );
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
        .publish_checkpoint(&one, &through, Bytes::from_static(b"one-state"))
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
        .publish_checkpoint(&one, &through, Bytes::from_static(b"one-state"))
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
        log.publish_checkpoint(&one, &through, Bytes::from_static(b"too large"))
            .await,
        Err(object_log::Error::LimitExceeded("encoded checkpoint bytes"))
    ));
    let current = log.load().await?;
    assert!(current.checkpoint().is_none());
    assert_eq!(current.tail(), one.tail());
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
        .publish_checkpoint(&one, &through, Bytes::from_static(b"snapshot"))
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
        .publish_checkpoint(&one, &through, Bytes::from_static(b"snapshot"))
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
        .publish_checkpoint(&one, &through_one, Bytes::from_static(b"state one"))
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
        .publish_checkpoint(&two, &through_two, Bytes::from_static(b"state two"))
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
        .publish_checkpoint(&committed, &through, Bytes::from_static(b"snapshot"))
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
        .publish_checkpoint(&committed, &through, Bytes::from_static(b"snapshot"))
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
async fn duplicate_commit_does_not_report_conflict_after_evidence_expires() -> TestResult {
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
    let CommitStatus::Committed(committed) = log.commit(prepared.clone()).await? else {
        return Err("first candidate did not publish".into());
    };
    let through = committed.tail()[0].clone();
    let CheckpointStatus::Published(_) = log
        .publish_checkpoint(&committed, &through, Bytes::from_static(b"snapshot"))
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };

    let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
        return Err("expired duplicate was given a definite outcome".into());
    };
    assert!(matches!(
        log.resolve(pending).await?,
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
    let location = Path::from(format!(
        "checkpoint-tests/v1/logs/checkpoint-missing-history/wal/{}.cbor",
        through.digest()
    ));
    backend.delete(&location).await?;

    assert!(matches!(
        log.publish_checkpoint(&committed, &through, Bytes::from_static(b"snapshot"))
            .await,
        Err(object_log::Error::InvalidFormat(_))
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
