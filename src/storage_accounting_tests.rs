use super::*;
use crate::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_store::{memory::InMemory, path::Path};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture(limit: usize) -> TestResult<(Log, ValidatedBackend, FaultStore)> {
    let faults = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from("accounting")).await?;
    let log = Log::open(
        &backend,
        &LogId::new("test")?,
        Options {
            max_collection_objects: limit,
            ..Options::default()
        },
    )
    .await?;
    Ok((log, backend, faults))
}

async fn root(log: &Log, view: &View) -> TestResult<StagedObject> {
    let blob = log.put_object(view, Bytes::from_static(b"leaf")).await?;
    Ok(log.put_node(view, Bytes::new(), vec![blob]).await?)
}

#[tokio::test]
async fn cold_graph_counts_preserve_sparse_reads_and_exact_collection_deduplication() -> TestResult
{
    let (log, backend, faults) = fixture(6).await?;
    let view = log.load().await?;
    let leaf = log.put_object(&view, Bytes::from_static(b"leaf")).await?;
    let left = log
        .put_node(&view, Bytes::new(), vec![leaf.clone()])
        .await?;
    let right = log.put_node(&view, Bytes::new(), vec![leaf]).await?;
    let root = log.put_node(&view, Bytes::new(), vec![left, right]).await?;
    assert_eq!(root.object.subtree_objects, 5);
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root],
    )?;
    faults.reset();
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("commit did not publish".into());
    };
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 0);
    let cold = Log::open_existing(&backend, log.store.log_id(), log.options()).await?;
    let roots = cold.read_tail(&view).await?.remove(0).objects;
    faults.reset();
    let roots = cold.stage_objects(&view, roots).await?;
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 4);
    faults.reset();
    let (_, children) = cold.read_staged_node(&view, &roots[0]).await?;
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    assert_eq!(
        children
            .iter()
            .map(|child| child.object.subtree_objects)
            .collect::<Vec<_>>(),
        [2, 2]
    );
    faults.reset();
    assert_eq!(cold.mark_live(&view).await?.len(), 5);
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 5);
    Ok(())
}

#[tokio::test]
async fn cold_reads_reject_understated_and_overstated_node_counts() -> TestResult {
    let (log, backend, _) = fixture(10).await?;
    let view = log.load().await?;
    let root = root(&log, &view).await?;
    let cold = Log::open_existing(&backend, log.store.log_id(), log.options()).await?;
    for count in [1, 3] {
        let mut reference = root.reference().clone();
        reference.subtree_objects = count;
        assert!(matches!(
            cold.read_node(&view, &reference).await,
            Err(Error::CorruptObject)
        ));
        assert!(matches!(
            cold.stage_objects(&view, vec![reference]).await,
            Err(Error::CorruptObject)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_physical_references_must_agree_on_counts() -> TestResult {
    let (log, _, faults) = fixture(10).await?;
    let view = log.load().await?;
    let child = log.put_node(&view, Bytes::new(), Vec::new()).await?;
    let mut conflicting = child.reference().clone();
    conflicting.subtree_objects = 2;
    let bytes = format::encode_node(
        &format::Node {
            payload: Bytes::new(),
            children: vec![child.reference().clone(), conflicting],
        },
        log.options(),
    )?;
    let reference = log
        .create_fresh_object_with(ObjectKind::Node, bytes, 4, None, StorageId::new)
        .await?;
    faults.reset();
    assert!(matches!(
        log.stage_objects(&view, vec![reference]).await,
        Err(Error::CorruptObject)
    ));
    // The conflicting edge fails before either copy of its child is read.
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    Ok(())
}

#[tokio::test]
async fn subtree_overflow_fails_during_staging_and_cold_verification() -> TestResult {
    let (log, _, faults) = fixture(10).await?;
    let view = log.load().await?;
    let mut child = log.put_node(&view, Bytes::new(), Vec::new()).await?;
    child.object.subtree_objects = u64::MAX;
    faults.reset();
    assert!(matches!(
        log.put_node(&view, Bytes::new(), vec![child.clone()]).await,
        Err(Error::LimitExceeded("subtree objects"))
    ));
    assert_eq!(faults.metrics().total_requests(), 0);
    let bytes = format::encode_node(
        &format::Node {
            payload: Bytes::new(),
            children: vec![child.object],
        },
        log.options(),
    )?;
    let forged = log
        .create_fresh_object_with(ObjectKind::Node, bytes, 1, None, StorageId::new)
        .await?;
    assert!(matches!(
        log.stage_objects(&view, vec![forged]).await,
        Err(Error::LimitExceeded("subtree objects"))
    ));
    Ok(())
}

#[tokio::test]
async fn resumed_commit_validates_counts_before_retrying_publication() -> TestResult {
    let (log, backend, faults) = fixture(4).await?;
    let view = log.load().await?;
    let root = root(&log, &view).await?;
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root],
    )?;
    let token = prepared.recovery_token()?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Pending(_)
    ));
    let cold = Log::open_existing(&backend, log.store.log_id(), log.options()).await?;
    for count in [0, 1, 3, 4, u64::MAX] {
        let mut forged = format::decode_recovery_token(&token)?;
        forged.objects[0].subtree_objects = count;
        let forged = forged.recovery_token()?;
        faults.reset();
        assert!(matches!(
            cold.resume(&forged).await,
            Err(Error::CorruptObject | Error::LimitExceeded(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(cold.load().await?.generation(), view.generation());
    }
    assert!(matches!(
        cold.resume(&token).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn reopened_pending_checkpoint_validates_its_aggregate_count() -> TestResult {
    let (log, backend, faults) = fixture(3).await?;
    let view = log.load().await?;
    let root = root(&log, &view).await?;
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root.clone()],
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("commit did not publish".into());
    };
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let CheckpointStatus::Pending(pending) = log
        .publish_checkpoint(&view, &view.tail()[0], Bytes::new(), vec![root])
        .await?
    else {
        return Err("checkpoint did not remain pending".into());
    };
    let cold = Log::open_existing(&backend, log.store.log_id(), log.options()).await?;
    let mut forged = pending.clone();
    forged.checkpoint.object.subtree_objects -= 1;
    faults.reset();
    assert!(matches!(
        cold.resolve_checkpoint(forged).await,
        Err(Error::CorruptObject)
    ));
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    let CheckpointResolution::Published(view) = cold.resolve_checkpoint(pending).await? else {
        return Err("valid checkpoint did not resolve".into());
    };
    cold.start_collection(&view).await?;
    Ok(())
}
