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
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 4);
    Ok(())
}

#[tokio::test]
async fn cold_collection_reads_only_edges_across_capped_and_empty_passes() -> TestResult {
    let (log, backend, faults) = fixture(8).await?;
    let initial = log.load().await?;
    let mut leaves = Vec::new();
    for value in [1, 2] {
        leaves.push(
            log.put_object(&initial, Bytes::from(vec![value; 4096]))
                .await?,
        );
    }
    let root = log.put_node(&initial, Bytes::new(), leaves.clone()).await?;
    let prepared = log.prepare(
        &initial,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root.clone()],
    )?;
    let CommitStatus::Committed(committed) = log.commit(prepared).await? else {
        return Err("live publication failed".into());
    };
    let CheckpointStatus::Published(checkpointed) = log
        .publish_checkpoint(
            &committed,
            &committed.tail()[0],
            Bytes::new(),
            vec![root.clone()],
        )
        .await?
    else {
        return Err("checkpoint did not publish".into());
    };
    let prepared = log.prepare(
        &checkpointed,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![root.clone()],
    )?;
    let CommitStatus::Committed(live) = log.commit(prepared).await? else {
        return Err("tail publication failed".into());
    };
    // The checkpoint and tail share the root. Its edges are read only once.
    let edge_bytes = live.tail()[0].len()
        + root.reference().len()
        + live
            .checkpoint()
            .ok_or("missing checkpoint")?
            .object()
            .len();
    for _ in 0..9 {
        log.put_object(&live, Bytes::from_static(b"orphan")).await?;
    }
    // Nine abandoned uploads and the checkpointed commit are unreachable.
    for candidates in [4, 4, 2, 0, 0] {
        let cold = Log::open_existing(&backend, log.store.log_id(), log.options()).await?;
        let view = cold.load().await?;
        faults.reset();
        let start = cold.start_collection_with_limit(&view, 4).await?;
        let reads = faults.metrics().operation(Operation::Get);
        assert_eq!(reads.requests, 3); // checkpoint, commit and node, no leaf payloads
        assert_eq!(reads.downloaded_bytes, edge_bytes);
        match (candidates, start) {
            (0, CollectionStart::Empty(_)) => {
                assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
                assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
            }
            (_, CollectionStart::Installed(fenced, report)) => {
                assert_eq!(report.candidate_count(), candidates);
                let CollectionFinish::Complete(current, _) =
                    cold.resume_collection(&fenced).await?
                else {
                    return Err("collection did not finish".into());
                };
                assert_eq!(current.tail(), live.tail());
            }
            _ => return Err("unexpected collection result".into()),
        }
        assert!(
            !faults
                .metrics()
                .events
                .iter()
                .any(|event| event.operation == Operation::Get && event.path.contains("/blobs/"))
        );
    }
    let current = log.load().await?;
    for (leaf, value) in leaves.iter().zip([1, 2]) {
        assert_eq!(
            log.read_object(&current, leaf.reference()).await?,
            Bytes::from(vec![value; 4096])
        );
    }
    Ok(())
}

#[tokio::test]
async fn reachability_preserves_blob_reference_bounds_and_conflicting_metadata_checks() -> TestResult
{
    let (log, _, faults) = fixture(8).await?;
    let view = log.load().await?;
    let leaf = log.put_object(&view, Bytes::from_static(b"leaf")).await?;
    let mut oversized = leaf.reference().clone();
    oversized.len = u64::try_from(log.options().max_object_bytes)? + 1;
    let mut invalid_count = leaf.reference().clone();
    invalid_count.subtree_objects = 2;
    let mut conflicting = leaf.reference().clone();
    conflicting.len += 1;
    for (roots, expected) in [
        (vec![oversized], Error::LimitExceeded("object bytes")),
        (vec![invalid_count], Error::CorruptObject),
        (
            vec![leaf.reference().clone(), conflicting],
            Error::CorruptObject,
        ),
    ] {
        faults.reset();
        let error = log
            .mark_object_graph(&roots, &mut HashMap::new(), None, GraphWalk::Reachability)
            .await
            .err()
            .ok_or("invalid references were accepted")?;
        assert!(matches!(
            (error, expected),
            (Error::CorruptObject, Error::CorruptObject)
                | (
                    Error::LimitExceeded("object bytes"),
                    Error::LimitExceeded("object bytes")
                )
        ));
        assert_eq!(faults.metrics().total_requests(), 0);
    }
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
            cold.stage_objects(&view, vec![reference.clone()]).await,
            Err(Error::CorruptObject)
        ));
        assert!(matches!(
            cold.mark_object_graph(
                &[reference],
                &mut HashMap::new(),
                None,
                GraphWalk::Reachability
            )
            .await,
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
        let forged = forged.encode()?;
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
    let _outcome = cold.start_collection(&view).await?;
    Ok(())
}
