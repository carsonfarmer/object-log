use crate::{Request, RequestDenied, RequestGuard};
use std::sync::Mutex;

#[derive(Debug)]
struct Guard {
    limit: usize,
    requests: Mutex<Vec<Request>>,
}
impl Guard {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            requests: Mutex::new(Vec::new()),
        })
    }
    fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
impl RequestGuard for Guard {
    fn before_request(&self, request: Request) -> Result<(), RequestDenied> {
        let mut requests = self.requests.lock().map_err(|_| RequestDenied)?;
        if requests.len() == self.limit {
            return Err(RequestDenied);
        }
        requests.push(request);
        Ok(())
    }
}
type GuardResult = Result<(), Box<dyn std::error::Error>>;

async fn guarded_fixture(
    name: &str,
) -> Result<(Log, FaultStore, ValidatedBackend), Box<dyn std::error::Error>> {
    let faults = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from(name)).await?;
    let log = Log::open(&backend, &LogId::new(name)?, Options::default()).await?;
    faults.reset();
    Ok((log, faults, backend))
}

#[tokio::test]
async fn request_guard_denies_before_io_and_preserves_clone_proofs() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-proof").await?;
    let view = log.load().await?;
    let blob = log.put_object(&view, Bytes::from_static(b"data")).await?;
    faults.reset();
    let denied = log.with_request_guard(Guard::new(0));
    assert!(matches!(denied.load().await, Err(Error::RequestDenied)));
    assert!(matches!(
        denied.put_object(&view, Bytes::new()).await,
        Err(Error::RequestDenied)
    ));
    assert_eq!(faults.metrics().total_requests(), 0);
    let guard = Guard::new(3);
    let guarded = log.with_request_guard(guard.clone());
    let node = guarded
        .clone()
        .put_node(&view, Bytes::new(), vec![blob])
        .await?;
    guarded.read_staged_node(&view, &node).await?;
    let prepared = guarded.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![node],
    )?;
    assert!(matches!(
        guarded.commit(prepared).await,
        Err(Error::RequestDenied)
    ));
    assert!(
        log.load().await?.tail().is_empty(),
        "denied head CAS never publishes"
    );
    let requests = guard.requests();
    assert!(matches!(
        requests.as_slice(),
        [
            Request::Write { .. },
            Request::Read { .. },
            Request::Write { .. }
        ]
    ));
    Ok(())
}

#[tokio::test]
async fn request_guard_counts_collisions_and_missing_classification() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-collision").await?;
    let bytes = Bytes::from_static(b"collision");
    let storage_id = StorageId::from_uuid(uuid::Uuid::from_u128(9));
    let object = ObjectRef {
        kind: ObjectKind::Blob,
        storage_id,
        digest: Digest::of(&bytes),
        len: bytes.len() as u64,
    };
    log.store
        .create(log.object_key(&object), bytes.clone())
        .await?;
    faults.reset();
    let guard = Guard::new(3);
    let guarded = log.with_request_guard(guard.clone());
    assert!(matches!(
        guarded
            .create_fresh_object_with(ObjectKind::Blob, bytes, None, || storage_id)
            .await,
        Err(Error::RequestDenied)
    ));
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 3);
    assert_eq!(guard.requests(), vec![Request::Write { bytes: 9 }; 3]);
    let view = log.load().await?;
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![],
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("commit".into());
    };
    log.store
        .delete_immutable_batch(std::iter::once(log.commit_immutable_key(&view.tail()[0])))
        .await?;
    faults.reset();
    let guard = Guard::new(2);
    assert!(
        log.with_request_guard(guard.clone())
            .read_tail(&view)
            .await
            .is_err()
    );
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 2);
    assert_eq!(
        guard.requests(),
        vec![
            Request::Read {
                max_bytes: usize::try_from(view.tail()[0].len())?
            },
            Request::Read {
                max_bytes: log.options.max_head_bytes
            }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn request_guard_concurrent_admission_and_cancellation_are_cumulative() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-cancel").await?;
    let guard = Guard::new(7);
    let guarded = log.with_request_guard(guard.clone());
    let results = futures::future::join_all((0..32).map(|_| guarded.load())).await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 7);
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 7);
    faults.reset();
    let guard = Guard::new(1);
    let weak = Arc::downgrade(&guard);
    let guarded = log.with_request_guard(guard.clone());
    let mut pause = faults.pause_get_at(1, FailurePhase::Before);
    let task_log = guarded.clone();
    let task = tokio::spawn(async move { task_log.load().await });
    assert!(pause.wait_until_entered().await);
    task.abort();
    assert!(task.await.is_err());
    assert_eq!(guard.requests().len(), 1);
    assert!(matches!(guarded.load().await, Err(Error::RequestDenied)));
    drop(guarded);
    drop(guard);
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[tokio::test]
async fn request_guard_denied_recovery_preserves_exact_commit_evidence() -> GuardResult {
    for limit in 0..4 {
        let name = format!("guard-resume-{limit}");
        let (log, faults, backend) = guarded_fixture(&name).await?;
        let view = log.load().await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"op"),
            Bytes::new(),
            vec![],
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
        let cold = Log::open_existing(&backend, &LogId::new(name)?, Options::default()).await?;
        let guard = Guard::new(limit);
        let Resolution::StillPending(pending) = cold
            .with_request_guard(guard.clone())
            .resume(&token)
            .await?
        else {
            return Err("denial lost pending".into());
        };
        assert_eq!(pending.prepared.recovery_token()?, token);
        assert_eq!(guard.requests().len(), limit);
        assert!(matches!(
            cold.resolve(pending).await?,
            Resolution::Committed(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn request_guard_denied_checkpoint_recovery_keeps_candidate() -> GuardResult {
    for limit in 0..4 {
        let name = format!("guard-checkpoint-{limit}");
        let (log, faults, backend) = guarded_fixture(&name).await?;
        let view = log.load().await?;
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            vec![],
        )?;
        let CommitStatus::Committed(view) = log.commit(prepared).await? else {
            return Err("commit".into());
        };
        faults.reset();
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::Before,
        });
        let CheckpointStatus::Pending(pending) = log
            .publish_checkpoint(
                &view,
                &view.tail()[0],
                Bytes::from_static(b"snapshot"),
                vec![],
            )
            .await?
        else {
            return Err("pending".into());
        };
        let expected = pending.checkpoint.clone();
        let cold = Log::open_existing(&backend, &LogId::new(name)?, Options::default()).await?;
        let CheckpointResolution::StillPending(pending) = cold
            .with_request_guard(Guard::new(limit))
            .resolve_checkpoint(pending)
            .await?
        else {
            return Err("lost checkpoint".into());
        };
        assert_eq!(pending.checkpoint, expected);
        assert!(matches!(
            cold.resolve_checkpoint(pending).await?,
            CheckpointResolution::Published(_)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn request_guard_listing_is_lazy_and_delete_describes_batch() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-streams").await?;
    let guard = Guard::new(1);
    let guarded = log.with_request_guard(guard.clone());
    let listing = guarded.store.list_scoped();
    assert!(guard.requests().is_empty());
    listing.try_collect::<Vec<_>>().await?;
    assert_eq!(guard.requests(), vec![Request::List]);
    let key = log.commit_immutable_key(&CommitRef {
        sequence: 0,
        transaction_id: TransactionId::new(),
        storage_id: StorageId::new(),
        digest: Digest::of(b"x"),
        len: 1,
    });
    faults.reset();
    assert!(matches!(
        guarded
            .store
            .delete_immutable_batch(std::iter::once(key))
            .await,
        Err(Error::RequestDenied)
    ));
    assert_eq!(faults.metrics().total_requests(), 0);
    let guard = Guard::new(1);
    log.with_request_guard(guard.clone())
        .store
        .delete_immutable_batch(std::iter::once(key))
        .await?;
    assert_eq!(guard.requests(), vec![Request::Delete { objects: 1 }]);
    Ok(())
}

#[tokio::test]
async fn request_guard_charges_active_plan_before_staging() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-plan").await?;
    let view = log.load().await?;
    log.put_object(&view, Bytes::from_static(b"orphan")).await?;
    let CollectionStart::Installed(view, _) = log.start_collection(&view).await? else {
        return Err("plan".into());
    };
    faults.reset();
    let guard = Guard::new(1);
    assert!(matches!(
        log.with_request_guard(guard.clone())
            .put_object(&view, Bytes::new())
            .await,
        Err(Error::RequestDenied)
    ));
    assert_eq!(faults.metrics().operation(Operation::Get).requests, 1);
    assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
    assert_eq!(
        guard.requests(),
        vec![Request::Read {
            max_bytes: usize::try_from(view.collection_plan_bytes().ok_or("plan length")?)?
        }]
    );
    Ok(())
}

#[tokio::test]
async fn request_guard_denied_cold_success_verification_retains_evidence() -> GuardResult {
    let (log, faults, backend) = guarded_fixture("guard-cold-success").await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        &view,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![],
    )?;
    let token = prepared.recovery_token()?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Pending(_)
    ));
    let cold = Log::open_existing(
        &backend,
        &LogId::new("guard-cold-success")?,
        Options::default(),
    )
    .await?;
    let Resolution::StillPending(pending) = cold
        .with_request_guard(Guard::new(1))
        .resume(&token)
        .await?
    else {
        return Err("verification was skipped".into());
    };
    assert_eq!(pending.prepared.recovery_token()?, token);
    assert!(matches!(
        cold.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    let view = log.load().await?;
    faults.reset();
    faults.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let CheckpointStatus::Pending(pending) = log
        .publish_checkpoint(&view, &view.tail()[0], Bytes::new(), vec![])
        .await?
    else {
        return Err("checkpoint pending".into());
    };
    let CheckpointResolution::StillPending(pending) = cold
        .with_request_guard(Guard::new(1))
        .resolve_checkpoint(pending)
        .await?
    else {
        return Err("checkpoint verification was skipped".into());
    };
    assert!(matches!(
        cold.resolve_checkpoint(pending).await?,
        CheckpointResolution::Published(_)
    ));
    Ok(())
}

#[tokio::test]
async fn request_guard_listing_retains_admission_until_stream_drop() -> GuardResult {
    let (log, _, _) = guarded_fixture("guard-list-lifetime").await?;
    let guard = Guard::new(1);
    let weak = Arc::downgrade(&guard);
    let guarded = log.with_request_guard(guard.clone());
    let mut listing = guarded.store.list_scoped();
    drop(guarded);
    drop(guard);
    assert!(listing.try_next().await?.is_some());
    assert!(
        weak.upgrade().is_some(),
        "lazy listing still owns admission"
    );
    drop(listing);
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[tokio::test]
async fn request_guard_collection_denial_does_not_count_unsubmitted_deletes() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-delete-report").await?;
    let view = log.load().await?;
    log.put_object(&view, Bytes::from_static(b"orphan")).await?;
    let CollectionStart::Installed(view, _) = log.start_collection(&view).await? else {
        return Err("plan".into());
    };
    faults.reset();
    let CollectionFinish::Pending(report) = log
        .with_request_guard(Guard::new(2))
        .resume_collection(&view)
        .await?
    else {
        return Err("pending deletion".into());
    };
    assert_eq!(report.delete_attempts, 0);
    assert_eq!(faults.metrics().operation(Operation::Delete).requests, 0);
    assert!(matches!(
        log.resume_collection(&view).await?,
        CollectionFinish::Complete(..)
    ));
    Ok(())
}

#[tokio::test]
async fn request_guard_composition_preserves_policy_and_prior_admissions() -> GuardResult {
    let (log, faults, _) = guarded_fixture("guard-composition").await?;
    let caller = Guard::new(0);
    let operation = Guard::new(10);
    let guarded = log
        .with_request_guard(caller.clone())
        .with_request_guard(operation.clone());
    assert!(matches!(guarded.load().await, Err(Error::RequestDenied)));
    assert!(operation.requests().is_empty());
    assert_eq!(faults.metrics().total_requests(), 0);

    let caller = Guard::new(10);
    let operation = Guard::new(1);
    let caller_log = log.with_request_guard(caller.clone());
    let guarded = caller_log.with_request_guard(operation.clone());
    guarded.load().await?;
    assert!(matches!(
        guarded.clone().load().await,
        Err(Error::RequestDenied)
    ));
    assert_eq!(
        caller.requests().len(),
        2,
        "later refusal never refunds prior admission"
    );
    assert_eq!(operation.requests().len(), 1);
    assert_eq!(faults.metrics().total_requests(), 1);
    caller_log.load().await?;
    assert_eq!(
        caller.requests().len(),
        3,
        "attachment did not mutate original handle"
    );
    assert_eq!(operation.requests().len(), 1);
    Ok(())
}
