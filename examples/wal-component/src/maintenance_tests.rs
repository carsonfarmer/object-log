use super::*;
use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure as StoreFailure, FailurePhase, FaultStore, Operation};
use object_log::{CollectionStart, Options, TransactionId};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

async fn session() -> SessionState {
    session_on(Arc::new(InMemory::new())).await
}
async fn session_on(store: Arc<dyn ObjectStore>) -> SessionState {
    let backend = object_log::ValidatedBackend::new(store, Path::from("maintenance-tests"))
        .await
        .unwrap();
    let log = Log::open(
        &backend,
        &object_log::LogId::new("repo").unwrap(),
        Options {
            max_collection_objects: 4,
            ..Options::default()
        },
    )
    .await
    .unwrap();
    let view = log.load().await.unwrap();
    SessionState {
        log,
        view: RefCell::new(view),
        transport: transport::Transport::new(100, 1024).unwrap(),
    }
}
async fn append(s: &mut SessionState, roots: Vec<StagedObject>) {
    let source = s.current_view();
    let prepared = s
        .log
        .prepare(
            &source,
            TransactionId::new(),
            Bytes::new(),
            Bytes::new(),
            roots,
        )
        .unwrap();
    let CommitStatus::Committed(view) = s.log.commit(prepared).await.unwrap() else {
        panic!("append failed")
    };
    s.view.replace(view);
}
#[tokio::test]
async fn checkpoint_and_resumed_batches_keep_live_objects() {
    let mut s = session().await;
    assert!(!GuestSession::has_active_collection(&s));
    let live = s
        .log
        .put_object(&s.current_view(), Bytes::from_static(b"keep"))
        .await
        .unwrap();
    let old = s
        .log
        .put_object(&s.current_view(), Bytes::from_static(b"remove"))
        .await
        .unwrap();
    append(&mut s, vec![live.clone(), old.clone()]).await;
    assert!(matches!(
        maintenance::checkpoint(&s.log, &s.current_view(), vec![], vec![live.clone()])
            .await
            .unwrap(),
        CheckpointStatus::Published(_)
    ));
    s.view.replace(s.log.load().await.unwrap());
    for i in 0..8 {
        s.log
            .put_object(&s.current_view(), Bytes::from(vec![i]))
            .await
            .unwrap();
    }
    // Leave an installed plan behind, as a stopped process would.
    assert!(matches!(
        s.log.start_collection(&s.current_view()).await.unwrap(),
        CollectionStart::Installed(..)
    ));
    // This reports only the cached view; observing another operation's fence
    // requires refreshing, without a hidden storage read from this method.
    assert!(!GuestSession::has_active_collection(&s));
    s.view.replace(s.log.load().await.unwrap());
    assert!(GuestSession::has_active_collection(&s));
    let mut complete = false;
    for _ in 0..8 {
        s.view.replace(s.log.load().await.unwrap());
        let report = maintenance::collect(&s, 4).await.unwrap();
        if matches!(report.state, MaintenanceState::Complete) {
            complete = true;
            break;
        }
        assert!(matches!(report.state, MaintenanceState::More));
    }
    assert!(complete);
    s.view.replace(s.log.load().await.unwrap());
    assert!(!GuestSession::has_active_collection(&s));
    assert_eq!(
        s.log
            .read_object(&s.current_view(), live.reference())
            .await
            .unwrap(),
        b"keep"[..]
    );
    assert!(
        s.log
            .read_object(&s.current_view(), old.reference())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn collection_calls_bound_new_plans_and_reject_invalid_caps() {
    let s = session().await;
    for value in 0..3 {
        s.log
            .put_object(&s.current_view(), Bytes::from(vec![value]))
            .await
            .unwrap();
    }
    for cap in [0, 5] {
        assert!(matches!(
            maintenance::collect(&s, cap).await,
            Err(Failure::Limit(_))
        ));
    }
    let report = maintenance::collect(&s, 1).await.unwrap();
    assert!(matches!(report.state, MaintenanceState::More));
    assert_eq!(report.objects, 1);
}
#[tokio::test]
async fn recovered_view_does_not_checkpoint_a_concurrent_append() {
    let mut s = session().await;
    append(&mut s, vec![]).await;
    let stale = s.current_view();
    let mut recovery = object_log::history(&s.log, stale).unwrap();
    while recovery.next().await.unwrap().is_some() {}
    append(&mut s, vec![]).await;
    let current = s.current_view();
    assert!(matches!(
        maintenance::checkpoint(&s.log, recovery.view(), vec![], vec![])
            .await
            .unwrap(),
        CheckpointStatus::Conflict(_)
    ));
    assert_eq!(
        s.log.load().await.unwrap().generation(),
        current.generation()
    );
}

#[tokio::test]
async fn pending_checkpoint_keeps_exact_evidence_until_resolution() {
    let faults = FaultStore::new(InMemory::new());
    let mut s = session_on(Arc::new(faults.clone())).await;
    append(&mut s, vec![]).await;
    let view = s.current_view();

    faults.reset();
    faults.schedule(StoreFailure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::After,
    });
    let CheckpointStatus::Pending(pending) =
        maintenance::checkpoint(&s.log, &view, b"snapshot".to_vec(), Vec::new())
            .await
            .unwrap()
    else {
        panic!("checkpoint outcome was not uncertain")
    };
    let pending = PendingCheckpointState {
        log: s.log.clone(),
        pending: RefCell::new(Some(pending)),
    };

    faults.reset();
    for occurrence in 1..=3 {
        faults.schedule(StoreFailure {
            operation: Operation::Get,
            occurrence,
            phase: FailurePhase::Before,
        });
    }
    assert_eq!(
        GuestPendingCheckpoint::resolve(&pending).unwrap(),
        CheckpointResolution::StillPending
    );
    assert!(pending.pending.borrow().is_some());

    faults.reset();
    assert_eq!(
        GuestPendingCheckpoint::resolve(&pending).unwrap(),
        CheckpointResolution::Published
    );
    assert!(pending.pending.borrow().is_none());
}

#[tokio::test]
async fn session_retention_blocks_collection_and_drained_recovery_clears_it() {
    let s = session().await;
    s.log
        .put_object(&s.current_view(), Bytes::from_static(b"orphan"))
        .await
        .unwrap();
    assert_eq!(
        GuestSession::retain(&s, vec![7; 16]).unwrap(),
        RetentionState::Applied
    );
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::Retained
    ));
    assert_eq!(
        GuestSession::release_retention(&s, vec![7; 16]).unwrap(),
        RetentionState::Applied
    );
    assert_eq!(
        GuestSession::retain(&s, vec![9; 16]).unwrap(),
        RetentionState::Applied
    );
    assert_eq!(
        GuestSession::clear_retentions_after_drain(&s).unwrap(),
        RetentionState::Applied
    );
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::More
    ));
    assert!(GuestSession::retain(&s, vec![0; 15]).is_err());
}

#[tokio::test]
async fn candidate_token_precedes_single_use_publication_and_resumes() {
    let s = session().await;
    let transaction_id = TransactionId::from_uuid(uuid::Uuid::from_u128(23));
    let prepared = s
        .log
        .prepare(
            &s.current_view(),
            transaction_id,
            Bytes::from_static(b"operation"),
            Bytes::from_static(b"result"),
            Vec::new(),
        )
        .unwrap();
    let candidate = CandidateState {
        log: s.log.clone(),
        prepared: RefCell::new(Some(prepared)),
    };
    let token = GuestCandidate::recovery_token(&candidate).unwrap();
    assert!(matches!(
        GuestCandidate::publish(&candidate).unwrap(),
        Outcome::Committed
    ));
    assert!(GuestCandidate::recovery_token(&candidate).is_err());
    assert!(GuestCandidate::publish(&candidate).is_err());
    assert!(matches!(
        GuestSession::resume(&s, token).unwrap(),
        Resolution::Committed
    ));
    let view = s.current_view();
    let records = s.log.read_tail(&view).await.unwrap();
    let [record] = records.as_slice() else {
        panic!("missing recovered commit")
    };
    assert_eq!(record.reference().transaction_id(), transaction_id);
    assert_eq!(record.operation(), b"operation".as_slice());
    assert_eq!(record.result(), b"result".as_slice());
}

#[tokio::test]
async fn recovery_does_not_skip_corrupt_older_commit_or_checkpoint() {
    for checkpoint in [false, true] {
        let store = Arc::new(InMemory::new());
        let mut s = session_on(store.clone()).await;
        append(&mut s, vec![]).await;
        if checkpoint {
            assert!(matches!(
                maintenance::checkpoint(
                    &s.log,
                    &s.current_view(),
                    b"old checkpoint".to_vec(),
                    vec![],
                )
                .await
                .unwrap(),
                CheckpointStatus::Published(_)
            ));
            s.view.replace(s.log.load().await.unwrap());
        }
        let suffix = if checkpoint {
            "/checkpoints/"
        } else {
            "/commits/"
        };
        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        let key = objects
            .into_iter()
            .find(|meta| meta.location.as_ref().contains(suffix))
            .unwrap_or_else(|| panic!("missing {suffix} object"))
            .location;
        append(&mut s, vec![]).await;
        let mut valid = object_log::history(&s.log, s.current_view()).unwrap();
        while valid.next().await.unwrap().is_some() {}
        store
            .put(&key, Bytes::from_static(b"corrupt earlier record").into())
            .await
            .unwrap();
        let mut cursor = object_log::history(&s.log, s.current_view()).unwrap();
        let mut error = None;
        loop {
            match cursor.next().await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(found) => {
                    error = Some(found);
                    break;
                }
            }
        }
        assert!(matches!(error, Some(object_log::Error::CorruptObject)));
    }
}
