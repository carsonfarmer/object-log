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
    session_on_with_options(store, Options::default()).await
}
async fn session_on_with_options(
    store: Arc<dyn ObjectStore>,
    mut options: Options,
) -> SessionState {
    let backend = object_log::ValidatedBackend::new(store, Path::from("maintenance-tests"))
        .await
        .unwrap();
    options.max_collection_objects = 4;
    let log = Log::open(&backend, &object_log::LogId::new("repo").unwrap(), options)
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
        s.log
            .publish_checkpoint(
                &s.current_view(),
                Some(s.current_view().tail().last().unwrap()),
                Bytes::from(vec![]),
                vec![live.clone()]
            )
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
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::Conflict
    ));
    assert!(GuestSession::has_active_collection(&s));
    let mut complete = false;
    for _ in 0..8 {
        let report = maintenance::collect(&s, 4).await.unwrap();
        if matches!(report.state, MaintenanceState::Complete) {
            complete = true;
            break;
        }
        assert!(matches!(report.state, MaintenanceState::More));
    }
    assert!(complete);
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
async fn pending_collection_continues_with_the_cached_fence() {
    let faults = FaultStore::new(InMemory::new());
    let s = session_on(Arc::new(faults.clone())).await;
    s.log
        .put_object(&s.current_view(), Bytes::from_static(b"orphan"))
        .await
        .unwrap();

    faults.reset();
    faults.schedule(StoreFailure {
        operation: Operation::Delete,
        occurrence: 1,
        phase: FailurePhase::Before,
    });
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::Pending
    ));
    assert!(GuestSession::has_active_collection(&s));

    faults.reset();
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::More
    ));
    assert!(!GuestSession::has_active_collection(&s));
    assert!(matches!(
        maintenance::collect(&s, 4).await.unwrap().state,
        MaintenanceState::Complete
    ));
}

#[tokio::test]
async fn recovered_view_checkpoints_prefix_and_preserves_concurrent_append() {
    let mut s = session().await;
    append(&mut s, vec![]).await;
    let stale = s.current_view();
    let mut recovery = object_log::history(&s.log, stale).unwrap();
    while recovery.next().await.unwrap().is_some() {}
    append(&mut s, vec![]).await;
    let current = s.current_view();
    let CheckpointStatus::Published(checkpointed) = s
        .log
        .publish_checkpoint(
            recovery.view(),
            Some(recovery.view().tail().last().unwrap()),
            Bytes::from(vec![]),
            vec![],
        )
        .await
        .unwrap()
    else {
        panic!("checkpoint did not preserve the appended suffix");
    };
    assert_eq!(checkpointed.tail(), &current.tail()[1..]);
    assert_eq!(
        checkpointed.checkpoint().unwrap().through_commit(),
        current.tail()[0].digest()
    );
    assert_eq!(s.log.read_tail(&checkpointed).await.unwrap().len(), 1);
    assert_eq!(
        s.log.load().await.unwrap().generation(),
        checkpointed.generation()
    );
}

#[tokio::test]
async fn checkpoint_summary_leaves_failed_resolution_pending() {
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
    let CheckpointStatus::Pending(pending) = s
        .log
        .publish_checkpoint(
            &view,
            Some(view.tail().last().unwrap()),
            Bytes::from(b"snapshot".to_vec()),
            Vec::new(),
        )
        .await
        .unwrap()
    else {
        panic!("checkpoint outcome was not uncertain")
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
        checkpoint_state(&s.log, CheckpointStatus::Pending(pending.clone())).unwrap(),
        MaintenanceState::Pending
    );

    faults.reset();
    assert_eq!(
        checkpoint_state(&s.log, CheckpointStatus::Pending(pending)).unwrap(),
        MaintenanceState::Complete
    );
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
        value: RefCell::new(Some(prepared)),
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
async fn latest_is_selective_view_bound_and_does_not_advance_history() {
    let faults = FaultStore::new(InMemory::new());
    let mut s = session_on(Arc::new(faults.clone())).await;
    for _ in 0..12 {
        append(&mut s, vec![]).await;
    }
    let view = s.log.load().await.unwrap();
    let first = view.tail().first().unwrap().sequence();
    let last = view.tail().last().unwrap().sequence();
    let recovery = RecoveryState {
        log: s.log.clone(),
        value: RefCell::new(object_log::history(&s.log, view).unwrap()),
    };
    append(&mut s, vec![]).await;
    faults.reset();
    let latest = GuestRecovery::latest(&recovery).unwrap();
    assert_eq!(latest.tail_entries, 12);
    let Some(HistoryItem::Commit(record)) = latest.item else {
        panic!("missing latest commit")
    };
    assert_eq!(record.sequence, last);
    assert_eq!(faults.metrics().total_requests(), 1);
    assert!(!recovery.value.borrow().is_complete());
    let Some(HistoryItem::Commit(record)) = GuestRecovery::next(&recovery).unwrap() else {
        panic!("history cursor advanced during latest read")
    };
    assert_eq!(record.sequence, first);
    // Checkpointing verifies the remaining tail and preserves the concurrent suffix.
    assert!(matches!(
        GuestRecovery::checkpoint(&recovery, vec![], vec![]),
        Ok(MaintenanceState::Complete)
    ));
    assert_eq!(s.log.load().await.unwrap().tail().len(), 1);
}

#[tokio::test]
async fn latest_reads_empty_logs_and_checkpoint_object_proofs() {
    let mut s = session().await;
    let log = s.log.clone();
    let recovery = |view| RecoveryState {
        log: log.clone(),
        value: RefCell::new(object_log::history(&log, view).unwrap()),
    };
    assert!(
        recovery(s.current_view())
            .latest_record()
            .unwrap()
            .1
            .is_none()
    );
    let view = s.current_view();
    let blob = s
        .log
        .put_object(&view, Bytes::from_static(b"payload"))
        .await
        .unwrap();
    let root = s
        .log
        .put_node(&view, Bytes::from_static(b"root"), vec![blob])
        .await
        .unwrap();
    append(&mut s, vec![root.clone()]).await;
    let view = s.current_view();
    assert!(matches!(
        s.log
            .publish_checkpoint(
                &view,
                Some(view.tail().last().unwrap()),
                Bytes::from_static(b"snapshot"),
                vec![root]
            )
            .await
            .unwrap(),
        CheckpointStatus::Published(_)
    ));
    let recovery = recovery(s.log.load().await.unwrap());
    let (tail, Some(LogHistoryItem::Checkpoint(record))) = recovery.latest_record().unwrap() else {
        panic!("missing current checkpoint")
    };
    assert_eq!(tail, 0);
    assert_eq!(record.record().snapshot(), b"snapshot".as_slice());
    let view = recovery.bound_view().clone();
    let (data, children) = s
        .log
        .read_staged_node(&view, &record.proofs()[0])
        .await
        .unwrap();
    assert_eq!(data, b"root".as_slice());
    assert_eq!(
        s.log
            .read_object(&view, children[0].reference())
            .await
            .unwrap(),
        b"payload".as_slice()
    );
}

#[tokio::test]
async fn resume_reports_losing_and_unresolved_tokens_without_discarding_them() {
    let faults = FaultStore::new(InMemory::new());
    let s = session_on(Arc::new(faults.clone())).await;
    let mut winner = session_on(Arc::new(faults.clone())).await;
    let losing = s
        .log
        .prepare(
            &s.current_view(),
            TransactionId::new(),
            Bytes::from_static(b"loser"),
            Bytes::new(),
            vec![],
        )
        .unwrap();
    let losing_token = losing.recovery_token().unwrap();
    append(&mut winner, vec![]).await;
    assert_ne!(
        s.current_view().generation(),
        winner.current_view().generation()
    );
    assert!(matches!(
        GuestSession::resume(&s, losing_token.to_vec()).unwrap(),
        Resolution::NotCommitted
    ));
    assert_eq!(
        s.current_view().generation(),
        winner.current_view().generation()
    );

    let candidate = s
        .log
        .prepare(
            &s.current_view(),
            TransactionId::new(),
            Bytes::from_static(b"retry"),
            Bytes::new(),
            vec![],
        )
        .unwrap();
    let token = candidate.recovery_token().unwrap().to_vec();
    let previous_generation = s.current_view().generation();
    faults.reset();
    for occurrence in 1..=3 {
        faults.schedule(StoreFailure {
            operation: Operation::Get,
            occurrence,
            phase: FailurePhase::Before,
        });
    }
    let Resolution::StillPending(returned) = GuestSession::resume(&s, token.clone()).unwrap()
    else {
        panic!("a failed evidence read must preserve the token")
    };
    assert_eq!(returned, token);
    assert_eq!(s.current_view().generation(), previous_generation);
    faults.reset();
    assert!(matches!(
        GuestSession::resume(&s, returned).unwrap(),
        Resolution::Committed
    ));
    assert!(s.current_view().generation() > previous_generation);
}

#[tokio::test]
async fn checkpoint_summary_reports_a_definite_loser_as_conflict() {
    let faults = FaultStore::new(InMemory::new());
    let mut s = session_on(Arc::new(faults.clone())).await;
    append(&mut s, vec![]).await;
    let view = s.current_view();
    faults.reset();
    faults.schedule(StoreFailure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let CheckpointStatus::Pending(pending) = s
        .log
        .publish_checkpoint(
            &view,
            Some(view.tail().last().unwrap()),
            Bytes::from(b"loser".to_vec()),
            vec![],
        )
        .await
        .unwrap()
    else {
        panic!("lost checkpoint response did not remain pending")
    };
    faults.reset();
    assert!(matches!(
        s.log
            .publish_checkpoint(
                &view,
                Some(view.tail().last().unwrap()),
                Bytes::from(b"winner".to_vec()),
                vec![]
            )
            .await
            .unwrap(),
        CheckpointStatus::Published(_)
    ));
    assert_eq!(
        checkpoint_state(&s.log, CheckpointStatus::Pending(pending)).unwrap(),
        MaintenanceState::Conflict
    );
}

#[tokio::test]
async fn resume_reports_expired_after_checkpoint_discards_commit_evidence() {
    let s = session_on_with_options(
        Arc::new(InMemory::new()),
        Options {
            resolution_window: 0,
            ..Options::default()
        },
    )
    .await;
    let prepared = s
        .log
        .prepare(
            &s.current_view(),
            TransactionId::new(),
            Bytes::from_static(b"old commit"),
            Bytes::new(),
            vec![],
        )
        .unwrap();
    let token = prepared.recovery_token().unwrap().to_vec();
    let CommitStatus::Committed(committed) = s.log.commit(prepared).await.unwrap() else {
        panic!("commit did not publish")
    };
    assert!(matches!(
        s.log
            .publish_checkpoint(
                &committed,
                Some(committed.tail().last().unwrap()),
                Bytes::from(b"snapshot".to_vec()),
                vec![]
            )
            .await
            .unwrap(),
        CheckpointStatus::Published(_)
    ));

    assert_eq!(s.current_view().generation(), 0);
    assert!(matches!(
        GuestSession::resume(&s, token).unwrap(),
        Resolution::Expired
    ));
    assert_eq!(s.current_view().generation(), 2);
    assert!(s.current_view().checkpoint().is_some());
    assert!(s.current_view().tail().is_empty());
}

#[tokio::test]
async fn checkpoint_summary_preserves_expired_evidence_as_pending() {
    let faults = FaultStore::new(InMemory::new());
    let mut s = session_on(Arc::new(faults.clone())).await;
    append(&mut s, vec![]).await;
    let view = s.current_view();
    faults.reset();
    faults.schedule(StoreFailure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let CheckpointStatus::Pending(pending) = s
        .log
        .publish_checkpoint(
            &view,
            Some(view.tail().last().unwrap()),
            Bytes::from(b"old snapshot".to_vec()),
            vec![],
        )
        .await
        .unwrap()
    else {
        panic!("checkpoint did not retain pending evidence")
    };
    faults.reset();
    assert!(matches!(
        s.log
            .publish_checkpoint(
                &view,
                Some(view.tail().last().unwrap()),
                Bytes::from(b"replacement".to_vec()),
                vec![]
            )
            .await
            .unwrap(),
        CheckpointStatus::Published(_)
    ));
    s.view.replace(s.log.load().await.unwrap());
    append(&mut s, vec![]).await;

    assert_eq!(
        checkpoint_state(&s.log, CheckpointStatus::Pending(pending)).unwrap(),
        MaintenanceState::Pending
    );
}

#[tokio::test]
async fn checkpoint_call_resolves_one_lost_head_acknowledgement() {
    for phase in [FailurePhase::Before, FailurePhase::After] {
        let faults = FaultStore::new(InMemory::new());
        let mut s = session_on(Arc::new(faults.clone())).await;
        append(&mut s, vec![]).await;
        let recovery = RecoveryState {
            log: s.log.clone(),
            value: RefCell::new(object_log::history(&s.log, s.current_view()).unwrap()),
        };
        faults.reset();
        faults.schedule(StoreFailure {
            operation: Operation::Put,
            occurrence: 2,
            phase,
        });
        assert!(matches!(
            GuestRecovery::checkpoint(&recovery, b"snapshot".to_vec(), vec![]),
            Ok(MaintenanceState::Complete)
        ));
        let fresh = s.log.load().await.unwrap();
        assert!(fresh.tail().is_empty());
        assert_eq!(
            s.log
                .read_checkpoint(&fresh)
                .await
                .unwrap()
                .unwrap()
                .snapshot(),
            b"snapshot".as_slice()
        );
    }
}

#[tokio::test]
async fn checkpoint_call_returns_pending_when_resolution_reads_fail() {
    for phase in [FailurePhase::Before, FailurePhase::After] {
        let faults = FaultStore::new(InMemory::new());
        let mut s = session_on(Arc::new(faults.clone())).await;
        append(&mut s, vec![]).await;
        let recovery = RecoveryState {
            log: s.log.clone(),
            value: RefCell::new(object_log::history(&s.log, s.current_view()).unwrap()),
        };
        faults.reset();
        faults.schedule(StoreFailure {
            operation: Operation::Put,
            occurrence: 2,
            phase,
        });
        for occurrence in 2..=4 {
            faults.schedule(StoreFailure {
                operation: Operation::Get,
                occurrence,
                phase: FailurePhase::Before,
            });
        }
        assert!(matches!(
            GuestRecovery::checkpoint(&recovery, b"snapshot".to_vec(), vec![]),
            Ok(MaintenanceState::Pending)
        ));
        faults.reset();
        let fresh = s.log.load().await.unwrap();
        assert_eq!(fresh.checkpoint().is_some(), phase == FailurePhase::After);
    }
}

#[tokio::test]
async fn checkpoint_call_preserves_a_concurrent_checkpoint_winner() {
    let mut s = session().await;
    append(&mut s, vec![]).await;
    let view = s.current_view();
    let recovery = RecoveryState {
        log: s.log.clone(),
        value: RefCell::new(object_log::history(&s.log, view.clone()).unwrap()),
    };
    let CheckpointStatus::Published(winner) = s
        .log
        .publish_checkpoint(
            &view,
            Some(view.tail().last().unwrap()),
            Bytes::from_static(b"winner"),
            vec![],
        )
        .await
        .unwrap()
    else {
        panic!("winner did not publish");
    };
    assert!(matches!(
        GuestRecovery::checkpoint(&recovery, b"loser".to_vec(), vec![]),
        Ok(MaintenanceState::Conflict)
    ));
    let fresh = s.log.load().await.unwrap();
    assert_eq!(fresh.generation(), winner.generation());
    assert_eq!(
        s.log
            .read_checkpoint(&fresh)
            .await
            .unwrap()
            .unwrap()
            .snapshot(),
        b"winner".as_slice()
    );
}

#[tokio::test]
async fn selective_reads_leave_older_corruption_visible_and_block_publication() {
    for checkpoint in [false, true] {
        let store = Arc::new(InMemory::new());
        let faults = FaultStore::new(store.clone());
        let mut s = session_on(Arc::new(faults.clone())).await;
        append(&mut s, vec![]).await;
        if checkpoint {
            assert!(matches!(
                s.log
                    .publish_checkpoint(
                        &s.current_view(),
                        Some(s.current_view().tail().last().unwrap()),
                        Bytes::from(b"old checkpoint".to_vec()),
                        vec![]
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
        // Fresh handles cannot reuse the earlier immutable-history proof.
        let backend = object_log::ValidatedBackend::assume_validated(
            Arc::new(faults.clone()),
            Path::from("maintenance-tests"),
        );
        let cold = Log::open_existing(
            &backend,
            &object_log::LogId::new("repo").unwrap(),
            Options {
                max_collection_objects: 4,
                ..Options::default()
            },
        )
        .await
        .unwrap();
        let view = cold.load().await.unwrap();
        let generation = view.generation();
        let recovery = RecoveryState {
            log: cold.clone(),
            value: RefCell::new(object_log::history(&cold, view).unwrap()),
        };
        faults.reset();
        assert!(matches!(
            GuestRecovery::latest(&recovery).unwrap().item,
            Some(HistoryItem::Commit(_))
        ));
        assert_eq!(faults.metrics().total_requests(), 1);
        assert!(matches!(
            GuestRecovery::prepare(
                &recovery,
                uuid::Uuid::new_v4().as_bytes().to_vec(),
                vec![],
                vec![],
                vec![],
            ),
            Err(Failure::Other(_))
        ));
        assert!(matches!(
            GuestRecovery::checkpoint(&recovery, vec![], vec![]),
            Err(Failure::Other(_))
        ));
        assert_eq!(faults.metrics().operation(Operation::Put).requests, 0);
        assert_eq!(cold.load().await.unwrap().generation(), generation);
        assert!(GuestRecovery::next(&recovery).is_err());
    }
}

#[tokio::test]
async fn empty_tail_checkpoint_rewrite_resolves_once_or_reports_pending() {
    for phase in [FailurePhase::Before, FailurePhase::After] {
        for unreadable_resolution in [false, true] {
            let faults = FaultStore::new(InMemory::new());
            let mut s = session_on(Arc::new(faults.clone())).await;
            append(&mut s, vec![]).await;
            let source = s.current_view();
            let CheckpointStatus::Published(base) = s
                .log
                .publish_checkpoint(
                    &source,
                    source.tail().last(),
                    Bytes::from_static(b"old"),
                    vec![],
                )
                .await
                .unwrap()
            else {
                panic!("base did not publish");
            };
            let boundary = base.checkpoint().unwrap().clone();
            let recovery = RecoveryState {
                log: s.log.clone(),
                value: RefCell::new(object_log::history(&s.log, base).unwrap()),
            };
            faults.reset();
            faults.schedule(StoreFailure {
                operation: Operation::Put,
                occurrence: 2,
                phase,
            });
            if unreadable_resolution {
                for occurrence in 2..=4 {
                    faults.schedule(StoreFailure {
                        operation: Operation::Get,
                        occurrence,
                        phase: FailurePhase::Before,
                    });
                }
            }
            let state = GuestRecovery::checkpoint(&recovery, b"pruned".to_vec(), vec![]).unwrap();
            assert_eq!(
                state,
                if unreadable_resolution {
                    MaintenanceState::Pending
                } else {
                    MaintenanceState::Complete
                }
            );
            faults.reset();
            let current = s.log.load().await.unwrap();
            assert!(current.tail().is_empty());
            let reference = current.checkpoint().unwrap();
            assert_eq!(reference.through_sequence(), boundary.through_sequence());
            assert_eq!(reference.through_commit(), boundary.through_commit());
            assert_eq!(
                s.log
                    .read_checkpoint(&current)
                    .await
                    .unwrap()
                    .unwrap()
                    .snapshot(),
                if unreadable_resolution && phase == FailurePhase::Before {
                    b"old".as_slice()
                } else {
                    b"pruned".as_slice()
                }
            );
        }
    }
}
