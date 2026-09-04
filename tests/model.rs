#![cfg(feature = "test-util")]

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation, RequestOutcome};
use object_log::{
    CommitRef, CommitStatus, Log, LogId, Options, PendingCommit, Resolution, TransactionId,
    ValidatedBackend, View,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use std::collections::HashSet;
use std::error::Error as StdError;
use std::sync::Arc;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn StdError>>;

#[tokio::test]
async fn validated_backend_opens_tenants_without_more_probes() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("cheap-open-tests")).await?;
    store.reset();

    for id in ["tenant-a", "tenant-b"] {
        Log::open(&backend, &LogId::new(id)?, Options::default()).await?;
    }

    let metrics = store.metrics();
    assert_eq!(metrics.operation(Operation::Put).requests, 2);
    assert_eq!(metrics.operation(Operation::Get).requests, 0);
    assert_eq!(metrics.operation(Operation::Delete).requests, 0);
    Ok(())
}

#[tokio::test]
async fn preflight_has_no_store_requests_and_reserves_nothing() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let backend =
        ValidatedBackend::new(Arc::new(store.clone()), Path::from("preflight-tests")).await?;
    let log = Log::open(&backend, &LogId::new("preflight")?, Options::default()).await?;
    let view = log.load().await?;
    let transaction_id = TransactionId::new();
    store.reset();

    log.preflight(&view, transaction_id)?;
    log.preflight(&view, transaction_id)?;

    assert_eq!(store.metrics().total_requests(), 0);
    Ok(())
}

#[tokio::test]
async fn failure_before_put_makes_no_change() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let path = Path::from("logs/test/head");
    store.fail_next(Operation::Put, FailurePhase::Before);

    let error = store
        .put(&path, Bytes::from_static(b"candidate").into())
        .await
        .err()
        .ok_or_else(|| test_error("injected write succeeded"))?;
    assert!(FaultStore::is_injected(&error));
    assert!(matches!(
        store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let metrics = store.metrics();
    let puts = metrics.operation(Operation::Put);
    assert_eq!(puts.requests, 1);
    assert_eq!(puts.visible_mutations, 0);
    assert_eq!(puts.injected_before, 1);
    assert_eq!(puts.uploaded_bytes, 0);
    assert_eq!(metrics.events[0].outcome, RequestOutcome::InjectedBefore);
    Ok(())
}

#[tokio::test]
async fn failure_after_conditional_put_hides_visible_change() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let path = Path::from("logs/test/head");
    let initial = store
        .put(&path, Bytes::from_static(b"initial").into())
        .await?;
    store.reset();
    store.fail_next(Operation::Put, FailurePhase::After);

    let options = PutOptions {
        mode: PutMode::Update(UpdateVersion::from(initial)),
        ..PutOptions::default()
    };
    let error = store
        .put_opts(&path, Bytes::from_static(b"committed").into(), options)
        .await
        .err()
        .ok_or_else(|| test_error("injected write returned success"))?;
    assert!(FaultStore::is_injected(&error));

    let stored = store.get(&path).await?.bytes().await?;
    assert_eq!(stored, Bytes::from_static(b"committed"));
    let metrics = store.metrics();
    let puts = metrics.operation(Operation::Put);
    assert_eq!(puts.requests, 1);
    assert_eq!(puts.visible_mutations, 1);
    assert_eq!(puts.injected_after, 1);
    assert_eq!(puts.uploaded_bytes, 9);
    assert_eq!(metrics.events[0].outcome, RequestOutcome::InjectedAfter);
    Ok(())
}

#[tokio::test]
async fn scheduled_occurrence_and_read_failure_are_reproducible() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let first = Path::from("logs/test/first");
    let second = Path::from("logs/test/second");
    store.schedule(Failure {
        operation: Operation::Put,
        occurrence: 2,
        phase: FailurePhase::Before,
    });

    store.put(&first, Bytes::from_static(b"one").into()).await?;
    let second_error = store
        .put(&second, Bytes::from_static(b"two").into())
        .await
        .err()
        .ok_or_else(|| test_error("second write did not fail"))?;
    assert!(FaultStore::is_injected(&second_error));
    assert!(store.pending_failures().is_empty());

    store.fail_next(Operation::Get, FailurePhase::Before);
    let read_error = store
        .get(&first)
        .await
        .err()
        .ok_or_else(|| test_error("read did not fail"))?;
    assert!(FaultStore::is_injected(&read_error));
    let stored = store.get(&first).await?.bytes().await?;
    assert_eq!(stored, Bytes::from_static(b"one"));

    let metrics = store.metrics();
    assert_eq!(metrics.operation(Operation::Put).requests, 2);
    assert_eq!(metrics.operation(Operation::Get).requests, 2);
    assert_eq!(metrics.total_requests(), 4);
    assert_eq!(metrics.uploaded_bytes(), 3);
    assert_eq!(metrics.downloaded_bytes(), 3);
    assert_eq!(
        metrics
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    Ok(())
}

#[tokio::test]
async fn clones_share_atomic_request_accounting() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let left = store.clone();
    let right = store.clone();
    let left_path = Path::from("logs/test/left");
    let right_path = Path::from("logs/test/right");

    let (left_result, right_result) = tokio::join!(
        left.put(&left_path, Bytes::from(vec![1_u8; 32]).into()),
        right.put(&right_path, Bytes::from(vec![2_u8; 64]).into())
    );
    left_result?;
    right_result?;

    let metrics = store.metrics();
    let puts = metrics.operation(Operation::Put);
    assert_eq!(puts.requests, 2);
    assert_eq!(puts.succeeded, 2);
    assert_eq!(puts.visible_mutations, 2);
    assert_eq!(puts.uploaded_bytes, 96);
    assert_eq!(metrics.events.len(), 2);
    assert_ne!(metrics.events[0].sequence, metrics.events[1].sequence);
    Ok(())
}

#[tokio::test]
async fn failure_after_delete_hides_a_visible_delete() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let path = Path::from("logs/test/to-delete");
    store
        .put(&path, Bytes::from_static(b"present").into())
        .await?;
    store.reset();
    store.fail_next(Operation::Delete, FailurePhase::After);

    let error = store
        .delete(&path)
        .await
        .err()
        .ok_or_else(|| test_error("injected delete returned success"))?;
    assert!(FaultStore::is_injected(&error));
    assert!(matches!(
        store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let deletes = store.metrics().operation(Operation::Delete);
    assert_eq!(deletes.requests, 1);
    assert_eq!(deletes.visible_mutations, 1);
    assert_eq!(deletes.injected_after, 1);
    Ok(())
}

#[tokio::test]
async fn failure_after_multipart_completion_hides_visible_object() -> TestResult {
    let store = FaultStore::new(InMemory::new());
    let path = Path::from("logs/test/multipart");
    let mut upload = store.put_multipart(&path).await?;
    upload
        .put_part(Bytes::from_static(b"payload").into())
        .await?;
    store.fail_next(Operation::MultipartComplete, FailurePhase::After);

    let error = upload
        .complete()
        .await
        .err()
        .ok_or_else(|| test_error("injected completion returned success"))?;
    assert!(FaultStore::is_injected(&error));
    let stored = store.get(&path).await?.bytes().await?;
    assert_eq!(stored, Bytes::from_static(b"payload"));

    let metrics = store.metrics();
    assert_eq!(
        metrics.operation(Operation::MultipartPart).uploaded_bytes,
        7
    );
    let complete = metrics.operation(Operation::MultipartComplete);
    assert_eq!(complete.visible_mutations, 1);
    assert_eq!(complete.injected_after, 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_writers_have_one_winner_and_one_definite_loser() -> TestResult {
    let (store, log, _) = open_model_log(7).await?;
    store.reset();
    let initial = log.load().await?;
    let left_id = transaction_id(7, 1);
    let right_id = transaction_id(7, 2);
    let left = log.prepare(
        &initial,
        left_id,
        Bytes::from_static(b"left"),
        Bytes::new(),
        Vec::new(),
    )?;
    let right = log.prepare(
        &initial,
        right_id,
        Bytes::from_static(b"right"),
        Bytes::new(),
        Vec::new(),
    )?;

    let (left_status, right_status) = tokio::join!(log.commit(left), log.commit(right));
    let left_status = left_status?;
    let right_status = right_status?;
    assert!(matches!(
        (&left_status, &right_status),
        (CommitStatus::Committed(_), CommitStatus::Conflict(_))
            | (CommitStatus::Conflict(_), CommitStatus::Committed(_))
    ));

    let view = log.load().await?;
    let records = log.read_tail(&view).await?;
    assert_eq!(records.len(), 1);
    let winner = records[0].reference().transaction_id();
    assert!(winner == left_id || winner == right_id);
    let loser = if winner == left_id { right_id } else { left_id };
    assert!(
        !records
            .iter()
            .any(|record| record.reference().transaction_id() == loser)
    );
    assert_eq!(
        store.metrics().operation(Operation::Put).visible_mutations,
        3
    );
    Ok(())
}

#[tokio::test]
async fn pending_evidence_survives_reopen_and_failed_resolution_read() -> TestResult {
    let (store, log, log_id) = open_model_log(11).await?;
    store.reset();
    let view = log.load().await?;
    let prepared = log.prepare(
        &view,
        transaction_id(11, 1),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"result"),
        Vec::new(),
    )?;
    let recovery_token = prepared.recovery_token()?;
    schedule_head_fault(&store, FailurePhase::After);
    match log.commit(prepared).await? {
        CommitStatus::Pending(_) => {}
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            return Err(test_error("lost response did not produce pending evidence").into());
        }
    }
    drop(log);

    let reopened = reopen_model_log(&store, &log_id).await?;
    store.fail_next(Operation::Get, FailurePhase::Before);
    match reopened.resume(&recovery_token).await? {
        Resolution::StillPending(_) => {}
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed resolution read was classified as definite").into());
        }
    }
    drop(reopened);

    let reopened = reopen_model_log(&store, &log_id).await?;
    let committed = match reopened.resume(&recovery_token).await? {
        Resolution::Committed(view) => view,
        Resolution::NotCommitted(_) | Resolution::StillPending(_) | Resolution::Expired(_) => {
            return Err(test_error("visible pending commit did not resolve as committed").into());
        }
    };
    let records = reopened.read_tail(&committed).await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].operation(), &Bytes::from_static(b"operation"));
    assert_eq!(records[0].result(), &Bytes::from_static(b"result"));
    Ok(())
}

#[tokio::test]
async fn recovery_token_can_stage_and_publish_after_process_loss() -> TestResult {
    let (store, log, log_id) = open_model_log(14).await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        &view,
        transaction_id(14, 1),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"result"),
        Vec::new(),
    )?;
    let recovery_token = prepared.recovery_token()?;
    drop(prepared);
    drop(log);

    let reopened = reopen_model_log(&store, &log_id).await?;
    let committed = match reopened.resume(&recovery_token).await? {
        Resolution::Committed(view) => view,
        Resolution::NotCommitted(_) | Resolution::StillPending(_) | Resolution::Expired(_) => {
            return Err(test_error("persisted candidate did not resume").into());
        }
    };
    assert_eq!(reopened.read_tail(&committed).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn recovery_token_survives_failed_referenced_object_validation() -> TestResult {
    let (store, log, _) = open_model_log(12).await?;
    store.reset();
    let view = log.load().await?;
    let object = log
        .put_object(&view, Bytes::from_static(b"referenced object"))
        .await?;
    let prepared = log.prepare(
        &view,
        transaction_id(12, 1),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        vec![object],
    )?;
    let token = prepared.recovery_token()?;
    schedule_head_fault(&store, FailurePhase::Before);
    match log.commit(prepared).await? {
        CommitStatus::Pending(_) => {}
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            return Err(test_error("failed publication did not remain pending").into());
        }
    }

    store.reset();
    store.schedule(Failure {
        operation: Operation::Get,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    match log.resume(&token).await? {
        Resolution::StillPending(_) => {}
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed object validation discarded pending evidence").into());
        }
    }

    assert!(matches!(
        log.resume(&token).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn staged_blob_and_node_publish_without_dependency_reads() -> TestResult {
    let (store, log, _) = open_model_log(120).await?;
    let view = log.load().await?;
    let child = log.put_object(&view, Bytes::from_static(b"child")).await?;

    store.reset();
    let node = log
        .put_node(&view, Bytes::from_static(b"node"), vec![child])
        .await?;
    assert_eq!(store.metrics().operation(Operation::Get).requests, 0);

    store.reset();
    let prepared = log.prepare(
        &view,
        transaction_id(120, 1),
        Bytes::new(),
        Bytes::new(),
        vec![node],
    )?;
    assert!(matches!(
        log.commit(prepared).await?,
        CommitStatus::Committed(_)
    ));
    assert_eq!(store.metrics().operation(Operation::Get).requests, 0);
    Ok(())
}

#[tokio::test]
async fn recovery_token_discards_staging_proof_and_verifies_the_blob() -> TestResult {
    let (store, log, _) = open_model_log(121).await?;
    let view = log.load().await?;
    let object = log
        .put_object(&view, Bytes::from_static(b"recover me"))
        .await?;
    let prepared = log.prepare(
        &view,
        transaction_id(121, 1),
        Bytes::new(),
        Bytes::new(),
        vec![object],
    )?;
    let token = prepared.recovery_token()?;

    store.reset();
    assert!(matches!(
        log.resume(&token).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(segment_gets(&store, "blobs"), 1);
    Ok(())
}

#[tokio::test]
async fn recovery_token_rejects_missing_and_corrupt_blobs_before_head_update() -> TestResult {
    for (seed, corrupt) in [(126, false), (127, true)] {
        let (store, log, _) = open_model_log(seed).await?;
        let view = log.load().await?;
        let object = log
            .put_object(&view, Bytes::from_static(b"original"))
            .await?;
        let blob_path = segment_path(&store, "blobs")?;
        let prepared = log.prepare(
            &view,
            transaction_id(seed, 1),
            Bytes::new(),
            Bytes::new(),
            vec![object],
        )?;
        let token = prepared.recovery_token()?;
        if corrupt {
            store
                .put(&blob_path, Bytes::from_static(b"changed!").into())
                .await?;
        } else {
            store.delete(&blob_path).await?;
        }

        store.reset();
        match (corrupt, log.resume(&token).await) {
            (false, Err(object_log::Error::InvalidFormat(_)))
            | (true, Err(object_log::Error::CorruptObject)) => {}
            _ => return Err(test_error("invalid blob recovery did not fail closed").into()),
        }
        assert_eq!(segment_gets(&store, "blobs"), 1);
        assert_eq!(head_puts(&store), 0);
    }
    Ok(())
}

#[tokio::test]
async fn decoded_published_commit_rejects_missing_and_corrupt_descendants() -> TestResult {
    for (seed, corrupt) in [(128, false), (129, true)] {
        let (store, log, log_id) = open_model_log(seed).await?;
        let view = log.load().await?;
        let child = log.put_object(&view, Bytes::from_static(b"child")).await?;
        let blob_path = segment_path(&store, "blobs")?;
        let node = log
            .put_node(&view, Bytes::from_static(b"node"), vec![child])
            .await?;
        let prepared = log.prepare(
            &view,
            transaction_id(seed, 1),
            Bytes::new(),
            Bytes::new(),
            vec![node],
        )?;
        let token = prepared.recovery_token()?;
        store.reset();
        schedule_head_fault(&store, FailurePhase::After);
        assert!(matches!(
            log.commit(prepared).await?,
            CommitStatus::Pending(_)
        ));
        if corrupt {
            store
                .put(&blob_path, Bytes::from_static(b"bad!!").into())
                .await?;
        } else {
            store.delete(&blob_path).await?;
        }
        drop(log);

        let reopened = reopen_model_log(&store, &log_id).await?;
        store.reset();
        match (corrupt, reopened.resume(&token).await) {
            (false, Err(object_log::Error::InvalidFormat(_)))
            | (true, Err(object_log::Error::CorruptObject)) => {}
            _ => return Err(test_error("invalid descendant recovery did not fail closed").into()),
        }
        assert_eq!(segment_gets(&store, "commits"), 1);
        assert_eq!(segment_gets(&store, "nodes"), 1);
        assert_eq!(segment_gets(&store, "blobs"), 1);
        assert_eq!(head_puts(&store), 0);
    }
    Ok(())
}

#[tokio::test]
async fn batched_existing_staging_deduplicates_the_object_graph() -> TestResult {
    let (store, log, _) = open_model_log(122).await?;
    let view = log.load().await?;
    let child = log.put_object(&view, Bytes::from_static(b"child")).await?;
    let node = log
        .put_node(&view, Bytes::from_static(b"node"), vec![child])
        .await?;

    store.reset();
    let staged = log
        .stage_objects(
            &view,
            vec![node.reference().clone(), node.reference().clone()],
        )
        .await?;
    assert_eq!(staged.len(), 2);
    assert_eq!(segment_gets(&store, "nodes"), 1);
    assert_eq!(segment_gets(&store, "blobs"), 1);
    Ok(())
}

#[tokio::test]
async fn separately_opened_handle_rejects_new_work_with_foreign_proof() -> TestResult {
    let (store, first, log_id) = open_model_log(123).await?;
    let first_view = first.load().await?;
    let object = first
        .put_object(&first_view, Bytes::from_static(b"isolated proof"))
        .await?;
    let second = reopen_model_log(&store, &log_id).await?;
    let second_view = second.load().await?;

    assert!(matches!(
        second.prepare(
            &second_view,
            transaction_id(123, 1),
            Bytes::new(),
            Bytes::new(),
            vec![object],
        ),
        Err(object_log::Error::InvalidStagedObject)
    ));
    Ok(())
}

#[tokio::test]
async fn separately_opened_handle_verifies_prepared_and_pending_work() -> TestResult {
    let (store, first, log_id) = open_model_log(124).await?;
    let view = first.load().await?;
    let object = first
        .put_object(&view, Bytes::from_static(b"verify on reopen"))
        .await?;
    let prepared = first.prepare(
        &view,
        transaction_id(124, 1),
        Bytes::new(),
        Bytes::new(),
        vec![object],
    )?;
    let reopened = reopen_model_log(&store, &log_id).await?;

    store.reset();
    assert!(matches!(
        reopened.commit(prepared).await?,
        CommitStatus::Committed(_)
    ));
    assert_eq!(segment_gets(&store, "blobs"), 1);

    let next = reopened.load().await?;
    let object = reopened
        .put_object(&next, Bytes::from_static(b"pending verify"))
        .await?;
    let prepared = reopened.prepare(
        &next,
        transaction_id(124, 2),
        Bytes::new(),
        Bytes::new(),
        vec![object],
    )?;
    store.reset();
    schedule_head_fault(&store, FailurePhase::Before);
    let CommitStatus::Pending(pending) = reopened.commit(prepared).await? else {
        return Err(test_error("failed update did not return pending").into());
    };
    let third = reopen_model_log(&store, &log_id).await?;
    store.reset();
    assert!(matches!(
        third.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(segment_gets(&store, "blobs"), 1);
    Ok(())
}

#[tokio::test]
async fn same_handle_pending_resolution_keeps_staging_proof() -> TestResult {
    let (store, log, _) = open_model_log(125).await?;
    let view = log.load().await?;
    let child = log.put_object(&view, Bytes::from_static(b"child")).await?;
    let node = log
        .put_node(&view, Bytes::from_static(b"node"), vec![child])
        .await?;
    let prepared = log.prepare(
        &view,
        transaction_id(125, 1),
        Bytes::new(),
        Bytes::new(),
        vec![node],
    )?;
    store.reset();
    schedule_head_fault(&store, FailurePhase::Before);
    let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
        return Err(test_error("failed update did not return pending").into());
    };

    store.reset();
    assert!(matches!(
        log.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(segment_gets(&store, "blobs"), 0);
    assert_eq!(segment_gets(&store, "nodes"), 0);
    assert_eq!(segment_gets(&store, "commits"), 0);
    Ok(())
}

#[tokio::test]
async fn pending_evidence_survives_failed_published_commit_verification() -> TestResult {
    let (store, log, log_id) = open_model_log(13).await?;
    store.reset();
    let view = log.load().await?;
    let prepared = log.prepare(
        &view,
        transaction_id(13, 1),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        Vec::new(),
    )?;
    schedule_head_fault(&store, FailurePhase::After);
    let pending = match log.commit(prepared).await? {
        CommitStatus::Pending(pending) => pending,
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            return Err(test_error("lost response did not remain pending").into());
        }
    };
    let reopened = reopen_model_log(&store, &log_id).await?;

    store.reset();
    store.schedule(Failure {
        operation: Operation::Get,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let pending = match reopened.resolve(pending).await? {
        Resolution::StillPending(pending) => pending,
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed commit verification discarded pending evidence").into());
        }
    };

    assert!(matches!(
        reopened.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn seeded_model_covers_reopen_readers_writers_and_pending_resolution() -> TestResult {
    for seed in [
        0x1_u64,
        0x5eed_u64,
        0x9e37_79b9_u64,
        0xd1b5_4a32_d192_ed03_u64,
    ] {
        run_scenario(seed, 96).await?;
    }
    Ok(())
}

#[derive(Debug)]
struct Writer {
    view: Option<View>,
    pending: Option<PendingCommit>,
}

async fn run_scenario(seed: u64, steps: usize) -> TestResult {
    let mut scenario = Scenario::new(seed, steps).await?;
    for step in 0..steps {
        scenario.step(step).await?;
        scenario.check().await?;
    }
    scenario.finish().await?;
    Ok(())
}

struct Scenario {
    seed: u64,
    store: FaultStore,
    log: Log,
    log_id: LogId,
    writers: [Writer; 2],
    reader: Option<View>,
    accepted: HashSet<TransactionId>,
    rejected: HashSet<TransactionId>,
    prior_history: Vec<TransactionId>,
    next_transaction: u64,
    random: Seeded,
    trace: Vec<String>,
}

impl Scenario {
    async fn new(seed: u64, steps: usize) -> Result<Self, Box<dyn StdError>> {
        let (store, log, log_id) = open_model_log(seed).await?;
        store.reset();
        let initial = log.load().await?;
        Ok(Self {
            seed,
            store,
            log,
            log_id,
            writers: [
                Writer {
                    view: Some(initial.clone()),
                    pending: None,
                },
                Writer {
                    view: Some(initial.clone()),
                    pending: None,
                },
            ],
            reader: Some(initial),
            accepted: HashSet::new(),
            rejected: HashSet::new(),
            prior_history: Vec::new(),
            next_transaction: 1,
            random: Seeded::new(seed),
            trace: Vec::with_capacity(steps),
        })
    }

    async fn step(&mut self, step: usize) -> TestResult {
        let choice = self.random.next() % 12;
        let writer = usize::from(!self.random.next().is_multiple_of(2));
        match choice {
            0..=5 if self.writers[writer].pending.is_some() => {
                self.resolve(writer, false).await?;
            }
            0..=5 => {
                self.commit(writer, choice).await?;
                self.next_transaction = self
                    .next_transaction
                    .checked_add(1)
                    .ok_or_else(|| test_error("model transaction counter overflowed"))?;
            }
            6 => {
                let fail_read = self.random.next().is_multiple_of(4);
                self.resolve(writer, fail_read).await?;
            }
            7 | 8 => {
                self.refresh_reader().await?;
                self.trace.push(format!("{step}: reader refresh"));
            }
            9 => {
                self.writers[writer].view = Some(self.log.load().await?);
                self.trace.push(format!("{step}: writer {writer} reload"));
            }
            10 => {
                self.log = reopen_model_log(&self.store, &self.log_id).await?;
                for writer in &mut self.writers {
                    writer.view = None;
                }
                self.reader = None;
                self.trace.push(format!("{step}: crash and reopen"));
            }
            _ => {
                let view = self.log.load().await?;
                self.log.read_tail(&view).await?;
                self.trace.push(format!("{step}: recovery read"));
            }
        }
        Ok(())
    }

    async fn commit(&mut self, writer: usize, fault_choice: u64) -> TestResult {
        if self.writers[writer].view.is_none() {
            self.writers[writer].view = Some(self.log.load().await?);
        }
        let view = self.writers[writer]
            .view
            .as_ref()
            .ok_or_else(|| test_error("writer view did not load"))?;
        let transaction_id = transaction_id(self.seed, self.next_transaction);
        let mut operation = Vec::with_capacity(24);
        operation.extend_from_slice(&self.seed.to_le_bytes());
        operation.extend_from_slice(&self.next_transaction.to_le_bytes());
        operation.extend_from_slice(&[0_u64, 1][writer].to_le_bytes());
        let prepared = self.log.prepare(
            view,
            transaction_id,
            Bytes::from(operation),
            Bytes::copy_from_slice(&self.next_transaction.to_le_bytes()),
            Vec::new(),
        )?;
        let fault = match fault_choice {
            0 => Some(FailurePhase::Before),
            1 => Some(FailurePhase::After),
            _ => None,
        };
        if let Some(phase) = fault {
            schedule_head_fault(&self.store, phase);
        }
        match self.log.commit(prepared).await? {
            CommitStatus::Committed(view) => {
                self.accepted.insert(transaction_id);
                self.writers[writer].view = Some(view);
                self.trace.push(format!(
                    "writer {writer} committed {}",
                    self.next_transaction
                ));
            }
            CommitStatus::Conflict(view) => {
                self.rejected.insert(transaction_id);
                self.writers[writer].view = Some(view);
                self.trace.push(format!(
                    "writer {writer} conflicted {}",
                    self.next_transaction
                ));
            }
            CommitStatus::Pending(pending) => {
                self.writers[writer].pending = Some(pending);
                self.trace.push(format!(
                    "writer {writer} pending {} {fault:?}",
                    self.next_transaction
                ));
            }
        }
        Ok(())
    }

    async fn resolve(&mut self, writer: usize, fail_read: bool) -> TestResult {
        let Some(pending) = self.writers[writer].pending.take() else {
            self.trace.push("resolve skipped".to_owned());
            return Ok(());
        };
        let transaction_id = pending.transaction_id();
        if fail_read {
            self.store.fail_next(Operation::Get, FailurePhase::Before);
        }
        match self.log.resolve(pending).await? {
            Resolution::Committed(view) => {
                self.accepted.insert(transaction_id);
                self.writers[writer].view = Some(view);
                self.trace
                    .push(format!("resolved {transaction_id} committed"));
            }
            Resolution::NotCommitted(view) => {
                self.rejected.insert(transaction_id);
                self.writers[writer].view = Some(view);
                self.trace
                    .push(format!("resolved {transaction_id} not committed"));
            }
            Resolution::StillPending(pending) => {
                self.writers[writer].pending = Some(pending);
                self.trace
                    .push(format!("resolved {transaction_id} still pending"));
            }
            Resolution::Expired(view) => {
                self.writers[writer].view = Some(view);
                return Err(test_error("pending result expired without checkpointing").into());
            }
        }
        Ok(())
    }

    async fn refresh_reader(&mut self) -> TestResult {
        let Some(view) = self.reader.as_ref() else {
            self.reader = Some(self.log.load().await?);
            return Ok(());
        };
        match self.log.refresh(view).await? {
            None => {}
            Some(updated) => self.reader = Some(updated),
        }
        Ok(())
    }

    async fn finish(&mut self) -> TestResult {
        for writer in 0..self.writers.len() {
            while self.writers[writer].pending.is_some() {
                self.resolve(writer, false).await?;
            }
        }
        self.check().await
    }

    async fn check(&mut self) -> TestResult {
        let durable_view = self.log.load().await?;
        assert!(
            durable_view.checkpoint().is_none(),
            "seed {:#x}: {:#?}",
            self.seed,
            self.trace
        );
        let records = self.log.read_tail(&durable_view).await?;
        let history = records
            .iter()
            .map(|record| record.reference().transaction_id())
            .collect::<Vec<_>>();
        assert!(
            history.starts_with(&self.prior_history),
            "history shrank for seed {:#x}: {:#?}",
            self.seed,
            self.trace
        );
        assert_eq!(records.len(), durable_view.tail().len());
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.reference(), &durable_view.tail()[index]);
            assert_eq!(record.reference().sequence(), u64::try_from(index)?);
        }
        let unique = history.iter().copied().collect::<HashSet<_>>();
        assert_eq!(unique.len(), history.len());
        assert!(self.accepted.iter().all(|transaction| {
            history
                .iter()
                .filter(|candidate| *candidate == transaction)
                .count()
                == 1
        }));
        assert!(
            self.rejected
                .iter()
                .all(|transaction| !history.contains(transaction))
        );
        for pending in self
            .writers
            .iter()
            .filter_map(|writer| writer.pending.as_ref())
        {
            assert!(
                history
                    .iter()
                    .filter(|transaction| **transaction == pending.transaction_id())
                    .count()
                    <= 1
            );
        }
        if let Some(reader) = self.reader.as_ref() {
            let reader_history = reader
                .tail()
                .iter()
                .map(CommitRef::transaction_id)
                .collect::<Vec<_>>();
            assert!(history.starts_with(&reader_history));
        }
        self.prior_history = history;
        Ok(())
    }
}

async fn open_model_log(seed: u64) -> Result<(FaultStore, Log, LogId), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let log_id = LogId::new(format!("model-{seed:016x}"))?;
    let log = reopen_model_log(&store, &log_id).await?;
    Ok((store, log, log_id))
}

async fn reopen_model_log(store: &FaultStore, log_id: &LogId) -> Result<Log, object_log::Error> {
    let backend = ValidatedBackend::new(Arc::new(store.clone()), Path::from("model-tests")).await?;
    Log::open(&backend, log_id, Options::default()).await
}

fn schedule_head_fault(store: &FaultStore, phase: FailurePhase) {
    let occurrence = store
        .metrics()
        .operation(Operation::Put)
        .requests
        .saturating_add(2);
    store.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase,
    });
}

fn segment_gets(store: &FaultStore, segment: &str) -> usize {
    let marker = format!("/{segment}/");
    store
        .metrics()
        .events
        .iter()
        .filter(|event| event.operation == Operation::Get && event.path.contains(&marker))
        .count()
}

fn head_puts(store: &FaultStore) -> usize {
    store
        .metrics()
        .events
        .iter()
        .filter(|event| event.operation == Operation::Put && event.path.ends_with("/index.cbor"))
        .count()
}

fn segment_path(store: &FaultStore, segment: &str) -> Result<Path, Box<dyn StdError>> {
    let marker = format!("/{segment}/");
    store
        .metrics()
        .events
        .iter()
        .find(|event| event.operation == Operation::Put && event.path.contains(&marker))
        .map(|event| Path::from(event.path.clone()))
        .ok_or_else(|| test_error("immutable test object is missing").into())
}

fn transaction_id(seed: u64, number: u64) -> TransactionId {
    TransactionId::from_uuid(Uuid::from_u128(
        (u128::from(seed) << 64) | u128::from(number),
    ))
}

#[derive(Debug)]
struct Seeded(u64);

impl Seeded {
    const fn new(seed: u64) -> Self {
        Self(seed ^ 0xa076_1d64_78bd_642f)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0 = self.0.wrapping_mul(0x2545_f491_4f6c_dd1d);
        self.0
    }
}

fn test_error(message: &'static str) -> std::io::Error {
    std::io::Error::other(message)
}
