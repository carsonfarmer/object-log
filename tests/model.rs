#![cfg(feature = "test-util")]

use bytes::Bytes;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation, RequestOutcome};
use object_log::{
    CommitStatus, Log, LogId, Options, PendingCommit, Refresh, Resolution, ScopedStore,
    TransactionId, View,
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
        initial.cursor(),
        left_id,
        Bytes::from_static(b"left"),
        Bytes::new(),
        Vec::new(),
    )?;
    let right = log.prepare(
        initial.cursor(),
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
    let winner = records[0].reference().transaction_id;
    assert!(winner == left_id || winner == right_id);
    let loser = if winner == left_id { right_id } else { left_id };
    assert!(
        !records
            .iter()
            .any(|record| record.reference().transaction_id == loser)
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
        view.cursor(),
        transaction_id(11, 1),
        Bytes::from_static(b"operation"),
        Bytes::from_static(b"result"),
        Vec::new(),
    )?;
    schedule_head_fault(&store, FailurePhase::After);
    let pending = match log.commit(prepared).await? {
        CommitStatus::Pending(pending) => pending,
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            return Err(test_error("lost response did not produce pending evidence").into());
        }
    };

    let reopened = reopen_model_log(&store, &log_id).await?;
    store.fail_next(Operation::Get, FailurePhase::Before);
    let pending = match reopened.resolve(pending).await? {
        Resolution::StillPending(pending) => pending,
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed resolution read was classified as definite").into());
        }
    };

    let reopened = reopen_model_log(&store, &log_id).await?;
    let committed = match reopened.resolve(pending).await? {
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
async fn pending_evidence_survives_failed_referenced_object_validation() -> TestResult {
    let (store, log, _) = open_model_log(12).await?;
    store.reset();
    let object = log
        .put_object(Bytes::from_static(b"referenced object"))
        .await?;
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
        transaction_id(12, 1),
        Bytes::from_static(b"operation"),
        Bytes::new(),
        vec![object],
    )?;
    schedule_head_fault(&store, FailurePhase::Before);
    let pending = match log.commit(prepared).await? {
        CommitStatus::Pending(pending) => pending,
        CommitStatus::Committed(_) | CommitStatus::Conflict(_) => {
            return Err(test_error("failed publication did not remain pending").into());
        }
    };

    store.reset();
    store.schedule(Failure {
        operation: Operation::Get,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let pending = match log.resolve(pending).await? {
        Resolution::StillPending(pending) => pending,
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed object validation discarded pending evidence").into());
        }
    };

    assert!(matches!(
        log.resolve(pending).await?,
        Resolution::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn pending_evidence_survives_failed_published_commit_verification() -> TestResult {
    let (store, log, _) = open_model_log(13).await?;
    store.reset();
    let view = log.load().await?;
    let prepared = log.prepare(
        view.cursor(),
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

    store.reset();
    store.schedule(Failure {
        operation: Operation::Get,
        occurrence: 2,
        phase: FailurePhase::Before,
    });
    let pending = match log.resolve(pending).await? {
        Resolution::StillPending(pending) => pending,
        Resolution::Committed(_) | Resolution::NotCommitted(_) | Resolution::Expired(_) => {
            return Err(test_error("failed commit verification discarded pending evidence").into());
        }
    };

    assert!(matches!(
        log.resolve(pending).await?,
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

#[allow(clippy::too_many_lines)]
async fn run_scenario(seed: u64, steps: usize) -> TestResult {
    let (store, mut log, log_id) = open_model_log(seed).await?;
    store.reset();
    let initial = log.load().await?;
    let mut writers = [
        Writer {
            view: Some(initial.clone()),
            pending: None,
        },
        Writer {
            view: Some(initial.clone()),
            pending: None,
        },
    ];
    let mut reader = Some(initial);
    let mut accepted = HashSet::new();
    let mut rejected = HashSet::new();
    let mut prior_history = Vec::new();
    let mut next_transaction = 1_u64;
    let mut random = Seeded::new(seed);
    let mut trace = Vec::with_capacity(steps);

    for step in 0..steps {
        let choice = random.next() % 12;
        let writer_index = usize::try_from(random.next() % 2).unwrap_or_default();
        match choice {
            0..=5 => {
                if writers[writer_index].pending.is_some() {
                    resolve_writer(
                        &log,
                        &store,
                        &mut writers[writer_index],
                        &mut accepted,
                        &mut rejected,
                        false,
                        &mut trace,
                    )
                    .await?;
                } else {
                    commit_writer(
                        seed,
                        writer_index,
                        next_transaction,
                        &log,
                        &store,
                        &mut writers[writer_index],
                        &mut accepted,
                        &mut rejected,
                        choice,
                        &mut trace,
                    )
                    .await?;
                    next_transaction = next_transaction.saturating_add(1);
                }
            }
            6 => {
                resolve_writer(
                    &log,
                    &store,
                    &mut writers[writer_index],
                    &mut accepted,
                    &mut rejected,
                    random.next().is_multiple_of(4),
                    &mut trace,
                )
                .await?;
            }
            7 | 8 => {
                refresh_reader(&log, &mut reader).await?;
                trace.push(format!("{step}: reader refresh"));
            }
            9 => {
                writers[writer_index].view = Some(log.load().await?);
                trace.push(format!("{step}: writer {writer_index} reload"));
            }
            10 => {
                log = reopen_model_log(&store, &log_id).await?;
                for writer in &mut writers {
                    writer.view = None;
                }
                reader = None;
                trace.push(format!("{step}: crash and reopen"));
            }
            _ => {
                let view = log.load().await?;
                let _records = log.read_tail(&view).await?;
                trace.push(format!("{step}: recovery read"));
            }
        }

        check_oracle(
            seed,
            &trace,
            &log,
            reader.as_ref(),
            &writers,
            &accepted,
            &rejected,
            &mut prior_history,
        )
        .await?;
    }

    for writer in &mut writers {
        while writer.pending.is_some() {
            resolve_writer(
                &log,
                &store,
                writer,
                &mut accepted,
                &mut rejected,
                false,
                &mut trace,
            )
            .await?;
        }
    }
    check_oracle(
        seed,
        &trace,
        &log,
        reader.as_ref(),
        &writers,
        &accepted,
        &rejected,
        &mut prior_history,
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn commit_writer(
    seed: u64,
    writer_index: usize,
    transaction_number: u64,
    log: &Log,
    store: &FaultStore,
    writer: &mut Writer,
    accepted: &mut HashSet<TransactionId>,
    rejected: &mut HashSet<TransactionId>,
    fault_choice: u64,
    trace: &mut Vec<String>,
) -> TestResult {
    if writer.view.is_none() {
        writer.view = Some(log.load().await?);
    }
    let view = writer
        .view
        .as_ref()
        .ok_or_else(|| test_error("writer view did not load"))?;
    let transaction_id = transaction_id(seed, transaction_number);
    let mut operation = Vec::with_capacity(24);
    operation.extend_from_slice(&seed.to_le_bytes());
    operation.extend_from_slice(&transaction_number.to_le_bytes());
    operation.extend_from_slice(
        &u64::try_from(writer_index)
            .unwrap_or_default()
            .to_le_bytes(),
    );
    let prepared = log.prepare(
        view.cursor(),
        transaction_id,
        Bytes::from(operation),
        Bytes::copy_from_slice(&transaction_number.to_le_bytes()),
        Vec::new(),
    )?;

    let fault = match fault_choice {
        0 => Some(FailurePhase::Before),
        1 => Some(FailurePhase::After),
        _ => None,
    };
    if let Some(phase) = fault {
        schedule_head_fault(store, phase);
    }

    match log.commit(prepared).await? {
        CommitStatus::Committed(view) => {
            accepted.insert(transaction_id);
            writer.view = Some(view);
            trace.push(format!(
                "writer {writer_index} committed {transaction_number}"
            ));
        }
        CommitStatus::Conflict(view) => {
            rejected.insert(transaction_id);
            writer.view = Some(view);
            trace.push(format!(
                "writer {writer_index} conflicted {transaction_number}"
            ));
        }
        CommitStatus::Pending(pending) => {
            writer.pending = Some(pending);
            trace.push(format!(
                "writer {writer_index} pending {transaction_number} {fault:?}"
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn resolve_writer(
    log: &Log,
    store: &FaultStore,
    writer: &mut Writer,
    accepted: &mut HashSet<TransactionId>,
    rejected: &mut HashSet<TransactionId>,
    fail_read: bool,
    trace: &mut Vec<String>,
) -> TestResult {
    let Some(pending) = writer.pending.take() else {
        trace.push("resolve skipped".to_owned());
        return Ok(());
    };
    let transaction_id = pending.prepared().transaction_id();
    if fail_read {
        store.fail_next(Operation::Get, FailurePhase::Before);
    }
    match log.resolve(pending).await? {
        Resolution::Committed(view) => {
            accepted.insert(transaction_id);
            writer.view = Some(view);
            trace.push(format!("resolved {transaction_id} committed"));
        }
        Resolution::NotCommitted(view) => {
            rejected.insert(transaction_id);
            writer.view = Some(view);
            trace.push(format!("resolved {transaction_id} not committed"));
        }
        Resolution::StillPending(pending) => {
            writer.pending = Some(pending);
            trace.push(format!("resolved {transaction_id} still pending"));
        }
        Resolution::Expired(view) => {
            writer.view = Some(view);
            return Err(test_error("pending result expired without checkpointing").into());
        }
    }
    Ok(())
}

async fn refresh_reader(log: &Log, reader: &mut Option<View>) -> TestResult {
    let Some(view) = reader.as_ref() else {
        *reader = Some(log.load().await?);
        return Ok(());
    };
    match log.refresh(view.cursor()).await? {
        Refresh::NotModified => {}
        Refresh::Updated(updated) => *reader = Some(*updated),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn check_oracle(
    seed: u64,
    trace: &[String],
    log: &Log,
    reader: Option<&View>,
    writers: &[Writer; 2],
    accepted: &HashSet<TransactionId>,
    rejected: &HashSet<TransactionId>,
    prior_history: &mut Vec<TransactionId>,
) -> TestResult {
    let durable_view = log.load().await?;
    assert!(
        durable_view.checkpoint().is_none(),
        "seed {seed:#x}: {trace:#?}"
    );
    let records = log.read_tail(&durable_view).await?;
    let history = records
        .iter()
        .map(|record| record.reference().transaction_id)
        .collect::<Vec<_>>();
    assert!(
        history.starts_with(prior_history),
        "history shrank for seed {seed:#x}: {trace:#?}"
    );
    assert_eq!(
        records.len(),
        durable_view.tail().len(),
        "tail load mismatch for seed {seed:#x}: {trace:#?}"
    );
    for (index, record) in records.iter().enumerate() {
        assert_eq!(
            record.reference(),
            &durable_view.tail()[index],
            "head/record mismatch for seed {seed:#x}: {trace:#?}"
        );
        assert_eq!(
            record.reference().sequence,
            u64::try_from(index).unwrap_or_default(),
            "non-contiguous sequence for seed {seed:#x}: {trace:#?}"
        );
    }
    let unique = history.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        unique.len(),
        history.len(),
        "duplicate transaction for seed {seed:#x}: {trace:#?}"
    );
    assert!(
        accepted.iter().all(|transaction| {
            history
                .iter()
                .filter(|candidate| *candidate == transaction)
                .count()
                == 1
        }),
        "acknowledged transaction missing for seed {seed:#x}: {trace:#?}"
    );
    assert!(
        rejected
            .iter()
            .all(|transaction| !history.contains(transaction)),
        "conflict candidate visible for seed {seed:#x}: {trace:#?}"
    );
    for pending in writers.iter().filter_map(|writer| writer.pending.as_ref()) {
        let occurrences = history
            .iter()
            .filter(|transaction| **transaction == pending.prepared().transaction_id())
            .count();
        assert!(
            occurrences <= 1,
            "pending transaction duplicated for seed {seed:#x}: {trace:#?}"
        );
    }
    if let Some(reader) = reader {
        let reader_history = reader
            .tail()
            .iter()
            .map(|reference| reference.transaction_id)
            .collect::<Vec<_>>();
        assert!(
            history.starts_with(&reader_history),
            "reader is not a prefix for seed {seed:#x}: {trace:#?}"
        );
    }
    *prior_history = history;
    Ok(())
}

async fn open_model_log(seed: u64) -> Result<(FaultStore, Log, LogId), Box<dyn StdError>> {
    let store = FaultStore::new(InMemory::new());
    let log_id = LogId::new(format!("model-{seed:016x}"))?;
    let log = reopen_model_log(&store, &log_id).await?;
    Ok((store, log, log_id))
}

async fn reopen_model_log(store: &FaultStore, log_id: &LogId) -> Result<Log, object_log::Error> {
    let scoped = ScopedStore::new(Arc::new(store.clone()), Path::from("model-tests"), log_id);
    Log::open(scoped, Options::default()).await
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
