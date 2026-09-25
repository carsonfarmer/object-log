#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::{sync::Arc, time::Duration};

use object_log::{
    Options,
    sim::{Failure, FailurePhase, FaultStore, Operation, RequestOutcome},
};
use object_log_spin_key_value::{HostLimits, Maintenance, Manager};
use object_store::{ObjectStore, memory::InMemory};
use spin_factor_key_value::{Store, StoreManager, SwapError};

type Result<T = ()> = anyhow::Result<T>;

async fn fixture(limits: HostLimits) -> Result<(Arc<FaultStore>, Manager, Arc<dyn Store>)> {
    let fault = Arc::new(FaultStore::new(InMemory::new()));
    let backend: Arc<dyn ObjectStore> = fault.clone();
    let manager = Manager::new(backend, "faults", Options::default(), limits)?;
    let store = manager.get("default").await?;
    fault.reset();
    Ok((fault, manager, store))
}

// Measure the head-publication position without depending on tree node counts.
async fn head_put() -> Result<u64> {
    let (fault, _, store) = fixture(HostLimits::default()).await?;
    store.increment("counter".into(), 1).await?;
    Ok(fault
        .metrics()
        .events
        .iter()
        .find(|e| e.operation == Operation::Put && e.path.ends_with("index.cbor"))
        .unwrap()
        .occurrence)
}

async fn set_head_put() -> Result<u64> {
    let (fault, _, store) = fixture(HostLimits::default()).await?;
    store.set("first", b"one").await?;
    Ok(fault
        .metrics()
        .events
        .iter()
        .find(|e| e.operation == Operation::Put && e.path.ends_with("index.cbor"))
        .unwrap()
        .occurrence)
}

#[tokio::test]
async fn lost_publication_response_is_resolved_after_retry() -> Result {
    let occurrence = head_put().await?;
    let (fault, _, store) = fixture(HostLimits {
        attempts: 2,
        ..HostLimits::default()
    })
    .await?;
    let mut paused = fault.pause_put_at(occurrence, FailurePhase::After);
    fault.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.increment("counter".into(), 1).await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    let failed_get = fault.metrics().operation(Operation::Get).requests + 1;
    fault.fail_next(Operation::Get, FailurePhase::Before);
    assert!(paused.release());
    assert_eq!(task.await??, 1);
    let events = fault.metrics().events;
    assert!(events.iter().any(|event| {
        event.operation == Operation::Get
            && event.occurrence == failed_get
            && event.outcome == RequestOutcome::InjectedBefore
    }));
    assert!(events.iter().any(|event| {
        event.operation == Operation::Get
            && event.occurrence > failed_get
            && event.outcome == RequestOutcome::Succeeded
    }));
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(1i64.to_le_bytes().to_vec())
    );
    let publications: Vec<_> = fault
        .metrics()
        .events
        .into_iter()
        .filter(|e| e.operation == Operation::Put && e.path.ends_with("index.cbor"))
        .collect();
    assert_eq!(publications.len(), 1);
    Ok(())
}

#[tokio::test]
async fn unresolved_publication_reports_unknown_and_never_replays() -> Result {
    let occurrence = head_put().await?;
    let (fault, _, store) = fixture(HostLimits::default()).await?;
    let mut paused = fault.pause_put_at(occurrence, FailurePhase::After);
    fault.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.increment("counter".into(), 1).await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    let next_get = fault.metrics().operation(Operation::Get).requests + 1;
    for occurrence in next_get..next_get + 100 {
        fault.schedule(Failure {
            operation: Operation::Get,
            occurrence,
            phase: FailurePhase::Before,
        });
    }
    assert!(paused.release());
    let error = task.await?.unwrap_err();
    assert!(error.to_string().contains("outcome unknown"));
    fault.clear_failures();
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(1i64.to_le_bytes().to_vec())
    );
    let count = fault
        .metrics()
        .events
        .iter()
        .filter(|e| e.operation == Operation::Put && e.path.ends_with("index.cbor"))
        .count();
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn cas_reports_unknown_after_a_lost_publication_response() -> Result {
    let occurrence = set_head_put().await?;
    let (fault, _, store) = fixture(HostLimits {
        attempts: 2,
        ..HostLimits::default()
    })
    .await?;
    let cas = store.new_compare_and_swap(0, "key").await?;
    assert_eq!(cas.current(usize::MAX).await?, None);
    fault.reset();
    let mut paused = fault.pause_put_at(occurrence, FailurePhase::After);
    fault.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    let writer = tokio::spawn(async move { cas.swap(b"value".to_vec()).await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    let next_get = fault.metrics().operation(Operation::Get).requests + 1;
    for occurrence in next_get..next_get + 100 {
        fault.schedule(Failure {
            operation: Operation::Get,
            occurrence,
            phase: FailurePhase::Before,
        });
    }
    assert!(paused.release());
    assert!(matches!(
        writer.await?.unwrap_err(),
        SwapError::Other(message) if message.contains("outcome unknown")
    ));
    fault.clear_failures();
    assert_eq!(store.get("key", usize::MAX).await?, Some(b"value".to_vec()));
    assert_eq!(
        fault
            .metrics()
            .events
            .iter()
            .filter(|event| event.operation == Operation::Put && event.path.ends_with("index.cbor"))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn definite_conflict_is_retried() -> Result {
    let occurrence = head_put().await?;
    let fault = Arc::new(FaultStore::new(InMemory::new()));
    let backend: Arc<dyn ObjectStore> = fault.clone();
    let limits = HostLimits::default();
    let first = Manager::new(
        backend.clone(),
        "conflict-retry",
        Options::default(),
        limits,
    )?;
    let second = Manager::new(backend, "conflict-retry", Options::default(), limits)?;
    let first = first.get("default").await?;
    let second = second.get("default").await?;
    fault.reset();

    let mut paused = fault.pause_put_at(occurrence, FailurePhase::Before);
    let writer = tokio::spawn(async move { first.increment("counter".into(), 1).await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    assert_eq!(second.increment("counter".into(), 1).await?, 1);
    assert!(paused.release());
    assert_eq!(writer.await??, 2);
    assert_eq!(
        second.get("counter", usize::MAX).await?,
        Some(2i64.to_le_bytes().to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn independent_write_owners_are_fenced_by_the_head() -> Result {
    let occurrence = set_head_put().await?;
    let fault = Arc::new(FaultStore::new(InMemory::new()));
    let backend: Arc<dyn ObjectStore> = fault.clone();
    let limits = HostLimits {
        attempts: 64,
        ..HostLimits::default()
    };
    let first = Manager::new(backend.clone(), "owner-fence", Options::default(), limits)?;
    let second = Manager::new(backend, "owner-fence", Options::default(), limits)?;
    let first = first.get("default").await?;
    let second = second.get("default").await?;
    fault.reset();

    let mut paused = fault.pause_put_at(occurrence, FailurePhase::Before);
    let writer = tokio::spawn(async move { first.set("first", b"one").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    second.set("second", b"two").await?;
    assert!(paused.release());
    writer.await??;
    assert!(fault.metrics().events.iter().any(|event| {
        event.operation == Operation::Put
            && event.path.ends_with("index.cbor")
            && event.outcome == RequestOutcome::BackendError
    }));
    assert_eq!(
        second.get("first", usize::MAX).await?,
        Some(b"one".to_vec())
    );
    assert_eq!(
        second.get("second", usize::MAX).await?,
        Some(b"two".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_keeps_admitted_work_and_permit_until_completion() -> Result {
    let limits = HostLimits {
        concurrent_operations: 1,
        ..HostLimits::default()
    };
    let (fault, _, store) = fixture(limits).await?;
    let mut paused = fault.pause_next_put(FailurePhase::Before);
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.set("key", b"value").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let error = store.set("other", b"value").await.unwrap_err();
    assert!(error.to_string().contains("concurrent operation limit"));
    assert!(paused.release());
    let value = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match store.get("key", usize::MAX).await {
                Ok(value) => break value,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await?;
    assert_eq!(value, Some(b"value".to_vec()));
    Ok(())
}

#[tokio::test]
async fn owner_input_limit_rejects_while_admitted_write_finishes() -> Result {
    let limits = HostLimits {
        concurrent_operations: 2,
        batch_bytes: 4,
        owner_input_bytes: 4,
        ..HostLimits::default()
    };
    let (fault, _, store) = fixture(limits).await?;
    let mut paused = fault.pause_next_put(FailurePhase::Before);
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.set("aa", b"bb").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    let error = store.set("cc", b"dd").await.unwrap_err();
    assert!(error.to_string().contains("owner input limit"));
    assert!(paused.release());
    task.await??;
    assert_eq!(store.get("aa", usize::MAX).await?, Some(b"bb".to_vec()));
    assert_eq!(store.get("cc", usize::MAX).await?, None);
    Ok(())
}

#[tokio::test]
async fn queued_write_expires_before_it_starts() -> Result {
    let limits = HostLimits {
        concurrent_operations: 2,
        owner_wait_ms: 20,
        ..HostLimits::default()
    };
    let (fault, _, store) = fixture(limits).await?;
    let mut paused = fault.pause_next_put(FailurePhase::Before);
    let first = store.clone();
    let first = tokio::spawn(async move { first.set("first", b"one").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    let second = store.clone();
    let second = tokio::spawn(async move { second.set("second", b"two").await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(paused.release());
    first.await??;
    assert!(
        second
            .await?
            .unwrap_err()
            .to_string()
            .contains("owner wait limit")
    );
    assert_eq!(store.get("first", usize::MAX).await?, Some(b"one".to_vec()));
    assert_eq!(store.get("second", usize::MAX).await?, None);
    Ok(())
}

#[tokio::test]
async fn drain_waits_for_admitted_publication_and_closes_manager() -> Result {
    let (fault, host, store) = fixture(HostLimits::default()).await?;
    let mut paused = fault.pause_next_put(FailurePhase::Before);
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.set("key", b"value").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    assert!(!host.drain(Duration::from_millis(10)).await);
    assert!(store.get("key", usize::MAX).await.is_err());
    assert!(paused.release());
    task.await??;
    assert!(host.drain(Duration::from_secs(5)).await);
    assert!(host.drain(Duration::from_secs(5)).await);
    assert!(host.get("default").await.is_err());
    let reopened = Manager::new(fault, "faults", Options::default(), HostLimits::default())?;
    assert_eq!(
        reopened
            .get("default")
            .await?
            .get("key", usize::MAX)
            .await?,
        Some(b"value".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn canceled_caller_keeps_owner_slot_until_publication_finishes() -> Result {
    let limits = HostLimits {
        owner_count: 1,
        ..HostLimits::default()
    };
    let (fault, host, store) = fixture(limits).await?;
    let mut paused = fault.pause_next_put(FailurePhase::Before);
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.set("key", b"value").await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(store);
    assert!(host.get("other").await.is_err());
    assert!(paused.release());
    let other = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match host.get("other").await {
                Ok(store) => break store,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await?;
    other.set("separate", b"value").await?;
    let reopened = Manager::new(fault, "faults", Options::default(), limits)?;
    assert_eq!(
        reopened
            .get("default")
            .await?
            .get("key", usize::MAX)
            .await?,
        Some(b"value".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn logical_request_budget_stops_work() -> Result {
    let (fault, _, store) = fixture(HostLimits {
        requests: 1,
        ..HostLimits::default()
    })
    .await?;
    assert!(store.set("key", b"value").await.is_err());
    assert_eq!(fault.metrics().total_requests(), 1);
    Ok(())
}

#[tokio::test]
async fn unchanged_writes_do_not_commit() -> Result {
    let (fault, _, store) = fixture(HostLimits::default()).await?;
    store.set("present", b"value").await?;
    fault.reset();
    store.set("present", b"value").await?;
    store.delete("absent").await?;
    store
        .set_many(vec![("present".into(), b"value".to_vec())])
        .await?;
    store
        .delete_many(vec!["absent".into(), "missing".into()])
        .await?;
    assert_eq!(
        store.get("present", usize::MAX).await?,
        Some(b"value".to_vec())
    );
    assert!(
        fault.metrics().events.iter().all(|event| {
            event.operation != Operation::Put || !event.path.ends_with("index.cbor")
        })
    );
    Ok(())
}

#[tokio::test]
async fn unchanged_writes_at_checkpoint_threshold_use_no_tail_slot() -> Result {
    let (fault, _, store) = fixture(HostLimits {
        checkpoint_entries: 1,
        ..HostLimits::default()
    })
    .await?;
    store.set("present", b"value").await?;
    fault.reset();
    store.set("present", b"value").await?;
    store.delete("absent").await?;
    let head_puts = |fault: &FaultStore| {
        fault
            .metrics()
            .events
            .iter()
            .filter(|event| event.operation == Operation::Put && event.path.ends_with("index.cbor"))
            .count()
    };
    assert_eq!(head_puts(&fault), 1); // checkpoint only

    store.set("present", b"new").await?;
    fault.reset();
    store.delete("absent").await?;
    assert_eq!(head_puts(&fault), 1); // checkpoint only
    assert_eq!(
        store.get("present", usize::MAX).await?,
        Some(b"new".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn collection_resumes_after_delete_failure_and_host_restart() -> Result {
    let (fault, manager, store) = fixture(HostLimits::default()).await?;
    for i in 0..5 {
        store.set("key", &[i]).await?;
    }
    fault.fail_next(Operation::Delete, FailurePhase::Before);
    assert_eq!(manager.maintain("default").await?, Maintenance::Pending);
    drop(store);
    drop(manager);
    let manager = Manager::new(fault, "faults", Options::default(), HostLimits::default())?;
    let mut complete = false;
    for _ in 0..5 {
        if manager.maintain("default").await? == Maintenance::Complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(
        manager.get("default").await?.get("key", usize::MAX).await?,
        Some(vec![4])
    );
    Ok(())
}

#[tokio::test]
async fn superseded_uncertain_checkpoint_does_not_fail_an_unstarted_mutation() -> Result {
    let limits = HostLimits {
        checkpoint_entries: 1,
        ..HostLimits::default()
    };
    let (probe, _, store) = fixture(limits).await?;
    store.set("base", b"v").await?;
    probe.reset();
    store.increment("counter".into(), 10).await?;
    let occurrence = probe
        .metrics()
        .events
        .iter()
        .find(|e| e.operation == Operation::Put && e.path.ends_with("index.cbor"))
        .unwrap()
        .occurrence;

    let (fault, manager, store) = fixture(limits).await?;
    let rival = manager.get("default").await?;
    store.set("base", b"v").await?;
    fault.reset();
    let mut paused = fault.pause_put_at(occurrence, FailurePhase::After);
    fault.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.increment("counter".into(), 10).await });
    assert!(tokio::time::timeout(Duration::from_secs(5), paused.wait_until_entered()).await?);
    rival.set("other", b"first").await?;
    rival.set("other", b"second").await?; // replaces the uncertain checkpoint
    assert!(paused.release());
    assert_eq!(task.await??, 10);
    assert_eq!(
        store.get("counter", usize::MAX).await?,
        Some(10i64.to_le_bytes().to_vec())
    );
    Ok(())
}
