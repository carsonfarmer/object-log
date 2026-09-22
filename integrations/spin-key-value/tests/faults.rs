#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::{sync::Arc, time::Duration};

use object_log::{
    Options,
    sim::{Failure, FailurePhase, FaultStore, Operation},
};
use object_log_spin_key_value::{HostLimits, Maintenance, Manager};
use object_store::{ObjectStore, memory::InMemory};
use spin_factor_key_value::{Store, StoreManager};

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

#[tokio::test]
async fn lost_publication_response_is_resolved_once() -> Result {
    let occurrence = head_put().await?;
    let (fault, _, store) = fixture(HostLimits::default()).await?;
    fault.schedule(Failure {
        operation: Operation::Put,
        occurrence,
        phase: FailurePhase::After,
    });
    assert_eq!(store.increment("counter".into(), 1).await?, 1);
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
