#![cfg(feature = "test-util")]

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::FaultStore;
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Error, Log, LogId, Options, Request,
    RequestDenied, RequestGuard, TransactionId, ValidatedBackend,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::{Arc, Mutex};

#[cfg(feature = "aws")]
mod support;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn fixture() -> Result<(Log, FaultStore), Error> {
    let store = FaultStore::new(InMemory::new());
    let backend = ValidatedBackend::new(Arc::new(store.clone()), Path::from("limits")).await?;
    let log = Log::open(&backend, &LogId::new("limits")?, Options::default()).await?;
    Ok((log, store))
}

#[tokio::test]
async fn invalid_candidate_limits_fail_before_storage_work() -> TestResult {
    let (log, store) = fixture().await?;
    let view = log.load().await?;
    for limit in [0, log.options().max_collection_objects + 1, usize::MAX] {
        store.reset();
        assert!(matches!(
            log.start_collection_with_limit(&view, limit).await,
            Err(Error::LimitExceeded("collection candidate objects"))
        ));
        assert_eq!(store.metrics().total_requests(), 0);
    }
    Ok(())
}

// One batch: two reads, candidate deletion, fence-clear CAS, and plan cleanup.
// The key budget also reserves the plan object's best-effort deletion.
#[derive(Debug)]
struct ResumeBudget(Mutex<(usize, usize)>);

impl ResumeBudget {
    fn new(candidates: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new((5, candidates + 1))))
    }
}

impl RequestGuard for ResumeBudget {
    fn before_request(&self, request: Request) -> Result<(), RequestDenied> {
        let mut remaining = self.0.lock().map_err(|_| RequestDenied)?;
        let keys = match request {
            Request::Delete { objects } => objects,
            _ => 0,
        };
        if remaining.0 == 0 || keys > remaining.1 {
            return Err(RequestDenied);
        }
        remaining.0 -= 1;
        remaining.1 -= keys;
        Ok(())
    }
}

#[tokio::test]
async fn smaller_candidate_limit_preserves_an_active_larger_plan() -> TestResult {
    let (log, store) = fixture().await?;
    let view = log.load().await?;
    for _ in 0..5 {
        log.put_object(&view, Bytes::from_static(b"orphan")).await?;
    }
    let CollectionStart::Installed(fenced, report) =
        log.start_collection_with_limit(&view, 5).await?
    else {
        return Err("expected installed plan".into());
    };
    assert_eq!(report.candidate_count(), 5);
    store.reset();
    let CollectionStart::Active(active) = log.start_collection_with_limit(&fenced, 1).await? else {
        return Err("expected original active plan".into());
    };
    assert_eq!(active.generation(), fenced.generation());
    assert_eq!(active.collection_epoch(), fenced.collection_epoch());
    assert_eq!(
        active.collection_plan_bytes(),
        fenced.collection_plan_bytes()
    );
    assert_eq!(store.metrics().total_requests(), 0);
    let CollectionFinish::Pending(report) = log
        .with_request_guard(ResumeBudget::new(1))
        .resume_collection(&active)
        .await?
    else {
        return Err("the original plan must still require its full budget".into());
    };
    assert_eq!(report.candidate_count(), 5);
    assert_eq!(report.delete_attempts(), 0);
    let CollectionFinish::Complete(cleared, report) = log
        .with_request_guard(ResumeBudget::new(5))
        .resume_collection(&active)
        .await?
    else {
        return Err("full budget did not finish original plan".into());
    };
    assert_eq!(report.delete_attempts(), 5);
    assert!(cleared.collection_plan_bytes().is_none());
    Ok(())
}

#[tokio::test]
async fn bounded_plans_drain_backlog_without_restricting_live_graph() -> TestResult {
    drain_backlog(Arc::new(InMemory::new())).await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires local MinIO"]
async fn minio_bounded_plans_drain_backlog_without_restricting_live_graph() -> TestResult {
    drain_backlog(Arc::new(support::minio::build_minio()?)).await
}

async fn drain_backlog(raw: Arc<dyn ObjectStore>) -> TestResult {
    const CANDIDATES: usize = 32;
    const ORPHANS: usize = 1001;
    let root = Path::from(format!(
        "collection-limits-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let backend = ValidatedBackend::new(Arc::clone(&raw), root.clone()).await?;
    let log = Log::open(&backend, &LogId::new("backlog")?, Options::default()).await?;
    let initial = log.load().await?;
    let mut leaves = Vec::new();
    for _ in 0..CANDIDATES {
        leaves.push(
            log.put_object(&initial, Bytes::from_static(b"live"))
                .await?,
        );
    }
    // The live graph includes these leaves, their parent and the commit,
    // exceeding the per-operation candidate limit while fitting durable limits.
    let parent = log.put_node(&initial, Bytes::new(), leaves.clone()).await?;
    let prepared = log.prepare(
        &initial,
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        vec![parent.clone()],
    )?;
    let CommitStatus::Committed(live) = log.commit(prepared).await? else {
        return Err("live publication failed".into());
    };
    for _ in 0..ORPHANS {
        log.put_object(&live, Bytes::from_static(b"orphan")).await?;
    }
    let mut view = live.clone();
    let mut collected = 0;
    for _ in 0..ORPHANS.div_ceil(CANDIDATES) {
        let CollectionStart::Installed(fenced, started) =
            log.start_collection_with_limit(&view, CANDIDATES).await?
        else {
            return Err("expected next bounded plan".into());
        };
        assert_eq!(
            started.candidate_count(),
            CANDIDATES.min(ORPHANS - collected)
        );
        let budget = ResumeBudget::new(started.candidate_count());
        let CollectionFinish::Complete(current, finished) = log
            .with_request_guard(budget.clone())
            .resume_collection(&fenced)
            .await?
        else {
            return Err("bounded plan did not finish within its complete budget".into());
        };
        assert_eq!(finished.delete_attempts(), started.candidate_count());
        assert_eq!(*budget.0.lock().map_err(|_| "budget lock")?, (0, 0));
        assert_eq!(current.tail(), live.tail());
        assert!(current.collection_plan_bytes().is_none());
        collected += started.candidate_count();
        view = current;
    }
    assert_eq!(collected, ORPHANS);
    assert!(matches!(
        log.start_collection_with_limit(&view, CANDIDATES).await?,
        CollectionStart::Empty(_)
    ));
    assert_eq!(
        log.read_node(&view, parent.reference())
            .await?
            .children()
            .len(),
        CANDIDATES
    );
    for leaf in leaves {
        assert_eq!(
            log.read_object(&view, leaf.reference()).await?,
            Bytes::from_static(b"live")
        );
    }
    let remaining = raw.list(Some(&root)).try_collect::<Vec<_>>().await?;
    assert_eq!(remaining.len(), CANDIDATES + 3); // leaves, node, commit, head
    raw.delete_stream(Box::pin(futures::stream::iter(
        remaining.into_iter().map(|entry| Ok(entry.location)),
    )))
    .try_collect::<Vec<_>>()
    .await?;
    Ok(())
}
