#![cfg(feature = "test-util")]

use std::error::Error as StdError;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, future, stream};
use object_log::{
    CollectionFinish, CollectionReport, CollectionStart, CommitStatus, Log, LogId, Options,
    TransactionId, ValidatedBackend, View,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;

#[cfg(feature = "aws")]
use uuid::Uuid;

#[cfg(feature = "aws")]
mod support;

#[cfg(feature = "aws")]
use support::minio::build_minio;

const DEADLINE: Duration = Duration::from_secs(30);
const MEMORY_CANDIDATES: usize = 100_000;
#[cfg(feature = "aws")]
const MINIO_CANDIDATES: usize = 10_001;

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;

#[tokio::test]
#[ignore = "large local GC acceptance test; run make gc-acceptance"]
async fn memory_gc_removes_100k_objects() -> TestResult {
    let result = run(
        Arc::new(InMemory::new()),
        Path::from("gc-acceptance"),
        LogId::new("memory-100k")?,
        MEMORY_CANDIDATES,
        256,
    )
    .await?;
    eprintln!("memory GC start and resume: {result:?}");
    Ok(())
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "large local MinIO GC acceptance test; run make gc-acceptance"]
async fn minio_gc_removes_10001_objects() -> TestResult {
    let raw: Arc<dyn ObjectStore> = Arc::new(build_minio()?);
    let result = run(
        raw,
        Path::from("object-log-gc-acceptance"),
        LogId::new(format!("minio-{}", Uuid::new_v4().simple()))?,
        MINIO_CANDIDATES,
        128,
    )
    .await?;
    eprintln!("MinIO GC start and resume: {result:?}");
    Ok(())
}

async fn run(
    raw: Arc<dyn ObjectStore>,
    root: Path,
    id: LogId,
    candidate_count: usize,
    concurrency: usize,
) -> TestResult<Duration> {
    let backend = ValidatedBackend::new(Arc::clone(&raw), root.clone()).await?;
    let log = Log::open(
        backend.scope(&id),
        Options {
            max_collection_objects: candidate_count
                .checked_add(100)
                .ok_or("candidate count overflow")?,
            ..Options::default()
        },
    )
    .await?;
    let leaf = log.put_object(Bytes::from_static(b"live")).await?;
    let node = log
        .put_node(Bytes::from_static(b"root"), vec![leaf.clone()])
        .await?;
    let prepared = log.prepare(
        log.load().await?.cursor(),
        TransactionId::new(),
        Bytes::from_static(b"live graph"),
        Bytes::new(),
        vec![node.clone()],
    )?;
    let CommitStatus::Committed(view) = log.commit(prepared).await? else {
        return Err("the live-graph commit did not complete".into());
    };
    put_unreachable(&log, candidate_count, concurrency).await?;

    let started = Instant::now();
    let (current, start_report, finish_report) =
        tokio::time::timeout(DEADLINE, collect_once(&log, &view))
            .await
            .map_err(|_| format!("GC did not complete within {DEADLINE:?}"))??;
    let elapsed = started.elapsed();

    let candidate_bytes = u64::try_from(candidate_count)?;
    assert_eq!(start_report.candidate_count(), candidate_count);
    assert_eq!(start_report.candidate_bytes(), candidate_bytes);
    assert_eq!(start_report.delete_attempts(), 0);
    assert_eq!(finish_report.candidate_count(), candidate_count);
    assert_eq!(finish_report.candidate_bytes(), candidate_bytes);
    assert_eq!(finish_report.delete_attempts(), candidate_count);
    assert_eq!(current.collection_epoch(), view.collection_epoch() + 1);

    assert_eq!(log.read_tail(&current).await?.len(), 1);
    assert_eq!(
        log.read_node(&current, &node).await?.children(),
        std::slice::from_ref(&leaf)
    );
    assert_eq!(
        log.read_object(&current, &leaf).await?,
        Bytes::from_static(b"live")
    );

    let generation = current.cursor().generation();
    let epoch = current.collection_epoch();
    let CollectionStart::Empty(empty) = log.start_collection(&current).await? else {
        return Err("the second collection was not empty".into());
    };
    assert_eq!(empty.candidate_count(), 0);
    let unchanged = log.load().await?;
    assert_eq!(unchanged.cursor().generation(), generation);
    assert_eq!(unchanged.collection_epoch(), epoch);

    let scope = root.join("v1").join("logs").join(id.as_str());
    let remaining = raw.list(Some(&scope)).try_collect::<Vec<_>>().await?;
    assert_eq!(remaining.len(), 4);
    assert_eq!(segment_count(&remaining, "commits"), 1);
    assert_eq!(segment_count(&remaining, "blobs"), 1);
    assert_eq!(segment_count(&remaining, "nodes"), 1);
    assert_eq!(segment_count(&remaining, "collection-plans"), 0);
    let head = scope.join("index.cbor");
    assert!(remaining.iter().any(|object| object.location == head));
    Ok(elapsed)
}

async fn put_unreachable(log: &Log, count: usize, concurrency: usize) -> TestResult {
    stream::iter(0..count)
        .map(|_| log.put_object(Bytes::from_static(b"x")))
        .buffer_unordered(concurrency)
        .try_for_each(|_| future::ready(Ok(())))
        .await?;
    Ok(())
}

async fn collect_once(
    log: &Log,
    view: &View,
) -> TestResult<(View, CollectionReport, CollectionReport)> {
    let CollectionStart::Installed(fenced, start_report) = log.start_collection(view).await? else {
        return Err("collection did not install its deletion plan".into());
    };
    let CollectionFinish::Complete(current, finish_report) = log.resume_collection(&fenced).await?
    else {
        return Err("collection did not finish its deletion plan".into());
    };
    Ok((current, start_report, finish_report))
}

fn segment_count(objects: &[object_store::ObjectMeta], segment: &str) -> usize {
    let marker = format!("/{segment}/");
    objects
        .iter()
        .filter(|object| object.location.as_ref().contains(&marker))
        .count()
}
