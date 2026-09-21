//! Finite measurements, not timing assertions. Run without competing workloads:
//! `cargo test --release --features test-util --test performance memory_performance -- --ignored --nocapture`
//! `scripts/test-minio.sh performance minio_performance`
//! The existing `MinIO` runner uses the test profile; set `CARGO_PROFILE_TEST_OPT_LEVEL=3`
//! when comparing optimized runs and record that choice with the raw output.
#![cfg(feature = "test-util")]

use std::{
    error::Error as StdError,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_log::sim::{Failure, FailurePhase, FaultStore, Metrics, Operation};
use object_log::{
    CheckpointStatus, CollectionStart, CommitStatus, Log, LogId, Options, Resolution, RetentionId,
    RetentionStatus, TransactionId, ValidatedBackend, View,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

#[cfg(feature = "aws")]
mod support;

type TestResult<T = ()> = Result<T, Box<dyn StdError>>;
type Samples = Vec<(Duration, Metrics)>;
const SAMPLES: usize = 32;

#[tokio::test]
#[ignore = "finite memory measurements; run separately from other workloads"]
async fn memory_performance() -> TestResult {
    measure_backend("memory", Arc::new(InMemory::new())).await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires the existing local MinIO runner"]
async fn minio_performance() -> TestResult {
    let endpoint = std::env::var("OBJECT_LOG_MINIO_ENDPOINT")?;
    let address: std::net::SocketAddr = endpoint
        .strip_prefix("http://")
        .ok_or("measurements require loopback HTTP MinIO")?
        .trim_end_matches('/')
        .parse()?;
    assert!(address.ip().is_loopback(), "MinIO must be local");
    measure_backend("minio", Arc::new(support::minio::build_minio()?)).await
}

async fn measure_backend(name: &str, store: Arc<dyn ObjectStore>) -> TestResult {
    let root = Path::from(format!("performance-{}", uuid::Uuid::new_v4().simple()));
    let faults = FaultStore::from_arc(Arc::clone(&store));
    faults.record_events(false);
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), root.clone()).await?;
    println!(
        "backend={name} samples={SAMPLES} concurrency=1 events=off \
         counters=logical_object_store_requests_and_body_bytes_not_HTTP_attempts \
         cache=warm_provider_and_handle_except_explicit_cold_log_reopens \
         cold_resume_excludes_initial_open=true percentiles=nearest_rank_p99_is_max_of_32 \
         reset=unique_namespace_setup_and_validation_excluded"
    );
    let result = async {
        for size in [256, 4 * 1024, 1024 * 1024] {
            let payload = Bytes::from(vec![0x5a; size]);
            append_windows(name, &backend, &faults, &payload).await?;
            control_samples(name, &backend, &faults, &payload).await?;
        }
        TestResult::Ok(())
    }
    .await;
    // Cleanup stays outside every measurement, including after assertion-free errors.
    let objects = store.list(Some(&root)).map_ok(|object| object.location);
    store
        .delete_stream(Box::pin(objects))
        .try_collect::<Vec<_>>()
        .await?;
    result
}

fn begin(faults: &FaultStore) -> Instant {
    faults.reset();
    faults.record_events(false);
    Instant::now()
}

fn record(samples: &mut Samples, faults: &FaultStore, started: Instant) {
    let elapsed = started.elapsed();
    samples.push((elapsed, faults.metrics()));
}

fn report(name: &str, case: &str, bytes: usize, mut samples: Samples) -> TestResult {
    let count = u32::try_from(samples.len())?;
    assert!(count > 0);
    let elapsed: Duration = samples.iter().map(|sample| sample.0).sum();
    let requests: u64 = samples.iter().map(|sample| sample.1.total_requests()).sum();
    let uploaded: u64 = samples.iter().map(|sample| sample.1.uploaded_bytes()).sum();
    let downloaded: u64 = samples
        .iter()
        .map(|sample| sample.1.downloaded_bytes())
        .sum();
    let puts: u64 = samples
        .iter()
        .map(|sample| sample.1.operation(Operation::Put).requests)
        .sum();
    let gets: u64 = samples
        .iter()
        .map(|sample| sample.1.operation(Operation::Get).requests)
        .sum();
    samples.sort_unstable_by_key(|sample| sample.0);
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1].0;
    println!(
        "backend={name} case={case} payload_bytes={bytes} n={count} \
         p50={:?} p95={:?} p99={:?} max={:?} ops_per_second={:.2} \
         requests={requests} gets={gets} puts={puts} uploaded={uploaded} downloaded={downloaded}",
        percentile(50),
        percentile(95),
        percentile(99),
        percentile(100),
        f64::from(count) / elapsed.as_secs_f64(),
    );
    Ok(())
}

async fn append(log: &Log, view: &View, payload: &Bytes) -> TestResult<View> {
    let (operation, objects) = if payload.len() > 4096 {
        (
            Bytes::from_static(b"staged payload"),
            vec![log.put_object(view, payload.clone()).await?],
        )
    } else {
        (payload.clone(), Vec::new())
    };
    let prepared = log.prepare(view, TransactionId::new(), operation, Bytes::new(), objects)?;
    let CommitStatus::Committed(next) = log.commit(prepared).await? else {
        return Err("sequential append did not commit".into());
    };
    Ok(next)
}

async fn append_windows(
    name: &str,
    backend: &ValidatedBackend,
    faults: &FaultStore,
    payload: &Bytes,
) -> TestResult {
    let options = Options {
        max_tail_entries: 1024 + SAMPLES,
        ..Options::default()
    };
    let log = Log::open(
        backend,
        &LogId::new(format!("append-{}", payload.len()))?,
        options,
    )
    .await?;
    let mut view = log.load().await?;
    let padding = Bytes::from_static(&[0; 32]);
    for tail in [0, 64, 256, 1024] {
        // Padding controls head size without staging a GiB of unmeasured payloads.
        while view.tail().len() < tail {
            view = append(&log, &view, &padding).await?;
        }
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = begin(faults);
            view = append(&log, &view, payload).await?;
            record(&mut samples, faults, started);
        }
        assert_eq!(view.tail().len(), tail + SAMPLES);
        assert_eq!(log.load().await?.tail(), view.tail());
        let records = log.read_tail(&view).await?;
        let last = records.last().ok_or("missing appended record")?;
        if payload.len() > 4096 {
            assert_eq!(log.read_object(&view, &last.objects()[0]).await?, *payload);
        } else {
            assert_eq!(last.operation(), payload);
        }
        report(
            name,
            &format!("append_tail_{tail}_to_{}", tail + SAMPLES - 1),
            payload.len(),
            samples,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // Keep the ordered recovery/compaction/retention scenario together.
async fn control_samples(
    name: &str,
    backend: &ValidatedBackend,
    faults: &FaultStore,
    payload: &Bytes,
) -> TestResult {
    let mut conflicts = Vec::with_capacity(SAMPLES);
    let mut recovery = Vec::with_capacity(SAMPLES);
    let mut checkpoints = Vec::with_capacity(SAMPLES);
    let mut compacted = Vec::with_capacity(SAMPLES);
    let mut retains = Vec::with_capacity(SAMPLES);
    let mut releases = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        let id = LogId::new(format!("controls-{}-{sample}", payload.len()))?;
        let log = Log::open(backend, &id, Options::default()).await?;
        let empty = log.load().await?;
        let object = log.put_object(&empty, payload.clone()).await?;
        let winner = log.prepare(
            &empty,
            TransactionId::new(),
            Bytes::from_static(b"winner"),
            Bytes::new(),
            vec![object.clone()],
        )?;
        let loser = log.prepare(
            &empty,
            TransactionId::new(),
            Bytes::from_static(b"loser"),
            Bytes::new(),
            Vec::new(),
        )?;
        let CommitStatus::Committed(view) = log.commit(winner).await? else {
            return Err("fixture did not commit".into());
        };
        let started = begin(faults);
        let rejected = log.commit(loser).await?;
        record(&mut conflicts, faults, started);
        let CommitStatus::Conflict(current) = rejected else {
            return Err("stale writer did not conflict".into());
        };
        assert_eq!(current.tail(), view.tail());

        let result = Bytes::from_static(b"exact result");
        let prepared = log.prepare(
            &view,
            TransactionId::new(),
            Bytes::from_static(b"recover"),
            result.clone(),
            vec![object.clone()],
        )?;
        faults.reset();
        faults.record_events(false);
        faults.schedule(Failure {
            operation: Operation::Put,
            occurrence: 2,
            phase: FailurePhase::After,
        });
        let CommitStatus::Pending(pending) = log.commit(prepared).await? else {
            return Err("lost head response did not leave pending evidence".into());
        };
        let token = pending.recovery_token()?;
        let cold = Log::open_existing(backend, &id, Options::default()).await?;
        let started = begin(faults);
        let resolved = cold.resume(&token).await?;
        record(&mut recovery, faults, started);
        let Resolution::Committed(view) = resolved else {
            return Err("cold exact recovery did not find committed result".into());
        };
        let records = cold.read_tail(&view).await?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].result(), &result);
        assert_eq!(cold.read_object(&view, object.reference()).await?, *payload);

        let objects = cold
            .stage_objects(&view, vec![object.reference().clone()])
            .await?;
        let started = begin(faults);
        let checkpoint = cold
            .publish_checkpoint(&view, &view.tail()[1], payload.clone(), objects)
            .await?;
        record(&mut checkpoints, faults, started);
        let CheckpointStatus::Published(view) = checkpoint else {
            return Err("checkpoint did not publish".into());
        };
        assert!(view.tail().is_empty());
        assert_eq!(
            cold.read_checkpoint(&view)
                .await?
                .ok_or("missing checkpoint")?
                .snapshot(),
            payload
        );

        let reopened = Log::open_existing(backend, &id, Options::default()).await?;
        let started = begin(faults);
        let resolved = reopened.resume(&token).await?;
        record(&mut compacted, faults, started);
        let Resolution::Committed(current) = resolved else {
            return Err("compacted exact outcome was not retained".into());
        };
        assert_eq!(current.generation(), view.generation());

        let retention = RetentionId::new();
        let started = begin(faults);
        let retained = reopened.retain(&current, retention).await?;
        record(&mut retains, faults, started);
        let RetentionStatus::Applied(retained) = retained else {
            return Err("retention was not acquired".into());
        };
        assert!(matches!(
            reopened.start_collection(&retained).await?,
            CollectionStart::Retained(_)
        ));
        let started = begin(faults);
        let cleared = reopened.release_retention(&retained, retention).await?;
        record(&mut releases, faults, started);
        let RetentionStatus::Applied(cleared) = cleared else {
            return Err("retention was not released".into());
        };
        assert_eq!(cleared.generation(), retained.generation() + 1);
        assert_eq!(reopened.load().await?.generation(), cleared.generation());
    }
    for (case, samples) in [
        ("stale_conflict", conflicts),
        ("cold_resume_tail", recovery),
        ("checkpoint_verified_roots", checkpoints),
        ("cold_resume_compacted", compacted),
        ("retain", retains),
        ("release", releases),
    ] {
        report(name, case, payload.len(), samples)?;
    }
    Ok(())
}
