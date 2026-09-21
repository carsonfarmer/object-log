use std::{
    collections::BTreeMap,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{TryStreamExt, future::try_join_all};
use object_log::{
    CheckpointStatus, CollectionFinish, CollectionStart, CommitStatus, Log, LogId, Options,
    RetentionId, RetentionStatus, TransactionId, ValidatedBackend,
    sim::{FaultStore, Operation},
};
use object_log_kv::{KvCommand, KvSnapshot, KvStore, Limits};
use object_store::{ObjectStore, memory::InMemory, path::Path};

#[cfg(feature = "aws")]
#[path = "support/minio.rs"]
mod minio;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Model = BTreeMap<Bytes, Bytes>;

const LIMITS: Limits = Limits {
    key_bytes: 64,
    value_bytes: 1024,
    batch_entries: 64,
    batch_bytes: 128 * 1024,
    page_entries: 32,
    response_bytes: 64 * 1024,
    tree_bytes: 4 * 1024 * 1024,
};
fn options() -> Options {
    Options {
        max_tail_entries: 64,
        resolution_window: 16,
        ..Options::default()
    }
}

// Keep provider counters optional without changing the tested workload.
type ProviderCounts = dyn Fn() -> [u64; 4];

#[tokio::test]
async fn memory_growth_and_contention() -> TestResult {
    qualify(Arc::new(InMemory::new()), &|| [0; 4], "memory").await
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires isolated local MinIO; see README"]
async fn minio_growth_and_contention() -> TestResult {
    let (storage, counts) = minio::build()?;
    qualify(storage, &move || counts.snapshot(), "minio").await
}

async fn open(backend: &ValidatedBackend) -> TestResult<KvStore> {
    Ok(KvStore::new(
        Log::open(backend, &LogId::new("growth")?, options()).await?,
        LIMITS,
    ))
}

fn key(n: u32) -> Bytes {
    // Ordered application/index keys with common prefixes, not a flat hash map.
    Bytes::from(format!("records/{n:08}"))
}

fn set(key: Bytes, value: Bytes) -> KvCommand {
    KvCommand::Set { key, value }
}

async fn commit(store: &KvStore, commands: &[KvCommand]) -> TestResult {
    let candidate = store
        .snapshot()
        .await?
        .prepare(TransactionId::new(), commands)
        .await?;
    if !matches!(
        store.log().commit(candidate).await?,
        CommitStatus::Committed(_)
    ) {
        return Err("unexpected noncommitted serial operation".into());
    }
    Ok(())
}

async fn checkpoint(store: &KvStore) -> TestResult {
    assert!(matches!(
        store.snapshot().await?.checkpoint().await?,
        Some(CheckpointStatus::Published(_)) | None
    ));
    Ok(())
}

async fn assert_model(snapshot: &KvSnapshot, model: &Model) -> TestResult {
    // Stream the oracle alongside bounded pages instead of collecting a second DB.
    let mut expected = model.iter();
    let mut after = None;
    loop {
        let page = snapshot.scan(b"", None, after.as_deref(), 32).await?;
        for (key, value) in &page.entries {
            assert_eq!(expected.next(), Some((key, value)));
        }
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(expected.next(), None);
    Ok(())
}

async fn stored(storage: &dyn ObjectStore) -> TestResult<(u64, u64)> {
    Ok(storage
        .list(None)
        .try_fold((0, 0), |(objects, bytes), meta| async move {
            Ok((objects + 1, bytes + meta.size))
        })
        .await?)
}

async fn collect(store: &KvStore) -> TestResult<u64> {
    let mut deleted = 0;
    for _ in 0..16 {
        match store
            .log()
            .start_collection(&store.log().load().await?)
            .await?
        {
            CollectionStart::Empty(_) => return Ok(deleted),
            CollectionStart::Installed(view, _) => {
                let CollectionFinish::Complete(_, report) =
                    store.log().resume_collection(&view).await?
                else {
                    return Err("unexpected incomplete collection".into());
                };
                deleted += report.candidate_bytes();
            }
            status => return Err(format!("unexpected collection status: {status:?}").into()),
        }
    }
    Err("collection did not drain within 16 bounded plans".into())
}

fn rss_kib() -> TestResult<u64> {
    // Phase-boundary RSS, not allocator admission or a claimed peak. Includes the
    // oracle, HTTP client, runtime and allocator slack; no unsafe allocator hooks.
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    if !output.status.success() {
        return Err("ps could not measure qualification process RSS".into());
    }
    Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?)
}

fn report(
    label: &str,
    faults: Option<&FaultStore>,
    http_before: [u64; 4],
    provider: &ProviderCounts,
    logical_written: u64,
    elapsed: Duration,
) {
    let http = provider();
    let delta = std::array::from_fn::<_, 4, _>(|i| http[i] - http_before[i]);
    eprintln!(
        "{label} elapsed_ms={} application_written={} http=[attempts,upload_body_bytes,conflicts,transport_or_5xx_errors]{delta:?} rss_kib={}",
        elapsed.as_millis(),
        logical_written,
        rss_kib().map_or_else(|_| "unavailable".to_owned(), |rss| rss.to_string()),
    );
    if let Some(faults) = faults {
        let metrics = faults.metrics();
        eprintln!(
            "{label} logical_calls={} gets={} puts={} lists={} deletes={} downloaded={} uploaded={}",
            metrics.total_requests(),
            metrics.operation(Operation::Get).requests,
            metrics.operation(Operation::Put).requests,
            metrics.operation(Operation::List).requests,
            metrics.operation(Operation::Delete).requests,
            metrics.downloaded_bytes(),
            metrics.uploaded_bytes(),
        );
        // Exact numerators/denominators above are retained for reproducibility.
        if logical_written > 0 {
            #[allow(clippy::cast_precision_loss)]
            let amplification = metrics.uploaded_bytes() as f64 / logical_written as f64;
            eprintln!("{label} logical_write_amplification={amplification:.2}");
        }
    } else {
        eprintln!("{label} logical_io=unmeasured (direct provider bulk deletion)");
    }
}

fn reset(faults: &FaultStore) {
    faults.reset();
    faults.record_events(false);
}

async fn qualify(
    storage: Arc<dyn ObjectStore>,
    provider: &ProviderCounts,
    name: &str,
) -> TestResult {
    let faults = FaultStore::from_arc(Arc::clone(&storage));
    faults.record_events(false);
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from("tests")).await?;
    // FaultStore serializes delete_stream to support per-object fault injection.
    // Use the real provider for GC so its bulk-delete batching stays intact.
    let direct = ValidatedBackend::new(Arc::clone(&storage), Path::from("tests")).await?;
    let mut store = open(&backend).await?;
    let mut model = Model::new();
    let mut count = 0;
    eprintln!(
        "backend={name} key=records/8-digit value_bytes=1024 sizes=256,1024,4096 \
        seed=0x5eed samples_per_size=32 batch_sizes=1,8,64 writers=4 readers=2 \
        tree_budget={} batch_budget={} response_budget={}",
        LIMITS.tree_bytes, LIMITS.batch_bytes, LIMITS.response_bytes
    );
    for target in [256, 1024, 4096] {
        reset(&faults);
        let http = provider();
        let started = Instant::now();
        let mut written = 0;
        while count < target {
            let mut commands = Vec::new();
            for n in count..count + 64 {
                let k = key(n);
                let value = Bytes::from(vec![7; 1024]);
                written += (k.len() + value.len()) as u64;
                commands.push(set(k.clone(), value.clone()));
                model.insert(k, value);
            }
            commit(&store, &commands).await?;
            count += 64;
            if (count / 64) % 8 == 0 {
                checkpoint(&store).await?;
            }
        }
        checkpoint(&store).await?;
        report(
            &format!("{name} keys={count} grow"),
            Some(&faults),
            http,
            provider,
            written,
            started.elapsed(),
        );
        // Sparse admission remains below total data size at the final stage.
        let bounded = KvStore::new(
            store.log().clone(),
            Limits {
                tree_bytes: 128 * 1024,
                ..LIMITS
            },
        );
        reset(&faults);
        let snapshot = bounded.snapshot().await?;
        assert!(faults.metrics().operation(Operation::Get).requests <= 2);
        reset(&faults);
        assert_eq!(
            snapshot.get(&key(count / 2)).await?,
            model.get(&key(count / 2)).cloned()
        );
        assert!(faults.metrics().operation(Operation::Get).requests <= 6);
        assert!(faults.metrics().downloaded_bytes() < 32 * 1024);
        drop(snapshot);

        mixed(&store, &faults, provider, name, count, &mut model).await?;
        contention(&backend, &faults, provider, name, &mut model).await?;

        // Drop every KV handle before reopening; no local tree cache survives.
        drop(bounded);
        drop(store);
        store = open(&backend).await?;
        assert_model(&store.snapshot().await?, &model).await?;
        checkpoint(&store).await?;
        let before = stored(storage.as_ref()).await?;
        let collector = open(&direct).await?;
        let http = provider();
        let started = Instant::now();
        let deleted = collect(&collector).await?;
        report(
            &format!("{name} keys={count} gc"),
            None,
            http,
            provider,
            0,
            started.elapsed(),
        );
        let after = stored(storage.as_ref()).await?;
        assert!(after.0 < before.0);
        // Mutable head encoding can change size when the collection epoch advances.
        assert!((before.1 - after.1).abs_diff(deleted) < 1024);
        let live_bytes: usize = model.iter().map(|(k, v)| k.len() + v.len()).sum();
        eprintln!(
            "{name} keys={count} objects_before={} bytes_before={} objects_after={} bytes_after={} live_application_bytes={live_bytes}",
            before.0, before.1, after.0, after.1
        );
        store = open(&backend).await?;
        assert_model(&store.snapshot().await?, &model).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // One ordered read/write workload against its oracle.
async fn mixed(
    store: &KvStore,
    faults: &FaultStore,
    provider: &ProviderCounts,
    name: &str,
    count: u32,
    model: &mut Model,
) -> TestResult {
    reset(faults);
    let http = provider();
    let phase = Instant::now();
    let mut samples = BTreeMap::<&str, Vec<Duration>>::new();
    let mut random = 0x5eed_u64;
    let mut written = 0;
    for round in 0..32_u8 {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let n = if round % 4 == 0 {
            0
        } else {
            u32::try_from(random >> 32)? % count
        };
        let k = key(n);
        let snapshot = store.snapshot().await?;
        let start = Instant::now();
        assert_eq!(snapshot.get(&k).await?, model.get(&k).cloned());
        samples.entry("get").or_default().push(start.elapsed());
        let start = Instant::now();
        assert_eq!(
            store.snapshot().await?.get(&k).await?,
            model.get(&k).cloned()
        );
        samples
            .entry("reopen_get")
            .or_default()
            .push(start.elapsed());

        let keys: Vec<_> = (0..8).map(|offset| key((n + offset) % count)).collect();
        let start = Instant::now();
        assert_eq!(
            snapshot.get_many(&keys).await?,
            keys.iter()
                .map(|k| model.get(k).cloned())
                .collect::<Vec<_>>()
        );
        samples
            .entry("get_many_8")
            .or_default()
            .push(start.elapsed());
        let start = Instant::now();
        let page = snapshot.scan(&k, None, None, 32).await?;
        let expected: Vec<_> = model
            .range(k.clone()..)
            .take(32)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(page.entries, expected);
        samples.entry("scan_32").or_default().push(start.elapsed());

        let width = if round % 2 == 0 { 1 } else { 8 };
        let commands: Vec<_> = keys
            .into_iter()
            .take(width)
            .map(|k| {
                if round % 7 == 0 {
                    written += k.len() as u64;
                    model.remove(&k);
                    KvCommand::Delete { key: k }
                } else {
                    let value = Bytes::from(vec![round; 1024]);
                    written += (k.len() + value.len()) as u64;
                    model.insert(k.clone(), value.clone());
                    set(k, value)
                }
            })
            .collect();
        let start = Instant::now();
        commit(store, &commands).await?;
        samples
            .entry(if width == 1 {
                "set_delete_1"
            } else {
                "batch_8"
            })
            .or_default()
            .push(start.elapsed());
        if round % 8 == 7 {
            checkpoint(store).await?;
        }
    }
    for (operation, mut times) in samples {
        times.sort_unstable();
        let p50 = times[(times.len() - 1) / 2].as_micros();
        let p95 = times[(times.len() * 95).div_ceil(100) - 1].as_micros();
        eprintln!(
            "{name} keys={count} op={operation} samples={} p50_us={p50} p95_us={p95}",
            times.len()
        );
    }
    report(
        &format!("{name} keys={count} mixed"),
        Some(faults),
        http,
        provider,
        written,
        phase.elapsed(),
    );
    Ok(())
}

#[allow(clippy::too_many_lines)] // One complete reader/writer/retention lifecycle.
async fn contention(
    backend: &ValidatedBackend,
    faults: &FaultStore,
    provider: &ProviderCounts,
    name: &str,
    model: &mut Model,
) -> TestResult {
    let store = open(backend).await?;
    let old = store.snapshot().await?;
    let retention = RetentionId::new();
    assert!(matches!(
        store.log().retain(old.view(), retention).await?,
        RetentionStatus::Applied(_)
    ));
    let initial = model
        .get(b"counter".as_slice())
        .map_or(Ok(0), |v| v.as_ref().try_into().map(i64::from_be_bytes))?;
    let writers = try_join_all((0..4).map(|_| open(backend))).await?;
    // Every writer starts from the same base so at least three conflicts occur.
    let bases = try_join_all(writers.iter().map(KvStore::snapshot)).await?;
    reset(faults);
    let http = provider();
    let started = Instant::now();
    let publications = try_join_all(writers.iter().zip(bases).map(|(writer, base)| async move {
        let mut conflicts = 0;
        let mut base = base;
        for _ in 0..8 {
            let mut committed = false;
            for _ in 0..64 {
                let commands = [
                    KvCommand::Increment {
                        key: Bytes::from("counter"),
                        delta: 1,
                    },
                    KvCommand::Increment {
                        key: Bytes::from("counter-copy"),
                        delta: 1,
                    },
                ];
                let candidate = base.prepare(TransactionId::new(), &commands).await?;
                tokio::task::yield_now().await;
                match writer.log().commit(candidate).await? {
                    CommitStatus::Committed(_) => {
                        committed = true;
                        break;
                    }
                    CommitStatus::Conflict(_) => {
                        conflicts += 1;
                        base = writer.snapshot().await?;
                    }
                    CommitStatus::Pending(_) => {
                        return Err("unexpected pending contention result; do not replay".into());
                    }
                }
            }
            if !committed {
                return Err("writer exhausted conflict allowance".into());
            }
            base = writer.snapshot().await?;
        }
        Ok::<_, Box<dyn Error>>(conflicts)
    }));
    let reads = try_join_all((0..2).map(|_| async {
        for _ in 0..24 {
            let snapshot = store.snapshot().await?;
            let pair = snapshot
                .get_many(&[Bytes::from("counter"), Bytes::from("counter-copy")])
                .await?;
            assert_eq!(pair[0], pair[1], "reader observed a torn atomic batch");
            tokio::task::yield_now().await;
        }
        assert_model(&old, model).await
    }));
    let (conflicts, _) = futures::try_join!(publications, reads)?;
    let conflicts: u64 = conflicts.into_iter().sum();
    assert!(conflicts >= 3);
    eprintln!(
        "{name} contention committed_batches=32 conflicts={conflicts} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    report(
        &format!("{name} contention"),
        Some(faults),
        http,
        provider,
        32 * (7 + 12 + 16),
        started.elapsed(),
    );
    let value = Bytes::copy_from_slice(&(initial + 32).to_be_bytes());
    model.insert(Bytes::from("counter"), value.clone());
    model.insert(Bytes::from("counter-copy"), value);
    checkpoint(&store).await?;
    assert!(matches!(
        store
            .log()
            .start_collection(&store.log().load().await?)
            .await?,
        CollectionStart::Retained(_)
    ));
    assert!(matches!(
        store
            .log()
            .release_retention(&store.log().load().await?, retention)
            .await?,
        RetentionStatus::Applied(_)
    ));
    Ok(())
}
