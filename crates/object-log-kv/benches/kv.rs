use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use object_log::{
    CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend,
    sim::{FaultStore, Operation},
};
use object_log_kv::{KvCommand, KvStore, Limits};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};

#[allow(clippy::expect_used)]
fn kv(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("memory_radix");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(2));
    for count in [256_u32, 4096] {
        let memory = Arc::new(InMemory::new());
        let faults = FaultStore::new(memory.clone());
        faults.record_events(false);
        let store = runtime.block_on(fixture(count, &faults));
        let snapshot = runtime.block_on(store.snapshot()).expect("snapshot");
        let key = (count / 2).to_be_bytes();
        group.bench_with_input(BenchmarkId::new("get", count), &count, |b, _| {
            b.to_async(&runtime)
                .iter(|| async { black_box(snapshot.get(&key).await.expect("get")) });
        });
        group.bench_with_input(BenchmarkId::new("reopen_get", count), &count, |b, _| {
            b.to_async(&runtime).iter(|| async {
                black_box(
                    store
                        .snapshot()
                        .await
                        .expect("snapshot")
                        .get(&key)
                        .await
                        .expect("get"),
                )
            });
        });
        group.bench_with_input(BenchmarkId::new("prepare_set", count), &count, |b, _| {
            let (snapshot, faults, memory) = (&snapshot, &faults, &memory);
            b.to_async(&runtime).iter_custom(|iterations| async move {
                let commands = [KvCommand::Set {
                    key: Bytes::copy_from_slice(&key),
                    value: Bytes::from(vec![8; 64]),
                }];
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    faults.reset();
                    faults.record_events(true);
                    let started = Instant::now();
                    black_box(
                        snapshot
                            .prepare(TransactionId::new(), &commands)
                            .await
                            .expect("prepare"),
                    );
                    measured += started.elapsed();
                    // Only this isolated, never-published candidate's new objects
                    // are removed. Cleanup is untimed and bounds benchmark growth.
                    for event in faults.metrics().events {
                        if event.operation == Operation::Put {
                            memory
                                .delete(&Path::from(event.path))
                                .await
                                .expect("cleanup");
                        }
                    }
                }
                faults.record_events(false);
                measured
            });
        });
        group.bench_with_input(BenchmarkId::new("scan_32", count), &count, |b, _| {
            b.to_async(&runtime).iter(|| async {
                black_box(snapshot.scan(&key, None, None, 32).await.expect("scan"))
            });
        });
    }
    group.finish();
}
#[allow(clippy::expect_used)]
async fn fixture(count: u32, faults: &FaultStore) -> KvStore {
    let backend = ValidatedBackend::new(Arc::new(faults.clone()), Path::from("bench"))
        .await
        .expect("backend");
    let log = Log::open(
        &backend,
        &LogId::new(format!("n-{count}")).expect("id"),
        Options::default(),
    )
    .await
    .expect("log");
    let store = KvStore::new(log, Limits::default());
    for batch in (0..count).collect::<Vec<_>>().chunks(128) {
        let commands: Vec<_> = batch
            .iter()
            .map(|key| KvCommand::Set {
                key: Bytes::copy_from_slice(&key.to_be_bytes()),
                value: Bytes::from(vec![7; 64]),
            })
            .collect();
        let candidate = store
            .snapshot()
            .await
            .expect("snapshot")
            .prepare(TransactionId::new(), &commands)
            .await
            .expect("prepare");
        assert!(matches!(
            store.log().commit(candidate).await.expect("commit"),
            CommitStatus::Committed(_)
        ));
    }
    store
        .snapshot()
        .await
        .expect("snapshot")
        .checkpoint()
        .await
        .expect("checkpoint");
    store
}
criterion_group!(benches, kv);
criterion_main!(benches);
