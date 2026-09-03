use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use object_log::sim::{Failure, FailurePhase, FaultStore, Operation};
use object_log::{
    CollectionFinish, CollectionStart, CommitStatus, Error, Log, LogId, ObjectRef, Options,
    StagedObject, TransactionId, ValidatedBackend, View,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

const BATCH_SIZES: [usize; 5] = [1, 4, 16, 64, 256];
const INLINE_BYTES: [usize; 2] = [256, 4 * 1_024];
const STAGED_BYTES: [usize; 2] = [64 * 1_024, 1_024 * 1_024];
const TAIL_LENGTHS: [usize; 5] = [0, 16, 64, 256, 1_024];
const WRITER_COUNTS: [usize; 4] = [1, 2, 8, 32];
const LOGICAL_OPERATION_BYTES: usize = 32;

static NEXT_LOG: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct BenchLog {
    store: FaultStore,
    log: Log,
    view: View,
}

#[derive(Clone, Copy, Debug)]
enum CollectionShape {
    FlatLive,
    DeepLive,
    HalfLiveWide,
    Dead,
}

fn benchmark_batch_size(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/append_batch");
    for batch_size in BATCH_SIZES {
        group.throughput(Throughput::Elements(usize_to_u64(batch_size)));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |bencher, &batch_size| {
                bencher.iter_batched(
                    || {
                        let state = runtime.block_on(fresh_log(Options::default()));
                        let operation =
                            Bytes::from(vec![0x5a; batch_size * LOGICAL_OPERATION_BYTES]);
                        (state, operation)
                    },
                    |(mut state, operation)| {
                        runtime.block_on(async move {
                            let prepared = require(state.log.prepare(
                                state.view.cursor(),
                                TransactionId::new(),
                                operation,
                                Bytes::new(),
                                Vec::new(),
                            ));
                            state.view = require_committed(state.log.commit(prepared).await);
                            black_box(state);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn benchmark_inline_bytes(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/append_inline_bytes");
    group.throughput(Throughput::Elements(1));
    for inline_bytes in INLINE_BYTES {
        group.bench_with_input(
            BenchmarkId::from_parameter(inline_bytes),
            &inline_bytes,
            |bencher, &inline_bytes| {
                bencher.iter_batched(
                    || {
                        (
                            runtime.block_on(fresh_log(Options::default())),
                            Bytes::from(vec![0xa5; inline_bytes]),
                        )
                    },
                    |(mut state, operation)| {
                        runtime.block_on(async move {
                            let prepared = require(state.log.prepare(
                                state.view.cursor(),
                                TransactionId::new(),
                                operation,
                                Bytes::new(),
                                Vec::new(),
                            ));
                            state.view = require_committed(state.log.commit(prepared).await);
                            black_box(state);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn benchmark_tail_recovery(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/recover_tail");
    for tail_length in TAIL_LENGTHS {
        let state = runtime.block_on(log_with_tail(tail_length));
        group.throughput(Throughput::Elements(usize_to_u64(tail_length)));
        group.bench_with_input(
            BenchmarkId::from_parameter(tail_length),
            &tail_length,
            |bencher, _| {
                bencher.iter(|| {
                    runtime.block_on(async {
                        let view = require(state.log.load().await);
                        let records = require(state.log.read_tail(&view).await);
                        black_box(records.len());
                    });
                });
            },
        );
    }
    group.finish();
}

fn benchmark_staged_bytes(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/append_staged_bytes");
    for staged_bytes in STAGED_BYTES {
        group.throughput(Throughput::Bytes(usize_to_u64(staged_bytes)));
        group.bench_with_input(
            BenchmarkId::from_parameter(staged_bytes),
            &staged_bytes,
            |bencher, &staged_bytes| {
                bencher.iter_batched(
                    || {
                        (
                            runtime.block_on(fresh_log(Options::default())),
                            Bytes::from(vec![0x3c; staged_bytes]),
                        )
                    },
                    |(mut state, payload)| {
                        runtime.block_on(async move {
                            let object =
                                require(state.log.put_object(state.view.cursor(), payload).await);
                            let prepared = require(state.log.prepare(
                                state.view.cursor(),
                                TransactionId::new(),
                                Bytes::from_static(b"staged payload"),
                                Bytes::new(),
                                vec![object],
                            ));
                            state.view = require_committed(state.log.commit(prepared).await);
                            black_box(state);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn benchmark_writer_contention(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/writer_contention");
    for writer_count in WRITER_COUNTS {
        group.throughput(Throughput::Elements(usize_to_u64(writer_count)));
        group.bench_with_input(
            BenchmarkId::from_parameter(writer_count),
            &writer_count,
            |bencher, &writer_count| {
                bencher.iter_batched(
                    || {
                        let state = runtime.block_on(fresh_log(Options::default()));
                        let candidates = (0..writer_count)
                            .map(|writer| {
                                require(state.log.prepare(
                                    state.view.cursor(),
                                    TransactionId::new(),
                                    Bytes::from(vec![u8::try_from(writer).unwrap_or_default(); 32]),
                                    Bytes::new(),
                                    Vec::new(),
                                ))
                            })
                            .collect::<Vec<_>>();
                        (state, candidates)
                    },
                    |(state, candidates)| {
                        runtime.block_on(async move {
                            let statuses = join_all(
                                candidates
                                    .into_iter()
                                    .map(|candidate| state.log.commit(candidate)),
                            )
                            .await;
                            let mut committed = 0_usize;
                            let mut conflicts = 0_usize;
                            for status in statuses {
                                match require(status) {
                                    CommitStatus::Committed(_) => {
                                        committed = committed.saturating_add(1);
                                    }
                                    CommitStatus::Conflict(_) => {
                                        conflicts = conflicts.saturating_add(1);
                                    }
                                    CommitStatus::Pending(_) => std::process::abort(),
                                }
                            }
                            if committed != 1 || conflicts != writer_count.saturating_sub(1) {
                                std::process::abort();
                            }
                            black_box((state, committed, conflicts));
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn benchmark_collection(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("memory/gc");
    for (name, shape, objects) in [
        ("flat_live_1k", CollectionShape::FlatLive, 1_000),
        ("deep_live_1k", CollectionShape::DeepLive, 1_000),
        ("half_live_wide_10k", CollectionShape::HalfLiveWide, 10_000),
        ("dead_100k", CollectionShape::Dead, 100_000),
    ] {
        group.throughput(Throughput::Elements(objects));
        group.bench_function(format!("start/{name}"), |bencher| {
            bencher.iter_batched(
                || runtime.block_on(collection_state(shape)),
                |state| {
                    black_box(require(
                        runtime.block_on(state.log.start_collection(&state.view)),
                    ));
                },
                BatchSize::LargeInput,
            );
        });
    }

    let (state, blocked) = runtime.block_on(fenced_dead_state(100_000));
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("fence_lookup/planned_ref_100k", |bencher| {
        bencher.iter(|| {
            if !matches!(
                runtime.block_on(
                    state
                        .log
                        .stage_objects(state.view.cursor(), vec![blocked.clone()]),
                ),
                Err(Error::CollectionFence)
            ) {
                std::process::abort();
            }
        });
    });

    for (name, partial) in [("clean_1k", false), ("partial_1001", true)] {
        group.throughput(Throughput::Elements(if partial { 1_001 } else { 1_000 }));
        group.bench_function(format!("resume/{name}"), |bencher| {
            bencher.iter_batched(
                || {
                    runtime.block_on(collection_ready(
                        if partial { 1_001 } else { 1_000 },
                        partial,
                    ))
                },
                |state| {
                    let result =
                        require(runtime.block_on(state.log.resume_collection(&state.view)));
                    if !matches!(result, CollectionFinish::Complete(_, _)) {
                        std::process::abort();
                    }
                    black_box(result);
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

async fn collection_state(shape: CollectionShape) -> BenchLog {
    let mut state = fresh_log(Options {
        max_collection_objects: 100_010,
        ..Options::default()
    })
    .await;
    match shape {
        CollectionShape::FlatLive => {
            let objects = put_blobs(&state.log, &state.view, 998).await;
            append_objects(&mut state, objects).await;
        }
        CollectionShape::DeepLive => {
            let mut root = require(
                state
                    .log
                    .put_object(state.view.cursor(), Bytes::from_static(b"x"))
                    .await,
            );
            for _ in 1..998 {
                root = require(
                    state
                        .log
                        .put_node(state.view.cursor(), Bytes::new(), vec![root])
                        .await,
                );
            }
            append_objects(&mut state, vec![root]).await;
        }
        CollectionShape::HalfLiveWide => {
            let leaves = put_blobs(&state.log, &state.view, 4_993).await;
            let mut roots = Vec::with_capacity(5);
            for children in leaves.chunks(1_000) {
                roots.push(require(
                    state
                        .log
                        .put_node(state.view.cursor(), Bytes::new(), children.to_vec())
                        .await,
                ));
            }
            append_objects(&mut state, roots).await;
            put_blobs(&state.log, &state.view, 5_000).await;
        }
        CollectionShape::Dead => drop(put_blobs(&state.log, &state.view, 99_999).await),
    }
    state
}

async fn fenced_dead_state(count: usize) -> (BenchLog, ObjectRef) {
    let mut state = fresh_log(Options {
        max_collection_objects: count.saturating_add(10),
        ..Options::default()
    })
    .await;
    let mut objects = put_blobs(&state.log, &state.view, count).await;
    let blocked = objects.swap_remove(0).reference().clone();
    let CollectionStart::Installed(view, report) =
        require(state.log.start_collection(&state.view).await)
    else {
        std::process::abort();
    };
    if report.candidate_count() != count {
        std::process::abort();
    }
    state.view = view;
    (state, blocked)
}

async fn collection_ready(count: usize, partial: bool) -> BenchLog {
    let (state, _) = fenced_dead_state(count).await;
    if partial {
        state.store.reset();
        state.store.record_events(false);
        state.store.schedule(Failure {
            operation: Operation::Delete,
            occurrence: 1_001,
            phase: FailurePhase::Before,
        });
        if !matches!(
            require(state.log.resume_collection(&state.view).await),
            CollectionFinish::Pending(_)
        ) {
            std::process::abort();
        }
    }
    state
}

async fn put_blobs(log: &Log, view: &View, count: usize) -> Vec<StagedObject> {
    let mut objects = Vec::with_capacity(count);
    for _ in 0..count {
        objects.push(require(
            log.put_object(view.cursor(), Bytes::from_static(b"x"))
                .await,
        ));
    }
    objects
}

async fn append_objects(state: &mut BenchLog, objects: Vec<StagedObject>) {
    let prepared = require(state.log.prepare(
        state.view.cursor(),
        TransactionId::new(),
        Bytes::new(),
        Bytes::new(),
        objects,
    ));
    state.view = require_committed(state.log.commit(prepared).await);
}

async fn fresh_log(options: Options) -> BenchLog {
    let store = FaultStore::new(InMemory::new());
    store.record_events(false);
    let number = NEXT_LOG.fetch_add(1, Ordering::Relaxed);
    let log_id = require(LogId::new(format!("criterion-{number:016x}")));
    let backend =
        require(ValidatedBackend::new(Arc::new(store.clone()), Path::from("criterion")).await);
    let scoped = backend.scope(&log_id);
    let log = require(Log::open(scoped, options).await);
    let view = require(log.load().await);
    store.reset();
    store.record_events(false);
    BenchLog { store, log, view }
}

async fn log_with_tail(tail_length: usize) -> BenchLog {
    let options = Options {
        max_tail_entries: tail_length.max(Options::default().max_tail_entries),
        ..Options::default()
    };
    let mut state = fresh_log(options).await;
    for sequence in 0..tail_length {
        let prepared = require(state.log.prepare(
            state.view.cursor(),
            TransactionId::new(),
            Bytes::from(vec![u8::try_from(sequence % 251).unwrap_or_default(); 32]),
            Bytes::new(),
            Vec::new(),
        ));
        state.view = require_committed(state.log.commit(prepared).await);
    }
    state.store.reset();
    state.store.record_events(false);
    state
}

fn runtime() -> Runtime {
    require(Builder::new_multi_thread().enable_all().build())
}

fn require<T, E>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(_) => std::process::abort(),
    }
}

fn require_committed(result: Result<CommitStatus, object_log::Error>) -> View {
    match require(result) {
        CommitStatus::Committed(view) => view,
        CommitStatus::Conflict(_) | CommitStatus::Pending(_) => std::process::abort(),
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = benchmark_batch_size, benchmark_inline_bytes, benchmark_staged_bytes,
        benchmark_tail_recovery, benchmark_writer_contention, benchmark_collection
}
criterion_main!(benches);
