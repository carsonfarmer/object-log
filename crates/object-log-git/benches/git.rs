use std::{hint::black_box, sync::Arc, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_log::{CheckpointStatus, Log, LogId, Options, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use object_store::{memory::InMemory, path::Path as StorePath};
use support::Fixture;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

#[path = "../tests/support/mod.rs"]
mod support;

const KIB: usize = 1_024;
const MIB: usize = KIB * KIB;

type RepositoryState = (Log, TempDir, Repository);

fn publication_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("git/publication");
    for (name, payload_bytes, options) in cases() {
        let fixture = fixture(payload_bytes);
        group.throughput(Throughput::Bytes(fixture.pack_bytes));
        group.bench_with_input(
            BenchmarkId::new("pack", name),
            &options,
            |bencher, &options| {
                bencher.iter_batched(
                    || runtime.block_on(fresh_repository(options)),
                    |(log, directory, repository)| {
                        let view = require(runtime.block_on(support::publish(
                            repository,
                            "refs/heads/main",
                            None,
                            Some(fixture.target),
                            Some(&fixture.pack),
                        )));
                        black_box((log, directory, view))
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn checkpoint_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("git/checkpoint");
    for (name, payload_bytes, options) in cases() {
        let fixture = fixture(payload_bytes);
        group.throughput(Throughput::Bytes(fixture.pack_bytes));
        group.bench_with_input(
            BenchmarkId::new("pack", name),
            &options,
            |bencher, &options| {
                bencher.iter_batched(
                    || runtime.block_on(published_repository(options, &fixture)),
                    |(log, directory, repository)| {
                        let CheckpointStatus::Published(view) =
                            require(runtime.block_on(repository.checkpoint()))
                        else {
                            std::process::abort();
                        };
                        black_box((log, directory, view))
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn recovery_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("git/cold_recovery");
    for (name, payload_bytes, options) in cases() {
        let fixture = fixture(payload_bytes);
        let log = runtime.block_on(checkpointed_log(options, &fixture));
        group.throughput(Throughput::Bytes(fixture.pack_bytes));
        group.bench_with_input(BenchmarkId::new("pack", name), &log, |bencher, log| {
            bencher.iter_batched(
                || require(tempfile::tempdir()),
                |directory| {
                    let repository = require(runtime.block_on(Repository::open(
                        log,
                        directory.path().join("cache"),
                        ObjectFormat::Sha1,
                    )));
                    black_box((repository, directory))
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn cases() -> [(&'static str, usize, Options); 2] {
    [
        ("small", 4 * KIB, Options::default()),
        (
            "chunked_8_mib",
            8 * MIB,
            Options {
                max_object_bytes: 256 * KIB,
                ..Options::default()
            },
        ),
    ]
}

async fn fresh_repository(options: Options) -> RepositoryState {
    let backend = require(
        ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from("git-bench")).await,
    );
    let log = require(Log::open(backend.scope(&require(LogId::new("repository"))), options).await);
    let directory = require(tempfile::tempdir());
    let repository =
        require(Repository::open(&log, directory.path().join("cache"), ObjectFormat::Sha1).await);
    (log, directory, repository)
}

async fn published_repository(options: Options, fixture: &Fixture) -> RepositoryState {
    let (log, directory, repository) = fresh_repository(options).await;
    require(
        support::publish(
            repository,
            "refs/heads/main",
            None,
            Some(fixture.target),
            Some(&fixture.pack),
        )
        .await,
    );
    let repository = require(
        Repository::open(
            &log,
            directory.path().join("checkpoint-cache"),
            ObjectFormat::Sha1,
        )
        .await,
    );
    (log, directory, repository)
}

async fn checkpointed_log(options: Options, fixture: &Fixture) -> Log {
    let (log, directory, repository) = published_repository(options, fixture).await;
    require(support::assert_repository(
        &directory.path().join("checkpoint-cache"),
        fixture,
    ));
    if !matches!(
        require(repository.checkpoint().await),
        CheckpointStatus::Published(_)
    ) {
        std::process::abort();
    }
    log
}

fn fixture(payload_bytes: usize) -> Fixture {
    require(support::fixture(
        "source",
        payload_bytes,
        u64::try_from(payload_bytes).unwrap_or(u64::MAX),
    ))
}

fn require<T, E>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(_) => std::process::abort(),
    }
}

fn runtime() -> Runtime {
    require(Builder::new_current_thread().enable_all().build())
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(8));
    targets = publication_benchmarks, checkpoint_benchmarks, recovery_benchmarks
}
criterion_main!(benches);
