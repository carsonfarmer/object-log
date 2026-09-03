use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use object_log::{CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_log_sqlite::{Database, SqliteCheckpointStatus, StageStatus, StagedWrite};
use object_store::memory::InMemory;
use object_store::path::Path;
use rusqlite::Connection;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

const MIB: usize = 1_024 * 1_024;
const SMALL_BYTES: usize = 64;
const STATE_REUSE: u64 = 16;

struct AdapterState {
    log: Log,
    database: Database,
    directory: TempDir,
}

fn transaction_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    for (name, payload_bytes) in [("small", SMALL_BYTES), ("1_mib", MIB)] {
        let mut group = criterion.benchmark_group(format!("sqlite/transaction/{name}"));
        group.throughput(Throughput::Bytes(usize_to_u64(payload_bytes)));
        group.bench_function("direct", |bencher| {
            let (mut connection, _directory) = direct_database(payload_bytes);
            let mut generation = 0_i64;
            bencher.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    generation = generation.saturating_add(1);
                    let started = Instant::now();
                    direct_update(&mut connection, generation, payload_bytes);
                    elapsed += started.elapsed();
                    truncate_direct(&connection);
                }
                elapsed
            });
        });

        group.bench_function("adapter", |bencher| {
            let mut state = runtime.block_on(checkpointed_adapter(payload_bytes));
            let mut generation = 0_i64;
            let mut state_uses = 0_u64;
            bencher.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        if state_uses == STATE_REUSE {
                            state = checkpointed_adapter(payload_bytes).await;
                            state_uses = 0;
                        }
                        generation = generation.saturating_add(1);
                        let started = Instant::now();
                        publish_update(&mut state.database, generation, payload_bytes).await;
                        elapsed += started.elapsed();
                        checkpoint(&mut state.database).await;
                        state_uses += 1;
                    }
                    elapsed
                })
            });
        });
        group.finish();
    }
}

fn chunked_wal_benchmark(criterion: &mut Criterion) {
    let runtime = runtime();
    let options = Options {
        max_inline_operation_bytes: 1_024,
        max_object_bytes: 8_240,
        ..Options::default()
    };
    let mut group = criterion.benchmark_group("sqlite/transaction/1_mib_chunked");
    group.throughput(Throughput::Bytes(usize_to_u64(MIB)));
    group.bench_function("adapter", |bencher| {
        let chunks = runtime.block_on(async {
            let mut state = checkpointed_adapter_with_options(MIB, options).await;
            publish_update(&mut state.database, 1, MIB).await;
            let view = require(state.log.load().await);
            let records = require(state.log.read_tail(&view).await);
            records.last().map_or(0, |record| record.objects().len())
        });
        if !(120..=140).contains(&chunks) {
            std::process::abort();
        }
        bencher.iter_batched(
            || runtime.block_on(checkpointed_adapter_with_options(MIB, options)),
            |mut state| {
                runtime.block_on(publish_update(&mut state.database, 1, MIB));
                black_box(state)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn read_benchmark(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("sqlite/read");
    group.bench_function("adapter/unchanged_refresh", |bencher| {
        let mut state = runtime.block_on(checkpointed_adapter(SMALL_BYTES));
        bencher.iter(|| {
            black_box(runtime.block_on(generation(&mut state.database)));
        });
    });
    group.finish();
}

fn conflict_benchmark(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("sqlite/conflict");
    group.bench_function("adapter/publish_and_rebuild", |bencher| {
        bencher.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let mut winner = checkpointed_adapter(SMALL_BYTES).await;
                    let mut loser = require(
                        Database::open(
                            winner.log.clone(),
                            winner.directory.path().join("loser.sqlite3"),
                        )
                        .await,
                    );
                    let winning = stage_update(&mut winner.database, 1, SMALL_BYTES).await;
                    let losing = stage_update(&mut loser, 2, SMALL_BYTES).await;
                    require_committed(winning.publish().await);

                    let started = Instant::now();
                    require_conflict(losing.publish().await);
                    let generation = generation(&mut loser).await;
                    elapsed += started.elapsed();
                    if generation != 1 {
                        std::process::abort();
                    }
                }
                elapsed
            })
        });
    });
    group.finish();
}

fn recovery_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("sqlite/cold_recovery");
    for records in [10_usize, 1_000] {
        group.throughput(Throughput::Elements(usize_to_u64(records)));
        group.bench_with_input(
            BenchmarkId::new("tail", records),
            &records,
            |bencher, &records| {
                let state = runtime.block_on(adapter_with_tail(records));
                let AdapterState {
                    log,
                    database,
                    directory: _directory,
                } = state;
                drop(database);
                bencher.iter_batched(
                    || {
                        let directory = require(tempfile::tempdir());
                        let cache = directory.path().join("database.sqlite3");
                        (directory, cache)
                    },
                    |(directory, cache)| {
                        black_box((
                            require(runtime.block_on(Database::open(log.clone(), cache))),
                            directory,
                        ))
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn checkpoint_benchmarks(criterion: &mut Criterion) {
    let runtime = runtime();
    let mut group = criterion.benchmark_group("sqlite/checkpoint");
    for (name, database_bytes) in [("1_mib", MIB), ("100_mib", 100 * MIB)] {
        group.throughput(Throughput::Bytes(usize_to_u64(database_bytes)));
        group.bench_with_input(
            BenchmarkId::new("database", name),
            &database_bytes,
            |bencher, &database_bytes| {
                bencher.iter_batched(
                    || runtime.block_on(fresh_adapter(database_bytes)),
                    |mut state| {
                        runtime.block_on(checkpoint(&mut state.database));
                        black_box(state)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

async fn fresh_adapter(payload_bytes: usize) -> AdapterState {
    fresh_adapter_with_options(payload_bytes, Options::default()).await
}

async fn fresh_adapter_with_options(payload_bytes: usize, options: Options) -> AdapterState {
    let backend =
        require(ValidatedBackend::new(Arc::new(InMemory::new()), Path::from("sqlite-bench")).await);
    let log = require(Log::open(backend.scope(&require(LogId::new("database"))), options).await);
    let directory = require(tempfile::tempdir());
    let mut database =
        require(Database::open(log.clone(), directory.path().join("active.sqlite3")).await);
    let bytes = usize_to_i64(payload_bytes);
    let StageStatus::Staged(staged) = require(
        database
            .stage_write(TransactionId::new(), move |transaction| {
                transaction.execute_batch(
                    "CREATE TABLE state (generation INTEGER NOT NULL, payload BLOB NOT NULL)",
                )?;
                transaction.execute("INSERT INTO state VALUES (0, zeroblob(?1))", [bytes])?;
                Ok(Bytes::new())
            })
            .await,
    ) else {
        std::process::abort();
    };
    require_committed(staged.publish().await);
    AdapterState {
        log,
        database,
        directory,
    }
}

async fn checkpointed_adapter(payload_bytes: usize) -> AdapterState {
    checkpointed_adapter_with_options(payload_bytes, Options::default()).await
}

async fn checkpointed_adapter_with_options(payload_bytes: usize, options: Options) -> AdapterState {
    let mut state = fresh_adapter_with_options(payload_bytes, options).await;
    checkpoint(&mut state.database).await;
    state
}

async fn adapter_with_tail(records: usize) -> AdapterState {
    let mut state = checkpointed_adapter(SMALL_BYTES).await;
    for generation in 1..=records {
        publish_update(&mut state.database, usize_to_i64(generation), SMALL_BYTES).await;
    }
    state
}

async fn stage_update(
    database: &mut Database,
    generation: i64,
    payload_bytes: usize,
) -> StagedWrite<'_> {
    let payload_bytes = usize_to_i64(payload_bytes);
    let StageStatus::Staged(staged) = require(
        database
            .stage_write(TransactionId::new(), move |transaction| {
                transaction.execute(
                    "UPDATE state SET generation = ?1, payload = randomblob(?2)",
                    [generation, payload_bytes],
                )?;
                Ok(Bytes::new())
            })
            .await,
    ) else {
        std::process::abort();
    };
    staged
}

async fn publish_update(database: &mut Database, generation: i64, payload_bytes: usize) {
    require_committed(
        stage_update(database, generation, payload_bytes)
            .await
            .publish()
            .await,
    );
}

async fn checkpoint(database: &mut Database) {
    if !matches!(
        require(database.checkpoint().await),
        SqliteCheckpointStatus::Published(_)
    ) {
        std::process::abort();
    }
}

async fn generation(database: &mut Database) -> i64 {
    require(
        database
            .read(|connection| {
                connection.query_row("SELECT generation FROM state", [], |row| row.get(0))
            })
            .await,
    )
}

fn direct_database(payload_bytes: usize) -> (Connection, TempDir) {
    let directory = require(tempfile::tempdir());
    let connection = require(Connection::open(directory.path().join("direct.sqlite3")));
    require(connection.execute_batch(
        "PRAGMA page_size = 4096;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA wal_autocheckpoint = 0;
         CREATE TABLE state (generation INTEGER NOT NULL, payload BLOB NOT NULL);",
    ));
    require(connection.execute(
        "INSERT INTO state VALUES (0, zeroblob(?1))",
        [usize_to_i64(payload_bytes)],
    ));
    truncate_direct(&connection);
    (connection, directory)
}

fn direct_update(connection: &mut Connection, generation: i64, payload_bytes: usize) {
    let transaction = require(connection.transaction());
    require(transaction.execute(
        "UPDATE state SET generation = ?1, payload = randomblob(?2)",
        [generation, usize_to_i64(payload_bytes)],
    ));
    require(transaction.commit());
}

fn truncate_direct(connection: &Connection) {
    let result: (i64, i64, i64) = require(connection.query_row(
        "PRAGMA wal_checkpoint(TRUNCATE)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ));
    if result.0 != 0 || result.2 != 0 {
        std::process::abort();
    }
}

fn require<T, E>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(_) => std::process::abort(),
    }
}

fn require_committed<E>(result: Result<CommitStatus, E>) {
    if !matches!(require(result), CommitStatus::Committed(_)) {
        std::process::abort();
    }
}

fn require_conflict<E>(result: Result<CommitStatus, E>) {
    if !matches!(require(result), CommitStatus::Conflict(_)) {
        std::process::abort();
    }
}

fn runtime() -> Runtime {
    require(Builder::new_current_thread().enable_all().build())
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = transaction_benchmarks, chunked_wal_benchmark, read_benchmark, conflict_benchmark,
        recovery_benchmarks, checkpoint_benchmarks
}
criterion_main!(benches);
