# object-log

`object-log` is an experimental Rust library for a linearizable log over
conditional object storage.

The durable model is small:

- One mutable `index.cbor` object for each logical log.
- Immutable WAL entries, payloads, reference nodes, checkpoints, and
  collection plans.
- Deterministic BLAKE3 content identity plus a random physical ID for each
  deletable object.
- `ETag` compare-and-swap as the publication point.
- A durable positive deletion plan as the collection fence.
- Explicit conflict and uncertain-result states.
- Local memory and disk are optional caches.
- One validated backend handle serves many isolated tenant logs.

The project is independent from Spin. The
[`object-log-kv`](crates/object-log-kv) crate is its first consumer and uses
only the public core API. The [`object-log-sqlite`](crates/object-log-sqlite)
crate is the second consumer. It stores a complete first snapshot and later
committed WAL ranges in the same log contract. Its local memory, fault,
garbage-collection, benchmark, and loopback `MinIO` paths are implemented. The
regular SQLite suite has 43 tests.

See [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md),
[SQLITE_PLAN.md](SQLITE_PLAN.md), and [docs/design.md](docs/design.md) for the
current contracts. The
[SQLite local evidence](docs/evidence/sqlite-local-2026-09-03.md) records the
tests, measured local results, and remaining qualification work.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered Git, WASI
filesystem, and live AWS qualification goals. The
[Git example plan](GIT_PLAN.md) defines a minimal serverless repository over
immutable packs and atomic ref transactions.

## Local checks

```sh
make check
```

Run the opt-in single-flow `MinIO` compatibility test with:

```sh
make minio-test
```

Run the separate SQLite recovery, checkpoint, collection, and cold-recovery
flow with:

```sh
make sqlite-minio-test
```

Run the opt-in large garbage-collection acceptance test with:

```sh
make gc-acceptance
```

The `MinIO` targets start a pinned container on a loopback port. They
create an empty test bucket and remove the container when the test ends. They
do not use a cloud account. The single-flow test includes a 1,001-object
collection boundary. The large acceptance target collects 100,000
memory-backed objects and 10,001 objects from local `MinIO`. Each collection
must complete its timed phase within 30 seconds. See the
[initial baseline](docs/evidence/local-baseline-2026-09-02.md) and the
[GC evidence](docs/evidence/gc-local-2026-09-03.md) for measured local results
and their limits. The
[large GC acceptance record](docs/evidence/gc-acceptance-2026-09-03.md)
contains the exact revision, results, and limitations. See the
[`SQLite` WAL prototype evidence](docs/evidence/sqlite-wal-prototype-2026-09-03.md)
for the accepted low-level WAL access boundary. These local results do not
qualify live AWS or remote object-store performance.
