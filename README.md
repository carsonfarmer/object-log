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
only the public core API. The log, checkpoints, object graph, Cursor-style
bounded garbage collection, local benchmarks, and `MinIO` compatibility flow
are complete for local use. The [`SQLite` demonstration plan](SQLITE_PLAN.md)
is complete. Its WAL-access gate passed on local macOS and Linux, and adapter
implementation is next. See [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md), and
[docs/design.md](docs/design.md) for the current core contract.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered `SQLite`, Git,
WASI filesystem, and live AWS qualification goals. The
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

Run the opt-in large garbage-collection acceptance test with:

```sh
make gc-acceptance
```

Both `MinIO` test targets start a pinned container on a loopback port. They
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
for the accepted low-level WAL access boundary.
