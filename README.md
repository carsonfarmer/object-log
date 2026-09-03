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

The project is independent from Spin. The first consumer is a small key-value
state machine. The log, checkpoints, object graph, Cursor-style bounded garbage
collection, local benchmarks, and `MinIO` compatibility flow are complete for
local use. `SQLite` storage is next. See [PLAN.md](PLAN.md),
[GC_PLAN.md](GC_PLAN.md), and [docs/design.md](docs/design.md) for the current
contract.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered `SQLite`, WASI
filesystem, and live AWS qualification goals.

## Local checks

```sh
make check
```

Run the opt-in single-flow `MinIO` compatibility test with:

```sh
make minio-test
```

This command starts a pinned `MinIO` container on a loopback port. It creates an
empty test bucket and removes the container when the test ends. It does not use
a cloud account. The flow includes a 1,001-object collection boundary. See the
[initial baseline](docs/evidence/local-baseline-2026-09-02.md) and the
[GC evidence](docs/evidence/gc-local-2026-09-03.md) for measured local results
and their limits.
