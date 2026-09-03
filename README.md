# object-log

`object-log` is an experimental Rust library for a linearizable log over
conditional object storage.

The durable model is small:

- One mutable `index.cbor` object for each logical log.
- Immutable content-addressed WAL entries, payloads, reference nodes, and
  bases.
- `ETag` compare-and-swap as the publication point.
- Explicit conflict and uncertain-result states.
- Local memory and disk are optional caches.
- One validated backend handle serves many isolated tenant logs.

The project is independent from Spin. The first consumer is a small key-value
state machine. See [PLAN.md](PLAN.md) and [docs/design.md](docs/design.md) for
the accepted scope and protocol.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered garbage
collection, `SQLite`, WASI filesystem, and live AWS qualification goals.

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
a cloud account. See the
[local baseline](docs/evidence/local-baseline-2026-09-02.md) for measured
in-memory results and the exact `MinIO` test evidence.
