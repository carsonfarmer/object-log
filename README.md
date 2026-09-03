# object-log

`object-log` is an experimental Rust library for a linearizable log over
conditional object storage.

The durable model is small:

- One mutable `index.cbor` object for each logical log.
- Immutable content-addressed WAL entries, payloads, and bases.
- `ETag` compare-and-swap as the publication point.
- Explicit conflict and uncertain-result states.
- Local memory and disk are optional caches.

The project is independent from Spin. The first consumer is a small key-value
state machine. See [PLAN.md](PLAN.md) and [docs/design.md](docs/design.md) for
the accepted scope and protocol.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered garbage
collection, `SQLite`, WASI filesystem, and live AWS qualification goals.

## Local checks

```sh
make check
```

`MinIO` qualification will use a separate opt-in local command. It will not use
a cloud account.
