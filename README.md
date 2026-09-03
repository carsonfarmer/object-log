# object-log

`object-log` is an experimental Rust library for a linearizable log over
conditional object storage.

The durable model is small:

- One mutable head object for each logical log.
- Immutable content-addressed commits, blobs, and checkpoints.
- `ETag` compare-and-swap as the publication point.
- Explicit conflict and uncertain-result states.
- Local memory and disk are optional caches.

The project is independent from Spin. The first consumer is a small key-value
state machine. See [PLAN.md](PLAN.md) and [docs/design.md](docs/design.md) for
the accepted scope and protocol.

## Local checks

```sh
make check
```

`MinIO` qualification will use a separate opt-in local command. It will not use
a cloud account.
