# object-log

[![CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is a pre-release Rust library for publishing a linearizable,
byte-oriented log over conditional object storage. Each logical log has one
mutable head. Commits, payloads, checkpoints, and collection plans are
immutable, so application state can be rebuilt without a durable local cache.

The library supplies ordering, recovery, authenticated object references,
checkpoints, reader retention, and bounded garbage collection. Applications
supply their own operation, result, and snapshot formats.

## Storage contract

A backend must provide:

- create-if-absent writes;
- version-based conditional updates;
- conditional reads;
- consistent read-after-write behavior; and
- stable immutable bytes until `object-log` garbage collection removes them.

`ValidatedBackend::new` probes those capabilities once and rejects unsupported
stores. `object_store::local::LocalFileSystem` is useful for immutable-object
tests but cannot host a log because it lacks conditional updates. The memory
backend, `MinIO`, and AWS S3 satisfy the tested protocol.

Only `<prefix>/v1/logs/<log-id>/index.cbor` is mutable. A conditional update to
that object is the publication point. Everything else has a create-only key
containing both a random physical identifier and a BLAKE3 content digest.
External lifecycle expiry, overwrite, or deletion of protocol objects violates
the storage contract.

## Example

```rust,no_run
use std::sync::Arc;

use bytes::Bytes;
use object_log::{CommitStatus, Log, LogId, Options, TransactionId, ValidatedBackend};
use object_store::{memory::InMemory, path::Path};

# async fn example() -> Result<(), object_log::Error> {
let backend = ValidatedBackend::new(
    Arc::new(InMemory::new()),
    Path::from("object-log-demo"),
)
.await?;
let log = Log::open(&backend, &LogId::new("orders")?, Options::default()).await?;
let view = log.load().await?;

let prepared = log.prepare(
    &view,
    TransactionId::new(),
    Bytes::from_static(b"set:order/42"),
    Bytes::from_static(b"accepted"),
    Vec::new(),
)?;

// Persist this before publication when the operation must survive process loss.
let recovery_token = prepared.recovery_token()?;

match log.commit(prepared).await? {
    CommitStatus::Committed(next) => {
        println!("published generation {}", next.generation());
    }
    CommitStatus::Conflict(winner) => {
        println!("retry against generation {} after revalidation", winner.generation());
    }
    CommitStatus::Pending(_) => {
        // The write may have succeeded. Preserve the token and resolve it with
        // `Log::resume`; never replay non-idempotent work as a new operation.
        println!("publication outcome is uncertain: {} token bytes", recovery_token.len());
    }
}
# Ok(())
# }
```

A candidate is prepared against one immutable `View`. Publication returns a
confirmed commit, a definite conflict with a newer view, or an explicit pending
result when a storage failure can hide success. The core never silently rebases
application work.

Large values can be written through `ByteWriter` and read with authenticated,
bounded `read_at` calls. Reference nodes form application-defined trees without
exposing storage paths. `materialize` can rebuild typed state from a checkpoint
and the active tail while preserving process-local publication proofs.

## Checkpoints and collection

The mutable head and every encoded object have configurable limits. Applications
publish checkpoints before the tail reaches its limit. Collection then:

1. authenticates the current checkpoint, tail, and every live reference-node edge;
2. publishes a positive deletion plan through the same conditional head;
3. deletes only the objects named by that durable plan; and
4. clears the exact plan after all deletions succeed.

Collection protects referenced blob keys without reading their opaque payloads.
It does not audit leaf corruption or missing contents; ordinary reads and
publication validation still verify those bytes.

The WAL checks each publication's dependency count, including its enclosing
commit or checkpoint, against `max_collection_objects`. Consumers do not count
storage chunks. Shared descendants count per reference path for admission;
collection deduplicates physical objects. Historical roots can exceed the bound
together, so checkpoint the full tail before collecting them.
`start_collection_with_limit` bounds a new deletion plan independently of that
live-graph limit. An already installed plan is resumed unchanged.

Long readers acquire a retention ID before opening application data and release
it after their last read. Any retention blocks a new collection plan. Retentions
do not expire automatically; clearing IDs lost by a stopped process requires an
explicit stop-ingress-and-drain procedure.

See [the protocol design](https://github.com/carsonfarmer/object-log/blob/main/docs/design.md)
for the durable format and recovery invariants. The schema is defined in
[`schema/object-log-v1.cddl`](https://github.com/carsonfarmer/object-log/blob/main/schema/object-log-v1.cddl).

## Examples

- [`object-log-kv`](https://github.com/carsonfarmer/object-log/tree/main/crates/object-log-kv)
  is a byte-key/value store with sparse reads and writes, atomic batches,
  immutable snapshots, ordered scans, and checkpoint recovery. Its guide defines
  a bounded small-record profile, local qualification and the caller's recovery
  and maintenance responsibilities.
- [`examples/git`](https://github.com/carsonfarmer/object-log/tree/main/examples/git)
  is a working Git service using go-git and the
  same public WAL API through a small `WASIp2` bridge. It supports ordinary Git
  clients, SHA-1 and SHA-256, protocol-v2 clone/fetch, classic push, shallow
  history, configured repository names, Cognito permissions, recovery, and
  automatic maintenance. The optional
  [AWS host](https://github.com/carsonfarmer/object-log/blob/main/examples/git/qualification/aws/HOSTING.md)
  supplies HTTPS, instance-role credentials and a bounded maintenance worker.
  Use the provider suite to qualify the intended storage, client and host limits.

The core library has no Git, Spin, or serverless-runtime dependency.

## Development

The repository pins its Rust and Go toolchains. Run the complete local gate:

```sh
make check
```

Run the opt-in `MinIO` protocol suite or large garbage-collection acceptance
case:

```sh
make minio-test
make gc-acceptance
```

The [Git example guide](https://github.com/carsonfarmer/object-log/blob/main/examples/git/README.md)
covers its build, local service, tests, maintenance, and known limits. See
[CONTRIBUTING.md](https://github.com/carsonfarmer/object-log/blob/main/CONTRIBUTING.md)
for the development workflow.

## Stability

The API and durable format are pre-release. Development revisions may require a
fresh object-store namespace; compatibility readers for earlier development
formats are intentionally absent. A tagged durable-format release will require a
new format version for incompatible changes.

`object-log` is licensed under
[Apache-2.0](https://github.com/carsonfarmer/object-log/blob/main/LICENSE).
Dependency and retained source notices are described in
[THIRD_PARTY.md](https://github.com/carsonfarmer/object-log/blob/main/THIRD_PARTY.md).
Please report security issues according to
[SECURITY.md](https://github.com/carsonfarmer/object-log/blob/main/SECURITY.md).
