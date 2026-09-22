# object-log

[![CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is a pre-release Rust library for publishing a linearizable,
byte-oriented log over conditional object storage. Each logical log has one
mutable head. Commits, payloads, checkpoints, and collection plans are
immutable, so application state can be rebuilt without a durable local cache.

The library supplies ordering, recovery, authenticated object references,
checkpoints, reader retention, and bounded garbage collection. Applications
supply their own operation, result, and snapshot formats.

Use it when an application needs to publish related immutable objects as one
ordered update, recover after an uncertain write, and eventually collect data
that is no longer reachable. The library handles durability and ordering;
queries, indexes, conflict policy, authentication, and application formats stay
with the consumer.

## Add the library

The crate has not been published to crates.io, and its API and durable format
have not reached a stable release. Depend on a reviewed repository revision and
use a fresh storage prefix after an incompatible format change. These are the
dependencies used by the example below:

```toml
[dependencies]
object-log = { git = "https://github.com/carsonfarmer/object-log", rev = "<commit>" }
bytes = "1.10"
object_store = { version = "0.14", default-features = false }
tokio = { version = "1.47", features = ["macros", "rt-multi-thread"] }
```

The default crate compiles for native targets and `WASIp2`. The convenience `aws`
feature enables `object_store`'s native AWS and HTTP stack and does not compile
for `WASIp2`. Rust applications that embed the library in WASI inject an
`ObjectStore` with a compatible host transport. Component applications can
instead compose the reusable
[`object-log` `WASIp2` component](examples/wal-component/README.md). It keeps S3
configuration, signing, credentials, HTTP transport, and bounded retries behind
the WIT interface while leaving the core crate runtime independent.

## Storage contract

A backend must provide:

- create-if-absent writes;
- version-based conditional updates;
- conditional reads;
- consistent read-after-write behavior; and
- stable immutable bytes until `object-log` garbage collection removes them;
- deletion for capability-probe cleanup; and
- prefix listing and repeatable deletion when collection is enabled.

`ValidatedBackend::new` probes those capabilities once, including prefix listing
and two deletions of the same probe object, and rejects unsupported stores.
`object_store::local::LocalFileSystem` is useful for immutable-object
tests but cannot host a log because it lacks conditional updates. The memory
backend, `MinIO`, and AWS S3 satisfy the tested protocol.

The application owns the bucket and access policy. `ValidatedBackend::new`
writes, reads, conditionally updates, lists, and repeatedly deletes one object
under an isolated probe log. Normal publication needs reads and conditional writes. Collection
also lists the log prefix and deletes immutable objects. A single deployment
credential therefore needs read, write, list, and delete access under its root
prefix; deployments that separate publication from maintenance can scope those
phases independently, while the process constructing `ValidatedBackend` still
needs permission to delete its probe object. Prevent external lifecycle rules
from expiring protocol objects.

Only `<prefix>/v1/logs/<log-id>/index.cbor` is mutable. A conditional update to
that object is the publication point. Everything else has a create-only key
containing both a random physical identifier and a BLAKE3 content digest.
External lifecycle expiry, overwrite, or deletion of protocol objects violates
the storage contract.

## Example

```rust,no_run
use std::sync::Arc;

use bytes::Bytes;
use object_log::{
    CommitStatus, Log, LogId, Options, Resolution, TransactionId, ValidatedBackend,
};
use object_store::{memory::InMemory, path::Path};

#[tokio::main]
async fn main() -> Result<(), object_log::Error> {
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

// Persist these before publication when the request must survive process loss.
let recovery_token = prepared.recovery_token()?;
let result = prepared.result().clone();

match log.commit(prepared).await? {
    CommitStatus::Committed(next) => {
        println!(
            "published generation {}; return {} result bytes",
            next.generation(),
            result.len()
        );
    }
    CommitStatus::Conflict(winner) => {
        println!("retry against generation {} after revalidation", winner.generation());
    }
    CommitStatus::Pending(_) => match log.resume(&recovery_token).await? {
        Resolution::Committed(next) => {
            println!(
                "published generation {}; return {} result bytes",
                next.generation(),
                result.len()
            );
        }
        Resolution::NotCommitted(winner) => {
            println!("revalidate generation {} before a deliberate retry", winner.generation());
        }
        Resolution::StillPending(_) => println!("retain the token and resolve it later"),
        Resolution::Expired(_) => println!("outcome unknown; do not replay the operation"),
    },
}
Ok(())
}
```

A candidate is prepared against one immutable `View`. Publication returns a
confirmed commit, a definite conflict with a newer view, or an explicit pending
result when a storage failure can hide success. The core never silently rebases
application work.

The recovery token identifies this exact candidate and publication position,
including its operation and result bytes. Durably retain it, along with any
other application response or state needed to finish the request, before
calling `commit`. Resolve a pending result with `Log::resume`; do not submit
non-idempotent work again under a new transaction ID. `Committed` permits
returning the saved result. `NotCommitted` permits a deliberate retry after the
application revalidates current state. Preserve `StillPending` evidence and
resolve it later. Once the answer is `Expired`, the library cannot prove whether
the candidate committed, so the application must not replay it.

Large values can be written through `ByteWriter` and read with authenticated,
bounded `read_at` calls. Reference nodes form application-defined trees without
exposing storage paths. `materialize` can rebuild typed state from a checkpoint
and the active tail while preserving process-local publication proofs.
`history` returns a bounded cursor over the same authenticated checkpoint and
ordered commits, including transaction IDs and recorded results, for bindings
or consumers that apply their state transitions outside Rust. It returns one
record at a time and remains bound to the exact view being reconstructed.

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

The [native Spin key-value provider](integrations/spin-key-value/README.md) lets
custom Spin runtimes register object-log as a host-side store at compile time.
Guests keep the standard key-value API; S3 credentials and transport stay in the
host. Spin dependencies remain outside the core and KV workspaces.

- [`object-log-kv`](https://github.com/carsonfarmer/object-log/tree/main/crates/object-log-kv)
  is a byte-key/value store with sparse reads and writes, atomic batches,
  immutable snapshots, ordered scans, and checkpoint recovery. Its guide defines
  the locally qualified small-record profile and the caller's recovery and
  maintenance responsibilities.
- [`examples/git`](https://github.com/carsonfarmer/object-log/tree/main/examples/git)
  is a working Git service using go-git and the
  same public WAL API through the reusable `WASIp2` component. It supports ordinary Git
  clients, SHA-1 and SHA-256, protocol-v2 clone/fetch, classic push, shallow
  history, configured repository names, Cognito permissions, recovery, and
  bounded maintenance endpoints. The optional
  [AWS host](https://github.com/carsonfarmer/object-log/blob/main/examples/git/qualification/aws/HOSTING.md)
  supplies HTTPS, instance-role credentials, and a worker that schedules those
  endpoints.
  Use the provider suite to qualify the intended storage, client and host limits.

The core library has no Git, Spin, or serverless-runtime dependency.

The Git service has also been qualified over public HTTPS with Cognito and S3.
That deployment demonstrates one operating model; it does not add hosting or
authentication concerns to the Rust library.

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

The crate forbids unsafe Rust and denies missing public documentation. Native
and `WASIp2` builds, deterministic fault simulation, `MinIO` provider tests, large
collection tests, and API doctests run in the project gates. Consumers should
still qualify their object-store provider, limits, and maintenance schedule with
their own workload before deploying it.

`object-log` is licensed under
[Apache-2.0](https://github.com/carsonfarmer/object-log/blob/main/LICENSE).
Dependency and retained source notices are described in
[THIRD_PARTY.md](https://github.com/carsonfarmer/object-log/blob/main/THIRD_PARTY.md).
Please report security issues according to
[SECURITY.md](https://github.com/carsonfarmer/object-log/blob/main/SECURITY.md).
