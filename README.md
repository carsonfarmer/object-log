# object-log

[![Rust CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is an experimental Rust library for a small, generic,
object-storage-backed write-ahead log. The key-value, `SQLite`, and Git consumers
test its public API.

The design is inspired by Cursor's [Git at any scale](https://cursor.com/blog/git-at-any-scale):
object storage holds the durable log and local repositories can be rebuilt.
The standalone log is the product. Its examples must be complete, useful
applications that demonstrate both correctness and ease of integration.
When an example becomes complicated, distinguish domain requirements from
missing generic capabilities and unnecessary integration machinery. Feed those
lessons back into the log API while keeping domain rules outside the core.

Durable Object behavior, tenancy, routing, and actor or service ownership are
out of scope.

The durable model has:

- One mutable `index.cbor` object for each logical log.
- Immutable WAL entries, payloads, reference nodes, checkpoints, and
  collection plans.
- Deterministic BLAKE3 content identity plus a random physical ID for each
  deletable object.
- `ETag` compare-and-swap as the publication point.
- A durable positive deletion plan as the collection fence.
- Explicit conflict and uncertain-result states.
- Local memory and disk are optional caches.
- One validated backend handle can open many isolated logs.

`Log::open` takes a `ValidatedBackend` and a `LogId`; the internal scoped store
is not part of the public API. `load` returns one cheap-clone `View` for reads
and conditional work. `refresh` returns `None` when that view is still current.
Adapters can use `open_existing` when a missing log must not be initialized,
and `node_size` to check exact reference-node fit before storing its children.
Adapters can call `preflight` before expensive local work. Its successful path
does no I/O and makes no allocation. They can then call `prepare` with the final
operation and staged objects.

For larger byte sequences, `byte_writer(&view)` accepts successive writes and
`finish()` returns one `StagedObject`; publish that root normally or leave it
unpublished for temporary storage. `open_bytes(&view, root.reference())` exposes
the logical length and authenticated `read_at` calls. Reads may be short; callers
advance their offset as with an ordinary reader. The WAL chooses chunk geometry
and enforces its object/reference limits. Discard a writer after a failed or
cancelled write.

Successful immutable creation has one required storage property: the exact
bytes remain at the same physical key until object-log garbage collection
deletes them. External lifecycle expiry, deletion, or overwrite violates this
contract.

`put_object` and `put_node` return process-local `StagedObject` proofs.
`prepare` and `publish_checkpoint` accept those proofs, so the same `Log`
handle or one of its clones can publish without reading the object graph back.
`materialize` accepts one loaded `View` and creates proofs for references in
its authenticated checkpoint and tail records. An adapter can retain those
proofs and publish them with that exact view. `read_staged_node` authenticates
a proven parent and derives child proofs for unchanged-subtree reuse.
`stage_objects` fully verifies
arbitrary durable references before it creates proofs. Recovery tokens do not
contain a proof. `resume` and publication from a separately opened handle fully
verify the referenced graph. A collection-epoch change rejects an older proof. Complete tail reads and
materialization also retain verification on that exact view. Local appends
extend it, so checkpointing avoids rereading immutable commits. A reopened
handle verifies the tail again. Graph verification keeps at most 32 reads in
flight, starting another as each finishes so one slow object does not delay
other ready reads.

The current durable format is v1. Before the first release, its byte layout can
change when a different layout makes the design smaller or better. The project
does not provide compatibility readers for earlier development layouts.

The project is independent from Spin. Its proof crates use only the public core
API:

- [`object-log-kv`](crates/object-log-kv) tests a key-value store.
- [`object-log-sqlite`](crates/object-log-sqlite) stores a complete first
  snapshot and later committed WAL ranges. Its tests cover in-memory storage,
  injected faults, garbage collection, and exact recovery of a 1,000-record WAL
  tail. It also has Criterion benchmarks and an opt-in loopback `MinIO` test.
- [`examples/git`](examples/git) uses go-git for Git protocols and object formats.
  A small [Rust component](examples/wal-component) connects it to the same WAL.
  Refs and a sparse object catalog publish together through one head update.
  Small compressed objects fit in catalog leaves; larger objects use WAL byte streams.
  Incoming deltas stream through go-git into the WAL. Fetch checks reachability
  from published refs without opening blob payloads.
  Spin supplies HTTP; the core has no Spin dependency or Git rules.

The Git consumer replaces the custom Rust Git engine and native maintenance
command. Installed Git remains the independent client and correctness oracle.
See [its README](examples/git/README.md) for build and local `MinIO` instructions,
current limitations, and opt-in provider tests. Local qualification covers
concurrent clients and cleanup, malformed inputs, resource limits and recovery
after a forced restart. A prior endurance-test failure was traced to installed
Git 2.54 background maintenance and reproduced without this service. Remote
provider and deployment testing remain before a production rollout.

The current contracts are in [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md),
[SQLITE_PLAN.md](SQLITE_PLAN.md), and [docs/design.md](docs/design.md).
[docs/follow-ons.md](docs/follow-ons.md) describes the next consumers and
[issue #11](https://github.com/carsonfarmer/object-log/issues/11) indexes the queue.

## Local checks

```sh
make check
```

Run the opt-in core protocol `MinIO` test with:

```sh
make minio-test
```

Run the separate `SQLite` recovery, checkpoint, collection, and cold-recovery
flow with:

```sh
make sqlite-minio-test
```

Run the large local `SQLite` recovery case with:

```sh
make sqlite-recovery-acceptance
```

Run the staged-object request accounting cases with:

```sh
make staged-performance-acceptance
```

Run the opt-in large garbage-collection acceptance test with:

```sh
make gc-acceptance
```

Build and test the Git consumer with Go 1.26.3, the pinned Rust toolchain,
`componentize-go` (from go.mod), and `wac`:

```sh
make git-check
make git-build
# Start the example with ordinary Spin against local MinIO, then:
GIT_PROBE_URL=http://127.0.0.1:19100 make git-provider-test
```

The `MinIO` targets default to a pinned container on a loopback port. To use an
installed native `MinIO` executable instead, set `OBJECT_LOG_MINIO_BINARY` to its
absolute path. Native mode requires Python, `lsof`, and `shasum`; it reports the
binary version and SHA-256, verifies ownership of the loopback listener, and
removes its temporary data after stopping the process. Both modes create an empty
test bucket and run the same assertions without a cloud account. Native results
qualify that host and binary, not Docker or Linux runtime memory limits.
The single-flow test includes a 1,001-object
collection boundary. The large acceptance target collects 100,000
memory-backed objects and 10,001 objects from local `MinIO`. Each collection
must complete its timed phase within 30 seconds, including repeated bounded
batches when the backlog exceeds one plan. Local results do not qualify live
AWS or remote object-store performance.

The next production-oriented KV consumer is scoped in
[#39](https://github.com/carsonfarmer/object-log/issues/39); `SQLite` hardening
and live AWS qualification remain separate work.
