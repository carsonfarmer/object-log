# object-log

[![Rust CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is an experimental Rust library for a small, generic,
object-storage-backed write-ahead log. The key-value, `SQLite`, and Git crates
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
verify the referenced graph. A collection-epoch change rejects an older proof.

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
- [`object-log-git`](crates/object-log-git) implements strict refs and records,
  SHA-1 and SHA-256 pack normalization, thin-pack normalization, bounded chunk
  storage, reachable-object validation, atomic ref publication, lost-response
  recovery, and cold fetch into an independent standard Git receiver. Its checkpoint
  keeps each pack that contains a live object. Its collection test removes more
  than 100 dead physical objects, cold-recovers the live repository, and passes
  strict Git validation. The proof also has benchmarks, a request audit, and a
  pinned `MinIO` lifecycle. Its replacement pack engine now compiles for
  `WASIp2`, retains a standard Git index, and applies explicit byte, work,
  object, and delta-depth limits. A private sparse reader loads standard
  indexes without a local repository and reads only the required durable pack
  chunks. A private host-neutral wire module implements protocol-v2 `ls-refs`
  and fetch framing plus classic receive-pack framing. A private bounded writer
  creates self-contained SHA-1 and SHA-256 fetch packs. It reuses validated
  compressed entries and materializes an object when reuse would omit its base.
- [`object-log-git-spin`](crates/object-log-git-spin) adapts WASI HTTP and the
  established S3 client to the same engine. It needs no filesystem preopens;
  the generic log remains independent from Spin.

The Git proof provides private pack, sparse reader/writer, wire, and budget
foundations plus one common `Repository::open(&Log, ObjectFormat)` for native
and `WASIp2`. The repository retains one exact view and exposes its refs without
local paths. Durable packs use authenticated variable chunk geometry, including
logs with 8,240-byte object limits. Head transfer, recovery scratch, retained
state, and catalog allocations are budgeted before allocation. The previous
filesystem-backed Git implementation, `open_native` API, and entire native HTTP
host have been removed. Installed Git remains the independent test and benchmark
reference. Spin is the Git HTTP host. The optional local
[`object-log-git-maintain` command](crates/object-log-git-spin/README.md) provides
existing-WAL status, exact commit-token recovery, and conservative metadata
checkpointing with `checkpoint --retain-packs` under a bounded metadata profile.
It clears a qualifying WAL tail while retaining every pack. The command also
starts or resumes collection with `collect`; `collect --resume-only` restricts
it to an already installed plan. Repeat collection until it reports empty to
drain a large backlog: each plan contains a bounded positive deletion set.
The live graph remains bounded, and excessive unknown namespace entries can
exhaust a scan without a plan. Existing retentions block fresh planning.
It migrates the legacy pack catalog with `migrate-catalog --recovery-file`. Migration publishes
an authenticated lookup tree through the same conditional WAL head; cold object
reads load only the indexes for selected packs. After migration,
`compact-packs --recovery-file` repacks reachable objects into bounded output
packs and publishes one replacement catalog. Follow with checkpointing and
collection to reclaim the old packs. Compaction preserves refs and symbolic
`HEAD`; its full live-graph traversal remains subject to operation limits.
The same command can explicitly change the persisted default branch with
`set-default-branch`, an expected old target, and a private recovery-file path.
Unborn targets are supported; cloning follows the persisted symbolic `HEAD`.
Pending publication still requires its exact recovery evidence.

Spin receive consumes bounded frames and stages replayable input before
publication. Small decoded scratch objects use charged request memory, and
larger objects use immutable storage. Interrupted input cannot publish refs;
reopening an expired view retains the same operation counters. Streaming receive
allows 1 GiB blobs and 1,040 MiB incoming packs; commits, trees, and tags retain
an 8 MiB limit. Fetch spans stored packs within cumulative work and transfer
limits. Buffered convenience APIs retain their smaller limits. Fetch streams bounded frames from an
authenticated view with backpressure; late failures abort the response without
a final pack digest. Selected deltas are decoded through bounded read-only
windows. The sustained provider test exercises 1,100 file-changing pushes per
hash, with 35 compaction/checkpoint/collection cycles and cold clone checks.
New Spin repositories use a versioned Git storage profile with 2,080 child
references per object. Repositories using the original default profile keep their durable
options and smaller pack geometry; opening them does not migrate those options.

The replacement has bounded iterative commit, tree, and tag traversal with
command-local catalogs. Ref listing without peeling avoids index loads. Common
advertised-tip fetches skip unrelated histories and blob bodies. Non-tip wants,
stored haves outside the wanted closure, and some shallow requests retain full
reachability checks. Ordinary fetch, receive and maintenance retain object
membership and a traversal frontier without storing every graph edge. Both-hash
Spin/MinIO tests cover a connected history of 32,770 objects across accepted
pushes, full and incremental fetch, compaction, checkpointing, collection and
cold clone. Individual stored packs remain limited to 32,768 objects; shallow,
filtered and URI fetches grow their graph within the existing memory allowance
and pass the same larger-history client tests. All paths remain
subject to memory and operation limits. Known blob leaves are
deferred until selected content needs verification. Exact want/have
selection, protocol-v2 upload commands, and classic receive preparation and
publication now use that same repository. Thin inputs become self-contained
packs; ref updates validate connectivity and exact old IDs before one atomic
publication. Fast-forward-only is the default; Spin operators can explicitly
allow rewritten history with `allow_non_fast_forward`. Ordinary Git-valid
ref namespaces include notes and mirrored refs. Spin passes unchanged-client
and local-provider qualification. Shallow clone, absolute/relative deepening,
unshallow, time cutoffs, and ref exclusions work through protocol v2 for both
hashes; `make git-spin-shallow-test` exercises unchanged clients against local
Spin and `MinIO`.
Partial clones support `blob:none` and `blob:limit` filters, with later retrieval
of reachable objects through ordinary Git promisor requests. The
`make git-spin-partial-test` gate checks both hashes, lazy checkout, shallow
interaction, and cold retrieval after checkpoint and collection. Optional
packfile URI downloads support byte ranges and resume; see the
[Spin configuration](crates/object-log-git-spin/README.md#optional-packfile-uri-downloads).
Spin defaults to authenticated access; see its
[credential-helper setup](crates/object-log-git-spin/README.md).
The current [performance review](https://github.com/carsonfarmer/object-log/issues/23)
passes all 14 functional/resource comparisons. Reusing full-entry receive scan
verification reduced 8 MiB push p50 by about 35% for both hashes in matched
before/after runs; all cases stayed below the native-Git timing-review threshold.
The private proof binds the exact stored source and request; deltas and structural
objects retain normal verification. This guest/InMemory comparison does not
measure HTTP or remote object-store latency.
An 88 MiB engine pool admits one operation per native process or WASI instance.
Git attaches an operation-local request guard to the log and retains it across
retries. Existing caller guards run first; denied admission never removes caller
policy. These counters cover logical storage-client calls and bounded payloads,
while Spin separately bounds HTTP traffic including bootstrap. They are not
whole-process memory measurements. Admission exhaustion returns HTTP 503.
Run the service with ordinary `spin up --from` and the application's manifest.
Tests use Spin defaults for pooling, instance count, and instance memory.
The engine's own bounded operation budgets remain; they do not impose a host
process-memory target or require Spin patches. The Git engine has no
high-level `gix` repository or Tokio filesystem runtime dependency. The
[Git proof contract](GIT_PLAN.md) defines behavior, resource bounds and checks.

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

Run the Git request audit, benchmarks, and pinned `MinIO` lifecycle with:

```sh
make git-performance-acceptance
make git-bench
make git-shared-performance-acceptance
make git-minio-test
make git-spin-memory-acceptance
make git-spin-performance-acceptance
make git-spin-minio-test
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

The Git service has passed the local client and provider proof. Resource bounds
remain explicit in [GIT_PLAN.md](GIT_PLAN.md). The next production-oriented KV
consumer is scoped in [#39](https://github.com/carsonfarmer/object-log/issues/39);
SQLite hardening and live AWS qualification remain separate work.
