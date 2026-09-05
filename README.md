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
proofs and publish them with that exact view. `stage_objects` fully verifies
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
  recovery, and cold recovery into a standard bare repository. Its checkpoint
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
- [`object-log-git-http`](crates/object-log-git-http) is the native adapter for
  the shared engine. It supports both hashes, protocol-v2 upload, and classic
  receive-pack. The earlier SHA-1 protocol-v0 oracle remains selectable.
- [`object-log-git-spin`](crates/object-log-git-spin) adapts WASI HTTP and the
  established S3 client to the same engine. It needs no filesystem preopens;
  the generic log remains independent from Spin.

Tasks 1–9 provide the private pack, sparse reader/writer, wire, and budget
foundations plus one common `Repository::open(&Log, ObjectFormat)` for native
and `WASIp2`. The repository retains one exact view and exposes its refs without
local paths. Durable packs use authenticated variable chunk geometry, including
logs with 8,240-byte object limits. Head transfer, recovery scratch, retained
state, and catalog allocations are budgeted before allocation. The native oracle
remains available through `open_native` until client, provider, runtime, performance, and required owner reviews permit
its deletion.

The replacement has bounded iterative commit, tree, and tag traversal with
command-local catalogs, so ref discovery avoids index loads. Known blob leaves
are deferred until selected content needs verification. Exact want/have
selection, protocol-v2 upload commands, and classic receive preparation and
publication now use that same repository. Thin inputs become self-contained
packs; ref updates validate connectivity and fast-forward rules before one
publication. The native HTTP replacement and Spin adapter are under final
client, provider, and memory qualification. See the
[receive evidence](docs/evidence/git-receive-2026-09-04.md).
The [Task 3 evidence](docs/evidence/git-repository-2026-09-04.md) records the
recovery fixes, unchanged small-limit tests, independent reviews, and limits.
An 88 MiB engine pool admits one operation per native process or WASI instance.
Spin deployment forces one live component instance. A fresh Linux serving
process passes a hard 128 MiB cgroup with a prepared executable cache, but
empty-cache compilation exceeds that cap. See the
[Linux qualification](docs/evidence/git-spin-linux-2026-09-04.md) for cache setup
and the measured lack of spare process-memory margin. The package manifest declares the
Tokio runtime support used by the native oracle, so standalone all-feature
checks do not depend on workspace feature unification. The
[Git proof plan](GIT_PLAN.md) defines the 12 tasks, phase limits, performance
gates, and source-size review thresholds.

The current contracts are in [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md),
[SQLITE_PLAN.md](SQLITE_PLAN.md), and [docs/design.md](docs/design.md). The
[`StagedObject` evidence](docs/evidence/staged-objects-local-2026-09-03.md)
records request counts, transferred bytes, and recovery checks. The
[`materialized proof` evidence](docs/evidence/materialized-proofs-2026-09-04.md)
records the no-read checkpoint path and its proof boundaries. The
[`API simplification` evidence](docs/evidence/api-simplification-local-2026-09-03.md)
records allocation, encoding, line-count, and API changes. The
[`observed-state API` evidence](docs/evidence/observed-state-api-2026-09-04.md)
records the current accepted API shape, local measurements, and line counts. The
[`SQLite` evidence](docs/evidence/sqlite-local-2026-09-03.md) records tests,
local measurements, and remaining qualification work. The
[`Git` evidence](docs/evidence/git-local-2026-09-03.md) records storage
measurements and the `MinIO` lifecycle. The
[`Git HTTP` evidence](docs/evidence/git-http-local-2026-09-03.md) records the
protocol proof. The
[`Git server` evidence](docs/evidence/git-server-local-2026-09-03.md) records
native host tests and limits. The
[`Git WASI baseline`](docs/evidence/git-wasi-baseline-2026-09-04.md) records the
native line count, protocol trace, request bytes, and latency baseline for the
WASI-compatible replacement. The
[`Git WASI contract`](docs/evidence/git-wasi-contract-2026-09-04.md) records the
first target boundary, CI gate, dependency graph, and limitations. The
[`Git WASI pack engine`](docs/evidence/git-wasi-pack-2026-09-04.md) records the
pack normalizer's behavior, limits, tests, source size, and local timing. The
[`Git WASI durable reader`](docs/evidence/git-wasi-durable-reader-2026-09-04.md)
records durable layout, sparse request counts, cache behavior, limits, and
remaining engine work. The
[`Git WASI wire protocol`](docs/evidence/git-wasi-wire-2026-09-04.md) records
protocol behavior, exact fixtures, limits, `WASIp2` checks, and remaining host
integration work. The
[`Git fetch-pack writer`](docs/evidence/git-fetch-pack-2026-09-04.md) records
task-2 behavior, validation, line counts, and the remaining connection work.

[docs/follow-ons.md](docs/follow-ons.md) orders the Git, WASI filesystem, and
live AWS qualification goals.

[GitHub issue #11](https://github.com/carsonfarmer/object-log/issues/11) is the
current index of active limitations and follow-on work. Each linked issue has
its own scope, acceptance criteria, dependencies, and evidence.

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
make git-shared-minio-test
make git-spin-memory-acceptance
make git-spin-minio-test
```

The `MinIO` targets start a pinned container on a loopback port. They create an
empty test bucket and remove the container when the test ends. They
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
for the accepted low-level WAL access boundary. Local results do not
qualify live AWS or remote object-store performance.
