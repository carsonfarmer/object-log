# object-log

[![Rust CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is an experimental Rust library for a small, generic,
object-storage-backed write-ahead log. The key-value, `SQLite`, and Git crates
test its public API.

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
`stage_objects` fully verifies existing durable references before it creates
proofs. Recovery tokens do not contain a proof. `resume` and publication from
a separately opened handle fully verify the referenced graph.

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
  object, and delta-depth limits.
- [`object-log-git-http`](crates/object-log-git-http) is the current native
  protocol-v0 reference host for SHA-1. Its tests use an unchanged Git client.
  Issue #17 moves protocol and storage into one WASI-compatible core while the
  native host remains the first adapter. Upload-pack discovery and fetch use
  protocol v2. Push remains standard receive-pack.

The current contracts are in [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md),
[SQLITE_PLAN.md](SQLITE_PLAN.md), and [docs/design.md](docs/design.md). The
[`StagedObject` evidence](docs/evidence/staged-objects-local-2026-09-03.md)
records request counts, transferred bytes, and recovery checks. The
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
next tranche's behavior, limits, tests, source size, and local timing.

[docs/follow-ons.md](docs/follow-ons.md) orders the Git, WASI filesystem, and
live AWS qualification goals. The [Git proof plan](GIT_PLAN.md) defines one
WASI-compatible Git engine over immutable packs and atomic ref transactions.

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
make git-minio-test
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
