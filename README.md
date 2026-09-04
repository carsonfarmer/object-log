# object-log

[![Rust CI](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml/badge.svg)](https://github.com/carsonfarmer/object-log/actions/workflows/ci.yml)

`object-log` is an experimental Rust library for a small, generic,
object-storage-backed write-ahead log. Higher-level storage systems use its
public API. The key-value, `SQLite`, and Git crates are API proofs.

Durable Object behavior, tenancy, routing, and actor or service ownership are
out of scope.

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
- One validated backend handle can open many isolated logs.

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

The project is independent from Spin. The
[`object-log-kv`](crates/object-log-kv),
[`object-log-sqlite`](crates/object-log-sqlite), and
[`object-log-git`](crates/object-log-git) proof crates use only the public core
API. The `SQLite` proof stores a complete first snapshot and later committed WAL
ranges in the same log contract. Its tests include in-memory storage, injected
faults, and garbage collection. A separate local acceptance test recovers an
exact 1,000-record WAL tail. The repository also has Criterion benchmarks and
an opt-in loopback `MinIO` test.

The Git proof implements strict refs and records, SHA-1 and SHA-256 pack
normalization, thin-pack normalization, bounded chunk storage, reachable-object
validation, atomic ref publication, lost-response recovery, and cold recovery
into a standard bare repository. Its checkpoint keeps each pack that contains a
live object. Its collection test removes more than 100 dead physical objects,
then cold-recovers the live repository and passes strict Git validation.
Benchmarks, `MinIO` qualification, a local evidence record, and smart HTTP
remain incomplete.

See [PLAN.md](PLAN.md), [GC_PLAN.md](GC_PLAN.md),
[SQLITE_PLAN.md](SQLITE_PLAN.md), and [docs/design.md](docs/design.md) for the
current contracts. The
[`StagedObject` local evidence](docs/evidence/staged-objects-local-2026-09-03.md)
records request counts, transferred bytes, and recovery checks. The
[`SQLite` local evidence](docs/evidence/sqlite-local-2026-09-03.md) records the
tests, local measurements, and remaining qualification work.

See [docs/follow-ons.md](docs/follow-ons.md) for the ordered Git, WASI
filesystem, and live AWS qualification goals. The
[Git example plan](GIT_PLAN.md) defines a minimal serverless repository over
immutable packs and atomic ref transactions.

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
