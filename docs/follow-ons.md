# Ordered follow-on goals

The local log, checkpoint, key-value proof, bounded garbage collection, and
SQLite proof are implemented. `object-log` remains a small, generic,
object-storage-backed WAL for higher-level storage systems. Git, key-value, and
SQLite are proof crates. Each next goal keeps object storage as the durable
authority.

Durable Object behavior, tenancy, routing, and actor or service ownership are
out of scope.

## Completed: garbage collection

The implementation contract and completion record are in
[`GC_PLAN.md`](../GC_PLAN.md). The v1 protocol has bounded graph marking,
reader retention, a positive durable plan and fence, complete-set retry, view
expiry, and best-effort plan-object cleanup. Current qualification is local.

## Completed locally: SQLite storage

The selected demonstration contract and implementation gates are in
[`SQLITE_PLAN.md`](../SQLITE_PLAN.md). The adapter uses a disposable SQLite
cache, the current canonical record, committed WAL ranges, per-record payload
bounds, and ordered object transfers.

### Implemented contract

- One log owns one SQLite database history.
- One SQLite transaction maps to one atomic log publication.
- Recovery produces one database image that passes SQLite integrity checks.
- A local database file is a cache. Removing it does not remove durable state.
- The adapter defines its page size, journal mode, and lock behavior.

### Local evidence

- The memory suite covers transactions, rollback, uncertain results,
  cancellation, conflicting writers, checkpoints, allocation limits,
  collection, and deleted-cache recovery.
- The loopback MinIO flow covers chunked writes, uncertain publication,
  checkpointing, collection, and cold recovery.
- The Criterion suite covers 11 local latency cases. A separate untimed audit
  records object requests, transferred bytes, and durable growth. Neither path
  measures a remote service.

The same WAL-access proof passed on macOS and Linux with SQLite's public
journal-pointer control. Before production use, add aggregate recovery and
transfer-byte limits, bound recovery retries, and isolate synchronous `SQLite`
work on a capped blocking executor. Recovery can stream validated WAL ranges
after those limits are in place. Windows, other VFS implementations, a native
memory-safety sanitizer, live AWS, and Spin integration remain.

## Core performance decision

The core now returns an opaque, process-local `StagedObject` proof from new
object writes. The proof belongs to one `Log` handle or its clones and one
collection epoch. Same-handle publication uses it without reading the new
object graph back. `stage_objects` fully verifies existing durable references
before it creates proofs. Recovery tokens omit the proof, and recovered or
separately opened work uses full graph verification.

`materialize` also creates same-handle proofs for references in the
authenticated checkpoint and tail records of its exact view. An adapter can
retain them in materialized state and publish a checkpoint without rereading
the graph. A collection-epoch change invalidates the proofs.

This fast path requires exact immutable bytes to remain at their physical key
until object-log garbage collection deletes them. External lifecycle expiry,
deletion, or overwrite violates the storage contract.

The API and its local acceptance evidence are complete. The
[staged-object evidence](evidence/staged-objects-local-2026-09-03.md) records
new-object request counts, transferred bytes, recovery checks, and limits. The
[materialized-proof evidence](evidence/materialized-proofs-2026-09-04.md)
records no-read Git checkpoints and the proof boundary.

[Issue #11](https://github.com/carsonfarmer/object-log/issues/11) indexes all
current limitations and follow-on work. The linked issues define separate
acceptance criteria for SQLite hardening, Spin factors, Git, WASI filesystem,
verification, performance, and live AWS qualification.

## 1. Git storage API proof

`object-log` is the product. It is a small, generic, object-storage-backed WAL
for higher-level storage systems. `object-log-git` is a separate proof of its
public API.

The native storage proof is implemented. It accepts parsed ref commands and an
optional pack path. It validates refs, pack bytes, reachable objects, and pack
provenance. It then prepares and publishes one atomic ref transaction. Cold
open rebuilds a standard bare repository from object storage. The proof uses
`gix` and `gix-pack`. It does not run a Git executable or call a C library. See
[`GIT_PLAN.md`](../GIT_PLAN.md) and the
[library review](evidence/git-library-selection-2026-09-03.md).

Checkpoint selection and collection acceptance are implemented. A checkpoint
retains each pack that contains a live object. The acceptance test removes more
than 100 dead physical objects, preserves the live pack, cold-recovers the
repository, and passes strict Git validation. The request audit, benchmarks,
pinned `MinIO` lifecycle, and local evidence are complete.

Smart HTTP is a separate proof crate. The core WAL does not depend on Git
protocol code or Git libraries. The current protocol v0 Axum host is a native
test reference. Its loopback tests use an unchanged client for clone, fetch,
push, branch and tag changes, and non-fast-forward rejection.

Issue [#17](https://github.com/carsonfarmer/object-log/issues/17) replaces the
native-only Git core. The first host can remain native. Protocol, pack, object
lookup, and publication code must compile for `wasm32-wasip2` so a later Spin
component remains a thin adapter. Upload-pack discovery and fetch must use
protocol v2 and subtract valid client `have` objects. Push keeps classic
receive-pack. The new engine must pass the current tests before the native
repository materializer and protocol v0 upload-pack path are deleted. See
[`GIT_PLAN.md`](../GIT_PLAN.md).

## 2. WASI filesystem storage

### Required contract

- One log owns one filesystem namespace.
- The adapter defines stable inode identity and capability-scoped roots.
- Directory mutation and rename have explicit atomicity rules.
- File data uses immutable chunk objects. Metadata publication makes new data
  visible.
- Open handles, sparse files, timestamps, links, and deletion have one stated
  behavior each.
- The adapter implements the current `wasi:filesystem` interface without a
  second durable metadata authority.

### Required evidence

- The WASI filesystem conformance surface passes for supported operations.
- Generated operation traces compare the adapter with its reference model.
- Tests cover rename races, removed open files, large files, partial writes,
  crash recovery, and namespace isolation.
- Benchmarks report metadata latency, sequential and random I/O, cold restore,
  write amplification, and object-store requests.

## 3. Live AWS qualification

Live AWS qualification is separate from local product completion.

Before any run, record:

- The exact revision and AWS region.
- One isolated bucket or prefix and its lifecycle settings.
- Required credentials and least-privilege actions.
- S3 storage class, versioning, encryption, and consistency assumptions.
- Workloads, request count limit, cost limit, and time limit.
- Health checks and terminal assertions.
- Recovery steps and mandatory teardown.

Run backend conformance first. Then run protocol faults and recovery. Run the
performance matrix last. Do not reuse production data or credentials. Do not
run a second live campaign after a failed campaign without owner review of the
cause and corrected plan.
