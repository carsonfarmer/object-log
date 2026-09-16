# Ordered follow-on goals

The local log, checkpoint, key-value proof, and bounded garbage collection are
implemented. `object-log` remains a small, generic, object-storage-backed WAL
for higher-level storage systems. Git and key-value are consumers of the public
API. Each next goal keeps object storage as the durable authority.

Durable Object behavior, tenancy, routing, and actor or service ownership are
out of scope.

## Completed: garbage collection

The implementation contract and completion record are in
[`GC_PLAN.md`](../GC_PLAN.md). The v1 protocol has bounded graph marking,
reader retention, a positive durable plan and fence, complete-set retry, view
expiry, and best-effort plan-object cleanup. Current qualification is local.

## Core performance decision

The core now returns an opaque, process-local `StagedObject` proof from new
object writes. The proof belongs to one `Log` handle or its clones and one
collection epoch. Same-handle publication uses it without reading the new
object graph back. `stage_objects` fully verifies existing durable references
before it creates proofs. Recovery tokens omit the proof, and recovered or
separately opened work uses full graph verification.

`materialize` accepts one loaded `View` and creates same-handle proofs for
references in its authenticated checkpoint and tail records. An adapter can
retain them in materialized state and publish a checkpoint with that exact view
without rereading the graph. A collection-epoch change invalidates the proofs.

This fast path requires exact immutable bytes to remain at their physical key
until object-log garbage collection deletes them. External lifecycle expiry,
deletion, or overwrite violates the storage contract.

The API and its local acceptance evidence are complete. The
[materialized-proof evidence](evidence/materialized-proofs-2026-09-04.md)
records no-read Git checkpoints and the proof boundary.

[Issue #11](https://github.com/carsonfarmer/object-log/issues/11) indexes all
current limitations and follow-on work. The linked issues define separate
acceptance criteria for Spin factors, Git, WASI filesystem, verification,
performance, and live AWS qualification.

## Git and key-value storage

The Git service now exercises the generic WAL through ordinary Spin and
unchanged clients: both hashes, sparse reads, atomic push, full and partial
fetch, large histories, compaction and cold recovery. The final cleanup is
recorded in #25; [GIT_PLAN.md](../GIT_PLAN.md) states its current contract and
finite resource bounds. Installed Git remains the test and benchmark reference.

[KV issue #39](https://github.com/carsonfarmer/object-log/issues/39) scopes a
production-oriented byte-key/value library with conditional atomic batches,
ordered scans, disposable caches, recovery and bounded maintenance. Select its
index and layout from measured workloads. Git's sparse reads, proof reuse and
batched collection should inform it without importing Git policy into the core.
Verifiable g-trees are a separate project.

## 1. WASI filesystem storage

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

## 2. Live AWS qualification

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
