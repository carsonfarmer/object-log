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

`materialize` accepts one loaded `View` and creates same-handle proofs for
references in its authenticated checkpoint and tail records. An adapter can
retain them in materialized state and publish a checkpoint with that exact view
without rereading the graph. A collection-epoch change invalidates the proofs.

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

The shared engine now supports both hashes, protocol-v2 discovery and
have-aware fetch, classic receive-pack, thin packs, atomic publication,
checkpoint/collection, and cold recovery. Native and actual Spin WASIp2
qualification passed with unchanged Git clients and local MinIO. The core
log remains generic and independent from Git and Spin.

The previous filesystem-backed native Git engine and entire native HTTP host
are removed. Useful client tests run against actual Spin, and portable tests
cover uncertain publication and recovery. Installed Git remains the independent
correctness/performance reference. The
shared library exposes one `Repository::open(&Log, ObjectFormat)` and
byte-oriented upload/receive commands, with command-local indexes and sparse
range reads. No scratch Git repository is required.

An 88 MiB pool admits one engine operation per native process or WASI instance.
Run ordinary Spin with default runtime settings. No one-instance launcher,
pooling workaround, or imposed host-memory cap is required. The SHA-1 8 MiB
WASIp2 push remains approximately 1.65 times its native Git timing baseline;
removing old product code does not resolve that observation.

The bounded functional proof is complete, but the broader Git proof remains
open until it demonstrates useful scale and straightforward integration:

- [#19: compaction and scale](https://github.com/carsonfarmer/object-log/issues/19):
  bound catalog work as live packs accumulate, preserve atomic replacement and
  GC safety, and measure sustained Spin/MinIO workloads before and after.
- [#21: memory and admission](https://github.com/carsonfarmer/object-log/issues/21):
  measure normal runtime behavior and concurrent clients using Spin defaults.
- [#22: pooled HTTP failure](https://github.com/carsonfarmer/object-log/issues/22):
  revisit only if ordinary Spin testing reveals a blocking problem. No Spin
  patch or upstream work is a prerequisite.
- [#23: large-push performance](https://github.com/carsonfarmer/object-log/issues/23):
  profile the SHA-1 8 MiB WASIp2 receive finding before choosing an optimization.
- [#24: clone extensions](https://github.com/carsonfarmer/object-log/issues/24):
  shallow/deepen/unshallow, partial and filtered clones, and packfile URIs with
  unchanged clients, both hashes, cold recovery and GC. Do not advertise them
  before implementation and provider acceptance. Larger object/pack/history
  support needs measured range-backed processing, not merely larger constants.
- [#25: simplification](https://github.com/carsonfarmer/object-log/issues/25):
  remove avoidable machinery and identify missing generic capabilities without
  moving Git policy into the core or reducing required behavior.

See [GIT_PLAN.md](../GIT_PLAN.md) and the dated evidence for exact scopes,
limits, and qualification conditions. The filesystem provider's missing
conditional compare-and-swap remains separately tested. Live AWS is #10.

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
