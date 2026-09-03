# Ordered follow-on goals

The local log, checkpoint, key-value proof, bounded garbage collection, and
SQLite demonstration are implemented. Each next goal keeps object storage as
the durable authority.

## Completed: garbage collection

The implementation contract and completion record are in
[`GC_PLAN.md`](../GC_PLAN.md). The v1 protocol has bounded graph marking,
reader retention, a positive durable plan and fence, complete-set retry, view
expiry, and best-effort plan-object cleanup. Current qualification is local.

## Completed locally: SQLite storage

The selected demonstration contract and implementation gates are in
[`SQLITE_PLAN.md`](../SQLITE_PLAN.md). The adapter uses a disposable SQLite
cache, a canonical v1 record, committed WAL ranges, per-record payload bounds,
and ordered object transfers.

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
journal-pointer control. Before multi-tenant use, add aggregate recovery and
transfer-byte limits, bound recovery retries, and isolate synchronous SQLite
work on a capped owner executor. Recovery can stream validated WAL ranges after
those limits are in place. Windows, other VFS implementations, a native
memory-safety sanitizer, live AWS, and Spin integration remain.

## Core performance decision

External publication currently reads each staged object back to prove its
hash and presence. The review recommends an opaque, process-local staged-object
capability bound to one log and collection epoch. New objects could then skip
the read-back. Serialized recovery tokens would keep the current full
verification path. This preserves the durable v1 format. The owner approved
the public API change. [Issue #1](https://github.com/carsonfarmer/object-log/issues/1)
tracks its implementation and evidence. The Git example follows this change.

[Issue #11](https://github.com/carsonfarmer/object-log/issues/11) indexes all
current limitations and follow-on work. The linked issues define separate
acceptance criteria for SQLite hardening, Spin factors, Git, WASI filesystem,
verification, performance, and live AWS qualification.

## 1. Minimal serverless Git

Build `object-log-git` as a separate demonstration crate after SQLite. One log
owns one Git repository. Immutable Git packs contain objects. One object-log
commit atomically records a validated ref transaction and its new pack
references. A checkpoint records the current refs and the packs needed to read
them. See [`GIT_PLAN.md`](../GIT_PLAN.md).

The first example uses a disposable bare repository or temporary directory per
serverless invocation. It keeps transport and authentication outside the
storage crate. A push conflict returns the current repository view and requires
the caller to validate the ref preconditions again.

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
  crash recovery, and tenant separation.
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
