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
cache, a canonical v1 record, committed WAL ranges, bounded payloads, and
32-way ordered object transfers.

### Required contract

- One log owns one SQLite database history.
- One SQLite transaction maps to one atomic log publication.
- Recovery produces one database image that passes SQLite integrity checks.
- A local database file is a cache. Removing it does not remove durable state.
- The adapter defines its page size, journal mode, lock behavior, and maximum
  recovery work.

### Local evidence

- The memory suite covers transactions, rollback, uncertain results,
  cancellation, conflicting writers, checkpoints, allocation limits,
  collection, and deleted-cache recovery.
- The loopback MinIO flow covers chunked writes, uncertain publication,
  checkpointing, collection, and cold recovery.
- The Criterion suite covers 10 local latency cases. It does not count object
  requests or measure a remote service.

The same WAL-access proof passed on macOS and Linux with SQLite's public
journal-pointer control. Windows, other VFS implementations, a native
memory-safety sanitizer, live AWS, and Spin integration remain.

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
