# Ordered follow-on goals

The local log, checkpoint, key-value proof, bounded garbage collection,
benchmarks, and MinIO compatibility flow are complete. Each next goal keeps
object storage as the durable authority.

## Completed: garbage collection

The implementation contract and completion record are in
[`GC_PLAN.md`](../GC_PLAN.md). The v1 protocol has bounded graph marking,
reader retention, a positive durable plan and fence, complete-set retry, view
expiry, and best-effort plan-object cleanup. Current qualification is local.

## 1. SQLite storage

The selected demonstration contract and implementation gates are in
[`SQLITE_PLAN.md`](../SQLITE_PLAN.md). Implementation is the current goal.

### Required contract

- One log owns one SQLite database history.
- One SQLite transaction maps to one atomic log publication.
- Recovery produces one database image that passes SQLite integrity checks.
- A local database file is a cache. Removing it does not remove durable state.
- The adapter defines its page size, journal mode, lock behavior, and maximum
  recovery work.

### Required evidence

- Transaction, rollback, crash, and concurrent-writer tests pass.
- Recovery is byte-stable or logically equivalent after each committed state.
- Tests cover large transactions and checkpoint races.
- Benchmarks report commit latency, write amplification, cold recovery, warm
  queries, and object-store requests.

The plan selects a raw SQLite WAL and snapshot design. Its first gate accepted
SQLite's public journal-pointer control after the same local proof passed on
macOS and Linux. Adapter implementation is next.

## 2. Minimal serverless Git

Build `object-log-git` as a separate demonstration crate after SQLite. One log
owns one Git repository. Immutable Git packs contain objects. One object-log
commit atomically records a validated ref transaction and its new pack
references. A checkpoint records the current refs and the packs needed to read
them. See [`GIT_PLAN.md`](../GIT_PLAN.md).

The first example uses a disposable bare repository or temporary directory per
serverless invocation. It keeps transport and authentication outside the
storage crate. A push conflict returns the current repository view and requires
the caller to validate the ref preconditions again.

## 3. WASI filesystem storage

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

## 4. Live AWS qualification

This is a separate qualification goal. It does not block local product
completion.

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
