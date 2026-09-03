# Ordered follow-on goals

These goals start only after the local log, checkpoint, key-value proof, model
tests, benchmarks, and MinIO tests pass. Each goal keeps object storage as the
durable authority.

## 1. Garbage collection

### Required contract

- Collection operates on one log namespace.
- A plan names one observed index generation and one grace-period boundary.
- A stale plan cannot delete an object that became reachable later.
- Interrupted collection is safe to repeat.
- Readers either finish from retained objects or receive an explicit expiry.
- The collector never derives reachability from a local cache.

### Required evidence

- Model tests race append, base publication, reader recovery, and collection.
- Fault tests stop before and after every deletion.
- Tests retain every index, entry, base, and payload that is reachable at the
  safety boundary.
- Benchmarks report list requests, delete requests, bytes retained, and time by
  namespace size.

## 2. SQLite storage

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

The first design study must compare page objects, SQLite sessions changesets,
and a VFS-level journal before it selects one current contract.

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
