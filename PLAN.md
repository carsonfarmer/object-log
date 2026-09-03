# object-log implementation plan

## Outcome

Create a small Rust library that publishes an ordered log through a general
object-store interface. The library must support concurrent writers, immutable
payload objects, disposable local caches, explicit uncertain outcomes, and
bounded recovery through checkpoints.

The first proof is a key-value state machine. Spin, SQLite, filesystem, Git,
and actor integrations are later consumers. They are not part of the first
release.

## Accepted architecture

The first release has one authority protocol:

- Each logical resource has its own log.
- Each log has one small mutable `head` object.
- A conditional update of `head` publishes a commit.
- Commit, blob, and checkpoint objects are immutable and content-addressed.
- The head contains a checkpoint reference and a bounded ordered tail of
  commit references.
- A transport error during head publication produces a `PendingCommit`.
- The core does not automatically merge or rebase application operations.
- Apache Arrow's `object_store` crate provides the storage interface.
- The first release does not delete durable objects.

This protocol favors predictable reads, checkpoint installation, and conflict
resolution over a one-write numbered-slot protocol.

## Public contract

The exact Rust names can change during implementation. The behavior cannot.

Required value types:

- `LogId`: a validated, non-path tenant resource identifier.
- `Cursor`: an opaque observed head position and storage version.
- `TransactionId`: a caller-supplied stable operation identity.
- `ObjectRef`: digest, byte length, and object kind.
- `CommitRef`: sequence, transaction ID, and commit object reference.
- `PreparedCommit`: expected cursor, transaction ID, operation bytes, result
  bytes, and staged object references.
- `PendingCommit`: enough evidence to resolve or retry one exact publication.
- `CheckpointRef`: covered sequence, covered commit, and snapshot object.

Required operations:

```rust
open(store, prefix, log_id, options) -> Log
load() -> View
refresh(cursor) -> Refresh
put_object(bytes) -> ObjectRef
prepare(cursor, transaction_id, operation, result, objects) -> PreparedCommit
commit(prepared) -> CommitStatus
resolve(pending) -> Resolution
read_tail(view) -> ordered commit records
publish_checkpoint(view, checkpoint) -> CheckpointStatus
```

Required result distinctions:

```rust
CommitStatus = Committed | Conflict | Pending
Resolution   = Committed | NotCommitted | StillPending | Expired
```

`Conflict` is a definite CAS rejection. `Pending` means that a storage error
can hide a successful CAS. No API can convert `Pending` to `Conflict` without
new evidence.

## Invariants

1. A committed log has one total commit order.
2. An acknowledged commit references only durable immutable objects.
3. A stale writer cannot replace a newer head.
4. A commit based on cursor `C` can publish only from `C`.
5. A conflict does not publish the rejected candidate.
6. A pending commit can be resolved by its transaction ID and commit digest
   while it remains in the declared resolution window.
7. A checkpoint covers one exact committed prefix.
8. Installing a checkpoint cannot remove later commits.
9. A corrupt object is reported. It is never used as valid state.
10. Removing every local cache still permits complete recovery.
11. One log cannot read or publish objects from another log namespace.
12. Head bytes cannot repeat across updates. A monotonic generation prevents
    ETag ABA when an object store derives ETags from content.

## Work streams

### 1. Repository and contract — root agent

Create the workspace, design record, test plan, CI-equivalent local commands,
and stable module ownership. Select a versioned durable encoding after a small
encode/decode benchmark and compatibility review.

Exit evidence:

- The contract documents define all public outcomes.
- `cargo fmt`, `cargo clippy`, and `cargo test` have one local command.
- No Spin dependency exists.

### 2. Storage boundary — backend agent

Implement the minimum operations needed from `object_store`. Add namespace
validation and a capability probe for conditional create, conditional update,
conditional read, strong read-after-write behavior, and listing.

Backends in this stream:

- `object_store::memory::InMemory`
- `object_store::local::LocalFileSystem`
- Common conformance tests that any later backend must pass

Exit evidence:

- Both local backends pass the same contract tests.
- Unsupported conditional behavior fails before a log is opened for writes.
- Tests use temporary directories and leave no files or processes behind.

### 3. Publication protocol — protocol agent

Implement immutable object staging, head creation, load, conditional refresh,
commit publication, conflict reporting, and pending-result resolution.

Exit evidence:

- Two writers produce one total order.
- Every returned conflict is proven not to have published its candidate.
- Lost success responses resolve to the original commit.
- A commit never becomes visible before all referenced objects exist.

### 4. Verification system — verification agent

Build a deterministic wrapper around an object store. It must inject failures
before and after each visible storage mutation. Add model-based concurrent
writer tests and Criterion benchmark scaffolding.

Exit evidence:

- A seed reproduces each generated execution.
- The oracle checks total order, prefix recovery, object integrity, and result
  classification after every action.
- Benchmarks report operation counts, bytes, latency distribution, logical
  operations per second, and durable commits per second.

### 5. Checkpoints — root agent

Add opaque snapshot objects and conditional checkpoint publication. A
checkpoint can cover a prefix while newer commits remain in the tail. Do not
delete old commits in this release.

Exit evidence:

- A checkpoint that races with appends preserves every later commit.
- Recovery uses the newest valid checkpoint and its ordered tail.
- An invalid or incomplete checkpoint cannot replace a valid base.

### 6. Materializer and key-value proof — root agent

Add an optional typed helper that restores a checkpoint and applies ordered
operation bytes. Keep serialization and domain validation outside the core.
Implement a small key-value example with `get`, `set`, `delete`, `increment`,
and compare-and-swap.

Exit evidence:

- Key-value operations remain linearizable under concurrent writers.
- Failed compare-and-swap makes no change.
- Increment returns the committed value.
- Replay and checkpoint restore produce identical state hashes.

### 7. Performance suite — verification agent and root agent

Measure:

- Warm append with inline operation bytes.
- Append with one staged payload.
- Group sizes 1, 4, 16, 64, and 256.
- One, two, eight, and thirty-two contending writers.
- Warm unchanged refresh.
- Cold recovery at several tail sizes.
- Checkpoint creation and checkpoint-based recovery.
- Memory and filesystem backends.
- MinIO with recorded local network and storage settings.

No fixed performance claim exists until a baseline is measured. After the
baseline, retain a machine-readable comparison and fail only on large,
repeatable regressions.

### 8. MinIO local qualification — backend agent and root agent

Add an opt-in test command that starts a pinned MinIO container, creates an
isolated bucket, runs the full backend conformance and protocol suites, and
always tears the container down.

Exit evidence:

- No cloud account or public endpoint is used.
- The exact MinIO image is pinned.
- Conditional writes and ambiguous-response recovery are exercised.
- Repeated runs start from empty isolated prefixes.

### 9. Independent review and reduction — review agent

Review the protocol after all local tests pass. Look for false conflict
classification, unbounded metadata, unsafe decoding, hidden mutable authority,
and public API that belongs in an adapter.

Exit evidence:

- Every finding is fixed, rejected with evidence, or recorded as a later
  limitation.
- Unused public API and speculative abstraction are removed.

## Agent file ownership

Initial exclusive ownership avoids merge conflicts:

- Backend agent: `src/store.rs`, `tests/store_conformance.rs`
- Protocol agent: `src/log.rs`, `tests/protocol.rs`
- Verification agent: `src/sim.rs`, `tests/model.rs`, `benches/`
- Root agent: `Cargo.toml`, `src/lib.rs`, `src/format.rs`, checkpoint and
  materializer modules, examples, MinIO orchestration, documentation, and
  integration changes

An agent can request an ownership change. The root agent must approve it before
the edit.

## Local completion gate

The first implementation is complete only when:

- Formatting and strict lint pass.
- Unit, conformance, protocol, model, and documentation tests pass.
- Filesystem tests pass from a new temporary directory.
- MinIO tests pass through automatic local setup and teardown.
- Criterion benchmarks run and save a baseline.
- The key-value example recovers after removal of its local cache.
- The limitations document states that one log has one serialized publication
  point.
- No remote provider has been contacted.

## Later high-throughput stage

A preferred owner is a performance layer, not a second source of truth.
Rendezvous hashing can select one process for each log. That process keeps a
materialized state, serializes requests through one bounded queue, and combines
compatible requests into one commit. It replies after the head CAS succeeds.
If two processes both believe they are the owner, CAS still selects one order.

This can produce thousands of logical operations per second through tens of
durable object-store commits per second. It does not produce thousands of
separate durable commits per second.

That higher target would require one of these later changes:

- Partition the logical state into independent logs.
- Add a faster linearizable sequencer while object storage retains immutable
  data and checkpoints.
- Add a replicated local journal and acknowledge from a quorum before object
  storage publication.

Each option changes the authority or atomicity boundary. None is part of the
first protocol.
