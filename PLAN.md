# object-log implementation plan

## Outcome

Create a small Rust library that publishes an ordered log through a general
object-store interface. The library must support concurrent writers, immutable
payload objects, disposable local caches, explicit uncertain outcomes, and
bounded recovery through checkpoints and bounded garbage collection.

The first proof is a key-value state machine. SQLite is the second public-API
consumer. Spin, filesystem, Git, and actor integrations are not part of the
first release.

## Accepted architecture

The first release has one authority protocol:

- Each logical resource has its own log.
- Each log has one small mutable `index.cbor` object.
- A conditional update of the index publishes an entry.
- Commit, blob, reference-node, checkpoint, and collection-plan objects are
  immutable. Each key combines a random physical ID with a deterministic
  BLAKE3 content digest.
- The head contains a checkpoint, a bounded ordered commit tail, recent
  outcomes, retention IDs, a collection epoch, and at most one active plan.
- A transport error during head publication produces a `PendingCommit`.
- The core does not automatically merge or rebase application operations.
- Apache Arrow's `object_store` crate provides the storage interface.
- A versioned CBOR map is the durable wire format. Numeric field keys follow
  `schema/object-log-v1.cddl`.
- Bounded garbage collection deletes only the positive set in an installed
  durable plan. The head CAS installs and clears its fence.
- One validated backend/root handle creates many tenant scopes without another
  capability probe.

The current format version is v1. Before the first release, its layout can
change when that improves or simplifies the design. The project does not keep
compatibility readers for earlier development layouts.

This protocol favors predictable reads, checkpoint installation, and conflict
resolution over a one-write numbered-slot protocol.

## Public contract

This is the current contract. Before the first release, the Rust API and wire
layout can change when that improves the design. One current API and one current
v1 layout replace earlier development forms.

Required value types:

- `LogId`: a validated, non-path tenant resource identifier.
- `View`: one opaque observed head and storage version. Cloning it does not
  copy the head.
- `TransactionId`: a caller-supplied stable operation identity.
- `ObjectRef`: digest, byte length, and object kind.
- `StagedObject`: process-local proof that one object graph is ready for
  publication by this log handle at this collection epoch.
- `ReferenceNode`: opaque payload and explicit child references.
- `CommitRef`: public sequence, transaction ID, digest, and encoded byte length.
- `PreparedCommit`: expected view, transaction ID, operation bytes, result
  bytes, and staged object references.
- `PendingCommit`: enough evidence to resolve or retry one exact publication.
- `PendingCheckpoint`: enough evidence to resolve one exact maintenance
  publication.
- `CheckpointRef`: covered sequence, covered commit, and snapshot object.
- `RetentionId`: one stable reader-retention attempt.
- `CollectionReport`: candidate count, candidate bytes, and submitted delete
  key count.

Required operations:

```rust
ValidatedBackend::new(store, prefix) -> ValidatedBackend
Log::open(&validated_backend, &log_id, options) -> Log
load() -> View
refresh(&view) -> Option<View>
preflight(&view, transaction_id) -> ()
put_object(&view, bytes) -> StagedObject
put_node(&view, payload, children: Vec<StagedObject>) -> StagedObject
stage_objects(&view, references: Vec<ObjectRef>) -> Vec<StagedObject>
staged.reference() -> &ObjectRef
read_object(&view, reference) -> bytes
read_node(&view, reference) -> ReferenceNode
prepare(&view, transaction_id, operation, result, objects: Vec<StagedObject>) -> PreparedCommit
commit(prepared) -> CommitStatus
resolve(pending) -> Resolution
resume(recovery_token) -> Resolution
read_tail(&view) -> ordered commit records
read_checkpoint(&view) -> Option<CheckpointRecord>
publish_checkpoint(&view, through, snapshot, roots: Vec<StagedObject>) -> CheckpointStatus
resolve_checkpoint(pending) -> CheckpointResolution
retain(&view, retention_id) -> RetentionStatus
release_retention(&view, retention_id) -> RetentionStatus
start_collection(&view) -> CollectionStart
resume_collection(&view) -> CollectionFinish
```

Required result distinctions:

```rust
CommitStatus = Committed | Conflict | Pending
Resolution   = Committed | NotCommitted | StillPending | Expired
CheckpointStatus = Published | Conflict | Pending
CheckpointResolution = Published | NotPublished | StillPending | Expired
RetentionStatus = Applied | ActiveCollection | Conflict | Pending
CollectionStart = Empty | Installed | Active | Retained | Conflict | Pending
CollectionFinish = Complete | Conflict | Pending
```

`Conflict` is a definite CAS rejection with its winning view. `Pending` means
that the safe final view or classification is not available. This includes an
ambiguous CAS result and a definite rejection followed by a failed read of the
winning view. No API can convert `Pending` to `Conflict` without that evidence.

`Expired` means that retained evidence cannot determine the outcome. It does
not mean `NotCommitted`. A caller must not retry non-idempotent work as a new
operation after expiry.

## Invariants

1. A committed log has one total commit order.
2. An acknowledged commit references only durable immutable objects.
3. A stale writer cannot replace a newer head.
4. A commit based on view `V` can publish only from `V`.
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
13. A durable random incarnation binds every view, WAL entry, and checkpoint
    to one log lifetime. Content digests remain deterministic within it.
14. Checkpoint roots and reference-node edges enumerate every durable object
    needed by a snapshot.
15. Normal replay verifies ordered metadata and loads payloads only on demand.
16. Every collection plan is a bounded, sorted, positive set of physical keys.
17. A fence blocks publication of a direct or transitive planned reference.
18. A collection retry submits the complete plan again. It stores no mutable
    progress record.
19. A missing read from a prior collection epoch returns view expiry. A
    missing read from the current epoch is corruption.
20. After immutable creation succeeds, the backend returns the exact bytes from
    that physical key until object-log collection deletes it. External
    lifecycle expiry, deletion, or overwrite violates the storage contract.
21. A staged proof is valid only for its source `Log` handle or a clone and its
    collection epoch. Serialization and a separately opened handle discard the
    proof and require full graph verification.

## Work streams

### 1. Repository and contract (root agent)

Create the workspace, design record, test plan, CI-equivalent local commands,
and stable module ownership. Use the one versioned CBOR contract in
`schema/object-log-v1.cddl`.

Exit evidence:

- The contract documents define all public outcomes.
- `cargo fmt`, `cargo clippy`, and `cargo test` have one local command.
- No Spin dependency exists.

### 2. Storage boundary (backend agent)

Implement the minimum operations needed from `object_store`. Add namespace
validation and a capability probe for conditional create, conditional update,
conditional read, and strong read-after-write behavior. Scoped listing and
immutable-only batch deletion support garbage collection.

Backends in this stream:

- `object_store::memory::InMemory`
- `object_store::local::LocalFileSystem`
- Common conformance tests that any later backend must pass

Exit evidence:

- The in-memory backend passes the writable contract.
- A filesystem backend either passes the same writable contract or fails
  closed before a writable log opens.
- Unsupported conditional behavior fails before a log is opened for writes.
- Tests use temporary directories and leave no files or processes behind.

### 3. Publication protocol (protocol agent)

Implement immutable object staging, head creation, load, conditional refresh,
commit publication, conflict reporting, and pending-result resolution.

Exit evidence:

- Two writers produce one total order.
- Every returned conflict is proven not to have published its candidate.
- Lost success responses resolve to the original commit.
- A commit never becomes visible before all referenced objects exist.
- New writes return opaque staged proofs. Existing references require
  `stage_objects` and full graph verification.
- Same-handle publication can use a current proof. Recovery and separately
  opened handles verify the full durable graph.

### 4. Verification system (verification agent)

Build a deterministic wrapper around an object store. It must inject failures
before and after each visible storage mutation. Add model-based concurrent
writer tests and Criterion benchmark scaffolding.

Exit evidence:

- A seed reproduces each generated execution.
- The oracle checks total order, prefix recovery, object integrity, and result
  classification after every action.
- Benchmarks report operation counts, bytes, latency distribution, logical
  operations per second, and durable commits per second.

Current status: partial. The seeded scenario covers two writers, one reader,
commit, resolve, refresh, reload, reopen, and read actions. It does not yet have
an independent history oracle, checkpoint worker, prepare-only action,
stage-only action, or explicit crash action.

### 5. Checkpoints (root agent)

Add opaque snapshot objects and conditional checkpoint publication. A
checkpoint can cover a prefix while newer commits remain in the tail. Garbage
collection can later delete the compacted commit bodies while the head keeps
the bounded resolution evidence.

Exit evidence:

- A checkpoint that races with appends preserves every later commit.
- Recovery uses the newest valid checkpoint and its ordered tail.
- An invalid or incomplete checkpoint cannot replace a valid base.

### 6. Materializer and key-value proof (root agent)

Add an optional typed helper that restores a checkpoint and applies ordered
operation bytes. Keep serialization and domain validation outside the core.
Implement a small key-value example with `get`, `set`, `delete`, `increment`,
and compare-and-swap.

Exit evidence:

- Key-value operations remain linearizable under concurrent writers.
- Failed compare-and-swap makes no change.
- Increment returns the committed value.
- Replay and checkpoint restore produce identical state hashes.

### 7. Performance suite (verification agent and root agent)

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

Current status: partial. The in-memory Criterion suite covers batch payload
size, inline size, staged payloads, contention, tail recovery, bounded
collection scans, graph shapes, fence lookup, and collection resume. It does
not yet cover filesystem or remote-service performance.

### 8. MinIO local qualification (backend agent and root agent)

Add an opt-in test command that starts a pinned MinIO container, creates an
isolated bucket, runs the full backend conformance and protocol suites, and
always tears the container down.

Exit evidence:

- No cloud account or public endpoint is used.
- The exact MinIO image is pinned.
- Conditional writes and ambiguous-response recovery are exercised.
- Repeated runs start from empty isolated prefixes.

Current status: partial. One local MinIO compatibility flow covers capability
probing, one ambiguous commit, resolution, checkpoint publication, reopen,
recovery, and collection of 1,001 objects across the bulk-delete boundary. The
complete backend and protocol suites do not yet run against MinIO.

### 9. Independent review and reduction (review agent)

Review the protocol after all local tests pass. Look for false conflict
classification, unbounded metadata, unsafe decoding, hidden mutable authority,
and public API that belongs in an adapter.

Exit evidence:

- Every finding is fixed, rejected with evidence, or recorded as a later
  limitation.
- Unused public API and speculative abstraction are removed.

Status: complete for garbage collection. The independent review found no
remaining P0 or P1 defect. The Rust simplification pass removed 80 net lines
from its code-and-test change while it removed redundant validation and
allocation.

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

The core log, checkpoint, key-value, and garbage-collection implementation is
locally complete. Its current gate requires:

- Formatting and strict lint pass.
- Unit, conformance, protocol, model, and documentation tests pass.
- Filesystem capability tests pass from a new temporary directory. A backend
  without conditional update support must fail closed.
- MinIO tests pass through automatic local setup and teardown.
- Criterion benchmarks run and save a baseline.
- The key-value example recovers after removal of its local cache.
- The limitations document states that one log has one serialized publication
  point and that current performance evidence is local.
- No remote provider has been contacted.

The remaining qualification gaps do not change this local product result.
They include broader generated-model coverage, more durable-format golden
tests for the current canonical encoding, filesystem benchmarks, full MinIO
conformance, and live AWS tests.

## Later high-throughput stage

A preferred owner adds performance without becoming a second source of truth.
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

## Ordered follow-on milestones

The local first release is the dependency for these milestones. Complete them
in this order unless a correctness defect changes the order.

### Completed: garbage collection

The v1 protocol now has bounded graph marking, reader retention, a durable
positive plan and publication fence, complete-set retry, view expiry, and
immutable deletion. The current evidence is local.

### Completed locally: SQLite storage

`object-log-sqlite` stores a complete first snapshot and later committed WAL
ranges. One log owns one database history, and the local SQLite file is a
disposable cache. The local evidence records transactions, recovery,
checkpoints, garbage collection, MinIO compatibility, object requests, byte
amplification, and latency. Live AWS and Spin integration remain separate.

### 1. Serverless Git example

Build `object-log-git` as a small public-API consumer. Store immutable Git
packs as log objects. Publish each validated push as one atomic ref
transaction. Recover from one pack-set checkpoint plus its ordered tail. Keep
the serverless transport outside the storage model. See `GIT_PLAN.md`.

### 2. WASI filesystem storage

Implement filesystem metadata and file content over the same log and object
model. Define inode identity, directory operations, rename atomicity, open-file
behavior, sparse files, and capability boundaries. Then expose the proven
model through `wasi:filesystem` for Spin.

### 3. Live AWS qualification

Run the backend conformance, fault, recovery, and performance suites against an
isolated AWS S3 prefix. Record the AWS region, bucket settings, request limits,
expected cost, time limit, health checks, failure recovery, and mandatory
teardown. This goal requires separate owner review. It is not part of local
product completion.
