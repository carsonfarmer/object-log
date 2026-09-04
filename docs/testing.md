# Test and benchmark contract

## Test order

Run tests in this order:

1. Pure format and state-transition unit tests.
2. In-memory backend conformance.
3. Temporary filesystem backend conformance.
4. Deterministic fault and concurrency tests.
5. Materializer and key-value tests.
6. Local MinIO compatibility test.
7. Staged-object request accounting.
8. Large local garbage-collection acceptance test.
9. Benchmarks.

No cloud backend is part of this stage.

At revision `03d4327b5d865912cfad6549faa619adc222e9c3`, the local
all-feature gate passes 189 regular tests. It also compiles six ignored,
opt-in tests. The focused garbage-collection suite has 28 tests. The memory and
temporary-filesystem tests prove repeatable immutable deletion. The opt-in
MinIO flow proves 1,001 candidates across the 1,000-key bulk-delete boundary.
The large acceptance target proves timely and exact cleanup of 100,000
memory-backed candidates and 10,001 local MinIO candidates. The staged-object
accounting target proves the request shape for 1 MiB update and 100 MiB
checkpoint workloads.

Format tests include one hexadecimal encoding assertion for an empty head.
Golden values test the current canonical encoding. They do not promise
pre-release compatibility and can change with a smaller or better v1 layout. A
complete set for commits, checkpoints, and recovery tokens remains
qualification work. The CDDL file defines the current schema. Tests reject
trailing data, wrong digests, unsupported versions, and unknown fields.
Protocol tests reject objects above configured limits.

## Backend conformance cases

Every backend must prove:

- Create succeeds once and then returns a distinct already-exists result.
- Conditional update succeeds only for the observed version.
- A stale version cannot update the object.
- A conditional read distinguishes changed and unchanged objects.
- A successful write is visible to the next read.
- Stored bytes are returned without truncation or substitution.
- A capability probe cleans up only its own object.

## Protocol cases

This section defines the target protocol matrix. The current local gate does
not yet prove every listed case. Current gaps appear below the matrix.

### Initialization

- Two writers opening an absent log create one valid initial head.
- Opening an existing log does not rewrite it.
- One validated backend handle opens many logs without another capability
  probe.
- Unsupported format versions fail closed.
- A malformed namespace is rejected before storage access.

### Publication

- One writer appends one commit.
- A batch is visible atomically as one commit.
- New object writes return a staged proof before the commit is visible.
- Same-handle publication does not read the newly staged object graph back.
- A separately opened handle cannot use another handle's proof.
- A collection-epoch change invalidates an earlier staged proof.
- `stage_objects` fully verifies and deduplicates an existing transitive graph.
- Recovery tokens omit process-local proofs and require full graph
  verification.
- Two writers from one view produce one winner and one conflict.
- A loser refreshes and can prepare a new candidate.
- A stale view cannot publish after any head update.
- A generation cannot decrease or repeat.

### Ambiguous outcomes

The target fault matrix injects a failure:

- Before immutable blob creation.
- After blob creation but before its response.
- Before commit-object creation.
- After commit-object creation but before its response.
- Before head mutation.
- After head mutation but before its response.
- During the first resolution read.
- During the head CAS after its mutation is visible.

For each point, assert the exact classification. A response lost after the head
mutation must resolve as committed. A failure before the head mutation must not
be misreported as committed.

Current tests cover head mutation before/after failures, resolution reads,
cancellation after a visible head mutation, raw object-store puts, and
referenced-object verification. Full log-level coverage of blob and commit
creation before/after failures remains qualification work.

### Recovery

- Remove all process state and reconstruct from durable objects.
- Start from a current checkpoint and replay its tail.
- Start from an old cached view after the base has advanced.
- Fetch tail commits in arbitrary completion order and apply them in sequence.
- Keep payload and node reads lazy during metadata recovery.
- Detect a missing or changed object when the adapter reads it.
- Traverse checkpoint roots and reference-node children without parsing opaque
  payload bytes.
- Reject a commit that names another log identity.

### Checkpoint races

- Checkpoint with no concurrent writer.
- Append wins before checkpoint CAS.
- Checkpoint wins before append CAS.
- Two checkpoints cover the same prefix.
- A stale checkpoint covers a prefix that is no longer in the active history.
- Checkpoint removes active tail references but preserves resolution evidence.
- Missing or corrupt checkpoint roots fail before publication.
- Missing or corrupt checkpoint objects fail during recovery.

### Tenant separation

- Equal operation bytes in two logs use distinct commit identities or verified
  log identities.
- One opened log cannot request another log's head, blob, or checkpoint key.
- Invalid separators, traversal elements, empty IDs, and overlong IDs fail.

### Garbage collection

- Exact reports and deletion of unreachable objects only.
- Preservation of the current checkpoint, tail, and nested live graph.
- Empty collection without a plan or head write.
- Both CAS orders for append, checkpoint, retention, and plan installation.
- Plan preservation through successful append and checkpoint publication.
- Direct and nested fence rejection for commits and checkpoints.
- Fence rejection for exact commit and checkpoint physical keys.
- Missing, corrupt, oversized, and over-budget live graphs before listing or
  deletion.
- Retention loss, release, reacquisition, and uncertain response recovery.
- Lost fence and clear responses.
- Complete-plan retry after partial delete and cancellation.
- Two collectors with one exact clear.
- Isolation from delayed deletes and old incarnations.
- Older-view expiry and current or retained-view corruption.
- Unknown entry scan accounting without deletion.
- Resolution of a compacted commit after its immutable body is collected.
- Large cleanup within a 30-second deadline, exact live-graph survival, and an
  empty second collection on memory and local MinIO.

A valid content-addressed cycle cannot be constructed because each node digest
binds its child references. An attempted cycle with false bytes fails digest or
format verification.

### Current matrix gaps

- Complete the blob-create and commit-create fault points listed above.
- Add golden tests for the current canonical commit, checkpoint, and recovery
  token encodings. Update them when the v1 layout changes.
- Extend the generated model with independent checkpoint and collection
  oracles.
- Run the complete conformance and protocol suites against MinIO.
- Add filesystem and live object-store performance evidence.

## Current deterministic scenario

The seeded scenario has two writers and one reader. It selects commit, resolve,
refresh, reload, reopen, and read actions. It records the seed and action trace
on failure. It currently checks:

- The committed history only grows before checkpoint projection.
- Every visible view equals a prefix of the canonical history.
- Every acknowledged transaction occurs exactly once.
- Every conflict candidate occurs zero times.
- Every pending result remains consistent with at least one allowed store
  history until it resolves.
- Every referenced object has a current staged proof or passes full integrity
  verification before publication.

The current model derives prior history from the implementation output, so it
is not independent. The remaining qualification work must add an
independent canonical history, a checkpoint worker, checkpoint and object
oracles, and separate prepare, stage, checkpoint, and crash actions.

## Benchmark contract

Use Criterion for process-local measurements. Add a separate executable for
MinIO latency and throughput because network timing does not fit Criterion's
tight-loop assumptions.

Report these metrics:

- Logical operations per second.
- Durable commits per second.
- p50, p95, and p99 operation latency.
- Object-store request count by operation.
- Bytes uploaded and downloaded.
- Head encoded size.
- Peak live memory when practical.

The target benchmark matrix is:

| Dimension | Values |
|---|---|
| Batch size | 1, 4, 16, 64, 256 |
| Inline operation | 32 B, 256 B, 4 KiB |
| Staged payload | none, 64 KiB, 1 MiB |
| Writers | 1, 2, 8, 32 |
| Tail length | 0, 16, 64, 256, 1024 |
| Backend | memory, filesystem, MinIO |

The garbage-collection matrix is:

| Operation | Shape and size |
|---|---|
| Start | 1,000 flat live objects |
| Start | 1,000 deep live objects |
| Start | 10,000 objects, half live, wide graph |
| Start | 100,000 unreachable objects |
| Fence | Planned reference in 100,000 candidates |
| Resume | 1,000 clean candidates |
| Resume | 1,001 candidates after a partial attempt |

Cold-recovery benchmarks must clear process caches. Filesystem results must
state whether the operating-system page cache was cold or warm. MinIO results
must record image version, container resources, filesystem, endpoint, and
whether the client and server share one host.

The current process-local suite covers batch payload size, inline operation
size, staged payloads, contending candidates, metadata-only active-tail
recovery, and the garbage-collection matrix above. The first measured baseline
is in
[`docs/evidence/local-baseline-2026-09-02.md`](evidence/local-baseline-2026-09-02.md).
The garbage-collection measurements are in
[`docs/evidence/gc-local-2026-09-03.md`](evidence/gc-local-2026-09-03.md).
The large acceptance results are in
[`docs/evidence/gc-acceptance-2026-09-03.md`](evidence/gc-acceptance-2026-09-03.md).
Add refresh, checkpoint, filesystem, and remote-service performance cases
before making claims about those paths.

The MinIO flow uses a pinned image and an isolated loopback endpoint. Its
integrated rerun passed one test in 2.22 seconds. The GC log contained only
`index.cbor` after collection. This provides compatibility evidence only; it
does not measure MinIO latency.

`CollectionReport::delete_attempts` counts candidate keys submitted for
deletion. It does not count HTTP requests. A provider can combine one batch
into fewer requests.

Later gates can compare against a retained machine-readable baseline. Do not
set a hard target that the system has not measured repeatedly.
