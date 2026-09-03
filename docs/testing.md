# Test and benchmark contract

## Test order

Run tests in this order:

1. Pure format and state-transition unit tests.
2. In-memory backend conformance.
3. Temporary filesystem backend conformance.
4. Deterministic fault and concurrency tests.
5. Materializer and key-value tests.
6. Local MinIO compatibility test.
7. Benchmarks.

No cloud backend is part of this stage.

Format tests include one stable hexadecimal encoding assertion for an empty
head. A complete set of checked-in golden values for commits, checkpoints, and
recovery tokens remains qualification work. The CDDL file defines the
published schema. Tests reject trailing data, wrong digests, unsupported
versions, and unknown fields. Protocol tests reject objects above configured
limits.

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
- Large payload references exist before the commit is visible.
- Two writers from one cursor produce one winner and one conflict.
- A loser refreshes and can prepare a new candidate.
- A stale cursor cannot publish after any head update.
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
- Start from an old cached cursor after the base has advanced.
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

### Current matrix gaps

- Complete the blob-create and commit-create fault points listed above.
- Add checked-in golden values for commits, checkpoints, and recovery tokens.
- Add a bounded full-graph scrub after the GC graph walker exists.

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
- Every referenced object passes integrity verification before publication.

This is not yet an independent model. It derives prior history from the
implementation output. The remaining qualification work must add an
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

Cold-recovery benchmarks must clear process caches. Filesystem results must
state whether the operating-system page cache was cold or warm. MinIO results
must record image version, container resources, filesystem, endpoint, and
whether the client and server share one host.

The current process-local suite covers batch payload size, inline operation
size, staged payloads, contending candidates, and metadata-only active-tail
recovery. The first measured baseline is in
[`docs/evidence/local-baseline-2026-09-02.md`](evidence/local-baseline-2026-09-02.md).
It states the cases that remain unmeasured. Add refresh, checkpoint, filesystem,
and MinIO performance cases before making claims about
those paths.

Later gates can compare against a retained machine-readable baseline. Do not
set a hard target that the system has not measured repeatedly.
