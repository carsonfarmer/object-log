# Test and benchmark contract

## Test order

Run tests in this order:

1. Pure format and state-transition unit tests.
2. In-memory backend conformance.
3. Temporary filesystem backend conformance.
4. Deterministic fault and concurrency tests.
5. Materializer and key-value tests.
6. Local MinIO conformance and protocol tests.
7. Benchmarks.

No cloud backend is part of this stage.

Format tests also compare encoded bytes with checked-in CBOR fixtures. The CDDL
file defines the published schema. Tests reject trailing data, wrong digests,
unsupported versions, and unknown fields. Protocol tests reject objects above
configured limits.

## Backend conformance cases

Every backend must prove:

- Create succeeds once and then returns a distinct already-exists result.
- Conditional update succeeds only for the observed version.
- A stale version cannot update the object.
- A conditional read distinguishes changed and unchanged objects.
- A successful write is visible to the next read and list.
- Listing is complete for an isolated prefix.
- Delete of a missing test object is idempotent, although protocol deletion is
  not used in the first release.
- Stored bytes are returned without truncation or substitution.
- A capability probe cleans up only its own random prefix.

## Protocol cases

### Initialization

- Two writers opening an absent log create one valid initial head.
- Opening an existing log does not rewrite it.
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

Inject a failure:

- Before immutable blob creation.
- After blob creation but before its response.
- Before commit-object creation.
- After commit-object creation but before its response.
- Before head mutation.
- After head mutation but before its response.
- During the first resolution read.

For each point, assert the exact classification. A response lost after the head
mutation must resolve as committed. A failure before the head mutation must not
be misreported as committed.

### Recovery

- Remove all process state and reconstruct from durable objects.
- Start from a current checkpoint and replay its tail.
- Start from an old cached cursor after the base has advanced.
- Fetch tail commits in arbitrary completion order and apply them in sequence.
- Detect a missing referenced object.
- Detect changed bytes at a digest key.
- Reject a commit that names another log identity.

### Checkpoint races

- Checkpoint with no concurrent writer.
- Append wins before checkpoint CAS.
- Checkpoint wins before append CAS.
- Two checkpoints cover the same prefix.
- A stale checkpoint covers a prefix that is no longer in the active history.
- Checkpoint removes active tail references but preserves resolution evidence.

### Tenant separation

- Equal operation bytes in two logs use distinct commit identities or verified
  log identities.
- One opened log cannot request another log's head, blob, or checkpoint key.
- Invalid separators, traversal elements, empty IDs, and overlong IDs fail.

## Deterministic model

The generated model has at least two writers, one reader, and one checkpoint
worker. Actions include open, prepare, stage, publish, refresh, resolve,
checkpoint, crash, and reopen.

After every action, the oracle checks:

- The committed history only grows before checkpoint projection.
- Every visible view equals a prefix of the canonical history.
- Every acknowledged transaction occurs exactly once.
- Every conflict candidate occurs zero times.
- Every pending result remains consistent with at least one allowed store
  history until it resolves.
- Every installed checkpoint equals its covered prefix.
- Every referenced object passes integrity verification.

Record the seed and action trace on failure.

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
size, contending candidates, and active-tail recovery. The first measured
baseline is in
[`docs/evidence/local-baseline-2026-09-02.md`](evidence/local-baseline-2026-09-02.md).
It states the cases that remain unmeasured. Add staged-payload, refresh,
checkpoint, filesystem, and MinIO performance cases before making claims about
those paths.

Later gates can compare against a retained machine-readable baseline. Do not
set a hard target that the system has not measured repeatedly.
