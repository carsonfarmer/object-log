# object-log-kv

A small byte-key/value library on object-log. Each atomic batch publishes one
immutable compressed radix-tree root through the WAL's conditional head. Reads
load only the requested paths. An atomic batch evaluates commands in order,
keeps changed paths transient, then stages each final changed node once while
reusing unchanged subtrees. Reopening materializes root proofs from the checkpoint
and bounded WAL tail, without reading the database. There is no local database
or second head.

```rust,no_run
# async fn example(log: object_log::Log) -> Result<(), Box<dyn std::error::Error>> {
use bytes::Bytes;
use object_log::{CommitStatus, TransactionId};
use object_log_kv::{KvCommand, KvStore, Limits, decode_results};

let limits = Limits {
    key_bytes: 64,
    value_bytes: 1024,
    batch_entries: 64,
    batch_bytes: 128 * 1024,
    page_entries: 32,
    response_bytes: 64 * 1024,
    tree_bytes: 4 * 1024 * 1024,
};
let store = KvStore::new(log, limits);
let snapshot = store.snapshot().await?;
let candidate = snapshot.prepare(TransactionId::new(), &[
    KvCommand::Set { key: Bytes::from("name"), value: Bytes::from("Ada") },
]).await?;
// Persist both before publication if this operation must survive process loss.
let recovery_token = candidate.recovery_token()?;
let result_bytes = candidate.result().clone();
match store.log().commit(candidate).await? {
    CommitStatus::Committed(_) => { let results = decode_results(&result_bytes)?; }
    CommitStatus::Conflict(_) => { /* refresh snapshot, then prepare again */ }
    CommitStatus::Pending(pending) => { /* preserve evidence; resolve, don't replay */ }
}
# Ok(())
# }
```

Snapshots are exact and immutable. `get_many` and each scan page use that same
view; `scan` uses inclusive start, exclusive end, and an exclusive continuation
key. Continue on the same snapshot. `scan_prefix` includes the empty prefix and
arbitrary binary keys. Missing values are `None`; empty keys and values are valid.
Commands in a batch execute in order and become visible atomically. Set/delete
return `Changed(bool)`; setting an identical value or deleting an absent key
returns false. Read from the same snapshot before preparing when the application
needs the previous value. A mismatched CAS returns false and changes nothing for
that command. Other batch commands still execute. Invalid integers, overflow, or
limits abort the entire preparation.
Even a no-op batch records durable results when committed.

## Operating a small-record store

Use the profile above for small application records, metadata and ordered
indexes. It is the profile exercised by the growth test below. Limits apply to
each call, not the total database: a tree can exceed `tree_bytes` while individual
paths and pages still fit. A batch that exceeds its tree budget fails as a whole,
even if its keys and values each fit. Keep the key layout and batch sizes within
the measured workload for your deployment; value-bearing ancestors increase
path-copying cost.

The qualified WAL options use `max_tail_entries = 64` and
`resolution_window = 16`, with other options at their defaults. The growth
workload checkpoints every eight batches. These are explicit deployment choices;
changing the outcome window also changes how long recovery evidence survives.

Open a dedicated WAL with `Log::open`, or use `Log::open_existing` when absence
must be an error. The backend must pass the WAL's conditional-update capability
check; the filesystem backend does not. Reuse the same WAL options when reopening.
All writers must use this crate's format and compatible key/value limits; a reader
with smaller limits can reject an existing tree. The format is pre-release; begin with a fresh
namespace. Do not let an object-store lifecycle rule or another application
delete objects in that namespace.

Prepare and publish are separate so applications can persist recovery evidence
before cancellation or process loss. Results are recorded in the WAL candidate;
persist its `result()` bytes alongside `recovery_token()` for typed process-loss
recovery, then use `Log::resume`. Only `Committed` permits returning those results.
Retry as new work only after `Conflict` or `NotCommitted`. `StillPending` requires
resolution; `Expired` means the result cannot safely be determined, not failure.
The WAL retention window bounds deduplication and recovery evidence. Limit
conflict retries and the number of concurrent calls in the host; per-call limits
do not cap process memory.

Call `snapshot.checkpoint()` before the WAL tail fills. It publishes the current
root and copies no tree data. Handle its conflict/pending statuses through the
WAL. Retain `snapshot.view()` with `Log::retain` before long scans or concurrent
collection; confirm retention before use and keep its ID until release is
confirmed, including on cancellation or errors. If acquisition conflicts, load
a fresh snapshot before retrying; do not resume an old pagination cursor on it.
Retentions have no expiry and block collection for the namespace. Recover a lost
retention only after preventing new readers and draining all existing readers.
Unretained reads can return `ViewExpired` and never silently switch to a newer
view. Use `Log::start_collection` / `resume_collection` after checkpointing,
following the WAL's fencing and drained-reader recovery contract. Lost checkpoint responses
leave a recoverable WAL; reopen before starting new maintenance.

Limits bound key/value sizes, batch inputs, response bytes, and cumulative tree
work per call. Tree work includes stored node bytes read, transient nodes created
or revisited within a batch, final encoded nodes, and every traversal prefix.
Prefix and encoding buffers are size-checked before allocation and request their
final capacity directly. Large values are rejected rather than streamed.
The default value limit is 64 KiB. Mutation results contain only booleans or
integers, so their size does not depend on stored value size. The WAL's result
allowance still bounds the encoded batch results. Preparation checks that exact
encoded length before flushing the tree, so an oversized result stages nothing.
The tree budget includes repeated path work and every command in a batch. It
excludes WAL metadata: WAL options separately bound tail, commit, checkpoint,
and head sizes. Use a shared WAL request guard across retries for cumulative
logical storage calls; provider-internal HTTP attempts remain provider-specific.
No automatic conflict retries, unbounded index cache, or background work exist.

For the documented small-record profile, a conservative per-call envelope for
KV-owned variable buffers is about 4.07 MiB: the 4 MiB cumulative tree allowance,
up to 64 KiB of returned key/value bytes, the WAL's 4 KiB default result allowance,
and one 64-byte range prefix. Calls do not normally use all four at once. Rust
container bookkeeping is additional but finite: its item counts are bounded by
the key, batch, page, and byte-fanout limits. Batch-local tree state is discarded
when preparation returns.

This is not a process-memory or allocator quota. Input `Bytes` backing belongs to
the caller and can retain a larger allocation than the admitted slice. WAL view,
decode, head, materialization, and collection buffers; provider and error bodies;
the async runtime; allocator slack; and returned values retained by the caller
are separate. Concurrent calls multiply their respective envelopes. Whole-process
RSS therefore corroborates the selected deployment profile but does not prove
the per-call bound.
The finite local `MinIO` checks below qualify the stated bounded small-record
workload. Streaming values and exact allocator quotas are outside this profile.
Callers own the checkpoint, recovery, retention and collection schedule. Remote deployments
need their own provider and workload qualification.

Run `cargo test -p object-log-kv` for deterministic correctness and logical-I/O
bounds, and `cargo bench -p object-log-kv --bench kv` for the in-memory baseline.
The benchmark uses four-byte ordered keys, 64-byte values, 256/4,096-key trees,
20 Criterion samples, and two-second measurement windows. The reopen case reads
a checkpointed root and then one key. `prepare_set` measures staging an overwrite,
not WAL publication: detailed fault-store events identify its unpublished nodes
for untimed cleanup, keeping benchmark storage bounded. The other cases disable
event recording. These measurements are local in-memory results, not S3 latency.


## Local `MinIO` qualification

Run memory and filesystem capability checks before the provider tests. The
filesystem backend is expected to reject conditional updates. From the repository
root (Docker, AWS CLI, and curl installed; `ps` is optional for RSS reporting):

```sh
cargo test --workspace --all-features
export CARGO_PROFILE_TEST_OPT_LEVEL=3 CARGO_PROFILE_TEST_DEBUG=0
cargo test -p object-log-kv --all-features
cargo test -p object-log-kv --test qualification memory_large_growth_and_contention -- --ignored --nocapture
./scripts/test-minio.sh kv minio_correctness_matrix object-log-kv aws
./scripts/test-minio.sh qualification minio_growth_and_contention object-log-kv aws
```

Each command starts the existing pinned `MinIO` image on a random loopback port
with its own container and disposable bucket, and verifies container removal.
Each scenario uses a random KV prefix. The tests require explicit `aws` and
`--ignored` opt-ins, reject non-loopback endpoints, and never provision AWS.
Manual invocations must use a disposable local bucket; random test prefixes are
left for inspection until the instance is removed. The existing native `MinIO`
runner option is also supported.

The correctness matrix runs the same assertions as memory: ordered batches,
binary-key reference-model checks, conflicting conditions, hidden successful
publication, cancellation after head mutation, process-loss recovery and expired
evidence, definite noncommit, pending checkpoints, retained scans, and resumed
collection after a lost deletion response, stale writer fencing, and fresh
publication while collection is active. It also checks deep value-bearing prefix
chains with byte-limited pages, and whole-batch rejection when the tree budget is
exhausted. Faults are injected around real provider operations; they do
not simulate network partitions or process kills.

The growth tests use a fixed seed, ordered `records/` keys and 1 KiB values. The
ordinary memory test grows through 256/1,024/4,096 keys; the ignored larger memory
test uses 16,384/65,536, and the opt-in `MinIO` test uses 256/4,096/16,384. Each
test keeps one namespace and grows in 64-command batches,
checkpoints every eight growth batches, then mixes 32 rounds of point reads,
fresh-snapshot reads, eight-key multi-gets, 32-entry scans, and alternating
one/eight-command overwrites and deletes. Every fourth round targets a hot key;
others use deterministic random keys. Each size also runs four writers with
32 total two-key increments, two readers checking atomic visibility, retained
snapshot scans, cold reopen, full model comparison, and collection. Retries are
bounded and occur only on definite conflicts; any pending result stops that
workload. The separate fault matrix verifies pending-result resolution.
These sizes describe this ordered-key workload, not a universal key limit.
Other key layouts can require more tree nodes and reach the WAL's default
100,000-object publication/collection bound sooner. A separate capacity test
checks that a rejected batch preserves the complete prior state, staged garbage
can be collected, and a smaller mutation can then publish.

Acceptance gates require exact model equality, untorn batches, definite
conflicts, recoverable maintenance, and smaller storage after collection.
Checkpointed snapshot loading takes at most two logical GETs; point reads use at
most six GETs at the smaller stages and seven when the larger tree also contains
the contention counters. Downloads stay below 32 KiB, within a 128 KiB tree
allowance even when live key/value data exceeds 4 MiB. The existing sparse-call
test additionally requires at most three path GETs and PUTs for an overwrite on
its binary-key fixture and no repeated path reads. Latency is reported, not used
as a machine-dependent pass/fail gate.

Output includes p50/p95 operation latency and sample counts, phase elapsed time,
logical calls and payload bytes, provider HTTP attempts (including retries and
list pagination), offered HTTP request-body bytes, conflict responses and transport/5xx errors,
stored object counts/bytes before and after GC, and process RSS at phase
boundaries. These HTTP counters wrap the established provider transport; they
are not a second storage client. HTTP body bytes include bulk-delete XML, exclude
headers/TLS, and are attempted bytes rather than proof of delivery. Logical
write amplification is uploaded object bytes divided by accepted command key
and value bytes (delete keys only); GC is reported separately through the direct provider so bulk deletion remains
intact; logical counters are unavailable for that phase. The fault wrapper used
by other phases serializes deletes to inject individual-object failures. Exact numerators
and denominators are printed, including live application bytes for storage
amplification. Operation samples include oracle comparisons; phase elapsed time also includes
untimed snapshot loads, checkpoints, and instrumentation.
RSS includes the oracle, runtime, transport, and allocator slack; it is neither
peak RSS nor an exact allocator/admission measure. It reports `unavailable` when
the platform cannot measure it. No per-request event archive
is kept. Preserve raw command/Criterion output locally under ignored `target/`
when comparing runs. Fresh-snapshot reads include bounded WAL replay, but do not
flush provider or OS caches. These are local `MinIO` results; they do not establish
remote-S3 latency or capacity for other key layouts and operation mixes.

## Remote AWS qualification

The repository's disposable AWS Terraform root, temporary local credentials,
and same-region instance-role runner are documented in
[`examples/git/qualification/aws/README.md`](../../examples/git/qualification/aws/README.md).
Set `OBJECT_LOG_AWS_BUCKET`, `OBJECT_LOG_AWS_REGION`, and a unique nonempty
`OBJECT_LOG_AWS_PREFIX`, then run the commands below. Load the temporary session
only for a local process; leave credential variables unset on the runner.

```sh
cargo test -p object-log-kv --features aws --test kv \
  aws_correctness_matrix -- --ignored --nocapture
cargo test -p object-log-kv --features aws --test qualification \
  aws_growth_and_contention -- --ignored --nocapture
cargo test -p object-log-kv --features aws --test qualification \
  aws_value_size_envelope -- --ignored --nocapture
```

The value envelope keeps the default 32 MiB cumulative tree allowance and
64 MiB WAL object allowance. It measures the default 64 KiB value and explicitly
configured 256 KiB, 1 MiB, 4 MiB, and 8 MiB values. These larger settings are
qualification profiles, not new defaults. A changed 12 MiB value is rejected by
the unchanged tree budget without publication. The test also shows why 8 MiB is
a point-operation ceiling: with an 8 MiB value on a prefix key, one descendant
mutation fits, while a two-command descendant batch exceeds the same tree-work
budget. Large values remain inline and buffered, so raising `value_bytes` also
requires coherent batch/response limits. Run against an isolated prefix and
remove that prefix before destroying the Terraform-managed bucket.

The KV measurement suite from commit `da4635f` ran on 2026-09-22 from a
same-region `t3.xlarge` Amazon Linux 2023 runner in `us-west-2`, using Rust
1.97.1 and 20 sequential set/get samples at each size. Times include the S3
work needed to publish or read one point value:

| Value | Set p50 / p95 | Get p50 / p95 |
| ---: | ---: | ---: |
| 64 KiB | 178 / 228 ms | 82 / 132 ms |
| 256 KiB | 239 / 347 ms | 127 / 309 ms |
| 1 MiB | 259 / 562 ms | 142 / 241 ms |
| 4 MiB | 289 / 373 ms | 138 / 192 ms |
| 8 MiB | 407 / 515 ms | 192 / 285 ms |

There were no provider conflicts, transport failures, or 5xx responses in the
value phases. Phase-boundary RSS rose from 20 MiB at 64 KiB to 36 MiB at 8 MiB;
the test process peaked at 75 MiB. These figures are evidence for this host,
region, sequential workload, and key layout rather than service-level promises.

The same run passed the complete AWS fault/recovery matrix and the 4,096-record
growth, contention, reopen, and collection workload. A point read used six
logical reads and downloaded 3.7 KiB. Initial growth had 1.17x logical write
amplification. Collection reduced 5,560 objects / 5.11 MB to 4,533 objects /
4.70 MB while preserving 4.23 MB of live key/value data. Phase-boundary RSS
remained below 29 MiB. Four writers completed 32 batches after 59 definite
conflicts within the cumulative request budget.

Keep 64 KiB as the general default. Up to 1 MiB is a reasonable configurable
KV profile when batch and response limits are raised together and deployment
memory is measured. The 4 MiB and 8 MiB cases are qualified for deliberately
bounded point workloads, but values remain inline and buffered, and concurrency
multiplies their memory use. Prefer object/blob storage for larger payloads or
for workloads that need high large-value concurrency.
